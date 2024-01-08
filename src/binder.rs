use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::{
    catalog::{Catalog, DataType, Schema},
    common::{PageID, TransactionID},
    parser::{
        BaseTableReferenceAST, BinaryExpressionAST, BinaryOperator, DeleteStatementAST,
        ExpressionAST, InsertStatementAST, LiteralExpressionAST, PathExpressionAST,
        SelectStatementAST, StatementAST, TableReferenceAST, UpdateStatementAST,
    },
    tuple::Tuple,
    value::{BooleanValue, Value},
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BoundStatementAST {
    Select(BoundSelectStatementAST),
    Insert(BoundInsertStatementAST),
    Delete(BoundDeleteStatementAST),
    Update(BoundUpdateStatementAST),
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundSelectStatementAST {
    pub select_elements: Vec<BoundSelectElementAST>,
    pub table_reference: BoundTableReferenceAST,
    pub condition: Option<BoundExpressionAST>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundSelectElementAST {
    pub expression: BoundExpressionAST,
    pub name: String,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BoundTableReferenceAST {
    Base(BoundBaseTableReferenceAST),
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundBaseTableReferenceAST {
    pub table_name: String,
    pub alias: Option<String>,
    pub first_page_id: PageID,
    pub schema: Schema,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundInsertStatementAST {
    pub table_name: String,
    pub values: Vec<BoundExpressionAST>,
    pub first_page_id: PageID,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundDeleteStatementAST {
    pub table_reference: BoundBaseTableReferenceAST,
    pub condition: Option<BoundExpressionAST>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundUpdateStatementAST {
    pub table_reference: BoundBaseTableReferenceAST,
    pub assignments: Vec<BoundAssignmentAST>,
    pub condition: Option<BoundExpressionAST>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundAssignmentAST {
    pub column_name: String,
    pub value: BoundExpressionAST,
    pub column_index: usize,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BoundExpressionAST {
    Path(BoundPathExpressionAST),
    Literal(BoundLiteralExpressionAST),
    Binary(BoundBinaryExpressionAST),
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundPathExpressionAST {
    pub path: Vec<String>,
    pub column_index: usize,
    pub data_type: DataType,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundLiteralExpressionAST {
    pub value: Value,
    pub data_type: DataType,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundBinaryExpressionAST {
    pub operator: BinaryOperator,
    pub left: Box<BoundExpressionAST>,
    pub right: Box<BoundExpressionAST>,
}

enum Scope {
    Base(BoundBaseTableReferenceAST),
}
pub struct Binder {
    catalog: Arc<Mutex<Catalog>>,
    txn_id: TransactionID,
    scope: Option<Scope>,
}

impl Binder {
    pub fn new(catalog: Arc<Mutex<Catalog>>, txn_id: TransactionID) -> Self {
        Self {
            catalog,
            txn_id,
            scope: None,
        }
    }

    pub fn bind_statement(&mut self, statement: &StatementAST) -> Result<BoundStatementAST> {
        match statement {
            StatementAST::Select(statement) => self.bind_select(statement),
            StatementAST::Insert(statement) => self.bind_insert(statement),
            StatementAST::Delete(statement) => self.bind_delete(statement),
            StatementAST::Update(statement) => self.bind_update(statement),
            _ => unimplemented!(),
        }
    }

    fn bind_select(&mut self, statement: &SelectStatementAST) -> Result<BoundStatementAST> {
        let table_reference = self.bind_table_reference(&statement.table_reference)?;
        let mut select_elements = Vec::new();
        let mut unknown_count = 0;
        for element in &statement.select_elements {
            let expression = self.bind_expression(&element.expression)?;
            let name = match &element.alias {
                Some(alias) => alias.clone(),
                None => {
                    if let BoundExpressionAST::Path(path_expression) = &expression {
                        path_expression
                            .path
                            .last()
                            .ok_or_else(|| {
                                anyhow::anyhow!("path expression must have at least one element")
                            })?
                            .clone()
                    } else {
                        let c = format!("__c{}", unknown_count);
                        unknown_count += 1;
                        c
                    }
                }
            };
            select_elements.push(BoundSelectElementAST { expression, name });
        }
        let condition = match &statement.condition {
            Some(condition) => Some(self.bind_expression(condition)?),
            None => None,
        };
        Ok(BoundStatementAST::Select(BoundSelectStatementAST {
            select_elements,
            table_reference,
            condition,
        }))
    }

    fn bind_table_reference(
        &mut self,
        table_reference: &TableReferenceAST,
    ) -> Result<BoundTableReferenceAST> {
        match table_reference {
            TableReferenceAST::Base(table_reference) => Ok(BoundTableReferenceAST::Base(
                self.bind_base_table_reference(table_reference)?,
            )),
        }
    }

    fn bind_base_table_reference(
        &mut self,
        table_reference: &BaseTableReferenceAST,
    ) -> Result<BoundBaseTableReferenceAST> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?;
        let first_page_id =
            catalog.get_first_page_id_by_table_name(&table_reference.table_name, self.txn_id)?;
        let schema = catalog.get_schema_by_table_name(&table_reference.table_name, self.txn_id)?;
        let table_reference = BoundBaseTableReferenceAST {
            table_name: table_reference.table_name.clone(),
            alias: table_reference.alias.clone(),
            first_page_id,
            schema,
        };
        self.scope = Some(Scope::Base(table_reference.clone()));
        Ok(table_reference)
    }

    fn bind_insert(&mut self, statement: &InsertStatementAST) -> Result<BoundStatementAST> {
        let first_page_id = self
            .catalog
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .get_first_page_id_by_table_name(&statement.table_name, self.txn_id)?;
        let schema = self
            .catalog
            .lock()
            .map_err(|_| anyhow::anyhow!("lock error"))?
            .get_schema_by_table_name(&statement.table_name, self.txn_id)?;
        if statement.values.len() != schema.columns.len() {
            return Err(anyhow::anyhow!(
                "expected {} values, but got {}",
                schema.columns.len(),
                statement.values.len()
            ));
        }
        let mut values = Vec::new();
        for value in &statement.values {
            values.push(self.bind_expression(value)?);
        }
        Ok(BoundStatementAST::Insert(BoundInsertStatementAST {
            table_name: statement.table_name.clone(),
            values,
            first_page_id,
        }))
    }

    fn bind_delete(&mut self, statement: &DeleteStatementAST) -> Result<BoundStatementAST> {
        let table_reference = self.bind_base_table_reference(&statement.table_reference)?;
        let condition = match &statement.condition {
            Some(condition) => Some(self.bind_expression(condition)?),
            None => None,
        };
        Ok(BoundStatementAST::Delete(BoundDeleteStatementAST {
            table_reference,
            condition,
        }))
    }

    fn bind_update(&mut self, statement: &UpdateStatementAST) -> Result<BoundStatementAST> {
        let table_reference = self.bind_base_table_reference(&statement.table_reference)?;
        let mut assignments = Vec::new();
        for assignment in &statement.assignments {
            let value = self.bind_expression(&assignment.value)?;
            let (column_index, _) = self.resolve_path_expression(&PathExpressionAST {
                path: vec![assignment.column_name.clone()],
            })?;
            assignments.push(BoundAssignmentAST {
                column_name: assignment.column_name.clone(),
                value,
                column_index,
            });
        }
        let condition = match &statement.condition {
            Some(condition) => Some(self.bind_expression(condition)?),
            None => None,
        };
        Ok(BoundStatementAST::Update(BoundUpdateStatementAST {
            table_reference,
            assignments,
            condition,
        }))
    }

    fn bind_expression(&mut self, expression: &ExpressionAST) -> Result<BoundExpressionAST> {
        match expression {
            crate::parser::ExpressionAST::Path(expression) => self.bind_path_expression(expression),
            crate::parser::ExpressionAST::Literal(expression) => {
                self.bind_literal_expression(expression)
            }
            crate::parser::ExpressionAST::Binary(expression) => {
                self.bind_binary_expression(expression)
            }
        }
    }

    fn bind_path_expression(
        &mut self,
        expression: &PathExpressionAST,
    ) -> Result<BoundExpressionAST> {
        let (column_index, data_type) = self.resolve_path_expression(expression)?;
        Ok(BoundExpressionAST::Path(BoundPathExpressionAST {
            path: expression.path.clone(),
            column_index,
            data_type,
        }))
    }

    fn bind_literal_expression(
        &mut self,
        expression: &LiteralExpressionAST,
    ) -> Result<BoundExpressionAST> {
        Ok(BoundExpressionAST::Literal(BoundLiteralExpressionAST {
            value: expression.value.clone(),
            data_type: match expression.value {
                Value::Boolean(_) => DataType::Boolean,
                Value::Int(_) => DataType::Int,
                Value::Varchar(_) => DataType::Varchar,
            },
        }))
    }

    fn bind_binary_expression(
        &mut self,
        expression: &BinaryExpressionAST,
    ) -> Result<BoundExpressionAST> {
        let left = Box::new(self.bind_expression(&expression.left)?);
        let right = Box::new(self.bind_expression(&expression.right)?);
        Ok(BoundExpressionAST::Binary(BoundBinaryExpressionAST {
            operator: expression.operator.clone(),
            left,
            right,
        }))
    }

    fn resolve_path_expression(
        &mut self,
        expression: &PathExpressionAST,
    ) -> Result<(usize, DataType)> {
        match &self.scope {
            Some(Scope::Base(scope)) => {
                self.resolve_path_expression_base(&scope.clone(), expression)
            }
            None => Err(anyhow::anyhow!("no scope")),
        }
    }

    fn resolve_path_expression_base(
        &mut self,
        scope: &BoundBaseTableReferenceAST,
        expression: &PathExpressionAST,
    ) -> Result<(usize, DataType)> {
        if expression.path.len() == 1 {
            for (i, column) in scope.schema.columns.iter().enumerate() {
                if column.name == expression.path[0] {
                    return Ok((i, column.data_type.clone()));
                }
            }
            Err(anyhow::anyhow!(
                "column {} not found in table {}",
                expression.path[0],
                scope.table_name
            ))
        } else if expression.path.len() == 2 {
            let table_name = scope.alias.as_ref().unwrap_or(&scope.table_name);
            if table_name != &expression.path[0] {
                return Err(anyhow::anyhow!("table {} not found", expression.path[0]));
            }
            for (i, column) in scope.schema.columns.iter().enumerate() {
                if column.name == expression.path[1] {
                    return Ok((i, column.data_type.clone()));
                }
            }
            Err(anyhow::anyhow!(
                "column {} not found in table {}",
                expression.path[1],
                scope.table_name
            ))
        } else {
            Err(anyhow::anyhow!("path expression length must be 1 or 2"))
        }
    }
}

impl BoundExpressionAST {
    pub fn eval(&self, tuple: &Tuple, schema: &Schema) -> Value {
        match self {
            BoundExpressionAST::Path(path_expression) => {
                let values = tuple.values(schema);
                values[path_expression.column_index].clone()
            }
            BoundExpressionAST::Literal(literal_expression) => literal_expression.value.clone(),
            BoundExpressionAST::Binary(binary_expression) => {
                let left = binary_expression.left.eval(tuple, schema);
                let right = binary_expression.right.eval(tuple, schema);
                match binary_expression.operator {
                    BinaryOperator::Equal => Value::Boolean(BooleanValue(left == right)),
                }
            }
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            BoundExpressionAST::Path(path_expression) => path_expression.data_type.clone(),
            BoundExpressionAST::Literal(literal_expression) => literal_expression.data_type.clone(),
            BoundExpressionAST::Binary(binary_expression) => {
                let left = binary_expression.left.data_type();
                let right = binary_expression.right.data_type();
                if left == right {
                    left
                } else {
                    unimplemented!()
                }
            }
        }
    }
}
