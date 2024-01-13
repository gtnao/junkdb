use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::{
    catalog::{Catalog, DataType, Schema},
    common::{PageID, TransactionID},
    parser::{
        BaseTableReferenceAST, BinaryExpressionAST, BinaryOperator, DeleteStatementAST,
        ExpressionAST, FunctionCallExpressionAST, InsertStatementAST, JoinTableReferenceAST,
        JoinType, LiteralExpressionAST, PathExpressionAST, SelectStatementAST, StatementAST,
        TableReferenceAST, UnaryExpressionAST, UnaryOperator, UpdateStatementAST,
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
    Join(BoundJoinTableReferenceAST),
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundBaseTableReferenceAST {
    pub table_name: String,
    pub alias: Option<String>,
    pub first_page_id: PageID,
    pub schema: Schema,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundJoinTableReferenceAST {
    pub left: Box<BoundTableReferenceAST>,
    pub right: Box<BoundTableReferenceAST>,
    pub condition: Option<BoundExpressionAST>,
    pub join_type: JoinType,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundInsertStatementAST {
    pub table_name: String,
    pub values: Vec<BoundExpressionAST>,
    pub first_page_id: PageID,
    pub table_schema: Schema,
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
    pub target: PathExpressionAST,
    pub value: BoundExpressionAST,
    pub column_index: usize,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BoundExpressionAST {
    Path(BoundPathExpressionAST),
    Literal(BoundLiteralExpressionAST),
    Unary(BoundUnaryExpressionAST),
    Binary(BoundBinaryExpressionAST),
    FunctionCall(BoundFunctionCallExpressionAST),
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundPathExpressionAST {
    pub path: Vec<String>,
    pub table_index: usize,
    pub column_index: usize,
    pub data_type: DataType,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundLiteralExpressionAST {
    pub value: Value,
    pub data_type: Option<DataType>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundUnaryExpressionAST {
    pub operator: UnaryOperator,
    pub operand: Box<BoundExpressionAST>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundBinaryExpressionAST {
    pub operator: BinaryOperator,
    pub left: Box<BoundExpressionAST>,
    pub right: Box<BoundExpressionAST>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundFunctionCallExpressionAST {
    pub function_name: String,
    pub arguments: Vec<BoundExpressionAST>,
}

struct Scope {
    tables: Vec<BoundBaseTableReferenceAST>,
}
pub struct Binder {
    catalog: Arc<Mutex<Catalog>>,
    txn_id: TransactionID,
    scopes: Vec<Scope>,
}

impl Binder {
    pub fn new(catalog: Arc<Mutex<Catalog>>, txn_id: TransactionID) -> Self {
        Self {
            catalog,
            txn_id,
            scopes: Vec::new(),
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
        self.scopes.push(Scope { tables: Vec::new() });
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
        self.scopes.pop();
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
            TableReferenceAST::Join(table_reference) => Ok(BoundTableReferenceAST::Join(
                self.bind_join_table_reference(table_reference)?,
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
        self.scopes
            .last_mut()
            .ok_or_else(|| anyhow::anyhow!("no scope"))?
            .tables
            .push(table_reference.clone());
        Ok(table_reference)
    }

    fn bind_join_table_reference(
        &mut self,
        table_reference: &JoinTableReferenceAST,
    ) -> Result<BoundJoinTableReferenceAST> {
        let left = Box::new(self.bind_table_reference(&table_reference.left)?);
        let right = Box::new(self.bind_table_reference(&table_reference.right)?);
        let condition: Option<BoundExpressionAST> = table_reference
            .condition
            .as_ref()
            .map(|condition| self.bind_expression(condition))
            .transpose()?;
        Ok(BoundJoinTableReferenceAST {
            left,
            right,
            condition,
            join_type: table_reference.join_type.clone(),
        })
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
            table_schema: schema,
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
            let (_, column_index, _) = self.resolve_path_expression(&PathExpressionAST {
                path: assignment.target.path.clone(),
            })?;
            assignments.push(BoundAssignmentAST {
                target: assignment.target.clone(),
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
            ExpressionAST::Path(expression) => self.bind_path_expression(expression),
            ExpressionAST::Literal(expression) => self.bind_literal_expression(expression),
            ExpressionAST::Unary(expression) => self.bind_unary_expression(expression),
            ExpressionAST::Binary(expression) => self.bind_binary_expression(expression),
            ExpressionAST::FunctionCall(expression) => {
                self.bind_function_call_expression(expression)
            }
        }
    }

    fn bind_path_expression(
        &mut self,
        expression: &PathExpressionAST,
    ) -> Result<BoundExpressionAST> {
        let (table_index, column_index, data_type) = self.resolve_path_expression(expression)?;
        Ok(BoundExpressionAST::Path(BoundPathExpressionAST {
            path: expression.path.clone(),
            table_index,
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
                Value::Integer(_) => Some(DataType::Integer),
                Value::UnsignedInteger(_) => Some(DataType::UnsignedInteger),
                Value::BigInteger(_) => Some(DataType::BigInteger),
                Value::UnsignedBigInteger(_) => Some(DataType::UnsignedBigInteger),
                Value::Varchar(_) => Some(DataType::Varchar),
                Value::Boolean(_) => Some(DataType::Boolean),
                Value::Null => None,
            },
        }))
    }

    fn bind_unary_expression(
        &mut self,
        expression: &UnaryExpressionAST,
    ) -> Result<BoundExpressionAST> {
        let operand = Box::new(self.bind_expression(&expression.operand)?);
        Ok(BoundExpressionAST::Unary(BoundUnaryExpressionAST {
            operator: expression.operator.clone(),
            operand,
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

    fn bind_function_call_expression(
        &mut self,
        expression: &FunctionCallExpressionAST,
    ) -> Result<BoundExpressionAST> {
        let mut arguments = Vec::new();
        for argument in &expression.arguments {
            arguments.push(self.bind_expression(argument)?);
        }
        Ok(BoundExpressionAST::FunctionCall(
            BoundFunctionCallExpressionAST {
                function_name: expression.function_name.clone(),
                arguments,
            },
        ))
    }

    fn resolve_path_expression(
        &mut self,
        expression: &PathExpressionAST,
    ) -> Result<(usize, usize, DataType)> {
        let scope = self
            .scopes
            .last()
            .ok_or_else(|| anyhow::anyhow!("no scope"))?;
        if expression.path.len() == 1 {
            for (i, table) in scope.tables.iter().enumerate() {
                for (j, column) in table.schema.columns.iter().enumerate() {
                    if column.name == expression.path[0] {
                        return Ok((i, j, column.data_type.clone()));
                    }
                }
            }
            Err(anyhow::anyhow!("column {} not found", expression.path[0]))
        } else if expression.path.len() == 2 {
            let table_names = scope
                .tables
                .iter()
                .map(|table| {
                    table
                        .alias
                        .as_ref()
                        .unwrap_or(&table.table_name)
                        .to_string()
                })
                .collect::<Vec<_>>();
            let matched_table_indexes = table_names
                .iter()
                .enumerate()
                .filter(|(_, table_name)| table_name == &&expression.path[0])
                .map(|(i, _)| i)
                .collect::<Vec<_>>();
            if matched_table_indexes.len() == 0 {
                return Err(anyhow::anyhow!("table {} not found", expression.path[0]));
            }
            if matched_table_indexes.len() > 1 {
                return Err(anyhow::anyhow!("ambiguous column {}", expression.path[0]));
            }
            for (i, column) in scope.tables[matched_table_indexes[0]]
                .schema
                .columns
                .iter()
                .enumerate()
            {
                if column.name == expression.path[1] {
                    return Ok((matched_table_indexes[0], i, column.data_type.clone()));
                }
            }
            Err(anyhow::anyhow!(
                "column {}.{} not found in table",
                expression.path[0],
                expression.path[1]
            ))
        } else {
            Err(anyhow::anyhow!("path expression length must be 1 or 2"))
        }
    }
}

impl BoundExpressionAST {
    pub fn eval(&self, tuples: &Vec<&Tuple>, schemas: &Vec<&Schema>) -> Value {
        match self {
            BoundExpressionAST::Path(path_expression) => {
                let tuple = &tuples[path_expression.table_index];
                let values = tuple.values(schemas[path_expression.table_index]);
                values[path_expression.column_index].clone()
            }
            BoundExpressionAST::Literal(literal_expression) => literal_expression.value.clone(),
            BoundExpressionAST::Unary(unary_expression) => {
                let operand = unary_expression.operand.eval(tuples, schemas);
                // TODO:
                unimplemented!()
            }
            BoundExpressionAST::Binary(binary_expression) => {
                let left = binary_expression.left.eval(tuples, schemas);
                let right = binary_expression.right.eval(tuples, schemas);
                match binary_expression.operator {
                    BinaryOperator::Equal => Value::Boolean(BooleanValue(left.perform_eq(&right))),
                    // TODO: implement other operators
                    _ => unimplemented!(),
                }
            }
            // TODO: function call
            _ => unimplemented!(),
        }
    }

    pub fn data_type(&self) -> Option<DataType> {
        match self {
            BoundExpressionAST::Path(path_expression) => Some(path_expression.data_type.clone()),
            BoundExpressionAST::Literal(literal_expression) => literal_expression.data_type.clone(),
            BoundExpressionAST::Unary(unary_expression) => unary_expression.operand.data_type(),
            BoundExpressionAST::Binary(binary_expression) => {
                let left = binary_expression.left.data_type();
                let right = binary_expression.right.data_type();
                if left.is_none() || right.is_none() {
                    return None;
                }
                if left == right {
                    left
                } else {
                    Some(left.unwrap().convert_with(right.unwrap()))
                }
            }
            // TODO: function call
            _ => unimplemented!(),
        }
    }
}
