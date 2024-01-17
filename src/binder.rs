use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::{
    catalog::{Catalog, Column, DataType, Schema},
    common::{PageID, TransactionID},
    parser::{
        BaseTableReferenceAST, BinaryExpressionAST, BinaryOperator, DeleteStatementAST,
        ExpressionAST, FunctionCallExpressionAST, InsertStatementAST, JoinTableReferenceAST,
        JoinType, LiteralExpressionAST, PathExpressionAST, SelectElementAST, SelectStatementAST,
        StatementAST, SubqueryTableReferenceAST, TableReferenceAST, UnaryExpressionAST,
        UnaryOperator, UpdateStatementAST, AGGREGATE_FUNCTION_NAMES,
    },
    tuple::Tuple,
    value::{boolean::BooleanValue, Value},
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
    pub table_reference: Box<BoundTableReferenceAST>,
    pub condition: Option<BoundExpressionAST>,
    pub group_by: Vec<BoundExpressionAST>,
    pub aggregate_functions: Vec<BoundExpressionAST>,
    pub having: Option<BoundExpressionAST>,
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
    Subquery(BoundSubqueryTableReferenceAST),
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
pub struct BoundSubqueryTableReferenceAST {
    pub select_statement: BoundSelectStatementAST,
    pub alias: String,
    pub schema: Schema,
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
    pub table_name: String,
    pub column_name: String,
    pub data_type: Option<DataType>,
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

struct ScopeTable {
    table_name: String,
    alias: Option<String>,
    columns: Vec<ScopeColumn>,
}
struct ScopeColumn {
    column_name: String,
    data_type: Option<DataType>,
}
struct ScopeAggregation {
    group_by: Vec<ScopeAggregationGroupByItem>,
    aggregate_functions: Vec<FunctionCallExpressionAST>,
}
struct ScopeAggregationGroupByItem {
    table_name: String,
    column_name: String,
    data_type: Option<DataType>,
}
struct Scope {
    tables: Vec<ScopeTable>,
    aggregation: Option<ScopeAggregation>,
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
            StatementAST::Select(statement) => {
                Ok(BoundStatementAST::Select(self.bind_select(statement)?))
            }
            StatementAST::Insert(statement) => self.bind_insert(statement),
            StatementAST::Delete(statement) => self.bind_delete(statement),
            StatementAST::Update(statement) => self.bind_update(statement),
            _ => unimplemented!(),
        }
    }

    fn bind_select(&mut self, statement: &SelectStatementAST) -> Result<BoundSelectStatementAST> {
        self.scopes.push(Scope {
            tables: Vec::new(),
            aggregation: None,
        });
        let table_reference = self.bind_table_reference(&statement.table_reference)?;
        let condition = match &statement.condition {
            Some(condition) => Some(self.bind_expression(condition)?),
            None => None,
        };

        let group_by = match &statement.group_by {
            Some(group_by) => group_by
                .iter()
                .map(|expression| self.bind_expression(expression))
                .collect::<Result<Vec<_>>>()?,
            None => vec![],
        };
        let aggregate_functions =
            self.extract_aggregate_functions(&statement.having, &statement.select_elements)?;
        let bound_aggregate_functions = aggregate_functions
            .iter()
            .map(|function| self.bind_function_call_expression(function))
            .collect::<Result<Vec<_>>>()?;
        let needs_aggregation = group_by.len() > 0 || bound_aggregate_functions.len() > 0;
        if needs_aggregation {
            self.replace_scope_by_aggregation(&group_by, &aggregate_functions);
        }
        let having = match &statement.having {
            Some(having) => Some(self.bind_expression(having)?),
            None => None,
        };
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
        self.scopes.pop();
        Ok(BoundSelectStatementAST {
            select_elements,
            table_reference: Box::new(table_reference),
            condition,
            group_by,
            aggregate_functions: bound_aggregate_functions,
            having,
        })
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
            TableReferenceAST::Subquery(table_reference) => Ok(BoundTableReferenceAST::Subquery(
                self.bind_subquery_table_reference(table_reference)?,
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
            .push(ScopeTable {
                table_name: table_reference.table_name.clone(),
                alias: table_reference.alias.clone(),
                columns: table_reference
                    .schema
                    .columns
                    .iter()
                    .map(|column| ScopeColumn {
                        column_name: column.name.clone(),
                        data_type: Some(column.data_type.clone()),
                    })
                    .collect::<Vec<_>>(),
            });
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

    fn bind_subquery_table_reference(
        &mut self,
        table_reference: &SubqueryTableReferenceAST,
    ) -> Result<BoundSubqueryTableReferenceAST> {
        let select_statement = self.bind_select(&table_reference.select_statement)?;
        let alias = table_reference.alias.clone();
        let schema = Schema {
            columns: select_statement
                .select_elements
                .iter()
                .map(|element| Column {
                    name: element.name.clone(),
                    // TODO:
                    data_type: element.expression.data_type().unwrap(),
                })
                .collect::<Vec<_>>(),
        };
        self.scopes
            .last_mut()
            .ok_or_else(|| anyhow::anyhow!("no scope"))?
            .tables
            .push(ScopeTable {
                table_name: alias.clone(),
                alias: Some(alias.clone()),
                columns: schema
                    .columns
                    .iter()
                    .map(|column| ScopeColumn {
                        column_name: column.name.clone(),
                        data_type: Some(column.data_type.clone()),
                    })
                    .collect::<Vec<_>>(),
            });
        Ok(BoundSubqueryTableReferenceAST {
            select_statement,
            alias,
            schema,
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
        self.scopes.push(Scope {
            tables: Vec::new(),
            aggregation: None,
        });
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
        self.scopes.push(Scope {
            tables: Vec::new(),
            aggregation: None,
        });
        let table_reference = self.bind_base_table_reference(&statement.table_reference)?;
        let mut assignments = Vec::new();
        for assignment in &statement.assignments {
            let value = self.bind_expression(&assignment.value)?;
            let (_, column_index, _, _, _) = self.resolve_path_expression(&PathExpressionAST {
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
        let (table_index, column_index, table_name, column_name, data_type) =
            self.resolve_path_expression(expression)?;
        Ok(BoundExpressionAST::Path(BoundPathExpressionAST {
            path: expression.path.clone(),
            table_index,
            column_index,
            table_name,
            column_name,
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
        let scope = self
            .scopes
            .last()
            .ok_or_else(|| anyhow::anyhow!("no scope"))?;
        if let Some(aggregation) = &scope.aggregation {
            for (i, function) in aggregation.aggregate_functions.iter().enumerate() {
                if function == expression {
                    return Ok(BoundExpressionAST::Path(BoundPathExpressionAST {
                        path: vec![format!("__agg{}", i)],
                        table_index: 0,
                        column_index: aggregation.group_by.len() + i,
                        table_name: "".to_string(),
                        column_name: "".to_string(),
                        // TODO:
                        data_type: Some(DataType::Integer),
                    }));
                }
            }
        }
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
    ) -> Result<(usize, usize, String, String, Option<DataType>)> {
        let scope = self
            .scopes
            .last()
            .ok_or_else(|| anyhow::anyhow!("no scope"))?;

        if let Some(aggregation) = &scope.aggregation {
            if expression.path.len() == 1 {
                for (i, group_by) in aggregation.group_by.iter().enumerate() {
                    if group_by.column_name == expression.path[0] {
                        return Ok((
                            0,
                            i,
                            group_by.table_name.clone(),
                            group_by.column_name.clone(),
                            group_by.data_type.clone(),
                        ));
                    }
                }
                return Err(anyhow::anyhow!("column {} not found", expression.path[0]));
            } else if expression.path.len() == 2 {
                for (i, group_by) in aggregation.group_by.iter().enumerate() {
                    if group_by.table_name == expression.path[0]
                        && group_by.column_name == expression.path[1]
                    {
                        return Ok((
                            0,
                            i,
                            group_by.table_name.clone(),
                            group_by.column_name.clone(),
                            group_by.data_type.clone(),
                        ));
                    }
                }
                return Err(anyhow::anyhow!(
                    "column {}.{} not found",
                    expression.path[0],
                    expression.path[1]
                ));
            } else {
                return Err(anyhow::anyhow!("path expression length must be 1 or 2"));
            }
        }

        if expression.path.len() == 1 {
            for (i, table) in scope.tables.iter().enumerate() {
                for (j, column) in table.columns.iter().enumerate() {
                    if column.column_name == expression.path[0] {
                        return Ok((
                            i,
                            j,
                            table.table_name.clone(),
                            column.column_name.clone(),
                            column.data_type.clone(),
                        ));
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
                .columns
                .iter()
                .enumerate()
            {
                if column.column_name == expression.path[1] {
                    return Ok((
                        matched_table_indexes[0],
                        i,
                        scope.tables[matched_table_indexes[0]].table_name.clone(),
                        column.column_name.clone(),
                        column.data_type.clone(),
                    ));
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

    fn extract_aggregate_functions(
        &mut self,
        having: &Option<ExpressionAST>,
        select_elements: &Vec<SelectElementAST>,
    ) -> Result<Vec<FunctionCallExpressionAST>> {
        let mut aggregate_functions = Vec::new();
        if let Some(having) = having {
            let mut functions = self.extract_aggregate_functions_from_expression(having)?;
            aggregate_functions.append(&mut functions);
        }
        for select_element in select_elements {
            let mut functions =
                self.extract_aggregate_functions_from_expression(&select_element.expression)?;
            aggregate_functions.append(&mut functions);
        }
        Ok(aggregate_functions)
    }

    fn extract_aggregate_functions_from_expression(
        &mut self,
        expression: &ExpressionAST,
    ) -> Result<Vec<FunctionCallExpressionAST>> {
        match expression {
            ExpressionAST::FunctionCall(expression) => {
                if AGGREGATE_FUNCTION_NAMES.contains(&expression.function_name.as_str()) {
                    Ok(vec![expression.clone()])
                } else {
                    Ok(Vec::new())
                }
            }
            ExpressionAST::Path(_) => Ok(Vec::new()),
            ExpressionAST::Literal(_) => Ok(Vec::new()),
            ExpressionAST::Unary(expression) => {
                self.extract_aggregate_functions_from_expression(&expression.operand)
            }
            ExpressionAST::Binary(expression) => {
                let mut left =
                    self.extract_aggregate_functions_from_expression(&expression.left)?;
                let mut right =
                    self.extract_aggregate_functions_from_expression(&expression.right)?;
                left.append(&mut right);
                Ok(left)
            }
        }
    }

    fn replace_scope_by_aggregation(
        &mut self,
        group_by: &Vec<BoundExpressionAST>,
        aggregate_functions: &Vec<FunctionCallExpressionAST>,
    ) {
        let mut group_by_items = Vec::new();
        for expression in group_by {
            if let BoundExpressionAST::Path(path_expression) = expression {
                group_by_items.push(ScopeAggregationGroupByItem {
                    table_name: path_expression.table_name.clone(),
                    column_name: path_expression.column_name.clone(),
                    data_type: path_expression.data_type.clone(),
                });
            } else {
                unimplemented!();
            }
        }
        let aggregation = ScopeAggregation {
            group_by: group_by_items,
            aggregate_functions: aggregate_functions.clone(),
        };
        self.scopes
            .last_mut()
            .unwrap()
            .aggregation
            .replace(aggregation);
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
            BoundExpressionAST::Unary(_) => {
                // let operand = unary_expression.operand.eval(tuples, schemas);
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
            BoundExpressionAST::Path(path_expression) => path_expression.data_type.clone(),
            BoundExpressionAST::Literal(literal_expression) => literal_expression.data_type.clone(),
            BoundExpressionAST::Unary(unary_expression) => unary_expression.operand.data_type(),
            BoundExpressionAST::Binary(binary_expression) => {
                if binary_expression.operator == BinaryOperator::Equal {
                    return Some(DataType::Boolean);
                }
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

#[cfg(test)]
mod tests {
    use crate::{
        catalog::Column,
        lexer::tokenize,
        parser::Parser,
        test_helpers::setup_test_database,
        value::{integer::IntegerValue, varchar::VarcharValue},
    };

    use super::*;
    use anyhow::Result;

    #[test]
    fn test_bind_select() -> Result<()> {
        let instance = setup_test_database()?;

        let sql = "select 1, c1 as _c1, _t1.c2, 'a' from t1 as _t1 where c1 = 1";
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);
        let statement = parser.parse()?;

        let txn_id = instance.begin(None)?;
        let mut binder = Binder::new(instance.catalog, txn_id);
        let bound_statement = binder.bind_statement(&statement)?;
        assert_eq!(
            bound_statement,
            BoundStatementAST::Select(BoundSelectStatementAST {
                select_elements: vec![
                    BoundSelectElementAST {
                        expression: BoundExpressionAST::Literal(BoundLiteralExpressionAST {
                            value: Value::Integer(IntegerValue(1)),
                            data_type: Some(DataType::Integer),
                        }),
                        name: "__c0".to_string(),
                    },
                    BoundSelectElementAST {
                        expression: BoundExpressionAST::Path(BoundPathExpressionAST {
                            path: vec!["c1".to_string()],
                            table_index: 0,
                            column_index: 0,
                            table_name: "t1".to_string(),
                            column_name: "c1".to_string(),
                            data_type: Some(DataType::Integer),
                        }),
                        name: "_c1".to_string(),
                    },
                    BoundSelectElementAST {
                        expression: BoundExpressionAST::Path(BoundPathExpressionAST {
                            path: vec!["_t1".to_string(), "c2".to_string()],
                            table_index: 0,
                            column_index: 1,
                            table_name: "t1".to_string(),
                            column_name: "c2".to_string(),
                            data_type: Some(DataType::Varchar),
                        }),
                        name: "c2".to_string(),
                    },
                    BoundSelectElementAST {
                        expression: BoundExpressionAST::Literal(BoundLiteralExpressionAST {
                            value: Value::Varchar(VarcharValue("a".to_string())),
                            data_type: Some(DataType::Varchar),
                        }),
                        name: "__c1".to_string(),
                    },
                ],
                table_reference: Box::new(BoundTableReferenceAST::Base(
                    BoundBaseTableReferenceAST {
                        table_name: "t1".to_string(),
                        alias: Some("_t1".to_string()),
                        first_page_id: PageID(3),
                        schema: Schema {
                            columns: vec![
                                Column {
                                    name: "c1".to_string(),
                                    data_type: DataType::Integer,
                                },
                                Column {
                                    name: "c2".to_string(),
                                    data_type: DataType::Varchar,
                                },
                            ],
                        },
                    }
                )),
                condition: Some(BoundExpressionAST::Binary(BoundBinaryExpressionAST {
                    operator: BinaryOperator::Equal,
                    left: Box::new(BoundExpressionAST::Path(BoundPathExpressionAST {
                        path: vec!["c1".to_string()],
                        table_index: 0,
                        column_index: 0,
                        table_name: "t1".to_string(),
                        column_name: "c1".to_string(),
                        data_type: Some(DataType::Integer),
                    })),
                    right: Box::new(BoundExpressionAST::Literal(BoundLiteralExpressionAST {
                        value: Value::Integer(IntegerValue(1)),
                        data_type: Some(DataType::Integer),
                    })),
                })),
                group_by: Vec::new(),
                aggregate_functions: Vec::new(),
                having: None,
            })
        );
        Ok(())
    }

    #[test]
    fn test_bind_select_join() -> Result<()> {
        let instance = setup_test_database()?;

        let sql = "select t1.c1, _t2.c1 from t1 inner join t2 as _t2 on t1.c1 = _t2.t1_c1";
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);
        let statement = parser.parse()?;

        let txn_id = instance.begin(None)?;
        let mut binder = Binder::new(instance.catalog, txn_id);
        let bound_statement = binder.bind_statement(&statement)?;
        assert_eq!(
            bound_statement,
            BoundStatementAST::Select(BoundSelectStatementAST {
                select_elements: vec![
                    BoundSelectElementAST {
                        expression: BoundExpressionAST::Path(BoundPathExpressionAST {
                            path: vec!["t1".to_string(), "c1".to_string()],
                            table_index: 0,
                            column_index: 0,
                            table_name: "t1".to_string(),
                            column_name: "c1".to_string(),
                            data_type: Some(DataType::Integer),
                        }),
                        name: "c1".to_string(),
                    },
                    BoundSelectElementAST {
                        expression: BoundExpressionAST::Path(BoundPathExpressionAST {
                            path: vec!["_t2".to_string(), "c1".to_string()],
                            table_index: 1,
                            column_index: 1,
                            table_name: "t2".to_string(),
                            column_name: "c1".to_string(),
                            data_type: Some(DataType::Integer),
                        }),
                        name: "c1".to_string(),
                    },
                ],
                table_reference: Box::new(BoundTableReferenceAST::Join(
                    BoundJoinTableReferenceAST {
                        left: Box::new(BoundTableReferenceAST::Base(BoundBaseTableReferenceAST {
                            table_name: "t1".to_string(),
                            alias: None,
                            first_page_id: PageID(3),
                            schema: Schema {
                                columns: vec![
                                    Column {
                                        name: "c1".to_string(),
                                        data_type: DataType::Integer,
                                    },
                                    Column {
                                        name: "c2".to_string(),
                                        data_type: DataType::Varchar,
                                    },
                                ],
                            },
                        })),
                        right: Box::new(BoundTableReferenceAST::Base(BoundBaseTableReferenceAST {
                            table_name: "t2".to_string(),
                            alias: Some("_t2".to_string()),
                            first_page_id: PageID(4),
                            schema: Schema {
                                columns: vec![
                                    Column {
                                        name: "t1_c1".to_string(),
                                        data_type: DataType::Integer,
                                    },
                                    Column {
                                        name: "c1".to_string(),
                                        data_type: DataType::Integer,
                                    },
                                    Column {
                                        name: "c2".to_string(),
                                        data_type: DataType::Varchar,
                                    },
                                ]
                            }
                        })),
                        condition: Some(BoundExpressionAST::Binary(BoundBinaryExpressionAST {
                            operator: BinaryOperator::Equal,
                            left: Box::new(BoundExpressionAST::Path(BoundPathExpressionAST {
                                path: vec!["t1".to_string(), "c1".to_string()],
                                table_index: 0,
                                column_index: 0,
                                table_name: "t1".to_string(),
                                column_name: "c1".to_string(),
                                data_type: Some(DataType::Integer),
                            })),
                            right: Box::new(BoundExpressionAST::Path(BoundPathExpressionAST {
                                path: vec!["_t2".to_string(), "t1_c1".to_string()],
                                table_index: 1,
                                column_index: 0,
                                table_name: "t2".to_string(),
                                column_name: "t1_c1".to_string(),
                                data_type: Some(DataType::Integer),
                            })),
                        })),
                        join_type: JoinType::Inner,
                    }
                )),
                condition: None,
                group_by: Vec::new(),
                aggregate_functions: Vec::new(),
                having: None,
            })
        );
        Ok(())
    }

    #[test]
    fn test_bind_subquery() -> Result<()> {
        let instance = setup_test_database()?;

        let sql = r#"
            select
              sub1.c1,
              literal1
            from (
              select
                'foo' AS literal1,
                c1,
                c2
              from t1
            ) as sub1;
        "#;
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);
        let statement = parser.parse()?;

        let txn_id = instance.begin(None)?;
        let mut binder = Binder::new(instance.catalog, txn_id);
        let bound_statement = binder.bind_statement(&statement)?;
        assert_eq!(
            bound_statement,
            BoundStatementAST::Select(BoundSelectStatementAST {
                select_elements: vec![
                    BoundSelectElementAST {
                        expression: BoundExpressionAST::Path(BoundPathExpressionAST {
                            path: vec!["sub1".to_string(), "c1".to_string()],
                            table_index: 0,
                            column_index: 1,
                            table_name: "sub1".to_string(),
                            column_name: "c1".to_string(),
                            data_type: Some(DataType::Integer),
                        }),
                        name: "c1".to_string(),
                    },
                    BoundSelectElementAST {
                        expression: BoundExpressionAST::Path(BoundPathExpressionAST {
                            path: vec!["literal1".to_string()],
                            table_index: 0,
                            column_index: 0,
                            table_name: "sub1".to_string(),
                            column_name: "literal1".to_string(),
                            data_type: Some(DataType::Varchar),
                        }),
                        name: "literal1".to_string(),
                    },
                ],
                table_reference: Box::new(BoundTableReferenceAST::Subquery(
                    BoundSubqueryTableReferenceAST {
                        select_statement: BoundSelectStatementAST {
                            select_elements: vec![
                                BoundSelectElementAST {
                                    expression: BoundExpressionAST::Literal(
                                        BoundLiteralExpressionAST {
                                            value: Value::Varchar(VarcharValue("foo".to_string())),
                                            data_type: Some(DataType::Varchar),
                                        }
                                    ),
                                    name: "literal1".to_string(),
                                },
                                BoundSelectElementAST {
                                    expression: BoundExpressionAST::Path(BoundPathExpressionAST {
                                        path: vec!["c1".to_string()],
                                        table_index: 0,
                                        column_index: 0,
                                        table_name: "t1".to_string(),
                                        column_name: "c1".to_string(),
                                        data_type: Some(DataType::Integer),
                                    }),
                                    name: "c1".to_string(),
                                },
                                BoundSelectElementAST {
                                    expression: BoundExpressionAST::Path(BoundPathExpressionAST {
                                        path: vec!["c2".to_string()],
                                        table_index: 0,
                                        column_index: 1,
                                        table_name: "t1".to_string(),
                                        column_name: "c2".to_string(),
                                        data_type: Some(DataType::Varchar),
                                    }),
                                    name: "c2".to_string(),
                                },
                            ],
                            table_reference: Box::new(BoundTableReferenceAST::Base(
                                BoundBaseTableReferenceAST {
                                    table_name: "t1".to_string(),
                                    alias: None,
                                    first_page_id: PageID(3),
                                    schema: Schema {
                                        columns: vec![
                                            Column {
                                                name: "c1".to_string(),
                                                data_type: DataType::Integer,
                                            },
                                            Column {
                                                name: "c2".to_string(),
                                                data_type: DataType::Varchar,
                                            },
                                        ],
                                    },
                                }
                            )),
                            condition: None,
                            group_by: Vec::new(),
                            aggregate_functions: Vec::new(),
                            having: None,
                        },
                        alias: "sub1".to_string(),
                        schema: Schema {
                            columns: vec![
                                Column {
                                    name: "literal1".to_string(),
                                    data_type: DataType::Varchar,
                                },
                                Column {
                                    name: "c1".to_string(),
                                    data_type: DataType::Integer,
                                },
                                Column {
                                    name: "c2".to_string(),
                                    data_type: DataType::Varchar,
                                },
                            ],
                        },
                    }
                )),
                condition: None,
                group_by: Vec::new(),
                aggregate_functions: Vec::new(),
                having: None,
            })
        );
        Ok(())
    }

    #[test]
    fn test_bind_insert() -> Result<()> {
        let instance = setup_test_database()?;

        let sql = "insert into t1 values (1, 'foo')";
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);
        let statement = parser.parse()?;

        let txn_id = instance.begin(None)?;
        let mut binder = Binder::new(instance.catalog, txn_id);
        let bound_statement = binder.bind_statement(&statement)?;
        assert_eq!(
            bound_statement,
            BoundStatementAST::Insert(BoundInsertStatementAST {
                table_name: "t1".to_string(),
                values: vec![
                    BoundExpressionAST::Literal(BoundLiteralExpressionAST {
                        value: Value::Integer(IntegerValue(1)),
                        data_type: Some(DataType::Integer),
                    }),
                    BoundExpressionAST::Literal(BoundLiteralExpressionAST {
                        value: Value::Varchar(VarcharValue("foo".to_string())),
                        data_type: Some(DataType::Varchar),
                    }),
                ],
                first_page_id: PageID(3),
                table_schema: Schema {
                    columns: vec![
                        Column {
                            name: "c1".to_string(),
                            data_type: DataType::Integer,
                        },
                        Column {
                            name: "c2".to_string(),
                            data_type: DataType::Varchar,
                        },
                    ],
                },
            })
        );
        Ok(())
    }

    #[test]
    fn test_bind_delete() -> Result<()> {
        let instance = setup_test_database()?;

        let sql = "delete from t1 where c1 = 1";
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);
        let statement = parser.parse()?;

        let txn_id = instance.begin(None)?;
        let mut binder = Binder::new(instance.catalog, txn_id);
        let bound_statement = binder.bind_statement(&statement)?;
        assert_eq!(
            bound_statement,
            BoundStatementAST::Delete(BoundDeleteStatementAST {
                table_reference: BoundBaseTableReferenceAST {
                    table_name: "t1".to_string(),
                    alias: None,
                    first_page_id: PageID(3),
                    schema: Schema {
                        columns: vec![
                            Column {
                                name: "c1".to_string(),
                                data_type: DataType::Integer,
                            },
                            Column {
                                name: "c2".to_string(),
                                data_type: DataType::Varchar,
                            },
                        ],
                    },
                },
                condition: Some(BoundExpressionAST::Binary(BoundBinaryExpressionAST {
                    operator: BinaryOperator::Equal,
                    left: Box::new(BoundExpressionAST::Path(BoundPathExpressionAST {
                        path: vec!["c1".to_string()],
                        table_index: 0,
                        column_index: 0,
                        table_name: "t1".to_string(),
                        column_name: "c1".to_string(),
                        data_type: Some(DataType::Integer),
                    })),
                    right: Box::new(BoundExpressionAST::Literal(BoundLiteralExpressionAST {
                        value: Value::Integer(IntegerValue(1)),
                        data_type: Some(DataType::Integer),
                    })),
                })),
            })
        );
        Ok(())
    }

    #[test]
    fn test_bind_update() -> Result<()> {
        let instance = setup_test_database()?;

        let sql = "update t1 set c1 = 2, c2 = 'foo' where c1 = 1";
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);
        let statement = parser.parse()?;

        let txn_id = instance.begin(None)?;
        let mut binder = Binder::new(instance.catalog, txn_id);
        let bound_statement = binder.bind_statement(&statement)?;
        assert_eq!(
            bound_statement,
            BoundStatementAST::Update(BoundUpdateStatementAST {
                table_reference: BoundBaseTableReferenceAST {
                    table_name: "t1".to_string(),
                    alias: None,
                    first_page_id: PageID(3),
                    schema: Schema {
                        columns: vec![
                            Column {
                                name: "c1".to_string(),
                                data_type: DataType::Integer,
                            },
                            Column {
                                name: "c2".to_string(),
                                data_type: DataType::Varchar,
                            },
                        ],
                    },
                },
                assignments: vec![
                    BoundAssignmentAST {
                        target: PathExpressionAST {
                            path: vec!["c1".to_string()],
                        },
                        value: BoundExpressionAST::Literal(BoundLiteralExpressionAST {
                            value: Value::Integer(IntegerValue(2)),
                            data_type: Some(DataType::Integer),
                        }),
                        column_index: 0,
                    },
                    BoundAssignmentAST {
                        target: PathExpressionAST {
                            path: vec!["c2".to_string()],
                        },
                        value: BoundExpressionAST::Literal(BoundLiteralExpressionAST {
                            value: Value::Varchar(VarcharValue("foo".to_string())),
                            data_type: Some(DataType::Varchar),
                        }),
                        column_index: 1,
                    }
                ],
                condition: Some(BoundExpressionAST::Binary(BoundBinaryExpressionAST {
                    operator: BinaryOperator::Equal,
                    left: Box::new(BoundExpressionAST::Path(BoundPathExpressionAST {
                        path: vec!["c1".to_string()],
                        table_index: 0,
                        column_index: 0,
                        table_name: "t1".to_string(),
                        column_name: "c1".to_string(),
                        data_type: Some(DataType::Integer),
                    })),
                    right: Box::new(BoundExpressionAST::Literal(BoundLiteralExpressionAST {
                        value: Value::Integer(IntegerValue(1)),
                        data_type: Some(DataType::Integer),
                    })),
                })),
            })
        );
        Ok(())
    }
}
