use crate::{
    binder::{
        BoundAssignmentAST, BoundBaseTableReferenceAST, BoundDeleteStatementAST,
        BoundExpressionAST, BoundFunctionCallExpressionAST, BoundInsertStatementAST,
        BoundJoinTableReferenceAST, BoundLimitAST, BoundOrderByElementAST, BoundSelectElementAST,
        BoundSelectStatementAST, BoundStatementAST, BoundSubqueryTableReferenceAST,
        BoundTableReferenceAST, BoundUpdateStatementAST,
    },
    catalog::{Column, DataType, Schema},
    common::PageID,
    parser::JoinType,
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Plan {
    SeqScan(SeqScanPlan),
    IndexScan(IndexScanPlan),
    Filter(FilterPlan),
    Project(ProjectPlan),
    NestedLoopJoin(NestedLoopJoinPlan),
    Aggregate(AggregatePlan),
    Sort(SortPlan),
    Limit(LimitPlan),
    EmptyRow(EmptyRowPlan),
    Insert(InsertPlan),
    Delete(DeletePlan),
    Update(UpdatePlan),
}
impl Plan {
    pub fn schema(&self) -> &Schema {
        match self {
            Plan::SeqScan(plan) => &plan.schema,
            Plan::IndexScan(plan) => &plan.schema,
            Plan::Filter(plan) => &plan.schema,
            Plan::Project(plan) => &plan.schema,
            Plan::NestedLoopJoin(plan) => &plan.schema,
            Plan::Aggregate(plan) => &plan.schema,
            Plan::Sort(plan) => &plan.schema,
            Plan::Limit(plan) => &plan.schema,
            Plan::EmptyRow(plan) => &plan.schema,
            Plan::Insert(plan) => &plan.schema,
            Plan::Delete(plan) => &plan.schema,
            Plan::Update(plan) => &plan.schema,
        }
    }
    pub fn children(&self) -> Vec<Box<Plan>> {
        match self {
            Plan::SeqScan(_) => vec![],
            Plan::IndexScan(_) => vec![],
            Plan::Filter(plan) => vec![plan.child.clone()],
            Plan::Project(plan) => vec![plan.child.clone()],
            Plan::NestedLoopJoin(plan) => plan.children.clone(),
            Plan::Aggregate(plan) => vec![plan.child.clone()],
            Plan::Sort(plan) => vec![plan.child.clone()],
            Plan::Limit(plan) => vec![plan.child.clone()],
            Plan::EmptyRow(_) => vec![],
            Plan::Insert(_) => vec![],
            Plan::Delete(plan) => vec![plan.child.clone()],
            Plan::Update(plan) => vec![plan.child.clone()],
        }
    }
    pub fn set_children(&mut self, children: Vec<Plan>) {
        match self {
            Plan::SeqScan(_) => {}
            Plan::IndexScan(_) => {}
            Plan::Filter(plan) => plan.child = Box::new(children[0].clone()),
            Plan::Project(plan) => plan.child = Box::new(children[0].clone()),
            Plan::NestedLoopJoin(plan) => {
                plan.children = children.into_iter().map(Box::new).collect()
            }
            Plan::Aggregate(plan) => plan.child = Box::new(children[0].clone()),
            Plan::Sort(plan) => plan.child = Box::new(children[0].clone()),
            Plan::Limit(plan) => plan.child = Box::new(children[0].clone()),
            Plan::EmptyRow(_) => {}
            Plan::Insert(_) => {}
            Plan::Delete(plan) => plan.child = Box::new(children[0].clone()),
            Plan::Update(plan) => plan.child = Box::new(children[0].clone()),
        }
    }
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SeqScanPlan {
    pub first_page_id: PageID,
    pub schema: Schema,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct IndexScanPlan {
    pub index_id: i64,
    pub schema: Schema,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct FilterPlan {
    pub condition: BoundExpressionAST,
    pub schema: Schema,
    pub child: Box<Plan>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ProjectPlan {
    pub select_elements: Vec<BoundSelectElementAST>,
    pub schema: Schema,
    pub child: Box<Plan>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct NestedLoopJoinPlan {
    pub schema: Schema,
    pub children: Vec<Box<Plan>>,
    pub conditions: Vec<Option<BoundExpressionAST>>,
    pub join_types: Vec<JoinType>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AggregatePlan {
    pub schema: Schema,
    pub child: Box<Plan>,
    pub group_by: Vec<BoundExpressionAST>,
    pub aggregate_functions: Vec<BoundFunctionCallExpressionAST>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SortPlan {
    pub order_by: Vec<BoundOrderByElementAST>,
    pub schema: Schema,
    pub child: Box<Plan>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct LimitPlan {
    pub limit: BoundLimitAST,
    pub schema: Schema,
    pub child: Box<Plan>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EmptyRowPlan {
    pub schema: Schema,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct InsertPlan {
    pub first_page_id: PageID,
    pub table_schema: Schema,
    pub column_names: Option<Vec<String>>,
    pub values: Vec<BoundExpressionAST>,
    pub schema: Schema,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DeletePlan {
    pub first_page_id: PageID,
    pub schema: Schema,
    pub child: Box<Plan>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct UpdatePlan {
    pub first_page_id: PageID,
    pub assignments: Vec<BoundAssignmentAST>,
    pub schema: Schema,
    pub child: Box<Plan>,
}

pub struct Planner {
    statement: BoundStatementAST,
}
impl Planner {
    pub fn new(statement: BoundStatementAST) -> Self {
        Self { statement }
    }
    pub fn plan(&self) -> Plan {
        match &self.statement {
            BoundStatementAST::Select(select_statement) => {
                self.plan_select_statement(select_statement)
            }
            BoundStatementAST::Insert(insert_statement) => {
                self.plan_insert_statement(insert_statement)
            }
            BoundStatementAST::Delete(delete_statement) => {
                self.plan_delete_statement(delete_statement)
            }
            BoundStatementAST::Update(update_statement) => {
                self.plan_update_statement(update_statement)
            }
        }
    }
    fn plan_select_statement(&self, select_statement: &BoundSelectStatementAST) -> Plan {
        if select_statement.table_reference.is_none() {
            return Plan::Project(ProjectPlan {
                select_elements: select_statement.select_elements.clone(),
                schema: Schema {
                    columns: select_statement
                        .select_elements
                        .iter()
                        .map(|select_element| Column {
                            name: select_element.name.clone(),
                            // TODO: not use dummy type
                            data_type: select_element
                                .expression
                                .data_type()
                                .unwrap_or(DataType::Boolean),
                        })
                        .collect(),
                },
                child: Box::new(Plan::EmptyRow(EmptyRowPlan {
                    schema: Schema { columns: vec![] },
                })),
            });
        }
        let mut plan = match select_statement.table_reference {
            Some(ref table_reference) => self.plan_table_reference(table_reference),
            None => unreachable!(),
        };
        if let Some(condition) = &select_statement.condition {
            plan = Plan::Filter(FilterPlan {
                condition: condition.clone(),
                schema: plan.schema().clone(),
                child: Box::new(plan),
            });
        }
        if !select_statement.group_by.is_empty() || !select_statement.aggregate_functions.is_empty()
        {
            let aggregate_functions = select_statement
                .aggregate_functions
                .clone()
                .into_iter()
                .map(|expression| {
                    if let BoundExpressionAST::FunctionCall(expression) = expression {
                        BoundFunctionCallExpressionAST {
                            function_name: expression.function_name,
                            arguments: expression.arguments,
                        }
                    } else {
                        unreachable!()
                    }
                })
                .collect::<Vec<_>>();
            plan = Plan::Aggregate(AggregatePlan {
                schema: Schema {
                    columns: select_statement
                        .group_by
                        .iter()
                        .map(|expression| {
                            if let BoundExpressionAST::Path(path_expression) = expression {
                                Column {
                                    name: path_expression.column_name.clone(),
                                    data_type: path_expression
                                        .data_type
                                        .clone()
                                        // TODO: not use dummy type
                                        .unwrap_or(DataType::Boolean),
                                }
                            } else {
                                unreachable!()
                            }
                        })
                        .chain(aggregate_functions.iter().map(|_| Column {
                            name: "__agg".to_string(),
                            // TODO: not use dummy type
                            data_type: DataType::Integer,
                        }))
                        .collect::<Vec<_>>(),
                },
                child: Box::new(plan),
                group_by: select_statement.group_by.clone(),
                aggregate_functions: aggregate_functions.clone(),
            });
        }
        if let Some(having) = &select_statement.having {
            plan = Plan::Filter(FilterPlan {
                condition: having.clone(),
                schema: plan.schema().clone(),
                child: Box::new(plan),
            });
        }
        if !select_statement.select_elements.is_empty() {
            plan = Plan::Project(ProjectPlan {
                select_elements: select_statement.select_elements.clone(),
                schema: Schema {
                    columns: select_statement
                        .select_elements
                        .iter()
                        .map(|select_element| Column {
                            name: select_element.name.clone(),
                            // TODO: not use dummy type
                            data_type: select_element
                                .expression
                                .data_type()
                                .unwrap_or(DataType::Boolean),
                        })
                        .collect(),
                },
                child: Box::new(plan),
            });
        }
        if let Some(order_by) = &select_statement.order_by {
            plan = Plan::Sort(SortPlan {
                order_by: order_by.clone(),
                schema: plan.schema().clone(),
                child: Box::new(plan),
            });
        }
        if let Some(limit) = &select_statement.limit {
            plan = Plan::Limit(LimitPlan {
                limit: limit.clone(),
                schema: plan.schema().clone(),
                child: Box::new(plan),
            });
        }
        plan
    }
    fn plan_table_reference(&self, table_reference: &BoundTableReferenceAST) -> Plan {
        match table_reference {
            BoundTableReferenceAST::Base(table_reference) => {
                self.plan_base_table_reference(table_reference)
            }
            BoundTableReferenceAST::Join(table_reference) => {
                self.plan_join_table_reference(table_reference)
            }
            BoundTableReferenceAST::Subquery(table_reference) => {
                self.plan_subquery_table_reference(table_reference)
            }
        }
    }
    fn plan_base_table_reference(&self, table_reference: &BoundBaseTableReferenceAST) -> Plan {
        Plan::SeqScan(SeqScanPlan {
            first_page_id: table_reference.first_page_id,
            schema: table_reference.schema.clone(),
        })
    }
    fn plan_join_table_reference(&self, table_reference: &BoundJoinTableReferenceAST) -> Plan {
        let mut conditions = vec![table_reference.condition.clone()];
        let mut join_types = vec![table_reference.join_type.clone()];
        let outer_child = self.plan_table_reference(&table_reference.left);
        let inner_children = self.recursive_plan_table_reference(
            &table_reference.right,
            &mut conditions,
            &mut join_types,
        );
        let mut schema = Schema {
            columns: outer_child.schema().columns.clone(),
        };
        for inner_child in &inner_children {
            schema.columns.extend(inner_child.schema().columns.clone());
        }
        let mut children = vec![Box::new(outer_child)];
        children.extend(inner_children.into_iter().map(Box::new));
        Plan::NestedLoopJoin(NestedLoopJoinPlan {
            schema,
            children,
            conditions,
            join_types,
        })
    }
    fn plan_subquery_table_reference(
        &self,
        table_reference: &BoundSubqueryTableReferenceAST,
    ) -> Plan {
        self.plan_select_statement(&table_reference.select_statement)
    }
    fn recursive_plan_table_reference(
        &self,
        table_reference: &BoundTableReferenceAST,
        conditions: &mut Vec<Option<BoundExpressionAST>>,
        join_types: &mut Vec<JoinType>,
    ) -> Vec<Plan> {
        match table_reference {
            BoundTableReferenceAST::Base(table_reference) => {
                vec![self.plan_base_table_reference(table_reference)]
            }
            BoundTableReferenceAST::Join(join) => {
                conditions.push(join.condition.clone());
                join_types.push(join.join_type.clone());
                vec![
                    self.recursive_plan_table_reference(&join.left, conditions, join_types),
                    self.recursive_plan_table_reference(&join.right, conditions, join_types),
                ]
                .into_iter()
                .flatten()
                .collect()
            }
            BoundTableReferenceAST::Subquery(table_reference) => {
                vec![self.plan_subquery_table_reference(table_reference)]
            }
        }
    }
    fn plan_insert_statement(&self, insert_statement: &BoundInsertStatementAST) -> Plan {
        Plan::Insert(InsertPlan {
            first_page_id: insert_statement.first_page_id,
            table_schema: insert_statement.table_schema.clone(),
            column_names: insert_statement.column_names.clone(),
            values: insert_statement.values.clone(),
            schema: Schema {
                columns: vec![Column {
                    name: "__insert_count".to_owned(),
                    data_type: DataType::Integer,
                }],
            },
        })
    }
    fn plan_delete_statement(&self, delete_statement: &BoundDeleteStatementAST) -> Plan {
        let mut plan = self.plan_base_table_reference(&delete_statement.table_reference);
        let first_page_id = match &plan {
            Plan::SeqScan(plan) => plan.first_page_id,
            _ => unreachable!(),
        };
        if let Some(condition) = &delete_statement.condition {
            plan = Plan::Filter(FilterPlan {
                condition: condition.clone(),
                schema: plan.schema().clone(),
                child: Box::new(plan),
            });
        }
        Plan::Delete(DeletePlan {
            first_page_id,
            schema: Schema {
                columns: vec![Column {
                    name: "__delete_count".to_owned(),
                    data_type: DataType::Integer,
                }],
            },
            child: Box::new(plan),
        })
    }
    fn plan_update_statement(&self, update_statement: &BoundUpdateStatementAST) -> Plan {
        let mut plan = self.plan_base_table_reference(&update_statement.table_reference);
        let first_page_id = match &plan {
            Plan::SeqScan(plan) => plan.first_page_id,
            _ => unreachable!(),
        };
        if let Some(condition) = &update_statement.condition {
            plan = Plan::Filter(FilterPlan {
                condition: condition.clone(),
                schema: plan.schema().clone(),
                child: Box::new(plan),
            });
        }
        Plan::Update(UpdatePlan {
            first_page_id,
            assignments: update_statement.assignments.clone(),
            schema: Schema {
                columns: vec![Column {
                    name: "__update_count".to_owned(),
                    data_type: DataType::Integer,
                }],
            },
            child: Box::new(plan),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        binder::{
            Binder, BoundBinaryExpressionAST, BoundLiteralExpressionAST, BoundPathExpressionAST,
        },
        lexer::tokenize,
        parser::{BinaryOperator, Parser},
        test_helpers::setup_test_database,
        value::{integer::IntegerValue, Value},
    };

    use super::*;
    use anyhow::Result;

    #[test]
    fn test_plan_delete_statement() -> Result<()> {
        let instance = setup_test_database()?;

        let sql = "DELETE FROM t1 WHERE c1 = 1";
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);
        let statement = parser.parse()?;

        let txn_id = instance.begin(None)?;
        let mut binder = Binder::new(instance.catalog, txn_id);
        let bound_statement = binder.bind_statement(&statement)?;

        let planner = Planner::new(bound_statement);
        let plan = planner.plan();
        assert_eq!(
            plan,
            Plan::Delete(DeletePlan {
                first_page_id: PageID(5),
                schema: Schema {
                    columns: vec![Column {
                        name: "__delete_count".to_owned(),
                        data_type: DataType::Integer,
                    }],
                },
                child: Box::new(Plan::Filter(FilterPlan {
                    condition: BoundExpressionAST::Binary(BoundBinaryExpressionAST {
                        operator: BinaryOperator::Equal,
                        left: Box::new(BoundExpressionAST::Path(BoundPathExpressionAST {
                            path: vec!["c1".to_owned()],
                            data_type: Some(DataType::Integer),
                            table_index: 0,
                            column_index: 0,
                            table_name: "t1".to_owned(),
                            column_name: "c1".to_owned(),
                        })),
                        right: Box::new(BoundExpressionAST::Literal(BoundLiteralExpressionAST {
                            value: Value::Integer(IntegerValue(1)),
                            data_type: Some(DataType::Integer),
                        })),
                    }),
                    schema: Schema {
                        columns: vec![
                            Column {
                                name: "c1".to_owned(),
                                data_type: DataType::Integer,
                            },
                            Column {
                                name: "c2".to_owned(),
                                data_type: DataType::Varchar,
                            },
                        ],
                    },
                    child: Box::new(Plan::SeqScan(SeqScanPlan {
                        first_page_id: PageID(5),
                        schema: Schema {
                            columns: vec![
                                Column {
                                    name: "c1".to_owned(),
                                    data_type: DataType::Integer,
                                },
                                Column {
                                    name: "c2".to_owned(),
                                    data_type: DataType::Varchar,
                                },
                            ],
                        },
                    })),
                })),
            })
        );
        Ok(())
    }
}
