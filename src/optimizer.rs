use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::{
    binder::BoundExpressionAST,
    catalog::Catalog,
    common::TransactionID,
    plan::{IndexScanPlan, Plan},
};

pub struct Optimizer {
    catalog: Arc<Mutex<Catalog>>,
    txn_id: TransactionID,
}
impl Optimizer {
    pub fn new(catalog: Arc<Mutex<Catalog>>, txn_id: TransactionID) -> Self {
        Self { catalog, txn_id }
    }
    pub fn optimize(&self, plan: Plan) -> Result<Plan> {
        let mut optimized_plan = plan;
        optimized_plan = self.optimize_filter_index_scan(optimized_plan)?;
        Ok(optimized_plan)
    }
    fn optimize_filter_index_scan(&self, plan: Plan) -> Result<Plan> {
        let mut children = Vec::new();
        for child in plan.children() {
            let optimized_child = self.optimize_filter_index_scan(*child)?;
            children.push(optimized_child);
        }
        let mut source_plan = plan.clone();
        source_plan.set_children(children);
        if let Plan::Filter(filter_plan) = plan {
            if let BoundExpressionAST::Binary(binary_expression) = filter_plan.condition {
                let binary_expression_clone = binary_expression.clone();
                if let BoundExpressionAST::Path(path_expression) = *binary_expression.left {
                    let indexes = self
                        .catalog
                        .lock()
                        .map_err(|_| anyhow::anyhow!("Catalog lock error"))?
                        .get_indexes_by_table_name(&path_expression.table_name, self.txn_id)?;
                    for index in indexes {
                        if index.columns.len() == 1
                            && index.columns[0] == path_expression.column_name
                        {
                            if let BoundExpressionAST::Literal(_) = *binary_expression.right {
                                let table_schema = self
                                    .catalog
                                    .lock()
                                    .map_err(|_| anyhow::anyhow!("Catalog lock error"))?
                                    .get_schema_by_table_name(
                                        &path_expression.table_name,
                                        self.txn_id,
                                    )?;
                                return Ok(Plan::IndexScan(IndexScanPlan {
                                    index_id: index.id,
                                    schema: filter_plan.schema,
                                    binary_expression: binary_expression_clone,
                                    table_schema,
                                }));
                            }
                        }
                    }
                }
            }
        }
        Ok(source_plan)
    }
}
