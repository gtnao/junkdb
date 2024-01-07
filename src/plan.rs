use crate::{
    catalog::Schema,
    tuple::Tuple,
    value::{BooleanValue, Value},
};

#[derive(Debug, Clone)]
pub enum Plan {
    SeqScan(SeqScanPlan),
    Filter(FilterPlan),
    Project(ProjectPlan),
    Insert(InsertPlan),
    Delete(DeletePlan),
    Update(UpdatePlan),
}
impl Plan {
    pub fn schema(&self) -> &Schema {
        match self {
            Plan::SeqScan(plan) => &plan.schema,
            Plan::Filter(plan) => &plan.schema,
            Plan::Project(plan) => &plan.schema,
            Plan::Insert(plan) => &plan.schema,
            Plan::Delete(plan) => &plan.schema,
            Plan::Update(plan) => &plan.schema,
        }
    }
}
#[derive(Debug, Clone)]
pub struct SeqScanPlan {
    pub table_name: String,
    pub schema: Schema,
}
#[derive(Debug, Clone)]
pub struct FilterPlan {
    pub predicate: Expression,
    pub schema: Schema,
    pub child: Box<Plan>,
}
#[derive(Debug, Clone)]
pub struct ProjectPlan {
    pub select_elements: Vec<SelectElement>,
    pub schema: Schema,
    pub child: Box<Plan>,
}
#[derive(Debug, Clone)]
pub struct SelectElement {
    pub expression: Expression,
    pub alias: Option<String>,
}
#[derive(Debug, Clone)]
pub struct InsertPlan {
    pub table_name: String,
    pub values: Vec<Expression>,
    pub schema: Schema,
}
#[derive(Debug, Clone)]
pub struct DeletePlan {
    pub table_name: String,
    pub schema: Schema,
    pub child: Box<Plan>,
}
#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub schema: Schema,
    pub child: Box<Plan>,
}
#[derive(Debug, Clone)]
pub struct Assignment {
    pub path: PathExpression,
    pub expression: Expression,
}

#[derive(Debug, Clone)]
pub enum Expression {
    Path(PathExpression),
    Literal(LiteralExpression),
    Binary(BinaryExpression),
}
#[derive(Debug, Clone)]
pub struct PathExpression {
    pub column_name: String,
}
#[derive(Debug, Clone)]
pub struct LiteralExpression {
    pub value: Value,
}
#[derive(Debug, Clone)]
pub struct BinaryExpression {
    pub operator: BinaryOperator,
    pub left: Box<Expression>,
    pub right: Box<Expression>,
}
#[derive(Debug, Clone)]
pub enum BinaryOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

impl Expression {
    pub fn eval(&self, tuple: &Tuple, schema: &Schema) -> Value {
        match self {
            Expression::Path(path_expression) => {
                let index = schema.column_index(&path_expression.column_name).unwrap();
                let values = tuple.values(schema);
                values[index].clone()
            }
            Expression::Literal(literal_expression) => literal_expression.value.clone(),
            Expression::Binary(binary_expression) => {
                let left = binary_expression.left.eval(tuple, schema);
                let right = binary_expression.right.eval(tuple, schema);
                match binary_expression.operator {
                    BinaryOperator::Equal => Value::Boolean(BooleanValue(left == right)),
                    BinaryOperator::NotEqual => Value::Boolean(BooleanValue(left != right)),
                    BinaryOperator::LessThan => Value::Boolean(BooleanValue(left < right)),
                    BinaryOperator::LessThanOrEqual => Value::Boolean(BooleanValue(left <= right)),
                    BinaryOperator::GreaterThan => Value::Boolean(BooleanValue(left > right)),
                    BinaryOperator::GreaterThanOrEqual => {
                        Value::Boolean(BooleanValue(left >= right))
                    }
                }
            }
        }
    }
}
