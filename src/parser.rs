use anyhow::{anyhow, Result};

use crate::{
    catalog::DataType,
    lexer::{Keyword, Token},
    value::{integer::IntegerValue, Value},
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum StatementAST {
    CreateTable(CreateTableStatementAST),
    Select(SelectStatementAST),
    Insert(InsertStatementAST),
    Delete(DeleteStatementAST),
    Update(UpdateStatementAST),
    Begin,
    Commit,
    Rollback,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CreateTableStatementAST {
    pub table_name: String,
    pub elements: Vec<TableElementAST>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TableElementAST {
    pub column_name: String,
    pub data_type: DataType,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SelectStatementAST {
    pub select_elements: Vec<SelectElementAST>,
    pub table_reference: TableReferenceAST,
    pub condition: Option<ExpressionAST>,
    pub group_by: Option<Vec<ExpressionAST>>,
    pub having: Option<ExpressionAST>,
    pub order_by: Option<Vec<OrderByElementAST>>,
    pub limit: Option<LimitAST>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SelectElementAST {
    pub expression: ExpressionAST,
    pub alias: Option<String>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum TableReferenceAST {
    Base(BaseTableReferenceAST),
    Join(JoinTableReferenceAST),
    Subquery(SubqueryTableReferenceAST),
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BaseTableReferenceAST {
    pub table_name: String,
    pub alias: Option<String>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct JoinTableReferenceAST {
    pub left: Box<TableReferenceAST>,
    pub right: Box<TableReferenceAST>,
    pub condition: Option<ExpressionAST>,
    pub join_type: JoinType,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum JoinType {
    Inner,
    Left,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SubqueryTableReferenceAST {
    pub select_statement: Box<SelectStatementAST>,
    pub alias: String,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct OrderByElementAST {
    pub expression: PathExpressionAST,
    pub order: Order,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Order {
    Asc,
    Desc,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct LimitAST {
    pub count: ExpressionAST,
    pub offset: ExpressionAST,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct InsertStatementAST {
    pub table_name: String,
    // TODO: support multiple rows
    pub values: Vec<ExpressionAST>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DeleteStatementAST {
    pub table_reference: BaseTableReferenceAST,
    pub condition: Option<ExpressionAST>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct UpdateStatementAST {
    pub table_reference: BaseTableReferenceAST,
    pub assignments: Vec<AssignmentAST>,
    pub condition: Option<ExpressionAST>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AssignmentAST {
    pub target: PathExpressionAST,
    pub value: ExpressionAST,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ExpressionAST {
    Path(PathExpressionAST),
    Literal(LiteralExpressionAST),
    Unary(UnaryExpressionAST),
    Binary(BinaryExpressionAST),
    FunctionCall(FunctionCallExpressionAST),
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PathExpressionAST {
    pub path: Vec<String>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct LiteralExpressionAST {
    pub value: Value,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct UnaryExpressionAST {
    pub operator: UnaryOperator,
    pub operand: Box<ExpressionAST>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum UnaryOperator {
    Negate,
    Not,
    IsNull,
    IsNotNull,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BinaryExpressionAST {
    pub operator: BinaryOperator,
    pub left: Box<ExpressionAST>,
    pub right: Box<ExpressionAST>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BinaryOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    And,
    Or,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct FunctionCallExpressionAST {
    pub function_name: String,
    pub arguments: Vec<ExpressionAST>,
}
pub const AGGREGATE_FUNCTION_NAMES: [&str; 5] = ["COUNT", "SUM", "AVG", "MIN", "MAX"];

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Parser {
    pub tokens: Vec<Token>,
    pub position: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Parser {
        Parser {
            tokens,
            position: 0,
        }
    }
    pub fn parse(&mut self) -> Result<StatementAST> {
        self.statement()
    }
    fn statement(&mut self) -> Result<StatementAST> {
        if self.match_token(Token::Keyword(Keyword::Create))
            && self.match_look_ahead(Token::Keyword(Keyword::Table))
        {
            return Ok(StatementAST::CreateTable(self.create_table_statement()?));
        }
        if self.match_token(Token::Keyword(Keyword::Select)) {
            return Ok(StatementAST::Select(self.select_statement()?));
        }
        if self.match_token(Token::Keyword(Keyword::Insert)) {
            return Ok(StatementAST::Insert(self.insert_statement()?));
        }
        if self.match_token(Token::Keyword(Keyword::Delete)) {
            return Ok(StatementAST::Delete(self.delete_statement()?));
        }
        if self.match_token(Token::Keyword(Keyword::Update)) {
            return Ok(StatementAST::Update(self.update_statement()?));
        }
        if self.match_token(Token::Keyword(Keyword::Begin)) {
            self.consume_token(Token::Keyword(Keyword::Begin));
            return Ok(StatementAST::Begin);
        }
        if self.match_token(Token::Keyword(Keyword::Commit)) {
            self.consume_token(Token::Keyword(Keyword::Commit));
            return Ok(StatementAST::Commit);
        }
        if self.match_token(Token::Keyword(Keyword::Rollback)) {
            self.consume_token(Token::Keyword(Keyword::Rollback));
            return Ok(StatementAST::Rollback);
        }
        Err(anyhow!("invalid statement"))
    }
    fn create_table_statement(&mut self) -> Result<CreateTableStatementAST> {
        self.consume_token_or_error(Token::Keyword(Keyword::Create))?;
        self.consume_token_or_error(Token::Keyword(Keyword::Table))?;
        let table_name = self.identifier()?;
        self.consume_token_or_error(Token::LeftParen)?;
        let mut elements = Vec::new();
        loop {
            elements.push(self.table_element()?);
            if !self.consume_token(Token::Comma) {
                break;
            }
        }
        self.consume_token_or_error(Token::RightParen)?;
        Ok(CreateTableStatementAST {
            table_name,
            elements,
        })
    }
    fn table_element(&mut self) -> Result<TableElementAST> {
        let column_name = self.identifier()?;
        let data_type = self.data_type()?;
        Ok(TableElementAST {
            column_name,
            data_type,
        })
    }
    fn data_type(&mut self) -> Result<DataType> {
        match self.tokens[self.position] {
            Token::Keyword(Keyword::Int) => {
                self.consume_token(Token::Keyword(Keyword::Int));
                Ok(DataType::Integer)
            }
            Token::Keyword(Keyword::Integer) => {
                self.consume_token(Token::Keyword(Keyword::Integer));
                Ok(DataType::Integer)
            }
            Token::Keyword(Keyword::Varchar) => {
                self.consume_token(Token::Keyword(Keyword::Varchar));
                Ok(DataType::Varchar)
            }
            Token::Keyword(Keyword::Boolean) => {
                self.consume_token(Token::Keyword(Keyword::Boolean));
                Ok(DataType::Boolean)
            }
            _ => Err(anyhow!("invalid data type")),
        }
    }
    fn select_statement(&mut self) -> Result<SelectStatementAST> {
        self.consume_token_or_error(Token::Keyword(Keyword::Select))?;
        let select_elements = if self.consume_token(Token::Asterisk) {
            Vec::new()
        } else {
            let mut res = Vec::new();
            loop {
                res.push(self.select_element()?);
                if !self.consume_token(Token::Comma) {
                    break;
                }
            }
            res
        };
        self.consume_token_or_error(Token::Keyword(Keyword::From))?;
        let table_reference = self.table_reference()?;
        let condition = if self.consume_token(Token::Keyword(Keyword::Where)) {
            Some(self.expression()?)
        } else {
            None
        };
        let group_by = if self.consume_token(Token::Keyword(Keyword::Group)) {
            self.consume_token_or_error(Token::Keyword(Keyword::By))?;
            let mut group_by = Vec::new();
            loop {
                group_by.push(self.expression()?);
                if !self.consume_token(Token::Comma) {
                    break;
                }
            }
            Some(group_by)
        } else {
            None
        };
        let having = if self.consume_token(Token::Keyword(Keyword::Having)) {
            Some(self.expression()?)
        } else {
            None
        };
        let order_by = if self.consume_token(Token::Keyword(Keyword::Order)) {
            self.consume_token_or_error(Token::Keyword(Keyword::By))?;
            let mut elements = Vec::new();
            loop {
                elements.push(self.order_by_element()?);
                if !self.consume_token(Token::Comma) {
                    break;
                }
            }
            Some(elements)
        } else {
            None
        };
        let limit = if self.consume_token(Token::Keyword(Keyword::Limit)) {
            let count = self.expression()?;
            let offset = if self.consume_token(Token::Keyword(Keyword::Offset)) {
                self.expression()?
            } else {
                ExpressionAST::Literal(LiteralExpressionAST {
                    value: Value::Integer(IntegerValue(0)),
                })
            };
            Some(LimitAST { count, offset })
        } else {
            None
        };
        Ok(SelectStatementAST {
            select_elements,
            table_reference,
            condition,
            group_by,
            having,
            order_by,
            limit,
        })
    }
    fn select_element(&mut self) -> Result<SelectElementAST> {
        let expression = self.expression()?;
        let alias = if self.consume_token(Token::Keyword(Keyword::As)) {
            Some(self.identifier()?)
        } else {
            None
        };
        Ok(SelectElementAST { expression, alias })
    }
    fn table_reference(&mut self) -> Result<TableReferenceAST> {
        let left = if self.match_token(Token::LeftParen) {
            TableReferenceAST::Subquery(self.subquery_table_reference()?)
        } else {
            TableReferenceAST::Base(self.base_table_reference()?)
        };
        Ok(self.recursive_visit_table_reference(left)?)
    }
    fn base_table_reference(&mut self) -> Result<BaseTableReferenceAST> {
        let table_name = self.identifier()?;
        let alias = if self.consume_token(Token::Keyword(Keyword::As)) {
            Some(self.identifier()?)
        } else {
            None
        };
        Ok(BaseTableReferenceAST { table_name, alias })
    }
    fn subquery_table_reference(&mut self) -> Result<SubqueryTableReferenceAST> {
        self.consume_token_or_error(Token::LeftParen)?;
        let select_statement = Box::new(self.select_statement()?);
        self.consume_token_or_error(Token::RightParen)?;
        self.consume_token_or_error(Token::Keyword(Keyword::As))?;
        let alias = self.identifier()?;
        Ok(SubqueryTableReferenceAST {
            select_statement,
            alias,
        })
    }
    fn recursive_visit_table_reference(
        &mut self,
        left: TableReferenceAST,
    ) -> Result<TableReferenceAST> {
        if let Ok(join_type) = self.join_type() {
            let right = if self.match_token(Token::LeftParen) {
                TableReferenceAST::Subquery(self.subquery_table_reference()?)
            } else {
                TableReferenceAST::Base(self.base_table_reference()?)
            };
            let condition = if self.consume_token(Token::Keyword(Keyword::On)) {
                Some(self.expression()?)
            } else {
                None
            };
            Ok(TableReferenceAST::Join(JoinTableReferenceAST {
                left: Box::new(left),
                right: Box::new(self.recursive_visit_table_reference(right)?),
                condition,
                join_type,
            }))
        } else {
            Ok(left)
        }
    }
    fn join_type(&mut self) -> Result<JoinType> {
        if self.consume_token(Token::Keyword(Keyword::Join)) {
            Ok(JoinType::Inner)
        } else if self.consume_token(Token::Keyword(Keyword::Inner)) {
            self.consume_token_or_error(Token::Keyword(Keyword::Join))?;
            Ok(JoinType::Inner)
        } else if self.consume_token(Token::Keyword(Keyword::Left)) {
            self.consume_token_or_error(Token::Keyword(Keyword::Join))?;
            Ok(JoinType::Left)
        } else {
            Err(anyhow!("invalid join type"))
        }
    }
    fn order_by_element(&mut self) -> Result<OrderByElementAST> {
        let expression = self.path_expression()?;
        let order = if self.consume_token(Token::Keyword(Keyword::Asc)) {
            Order::Asc
        } else if self.consume_token(Token::Keyword(Keyword::Desc)) {
            Order::Desc
        } else {
            return Err(anyhow!("invalid order"));
        };
        Ok(OrderByElementAST { expression, order })
    }
    fn insert_statement(&mut self) -> Result<InsertStatementAST> {
        self.consume_token_or_error(Token::Keyword(Keyword::Insert))?;
        self.consume_token_or_error(Token::Keyword(Keyword::Into))?;
        let table_name = self.identifier()?;
        self.consume_token_or_error(Token::Keyword(Keyword::Values))?;
        self.consume_token_or_error(Token::LeftParen)?;
        let mut values = Vec::new();
        loop {
            values.push(self.expression()?);
            if !self.consume_token(Token::Comma) {
                break;
            }
        }
        self.consume_token_or_error(Token::RightParen)?;
        Ok(InsertStatementAST { table_name, values })
    }
    fn delete_statement(&mut self) -> Result<DeleteStatementAST> {
        self.consume_token_or_error(Token::Keyword(Keyword::Delete))?;
        self.consume_token_or_error(Token::Keyword(Keyword::From))?;
        let table_reference = self.base_table_reference()?;
        let condition = if self.consume_token(Token::Keyword(Keyword::Where)) {
            Some(self.expression()?)
        } else {
            None
        };
        Ok(DeleteStatementAST {
            table_reference,
            condition,
        })
    }
    fn update_statement(&mut self) -> Result<UpdateStatementAST> {
        self.consume_token_or_error(Token::Keyword(Keyword::Update))?;
        let table_reference = self.base_table_reference()?;
        self.consume_token_or_error(Token::Keyword(Keyword::Set))?;
        let mut assignments = Vec::new();
        loop {
            assignments.push(self.assignment()?);
            if !self.consume_token(Token::Comma) {
                break;
            }
        }
        let condition = if self.consume_token(Token::Keyword(Keyword::Where)) {
            Some(self.expression()?)
        } else {
            None
        };
        Ok(UpdateStatementAST {
            table_reference,
            assignments,
            condition,
        })
    }
    fn assignment(&mut self) -> Result<AssignmentAST> {
        let target = self.path_expression()?;
        self.consume_token_or_error(Token::Equal)?;
        let value = self.expression()?;
        Ok(AssignmentAST { target, value })
    }

    fn expression(&mut self) -> Result<ExpressionAST> {
        self.logical_or_expression()
    }
    fn logical_or_expression(&mut self) -> Result<ExpressionAST> {
        let left = self.logical_and_expression()?;
        if self.consume_token(Token::Keyword(Keyword::Or)) {
            let right = self.logical_or_expression()?;
            return Ok(ExpressionAST::Binary(BinaryExpressionAST {
                operator: BinaryOperator::Or,
                left: Box::new(left),
                right: Box::new(right),
            }));
        }
        Ok(left)
    }
    fn logical_and_expression(&mut self) -> Result<ExpressionAST> {
        let left = self.logical_not_expression()?;
        if self.consume_token(Token::Keyword(Keyword::And)) {
            let right = self.logical_and_expression()?;
            return Ok(ExpressionAST::Binary(BinaryExpressionAST {
                operator: BinaryOperator::And,
                left: Box::new(left),
                right: Box::new(right),
            }));
        }
        Ok(left)
    }
    fn logical_not_expression(&mut self) -> Result<ExpressionAST> {
        if self.consume_token(Token::Keyword(Keyword::Not)) {
            let operand = self.comparison_expression()?;
            return Ok(ExpressionAST::Unary(UnaryExpressionAST {
                operator: UnaryOperator::Not,
                operand: Box::new(operand),
            }));
        }
        self.comparison_expression()
    }
    fn comparison_expression(&mut self) -> Result<ExpressionAST> {
        let left = self.function_call_expression();
        let operator = if self.consume_token(Token::Equal) {
            BinaryOperator::Equal
        } else if self.consume_token(Token::NotEqual) {
            BinaryOperator::NotEqual
        } else if self.consume_token(Token::LessThan) {
            BinaryOperator::LessThan
        } else if self.consume_token(Token::LessThanOrEqual) {
            BinaryOperator::LessThanOrEqual
        } else if self.consume_token(Token::GreaterThan) {
            BinaryOperator::GreaterThan
        } else if self.consume_token(Token::GreaterThanOrEqual) {
            BinaryOperator::GreaterThanOrEqual
        } else if self.consume_token(Token::Keyword(Keyword::Is)) {
            if self.consume_token(Token::Keyword(Keyword::Not))
                && self.consume_token(Token::Literal(Value::Null))
            {
                return Ok(ExpressionAST::Unary(UnaryExpressionAST {
                    operator: UnaryOperator::IsNotNull,
                    operand: Box::new(left?),
                }));
            } else if self.consume_token(Token::Literal(Value::Null)) {
                return Ok(ExpressionAST::Unary(UnaryExpressionAST {
                    operator: UnaryOperator::IsNull,
                    operand: Box::new(left?),
                }));
            } else {
                return Err(anyhow!("invalid expression"));
            }
        } else {
            return Ok(left?);
        };
        let right = self.function_call_expression()?;
        Ok(ExpressionAST::Binary(BinaryExpressionAST {
            operator,
            left: Box::new(left?),
            right: Box::new(right),
        }))
    }
    fn function_call_expression(&mut self) -> Result<ExpressionAST> {
        if self.match_identifier() && self.match_look_ahead(Token::LeftParen) {
            let function_name = self.identifier()?;
            self.consume_token_or_error(Token::LeftParen)?;
            let mut arguments = Vec::new();
            loop {
                arguments.push(self.expression()?);
                if !self.consume_token(Token::Comma) {
                    break;
                }
            }
            self.consume_token_or_error(Token::RightParen)?;
            return Ok(ExpressionAST::FunctionCall(FunctionCallExpressionAST {
                function_name: function_name.to_uppercase(),
                arguments,
            }));
        }
        self.arithmetic_expression()
    }
    fn arithmetic_expression(&mut self) -> Result<ExpressionAST> {
        let left = self.term_expression()?;
        let operator = if self.consume_token(Token::Plus) {
            BinaryOperator::Add
        } else if self.consume_token(Token::Minus) {
            BinaryOperator::Subtract
        } else {
            return Ok(left);
        };
        let right = self.term_expression()?;
        Ok(ExpressionAST::Binary(BinaryExpressionAST {
            operator,
            left: Box::new(left),
            right: Box::new(right),
        }))
    }
    fn term_expression(&mut self) -> Result<ExpressionAST> {
        let left = self.factor_expression()?;
        let operator = if self.consume_token(Token::Asterisk) {
            BinaryOperator::Multiply
        } else if self.consume_token(Token::Slash) {
            BinaryOperator::Divide
        } else if self.consume_token(Token::Percent) {
            BinaryOperator::Modulo
        } else {
            return Ok(left);
        };
        let right = self.factor_expression()?;
        Ok(ExpressionAST::Binary(BinaryExpressionAST {
            operator,
            left: Box::new(left),
            right: Box::new(right),
        }))
    }
    fn factor_expression(&mut self) -> Result<ExpressionAST> {
        match self.tokens[self.position] {
            Token::Literal(_) => {
                let v = self.literal()?;
                Ok(ExpressionAST::Literal(LiteralExpressionAST { value: v }))
            }
            Token::Identifier(_) => {
                let path = self.path_expression()?;
                Ok(ExpressionAST::Path(path))
            }
            Token::Minus => {
                self.consume_token_or_error(Token::Minus)?;
                let operand = self.factor_expression()?;
                Ok(ExpressionAST::Unary(UnaryExpressionAST {
                    operator: UnaryOperator::Negate,
                    operand: Box::new(operand),
                }))
            }
            Token::LeftParen => {
                self.consume_token_or_error(Token::LeftParen)?;
                let expression = self.expression()?;
                self.consume_token_or_error(Token::RightParen)?;
                Ok(expression)
            }
            _ => Err(anyhow!("invalid expression")),
        }
    }
    fn path_expression(&mut self) -> Result<PathExpressionAST> {
        let mut path = Vec::new();
        path.push(self.identifier()?);
        while self.consume_token(Token::Dot) {
            path.push(self.identifier()?);
        }
        Ok(PathExpressionAST { path })
    }

    fn identifier(&mut self) -> Result<String> {
        if let Token::Identifier(identifier) = &self.tokens[self.position] {
            self.position += 1;
            Ok(identifier.clone())
        } else {
            Err(anyhow!("invalid identifier"))
        }
    }
    fn literal(&mut self) -> Result<Value> {
        if let Token::Literal(value) = &self.tokens[self.position] {
            self.position += 1;
            Ok(value.clone())
        } else {
            Err(anyhow!("invalid literal"))
        }
    }

    fn match_token(&mut self, token: Token) -> bool {
        self.tokens[self.position] == token
    }
    fn match_look_ahead(&mut self, token: Token) -> bool {
        self.tokens[self.position + 1] == token
    }
    fn match_identifier(&mut self) -> bool {
        match self.tokens[self.position] {
            Token::Identifier(_) => true,
            _ => false,
        }
    }
    fn consume_token(&mut self, token: Token) -> bool {
        if self.match_token(token) {
            self.position += 1;
            true
        } else {
            false
        }
    }
    fn consume_token_or_error(&mut self, token: Token) -> Result<()> {
        if self.consume_token(token) {
            Ok(())
        } else {
            Err(anyhow!("invalid token"))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        lexer::tokenize,
        value::{boolean::BooleanValue, varchar::VarcharValue},
    };

    use anyhow::Result;

    use super::*;

    #[test]
    fn test_parse_create_table() -> Result<()> {
        let sql = r#"
            CREATE TABLE users (
                c0 INT,
                c1 INTEGER,
                c2 VARCHAR,
                c3 BOOLEAN
            );
        "#;
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);

        let statement = parser.parse()?;
        assert_eq!(
            statement,
            StatementAST::CreateTable(CreateTableStatementAST {
                table_name: String::from("users"),
                elements: vec![
                    TableElementAST {
                        column_name: String::from("c0"),
                        data_type: DataType::Integer,
                    },
                    TableElementAST {
                        column_name: String::from("c1"),
                        data_type: DataType::Integer,
                    },
                    TableElementAST {
                        column_name: String::from("c2"),
                        data_type: DataType::Varchar,
                    },
                    TableElementAST {
                        column_name: String::from("c3"),
                        data_type: DataType::Boolean,
                    },
                ],
            })
        );
        Ok(())
    }

    #[test]
    fn test_parse_select() -> Result<()> {
        let sql = r#"
            SELECT
              id AS user_id,
              u.name,
              'hoge'
            FROM users AS u
            WHERE id = 1
            ORDER BY id DESC, name ASC
            LIMIT 10 OFFSET 5;
        "#;
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);

        let statement = parser.parse()?;
        assert_eq!(
            statement,
            StatementAST::Select(SelectStatementAST {
                select_elements: vec![
                    SelectElementAST {
                        expression: ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("id")],
                        }),
                        alias: Some(String::from("user_id")),
                    },
                    SelectElementAST {
                        expression: ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("u"), String::from("name")],
                        }),
                        alias: None,
                    },
                    SelectElementAST {
                        expression: ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Varchar(VarcharValue(String::from("hoge"))),
                        }),
                        alias: None,
                    },
                ],
                table_reference: TableReferenceAST::Base(BaseTableReferenceAST {
                    table_name: String::from("users"),
                    alias: Some(String::from("u")),
                }),
                condition: Some(ExpressionAST::Binary(BinaryExpressionAST {
                    operator: BinaryOperator::Equal,
                    left: Box::new(ExpressionAST::Path(PathExpressionAST {
                        path: vec![String::from("id")],
                    })),
                    right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                        value: Value::Integer(IntegerValue(1)),
                    })),
                })),
                group_by: None,
                having: None,
                order_by: Some(vec![
                    OrderByElementAST {
                        expression: PathExpressionAST {
                            path: vec![String::from("id")],
                        },
                        order: Order::Desc,
                    },
                    OrderByElementAST {
                        expression: PathExpressionAST {
                            path: vec![String::from("name")],
                        },
                        order: Order::Asc,
                    },
                ]),
                limit: Some(LimitAST {
                    count: ExpressionAST::Literal(LiteralExpressionAST {
                        value: Value::Integer(IntegerValue(10)),
                    }),
                    offset: ExpressionAST::Literal(LiteralExpressionAST {
                        value: Value::Integer(IntegerValue(5)),
                    }),
                }),
            })
        );
        Ok(())
    }

    #[test]
    fn test_parse_select_asterisk() -> Result<()> {
        let sql = "SELECT * FROM users";
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);

        let statement = parser.parse()?;
        assert_eq!(
            statement,
            StatementAST::Select(SelectStatementAST {
                select_elements: vec![],
                table_reference: TableReferenceAST::Base(BaseTableReferenceAST {
                    table_name: String::from("users"),
                    alias: None,
                }),
                condition: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
            })
        );
        Ok(())
    }

    #[test]
    fn test_parse_select_group_by() -> Result<()> {
        let sql = "SELECT id FROM users GROUP BY id, name HAVING id = 1";
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);

        let statement = parser.parse()?;
        assert_eq!(
            statement,
            StatementAST::Select(SelectStatementAST {
                select_elements: vec![SelectElementAST {
                    expression: ExpressionAST::Path(PathExpressionAST {
                        path: vec![String::from("id")],
                    }),
                    alias: None,
                }],
                table_reference: TableReferenceAST::Base(BaseTableReferenceAST {
                    table_name: String::from("users"),
                    alias: None,
                }),
                condition: None,
                group_by: Some(vec![
                    ExpressionAST::Path(PathExpressionAST {
                        path: vec![String::from("id")],
                    }),
                    ExpressionAST::Path(PathExpressionAST {
                        path: vec![String::from("name")],
                    }),
                ]),
                having: Some(ExpressionAST::Binary(BinaryExpressionAST {
                    operator: BinaryOperator::Equal,
                    left: Box::new(ExpressionAST::Path(PathExpressionAST {
                        path: vec![String::from("id")],
                    })),
                    right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                        value: Value::Integer(IntegerValue(1)),
                    })),
                })),
                order_by: None,
                limit: None,
            })
        );
        Ok(())
    }

    #[test]
    fn test_parse_insert() -> Result<()> {
        let sql = "INSERT INTO users VALUES (1, 'foo', true)";
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);

        let statement = parser.parse()?;
        assert_eq!(
            statement,
            StatementAST::Insert(InsertStatementAST {
                table_name: String::from("users"),
                values: vec![
                    ExpressionAST::Literal(LiteralExpressionAST {
                        value: Value::Integer(IntegerValue(1)),
                    }),
                    ExpressionAST::Literal(LiteralExpressionAST {
                        value: Value::Varchar(VarcharValue(String::from("foo"))),
                    }),
                    ExpressionAST::Literal(LiteralExpressionAST {
                        value: Value::Boolean(BooleanValue(true)),
                    }),
                ],
            })
        );
        Ok(())
    }

    #[test]
    fn test_parse_delete() -> Result<()> {
        let sql = "DELETE FROM users WHERE id = 1";
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);

        let statement = parser.parse()?;
        assert_eq!(
            statement,
            StatementAST::Delete(DeleteStatementAST {
                table_reference: BaseTableReferenceAST {
                    table_name: String::from("users"),
                    alias: None,
                },
                condition: Some(ExpressionAST::Binary(BinaryExpressionAST {
                    operator: BinaryOperator::Equal,
                    left: Box::new(ExpressionAST::Path(PathExpressionAST {
                        path: vec![String::from("id")],
                    })),
                    right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                        value: Value::Integer(IntegerValue(1)),
                    })),
                })),
            })
        );
        Ok(())
    }

    #[test]
    fn test_parse_update() -> Result<()> {
        let sql = "UPDATE users SET name = 'foo', users.age = 30 WHERE id = 1";
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);

        let statement = parser.parse()?;
        assert_eq!(
            statement,
            StatementAST::Update(UpdateStatementAST {
                table_reference: BaseTableReferenceAST {
                    table_name: String::from("users"),
                    alias: None,
                },
                assignments: vec![
                    AssignmentAST {
                        target: PathExpressionAST {
                            path: vec![String::from("name")],
                        },
                        value: ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Varchar(VarcharValue(String::from("foo"))),
                        }),
                    },
                    AssignmentAST {
                        target: PathExpressionAST {
                            path: vec![String::from("users"), String::from("age")],
                        },
                        value: ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Integer(IntegerValue(30)),
                        }),
                    }
                ],
                condition: Some(ExpressionAST::Binary(BinaryExpressionAST {
                    operator: BinaryOperator::Equal,
                    left: Box::new(ExpressionAST::Path(PathExpressionAST {
                        path: vec![String::from("id")],
                    })),
                    right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                        value: Value::Integer(IntegerValue(1)),
                    })),
                })),
            })
        );
        Ok(())
    }

    #[test]
    fn test_parse_nested_join() -> Result<()> {
        let sql = r#"
            SELECT *
            FROM t1
            INNER JOIN t2 ON t1.id = t2.t1_id
            JOIN t3 ON t2.id = t3.t2_id
            LEFT JOIN t4 ON t3.id = t4.t3_id;
        "#;
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);

        let statement = parser.parse()?;
        assert_eq!(
            statement,
            StatementAST::Select(SelectStatementAST {
                select_elements: vec![],
                table_reference: TableReferenceAST::Join(JoinTableReferenceAST {
                    left: Box::new(TableReferenceAST::Base(BaseTableReferenceAST {
                        table_name: String::from("t1"),
                        alias: None,
                    })),
                    right: Box::new(TableReferenceAST::Join(JoinTableReferenceAST {
                        left: Box::new(TableReferenceAST::Base(BaseTableReferenceAST {
                            table_name: String::from("t2"),
                            alias: None,
                        })),
                        right: Box::new(TableReferenceAST::Join(JoinTableReferenceAST {
                            left: Box::new(TableReferenceAST::Base(BaseTableReferenceAST {
                                table_name: String::from("t3"),
                                alias: None,
                            })),
                            right: Box::new(TableReferenceAST::Base(BaseTableReferenceAST {
                                table_name: String::from("t4"),
                                alias: None,
                            })),
                            condition: Some(ExpressionAST::Binary(BinaryExpressionAST {
                                operator: BinaryOperator::Equal,
                                left: Box::new(ExpressionAST::Path(PathExpressionAST {
                                    path: vec![String::from("t3"), String::from("id")],
                                })),
                                right: Box::new(ExpressionAST::Path(PathExpressionAST {
                                    path: vec![String::from("t4"), String::from("t3_id")],
                                })),
                            })),
                            join_type: JoinType::Left,
                        })),
                        condition: Some(ExpressionAST::Binary(BinaryExpressionAST {
                            operator: BinaryOperator::Equal,
                            left: Box::new(ExpressionAST::Path(PathExpressionAST {
                                path: vec![String::from("t2"), String::from("id")],
                            })),
                            right: Box::new(ExpressionAST::Path(PathExpressionAST {
                                path: vec![String::from("t3"), String::from("t2_id")],
                            })),
                        })),
                        join_type: JoinType::Inner,
                    })),
                    condition: Some(ExpressionAST::Binary(BinaryExpressionAST {
                        operator: BinaryOperator::Equal,
                        left: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("t1"), String::from("id")],
                        })),
                        right: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("t2"), String::from("t1_id")],
                        })),
                    })),
                    join_type: JoinType::Inner,
                }),
                condition: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
            })
        );
        Ok(())
    }

    #[test]
    fn test_parser_subquery() -> Result<()> {
        let sql = r#"
            SELECT *
            FROM (
              SELECT *
              FROM accounts
            ) AS a
            INNER JOIN (
              SELECT *
              FROM users
            ) AS u ON a.id = u.account_id;
        "#;
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);

        let statement = parser.parse()?;
        assert_eq!(
            statement,
            StatementAST::Select(SelectStatementAST {
                select_elements: vec![],
                table_reference: TableReferenceAST::Join(JoinTableReferenceAST {
                    left: Box::new(TableReferenceAST::Subquery(SubqueryTableReferenceAST {
                        select_statement: Box::new(SelectStatementAST {
                            select_elements: vec![],
                            table_reference: TableReferenceAST::Base(BaseTableReferenceAST {
                                table_name: String::from("accounts"),
                                alias: None,
                            }),
                            condition: None,
                            group_by: None,
                            having: None,
                            order_by: None,
                            limit: None,
                        }),
                        alias: String::from("a"),
                    })),
                    right: Box::new(TableReferenceAST::Subquery(SubqueryTableReferenceAST {
                        select_statement: Box::new(SelectStatementAST {
                            select_elements: vec![],
                            table_reference: TableReferenceAST::Base(BaseTableReferenceAST {
                                table_name: String::from("users"),
                                alias: None,
                            }),
                            condition: None,
                            group_by: None,
                            having: None,
                            order_by: None,
                            limit: None,
                        }),
                        alias: String::from("u"),
                    })),
                    condition: Some(ExpressionAST::Binary(BinaryExpressionAST {
                        operator: BinaryOperator::Equal,
                        left: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("a"), String::from("id")],
                        })),
                        right: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("u"), String::from("account_id")],
                        })),
                    })),
                    join_type: JoinType::Inner,
                }),
                condition: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
            })
        );
        Ok(())
    }

    #[test]
    fn test_parse_expression() -> Result<()> {
        let sql = r#"
            SELECT
              age + 1,
              age - 1,
              age * 1,
              age / 1,
              age % 1,
              age = 1,
              age <> 1,
              age < 1,
              age <= 1,
              age > 1,
              age >= 1,
              is_deleted AND True,
              is_deleted OR True,
              NOT is_deleted,
              -age,
              COUNT(id),
              foo.bar,
              1,
              'foo',
              true,
              NULL,
              (age),
              name IS NULL,
              name IS NOT NULL
            FROM users;
        "#;
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);

        let statement = parser.parse()?;
        match statement {
            StatementAST::Select(select_statement) => {
                assert_eq!(
                    select_statement.select_elements[0].expression,
                    ExpressionAST::Binary(BinaryExpressionAST {
                        operator: BinaryOperator::Add,
                        left: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("age")],
                        })),
                        right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Integer(IntegerValue(1)),
                        })),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[1].expression,
                    ExpressionAST::Binary(BinaryExpressionAST {
                        operator: BinaryOperator::Subtract,
                        left: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("age")],
                        })),
                        right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Integer(IntegerValue(1)),
                        })),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[2].expression,
                    ExpressionAST::Binary(BinaryExpressionAST {
                        operator: BinaryOperator::Multiply,
                        left: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("age")],
                        })),
                        right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Integer(IntegerValue(1)),
                        })),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[3].expression,
                    ExpressionAST::Binary(BinaryExpressionAST {
                        operator: BinaryOperator::Divide,
                        left: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("age")],
                        })),
                        right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Integer(IntegerValue(1)),
                        })),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[4].expression,
                    ExpressionAST::Binary(BinaryExpressionAST {
                        operator: BinaryOperator::Modulo,
                        left: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("age")],
                        })),
                        right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Integer(IntegerValue(1)),
                        })),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[5].expression,
                    ExpressionAST::Binary(BinaryExpressionAST {
                        operator: BinaryOperator::Equal,
                        left: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("age")],
                        })),
                        right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Integer(IntegerValue(1)),
                        })),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[6].expression,
                    ExpressionAST::Binary(BinaryExpressionAST {
                        operator: BinaryOperator::NotEqual,
                        left: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("age")],
                        })),
                        right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Integer(IntegerValue(1)),
                        })),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[7].expression,
                    ExpressionAST::Binary(BinaryExpressionAST {
                        operator: BinaryOperator::LessThan,
                        left: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("age")],
                        })),
                        right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Integer(IntegerValue(1)),
                        })),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[8].expression,
                    ExpressionAST::Binary(BinaryExpressionAST {
                        operator: BinaryOperator::LessThanOrEqual,
                        left: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("age")],
                        })),
                        right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Integer(IntegerValue(1)),
                        })),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[9].expression,
                    ExpressionAST::Binary(BinaryExpressionAST {
                        operator: BinaryOperator::GreaterThan,
                        left: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("age")],
                        })),
                        right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Integer(IntegerValue(1)),
                        })),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[10].expression,
                    ExpressionAST::Binary(BinaryExpressionAST {
                        operator: BinaryOperator::GreaterThanOrEqual,
                        left: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("age")],
                        })),
                        right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Integer(IntegerValue(1)),
                        })),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[11].expression,
                    ExpressionAST::Binary(BinaryExpressionAST {
                        operator: BinaryOperator::And,
                        left: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("is_deleted")],
                        })),
                        right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Boolean(BooleanValue(true)),
                        })),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[12].expression,
                    ExpressionAST::Binary(BinaryExpressionAST {
                        operator: BinaryOperator::Or,
                        left: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("is_deleted")],
                        })),
                        right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Boolean(BooleanValue(true)),
                        })),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[13].expression,
                    ExpressionAST::Unary(UnaryExpressionAST {
                        operator: UnaryOperator::Not,
                        operand: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("is_deleted")],
                        })),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[14].expression,
                    ExpressionAST::Unary(UnaryExpressionAST {
                        operator: UnaryOperator::Negate,
                        operand: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("age")],
                        })),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[15].expression,
                    ExpressionAST::FunctionCall(FunctionCallExpressionAST {
                        function_name: String::from("COUNT"),
                        arguments: vec![ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("id")],
                        })],
                    })
                );
                assert_eq!(
                    select_statement.select_elements[16].expression,
                    ExpressionAST::Path(PathExpressionAST {
                        path: vec![String::from("foo"), String::from("bar")],
                    })
                );
                assert_eq!(
                    select_statement.select_elements[17].expression,
                    ExpressionAST::Literal(LiteralExpressionAST {
                        value: Value::Integer(IntegerValue(1)),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[18].expression,
                    ExpressionAST::Literal(LiteralExpressionAST {
                        value: Value::Varchar(VarcharValue(String::from("foo"))),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[19].expression,
                    ExpressionAST::Literal(LiteralExpressionAST {
                        value: Value::Boolean(BooleanValue(true)),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[20].expression,
                    ExpressionAST::Literal(LiteralExpressionAST { value: Value::Null })
                );
                assert_eq!(
                    select_statement.select_elements[21].expression,
                    ExpressionAST::Path(PathExpressionAST {
                        path: vec![String::from("age")],
                    })
                );
                assert_eq!(
                    select_statement.select_elements[22].expression,
                    ExpressionAST::Unary(UnaryExpressionAST {
                        operator: UnaryOperator::IsNull,
                        operand: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("name")],
                        })),
                    })
                );
                assert_eq!(
                    select_statement.select_elements[23].expression,
                    ExpressionAST::Unary(UnaryExpressionAST {
                        operator: UnaryOperator::IsNotNull,
                        operand: Box::new(ExpressionAST::Path(PathExpressionAST {
                            path: vec![String::from("name")],
                        })),
                    })
                );
            }
            _ => panic!("not select statement"),
        }
        Ok(())
    }
}
