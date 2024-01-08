use anyhow::{anyhow, Result};

use crate::{
    catalog::DataType,
    lexer::{Keyword, Token},
    value::Value,
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum StatementAST {
    CreateTable(CreateTableStatementAST),
    Select(SelectStatementAST),
    Insert(InsertStatementAST),
    Delete(DeleteStatementAST),
    Update(UpdateStatementAST),
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
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SelectElementAST {
    pub expression: ExpressionAST,
    pub alias: Option<String>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum TableReferenceAST {
    Base(BaseTableReferenceAST),
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BaseTableReferenceAST {
    pub table_name: String,
    pub alias: Option<String>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct InsertStatementAST {
    pub table_name: String,
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
    pub column_name: String,
    pub value: ExpressionAST,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ExpressionAST {
    Path(PathExpressionAST),
    Literal(LiteralExpressionAST),
    Binary(BinaryExpressionAST),
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
pub struct BinaryExpressionAST {
    pub operator: BinaryOperator,
    pub left: Box<ExpressionAST>,
    pub right: Box<ExpressionAST>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BinaryOperator {
    Equal,
}

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
            if self.consume_token(Token::Comma) {
                continue;
            }
            break;
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
            Token::Keyword(Keyword::Integer) => {
                self.consume_token(Token::Keyword(Keyword::Integer));
                Ok(DataType::Int)
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
                if self.consume_token(Token::Comma) {
                    continue;
                }
                break;
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
        Ok(SelectStatementAST {
            select_elements,
            table_reference,
            condition,
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
        Ok(TableReferenceAST::Base(self.base_table_reference()?))
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
    fn insert_statement(&mut self) -> Result<InsertStatementAST> {
        self.consume_token_or_error(Token::Keyword(Keyword::Insert))?;
        self.consume_token_or_error(Token::Keyword(Keyword::Into))?;
        let table_name = self.identifier()?;
        self.consume_token_or_error(Token::Keyword(Keyword::Values))?;
        self.consume_token_or_error(Token::LeftParen)?;
        let mut values = Vec::new();
        loop {
            values.push(self.expression()?);
            if self.consume_token(Token::Comma) {
                continue;
            }
            break;
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
            if self.consume_token(Token::Comma) {
                continue;
            }
            break;
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
        let column_name = self.identifier()?;
        self.consume_token_or_error(Token::Equal)?;
        let value = self.expression()?;
        Ok(AssignmentAST { column_name, value })
    }

    fn expression(&mut self) -> Result<ExpressionAST> {
        self.logical_or_expression()
    }
    fn logical_or_expression(&mut self) -> Result<ExpressionAST> {
        // TODO: implement
        self.logical_and_expression()
    }
    fn logical_and_expression(&mut self) -> Result<ExpressionAST> {
        // TODO: implement
        self.logical_not_expression()
    }
    fn logical_not_expression(&mut self) -> Result<ExpressionAST> {
        // TODO: implement
        self.comparison_expression()
    }
    fn comparison_expression(&mut self) -> Result<ExpressionAST> {
        let left = self.function_call_expression();
        if self.consume_token(Token::Equal) {
            let right = self.function_call_expression()?;
            return Ok(ExpressionAST::Binary(BinaryExpressionAST {
                operator: BinaryOperator::Equal,
                left: Box::new(left?),
                right: Box::new(right),
            }));
        }
        left
    }
    fn function_call_expression(&mut self) -> Result<ExpressionAST> {
        // TODO: implement
        self.arithmetic_expression()
    }
    fn arithmetic_expression(&mut self) -> Result<ExpressionAST> {
        // TODO: implement
        self.term_expression()
    }
    fn term_expression(&mut self) -> Result<ExpressionAST> {
        // TODO: implement
        self.factor_expression()
    }
    fn factor_expression(&mut self) -> Result<ExpressionAST> {
        match self.tokens[self.position] {
            Token::Literal(_) => {
                let v = self.literal()?;
                Ok(ExpressionAST::Literal(LiteralExpressionAST { value: v }))
            }
            Token::Identifier(_) => {
                let path = self.path_expression()?;
                Ok(ExpressionAST::Path(PathExpressionAST { path }))
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
    fn path_expression(&mut self) -> Result<Vec<String>> {
        let mut path = Vec::new();
        path.push(self.identifier()?);
        while self.consume_token(Token::Dot) {
            path.push(self.identifier()?);
        }
        Ok(path)
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
        value::{BooleanValue, IntValue, VarcharValue},
    };

    use anyhow::Result;

    use super::*;

    #[test]
    fn test_parse_create_table() -> Result<()> {
        let sql = "CREATE TABLE users (id INTEGER, name VARCHAR, active BOOLEAN)";
        let mut parser = Parser::new(tokenize(&mut sql.chars().peekable())?);

        let statement = parser.parse()?;
        assert_eq!(
            statement,
            StatementAST::CreateTable(CreateTableStatementAST {
                table_name: String::from("users"),
                elements: vec![
                    TableElementAST {
                        column_name: String::from("id"),
                        data_type: DataType::Int,
                    },
                    TableElementAST {
                        column_name: String::from("name"),
                        data_type: DataType::Varchar,
                    },
                    TableElementAST {
                        column_name: String::from("active"),
                        data_type: DataType::Boolean,
                    },
                ],
            })
        );
        Ok(())
    }

    #[test]
    fn test_parse_select() -> Result<()> {
        let sql = "SELECT id AS user_id, u.name FROM users AS u WHERE id = 1";
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
                        value: Value::Int(IntValue(1)),
                    })),
                })),
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
                        value: Value::Int(IntValue(1)),
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
                        value: Value::Int(IntValue(1)),
                    })),
                })),
            })
        );
        Ok(())
    }

    #[test]
    fn test_parse_update() -> Result<()> {
        let sql = "UPDATE users SET name = 'foo', age = 30 WHERE id = 1";
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
                        column_name: String::from("name"),
                        value: ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Varchar(VarcharValue(String::from("foo"))),
                        }),
                    },
                    AssignmentAST {
                        column_name: String::from("age"),
                        value: ExpressionAST::Literal(LiteralExpressionAST {
                            value: Value::Int(IntValue(30)),
                        }),
                    }
                ],
                condition: Some(ExpressionAST::Binary(BinaryExpressionAST {
                    operator: BinaryOperator::Equal,
                    left: Box::new(ExpressionAST::Path(PathExpressionAST {
                        path: vec![String::from("id")],
                    })),
                    right: Box::new(ExpressionAST::Literal(LiteralExpressionAST {
                        value: Value::Int(IntValue(1)),
                    })),
                })),
            })
        );
        Ok(())
    }
}
