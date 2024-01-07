use std::iter::Peekable;
use std::str::Chars;

use anyhow::{anyhow, Result};

use crate::value::{BooleanValue, IntValue, Value, VarcharValue};

#[derive(Debug, PartialEq, Eq)]
pub enum Token {
    Identifier(String),
    Keyword(Keyword),
    Literal(Value),
    Asterisk,
    Semicolon,
    Comma,
    LeftParen,
    RightParen,
    Equal,
    EOF,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Keyword {
    Create,
    Table,
    Insert,
    Into,
    Values,
    Delete,
    From,
    Where,
    Update,
    Set,
    Select,
    Integer,
    Varchar,
    Boolean,
    Begin,
    Commit,
    Rollback,
}
impl TryFrom<&str> for Keyword {
    type Error = anyhow::Error;
    fn try_from(s: &str) -> Result<Self> {
        match &*s.to_uppercase() {
            "CREATE" => Ok(Keyword::Create),
            "TABLE" => Ok(Keyword::Table),
            "INSERT" => Ok(Keyword::Insert),
            "INTO" => Ok(Keyword::Into),
            "VALUES" => Ok(Keyword::Values),
            "DELETE" => Ok(Keyword::Delete),
            "FROM" => Ok(Keyword::From),
            "WHERE" => Ok(Keyword::Where),
            "UPDATE" => Ok(Keyword::Update),
            "SET" => Ok(Keyword::Set),
            "SELECT" => Ok(Keyword::Select),
            "INTEGER" => Ok(Keyword::Integer),
            "VARCHAR" => Ok(Keyword::Varchar),
            "BOOLEAN" => Ok(Keyword::Boolean),
            "BEGIN" => Ok(Keyword::Begin),
            "COMMIT" => Ok(Keyword::Commit),
            "ROLLBACK" => Ok(Keyword::Rollback),
            _ => Err(anyhow!("invalid keyword: {}", s)),
        }
    }
}

pub fn tokenize(iter: &mut Peekable<Chars>) -> Result<Vec<Token>> {
    let mut tokens = Vec::new();
    loop {
        match iter.peek() {
            Some(c) if c.is_whitespace() => {
                iter.next();
            }
            Some(c) if '_' == *c || c.is_alphabetic() => {
                let mut ret = String::new();
                loop {
                    match iter.peek() {
                        Some(cc) if '_' == *cc || cc.is_digit(10) || cc.is_alphabetic() => {
                            ret = format!("{}{}", ret, cc.to_string());
                            iter.next();
                        }
                        _ => {
                            break;
                        }
                    }
                }
                if let Some(keyword) = Keyword::try_from(&*ret).ok() {
                    tokens.push(Token::Keyword(keyword));
                } else {
                    match &*ret.to_uppercase() {
                        "TRUE" => tokens.push(Token::Literal(Value::Boolean(BooleanValue(true)))),
                        "FALSE" => tokens.push(Token::Literal(Value::Boolean(BooleanValue(false)))),
                        _ => tokens.push(Token::Identifier(ret)),
                    }
                }
            }
            Some(c) if vec![',', '(', ')', '*', ';', '='].contains(c) => {
                tokens.push(match *c {
                    ',' => Token::Comma,
                    '(' => Token::LeftParen,
                    ')' => Token::RightParen,
                    '*' => Token::Asterisk,
                    ';' => Token::Semicolon,
                    '=' => Token::Equal,
                    _ => unreachable!(),
                });
                iter.next();
            }
            Some(c) if c.is_digit(10) => {
                let mut ret = String::new();
                loop {
                    match iter.peek() {
                        Some(cc) if cc.is_digit(10) => {
                            ret = format!("{}{}", ret, cc.to_string());
                            iter.next();
                        }
                        _ => {
                            break;
                        }
                    }
                }
                if let Ok(v) = ret.parse::<i32>() {
                    tokens.push(Token::Literal(Value::Int(IntValue(v))));
                } else {
                    return Err(anyhow!("failed convert: {}", ret));
                }
            }
            Some('\'') => {
                let mut ret = String::new();
                iter.next();
                loop {
                    match iter.peek() {
                        Some(c) if '\'' == *c => {
                            iter.next();
                            break;
                        }
                        Some(c) if '\\' == *c => {
                            iter.next();
                            match iter.peek() {
                                Some(cc) if '\'' == *cc => {
                                    ret = format!("{}{}", ret, cc.to_string());
                                    iter.next();
                                }
                                _ => {
                                    return Err(anyhow!("invalid string literal: {}", ret));
                                }
                            }
                        }
                        Some(c) => {
                            ret = format!("{}{}", ret, c.to_string());
                            iter.next();
                        }
                        None => {
                            return Err(anyhow!("invalid string literal: {}", ret));
                        }
                    }
                }
                tokens.push(Token::Literal(Value::Varchar(VarcharValue(ret))));
            }
            Some(c) => return Err(anyhow!("invalid token: {}", c)),
            None => {
                tokens.push(Token::EOF);
                break;
            }
        }
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_select_query() -> Result<()> {
        let query = "SELECT * FROM users WHERE id = 1;";
        let mut iter = query.chars().peekable();
        let tokens = tokenize(&mut iter)?;
        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Select),
                Token::Asterisk,
                Token::Keyword(Keyword::From),
                Token::Identifier("users".to_string()),
                Token::Keyword(Keyword::Where),
                Token::Identifier("id".to_string()),
                Token::Equal,
                Token::Literal(Value::Int(IntValue(1))),
                Token::Semicolon,
                Token::EOF,
            ]
        );
        Ok(())
    }

    #[test]
    fn test_all_keywords() -> Result<()> {
        let query = "CREATE table Insert INTO VALUES DELETE FROM WHERE UPDATE SET SELECT INTEGER VARCHAR BOOLEAN BEGIN COMMIT ROLLBACK";
        let mut iter = query.chars().peekable();
        let tokens = tokenize(&mut iter)?;
        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Create),
                Token::Keyword(Keyword::Table),
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Keyword(Keyword::Values),
                Token::Keyword(Keyword::Delete),
                Token::Keyword(Keyword::From),
                Token::Keyword(Keyword::Where),
                Token::Keyword(Keyword::Update),
                Token::Keyword(Keyword::Set),
                Token::Keyword(Keyword::Select),
                Token::Keyword(Keyword::Integer),
                Token::Keyword(Keyword::Varchar),
                Token::Keyword(Keyword::Boolean),
                Token::Keyword(Keyword::Begin),
                Token::Keyword(Keyword::Commit),
                Token::Keyword(Keyword::Rollback),
                Token::EOF,
            ]
        );
        Ok(())
    }

    #[test]
    fn test_all_literals() -> Result<()> {
        let query = "1 2345 'a' 'b\\'c' true False";
        let mut iter = query.chars().peekable();
        let tokens = tokenize(&mut iter)?;
        assert_eq!(
            tokens,
            vec![
                Token::Literal(Value::Int(IntValue(1))),
                Token::Literal(Value::Int(IntValue(2345))),
                Token::Literal(Value::Varchar(VarcharValue("a".to_string()))),
                Token::Literal(Value::Varchar(VarcharValue("b'c".to_string()))),
                Token::Literal(Value::Boolean(BooleanValue(true))),
                Token::Literal(Value::Boolean(BooleanValue(false))),
                Token::EOF,
            ]
        );
        Ok(())
    }

    #[test]
    fn test_all_symbols() -> Result<()> {
        let query = ", ( ) * ; =";
        let mut iter = query.chars().peekable();
        let tokens = tokenize(&mut iter)?;
        assert_eq!(
            tokens,
            vec![
                Token::Comma,
                Token::LeftParen,
                Token::RightParen,
                Token::Asterisk,
                Token::Semicolon,
                Token::Equal,
                Token::EOF,
            ]
        );
        Ok(())
    }
}
