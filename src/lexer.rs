use std::iter::Peekable;
use std::str::Chars;

use anyhow::{anyhow, Result};

use crate::value::{
    big_integer::BigIntegerValue, boolean::BooleanValue, integer::IntegerValue,
    unsigned_big_integer::UnsignedBigIntegerValue, unsigned_integer::UnsignedIntegerValue,
    varchar::VarcharValue, Value,
};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Token {
    Identifier(String),
    Keyword(Keyword),
    Literal(Value),
    Asterisk,
    Semicolon,
    Comma,
    Dot,
    LeftParen,
    RightParen,
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Plus,
    Minus,
    Slash,
    Percent,
    EOF,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
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
    Inner,
    Left,
    Join,
    On,
    Group,
    By,
    Having,
    Order,
    Asc,
    Desc,
    Limit,
    Offset,
    Int,
    Integer,
    BigInt,
    BigInteger,
    Unsigned,
    Varchar,
    Boolean,
    Begin,
    Commit,
    Rollback,
    As,
    And,
    Or,
    Not,
    Is,
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
            "INNER" => Ok(Keyword::Inner),
            "LEFT" => Ok(Keyword::Left),
            "JOIN" => Ok(Keyword::Join),
            "ON" => Ok(Keyword::On),
            "GROUP" => Ok(Keyword::Group),
            "BY" => Ok(Keyword::By),
            "HAVING" => Ok(Keyword::Having),
            "ORDER" => Ok(Keyword::Order),
            "ASC" => Ok(Keyword::Asc),
            "DESC" => Ok(Keyword::Desc),
            "LIMIT" => Ok(Keyword::Limit),
            "OFFSET" => Ok(Keyword::Offset),
            "INT" => Ok(Keyword::Int),
            "INTEGER" => Ok(Keyword::Integer),
            "BIGINT" => Ok(Keyword::BigInt),
            "BIGINTEGER" => Ok(Keyword::BigInteger),
            "UNSIGNED" => Ok(Keyword::Unsigned),
            "VARCHAR" => Ok(Keyword::Varchar),
            "BOOLEAN" => Ok(Keyword::Boolean),
            "BEGIN" => Ok(Keyword::Begin),
            "COMMIT" => Ok(Keyword::Commit),
            "ROLLBACK" => Ok(Keyword::Rollback),
            "AS" => Ok(Keyword::As),
            "AND" => Ok(Keyword::And),
            "OR" => Ok(Keyword::Or),
            "NOT" => Ok(Keyword::Not),
            "IS" => Ok(Keyword::Is),
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
                            ret.push(*cc);
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
                        "NULL" => tokens.push(Token::Literal(Value::Null)),
                        _ => tokens.push(Token::Identifier(ret)),
                    }
                }
            }
            Some(c) if *c == '<' => {
                iter.next();
                match iter.peek() {
                    Some(cc) if *cc == '=' => {
                        iter.next();
                        tokens.push(Token::LessThanOrEqual);
                    }
                    Some(cc) if *cc == '>' => {
                        iter.next();
                        tokens.push(Token::NotEqual);
                    }
                    _ => {
                        tokens.push(Token::LessThan);
                    }
                }
            }
            Some(c) if *c == '>' => {
                iter.next();
                match iter.peek() {
                    Some(cc) if *cc == '=' => {
                        iter.next();
                        tokens.push(Token::GreaterThanOrEqual);
                    }
                    _ => {
                        tokens.push(Token::GreaterThan);
                    }
                }
            }
            Some(c) if vec![',', '.', '(', ')', '*', ';', '=', '+', '/', '%'].contains(c) => {
                tokens.push(match *c {
                    ',' => Token::Comma,
                    '.' => Token::Dot,
                    '(' => Token::LeftParen,
                    ')' => Token::RightParen,
                    '*' => Token::Asterisk,
                    ';' => Token::Semicolon,
                    '=' => Token::Equal,
                    '+' => Token::Plus,
                    '/' => Token::Slash,
                    '%' => Token::Percent,
                    _ => unreachable!(),
                });
                iter.next();
            }
            Some(c) if *c == '-' => {
                iter.next();
                if let Some(cc) = iter.peek() {
                    if cc.is_digit(10) {
                        let mut ret = String::new();
                        loop {
                            match iter.peek() {
                                Some(ccc) if ccc.is_digit(10) => {
                                    ret.push(*ccc);
                                    iter.next();
                                }
                                _ => {
                                    break;
                                }
                            }
                        }
                        if let Ok(v) = ret.parse::<i32>() {
                            tokens.push(Token::Literal(Value::Integer(IntegerValue(-v))));
                        } else {
                            if let Ok(v) = ret.parse::<i64>() {
                                tokens.push(Token::Literal(Value::BigInteger(BigIntegerValue(-v))));
                            } else {
                                return Err(anyhow!("failed convert: {}", ret));
                            }
                        }
                    } else {
                        tokens.push(Token::Minus);
                    }
                } else {
                    tokens.push(Token::Minus);
                }
            }
            Some(c) if c.is_digit(10) => {
                let mut ret = String::new();
                loop {
                    match iter.peek() {
                        Some(cc) if cc.is_digit(10) => {
                            ret.push(*cc);
                            iter.next();
                        }
                        _ => {
                            break;
                        }
                    }
                }
                if let Ok(v) = ret.parse::<i32>() {
                    tokens.push(Token::Literal(Value::Integer(IntegerValue(v))));
                } else {
                    if let Ok(v) = ret.parse::<u32>() {
                        tokens.push(Token::Literal(Value::UnsignedInteger(
                            UnsignedIntegerValue(v),
                        )));
                    } else {
                        if let Ok(v) = ret.parse::<i64>() {
                            tokens.push(Token::Literal(Value::BigInteger(BigIntegerValue(v))));
                        } else {
                            if let Ok(v) = ret.parse::<u64>() {
                                tokens.push(Token::Literal(Value::UnsignedBigInteger(
                                    UnsignedBigIntegerValue(v),
                                )));
                            } else {
                                return Err(anyhow!("failed convert: {}", ret));
                            }
                        }
                    }
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
                                    ret.push(*cc);
                                    iter.next();
                                }
                                _ => {
                                    return Err(anyhow!("invalid string literal: {}", ret));
                                }
                            }
                        }
                        Some(c) => {
                            ret.push(*c);
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
                Token::Literal(Value::Integer(IntegerValue(1))),
                Token::Semicolon,
                Token::EOF,
            ]
        );
        Ok(())
    }

    #[test]
    fn test_all_keywords() -> Result<()> {
        let text = r#"
            CREATE table Insert INTO VALUES DELETE FROM WHERE UPDATE SET
            SELECT INNER LEFT JOIN ON GROUP BY HAVING ORDER ASC
            DESC LIMIT OFFSET INT INTEGER BIGINT BIGINTEGER VARCHAR BOOLEAN BEGIN
            COMMIT ROLLBACK AS AND OR NOT IS
        "#;
        let mut iter = text.chars().peekable();
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
                Token::Keyword(Keyword::Inner),
                Token::Keyword(Keyword::Left),
                Token::Keyword(Keyword::Join),
                Token::Keyword(Keyword::On),
                Token::Keyword(Keyword::Group),
                Token::Keyword(Keyword::By),
                Token::Keyword(Keyword::Having),
                Token::Keyword(Keyword::Order),
                Token::Keyword(Keyword::Asc),
                Token::Keyword(Keyword::Desc),
                Token::Keyword(Keyword::Limit),
                Token::Keyword(Keyword::Offset),
                Token::Keyword(Keyword::Int),
                Token::Keyword(Keyword::Integer),
                Token::Keyword(Keyword::BigInt),
                Token::Keyword(Keyword::BigInteger),
                Token::Keyword(Keyword::Varchar),
                Token::Keyword(Keyword::Boolean),
                Token::Keyword(Keyword::Begin),
                Token::Keyword(Keyword::Commit),
                Token::Keyword(Keyword::Rollback),
                Token::Keyword(Keyword::As),
                Token::Keyword(Keyword::And),
                Token::Keyword(Keyword::Or),
                Token::Keyword(Keyword::Not),
                Token::Keyword(Keyword::Is),
                Token::EOF,
            ]
        );
        Ok(())
    }

    #[test]
    fn test_all_literals() -> Result<()> {
        let text = r#"
            1 2345 -1 -2345 -3000000000 3000000000 5000000000 9223372036854775808
            'a' 'b\'c' true False NULL
        "#;
        let mut iter = text.chars().peekable();
        let tokens = tokenize(&mut iter)?;
        assert_eq!(
            tokens,
            vec![
                Token::Literal(Value::Integer(IntegerValue(1))),
                Token::Literal(Value::Integer(IntegerValue(2345))),
                Token::Literal(Value::Integer(IntegerValue(-1))),
                Token::Literal(Value::Integer(IntegerValue(-2345))),
                Token::Literal(Value::BigInteger(BigIntegerValue(-3000000000))),
                Token::Literal(Value::UnsignedInteger(UnsignedIntegerValue(3000000000))),
                Token::Literal(Value::BigInteger(BigIntegerValue(5000000000))),
                Token::Literal(Value::UnsignedBigInteger(UnsignedBigIntegerValue(
                    9223372036854775808
                ))),
                Token::Literal(Value::Varchar(VarcharValue("a".to_string()))),
                Token::Literal(Value::Varchar(VarcharValue("b'c".to_string()))),
                Token::Literal(Value::Boolean(BooleanValue(true))),
                Token::Literal(Value::Boolean(BooleanValue(false))),
                Token::Literal(Value::Null),
                Token::EOF,
            ]
        );
        Ok(())
    }

    #[test]
    fn test_all_symbols() -> Result<()> {
        let text = ", . ( ) * ; - = + / % < > <= >= <>";
        let mut iter = text.chars().peekable();
        let tokens = tokenize(&mut iter)?;
        assert_eq!(
            tokens,
            vec![
                Token::Comma,
                Token::Dot,
                Token::LeftParen,
                Token::RightParen,
                Token::Asterisk,
                Token::Semicolon,
                Token::Minus,
                Token::Equal,
                Token::Plus,
                Token::Slash,
                Token::Percent,
                Token::LessThan,
                Token::GreaterThan,
                Token::LessThanOrEqual,
                Token::GreaterThanOrEqual,
                Token::NotEqual,
                Token::EOF,
            ]
        );
        Ok(())
    }
}
