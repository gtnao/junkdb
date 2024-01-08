use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, RwLock},
    thread,
};

use anyhow::{anyhow, Result};
use prettytable::{Cell, Row, Table};

use crate::{
    instance::Instance,
    lexer::tokenize,
    parser::{Parser, StatementAST},
    value::Value,
};

pub fn server_start(init: bool) -> Result<()> {
    println!("toydb server started");

    // init
    let instance = Arc::new(RwLock::new(Instance::new("data", init)?));

    // listen
    let listener = TcpListener::bind("127.0.0.1:7878")?;
    for stream in listener.incoming() {
        let stream = stream?;
        let instance = instance.clone();
        thread::spawn(|| -> Result<()> { handle_connection(stream, instance) });
    }
    Ok(())
}

fn handle_connection(mut stream: TcpStream, instance: Arc<RwLock<Instance>>) -> Result<()> {
    // read request
    let mut size_buffer = [0u8; 4];
    stream.read_exact(&mut size_buffer)?;
    let mut buffer = vec![0u8; u32::from_be_bytes(size_buffer) as usize];
    stream.read_exact(&mut buffer)?;
    let request = String::from_utf8(buffer)?;

    // parse
    let mut iter = request.chars().peekable();
    let tokens = tokenize(&mut iter)?;
    let mut parser = Parser::new(tokens);
    let statement = parser.parse()?;

    let txn_id = instance
        .read()
        .map_err(|_| anyhow!("lock error"))?
        .begin(None)?;
    let response = match statement {
        StatementAST::CreateTable(ast) => {
            instance
                .write()
                .map_err(|_| anyhow!("lock error"))?
                .create_table(&ast, txn_id)?;
            format!("table {} created", ast.table_name)
        }
        _ => {
            let (rows, schema) = instance
                .write()
                .map_err(|_| anyhow!("lock error"))?
                .execute(&statement, txn_id)?;
            // TODO: move to client
            let mut table_view = Table::new();
            let mut header = vec![];
            for column in schema.columns {
                header.push(Cell::new(&column.name));
            }
            table_view.set_titles(Row::new(header));
            for row in rows {
                let cells = row
                    .iter()
                    .map(|v| match v {
                        Value::Int(v) => Cell::new(&v.0.to_string()),
                        Value::Varchar(v) => Cell::new(&v.0),
                        Value::Boolean(v) => Cell::new(&v.0.to_string()),
                    })
                    .collect::<Vec<_>>();
                table_view.add_row(Row::new(cells));
            }
            format!("{}", table_view)
        }
    };
    instance
        .write()
        .map_err(|_| anyhow!("lock error"))?
        .commit(txn_id)?;

    stream.write(&(response.len() as u32).to_be_bytes())?;
    stream.write_all(response.as_bytes())?;
    stream.flush()?;

    // TODO: signal trap
    instance
        .read()
        .map_err(|_| anyhow!("lock error"))?
        .shutdown()?;

    Ok(())
}
