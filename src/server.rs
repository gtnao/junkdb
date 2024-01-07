use std::{
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex, RwLock},
    thread,
};

use anyhow::{anyhow, Result};
use prettytable::{Cell, Row, Table};

use crate::{
    buffer::BufferPoolManager,
    catalog::{Catalog, Column, DataType, Schema},
    concurrency::{IsolationLevel, TransactionManager},
    disk::DiskManager,
    executor::{ExecutorContext, ExecutorEngine},
    lexer::tokenize,
    lock::LockManager,
    parser::{ExpressionAST, Parser, StatementAST, TableReferenceAST},
    plan::{Expression, InsertPlan, LiteralExpression, Plan, SeqScanPlan},
    value::Value,
};

pub fn server_start() -> Result<()> {
    println!("toydb server started");

    // init
    let file_name = "test.db";
    let init = need_init(file_name);
    let disk_manager = DiskManager::new(file_name)?;
    let buffer_pool_manager = Arc::new(Mutex::new(BufferPoolManager::new(disk_manager, 32)));
    let lock_manager = Arc::new(RwLock::new(LockManager::default()));
    let transaction_manager = Arc::new(Mutex::new(TransactionManager::new(
        lock_manager.clone(),
        "transaction.log",
        IsolationLevel::RepeatableRead,
    )?));
    let mut catalog = Catalog::new(
        buffer_pool_manager.clone(),
        transaction_manager.clone(),
        lock_manager.clone(),
    );
    catalog.bootstrap(init)?;
    let catalog = Arc::new(Mutex::new(catalog));

    // listen
    let listener = TcpListener::bind("127.0.0.1:7878")?;
    for stream in listener.incoming() {
        let stream = stream?;
        let buffer_pool_manager = buffer_pool_manager.clone();
        let lock_manager = lock_manager.clone();
        let transaction_manager = transaction_manager.clone();
        let catalog = catalog.clone();
        thread::spawn(|| -> Result<()> {
            handle_connection(
                stream,
                buffer_pool_manager,
                lock_manager,
                transaction_manager,
                catalog,
            )
        });
    }
    Ok(())
}

fn handle_connection(
    mut stream: TcpStream,
    buffer_pool_manager: Arc<Mutex<BufferPoolManager>>,
    lock_manager: Arc<RwLock<LockManager>>,
    transaction_manager: Arc<Mutex<TransactionManager>>,
    catalog: Arc<Mutex<Catalog>>,
) -> Result<()> {
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

    let mut response = String::new();

    match statement {
        StatementAST::CreateTable(ast) => {
            let schema = Schema {
                columns: ast
                    .elements
                    .into_iter()
                    .map(|c| Column {
                        name: c.column_name,
                        data_type: c.data_type,
                    })
                    .collect(),
            };
            let txn_id = transaction_manager
                .lock()
                .map_err(|_| anyhow!("lock error"))?
                .begin();
            catalog
                .lock()
                .map_err(|_| anyhow!("lock error"))?
                .create_table(&ast.table_name, &schema, txn_id)?;
            transaction_manager
                .lock()
                .map_err(|_| anyhow!("lock error"))?
                .commit(txn_id)?;
            response = format!("table {} created", ast.table_name);
        }
        _ => {
            let txn_id = transaction_manager
                .lock()
                .map_err(|_| anyhow!("lock error"))?
                .begin();
            let executor_context = ExecutorContext {
                transaction_id: txn_id,
                buffer_pool_manager: buffer_pool_manager.clone(),
                lock_manager,
                transaction_manager: transaction_manager.clone(),
                catalog: catalog.clone(),
            };
            match statement {
                StatementAST::Insert(ast) => {
                    let table_name = ast.table_name;
                    let values = ast.values;
                    let plan = Plan::Insert(InsertPlan {
                        table_name,
                        values: values
                            .iter()
                            .map(|exp| {
                                Expression::Literal(LiteralExpression {
                                    value: match exp {
                                        ExpressionAST::Literal(ast) => ast.value.clone(),
                                        _ => unreachable!(),
                                    },
                                })
                            })
                            .collect(),
                        schema: Schema {
                            columns: vec![Column {
                                name: "__cnt".to_string(),
                                data_type: DataType::Int,
                            }],
                        },
                    });
                    let mut executor = ExecutorEngine::new(plan, executor_context);
                    executor.execute()?;
                    transaction_manager
                        .lock()
                        .map_err(|_| anyhow!("lock error"))?
                        .commit(txn_id)?;
                    response = format!("inserted.");
                }
                StatementAST::Select(ast) => {
                    let table_name = match ast.table_reference {
                        TableReferenceAST::Base(ast) => ast.table_name,
                        _ => unreachable!(),
                    };
                    let schema = catalog
                        .lock()
                        .map_err(|_| anyhow!("lock error"))?
                        .get_schema_by_table_name(&table_name, txn_id)?;
                    let plan = Plan::SeqScan(SeqScanPlan {
                        table_name,
                        schema: schema.clone(),
                    });
                    let mut executor = ExecutorEngine::new(plan, executor_context);
                    let rows = executor.execute()?;
                    transaction_manager
                        .lock()
                        .map_err(|_| anyhow!("lock error"))?
                        .commit(txn_id)?;
                    // TODO: move to client
                    let mut table_view = Table::new();
                    table_view.set_titles(Row::new(
                        schema.columns.iter().map(|c| Cell::new(&c.name)).collect(),
                    ));
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
                    response = table_view.to_string();
                }
                _ => {}
            }
        }
    };

    stream.write(&(response.len() as u32).to_be_bytes())?;
    stream.write_all(response.as_bytes())?;
    stream.flush()?;

    // TODO: signal trap
    buffer_pool_manager
        .lock()
        .map_err(|_| anyhow!("lock error"))?
        .shutdown()?;

    Ok(())
}

fn need_init(file_name: &str) -> bool {
    match fs::metadata(file_name) {
        Ok(_) => false,
        Err(_) => true,
    }
}
