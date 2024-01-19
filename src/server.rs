use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, RwLock},
    thread,
};

use anyhow::{anyhow, Result};
use prettytable::{Cell, Row, Table};
use signal_hook::{consts::TERM_SIGNALS, iterator::Signals};

use crate::{
    common::TransactionID,
    instance::Instance,
    lexer::tokenize,
    parser::{Parser, StatementAST},
};

const SERVER_DEFAULT_PORT: u16 = 7878;

pub fn server_start(init: bool) -> Result<()> {
    println!("junkdb server started");

    // init
    let instance = Arc::new(RwLock::new(Instance::new("data", init)?));

    // trap signals
    let instance_clone = instance.clone();
    let mut signals = Signals::new(TERM_SIGNALS)?;
    thread::spawn(move || {
        for _ in signals.forever() {
            println!("junkdb server shutdown...");
            if let Ok(instance) = instance_clone.read() {
                if let Err(e) = instance.shutdown() {
                    println!("shutdown error: {}", e);
                    std::process::exit(1);
                }
            } else {
                println!("shutdown error: lock error");
                std::process::exit(1);
            }
            std::process::exit(0);
        }
    });

    // listen
    let listener = TcpListener::bind(format!("127.0.0.1:{}", SERVER_DEFAULT_PORT))?;
    for stream in listener.incoming() {
        let stream = stream?;
        println!("connection established: {}", stream.peer_addr()?);
        let instance = instance.clone();
        thread::spawn(|| -> Result<()> {
            let mut session = Session::new(stream, instance);
            session.start()
        });
    }
    Ok(())
}

struct Session {
    stream: TcpStream,
    instance: Arc<RwLock<Instance>>,
    current_txn_id: Option<TransactionID>,
}
impl Session {
    pub fn new(stream: TcpStream, instance: Arc<RwLock<Instance>>) -> Self {
        Self {
            stream,
            instance,
            current_txn_id: None,
        }
    }
    fn start(&mut self) -> Result<()> {
        loop {
            let request = self.read()?;
            match self.execute(&request) {
                Ok(response) => {
                    self.write(&response)?;
                }
                Err(e) => {
                    self.write(&format!("error: {}", e))?;
                    self.rollback()?;
                }
            }
        }
    }
    fn read(&mut self) -> Result<String> {
        match read_from_stream(&mut self.stream) {
            Ok(request) => {
                return Ok(request);
            }
            Err(e) => {
                self.rollback()?;
                if let Some(io_err) = e.downcast_ref::<io::Error>() {
                    if io_err.kind() == io::ErrorKind::UnexpectedEof {
                        println!("connection closed: {}", self.stream.peer_addr()?);
                        return Err(e);
                    }
                }
                println!("read error: {}", e);
                return Err(e);
            }
        }
    }
    fn write(&mut self, response: &str) -> Result<()> {
        match write_to_stream(&mut self.stream, &response) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                self.rollback()?;
                if let Some(io_err) = e.downcast_ref::<io::Error>() {
                    if io_err.kind() == io::ErrorKind::BrokenPipe {
                        println!("connection closed: {}", self.stream.peer_addr()?);
                        return Err(e);
                    }
                }
                println!("write error: {}", e);
                return Err(e);
            }
        }
    }
    fn rollback(&mut self) -> Result<()> {
        if let Some(txn_id) = self.current_txn_id {
            self.instance
                .write()
                .map_err(|_| anyhow!("lock error"))?
                .rollback(txn_id)?;
            self.current_txn_id = None;
        }
        Ok(())
    }
    fn execute(&mut self, query: &str) -> Result<String> {
        // parse
        let mut iter = query.chars().peekable();
        let tokens = tokenize(&mut iter)?;
        let mut parser = Parser::new(tokens);
        let statement = parser.parse()?;

        let response = match statement {
            StatementAST::Begin => {
                let txn_id = self
                    .instance
                    .read()
                    .map_err(|_| anyhow!("lock error"))?
                    .begin(self.current_txn_id)?;
                self.current_txn_id = Some(txn_id);
                format!("transaction started.")
            }
            _ => {
                let txn_id_existed = self.current_txn_id.is_some();
                if !txn_id_existed {
                    let txn_id = Some(
                        self.instance
                            .read()
                            .map_err(|_| anyhow!("lock error"))?
                            .begin(None)?,
                    );
                    self.current_txn_id = txn_id;
                }
                let txn_id = self.current_txn_id.unwrap();
                let response = match statement {
                    StatementAST::Commit => {
                        self.instance
                            .write()
                            .map_err(|_| anyhow!("lock error"))?
                            .commit(txn_id)?;
                        self.current_txn_id = None;
                        format!("transaction committed.")
                    }
                    StatementAST::Rollback => {
                        self.instance
                            .write()
                            .map_err(|_| anyhow!("lock error"))?
                            .rollback(txn_id)?;
                        self.current_txn_id = None;
                        format!("transaction rolled back.")
                    }
                    StatementAST::CreateTable(ast) => {
                        self.instance
                            .write()
                            .map_err(|_| anyhow!("lock error"))?
                            .create_table(&ast, txn_id)?;
                        format!("table {} created", ast.table_name)
                    }
                    _ => {
                        let (rows, schema) = self
                            .instance
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
                                .map(|v| Cell::new(&v.to_string()))
                                .collect::<Vec<_>>();
                            table_view.add_row(Row::new(cells));
                        }
                        format!("{}", table_view)
                    }
                };
                if !txn_id_existed {
                    self.instance
                        .write()
                        .map_err(|_| anyhow!("lock error"))?
                        .commit(txn_id)?;
                    self.current_txn_id = None;
                }
                response
            }
        };
        Ok(response)
    }
}

pub fn write_to_stream(stream: &mut TcpStream, response: &str) -> Result<()> {
    stream.write(&(response.len() as u32).to_be_bytes())?;
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

pub fn read_from_stream(stream: &mut TcpStream) -> Result<String> {
    let mut size_buffer = [0u8; 4];
    stream.read_exact(&mut size_buffer)?;
    let mut buffer = vec![0u8; u32::from_be_bytes(size_buffer) as usize];
    stream.read_exact(&mut buffer)?;
    let response = String::from_utf8(buffer)?;
    Ok(response)
}
