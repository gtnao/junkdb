use anyhow::Result;
use dialoguer::{theme::ColorfulTheme, BasicHistory, Input};
use std::{
    io::{Read, Write},
    net::TcpStream,
    process,
};

pub fn client_start() -> Result<()> {
    println!("Welcome to toydb!");

    let mut history = BasicHistory::new().max_entries(8).no_duplicates(true);
    loop {
        if let Ok(cmd) = Input::<String>::with_theme(&ColorfulTheme::default())
            .with_prompt("Query")
            .history_with(&mut history)
            .interact_text()
        {
            if cmd == "exit" || cmd == "quit" {
                process::exit(0);
            }

            let mut stream = TcpStream::connect("127.0.0.1:7878")?;
            stream.write(&(cmd.len() as u32).to_be_bytes())?;
            stream.write(cmd.as_bytes())?;
            stream.flush()?;

            let mut size_buffer = [0u8; 4];
            stream.read_exact(&mut size_buffer)?;
            let mut buffer = vec![0u8; u32::from_be_bytes(size_buffer) as usize];
            stream.read_exact(&mut buffer)?;
            let response = String::from_utf8(buffer)?;
            println!("{}", response);
        }
    }
}
