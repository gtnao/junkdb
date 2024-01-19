use anyhow::Result;
use dialoguer::{theme::ColorfulTheme, BasicHistory, Input};
use std::{net::TcpStream, process};

use crate::server::{read_from_stream, write_to_stream};

pub fn client_start() -> Result<()> {
    println!("connecting to junkdb server...");
    let mut stream = TcpStream::connect("127.0.0.1:7878")?;
    println!("connected!");
    let ascii = r#"
     ██╗██╗   ██╗███╗   ██╗██╗  ██╗██████╗ ██████╗
     ██║██║   ██║████╗  ██║██║ ██╔╝██╔══██╗██╔══██╗
     ██║██║   ██║██╔██╗ ██║█████╔╝ ██║  ██║██████╔╝
██   ██║██║   ██║██║╚██╗██║██╔═██╗ ██║  ██║██╔══██╗
╚█████╔╝╚██████╔╝██║ ╚████║██║  ██╗██████╔╝██████╔╝
 ╚════╝  ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═╝╚═════╝ ╚═════╝
    "#;
    println!("{}", ascii);
    println!("Welcome to junkdb!");
    println!("Type \"exit\" or \"quit\" to exit.");
    let mut history = BasicHistory::new().max_entries(100).no_duplicates(true);
    loop {
        if let Ok(cmd) = Input::<String>::with_theme(&ColorfulTheme::default())
            .with_prompt("Query")
            .history_with(&mut history)
            .interact_text()
        {
            if cmd == "exit" || cmd == "quit" {
                println!("Bye!");
                process::exit(0);
            }
            write_to_stream(&mut stream, &cmd)?;
            let response = read_from_stream(&mut stream)?;
            println!("{}", response);
        }
    }
}
