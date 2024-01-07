use std::process;

use anyhow::Result;
use toydb::{client::client_start, server::server_start};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        println!("Usage: cargo run client|server");
        process::exit(1);
    }
    match &*args[1] {
        "client" => client_start()?,
        "server" => server_start()?,
        _ => {
            println!("Usage: cargo run client|server");
            process::exit(1);
        }
    }
    Ok(())
}
