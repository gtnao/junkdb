use std::process;

use anyhow::Result;
use junkdb::{client::client_start, server::server_start};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Usage: cargo run client|server");
        process::exit(1);
    }
    match &*args[1] {
        "client" => client_start()?,
        "server" => {
            let init = args.iter().any(|arg| arg == "--init");
            let recover = args.iter().any(|arg| arg == "--recover");
            server_start(init, recover)?;
        }
        _ => {
            println!("Usage: cargo run client|server");
            process::exit(1);
        }
    }
    Ok(())
}
