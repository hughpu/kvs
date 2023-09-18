use clap::Parser;
use kvs::{KvStore, KvsEngine, KvsError, client::KvsClient, Result, Command};
use std::io::{Write, Read};
use std::net;
use std::{env::current_dir, net::TcpStream};
use std::process::exit;
use {slog, slog::{Drain, o, info}, slog_term};


#[derive(Parser)]
#[command(name=env!("CARGO_PKG_NAME"))]
#[command(version=env!("CARGO_PKG_VERSION"))]
#[command(author=env!("CARGO_PKG_AUTHORS"))]
#[command(about=env!("CARGO_PKG_DESCRIPTION"))]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

fn main() -> Result<()> {
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let root = slog::Logger::root(drain, o!("version" => env!("CARGO_PKG_VERSION")));
    info!(root, "starting kvs client...");

    let cli = Cli::parse();

    match &cli.command {
        Command::Set { key, value, addr } => {
            let mut kvs_cli = KvsClient::connect(addr)?;
            kvs_cli.set(key.clone(), value.clone())?;
            info!(root, "successfully set {key} with {value} in kvs-store proxied via server at {addr}", key=key, value=value, addr=addr);
        }
        Command::Get { key, addr } => {
            let mut kvs_cli = KvsClient::connect(addr)?;
            match kvs_cli.get(key.clone())? {
                Some(value) => {
                    println!("{value}");
                    info!(root, "successfully get {value} from {key} in kvs-store proxied via server at {addr}", key=key, value=&value, addr=addr);
                },
                None => {
                    println!("Key not found for {key}");
                },
            };
        }
        Command::Rm { key, addr } => {
            let mut kvs_cli = KvsClient::connect(addr)?;
            kvs_cli.remove(key.clone())?;
            info!(root, "successfully remove {key} in kvs-store proxied via server at {addr}", key=key, addr=addr);
        }
    };

    Ok(())
}
