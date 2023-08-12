use clap::Parser;
use kvs::{KvStore, KvsEngine, KvsError, Result, Command};
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

    let (msg, addr) = match &cli.command {
        Command::Set { key, value, addr } => {
            let msg = format!("command set {key} as {value}");
            (msg, addr)
        }
        Command::Get { key, addr } => {
            let msg = format!("command get {key}");
            (msg, addr)
        }
        Command::Rm { key, addr } => {
            let msg = format!("command remove {key}");
            (msg, addr)
        }
    };

    info!(root, "connecting {addr}", addr=addr);
    let mut connection = TcpStream::connect(addr)?;
    let local_addr = connection.local_addr()?;
    let remote_addr = connection.peer_addr()?;
    let connect_log = root.new(o!("remote" => remote_addr, "local" => local_addr));
    info!(connect_log, "connection establised.");
    info!(connect_log, "sending {message}", message=&msg);
    connection.write_all(msg.as_bytes())?;
    connection.shutdown(net::Shutdown::Write)?;
    
    let mut respond = String::from("");
    connection.read_to_string(&mut respond)?;
    info!(connect_log, "receive {respond} from connection.", respond=&respond);
    
    info!(connect_log, "existing connection...");
    Ok(())
}
