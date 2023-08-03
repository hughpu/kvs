use clap::{Parser, ValueEnum};
use kvs::{KvStore, KvsError, Result, Command};
use std::env::current_dir;
use std::process::exit;


#[derive(ValueEnum, Clone)]
enum Engine {
    /// kvs
    Kvs,
    
    /// sled
    Sled,
}


#[derive(Parser)]
#[command(name=env!("CARGO_PKG_NAME"))]
#[command(version=env!("CARGO_PKG_VERSION"))]
#[command(author=env!("CARGO_PKG_AUTHORS"))]
#[command(about=env!("CARGO_PKG_DESCRIPTION"))]
struct Cli {
    /// the ip:port address to bind to
    #[arg(long, default_value_t = String::from("127.0.0.1:12368"))]
    addr: String,
    
    /// the key value engine to use, supported `kvs`, `sled`
    #[arg(long, value_enum, default_value_t = Engine::Kvs)]
    engine: Engine
}



fn main() -> Result<()> {
    let cli = Cli::parse();

    let engine: KvsEngine = match cli.engine {
        Engine::Kvs => KvStore::open(current_dir()?),
        Engine::Sled => SledStore::open(current_dir()?)
    }?;
    
    let addr: String = cli.addr.clone();

    Ok(())
}
