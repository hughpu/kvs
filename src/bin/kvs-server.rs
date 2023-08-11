use clap::{Parser, ValueEnum};
use kvs::{KvStore, Result, KvsEngine, SledKvsEngine};
use std::env::current_dir;


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
    #[arg(long, default_value_t = String::from("127.0.0.1:4000"))]
    addr: String,
    
    /// the key value engine to use, supported `kvs`, `sled`
    #[arg(long, value_enum, default_value_t = Engine::Kvs)]
    engine: Engine
}



fn main() -> Result<()> {
    let cli = Cli::parse();

    let engine: Box<dyn KvsEngine> = match cli.engine {
        Engine::Kvs => Box::new(KvStore::open(current_dir()?)?),
        Engine::Sled => Box::new(SledKvsEngine::open(current_dir()?)?)
    };
    
    let addr: String = cli.addr.clone();

    Ok(())
}
