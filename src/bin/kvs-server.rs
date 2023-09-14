use clap::{Parser, ValueEnum};
use kvs::{KvStore, Result, KvsEngine, SledKvsEngine, server::KvsServer};
use std::{env::current_dir, net, io::{BufReader, prelude::*}};
use slog::{Drain, o, info};
use slog_term;


#[derive(ValueEnum, Clone)]
enum Engine {
    /// kvs
    Kvs,
    
    /// sled
    Sled,
}


impl std::fmt::Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::Kvs => write!(f, "KvStore"),
            Engine::Sled => write!(f, "SledKvsEngine"),
        }
    }
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
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let root_log = slog::Logger::root(drain, o!("version" => env!("CARGO_PKG_VERSION")));

    let cli = Cli::parse();

    info!(root_log, "starting kv server...");

    let server_log = root_log.new(
        o!("addr" => cli.addr.clone(), "engine" => cli.engine.to_string())
    );

    info!(server_log, "starting server...");
    match cli.engine {
        Engine::Kvs => KvsServer::new(KvStore::open(current_dir()?)?).run(&cli.addr),
        Engine::Sled => KvsServer::new(SledKvsEngine::open(current_dir()?)?).run(&cli.addr)
    }
}
