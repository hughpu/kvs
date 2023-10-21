use clap::{Parser, ValueEnum};
use kvs::{KvStore, Result, KvsEngine, SledKvsEngine, server::KvsServer, KvsError, thread_pool::*};
use std::{env::current_dir, net, io::{BufReader, prelude::*}};
use slog::{Drain, o, info, warn, Logger};
use slog_term;
use num_cpus;


static ENGINE_FILE: &str = "engine";


#[derive(ValueEnum, Clone, Debug, PartialEq)]
enum Engine {
    /// kvs
    Kvs,
    
    /// sled
    Sled,
}


impl std::str::FromStr for Engine {
    type Err = KvsError;
    fn from_str(input: &str) -> Result<Engine> {
        match input {
            "KvStore" => Ok(Engine::Kvs),
            "SledKvsEngine" => Ok(Engine::Sled),
            _ => Err(KvsError::StringError(format!("unable to parse {input}"))),
        }
    }
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
    
    /// the key value engine to use, supported `kvs`, `sled`, default as `kvs`
    #[arg(long, value_enum)]
    engine: Option<Engine>
}


fn main() -> Result<()> {
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let root_log = slog::Logger::root(drain, o!("version" => env!("CARGO_PKG_VERSION")));

    let mut cli = Cli::parse();

    info!(root_log, "starting kvs server...");
    if let Some(engine) = current_engine(&root_log)? {
        if cli.engine.is_none() {
            cli.engine = Some(engine.clone());
        } 

        if let Some(cli_engine) = cli.engine.clone() {
            if cli_engine != engine {
                return Err(KvsError::StringError(format!("specified engine {cli_engine} is not match existing engine {engine}")))
            }
        }
    }
    
    let engine = cli.engine.unwrap_or(Engine::Kvs);
    std::fs::write(current_dir()?.join(ENGINE_FILE), format!("{}", engine))?;

    let server_log = root_log.new(
        o!("addr" => cli.addr.clone(), "engine" => engine.to_string())
    );

    info!(server_log, "starting server...");
    let pool = NaiveThreadPool::new(num_cpus::get() as u32)?;

    match engine {
        Engine::Kvs => KvsServer::new(
            KvStore::open(current_dir()?)?,
            pool
        ).run(&cli.addr),
        Engine::Sled => KvsServer::new(
            SledKvsEngine::open(current_dir()?)?,
            pool
        ).run(&cli.addr)
    }
}

fn current_engine(log: &Logger) -> Result<Option<Engine>> {
    let engine_filepath = current_dir()?.join(ENGINE_FILE);
    
    if !engine_filepath.exists() {
        return Ok(None);
    }

    match std::fs::read_to_string(engine_filepath)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            warn!(log, "Failed to parse engine file with {error}", error=format!("{}", e));
            Ok(None)
        }
    }
}