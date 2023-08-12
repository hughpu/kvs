use clap::{Parser, ValueEnum};
use kvs::{KvStore, Result, KvsEngine, SledKvsEngine};
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

    let engine: Box<dyn KvsEngine> = match cli.engine {
        Engine::Kvs => Box::new(KvStore::open(current_dir()?)?),
        Engine::Sled => Box::new(SledKvsEngine::open(current_dir()?)?)
    };

    let addr: String = cli.addr.clone();

    let server_log = root_log.new(
        o!("addr" => addr.clone(), "engine" => cli.engine.to_string())
    );

    info!(server_log, "kv server is ready.");
    
    let listener = net::TcpListener::bind(addr)?;
    
    for stream in listener.incoming() {
        let mut mut_stream = stream?;
        let peer_addr = mut_stream.peer_addr()?;

        info!(server_log, "receive connection from {peer_addr}", peer_addr=peer_addr);
        let connection_log = server_log.new(o!("peer_address" => peer_addr));

        let buf_read = BufReader::new(&mut mut_stream);
        let http_request = buf_read
            .lines()
            .map(|l| l)
            .filter(
                |lres| lres
                    .as_ref()
                    .map_or(false, |l| !l.is_empty()))
            .collect::<std::result::Result<Vec<String>, std::io::Error>>()?;
        
        for msg in http_request {
            info!(connection_log, "get request as {message}", message=&msg);
        }
        
        mut_stream.write_all("pong".as_bytes())?;
        info!(connection_log, "closing connection");
    }
    
    info!(root_log, "exiting server.");
    Ok(())
}
