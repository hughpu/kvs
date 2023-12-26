use std::{net::{TcpListener, ToSocketAddrs, TcpStream}, io::{BufWriter, Write}};

use serde_json::Deserializer;
use slog::{Drain, o, info, error, Logger};

use crate::{KvsEngine, Result, protocols::{Request, GetResponse, SetResponse, RemoveResponse}, thread_pool::ThreadPool};


/// kvs server to receive requests from kvs-client
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    log: Logger,
    pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// create a kvs server as proxy for specified kvs-store engine
    pub fn new(engine: E, pool: P) -> Self {
        let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let log = slog::Logger::root(drain, o!("version" => env!("CARGO_PKG_VERSION")));
        KvsServer { engine, log, pool }
    }
    
    /// listen to specified address for requests from kvs-client
    pub fn run<A: ToSocketAddrs>(&mut self, addr: &A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for possible_stream in listener.incoming() {
            let engine = self.engine.clone();
            let logger = self.log.new(o!("name" => "thread_logger"));
            match possible_stream {
                Ok(stream) => self.pool.spawn(
                    move || { 
                        if let Err(err) = serve(engine, stream, &logger) {
                            error!(logger, "failed to serve with {err}", err=err.to_string())
                        }
                    }
                ),
                Err(err) => error!(self.log, "connection failed with {err}", err=err.to_string())
            }
        }
        Ok(())
    }
    
}

fn serve<E: KvsEngine>(engine: E, read_stream: TcpStream, logger: &Logger) -> Result<()> {
    let write_stream = read_stream.try_clone()?;
    let reader = Deserializer::from_reader(&read_stream);
    let mut writer = BufWriter::new(write_stream);
    let stream_reader = reader.into_iter::<Request>();
    let remote_addr = read_stream.peer_addr()?;
    info!(logger, "serving connection from {addr}", addr=remote_addr);
    
    macro_rules! send_resp {
        ($resp:expr) => {{
            let respond = $resp;
            serde_json::to_writer(&mut writer, &respond)?;
            writer.flush()?;
        }};
    }
    
    for request in stream_reader {
        match request? {
            Request::Get { key } => {
                info!(logger, "handling request try to {method} {key}", method="get", key=&key);
                send_resp!(match engine.get(key) {
                    Result::Ok(result) => GetResponse::Ok(result),
                    Result::Err(kvs_error) => GetResponse::Err(format!("{}", kvs_error)),
                })
            },
            Request::Set { key, value } => {
                info!(logger, "handling request try to {method} {key} as {value}", method="set", key=&key, value=&value);
                send_resp!(match engine.set(key, value) {
                    Result::Ok(_) => SetResponse::Ok(()),
                    Result::Err(kvs_error) => SetResponse::Err(format!("{}", kvs_error)),
                })
            },
            Request::Remove { key } => {
                info!(logger, "handling request try to {method} {key}", method="remove", key=&key);
                send_resp!(match engine.remove(key) {
                    Result::Ok(_) => RemoveResponse::Ok(()),
                    Result::Err(kvs_error) => RemoveResponse::Err(format!("{}", kvs_error)),
                })
            },
        }
    }
    Ok(())
}