use core::time;
use std::{net::{TcpListener, ToSocketAddrs, TcpStream}, io::{BufWriter, Write, self}, sync::{Arc, atomic::{AtomicBool, Ordering}}, thread};

use serde_json::Deserializer;
use slog::{Drain, o, info, error, Logger, warn};

use crate::{KvsEngine, Result, protocols::{Request, GetResponse, SetResponse, RemoveResponse}, thread_pool::ThreadPool, KvsError};


/// kvs server to receive requests from kvs-client
#[derive(Clone)]
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    log: Logger,
    pool: P,
    terminated: Arc<AtomicBool>,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// create a kvs server as proxy for specified kvs-store engine
    pub fn new(engine: E, pool: P) -> Self {
        let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let log = slog::Logger::root(drain, o!("version" => env!("CARGO_PKG_VERSION")));
        let terminated = Arc::new(AtomicBool::new(false));
        KvsServer { engine, log, pool, terminated }
    }
    
    /// listen to specified address for requests from kvs-client
    pub fn run<A: ToSocketAddrs>(&mut self, addr: &A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        for possible_stream in listener.incoming() {
            if self.terminated.load(Ordering::SeqCst) {
                warn!(self.log, "server got terminated");
                break;
            }
            let engine = self.engine.clone();
            let logger = self.log.new(o!("name" => "thread_logger"));
            let terminated_clone = self.terminated.clone();
            match possible_stream {
                Ok(stream) => self.pool.spawn(
                    move || { 
                        if let Err(err) = serve(engine, stream, &logger, terminated_clone) {
                            error!(logger, "failed to serve with {err}", err=err.to_string())
                        }
                    }
                ),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(time::Duration::from_millis(10));
                },
                Err(err) => {
                    error!(self.log, "connection failed with {err}", err=err.to_string())
                },
            }
        }
        Ok(())
    }
    
    /// stop to accept new connection and ask existing connections to exit
    pub fn close(&self) {
        self.terminated.store(true, Ordering::SeqCst);
    }
}

fn serve<E: KvsEngine>(engine: E, read_stream: TcpStream, logger: &Logger, terminated: Arc<AtomicBool>) -> Result<()> {
    let write_stream = read_stream.try_clone()?;
    read_stream.set_read_timeout(Some(time::Duration::from_secs(2)))?;
    let mut writer = BufWriter::new(write_stream);
    let remote_addr = read_stream.peer_addr()?;
    info!(logger, "serving connection from {addr}", addr=remote_addr);
    
    macro_rules! send_resp {
        ($resp:expr) => {{
            let respond = $resp;
            serde_json::to_writer(&mut writer, &respond)?;
            writer.flush()?;
        }};
    }
    
    let reader = Deserializer::from_reader(&read_stream);
    let stream_reader = reader.into_iter::<Request>();
    for chunck in stream_reader {
        if terminated.load(Ordering::SeqCst) {
            warn!(logger, "connection handling got terminated!");
            break;
        }
        
        match chunck {
            Ok(request) => match request {
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
            },
            Err(err) => {
                if let Some(kind) = err.io_error_kind() {
                    if kind == io::ErrorKind::TimedOut {
                        continue;
                    }
                    return Result::Err(KvsError::from(err));
                }
            }
        }

    }
    Ok(())
}