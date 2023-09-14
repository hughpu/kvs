use std::{net::{TcpListener, ToSocketAddrs, TcpStream}, io::{BufWriter, Write}};

use serde_json::Deserializer;
use slog::{Drain, o, info, error, Logger};

use crate::{KvsEngine, Result, protocols::{Request, GetResponse, SetResponse, RemoveResponse}, KvsError};


/// kvs server to receive requests from kvs-client
pub struct KvsServer<E: KvsEngine> {
    engine: E,
    log: Logger
}

impl<E: KvsEngine> KvsServer<E> {
    /// create a kvs server as proxy for specified kvs-store engine
    pub fn new(engine: E) -> Self {
        let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let log = slog::Logger::root(drain, o!("version" => env!("CARGO_PKG_VERSION")));
        KvsServer { engine, log }
    }
    
    /// listen to specified address for requests from kvs-client
    pub fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for possible_stream in listener.incoming() {
            if let Err(kvs_error) = self.serve(possible_stream?) {
                error!(self.log, "connection failed with {err}", err=kvs_error.to_string());
            }
        }
        Ok(())
    }
    
    fn serve(&mut self, read_stream: TcpStream) -> Result<()> {
        let write_stream = read_stream.try_clone()?;
        let reader = Deserializer::from_reader(read_stream);
        let mut writer = BufWriter::new(write_stream);
        let stream_reader = reader.into_iter::<Request>();
        
        macro_rules! send_resp {
            ($resp:expr) => {{
                let respond = $resp;
                serde_json::to_writer(&mut writer, &respond)?;
                writer.flush()?;
            }};
        }
        
        for request in stream_reader {
            match request? {
                Request::Get { key } => send_resp!(match self.engine.get(key) {
                    Result::Ok(result) => GetResponse::Ok(result),
                    Result::Err(kvs_error) => GetResponse::Err(format!("{}", kvs_error)),
                }),
                Request::Set { key, value } => send_resp!(match self.engine.set(key, value) {
                    Result::Ok(_) => SetResponse::Ok(()),
                    Result::Err(kvs_error) => SetResponse::Err(format!("{}", kvs_error)),
                }),
                Request::Remove { key } => send_resp!(match self.engine.remove(key) {
                    Result::Ok(_) => RemoveResponse::Ok(()),
                    Result::Err(kvs_error) => RemoveResponse::Err(format!("{}", kvs_error)),
                }),
            }
        }
        Ok(())
    }
}