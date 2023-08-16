use std::io::Write;
use std::net::{TcpStream,ToSocketAddrs};
use serde::Deserialize;
use serde_json::{Deserializer, de::IoRead};
use crate::protocols::*;
use crate::Result;
use crate::KvsError;
use std::io::BufWriter;


pub struct KvsClient {
    writer: BufWriter<TcpStream>,
    reader: Deserializer<IoRead<TcpStream>>
}

impl KvsClient {
    fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let write_connection = TcpStream::connect(addr)?;
        let read_connection = write_connection.try_clone()?;
        let writer = BufWriter::new( write_connection);
        let reader = Deserializer::from_reader(read_connection);
        Ok(
            KvsClient { writer, reader }
        )
    }
    
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let request = Request::Set { key, value };
        serde_json::to_writer(&mut self.writer, &request)?;
        self.writer.flush()?;
        
        let response = SetResponse::deserialize(&mut self.reader)?;
        match response {
            SetResponse::Ok(_) => Ok(()),
            SetResponse::Err(err) => Err(KvsError::StringError(err)),
        }
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let request = Request::Get { key };
        serde_json::to_writer(&mut self.writer, &request)?;
        self.writer.flush()?;
        
        let response = GetResponse::deserialize(&mut self.reader)?;
        match response {
            GetResponse::Ok(result) => Ok(result),
            GetResponse::Err(err) => Err(KvsError::StringError(err)),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let request = Request::Remove { key };
        serde_json::to_writer(&mut self.writer, &request)?;
        self.writer.flush()?;
        
        let response = RemoveResponse::deserialize(&mut self.reader)?;
        match response {
            RemoveResponse::Ok(_) => Ok(()),
            RemoveResponse::Err(err) => Err(KvsError::StringError(err)),
        }
    }
}