use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}};

use clap::Subcommand;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use crossbeam_skiplist::SkipMap;

use crate::{KvsError, Result, KvsEngine};
use std::ffi::OsStr;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;
pub const DEFAULT_ADDR: &str = "127.0.0.1:4000";


/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log files. Log files are named after
/// monotonically increasing generation numbers with a `log` extension name.
/// A `BTreeMap` in memory stores the keys and the value locations for fast query.
///
/// ```rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use kvs::KvsEngine;
/// use std::env::current_dir;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```
pub struct KvStore {
    // directory for the log and other data.
    path: Arc<PathBuf>,
    // kvs reader
    kvs_reader: KvReader,
    // kvs writer
    kvs_writer: Arc<Mutex<KvWriter>>,
    // index
    index: Arc<SkipMap<String, CommandPos>>,
}

struct KvWriter {
    // path
    path: Arc<PathBuf>,
    // writer of the current log.
    writer: BufWriterWithPos<File>,
    current_gen: u64,
    index: Arc<SkipMap<String, CommandPos>>,
    // the number of bytes representing "stale" commands that could be
    // deleted during a compaction.
    uncompacted: u64,
    // reader
    kvs_reader: KvReader,
}

struct KvReader {
    // map generation number to the file reader.
    readers: RefCell<BTreeMap<u64, BufReaderWithPos<File>>>,
    // latest generation
    safe_point: Arc<AtomicU64>,
    // dir path of logs
    path: Arc<PathBuf>
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        KvStore {
            path: self.path.clone(),
            kvs_reader: self.kvs_reader.clone(),
            kvs_writer: self.kvs_writer.clone(),
            index: self.index.clone(),
        }
    }
}

impl Clone for KvReader {
    fn clone(&self) -> Self {
        KvReader {
            readers: RefCell::new(BTreeMap::new()),
            safe_point: self.safe_point.clone(),
            path: self.path.clone()
        }
    }
}

impl KvReader {
    fn read_and<F, R>(&self, cmd_pos: &CommandPos, f: F) -> Result<R>
    where F: FnOnce(io::Take<&mut BufReaderWithPos<File>>) -> Result<R>
    {
        let mut readers  = self.readers.borrow_mut();
        if !readers.contains_key(&cmd_pos.gen) {
            let reader = BufReaderWithPos::new(
                File::open(log_path(&self.path, cmd_pos.gen))?
            )?;
            readers.insert(cmd_pos.gen, reader);
        }
        let reader = readers.get_mut(&cmd_pos.gen).expect("Cannot find log reader");
        reader.seek(SeekFrom::Start(cmd_pos.pos))?;
        let cmd_reader = reader.take(cmd_pos.len);
        f(cmd_reader)
    }
    
    fn close_stale_readers(&self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let first_gen = *readers.keys().next().unwrap();
            if first_gen >= self.safe_point.load(Ordering::SeqCst) {
                break;
            }
            readers.remove(&first_gen);
        }
    }
}

impl KvWriter {
    /// Clears stale entries in the log.
    fn compact(&mut self) -> Result<()> {
        // increase current gen by 2. current_gen + 1 is for the compaction file.
        let compaction_gen = self.current_gen + 1;
        self.current_gen += 2;
        
        self.writer = new_log_file(&self.path, self.current_gen)?;

        let mut compaction_writer = new_log_file(&self.path, compaction_gen)?;

        let mut new_pos = 0; // pos in the new log file.
        for entry in self.index.iter() {
            let key = entry.key();
            let cmd_pos = entry.value();

            // let mut entry_reader = self.kvs_reader.get_reader(&cmd_pos)?;
            let len = self.kvs_reader.read_and(
                &cmd_pos,
                |mut reader| { Ok(io::copy(&mut reader, &mut compaction_writer)?) }
            )?;
            self.index.insert(key.clone(), (compaction_gen, new_pos..new_pos + len).into());
            new_pos += len;
        }
        compaction_writer.flush()?;
        
        self.kvs_reader.safe_point.store(compaction_gen, Ordering::SeqCst);

        // remove stale log files.
        let stale_gens: Vec<_> = sorted_gen_list(&self.path)?
            .iter()
            .filter(|&&gen| gen < compaction_gen)
            .cloned()
            .collect();

        for stale_gen in stale_gens {
            self.kvs_reader.readers.borrow_mut().remove(&stale_gen);
            fs::remove_file(log_path(&self.path, stale_gen))?;
        }
        
        self.uncompacted = 0;

        Ok(())
    }
    
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);

        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        if let Command::Set { key, .. } = cmd {
            if let Some(old_cmd) = self.index.get(&key) {
                self.uncompacted += old_cmd.value().len;
            }

            self.index
                .insert(key, (self.current_gen, pos..self.writer.pos).into());
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::remove(key);
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Command::Rm { key, .. } = cmd {
                let old_entry = self.index.remove(&key).expect("key not found");
                let old_cmd = old_entry.value();
                self.uncompacted += old_cmd.len;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

impl KvStore {
    /// Opens a `KvStore` with the given path.
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during the log replay.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = BTreeMap::new();
        let index = SkipMap::new();

        let gen_list = sorted_gen_list(&path)?;
        let mut uncompacted = 0;

        for &gen in &gen_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, gen))?)?;
            uncompacted += load(gen, &mut reader, &index)?;
            readers.insert(gen, reader);
        }

        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, current_gen)?;

        let index = Arc::new(index);

        let safe_point = Arc::new(AtomicU64::new(gen_list.first().unwrap_or(&current_gen).clone()));

        let path = Arc::new(path);
        let kvs_reader = KvReader {
            readers: RefCell::new(readers),
            safe_point,
            path: path.clone(),
        };

        let kvs_writer = Arc::new(
            Mutex::new(
                KvWriter {
                    writer,
                    current_gen,
                    index: index.clone(),
                    uncompacted,
                    kvs_reader: kvs_reader.clone(),
                    path: path.clone()
                }
            )
        );

        Ok(KvStore {
            path,
            index,
            kvs_reader,
            kvs_writer
        })
    }
}

impl KvsEngine for KvStore {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    ///
    /// # Errors
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn set(&self, key: String, value: String) -> Result<()> {
        let mut kvs_writer = self.kvs_writer.lock()?;
        kvs_writer.set(key, value)
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::UnexpectedCommandType` if the given command type unexpected.
    fn get(&self, key: String) -> Result<Option<String>> {
        self.kvs_reader.close_stale_readers();

        if let Some(entry) = self.index.get(&key) {
            let cmd_pos = entry.value();
            self.kvs_reader.read_and(
                cmd_pos,
                |cmd_reader| {
                    if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                        Ok(Some(value))
                    } else {
                        Err(KvsError::UnexpectedCommandType)
                    }
                }
            )
        } else {
            Ok(None)
        }
    }

    /// Removes a given key.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn remove(&self, key: String) -> Result<()> {
        let mut kvs_writer = self.kvs_writer.lock()?;
        kvs_writer.remove(key)
    }
}

/// Create a new log file with given generation number and add the reader to the readers map.
///
/// Returns the writer to the log.
fn new_log_file(
    path: &Path,
    gen: u64
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&path, gen);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    Ok(writer)
}

/// Returns sorted generation numbers in the given directory.
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
}

/// Load the whole log file and store value locations in the index map.
///
/// Returns how many bytes can be saved after a compaction.
fn load(
    gen: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &SkipMap<String, CommandPos>,
) -> Result<u64> {
    // To make sure we read from the beginning of the file.
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted = 0; // number of bytes that can be saved after a compaction.
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.value().len;
                }
                let new_len = new_pos - pos;
                index.insert(key, CommandPos{gen, pos, len: new_len});
            }
            Command::Rm { key, .. } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.value().len;
                }
                // the "remove" command itself can be deleted in the next compaction.
                // so we add its length to `uncompacted`.
                uncompacted += new_pos - pos;
            }
            _ => ()
        }
        pos = new_pos;
    }
    Ok(uncompacted)
}

fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

/// Struct representing a command.
#[derive(Serialize, Deserialize, Debug, Subcommand)]
pub enum Command {
    /// set the key with supplied value in database
    Set { 
        /// key to set
        key: String,

        /// value to set
        value: String,
        
        /// address:port of kvs server
        #[serde(skip)]
        #[arg(long, default_value_t = DEFAULT_ADDR.to_string())]
        addr: String
    },
    
    /// remove the key in database
    Rm { 
        /// key to remove
        key: String,
        
        /// address:port of kvs server
        #[serde(skip)]
        #[arg(long, default_value_t = DEFAULT_ADDR.to_string())]
        addr: String
    },
    
    /// get the value of the key in database
    Get {
        /// key to get
        key: String,
        
        /// address:port of kvs server
        #[serde(skip)]
        #[arg(long, default_value_t = DEFAULT_ADDR.to_string())]
        addr: String
    },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value, addr: String::from("") }
    }

    fn remove(key: String) -> Command {
        Command::Rm { key, addr: String::from("") }
    }
}

/// Represents the position and length of a json-serialized command in the log.
struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        CommandPos {
            gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
