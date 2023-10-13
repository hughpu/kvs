use crate::error::Result;
use std::thread;

/// thread pool trait
pub trait ThreadPool {
    /// construct a new thread pool with the specified threads num
    fn new(threads: u32) -> Result<Self>
    where Self: Sized;

    /// create a new job and assign it to the thread pool
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}

/// the naive dummy implementation of thread pool
pub struct NaiveThreadPool {}

impl ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> Result<Self> {
        Ok(Self{})
    }
    
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        thread::spawn(job);
    }
}

