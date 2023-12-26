use crate::error::Result;
use std::{thread, sync::{atomic::{AtomicBool, Ordering}, Arc}};
use crossbeam::channel::{bounded, Sender, Receiver};

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
    fn new(_threads: u32) -> Result<Self> {
        Ok(Self{})
    }
    
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        thread::spawn(job);
    }
}

type DynFunc = Box<dyn FnOnce() + Send + 'static>;

#[derive(Clone)]
struct RecoverableReceiver(Receiver<DynFunc>, Arc<AtomicBool>);

fn run_receiver(receiver: RecoverableReceiver) {
    thread::spawn(move|| {
        while !receiver.1.load(Ordering::SeqCst) {
            if let Ok(job) = receiver.0.recv() {
                job();
            }
        }
    });
}

impl Drop for RecoverableReceiver {
    fn drop(&mut self) {
        if thread::panicking() {
            run_receiver(self.clone());
        }
    }
}


/// the shared queue thread pool
pub struct SharedQueueThreadPool
{
    sender: Sender<DynFunc>,
    terminated: Arc<AtomicBool>,
    threads: u32,
}

impl ThreadPool for SharedQueueThreadPool
{
    fn new(threads: u32) -> Result<Self> {
        let (sender, receiver) = bounded::<DynFunc>((threads * 2) as usize);
        let terminated = Arc::new(AtomicBool::new(false));
        
        for _ in 0..threads {
            let cur_receiver = RecoverableReceiver(
                receiver.clone(),
                terminated.clone(),
            );
            run_receiver(cur_receiver);
        }

        return Ok(
            Self {
                sender,
                terminated,
                threads,
            });
    }
    
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        let _ = self.sender.send(Box::new(job));
    }
}

impl SharedQueueThreadPool {
    /// wait current tasks to finish and terminated all hosted threads
    pub fn stop(&self) {
        let cur_terminated = self.terminated.clone();
        cur_terminated.store(true, Ordering::SeqCst);
        for _ in 0..self.threads {
            let _ = self.sender.send(
                Box::new(|| {})
            );
        }
    }
}
