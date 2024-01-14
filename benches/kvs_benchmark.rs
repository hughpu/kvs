use core::time;
use std::{sync::{atomic::{AtomicU32, Ordering}, Arc}, thread};

use criterion::{Criterion, criterion_group, criterion_main, BenchmarkId};
use crossbeam::channel::unbounded;
use kvs::{KvStore, SledKvsEngine, KvsEngine, thread_pool::{SharedQueueThreadPool, ThreadPool}, server::KvsServer, client::KvsClient};
use rand::prelude::*;
use tempfile::TempDir;
use rand_chacha::ChaCha8Rng;

static RNG_SEED: u64 = 12109973;
static INPUT_LEN: usize = 100;
static MAX_STR_LEN: usize = 1000;
static READ_LEN: usize = 10000;

fn get_inputs(rng: &mut ChaCha8Rng) -> Vec<String> {
    let inputs = (0..INPUT_LEN)
        .map(|_| {
            let str_len = rng.gen_range(1..=MAX_STR_LEN);
            let rand_str: String = (0..str_len)
                .map(|_| rng.gen::<char>())
                .collect();
            rand_str
        })
        .collect();
    inputs
}


fn get_multithread_inputs() -> Vec<String> {
    (10000 .. 11000).map(|num| num.to_string()).collect()
}


fn kvs_bench(c: &mut Criterion) {
    let mut rng = ChaCha8Rng::seed_from_u64(RNG_SEED);
    let keys = get_inputs(&mut rng);
    let values = get_inputs(&mut rng);
    let key_values: Vec<(String, String)> = keys.clone().into_iter().zip(values).collect();
    let cur_dir = TempDir::new().unwrap();
    let kvs_store = KvStore::open(&cur_dir.path()).expect(&format!("can not open {:?} with KvStore", cur_dir));
    let mut group = c.benchmark_group("KvStoreBench");
    group.bench_with_input(BenchmarkId::new("kvs_write", 100), &key_values, |b, kvs| {
        b.iter(|| {
            kvs.into_iter().for_each(
                |(k, v)| kvs_store.set(k.clone(), v.clone()).expect(&format!("failed to write ({}, {}) to sled", k, v))
            )
        });
    });

    let read_keys: Vec<String> = (0..READ_LEN)
        .map(|_| keys[rng.gen_range(0..INPUT_LEN)].clone())
        .collect();

    group.bench_with_input(BenchmarkId::new("kvs_read", 10000), &read_keys, |b, keys| {
        b.iter(|| {
            keys.into_iter().for_each(
                |k| {
                    kvs_store
                        .get(k.clone())
                        .expect(&format!("failed to read some key from KvStore"))
                        .expect(&format!("the value of some key in KvStore is empty"));
                }
            )
        });
    });
    group.finish();
}


fn sled_bench(c: &mut Criterion) {
    let mut rng = ChaCha8Rng::seed_from_u64(RNG_SEED);
    let keys = get_inputs(&mut rng);
    let values = get_inputs(&mut rng);
    let key_values: Vec<(String, String)> = keys.clone().into_iter().zip(values).collect();
    let cur_dir = TempDir::new().unwrap();
    let sled_store = SledKvsEngine::open(&cur_dir.path()).expect(&format!("can not open {:?} with sled store", cur_dir));
    let mut group = c.benchmark_group("SledStoreBench");
    group.bench_with_input(BenchmarkId::new("sled_write", 100), &key_values, |b, kvs| {
        b.iter(|| {
            kvs.into_iter().for_each(
                |(k, v)| sled_store.set(k.clone(), v.clone()).expect(&format!("failed to write ({}, {}) to sled", k, v))
            )
        });
    });

    let read_keys: Vec<String> = (0..READ_LEN)
        .map(|_| keys[rng.gen_range(0..INPUT_LEN)].clone())
        .collect();

    group.bench_with_input(BenchmarkId::new("sled_read", 10000), &read_keys, |b, keys| {
        b.iter(|| {
            keys.into_iter().for_each(
                |k| {
                    sled_store
                        .get(k.clone())
                        .expect(&format!("failed to read some key from sled"))
                        .expect(&format!("the value of some key in sled is empty"));
                }
            )
        });
    });
    group.finish();
}


enum CliMessage {
    Normal(String, String),
    Stop(Arc<AtomicU32>),
}


fn write_queued_kvstore(c: &mut Criterion) {
    let keys_to_write = get_multithread_inputs();
    let val_to_write = "test";
    let pool_sizes: Vec<u32> = vec![1, 2, 4, 8, 16, 32, 64];
    let mut group = c.benchmark_group("SharedQueueKvsStore");
    for pool_size in pool_sizes {
        group.bench_with_input(
            BenchmarkId::new("shared_kvs_write", format!("pool_size({})", pool_size)),
            &pool_size,
            |b, ps| {
                let pool = SharedQueueThreadPool::new(pool_size).unwrap();
                let tmp_dir = TempDir::new().unwrap();
                let engine = KvStore::open(tmp_dir.path()).unwrap();
                let kvs_server = KvsServer::new(engine, pool.clone());
                let addr = String::from("127.0.0.1:4000");
                let mut clone_server = kvs_server.clone();
                let addr_clone = addr.clone();

                let server_handler = thread::spawn(move || {
                    clone_server.run(&addr_clone).unwrap();
                });

                println!("kvs server started!");
                thread::sleep(time::Duration::from_secs(2));
                let clis: Vec<KvsClient> = (0..*ps as i32).map(
                    |_| KvsClient::connect(addr.clone()).unwrap()
                ).collect();
                println!("number of {} kvs clients started!", clis.len());

                let (cli_sender, cli_receiver) = unbounded::<CliMessage>();
                for mut cli in clis.into_iter() {
                    let thread_receiver = cli_receiver.clone();
                    thread::spawn(
                        move|| {
                            for msg in thread_receiver {
                                match msg {
                                    CliMessage::Normal(k, v) => cli.set(k, v).unwrap(),
                                    CliMessage::Stop(counter) => {
                                        counter.fetch_sub(1, Ordering::SeqCst);
                                        loop {
                                            if counter.load(Ordering::SeqCst) == 0 {
                                                break;
                                            }
                                        }
                                    },
                                }
                            }
                        }
                    );
                }
                println!("{} kvs clients concurrent pool started!", *ps);

                println!("sharedthreadpool with kvs engine start benchmark");
                b.iter(
                    || {
                        for k in keys_to_write.iter() {
                            let key = k.to_owned();
                            let value = val_to_write.to_owned();
                            cli_sender.send(CliMessage::Normal(key, value)).unwrap();
                        };
                        
                        let cli_unfinished = Arc::new(AtomicU32::new(*ps));
                        for _ in 0..*ps {
                            cli_sender.send(CliMessage::Stop(cli_unfinished.clone())).unwrap();
                        }
                        
                        loop {
                            if cli_unfinished.load(Ordering::SeqCst) == 0 {
                                break;
                            }
                        }
                    }
                );
                kvs_server.close();
                server_handler.join().unwrap();
                println!("server stopped!");
            }
        );
    }
    group.finish();
}

criterion_group!(benches, kvs_bench, sled_bench, write_queued_kvstore);
criterion_main!(benches);