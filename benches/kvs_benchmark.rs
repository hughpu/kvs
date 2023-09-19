use criterion::{Criterion, criterion_group, criterion_main, BenchmarkId};
use kvs::{KvStore, SledKvsEngine, KvsEngine};
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


fn kvs_bench(c: &mut Criterion) {
    let mut rng = ChaCha8Rng::seed_from_u64(RNG_SEED);
    let keys = get_inputs(&mut rng);
    let values = get_inputs(&mut rng);
    let key_values: Vec<(String, String)> = keys.clone().into_iter().zip(values).collect();
    let cur_dir = TempDir::new().unwrap();
    let mut kvs_store = KvStore::open(&cur_dir.path()).expect(&format!("can not open {:?} with KvStore", cur_dir));
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
    let mut sled_store = SledKvsEngine::open(&cur_dir.path()).expect(&format!("can not open {:?} with sled store", cur_dir));
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

criterion_group!(benches, kvs_bench, sled_bench);
criterion_main!(benches);