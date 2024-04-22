//! Evaluate the S3FIFO on zipf distribution

use rand::Rng;
use s3fifo::S3FIFO;

fn main() {
    let upper_bound: u64 = 1_000_000;
    let cache_cap = upper_bound as usize / 100;
    let zipf_distr = rand_distr::Zipf::new(upper_bound, 0.99).unwrap();
    let rng = rand::thread_rng();
    let mut iter = rng.sample_iter(zipf_distr);

    let mut cache = S3FIFO::<u64, ()>::new(cache_cap);

    let keys: Vec<u64> = (0..upper_bound)
        .map(|_| iter.next().unwrap() as u64)
        .collect::<Vec<_>>();

    let now = std::time::Instant::now();
    let mut hit_count = 0;
    for &key in keys.iter() {
        if cache.get(&key).is_some() {
            hit_count += 1;
        } else {
            cache.put(key, ());
        }
    }

    println!(
        "S3FIFO elapsed: {:?}. hit ratio: {}",
        now.elapsed(),
        hit_count as f64 / upper_bound as f64
    );

    // // LRU
    // let mut hit_count = 0;
    // let mut cache = lru::LruCache::new(std::num::NonZeroUsize::new(cache_cap).unwrap());
    // let now = std::time::Instant::now();
    // for &key in keys.iter() {
    //     if cache.get(&key).is_some() {
    //         hit_count += 1;
    //     } else {
    //         cache.put(key, ());
    //     }
    // }

    // println!(
    //     "LRU elapsed: {:?}. hit ratio: {}",
    //     now.elapsed(),
    //     hit_count as f64 / upper_bound as f64
    // );
}
