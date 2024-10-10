//! Evaluate the S3FIFO on zipf distribution

use rand::Rng;
use s3fifo::S3FIFO;
use std::time::Duration;

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
    let mut get_time = Duration::default();
    let mut put_time = Duration::default();
    let mut hit_count = 0;
    for key in keys.iter() {
        let now = std::time::Instant::now();
        let entry = cache.get(key);
        get_time += now.elapsed();

        if entry.is_some() {
            hit_count += 1;
        } else {
            let now = std::time::Instant::now();
            cache.put(*key, ());
            put_time += now.elapsed();
        }
    }

    println!(
        "S3FIFO elapsed: {:?}. hit ratio: {}. Get time: {:?}. Put time: {:?}",
        now.elapsed(),
        hit_count as f64 / upper_bound as f64,
        get_time,
        put_time
    );

    // // LRU
    // let mut hit_count = 0;
    // let cache = Mutex::new(lru::LruCache::new(
    //     std::num::NonZeroUsize::new(cache_cap).unwrap(),
    // ));
    // let now = std::time::Instant::now();
    // for &key in keys.iter() {
    //     if cache.lock().get(&key).is_some() {
    //         hit_count += 1;
    //     } else {
    //         cache.lock().put(key, ());
    //     }
    // }
    // println!(
    //     "LRU elapsed: {:?}. hit ratio: {}",
    //     now.elapsed(),
    //     hit_count as f64 / upper_bound as f64
    // );

    // QuickCache
    let mut cache = quick_cache::unsync::Cache::new(cache_cap);

    let mut hit_count = 0;
    let now = std::time::Instant::now();
    for &key in keys.iter() {
        if cache.get(&key).is_some() {
            hit_count += 1;
        } else {
            cache.insert(key, ());
        }
    }

    println!(
        "QuickCache elapsed: {:?}. hit ratio: {}",
        now.elapsed(),
        hit_count as f64 / upper_bound as f64
    );

    // // Foyer
    // let cache = foyer::CacheBuilder::new(cache_cap)
    //     .with_eviction_config(foyer::EvictionConfig::S3Fifo(foyer::S3FifoConfig::default()))
    //     .build();

    // let mut hit_count = 0;
    // let now = std::time::Instant::now();
    // for &key in keys.iter() {
    //     if cache.get(&key).is_some() {
    //         hit_count += 1;
    //     } else {
    //         cache.insert(key, ());
    //     }
    // }

    // println!(
    //     "Foyer elapsed: {:?}. hit ratio: {}",
    //     now.elapsed(),
    //     hit_count as f64 / upper_bound as f64
    // );
}
