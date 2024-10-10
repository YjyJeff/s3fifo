//! Implementation of the `S3FIFO` algorithm described in [paper]
//!
//! [paper]: https://dl.acm.org/doi/10.1145/3600006.3613147

use std::collections::VecDeque;
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash};
use std::mem;
use std::ptr::NonNull;

use hashbrown::hash_table::HashTable;
use hashbrown::DefaultHashBuilder;

type HashValue = u64;

/// A non-thread safe `S3FIFO` cache
pub struct S3FIFO<K, V, S = DefaultHashBuilder> {
    hash_builder: S,
    small_fifo: VecDeque<Bucket<K, V>>,
    main_fifo: VecDeque<Bucket<K, V>>,
    ghost_fifo: GhostFIFOCache,
    table: HashTable<NonNull<Bucket<K, V>>>,
}

impl<K, V> S3FIFO<K, V, DefaultHashBuilder>
where
    K: Eq + Hash + Debug,
{
    /// Create a new `S3FIFO`
    pub fn new(cap: usize) -> Self {
        Self::with_hasher(cap, DefaultHashBuilder::default())
    }
}

impl<K, V, S> S3FIFO<K, V, S>
where
    K: Eq + Hash + Debug,
    S: BuildHasher,
{
    /// Create a new empty `S3FIFO` with hash builder
    pub fn with_hasher(cap: usize, hash_builder: S) -> Self {
        let small_size = cap / 10;
        let main_size = cap * 9 / 10;
        let ghost_size = main_size;
        S3FIFO {
            hash_builder,
            small_fifo: VecDeque::with_capacity(small_size),
            main_fifo: VecDeque::with_capacity(main_size),
            ghost_fifo: GhostFIFOCache::new(ghost_size),
            table: HashTable::with_capacity(cap),
        }
    }

    /// Get the value with given key
    pub fn get(&mut self, k: &K) -> Option<&V> {
        let hash = self.hash_builder.hash_one(k);
        self.table
            .find_mut(hash, |probe_bucket| unsafe {
                (probe_bucket.as_ref().key).eq(k)
            })
            .map(|element| unsafe {
                element.as_mut().incr_freq();
                &element.as_mut().value
            })
    }

    /// Get the mutable reference with given key
    pub fn get_mut(&mut self, k: &K) -> Option<&mut V> {
        let hash = self.hash_builder.hash_one(k);
        self.table
            .find_mut(hash, |probe_bucket| unsafe {
                (probe_bucket.as_ref().key).eq(k)
            })
            .map(|element| unsafe {
                element.as_mut().incr_freq();
                &mut element.as_mut().value
            })
    }

    /// Put the key-value pair into the cache. If the cache is has this key present
    /// the value is updated and return `Some(old)`
    pub fn put(&mut self, k: K, v: V) -> Option<V> {
        if let Some(old) = self.get_mut(&k) {
            return Some(mem::replace(old, v));
        }

        let hash = self.hash_builder.hash_one(&k);

        if self.ghost_fifo.contains(hash) {
            if self.main_fifo.len() == self.main_fifo.capacity() {
                self.evict_main();
            }
            let bucket = Bucket {
                key: k,
                value: v,
                freq: 0,
                hash,
            };
            self.main_fifo.push_back(bucket);
            let ptr: NonNull<Bucket<K, V>> = self.main_fifo.back().unwrap().into();
            self.table
                .insert_unique(hash, ptr, |bucket| unsafe { bucket.as_ref().hash });
        } else {
            if self.small_fifo.len() == self.small_fifo.capacity() {
                self.evict_small();
            }
            let bucket = Bucket {
                key: k,
                value: v,
                freq: 0,
                hash,
            };
            self.small_fifo.push_back(bucket);
            let ptr: NonNull<Bucket<K, V>> = self.small_fifo.back().unwrap().into();
            self.table
                .insert_unique(hash, ptr, |bucket| unsafe { bucket.as_ref().hash });
        }

        None
    }

    #[inline]
    fn evict_small(&mut self) {
        unsafe {
            while let Some(mut evicted_bucket) = self.small_fifo.pop_front() {
                let freq = evicted_bucket.freq.saturating_sub(1);
                let hash = evicted_bucket.hash;
                if freq > 0 {
                    evicted_bucket.freq = freq;
                    if self.main_fifo.len() == self.main_fifo.capacity() {
                        self.evict_main();
                    }
                    self.main_fifo.push_back(evicted_bucket);
                    let ptr: NonNull<Bucket<K, V>> = self.main_fifo.back().unwrap().into();
                    // Update the ptr in the table, because it is in main FIFO now.
                    // The old ptr is invalid now
                    match self.table.find_entry(hash, |probe_bucket| {
                        (probe_bucket.as_ref().key).eq(&ptr.as_ref().key)
                    }) {
                        Ok(mut entry) => {
                            let v = entry.get_mut();
                            *v = ptr;
                        }
                        Err(_) => unreachable!("Key in main FIFO must in table"),
                    }
                } else {
                    self.ghost_fifo.insert(hash);
                    match self.table.find_entry(evicted_bucket.hash, |probe_bucket| {
                        (probe_bucket.as_ref().key).eq(&evicted_bucket.key)
                    }) {
                        Ok(entry) => {
                            entry.remove();
                            return;
                        }
                        Err(_) => unreachable!("Key in small FIFO must in table"),
                    }
                }
            }
        }
    }

    #[inline]
    fn evict_main(&mut self) {
        unsafe {
            while let Some(mut evicted_bucket) = self.main_fifo.pop_front() {
                let freq = evicted_bucket.freq.saturating_sub(1);
                if freq > 0 {
                    evicted_bucket.freq = freq;
                    let hash = evicted_bucket.hash;
                    // Insert back to main
                    self.main_fifo.push_back(evicted_bucket);
                    let ptr: NonNull<Bucket<K, V>> = self.main_fifo.back().unwrap().into();
                    // Update the ptr in the table, because it changes its location in the main FIFO.
                    // The old ptr is invalid now
                    match self.table.find_entry(hash, |probe_bucket| {
                        (probe_bucket.as_ref().key).eq(&ptr.as_ref().key)
                    }) {
                        Ok(mut entry) => {
                            let v = entry.get_mut();
                            *v = ptr;
                        }
                        Err(_) => unreachable!("Key in main FIFO must in table"),
                    }
                } else {
                    match self.table.find_entry(evicted_bucket.hash, |probe_bucket| {
                        (probe_bucket.as_ref().key).eq(&evicted_bucket.key)
                    }) {
                        Ok(entry) => {
                            entry.remove();
                            return;
                        }
                        Err(_) => unreachable!("Key in main FIFO must in table"),
                    }
                }
            }
        }
    }
}

/// TBD: Should we store the hash value? Or should we recompute it?
struct Bucket<K, V> {
    /// Key
    key: K,
    /// Value
    value: V,
    /// Frequency
    freq: u8,
    /// Hash value of the key, used to avoid recomputing the hash value
    hash: HashValue,
}

impl<K, V> Bucket<K, V> {
    #[inline]
    fn incr_freq(&mut self) {
        self.freq = (self.freq + 1) & 3;
    }
}

/// A ghost fifo cache that only contains the hash
///
/// FIXME: Redundant HashValue
struct GhostFIFOCache {
    /// Hash table for fast look up
    table: HashTable<HashValue>,
    /// FIFO queue contains the pointer to the map. The lifetime of the pointer is the
    /// lifetime of the K in the map. It is used to preserve the FIFO order of the map
    ring_buffer: VecDeque<HashValue>,
}

impl GhostFIFOCache {
    fn new(cap: usize) -> Self {
        Self {
            table: HashTable::with_capacity(cap),
            ring_buffer: VecDeque::with_capacity(cap),
        }
    }

    #[inline]
    fn contains(&self, hash: HashValue) -> bool {
        self.table.find(hash, |&probe| probe == hash).is_some()
    }

    #[inline]
    fn insert(&mut self, hash: HashValue) {
        if self.contains(hash) {
            return;
        }

        if self.ring_buffer.len() == self.ring_buffer.capacity() {
            // full
            let garbage_hash = self.ring_buffer.pop_front().unwrap();
            let entry = self
                .table
                .find_entry(garbage_hash, |&probe| probe == garbage_hash)
                .unwrap();
            entry.remove();
        }

        self.ring_buffer.push_back(hash);

        self.table.insert_unique(hash, hash, |&probe| probe);
    }
}
