//! Implementation of the `S3FIFO` algorithm described in [paper]
//!
//! [paper]: https://dl.acm.org/doi/10.1145/3600006.3613147

use std::collections::VecDeque;
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash};
use std::mem;

use hashbrown::hash_table::HashTable;
use hashbrown::DefaultHashBuilder;

type HashValue = u64;

/// A non-thread safe `S3FIFO` cache
pub struct S3FIFO<K, V, S = DefaultHashBuilder> {
    hash_builder: S,
    small_fifo: VecDeque<(K, HashValue)>,
    main_fifo: VecDeque<(K, HashValue)>,
    ghost_fifo: GhostFIFOCache,
    table: HashTable<Bucket<K, V>>,
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
            table: HashTable::with_capacity(10 * cap),
        }
    }

    /// Get the value with given key
    pub fn get(&mut self, k: &K) -> Option<&V> {
        let hash = self.hash_builder.hash_one(k);
        self.table
            .find_mut(hash, |probe_bucket| unsafe {
                (*probe_bucket.key_ptr).eq(k)
            })
            .map(|element| {
                element.incr_freq();
                &element.value
            })
    }

    /// Get the mutable reference with given key
    pub fn get_mut(&mut self, k: &K) -> Option<&mut V> {
        let hash = self.hash_builder.hash_one(k);
        self.table
            .find_mut(hash, |probe_bucket| unsafe {
                (*probe_bucket.key_ptr).eq(k)
            })
            .map(|element| {
                element.incr_freq();
                &mut element.value
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
            println!("Ghost");
            if self.main_fifo.len() == self.main_fifo.capacity() {
                println!("Ghost len: {}", self.ghost_fifo.ring_buffer.len());
                self.assert_main_fifo();
                self.evict_main();
                println!("After evict main");
                self.assert_main_fifo();
            }
            self.main_fifo.push_back((k, hash));
            let key_ptr: *const K = &self.main_fifo.back().unwrap().0;
            self.table.insert_unique(
                hash,
                Bucket {
                    key_ptr,
                    value: v,
                    freq: 0,
                },
                |bucket| self.hash_builder.hash_one(unsafe { &*bucket.key_ptr }),
            );
        } else {
            if self.small_fifo.len() == self.small_fifo.capacity() {
                println!("Before evict small");
                self.assert_main_fifo();
                self.evict_small();
                println!("After evict small");
                self.assert_main_fifo();
            }
            assert!(self.small_fifo.len() != self.small_fifo.capacity());
            unsafe {
                assert!(self
                    .table
                    .find(hash, |probe_bucket| (*probe_bucket.key_ptr).eq(&k))
                    .is_none());
            }
            println!("aaaaa");
            self.assert_main_fifo();
            println!("bbbbbb");
            println!("key: {:?}, Hash: {}", k, hash);
            self.small_fifo.push_back((k, hash));
            let key_ptr: *const K = &self.small_fifo.back().unwrap().0;
            println!("Small size: {}", self.small_fifo.len());
            println!("Main size: {}", self.main_fifo.len());
            println!("table len: {}", self.table.len());
            for e in self.main_fifo.iter() {
                print!("e.key: {:?},e.hash in main: {}", e.0, e.1);
            }
            println!();
            self.table.insert_unique(
                hash,
                Bucket {
                    key_ptr,
                    value: v,
                    freq: 0,
                },
                |bucket| {
                    println!("Rehash");
                    self.hash_builder.hash_one(unsafe { &*bucket.key_ptr })
                },
            );
            println!("After Insert");
            println!("table len: {}", self.table.len());
            // WTF? After insert, we can not find main in the hashmap
            self.assert_main_fifo();
            println!("llll")
        }

        None
    }

    #[inline]
    fn evict_small(&mut self) {
        println!("Evict small");
        unsafe {
            while let Some((key, hash)) = self.small_fifo.pop_front() {
                match self
                    .table
                    .find_entry(hash, |probe_bucket| (*probe_bucket.key_ptr).eq(&key))
                {
                    Ok(mut entry) => {
                        let bucket_mut = entry.get_mut();
                        let freq = bucket_mut.freq.saturating_sub(1);
                        bucket_mut.freq = freq;
                        if freq > 0 {
                            if self.main_fifo.len() == self.main_fifo.capacity() {
                                self.evict_main();
                                println!("After evict main in evict_small");
                                self.assert_main_fifo();
                            }
                            self.main_fifo.push_back((key, hash));
                        } else {
                            println!("Before Remove");
                            // self.assert_main_fifo();
                            entry.remove();
                            self.assert_main_fifo();

                            self.ghost_fifo.insert(hash);
                            return;
                        }
                    }
                    Err(_) => unreachable!("Key in small FIFO must in table"),
                }
            }
        }
    }

    #[inline]
    fn evict_main(&mut self) {
        unsafe {
            while let Some((key, hash)) = self.main_fifo.pop_front() {
                match self
                    .table
                    .find_entry(hash, |probe_bucket| (*probe_bucket.key_ptr).eq(&key))
                {
                    Ok(mut entry) => {
                        let bucket_mut = entry.get_mut();
                        let freq = bucket_mut.freq.saturating_sub(1);
                        bucket_mut.freq = freq;
                        if freq > 0 {
                            self.main_fifo.push_back((key, hash));
                        } else {
                            entry.remove();
                            return;
                        }
                    }
                    Err(_) => unreachable!("Key in main FIFO must in table"),
                }
            }
        }
    }

    fn assert_main_fifo(&self) {
        for e in self.main_fifo.iter() {
            unsafe {
                assert!(self
                    .table
                    .find(e.1, |probe_bucket| (*probe_bucket.key_ptr).eq(&e.0))
                    .is_some());
            }
        }
    }
}

struct FIFOCache<K, V> {
    /// Hash table for fast look up
    table: HashTable<Bucket<K, V>>,
    /// FIFO queue contains the key in the map. It is used to preserve the FIFO order of
    /// the map
    ring_buffer: VecDeque<(K, HashValue)>,
}

impl<K, V> FIFOCache<K, V>
where
    K: Eq + Hash,
{
    fn new(cap: usize) -> Self {
        Self {
            table: HashTable::with_capacity(cap),
            ring_buffer: VecDeque::with_capacity(cap),
        }
    }

    #[inline]
    fn get(&mut self, k: &K, hash: HashValue) -> Option<&V> {
        self.table
            .find_mut(hash, |probe_bucket| unsafe {
                (*probe_bucket.key_ptr).eq(k)
            })
            .map(|element| {
                element.incr_freq();
                &element.value
            })
    }

    #[inline]
    fn get_mut(&mut self, k: &K, hash: HashValue) -> Option<&mut V> {
        self.table
            .find_mut(hash, |probe_bucket| unsafe {
                (*probe_bucket.key_ptr).eq(k)
            })
            .map(|element| {
                element.incr_freq();
                &mut element.value
            })
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.ring_buffer.len() == self.ring_buffer.capacity()
    }

    #[inline]
    fn insert<S>(&mut self, k: K, v: V, freq: u8, hash: HashValue, hash_builder: &S)
    where
        S: BuildHasher,
    {
        debug_assert!(!self.is_full());
        match self.table.find(hash, |probe_bucket| unsafe {
            (*probe_bucket.key_ptr).eq(&k)
        }) {
            Some(_) => {
                unreachable!(
                    "Should not insert an already present key in to the FIFOCache, use `get_mut` before insert to update the frequency"
                );
            }
            None => {
                self.ring_buffer.push_back((k, hash));
                let key_ptr: *const K = &self.ring_buffer.back().unwrap().0;
                self.table.insert_unique(
                    hash,
                    Bucket {
                        key_ptr,
                        value: v,
                        freq,
                    },
                    |bucket| hash_builder.hash_one(unsafe { &*bucket.key_ptr }),
                );
            }
        }
    }

    #[inline]
    fn pop(&mut self) -> Option<(K, V, u8, HashValue)> {
        self.ring_buffer.pop_front().map(|(key, hash)| unsafe {
            let entry = self
                .table
                .find_entry(hash, |probe_bucket| (*probe_bucket.key_ptr).eq(&key))
                .unwrap_unchecked();

            let (bucket, _) = entry.remove();
            (key, bucket.value, bucket.freq.saturating_sub(1), hash)
        })
    }
}

/// TBD: Should we store the hash value? Or should we recompute it?
struct Bucket<K, V> {
    /// Pointer to the key in the fifo. We store the pointer here because the raw table
    /// may perform rehashing when lots of deletion are performed. However, the fifo
    /// queue guarantees the reallocation is never performed.
    key_ptr: *const K,
    /// Value
    value: V,
    /// Frequency
    freq: u8,
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
