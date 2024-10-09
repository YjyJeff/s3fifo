//! Implementation of the `S3FIFO` algorithm described in [paper]
//!
//! [paper]: https://dl.acm.org/doi/10.1145/3600006.3613147

use std::collections::VecDeque;
use std::hash::{BuildHasher, Hash};
use std::mem;

use hashbrown::hash_table::HashTable;
use hashbrown::DefaultHashBuilder;

type HashValue = u64;

/// A non-thread safe `S3FIFO` cache
pub struct S3FIFO<K, V, S = DefaultHashBuilder> {
    hash_builder: S,
    small_fifo: FIFOCache<K, V>,
    main_fifo: FIFOCache<K, V>,
    ghost_fifo: GhostFIFOCache,
}

impl<K, V> S3FIFO<K, V, DefaultHashBuilder>
where
    K: Eq + Hash,
{
    /// Create a new `S3FIFO`
    pub fn new(cap: usize) -> Self {
        Self::with_hasher(cap, DefaultHashBuilder::default())
    }
}

impl<K, V, S> S3FIFO<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Create a new empty `S3FIFO` with hash builder
    pub fn with_hasher(cap: usize, hash_builder: S) -> Self {
        let small_size = cap / 10;
        let main_size = cap * 9 / 10;
        let ghost_size = main_size;
        S3FIFO {
            hash_builder,
            small_fifo: FIFOCache::new(small_size),
            main_fifo: FIFOCache::new(main_size),
            ghost_fifo: GhostFIFOCache::new(ghost_size),
        }
    }

    /// Get the value with given key
    pub fn get(&mut self, k: &K) -> Option<&V> {
        let hash = self.hash_builder.hash_one(k);
        if let Some(value) = self.small_fifo.get(k, hash) {
            Some(value)
        } else if let Some(value) = self.main_fifo.get(k, hash) {
            Some(value)
        } else {
            None
        }
    }

    /// Get the mutable reference with given key
    pub fn get_mut(&mut self, k: &K) -> Option<&mut V> {
        let hash = self.hash_builder.hash_one(k);
        if let Some(value) = self.small_fifo.get_mut(k, hash) {
            Some(value)
        } else if let Some(value) = self.main_fifo.get_mut(k, hash) {
            Some(value)
        } else {
            None
        }
    }

    /// Put the key-value pair into the cache. If the cache is has this key present
    /// the value is updated and return `Some(old)`
    pub fn put(&mut self, k: K, v: V) -> Option<V> {
        if let Some(old) = self.get_mut(&k) {
            return Some(mem::replace(old, v));
        }

        let hash = self.hash_builder.hash_one(&k);

        if self.ghost_fifo.contains(hash) {
            if self.main_fifo.is_full() {
                self.evict_main();
            }
            self.main_fifo.insert(k, v, 0, hash, &self.hash_builder);
        } else {
            if self.small_fifo.is_full() {
                self.evict_small();
            }
            self.small_fifo.insert(k, v, 0, hash, &self.hash_builder);
        }

        None
    }

    #[inline]
    fn evict_small(&mut self) {
        while let Some((k, v, freq, hash)) = self.small_fifo.pop() {
            if freq > 0 {
                if self.main_fifo.is_full() {
                    self.evict_main();
                }
                self.main_fifo.insert(k, v, freq, hash, &self.hash_builder);
            } else {
                self.ghost_fifo.insert(hash);
                return;
            }
        }
    }

    #[inline]
    fn evict_main(&mut self) {
        while let Some((k, v, freq, hash)) = self.main_fifo.pop() {
            if freq > 0 {
                self.main_fifo.insert(k, v, freq, hash, &self.hash_builder);
            } else {
                return;
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
