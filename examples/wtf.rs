//! Wtf grow?

use std::hash::BuildHasher;

use hashbrown::hash_map::DefaultHashBuilder;
use hashbrown::raw::RawTable;
use rand::Rng;

const CAP: usize = 10;

fn main() {
    let mut table = RawTable::<u64>::with_capacity(CAP);
    let hash_builder = DefaultHashBuilder::default();
    let mut prev = 0;
    let mut rng = rand::thread_rng();
    loop {
        let val = rng.gen::<u64>();
        let hash = hash_builder.hash_one(val);
        if table.find(hash, |&probe| probe == val).is_none() {
            if table.len() == CAP {
                table.remove_entry(hash, |&probe| probe == prev);
            }
            table.try_insert_no_grow(hash, val).unwrap();
        }
        prev = val;
    }
}
