// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;

use ouroboros::self_referencing;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
/// This is particularly useful when working with the skip list implementation which expects owned Bytes rather than references
///
/// Efficiency:
///     This function is actually quite efficient for several reasons:
///     1. Zero-Copy for Unbounded:
///         When the bound is Unbounded, it's just a simple enum variant copy
///         No actual data copying occurs
///     2. Efficient Bytes Implementation:
///         Bytes::copy_from_slice is highly optimized
///         It uses reference counting internally
///         For small slices, it might use stack allocation
///         For larger slices, it uses efficient heap allocation
///     3. Rare Operation:
///         This function is typically only called when:
///             Setting up a range scan
///             Creating a new iterator
///         It's not called repeatedly in tight loops
///     4. The cost is amortized over the actual data access operations
///
/// Memory Safety:
///     The conversion is necessary for memory safety
///     The skip list needs owned data to ensure the data lives as long as needed
///     This is a one-time cost for the safety guarantees
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    // Bound::Included: For inclusive bounds (e.g., [start, end])
    // Bound::Excluded: For exclusive bounds (e.g., (start, end))
    // Bound::Unbounded: For unbounded ranges (e.g., .. or ..=end)
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(_id: usize) -> Self {
        Self {
            id: _id,
            map: Arc::new(SkipMap::new()),
            wal: None,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        unimplemented!()
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        unimplemented!()
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(lower, upper)
    }

    /// Get a value by key.
    pub fn get(&self, _key: &[u8]) -> Option<Bytes> {
        self.map.get(_key).map(|e| e.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let estimated_size = _key.len() + _value.len();
        self.map
            .insert(Bytes::copy_from_slice(_key), Bytes::copy_from_slice(_value));
        self.approximate_size
            .fetch_add(estimated_size, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, _lower: Bound<&[u8]>, _upper: Bound<&[u8]>) -> MemTableIterator {
        let (lower, upper) = (map_bound(_lower), map_bound(_upper));
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((lower, upper)),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        // with_iter_mut is likely a method that provides safe mutable access to the underlying iterator
        // It takes a closure that receives a mutable reference to the iterator
        // The closure calls iter.next() to get the next item
        // MemTableIterator::entry_to_item converts the raw entry into a proper item type
        let entry = iter.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        // with_mut is likely a method that provides safe mutable access to the iterator's state
        // It takes a closure that receives a mutable reference to the iterator's state
        // The closure assigns the new entry to x.item
        // This pattern is common when you need to:
        // 1. Safely access and modify internal state
        // 2. Ensure proper locking or safety invariants are maintained
        // 3. Provide controlled access to mutable data
        // The pattern is similar to how you might use a mutex or other synchronization primitive, but at a higher level of abstraction. It's a way to ensure that modifications to the iterator's state are done safely and consistently.
        iter.with_mut(|x| *x.item = entry);
        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, _builder: &mut SsTableBuilder) -> Result<()> {
        unimplemented!()
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
///
/// In standard Rust, you cannot easily create a struct that contains references to its own fields because of the borrow checker rules. For example, this wouldn't work:
/// struct Problematic {
///     data: Vec<u8>,
///     reference: &Vec<u8>, // Can't reference data field!
/// }
/// ouroboros is a crate that allows you to create self-referential structs.
///
/// In the case below:
/// struct MemTableIterator {
///     map: SkipMap<...>,          // The owned data
///     iter: SkipMapRangeIter<'??>, // Iterator borrowing from map
/// }
///
/// Without the self-referential machinery and these attributes:
/// - We couldn't express that iter borrows from map in the same struct
/// - We might run into lifetime issues where the iterator could outlive the data it's iterating over
/// The combination of self_referencing, borrows, and not_covariant allows us to safely create and use iterators that reference the struct's own data while maintaining Rust's memory safety guarantees.
///
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    /// We want to avoid putting a lifetime on `MemTableIterator` itself
    ///   that's why we have the underlying skipmap as a member here.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl MemTableIterator {
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> KeySlice {
        KeySlice::from_slice(&self.borrow_item().0[..])
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}
