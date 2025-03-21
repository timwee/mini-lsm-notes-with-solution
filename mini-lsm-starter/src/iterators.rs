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

pub mod concat_iterator;
pub mod merge_iterator;
pub mod two_merge_iterator;

pub trait StorageIterator {
    /// The type of the key used by this iterator, defined as a Generic Associated Type (GAT).
    /// GATs allow us to define types that can be different for each implementation while also
    /// containing lifetime parameters. This is useful for iterators that may return references
    /// with different lifetimes.
    ///
    /// The type must implement PartialEq, Eq, PartialOrd and Ord to allow key comparisons.
    ///
    /// The `where Self: 'a` bound indicates that the implementing type must live at least as
    /// long as the lifetime 'a. This ensures that any references in KeyType<'a> cannot outlive
    /// the iterator itself.
    type KeyType<'a>: PartialEq + Eq + PartialOrd + Ord
    where
        Self: 'a;

    /// Get the current value.
    fn value(&self) -> &[u8];

    /// Get the current key.
    fn key(&self) -> Self::KeyType<'_>;

    /// Check if the current iterator is valid.
    fn is_valid(&self) -> bool;

    /// Move to the next position.
    fn next(&mut self) -> anyhow::Result<()>;

    /// Number of underlying active iterators for this iterator.
    fn num_active_iterators(&self) -> usize {
        1
    }
}
