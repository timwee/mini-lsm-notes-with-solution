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

use std::cmp::{self};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        let mut heap = BinaryHeap::new();

        if iters.iter().all(|x| !x.is_valid()) {
            // All invalid, select the last one as the current.
            let mut iters = iters;
            return Self {
                iters: heap,
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            };
        }

        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        let current = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(current),
        }
    }
}

// for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>> - This is the more complex part:
// 1. for<'a> is a higher-ranked trait bound (HRTB) that says "for any lifetime 'a"
// 2. StorageIterator is the trait being implemented
// 3. <KeyType<'a> = KeySlice<'a>> is an associated type specification that says "the KeyType associated type for any lifetime 'a' must be KeySlice<'a>'"
// In simpler terms, this is saying:- The iterator type I must live for the entire program- For any lifetime 'a, I must implement StorageIterator where its key type is KeySlice<'a>
// This is a common pattern in Rust when working with iterators that need to handle different lifetimes for their keys. The for<'a> syntax allows the implementation to work with keys of any lifetime, while still maintaining type safety.
// A more concrete example might help:
// This would be valid for I
// struct MyIterator {
//     keys: Vec<KeySlice<'static>>,
// }
//
// // This would also be valid
// struct AnotherIterator<'a> {
//     keys: Vec<KeySlice<'a>>,
// }
//
// // But this would NOT be valid
// struct InvalidIterator {
//     keys: Vec<KeySlice<'a>>, // Error: 'a is not defined
// }
//
//
impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    // generic associated type (GAT) pattern
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            // as_ref to not consume/take ownership
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();
        // Pop the item out of the heap if they have the same value.
        // PeekMut is a wrapper around a reference to the top element of the heap.
        // It provides a way to peek at the top element without removing it.
        // It also provides a way to pop the top element from the heap.
        // Can dereference the inner_iter to get the actual iterator, because PeekMut acts like a smart pointer to the top element of the heap.
        while let Some(mut inner_iter) = self.iters.peek_mut() {
            debug_assert!(
                inner_iter.1.key() >= current.1.key(),
                "heap invariant violated"
            );
            if inner_iter.1.key() == current.1.key() {
                // Case 1: an error occurred when calling `next`.
                // The @ Err(_) syntax in if let e @ Err(_) = inner_iter.1.next() is a pattern matching feature in Rust that allows you to both match a pattern AND bind the entire matched value to a variable.
                // Let's break it down:
                // e @ binds the entire matched value to the variable e
                // Err(_) is the pattern we're matching against
                // The _ means we don't care about the specific error value inside the Err
                if let e @ Err(_) = inner_iter.1.next() {
                    PeekMut::pop(inner_iter);
                    return e;
                }

                // Case 2: iter is no longer valid.
                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            } else {
                break;
            }
        }

        current.1.next()?;

        // If the current iterator is invalid, pop it out of the heap and select the next one.
        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current = iter;
            }
            return Ok(());
        }

        // Otherwise, compare with heap top and swap if necessary.
        if let Some(mut inner_iter) = self.iters.peek_mut() {
            // why both `*`?
            // 1. current is a &HeapWrapper<I>, so need to dereference to access the HeapWrapper
            // 2. inner_iter is a PeekMut<HeapWrapper<I>>, so need to dereference from PeekMut to access the HeapWrapper
            if *current < *inner_iter {
                // inner_iter is a PeekMut<HeapWrapper<I>>, so need to dereference from PeekMut to access the HeapWrapper, then get a &mut to it.
                std::mem::swap(&mut *inner_iter, current);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters
            .iter()
            .map(|x| x.1.num_active_iterators())
            .sum::<usize>()
            + self
                .current
                .as_ref()
                .map(|x| x.1.num_active_iterators())
                .unwrap_or(0)
    }
}
