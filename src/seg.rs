//! Bucket lists in recycled memory.
//!
//! Every pass reads bucket lists written by the previous pass and writes new
//! ones. All lists are made of fixed-size segments from one pool; a list that
//! has been read is given back, so the next pass mostly writes into memory
//! that is already paged in instead of asking the OS for fresh pages (a page
//! fault each). Records never move once stored, so their address can be kept.
//! Segments are never returned to the OS (the process exits without freeing).

use std::alloc::{Layout, alloc};
use std::sync::Mutex;

/// Segment size in bytes: 768 records of 32 bytes or 512 of 48 bytes.
pub const SEG: usize = 24 << 10;
const ALIGN: usize = 64;
/// Fresh segments are allocated this many at a time.
const BATCH: usize = 64;

pub struct SegPool {
    free: Mutex<Vec<usize>>,
}

impl SegPool {
    pub fn new() -> Self {
        SegPool { free: Mutex::new(Vec::new()) }
    }

    /// A free segment (SEG bytes, 64-byte aligned).
    pub fn take(&self) -> *mut u8 {
        let mut free = self.free.lock().unwrap();
        if let Some(a) = free.pop() {
            return a as *mut u8;
        }
        let layout = Layout::from_size_align(SEG * BATCH, ALIGN).unwrap();
        // SAFETY: non-zero size; the memory is leaked on purpose.
        let p = unsafe { alloc(layout) };
        assert!(!p.is_null(), "out of memory");
        let base = p as usize;
        free.extend((1..BATCH).map(|i| base + i * SEG));
        p
    }
}

/// An append-only list of `T` stored in pool segments.
pub struct SegList<T> {
    done: Vec<(*const T, usize)>,
    cur: *mut T,
    n: usize,
    cap: usize,
    total: usize,
}

// SAFETY: a SegList owns its segments exclusively; T is plain data.
unsafe impl<T: Send> Send for SegList<T> {}
unsafe impl<T: Sync> Sync for SegList<T> {}

impl<T: Copy> SegList<T> {
    const PER_SEG: usize = SEG / std::mem::size_of::<T>();

    pub fn new() -> Self {
        const { assert!(std::mem::align_of::<T>() <= ALIGN && SEG / std::mem::size_of::<T>() > 0) };
        SegList { done: Vec::new(), cur: std::ptr::null_mut(), n: 0, cap: 0, total: 0 }
    }

    /// Append `x`; returns where it was stored, which stays valid (segments
    /// never move) until the list is recycled.
    #[inline(always)]
    pub fn push(&mut self, pool: &SegPool, x: T) -> *mut T {
        if self.n == self.cap {
            self.next_seg(pool);
        }
        // SAFETY: n < cap, and cur points to a segment of cap elements.
        let p = unsafe { self.cur.add(self.n) };
        unsafe { p.write(x) };
        self.n += 1;
        p
    }

    /// Give this list's segments back to the pool.
    pub fn recycle(self, pool: &SegPool) {
        let mut free = pool.free.lock().unwrap();
        free.extend(self.done.iter().map(|&(p, _)| p as usize));
        if self.cap > 0 {
            free.push(self.cur as usize);
        }
    }

    #[cold]
    #[inline(never)]
    fn next_seg(&mut self, pool: &SegPool) {
        if self.cap > 0 {
            self.done.push((self.cur, self.n));
            self.total += self.n;
        }
        self.cur = pool.take() as *mut T;
        self.cap = Self::PER_SEG;
        self.n = 0;
    }

    pub fn len(&self) -> usize {
        self.total + self.n
    }

    /// The stored elements, one slice per segment, in push order.
    pub fn slices(&self) -> impl Iterator<Item = &[T]> {
        let cur = (self.cap > 0).then_some((self.cur as *const T, self.n));
        // SAFETY: each (ptr, len) is an initialized run inside a live segment.
        self.done.iter().copied().chain(cur).map(|(p, n)| unsafe { std::slice::from_raw_parts(p, n) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seglist_keeps_order_and_recycles() {
        let pool = SegPool::new();
        // recycled segments from a used list
        let mut old: SegList<u64> = SegList::new();
        for i in 0..100_000u64 {
            old.push(&pool, i);
        }
        old.recycle(&pool);
        let mut a: SegList<[u64; 4]> = SegList::new();
        let mut b: SegList<(u128, u128, i64, u32)> = SegList::new();
        for i in 0..50_000u64 {
            a.push(&pool, [i, i + 1, i + 2, i + 3]);
            if i % 3 == 0 {
                b.push(&pool, (i as u128, !(i as u128), -(i as i64), i as u32));
            }
        }
        assert_eq!(a.len(), 50_000);
        let got: Vec<[u64; 4]> = a.slices().flat_map(|s| s.iter().copied()).collect();
        assert!(got.iter().enumerate().all(|(i, x)| *x == [i as u64, i as u64 + 1, i as u64 + 2, i as u64 + 3]));
        let got: Vec<_> = b.slices().flat_map(|s| s.iter().copied()).collect();
        assert_eq!(got.len(), (0..50_000).filter(|i| i % 3 == 0).count());
        assert!(got.iter().zip((0..50_000u64).filter(|i| i % 3 == 0)).all(|(x, i)| *x == (i as u128, !(i as u128), -(i as i64), i as u32)));
        let empty: SegList<u64> = SegList::new();
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.slices().count(), 0);
    }
}
