//! This module provides a replacement allocator for use in testing code, so we can trace & track
//! memory allocations in tests.
//!
//! This only used when compiled with the `memusage` feature flag is enabled.
//!
//! This code is not part of the standard diamond types API surface. It will be removed at some
//! point (or moved into testing code). DO NOT DEPEND ON THIS.
//!
//! TODO: Make this not public (or move it into a private module).

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::RefCell;

#[derive(Debug, Clone, Copy, Default)]
pub struct AllocStats {
    pub num_allocations: usize,
    pub current_allocated_bytes: usize,
    pub peak_allocated_bytes: usize,
}

thread_local! {
    // Pair of (num allocations, total bytes allocated).
    static ALLOCATED: RefCell<AllocStats> = RefCell::default();
}
// pub static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

pub struct TracingAlloc;

unsafe impl GlobalAlloc for TracingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // println!("{}", std::backtrace::Backtrace::force_capture());
        let ret = System.alloc(layout);
        if !ret.is_null() {
            // ALLOCATED.fetch_add(layout.size(), Ordering::AcqRel);
            ALLOCATED.with(|s| {
                let mut r = s.borrow_mut();
                r.num_allocations += 1;
                r.current_allocated_bytes += layout.size();
                r.peak_allocated_bytes = r.peak_allocated_bytes.max(r.current_allocated_bytes);
            });
        }
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // ALLOCATED.fetch_sub(layout.size(), Ordering::AcqRel);
        ALLOCATED.with_borrow_mut(|r| {
            r.num_allocations -= 1;
            // It should be impossible to wrap, but since this is debugging code we'll silently
            // ignore if that happens.
            r.current_allocated_bytes = r.current_allocated_bytes.saturating_sub(layout.size());
        });
        System.dealloc(ptr, layout);
    }

    // Eh, would be better to implement this but it'd be easy to mess this up.
    // unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
    // }
}

#[allow(unused)]
pub fn get_thread_num_allocations() -> usize {
    ALLOCATED.with(|s| {
        s.borrow().num_allocations
    })
}

#[allow(unused)]
pub fn get_thread_memory_usage() -> usize {
    ALLOCATED.with(|s| {
        s.borrow().current_allocated_bytes
    })
}

#[allow(unused)]
pub fn get_peak_memory_usage() -> usize {
    ALLOCATED.with(|s| {
        s.borrow().peak_allocated_bytes
    })
}

#[allow(unused)]
pub fn reset_peak_memory_usage() {
    ALLOCATED.with_borrow_mut(|s| {
        s.peak_allocated_bytes = s.current_allocated_bytes
    });
}

// #[derive(Debug, Clone, Copy, Serialize)]
// #[derive(Debug, Clone, Copy)]
// pub struct MemUsage {
//     steady_state: usize,
//     peak: usize,
// }

// Returns (peak memory, resulting memory usage, R).
pub fn measure_memusage<F: FnOnce() -> R, R>(f: F) -> (usize, usize, R) {
    let before = get_thread_memory_usage();
    reset_peak_memory_usage();

    let result = f();

    (
        get_peak_memory_usage() - before,
        get_thread_memory_usage() - before,
        result
    )
}

#[cfg(any(test, feature = "memusage"))]
mod trace_alloc {
    use super::TracingAlloc;

    #[global_allocator]
    static A: TracingAlloc = TracingAlloc;
}