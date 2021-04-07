#![feature(allocator_api)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::alloc::{GlobalAlloc, Layout, System};

pub static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

pub struct TracingAlloc;

unsafe impl GlobalAlloc for TracingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);
        if !ret.is_null() {
            ALLOCATED.fetch_add(layout.size(), Ordering::AcqRel);
        }
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        ALLOCATED.fetch_sub(layout.size(), Ordering::AcqRel);
        System.dealloc(ptr, layout);
    }

    // Eh, would be better to implement this but it'd be easy to mess this up.
    // unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
    // }
}


#[cfg(test)]
mod trace_alloc {
    use crate::alloc::TracingAlloc;

    #[global_allocator]
    static A: TracingAlloc = TracingAlloc;
}