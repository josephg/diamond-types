// use std::sync::atomic::{AtomicUsize, Ordering};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::RefCell;

thread_local! {
    // Pair of (num allocations, total bytes allocated).
    static ALLOCATED: RefCell<(usize, isize)> = RefCell::new((0, 0));
}
// pub static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

pub struct TracingAlloc;

unsafe impl GlobalAlloc for TracingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);
        if !ret.is_null() {
            // ALLOCATED.fetch_add(layout.size(), Ordering::AcqRel);
            ALLOCATED.with(|s| {
                let mut r = s.borrow_mut();
                r.0 += 1;
                r.1 += layout.size() as isize;
            });
        }
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // ALLOCATED.fetch_sub(layout.size(), Ordering::AcqRel);
        ALLOCATED.with(|s| {
            s.borrow_mut().1 -= layout.size() as isize;
        });
        System.dealloc(ptr, layout);
    }

    // Eh, would be better to implement this but it'd be easy to mess this up.
    // unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
    // }
}

pub fn get_thread_num_allocations() -> usize {
    ALLOCATED.with(|s| {
        s.borrow().0
    })
}

pub fn get_thread_memory_usage() -> isize {
    ALLOCATED.with(|s| {
        s.borrow().1
    })
}

#[cfg(test)]
mod trace_alloc {
    use crate::alloc::TracingAlloc;

    #[global_allocator]
    static A: TracingAlloc = TracingAlloc;
}