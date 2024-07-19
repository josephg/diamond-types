#[cfg(feature = "stats")]
use std::cell::RefCell;

#[cfg(feature = "stats")]
thread_local! {
    static CACHE_HITS: RefCell<usize> = RefCell::default();
    static CACHE_MISSES: RefCell<usize> = RefCell::default();
    static AS: RefCell<usize> = RefCell::default();
    static BS: RefCell<usize> = RefCell::default();
    static CS: RefCell<usize> = RefCell::default();
}

pub(crate) fn cache_hit() {
    #[cfg(feature = "stats")] {
        let old_val = CACHE_HITS.take();
        CACHE_HITS.set(old_val + 1);
    }
}

pub(crate) fn cache_miss() {
    #[cfg(feature = "stats")] {
        let old_val = CACHE_MISSES.take();
        CACHE_MISSES.set(old_val + 1);
    }
}

pub(crate) fn marker_a() {
    #[cfg(feature = "stats")] {
        let old_val = AS.take();
        AS.set(old_val + 1);
    }
}
pub(crate) fn marker_b() {
    #[cfg(feature = "stats")] {
        let old_val = BS.take();
        BS.set(old_val + 1);
    }
}
pub(crate) fn marker_c() {
    #[cfg(feature = "stats")] {
        let old_val = CS.take();
        CS.set(old_val + 1);
    }
}

/// Returns (cache hits, cache misses).
pub fn take_stats() -> (usize, usize) {
    #[cfg(feature = "stats")] {
        let (a, b, c) = (AS.take(), BS.take(), CS.take());
        if a != 0 || b != 0 || c != 0 {
            println!("A: {a} / B: {b} / C: {c}");
        }
        
        (CACHE_HITS.take(), CACHE_MISSES.take())
    }

    #[cfg(not(feature = "stats"))] {
        (0, 0)
    }
}