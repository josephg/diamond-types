use diamond_types::list::ListOpLog;

fn main() {

    // const PAPER_DATASETS: &[&str] = &["C1"];
    for name in &["S1", "S2", "S3", "C1", "C2", "A1", "A2"] {
        let bytes = std::fs::read(format!("paper_benchmark_data/{name}.dt")).unwrap();

        #[cfg(feature = "memusage")]
            let (start_bytes, start_count) = {
            reset_peak_memory_usage();
            (get_thread_memory_usage(), get_thread_num_allocations())
        };

        {
            // get_txns_from_file
            let oplog = ListOpLog::load_from(&bytes).unwrap();
            let _state = oplog.checkout_tip().into_inner();
        }

        #[cfg(feature = "memusage")]
        println!("{name}: allocated {} bytes in {} blocks, peak usage {}",
                 format_size((get_thread_memory_usage() - start_bytes) as usize, DECIMAL),
                 get_thread_num_allocations() - start_count,
                 get_peak_memory_usage() - start_bytes
        );
    }

}