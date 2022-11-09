use smallvec::smallvec;
use crate::{CausalGraph, Parents, Frontier};
use crate::frontier::{clone_smallvec, debug_assert_frontier_sorted};

impl CausalGraph {
    #[allow(unused)]
    pub fn dbg_check(&self, deep: bool) {
        if deep {
            self.parents.dbg_check();
        }

        // The client_with_localtime should match with the corresponding items in client_data
        self.client_with_localtime.check_packed();

        for pair in self.client_with_localtime.iter() {
            let expected_range = pair.range();

            let span = pair.1;
            let client = &self.client_data[span.agent as usize];
            let actual_range = client.item_times.find_packed_and_split(span.seq_range);

            assert_eq!(actual_range.1, expected_range);
        }

        assert_eq!(self.version, self.parents.dbg_get_frontier_inefficiently());

        if deep {
            // Also check the other way around.
            for (agent, client) in self.client_data.iter().enumerate() {
                for range in client.item_times.iter() {
                    let actual = self.client_with_localtime.find_packed_and_split(range.1);
                    assert_eq!(actual.1.agent as usize, agent);
                }
            }
        }
    }
}
