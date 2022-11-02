// WIP.

use crate::causalgraph::agent_span::AgentVersion;
use crate::list::ListOpLog;
use crate::LV;

impl ListOpLog {

    /// Sooo, when 2 peers love each other very much...
    ///
    /// They connect together. And they need to find the shared point in time from which they should
    /// send changes.
    ///
    /// Over the network this problem fundamentally pits round-trip time against bandwidth overhead.
    /// The algorithmic approach which would result in the fewest round-trips would just be for both
    /// peers to send their entire histories immediately. But this would waste bandwidth. And the
    /// approach using the least bandwidth would have peers essentially do a distributed binary
    /// search to find a common point in time. But this would take log(n) round-trips, and over long
    /// network distances this is really slow.
    ///
    /// In practice this is usually mostly unnecessary - usually one peer's version is a direct
    /// ancestor of the other peer's version. (Eg, I'm modifying a document and you're just
    /// observing it.)
    ///
    /// Ny design here is a hybrid approach. I'm going to construct a fixed-sized chunk of known
    /// versions we can send to our remote peer. (And the remote peer can do the same with us). The
    /// chunk will contain exponentially less information the further back in time we scan; so the
    /// more time which has passed since we have a common ancestor, the more wasted bytes of changes
    /// we'll send to the remote peer. But this approach will always only need 1RTT to sync.
    ///
    /// Its not perfect, but it'll do donkey. It'll do.
    #[allow(unused)]
    fn get_stochastic_version(&self, target_count: usize) -> Vec<AgentVersion> {
        // TODO: WIP.
        let target_count = target_count.max(self.version.len());
        let mut result = Vec::with_capacity(target_count + 10);

        let time_len = self.len();

        // If we have no changes, just return the empty set. Descending from ROOT is implied anyway.
        if time_len == 0 { return result; }

        let mut push_time = |t: LV| {
            result.push(self.cg.lv_to_agent_version(t));
        };

        // No matter what, we'll send the current frontier:
        for t in self.version.iter() {
            push_time(*t);
        }

        // So we want about target_count items. I'm assuming there's an exponentially decaying
        // probability of syncing as we go further back in time. This is a big assumption - and
        // probably not true in practice. But it'll do. (TODO: Quadratic might be better?)
        //
        // Given factor, the approx number of operations we'll return is log_f(|ops|).
        // Solving for f gives f = |ops|^(1/target).
        if target_count > self.version.len() {
            // Note I'm using n_ops here rather than time, since this easily scales time by the
            // approximate size of the transmitted operations. TODO: This might be a faulty
            // assumption given we're probably sending inserted content? Hm!
            let remaining_count = target_count - self.version.len();
            let n_ops = self.operations.0.len();
            let mut factor = f32::powf(n_ops as f32, 1f32 / (remaining_count) as f32);
            factor = factor.max(1.1);

            let mut t_inv = 1f32;
            while t_inv < time_len as f32 {
                dbg!(t_inv);
                push_time(time_len - (t_inv as usize));
                t_inv *= factor;
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use crate::list::ListOpLog;

    #[test]
    fn test_versions_since() {
        let mut oplog = ListOpLog::new();
        // Should be an empty set
        assert_eq!(oplog.get_stochastic_version(10), &[]);

        oplog.get_or_create_agent_id("seph");
        oplog.add_insert(0, 0, "a");
        oplog.add_insert(0, 0, "a");
        oplog.add_insert(0, 0, "a");
        oplog.add_insert(0, 0, "a");
        dbg!(oplog.get_stochastic_version(10));
    }
}