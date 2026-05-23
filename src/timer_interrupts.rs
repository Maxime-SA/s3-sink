use crate::SinkConfig;
use std::time::Duration;
use tokio::time::{Interval, interval};

pub struct TimerInterrupts {
    pub fairness_scheduler_tick: Interval,
    pub commit_tick: Interval,
    pub upload_tick: Interval,
}
impl TimerInterrupts {
    pub fn new(config: &SinkConfig) -> Self {
        // manage per-topic consumption budget
        let fairness_scheduler_tick = interval(Duration::from_millis(
            config.timers.fairness_scheduler_tick_ms,
        ));

        // commit accumulated offsets
        let commit_tick = interval(Duration::from_millis(config.timers.commit_tick_ms));

        // upload dormant files
        let upload_tick = interval(Duration::from_millis(
            config.uploads.max_active_file_timeout_ms / 5,
        ));

        TimerInterrupts {
            fairness_scheduler_tick,
            commit_tick,
            upload_tick,
        }
    }
}
