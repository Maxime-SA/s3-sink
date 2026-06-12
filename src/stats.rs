use std::time::Instant;
use tracing::info;

/*
Todo:
- Unit tests
- Remove duplicate code when printing report
*/

pub struct Counter(u64, u64); // (aggregate count, window count)
impl Counter {
    fn inc(&mut self, len: u64) {
        self.0 += len;
        self.1 += len;
    }
}

pub struct Stats {
    record_count: Counter,
    bytes_consumed: Counter,
    files_sealed: Counter,
    success_uploads: Counter,
    failure_uploads: Counter,
    dormant_files: Counter,
    started_at: Instant,
    last_tick: Instant,
}

impl Stats {
    const ONE_MB: u64 = 1024 * 1024;
    const ONE_GB: f64 = (1024 * 1024 * 1024) as f64;

    pub fn new() -> Self {
        let now = Instant::now();
        Stats {
            record_count: Counter(0, 0),
            bytes_consumed: Counter(0, 0),
            files_sealed: Counter(0, 0),
            success_uploads: Counter(0, 0),
            failure_uploads: Counter(0, 0),
            dormant_files: Counter(0, 0),
            started_at: now,
            last_tick: now,
        }
    }

    pub fn print_report(
        &mut self,
        active_file_count: u64,
        in_flight_uploads: u64,
        offsets_size: usize,
    ) {
        // snapshot stats
        let disk_space_gb = (fs2::available_space("/").unwrap_or(0) as f64 / Self::ONE_GB * 1000.0)
            .round()
            / 1000.0;

        info!(
            active_files = active_file_count,
            in_flight_uploads = in_flight_uploads,
            offsets_size_b = offsets_size,
            disk_space_gb = disk_space_gb,
            "snapshot stats"
        );

        // window stats
        let since_last_tick = self.last_tick.elapsed().as_secs_f64();

        let window_records_per_sec = self.rate(self.record_count.1, since_last_tick);

        let window_mb_consumed = self.bytes_consumed.1 / Self::ONE_MB;

        let window_mb_per_sec = self.rate(window_mb_consumed, since_last_tick);

        let window_files_sealed_per_sec = self.rate(self.files_sealed.1, since_last_tick);

        let window_successful_uploads_per_sec = self.rate(self.success_uploads.1, since_last_tick);

        let window_failed_uploads_per_sec = self.rate(self.failure_uploads.1, since_last_tick);

        info!(
            record_count = self.record_count.1,
            record_per_sec = window_records_per_sec,
            mb_consumed = window_mb_consumed,
            mb_per_sec = window_mb_per_sec,
            files_sealed = self.files_sealed.1,
            files_sealed_per_sec = window_files_sealed_per_sec,
            successful_uploads = self.success_uploads.1,
            successful_uploads_per_sec = window_successful_uploads_per_sec,
            failed_uploads = self.failure_uploads.1,
            failed_uploads_per_sec = window_failed_uploads_per_sec,
            dormant_files = self.dormant_files.1,
            window_duration_sec = since_last_tick,
            "window stats"
        );

        // aggregate stats
        let since_started = self.started_at.elapsed().as_secs_f64();

        let agg_records_per_sec = self.rate(self.record_count.0, since_started);

        let agg_mb_consumed = self.bytes_consumed.0 / Self::ONE_MB;

        let agg_mb_per_sec = self.rate(agg_mb_consumed, since_started);

        let agg_files_sealed_per_sec = self.rate(self.files_sealed.0, since_started);

        let agg_successful_uploads_per_sec = self.rate(self.success_uploads.0, since_started);

        let agg_failed_uploads_per_sec = self.rate(self.failure_uploads.0, since_started);

        info!(
            record_count = self.record_count.0,
            record_per_sec = agg_records_per_sec,
            mb_consumed = agg_mb_consumed,
            mb_per_sec = agg_mb_per_sec,
            files_sealed = self.files_sealed.0,
            files_sealed_per_sec = agg_files_sealed_per_sec,
            successful_uploads = self.success_uploads.0,
            successful_uploads_per_sec = agg_successful_uploads_per_sec,
            failed_uploads = self.failure_uploads.0,
            failed_uploads_per_sec = agg_failed_uploads_per_sec,
            dormant_files = self.dormant_files.0,
            since_started_sec = since_started,
            "aggregate stats"
        );

        self.reset_window();
    }

    pub fn inc_bytes_consumed(&mut self, len: u64) {
        self.bytes_consumed.inc(len);
        self.record_count.inc(1);
    }

    pub fn inc_success_uploads(&mut self) {
        self.success_uploads.inc(1);
    }

    pub fn inc_failure_uploads(&mut self) {
        self.failure_uploads.inc(1);
    }

    pub fn inc_files_sealed(&mut self) {
        self.files_sealed.inc(1);
    }

    pub fn inc_dormant_files(&mut self) {
        self.dormant_files.inc(1)
    }

    fn rate(&self, counter: u64, time_s: f64) -> f64 {
        if time_s > 0.0 {
            (counter as f64 / time_s * 100.0).round() / 100.0
        } else {
            0.0
        }
    }

    fn reset_window(&mut self) {
        self.record_count.1 = 0;
        self.bytes_consumed.1 = 0;
        self.files_sealed.1 = 0;
        self.success_uploads.1 = 0;
        self.failure_uploads.1 = 0;
        self.dormant_files.1 = 0;
        self.last_tick = Instant::now();
    }
}
