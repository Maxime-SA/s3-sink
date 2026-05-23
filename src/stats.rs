use std::time::Instant;
use tracing::info;

pub struct Stats {
    pub record_count: u64,
    pub bytes_consumed: u64,
    pub files_sealed: u64,
    pub successful_uploads: u64,
    pub failed_uploads: u64,
    pub uploads_backpressure_count: u64,
    pub started_at: Instant,
    last_report: Instant,
}

impl Stats {
    const ONE_MB: u64 = 1024 * 1024;

    pub fn new() -> Self {
        let now = Instant::now();
        Stats {
            record_count: 0,
            bytes_consumed: 0,
            files_sealed: 0,
            successful_uploads: 0,
            failed_uploads: 0,
            uploads_backpressure_count: 0,
            started_at: now,
            last_report: now,
        }
    }

    pub fn print_report(&mut self, active_file_count: u64, in_flight_uploads: u64) {
        let elapsed = self.last_report.elapsed().as_secs_f64();

        let mb_consumed = self.bytes_consumed / Self::ONE_MB;

        let mb_per_sec = format_args!(
            "{:.1}",
            self.bytes_consumed as f64 / elapsed / (Self::ONE_MB as f64)
        );

        let records_per_sec = (self.record_count as f64 / elapsed) as u64;

        info!(
            record_count = self.record_count,
            mb_consumed = mb_consumed,
            records_per_sec = records_per_sec,
            mb_per_sec = mb_per_sec,
            files_sealed = self.files_sealed,
            successful_uploads = self.successful_uploads,
            failed_uploads = self.failed_uploads,
            uploads_backpressure = self.uploads_backpressure_count,
            active_files = active_file_count,
            in_flight_uploads = in_flight_uploads,
            elapsed_s = self.started_at.elapsed().as_secs(),
            "stats"
        );
    }
}
