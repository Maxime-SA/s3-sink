use std::time::Instant;
use tracing::info;

pub struct Stats {
    pub records: u64,
    pub bytes: u64,
    pub seals: u64,
    pub uploads_ok: u64,
    pub uploads_failed: u64,
    pub started_at: Instant,
    last_report: Instant,
}

impl Stats {
    pub fn new() -> Self {
        let now = Instant::now();
        Stats {
            records: 0,
            bytes: 0,
            seals: 0,
            uploads_ok: 0,
            uploads_failed: 0,
            started_at: now,
            last_report: now,
        }
    }

    fn reset(&mut self) {
        self.records = 0;
        self.bytes = 0;
        self.seals = 0;
        self.uploads_ok = 0;
        self.uploads_failed = 0;
        self.last_report = Instant::now();
    }

    pub fn print_report(&mut self, active_file_count: u64, upload_pool_size: u64) {
        let elapsed = self.last_report.elapsed().as_secs_f64();

        info!(
            records_per_sec = (self.records as f64 / elapsed) as u64,
            mb_per_sec = format_args!("{:.1}", self.bytes as f64 / elapsed / 1_048_576.0),
            seals = self.seals,
            uploads_ok = self.uploads_ok,
            uploads_failed = self.uploads_failed,
            active_files = active_file_count,
            upload_pool_size = upload_pool_size,
            elapsed_s = self.started_at.elapsed().as_secs(),
            "stats"
        );

        self.reset();
    }
}
