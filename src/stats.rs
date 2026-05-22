use std::time::Instant;

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

    pub fn elapsed_since_last_report(&self) -> f64 {
        self.last_report.elapsed().as_secs_f64()
    }

    pub fn total_elapsed_s(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    pub fn reset(&mut self) {
        self.records = 0;
        self.bytes = 0;
        self.seals = 0;
        self.uploads_ok = 0;
        self.uploads_failed = 0;
        self.last_report = Instant::now();
    }
}
