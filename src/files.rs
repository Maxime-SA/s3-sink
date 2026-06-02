use std::path::PathBuf;

use crate::{Result, data_model::StreamId};

mod file_io;
mod file_registry;

pub use file_registry::DiskFileRegistry;

pub trait FileRegistry {
    fn close(&mut self, id: &StreamId) -> Result<(PathBuf, u64)>;

    fn write_all(&mut self, id: StreamId, bytes: &[u8]) -> Result<()>;

    fn active_file_count(&self) -> u64;
}
