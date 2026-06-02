use crate::{Result, data_model::StreamId};
pub use disk_file_registry::DiskFileRegistry;
use std::path::PathBuf;

mod disk_file_registry;
mod file_io;

pub trait FileRegistry {
    fn close(&mut self, id: &StreamId) -> Result<(PathBuf, u64)>;

    fn write_all(&mut self, id: StreamId, bytes: &[u8]) -> Result<()>;

    fn active_file_count(&self) -> u64;
}
