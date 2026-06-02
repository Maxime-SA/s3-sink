use crate::{Result, data_model::StreamId, envelopes::ClosedFile};

mod file_io;
mod file_registry;

pub use file_io::ActiveFile;
pub use file_registry::DiskFileRegistry;

pub trait FileRegistry {
    fn close(&mut self, id: &StreamId) -> Result<ClosedFile>;

    fn write_all(&mut self, id: StreamId, bytes: &[u8]) -> Result<()>;

    fn active_file_count(&self) -> u64;
}
