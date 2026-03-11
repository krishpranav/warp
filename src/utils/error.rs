//! Unified error type for the entire eventide codebase.
//!
//! One enum, no `Box<dyn Error>` unless unavoidable. Callers can match on
//! variants; the `Display` impl produces human-readable messages.

use std::{io, path::PathBuf};
use thiserror::Error;
use crate::queue::JobId;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("segment filename is not a valid base offset: {0}")]
    InvalidSegmentName(PathBuf),

    #[error("record at offset {offset} is corrupt (expected crc {crc_expected:#010x}, got {crc_actual:#010x})")]
    Corruption {
        offset:       u64,
        crc_expected: u32,
        crc_actual:   u32,
    },

    #[error("unknown topic: {0}")]
    UnknownTopic(String),

    #[error("unknown job: {0}")]
    UnknownJob(JobId),

    #[error("encode error: {0}")]
    Encode(String),

    #[error("decode error: {0}")]
    Decode(String),

    #[error("HTTP error: {0}")]
    Http(String),

    #[error("failed to bind {addr}: {source}")]
    Bind { addr: String, source: io::Error },

    #[error("accept error: {0}")]
    Accept(io::Error),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("plugin error: {0}")]
    Plugin(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;