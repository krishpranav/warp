//! Commit log — append-only, segmented, mmap-backed durable storage.
//!
//! Design goals (in priority order):
//!   1. Never lose an acknowledged write.
//!   2. O(1) append, O(log n) seek-to-offset.
//!   3. Zero-copy reads via mmap for consumers catching up.
//!   4. Segment rotation + retention so disk usage stays bounded.
//!
//! ## On-disk layout
//!
//! ```text
//! <data_dir>/<topic>/
//!   00000000000000000000.log   ← segment data: [record][record]…
//!   00000000000000000000.idx   ← sparse index: [(relative_offset, file_pos)]…
//!   00000000000001048576.log
//!   00000000000001048576.idx
//!   …
//! ```
//!
//! Each segment filename is the base offset of the first record it contains,
//! zero-padded to 20 digits — identical to Kafka's convention so existing
//! tooling can be reused.
//!
//! ## Record wire format
//!
//! ```text
//! ┌──────────┬──────────┬──────────┬──────────────────────────────┐
//! │ len (4B) │ crc (4B) │ ts  (8B) │ payload (len - 12 bytes)     │
//! └──────────┴──────────┴──────────┴──────────────────────────────┘
//! ```
//! All integers are little-endian. `crc` covers `ts + payload`.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use memmap2::MmapOptions;
use parking_lot::RwLock;
use tracing::{debug, instrument, warn};

use crate::utils::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(pub u64);

impl std::fmt::Display for Offset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct Record {
    pub offset:    Offset,
    pub timestamp: u64,
    pub payload:   bytes::Bytes,
}

#[derive(Debug, Clone)]
pub struct SegmentConfig {
    pub max_segment_bytes: u64,
    pub retention:         std::time::Duration,
    pub index_bytes:       u32,
    pub index_interval:    u32,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: 1 << 30,
            retention:         std::time::Duration::from_secs(7 * 24 * 3600),
            index_bytes:       10 << 20,
            index_interval:    256,
        }
    }
}

const RECORD_HEADER_LEN: usize = 16;
const INDEX_ENTRY_LEN:   usize = 8;

struct Segment {
    base_offset: u64,
    next_offset: u64,
    since_index: u32,
    log_writer:  BufWriter<File>,
    log_pos:     u64,
    idx_writer:  BufWriter<File>,
    log_path:    PathBuf,
}

impl Segment {
    fn create(dir: &Path, base_offset: u64) -> io::Result<Self> {
        let log_path = dir.join(format!("{base_offset:020}.log"));
        let idx_path = dir.join(format!("{base_offset:020}.idx"));

        let log_file = OpenOptions::new()
            .create(true).append(true).open(&log_path)?;
        let idx_file = OpenOptions::new()
            .create(true).append(true).open(&idx_path)?;

        let log_pos = log_file.metadata()?.len();

        Ok(Self {
            base_offset,
            next_offset: base_offset,
            since_index: 0,
            log_writer:  BufWriter::with_capacity(64 << 10, log_file),
            log_pos,
            idx_writer:  BufWriter::with_capacity(4 << 10, idx_file),
            log_path,
        })
    }

    fn append(&mut self, payload: &[u8], config: &SegmentConfig) -> io::Result<(Offset, u64)> {
        let timestamp = unix_millis();
        let crc = crc32fast::hash(&{
            let mut v = timestamp.to_le_bytes().to_vec();
            v.extend_from_slice(payload);
            v
        });

        let total_len = (RECORD_HEADER_LEN + payload.len()) as u32;

        self.log_writer.write_all(&total_len.to_le_bytes())?;
        self.log_writer.write_all(&crc.to_le_bytes())?;
        self.log_writer.write_all(&timestamp.to_le_bytes())?;
        self.log_writer.write_all(payload)?;
        self.log_writer.flush()?;

        if self.since_index == 0 {
            let rel: u32 = (self.next_offset - self.base_offset) as u32;
            let pos: u32 = self.log_pos as u32;
            self.idx_writer.write_all(&rel.to_le_bytes())?;
            self.idx_writer.write_all(&pos.to_le_bytes())?;
            self.idx_writer.flush()?;
        }

        let assigned    = Offset(self.next_offset);
        self.next_offset += 1;
        self.since_index  = (self.since_index + 1) % config.index_interval;
        self.log_pos     += u64::from(total_len) + 4;

        Ok((assigned, timestamp))
    }

    fn is_full(&self, config: &SegmentConfig) -> bool {
        self.log_pos >= config.max_segment_bytes
    }
}

#[derive(Clone)]
pub struct CommitLog {
    inner: Arc<RwLock<LogInner>>,
}

struct LogInner {
    dir:    PathBuf,
    config: SegmentConfig,
    active: Segment,
}

impl CommitLog {
    #[instrument(skip(config))]
    pub fn open(dir: &Path, config: SegmentConfig) -> Result<Self> {
        fs::create_dir_all(dir)?;

        let base_offset = discover_latest_base_offset(dir)?.unwrap_or(0);
        debug!(base_offset, "opening active segment");

        let active = Segment::create(dir, base_offset)?;

        Ok(Self {
            inner: Arc::new(RwLock::new(LogInner {
                dir:    dir.to_owned(),
                config,
                active,
            })),
        })
    }

    pub fn append(&self, payload: &[u8]) -> Result<(Offset, u64)> {
        let mut inner = self.inner.write();

        if inner.active.is_full(&inner.config) {
            let next_base = inner.active.next_offset;
            let new_seg   = Segment::create(&inner.dir, next_base)?;
            debug!(base_offset = next_base, "rolled new segment");
            inner.active = new_seg;
        }

        Ok(inner.active.append(payload, &inner.config)?)
    }

    pub fn read(&self, start: Offset, max_records: usize) -> Result<Vec<Record>> {
        let inner = self.inner.read();
        read_from_dir(&inner.dir, start, max_records)
    }

    pub fn next_offset(&self) -> Offset {
        Offset(self.inner.read().active.next_offset)
    }

    pub fn compact(&self, before_offset: Offset) -> Result<usize> {
        let inner  = self.inner.read();
        let active = inner.active.base_offset;
        let mut removed = 0;

        for entry in fs::read_dir(&inner.dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("log") {
                continue;
            }
            let base = parse_base_offset(&path)?;
            if base < before_offset.0 && base < active {
                let idx = path.with_extension("idx");
                fs::remove_file(&path)?;
                let _ = fs::remove_file(&idx);
                removed += 1;
                debug!(base, "compacted segment");
            }
        }

        Ok(removed)
    }
}

fn read_from_dir(dir: &Path, start: Offset, max: usize) -> Result<Vec<Record>> {
    let mut bases: Vec<u64> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("log"))
        .filter_map(|e| parse_base_offset(&e.path()).ok())
        .collect();
    bases.sort_unstable();

    let seg_idx = bases.partition_point(|&b| b <= start.0).saturating_sub(1);
    let mut records = Vec::with_capacity(max.min(256));

    for &base in &bases[seg_idx..] {
        if records.len() >= max { break; }
        let log_path = dir.join(format!("{base:020}.log"));
        read_segment(&log_path, base, start, max - records.len(), &mut records)?;
    }

    Ok(records)
}

fn read_segment(
    path:        &Path,
    base_offset: u64,
    start:       Offset,
    max:         usize,
    out:         &mut Vec<Record>,
) -> Result<()> {
    let file = File::open(path)?;
    let len  = file.metadata()?.len() as usize;
    if len == 0 { return Ok(()); }

    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let data: &[u8] = &mmap;
    let mut pos        = 0usize;
    let mut cur_offset = base_offset;

    while pos + RECORD_HEADER_LEN <= len && out.len() < max {
        let total_len = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap()) as usize;
        let crc       = u32::from_le_bytes(data[pos+4..pos+8].try_into().unwrap());
        let timestamp = u64::from_le_bytes(data[pos+8..pos+16].try_into().unwrap());

        let payload_start = pos + RECORD_HEADER_LEN;
        let payload_end   = pos + total_len;

        if payload_end > len {
            warn!(pos, total_len, "truncated record — stopping read");
            break;
        }

        let payload_slice = &data[payload_start..payload_end];

        let expected = crc32fast::hash(&{
            let mut v = timestamp.to_le_bytes().to_vec();
            v.extend_from_slice(payload_slice);
            v
        });
        if crc != expected {
            return Err(Error::Corruption {
                offset:       cur_offset,
                crc_expected: expected,
                crc_actual:   crc,
            });
        }

        if cur_offset >= start.0 {
            out.push(Record {
                offset:    Offset(cur_offset),
                timestamp,
                payload:   bytes::Bytes::copy_from_slice(payload_slice),
            });
        }

        pos        += total_len + 4;
        cur_offset += 1;
    }

    Ok(())
}

fn discover_latest_base_offset(dir: &Path) -> io::Result<Option<u64>> {
    let mut max: Option<u64> = None;
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("log") {
            if let Ok(base) = parse_base_offset(&path) {
                max = Some(max.map_or(base, |m: u64| m.max(base)));
            }
        }
    }
    Ok(max)
}

fn parse_base_offset(path: &Path) -> Result<u64> {
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Error::InvalidSegmentName(path.to_owned()))
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before epoch")
        .as_millis() as u64
}