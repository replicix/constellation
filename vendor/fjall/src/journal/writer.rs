// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::entry::{serialize_marker_item, Entry};
use super::rotation::{Seal, SealedJournal};
use crate::{
    batch::item::Item as BatchItem, journal::recovery::JournalId, keyspace::InternalKeyspaceId,
};
use lsm_tree::{CompressionType, SeqNo, ValueType};
use std::{
    fs::{File, OpenOptions},
    hash::Hasher,
    io::{BufWriter, Seek, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

// TODO: this should be a database configuration
pub const PRE_ALLOCATED_BYTES: u64 = 64 * 1_024 * 1_024;

pub const JOURNAL_BUFFER_BYTES: usize = 8 * 1_024;

pub struct Writer {
    pub(crate) path: PathBuf,
    file: BufWriter<File>,
    buf: Vec<u8>,
    is_buffer_dirty: bool,

    compression: CompressionType,
    compression_threshold: usize,

    /// CONSTELLATION PATCH (CONSTELLATION-PATCH.md, change 7): the seqno
    /// of the last batch this writer wrote (the next journal's rotation
    /// marker).
    last_seqno: Option<SeqNo>,
    /// CONSTELLATION PATCH (CONSTELLATION-PATCH.md, change 7): the sync of
    /// the journal this one replaced, which a durable persist waits for.
    seal: Option<Arc<Seal>>,
}

/// CONSTELLATION PATCH (CONSTELLATION-PATCH.md, changes 4, 7): what
/// [`Writer::flush_for_sync`] hands back to sync outside the lock: a
/// duplicate of the file's descriptor, its path (for the error), and the
/// sealed predecessor's pending sync.
pub type SyncTarget = (File, PathBuf, Option<Arc<Seal>>);

/// The persist mode allows setting the durability guarantee of previous writes
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PersistMode {
    /// Flushes data to OS buffers. This allows the OS to write out data in case of an
    /// application crash.
    ///
    /// When this function returns, data is **not** guaranteed to be persisted in case
    /// of a power loss event or OS crash.
    Buffer,

    /// Flushes data using `fdatasync`.
    ///
    /// Use if you know that `fdatasync` is sufficient for your file system and/or operating system.
    SyncData,

    /// Flushes data + metadata using `fsync`.
    SyncAll,
}

impl Writer {
    pub fn set_compression(&mut self, comp: CompressionType, threshold: usize) {
        self.compression = comp;
        self.compression_threshold = threshold;
    }

    pub fn pos(&mut self) -> crate::Result<u64> {
        self.file.stream_position().map_err(Into::into)
    }

    pub fn len(&self) -> crate::Result<u64> {
        Ok(self.file.get_ref().metadata()?.len())
    }

    /// Upstream's rotation in one call, which its journal unit tests use.
    ///
    /// CONSTELLATION PATCH (CONSTELLATION-PATCH.md, change 7): the database
    /// itself rotates through `rotation::rotate_if_full`, which runs the
    /// same steps with the syncs outside the journal lock.
    #[cfg(test)]
    pub fn rotate(&mut self) -> crate::Result<(PathBuf, PathBuf)> {
        let next_path = self.next_path()?;
        let next = Self::create_new(&next_path)?;

        // IMPORTANT: fsync folder on Unix
        #[expect(clippy::expect_used)]
        crate::file::fsync_directory(next_path.parent().expect("should have parent"))?;

        let sealed = self.swap_to(next)?;
        let prev_path = sealed.path.clone();
        sealed.sync()?;

        Ok((prev_path, next_path))
    }

    /// CONSTELLATION PATCH (CONSTELLATION-PATCH.md, change 7): the path of
    /// the journal after this one (upstream's `rotate` computed it).
    pub(crate) fn next_path(&self) -> crate::Result<PathBuf> {
        let folder = self.path.parent().expect("should have parent");

        let Some(basename) = self
            .path
            .file_name()
            .expect("should be valid file name")
            .to_str()
            .expect("should be valid utf-8")
            .strip_suffix(".jnl")
        else {
            log::error!("Invalid journal file name: {}", self.path.display());
            return Err(crate::Error::JournalRecovery(
                crate::JournalRecoveryError::InvalidFileName,
            ));
        };

        let journal_id = basename.parse::<JournalId>().map_err(|_| {
            log::error!("Invalid journal file name: {}", self.path.display());
            crate::Error::JournalRecovery(crate::JournalRecoveryError::InvalidFileName)
        })?;

        Ok(folder.join(format!("{}.jnl", journal_id + 1)))
    }

    /// CONSTELLATION PATCH (CONSTELLATION-PATCH.md, change 7): switches to
    /// `next` (created, pre-allocated and synced by
    /// [`Self::create_new`], its folder synced) without any fsync, and
    /// hands back the sealed journal, whose sync the caller owes after
    /// releasing the lock (`rotation.rs`).
    ///
    /// The sealed journal's buffer is written to the OS first, and `next`
    /// starts with the rotation marker: an empty batch whose seqno is the
    /// last one written here. A journal this writer has not written to was
    /// fully synced when it was opened or created (recovery syncs every
    /// journal), so it has no tail to lose and gets no marker.
    pub(crate) fn swap_to(&mut self, mut next: Self) -> crate::Result<SealedJournal> {
        log::debug!(
            "Sealing active journal at {}, rotating to {}",
            self.path.display(),
            next.path.display(),
        );

        self.file.flush().inspect_err(|e| {
            log::error!(
                "Failed to flush journal IO buffers at {}: {e:?}",
                self.path.display(),
            );
        })?;
        self.is_buffer_dirty = false;

        next.set_compression(self.compression, self.compression_threshold);
        if let Some(last) = self.last_seqno {
            next.write_marker(last)?;
        }
        let seal = Arc::new(Seal::after(self.pending_seal()));
        next.seal = Some(seal.clone());

        let sealed = std::mem::replace(self, next);
        // Flushed above: the buffer is empty.
        let (file, _) = sealed.file.into_parts();
        Ok(SealedJournal {
            file,
            path: sealed.path,
            seal,
        })
    }

    /// CONSTELLATION PATCH (CONSTELLATION-PATCH.md, change 7): the sealed
    /// predecessor's sync, while it is not done (or failed).
    pub(crate) fn pending_seal(&mut self) -> Option<Arc<Seal>> {
        if self.seal.as_ref().is_some_and(|seal| seal.succeeded()) {
            self.seal = None;
        }
        self.seal.clone()
    }

    /// CONSTELLATION PATCH (CONSTELLATION-PATCH.md, change 7): the
    /// rotation marker, a batch with no items (see `rotation.rs`).
    fn write_marker(&mut self, seqno: SeqNo) -> crate::Result<()> {
        self.is_buffer_dirty = true;
        self.buf.clear();
        self.write_start(0, seqno)?;
        self.buf.clear();
        let checksum = xxhash_rust::xxh3::Xxh3::default().finish();
        self.write_end(checksum)?;
        self.last_seqno = Some(seqno);
        Ok(())
    }

    pub fn create_new<P: Into<PathBuf>>(path: P) -> crate::Result<Self> {
        let path = path.into();

        let file = File::create_new(&path).inspect_err(|e| {
            log::error!("Failed to create journal file at {}: {e:?}", path.display());
        })?;

        file.set_len(PRE_ALLOCATED_BYTES).inspect_err(|e| {
            log::error!(
                "Failed to set journal file size to {PRE_ALLOCATED_BYTES}B at {}: {e:?}",
                path.display(),
            );
        })?;

        file.sync_all().inspect_err(|e| {
            log::error!("Failed to fsync journal file at {}: {e:?}", path.display());
        })?;
        super::rotation::after_sync(&path);

        Ok(Self {
            path,
            file: BufWriter::new(file),
            buf: Vec::new(),
            is_buffer_dirty: false,
            compression: CompressionType::None,
            compression_threshold: 0,
            last_seqno: None,
            seal: None,
        })
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();

        if !path.try_exists()? {
            let file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path)
                .inspect_err(|e| {
                    log::error!("Failed to create journal file at {}: {e:?}", path.display());
                })?;

            file.set_len(PRE_ALLOCATED_BYTES).inspect_err(|e| {
                log::error!(
                    "Failed to set journal file size to {PRE_ALLOCATED_BYTES}B at {}: {e:?}",
                    path.display(),
                );
            })?;

            file.sync_all().inspect_err(|e| {
                log::error!("Failed to fsync journal file at {}: {e:?}", path.display());
            })?;

            return Ok(Self {
                path: path.into(),
                file: BufWriter::with_capacity(JOURNAL_BUFFER_BYTES, file),
                buf: Vec::new(),
                is_buffer_dirty: false,
                compression: CompressionType::None,
                compression_threshold: 0,
                last_seqno: None,
                seal: None,
            });
        }

        let file = OpenOptions::new()
            .append(true)
            .open(path)
            .inspect_err(|e| {
                log::error!("Failed to open journal file at {}: {e:?}", path.display());
            })?;

        Ok(Self {
            path: path.into(),
            file: BufWriter::with_capacity(JOURNAL_BUFFER_BYTES, file),
            buf: Vec::new(),
            is_buffer_dirty: false,
            compression: CompressionType::None,
            compression_threshold: 0,
            last_seqno: None,
            seal: None,
        })
    }

    /// CONSTELLATION PATCH (CONSTELLATION-PATCH.md, change 4): flushes
    /// the IO buffer, and for a syncing `mode` hands back a duplicate of
    /// the file's descriptor (and the file's path, for the error) to sync
    /// without holding the writer. A sync through it covers everything
    /// written before the flush.
    ///
    /// CONSTELLATION PATCH (change 7): with the sealed predecessor's sync
    /// while it is pending; the caller's sync is durable once both are.
    pub(crate) fn flush_for_sync(
        &mut self,
        mode: PersistMode,
    ) -> std::io::Result<Option<SyncTarget>> {
        log::trace!(
            "Persisting journal at {} with mode={mode:?}",
            self.path.display(),
        );

        if self.is_buffer_dirty {
            self.file.flush().inspect_err(|e| {
                log::error!(
                    "Failed to flush journal IO buffers at {}: {e:?}",
                    self.path.display(),
                );
            })?;
            self.is_buffer_dirty = false;
        }
        match mode {
            PersistMode::Buffer => Ok(None),
            PersistMode::SyncAll | PersistMode::SyncData => self
                .file
                .get_ref()
                .try_clone()
                .map(|file| Some((file, self.path.clone(), self.pending_seal()))),
        }
    }

    /// Persists the journal file.
    ///
    /// CONSTELLATION PATCH (change 7): a syncing `mode` also waits for the
    /// sealed predecessor's sync while it is pending.
    pub(crate) fn persist(&mut self, mode: PersistMode) -> std::io::Result<()> {
        log::trace!(
            "Persisting journal at {} with mode={mode:?}",
            self.path.display(),
        );

        if self.is_buffer_dirty {
            self.file.flush().inspect_err(|e| {
                log::error!(
                    "Failed to flush journal IO buffers at {}: {e:?}",
                    self.path.display(),
                );
            })?;
            self.is_buffer_dirty = false;
        }

        match mode {
            PersistMode::SyncAll => self.file.get_mut().sync_all().inspect_err(|e| {
                log::error!(
                    "Failed to fsync journal file at {}: {e:?}",
                    self.path.display(),
                );
            }),
            PersistMode::SyncData => self.file.get_mut().sync_data().inspect_err(|e| {
                log::error!(
                    "Failed to fsyncdata journal file at {}: {e:?}",
                    self.path.display(),
                );
            }),
            PersistMode::Buffer => return Ok(()),
        }?;
        super::rotation::after_sync(&self.path);
        self.pending_seal().map_or(Ok(()), |seal| seal.wait())
    }

    /// Writes a batch start marker to the journal
    fn write_start(&mut self, item_count: u32, seqno: SeqNo) -> Result<usize, crate::Error> {
        debug_assert!(self.buf.is_empty());

        Entry::Start { item_count, seqno }.encode_into(&mut self.buf)?;

        self.file.write_all(&self.buf)?;

        Ok(self.buf.len())
    }

    /// Writes a batch end marker to the journal
    fn write_end(&mut self, checksum: u64) -> Result<usize, crate::Error> {
        debug_assert!(self.buf.is_empty());

        Entry::End(checksum).encode_into(&mut self.buf)?;

        self.file.write_all(&self.buf)?;

        Ok(self.buf.len())
    }

    pub(crate) fn write_raw(
        &mut self,
        keyspace_id: InternalKeyspaceId,
        key: &[u8],
        value: &[u8],
        value_type: ValueType,
        seqno: u64,
    ) -> crate::Result<usize> {
        self.is_buffer_dirty = true;

        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        let mut byte_count = 0;

        self.buf.clear();
        byte_count += self.write_start(1, seqno)?;
        self.buf.clear();

        serialize_marker_item(
            &mut self.buf,
            keyspace_id,
            key,
            value,
            value_type,
            if self.compression_threshold > 0 && value.len() >= self.compression_threshold {
                self.compression
            } else {
                CompressionType::None
            },
        )?;

        self.file.write_all(&self.buf)?;

        hasher.update(&self.buf);
        byte_count += self.buf.len();

        self.buf.clear();
        let checksum = hasher.finish();
        byte_count += self.write_end(checksum)?;
        // CONSTELLATION PATCH (CONSTELLATION-PATCH.md, change 7): only a
        // whole batch is the journal's last one.
        self.last_seqno = Some(seqno);

        Ok(byte_count)
    }

    pub(crate) fn write_clear(
        &mut self,
        keyspace_id: InternalKeyspaceId,
        seqno: SeqNo,
    ) -> crate::Result<usize> {
        self.is_buffer_dirty = true;

        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        let mut byte_count = 0;

        self.buf.clear();
        byte_count += self.write_start(1, seqno)?;
        self.buf.clear();

        Entry::Clear { keyspace_id }.encode_into(&mut self.buf)?;
        self.file.write_all(&self.buf)?;
        hasher.update(&self.buf);
        byte_count += self.buf.len();

        self.buf.clear();
        let checksum = hasher.finish();
        byte_count += self.write_end(checksum)?;
        // CONSTELLATION PATCH (CONSTELLATION-PATCH.md, change 7): only a
        // whole batch is the journal's last one.
        self.last_seqno = Some(seqno);

        Ok(byte_count)
    }

    pub fn write_batch<'a>(
        &mut self,
        items: impl Iterator<Item = &'a BatchItem>,
        batch_size: usize,
        seqno: SeqNo,
    ) -> crate::Result<usize> {
        if batch_size == 0 {
            return Ok(0);
        }

        self.is_buffer_dirty = true;

        self.buf.clear();

        // NOTE: entries.len() is surely never > u32::MAX
        #[expect(clippy::cast_possible_truncation)]
        let item_count = batch_size as u32;

        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        let mut byte_count = 0;

        byte_count += self.write_start(item_count, seqno)?;
        self.buf.clear();

        for item in items {
            debug_assert!(self.buf.is_empty());

            serialize_marker_item(
                &mut self.buf,
                item.keyspace.id,
                &item.key,
                &item.value,
                item.value_type,
                if self.compression_threshold > 0 && item.value.len() >= self.compression_threshold
                {
                    self.compression
                } else {
                    CompressionType::None
                },
            )?;

            self.file.write_all(&self.buf)?;

            hasher.update(&self.buf);
            byte_count += self.buf.len();

            self.buf.clear();
        }

        let checksum = hasher.finish();
        byte_count += self.write_end(checksum)?;
        // CONSTELLATION PATCH (CONSTELLATION-PATCH.md, change 7): only a
        // whole batch is the journal's last one.
        self.last_seqno = Some(seqno);

        Ok(byte_count)
    }
}

/// CONSTELLATION PATCH (CONSTELLATION-PATCH.md, change 7).
#[cfg(test)]
mod last_seqno_test {
    use super::{Writer, JOURNAL_BUFFER_BYTES};
    use lsm_tree::ValueType;
    use std::io::BufWriter;

    /// A batch that fails part-way is not the journal's last one: the next
    /// rotation marker must name the last whole batch.
    #[test]
    fn a_failed_batch_is_not_the_last_seqno() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("0.jnl");
        let mut writer = Writer::create_new(&path)?;
        writer.write_raw(1, b"a", b"whole", ValueType::Value, 5)?;
        assert_eq!(writer.last_seqno, Some(5));

        // The file turns read-only: the batch's start is buffered, its item
        // (larger than the buffer) fails to reach the file.
        writer.file = BufWriter::with_capacity(JOURNAL_BUFFER_BYTES, std::fs::File::open(&path)?);
        let value = vec![0u8; 2 * JOURNAL_BUFFER_BYTES];
        assert!(writer
            .write_raw(1, b"b", &value, ValueType::Value, 6)
            .is_err());
        assert_eq!(writer.last_seqno, Some(5));
        Ok(())
    }
}
