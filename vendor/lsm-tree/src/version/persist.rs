use crate::{
    checksum::ChecksummedWriter,
    file::{fsync_directory, retry_transient_io, rewrite_atomic, CURRENT_VERSION_FILE},
    version::Version,
};
use byteorder::{LittleEndian, WriteBytesExt};
use std::{
    io::{BufWriter, Write},
    path::Path,
};

/// CONSTELLATION PATCH (tests only): a tree folder whose version persists
/// take this much longer, as on a saturated disk where each fsync waits.
#[cfg(test)]
pub(crate) static SLOW_PERSIST: std::sync::Mutex<
    Option<(std::path::PathBuf, std::time::Duration)>,
> = std::sync::Mutex::new(None);

pub fn persist_version(folder: &Path, version: &Version) -> crate::Result<()> {
    log::trace!(
        "Persisting version {} in {}",
        version.id(),
        folder.display(),
    );

    #[cfg(test)]
    {
        let slow = SLOW_PERSIST
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some((slow_folder, delay)) = slow {
            if slow_folder == folder {
                std::thread::sleep(delay);
            }
        }
    }

    let path = folder.join(format!("v{}", version.id()));
    let file = retry_transient_io(|| std::fs::File::create(&path))?;
    let writer = BufWriter::new(&file);
    let mut writer = ChecksummedWriter::new(writer);

    {
        let mut writer = sfa::Writer::from_writer(&mut writer);

        version.encode_into(&mut writer)?;

        writer.finish().map_err(|e| match e {
            sfa::Error::Io(e) => crate::Error::from(e),
            _ => unreachable!(),
        })?;
    }

    writer.flush()?;

    let checksum = writer.checksum();

    drop(writer);

    file.sync_all()?;

    // IMPORTANT: fsync folder on Unix
    fsync_directory(folder)?;

    let mut current_file_content = vec![];
    current_file_content.write_u64::<LittleEndian>(version.id())?;
    current_file_content.write_u128::<LittleEndian>(checksum.into_u128())?;
    current_file_content.write_u8(0)?; // 0 = xxh3

    rewrite_atomic(&folder.join(CURRENT_VERSION_FILE), &current_file_content)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TreeType;
    use test_log::test;

    #[test]
    fn version_persist_replaces_orphaned_file() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let version = Version::new(0, TreeType::Standard);

        // Simulates the leftover of a persist that failed midway
        std::fs::write(dir.path().join("v0"), b"partial")?;

        persist_version(dir.path(), &version)?;

        assert_ne!(
            b"partial".as_slice(),
            &*std::fs::read(dir.path().join("v0"))?
        );
        assert!(dir.path().join(CURRENT_VERSION_FILE).try_exists()?);

        Ok(())
    }
}
