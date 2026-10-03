//! The byte stream a [`HandoffPhase::Transfer`] writes (plan 37 §8 step 4):
//! how a serving daemon hands its FUSE sessions' descriptors and records to
//! the node plugin, which relays each to the replacement engine pod with a
//! [`HandoffPhase::Receive`].
//!
//! The control protocol carries at most one descriptor per request and none
//! in an answer, so the sessions travel on a socket of their own, attached
//! to the `Transfer` request: a unix *stream* socket (a handle table can
//! outgrow a datagram), carrying records
//!
//! ```text
//! [u32 big-endian length][length bytes]   the descriptor rides on the length
//! ```
//!
//! each with exactly one descriptor in an `SCM_RIGHTS` message attached to
//! its first byte, and a final zero length with none (the end mark: a
//! stream cut short is an error, never a shorter handoff). The record is
//! opaque here — the daemon's own JSON, which the plugin relays without
//! reading. Blocking I/O on a blocking socket: both ends run it on a
//! thread of their own.
//!
//! [`HandoffPhase::Transfer`]: crate::proto::types::HandoffPhase::Transfer
//! [`HandoffPhase::Receive`]: crate::proto::types::HandoffPhase::Receive

use crate::transport::unix::{recv_with_fds, send_with_fds};
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::os::unix::net::UnixStream;

/// The largest record accepted (a view's handle table, in JSON).
pub const MAX_RECORD: usize = 256 * 1024 * 1024;

/// Write one record with its descriptor.
pub fn write_record(sock: &mut UnixStream, record: &[u8], fd: BorrowedFd<'_>) -> io::Result<()> {
    if record.is_empty() || record.len() > MAX_RECORD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("a handoff record is 1..={MAX_RECORD} bytes"),
        ));
    }
    let header = (record.len() as u32).to_be_bytes();
    let sent = send_with_fds(sock.as_raw_fd(), &header, &[fd.as_raw_fd()])?;
    // The descriptor went with the first byte; the rest is plain data.
    sock.write_all(&header[sent..])?;
    sock.write_all(record)
}

/// Write the end mark: no record follows.
pub fn write_end(sock: &mut UnixStream) -> io::Result<()> {
    sock.write_all(&0u32.to_be_bytes())?;
    sock.flush()
}

/// Read the next record and its descriptor; `None` at the end mark. A
/// record without exactly one descriptor, or a stream that ends without
/// the end mark, is an error.
pub fn read_record(sock: &mut UnixStream) -> io::Result<Option<(Vec<u8>, OwnedFd)>> {
    let mut header = [0u8; 4];
    let mut fds = VecDeque::new();
    let mut got = 0;
    while got < header.len() {
        let n = recv_with_fds(sock.as_fd().as_raw_fd(), &mut header[got..], &mut fds)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the handoff stream ended without its end mark",
            ));
        }
        got += n;
    }
    let len = u32::from_be_bytes(header) as usize;
    if len == 0 {
        return match fds.is_empty() {
            true => Ok(None),
            false => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a descriptor arrived with the handoff's end mark",
            )),
        };
    }
    if len > MAX_RECORD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("a {len}-byte handoff record (at most {MAX_RECORD})"),
        ));
    }
    let fd = match (fds.pop_front(), fds.is_empty()) {
        (Some(fd), true) => fd,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a handoff record carries exactly one descriptor",
            ))
        }
    };
    let mut record = vec![0u8; len];
    sock.read_exact(&mut record)?;
    Ok(Some((record, fd)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsFd;

    #[test]
    fn records_and_their_descriptors_cross_in_order() {
        let (mut a, mut b) = UnixStream::pair().unwrap();
        let files: Vec<std::fs::File> = (0..3).map(|_| tempfile::tempfile().unwrap()).collect();
        let writer = std::thread::spawn(move || {
            for (i, f) in files.iter().enumerate() {
                let body = vec![b'a' + i as u8; 100_000 * (i + 1)];
                write_record(&mut a, &body, f.as_fd()).unwrap();
            }
            write_end(&mut a).unwrap();
            files
        });
        let mut got = Vec::new();
        while let Some((record, fd)) = read_record(&mut b).unwrap() {
            got.push((record, fd));
        }
        let files = writer.join().unwrap();
        assert_eq!(got.len(), 3);
        for (i, ((record, fd), f)) in got.iter().zip(&files).enumerate() {
            assert_eq!(record.len(), 100_000 * (i + 1));
            assert!(record.iter().all(|c| *c == b'a' + i as u8));
            // The same open file: an inode both name.
            use std::os::unix::fs::MetadataExt;
            let theirs = std::fs::File::from(fd.try_clone().unwrap())
                .metadata()
                .unwrap();
            assert_eq!(theirs.ino(), f.metadata().unwrap().ino());
        }
    }

    #[test]
    fn a_stream_cut_short_is_an_error_not_an_end() {
        let (mut a, mut b) = UnixStream::pair().unwrap();
        let f = tempfile::tempfile().unwrap();
        write_record(&mut a, b"{}", f.as_fd()).unwrap();
        drop(a);
        assert!(read_record(&mut b).unwrap().is_some());
        let err = read_record(&mut b).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }
}
