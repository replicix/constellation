//! CONSTELLATION PATCH (io-uring): this whole file is Constellation's, not upstream
//! fuser's (vendor/fuser/CONSTELLATION-PATCH.md, patches/0002-io-uring-transport.patch).
//!
//! Buffer layout of a ring: one anonymous mapping holding every entry's stride.
//!
//! ```text
//! entry e at base + e * stride, stride = HEADER_AREA + payload_cap, HEADER_AREA = PAGE_SIZE
//!   [0, 288)                 fuse_uring_req_header, iov[0]        kernel owned while a SQE is pending
//!   [GAP - 168, GAP)         staging: fuse_in_header + op_in copy, ends at GAP
//!   [GAP, GAP + payload_cap) payload, iov[1]                      kernel owned while a SQE is pending
//! ```
//!
//! The staging copy makes the request contiguous for the unchanged `/dev/fuse` parser: the
//! header lands 8-byte aligned because `base`, `stride` and `GAP` are page multiples.

use std::io;
use std::mem::offset_of;
use std::num::NonZeroUsize;
use std::ptr::NonNull;

use nix::sys::mman::MapFlags;
use nix::sys::mman::MmapAdvise;
use nix::sys::mman::ProtFlags;

use crate::ll::fuse_abi as abi;

/// Bytes of `fuse_uring_req_header`, `iov[0]` of a REGISTER.
pub(crate) const HEADER_SZ: usize = size_of::<abi::fuse_uring_req_header>();
/// Largest staged request prefix: `fuse_in_header` plus a full `op_in`.
pub(crate) const STAGING_SZ: usize =
    size_of::<abi::fuse_in_header>() + abi::FUSE_URING_OP_IN_OUT_SZ;
pub(crate) const OP_IN_OFFSET: usize = offset_of!(abi::fuse_uring_req_header, op_in);
const ENT_IN_OUT_OFFSET: usize = offset_of!(abi::fuse_uring_req_header, ring_ent_in_out);
pub(crate) const FLAGS_OFFSET: usize =
    ENT_IN_OUT_OFFSET + offset_of!(abi::fuse_uring_ent_in_out, flags);
pub(crate) const COMMIT_ID_OFFSET: usize =
    ENT_IN_OUT_OFFSET + offset_of!(abi::fuse_uring_ent_in_out, commit_id);
pub(crate) const PAYLOAD_SZ_OFFSET: usize =
    ENT_IN_OUT_OFFSET + offset_of!(abi::fuse_uring_ent_in_out, payload_sz);
/// CONSTELLATION PATCH (io-uring): `fuse_uring_ent_in_out.offset` (ABI 7.46): where in its
/// queue's buffer pool a fetched request's payload buffer lies.
pub(crate) const POOL_OFFSET_OFFSET: usize =
    ENT_IN_OUT_OFFSET + offset_of!(abi::fuse_uring_ent_in_out, offset);

/// Size of the header area of every entry, and so the offset of its payload.
pub(crate) fn header_area() -> usize {
    page_size::get()
}

/// The buffers of one ring. Unmapped on drop, so the owner must only drop it once no SQE
/// naming the buffers is pending in the kernel.
#[derive(Debug)]
pub(crate) struct RingMemory {
    base: NonNull<u8>,
    len: usize,
    stride: usize,
    payload_cap: usize,
    entries: usize,
}

// SAFETY: the mapping is plain memory owned by this value; the pointer is only ever used
// through the raw-pointer discipline of the ring, never as a shared Rust reference.
unsafe impl Send for RingMemory {}
unsafe impl Sync for RingMemory {}

impl RingMemory {
    /// Reserves address space for `entries` strides. `payload_cap` is rounded up to a page
    /// multiple so every header is page aligned, and must fit `payload_sz: u32`.
    pub(crate) fn new(entries: usize, payload_cap: usize) -> io::Result<Self> {
        let page = header_area();
        if page < HEADER_SZ + STAGING_SZ {
            return Err(io::Error::other(format!(
                "page size {page} cannot hold the {HEADER_SZ} byte header and {STAGING_SZ} \
                 byte staging area"
            )));
        }
        let overflow = || io::Error::other("ring buffer size overflows the address space");
        let payload_cap = payload_cap
            .checked_next_multiple_of(page)
            .filter(|cap| u32::try_from(*cap).is_ok())
            .ok_or_else(overflow)?;
        let stride = page.checked_add(payload_cap).ok_or_else(overflow)?;
        let len = entries
            .checked_mul(stride)
            .and_then(NonZeroUsize::new)
            .ok_or_else(overflow)?;
        let flags = MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS | MapFlags::MAP_NORESERVE;
        // SAFETY: an anonymous private mapping at a kernel-chosen address aliases nothing.
        let base = unsafe {
            nix::sys::mman::mmap_anonymous(
                None,
                len,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                flags,
            )
        }
        .map_err(|err| {
            io::Error::new(
                io::Error::from(err).kind(),
                format!("reserving {len} bytes for ring buffers failed ({err})"),
            )
        })?;
        let mem = Self {
            base: base.cast(),
            len: len.get(),
            stride,
            payload_cap,
            entries,
        };
        // The kernel writes into these buffers on behalf of this process only
        // SAFETY: the range is the mapping just created.
        unsafe { nix::sys::mman::madvise(base, mem.len, MmapAdvise::MADV_DONTFORK)? };
        Ok(mem)
    }

    /// Offset of the payload within a stride; the staging area ends here.
    pub(crate) fn gap(&self) -> usize {
        self.stride - self.payload_cap
    }

    pub(crate) fn payload_cap(&self) -> usize {
        self.payload_cap
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Start of entry `e`'s stride.
    pub(crate) fn entry(&self, e: usize) -> NonNull<u8> {
        assert!(e < self.entries, "entry {e} of {}", self.entries);
        // SAFETY: e * stride < len, so the result stays inside the mapping.
        unsafe { self.base.add(e * self.stride) }
    }
}

impl Drop for RingMemory {
    fn drop(&mut self) {
        // SAFETY: base/len describe the mapping created in `new`, which nothing else unmaps.
        let _ = unsafe { nix::sys::mman::munmap(self.base.cast(), self.len) };
    }
}

/// CONSTELLATION PATCH (io-uring): the buffer pools of one zero-copy ring (plan 38 Z4): one
/// anonymous mapping, cut into one slice of `depth` buffers of `buf_size` bytes per queue of
/// the ring, each slice handed to the kernel with `FUSE_IO_URING_CMD_ADD_BUFPOOL`.
///
/// ```text
/// queue q of the ring (its position among the ring's queues) at base + q * depth * buf_size
///   [i * buf_size, (i + 1) * buf_size)   buffer i, chosen by the kernel per request
/// ```
///
/// `buf_size` must be the kernel's own `max_payload_sz` exactly: the kernel cuts the slice
/// into `len / max_payload_sz` buffers at that stride and reports a request's buffer by its
/// byte offset (`fuse_uring_ent_in_out.offset`), so a reply this side writes up to a larger
/// cap would spill into the next buffer. The whole mapping is registered as one io_uring fixed
/// buffer when the pool is registered, which pins (and so faults in) every page of it.
/// Unmapped on drop, so the owner must only drop it once no SQE naming it is pending.
#[derive(Debug)]
pub(crate) struct PoolMemory {
    base: NonNull<u8>,
    len: usize,
    slice: usize,
    buf_size: usize,
}

// SAFETY: as for `RingMemory`: plain memory owned by this value, reached only through raw
// pointers under the entry state machine.
unsafe impl Send for PoolMemory {}
unsafe impl Sync for PoolMemory {}

impl PoolMemory {
    /// Maps `queues` slices of `depth` buffers of `buf_size` bytes. `buf_size` must be a page
    /// multiple (the kernel's `max_payload_sz` always is: at least 8192, else `max_write` or
    /// `max_pages * PAGE_SIZE`) and every slice must fit the ADD_BUFPOOL's `u32` length.
    pub(crate) fn new(queues: usize, depth: usize, buf_size: usize) -> io::Result<Self> {
        let overflow = || io::Error::other("buffer pool size overflows");
        if buf_size == 0 || buf_size % header_area() != 0 {
            return Err(io::Error::other(format!(
                "buffer pool buffers of {buf_size} bytes are not a page multiple"
            )));
        }
        let slice = depth
            .checked_mul(buf_size)
            .filter(|s| *s > 0 && u32::try_from(*s).is_ok())
            .ok_or_else(|| {
                io::Error::other(format!(
                    "a queue's pool of {depth} x {buf_size} bytes exceeds the 4 GiB an \
                     ADD_BUFPOOL can name"
                ))
            })?;
        let len = queues
            .checked_mul(slice)
            .and_then(NonZeroUsize::new)
            .ok_or_else(overflow)?;
        // SAFETY: an anonymous private mapping at a kernel-chosen address aliases nothing.
        let base = unsafe {
            nix::sys::mman::mmap_anonymous(
                None,
                len,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS | MapFlags::MAP_NORESERVE,
            )
        }
        .map_err(|err| {
            io::Error::new(
                io::Error::from(err).kind(),
                format!("reserving {len} bytes for buffer pools failed ({err})"),
            )
        })?;
        let pool = Self {
            base: base.cast(),
            len: len.get(),
            slice,
            buf_size,
        };
        // As for the entry buffers: the kernel writes here on behalf of this process only
        // SAFETY: the range is the mapping just created.
        unsafe { nix::sys::mman::madvise(base, pool.len, MmapAdvise::MADV_DONTFORK)? };
        Ok(pool)
    }

    /// The whole mapping, as the one io_uring fixed buffer that registers it.
    pub(crate) fn iovec(&self) -> libc::iovec {
        libc::iovec {
            iov_base: self.base.as_ptr().cast(),
            iov_len: self.len,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn buf_size(&self) -> usize {
        self.buf_size
    }

    /// Start and length of the slice of the ring's `q`-th queue: the ADD_BUFPOOL's pool.
    pub(crate) fn slice(&self, q: usize) -> (NonNull<u8>, usize) {
        assert!((q + 1) * self.slice <= self.len, "queue {q} of the pool");
        // SAFETY: q * slice < len, so the result stays inside the mapping.
        (unsafe { self.base.add(q * self.slice) }, self.slice)
    }

    /// The buffer at `offset` into queue `q`'s slice, as the kernel reports it on a fetch;
    /// `None` when that is not the start of a whole buffer of the slice, which a well-behaved
    /// kernel never reports.
    pub(crate) fn buffer(&self, q: usize, offset: u32) -> Option<NonNull<u8>> {
        let offset = offset as usize;
        if offset % self.buf_size != 0 || offset + self.buf_size > self.slice {
            return None;
        }
        let (start, _) = self.slice(q);
        // SAFETY: inside queue q's slice, checked just above.
        Some(unsafe { start.add(offset) })
    }
}

impl Drop for PoolMemory {
    fn drop(&mut self) {
        // SAFETY: base/len describe the mapping created in `new`, which nothing else unmaps.
        let _ = unsafe { nix::sys::mman::munmap(self.base.cast(), self.len) };
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::fs;

    use super::*;

    /// Held by every test that asserts a mapping is gone: a concurrent test's mapping of the
    /// same size would otherwise be placed exactly where the unmapped one was
    pub(crate) static UNMAP_CHECK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    /// A mapping large enough that the small mappings of concurrent tests, which the kernel
    /// places top-down in a freed hole, never reach its base. `None` (with a message) when
    /// the host refuses the reservation, as `vm.overcommit_memory=2` does
    pub(crate) fn try_big(entries: usize) -> Option<RingMemory> {
        match RingMemory::new(entries, 1 << 30) {
            Ok(mem) => Some(mem),
            Err(e) if e.kind() == io::ErrorKind::OutOfMemory => {
                eprintln!("skipping: cannot reserve {entries} GiB of address space: {e}");
                None
            }
            Err(e) => panic!("mmap: {e}"),
        }
    }

    /// The `/proc/self/maps` line covering `addr`, if any. Containment rather than a start
    /// address because the kernel merges adjacent anonymous mappings
    fn mapping_of(addr: usize) -> Option<String> {
        let maps = fs::read_to_string("/proc/self/maps").unwrap();
        maps.lines()
            .find(|line| {
                let range = line.split(' ').next().unwrap();
                let (start, end) = range.split_once('-').unwrap();
                let start = usize::from_str_radix(start, 16).unwrap();
                let end = usize::from_str_radix(end, 16).unwrap();
                (start..end).contains(&addr)
            })
            .map(str::to_owned)
    }

    pub(crate) fn is_mapped(addr: usize) -> bool {
        mapping_of(addr).is_some()
    }

    /// The `VmFlags:` of the `/proc/self/smaps` entry covering `addr`, found in `smaps`
    /// alone. Taking the range from a separate `/proc/self/maps` read races a concurrent
    /// test creating or freeing a mapping, after which the kernel has merged or split the
    /// entry and that exact range no longer starts any `smaps` line.
    fn vm_flags(addr: usize) -> String {
        let smaps = fs::read_to_string("/proc/self/smaps").unwrap();
        let mut covering = false;
        for line in smaps.lines() {
            // A header line starts with `<start>-<end>`; every other line is `Key: value`
            if let Some((start, end)) = line
                .split(' ')
                .next()
                .and_then(|range| range.split_once('-'))
                .and_then(|(start, end)| {
                    Some((
                        usize::from_str_radix(start, 16).ok()?,
                        usize::from_str_radix(end, 16).ok()?,
                    ))
                })
            {
                covering = (start..end).contains(&addr);
            } else if covering {
                if let Some(flags) = line.strip_prefix("VmFlags:") {
                    return flags.trim().to_owned();
                }
            }
        }
        panic!("no /proc/self/smaps entry covers {addr:#x}");
    }

    #[test]
    fn layout_offsets() {
        assert_eq!(HEADER_SZ, 288);
        assert_eq!(STAGING_SZ, 168);
        assert_eq!(OP_IN_OFFSET, 128);
        assert_eq!(FLAGS_OFFSET, 256);
        assert_eq!(COMMIT_ID_OFFSET, 264);
        assert_eq!(PAYLOAD_SZ_OFFSET, 272);
        assert_eq!(POOL_OFFSET_OFFSET, 276);
        assert!(header_area() >= HEADER_SZ + STAGING_SZ);
    }

    #[test]
    fn strides_are_page_aligned_and_unmapped_on_drop() {
        let _serial = UNMAP_CHECK.lock();
        let page = page_size::get();
        let Some(mem) = try_big(3) else { return };
        let base = mem.entry(0).as_ptr() as usize;
        let stride = mem.entry(1).as_ptr() as usize - base;
        assert_eq!(base % page, 0);
        assert_eq!(stride, page + (1 << 30));
        assert_eq!(mem.gap(), page);
        assert_eq!(mem.payload_cap(), 1 << 30);
        assert_eq!(mem.len(), 3 * stride);
        for e in 0..3 {
            let entry = mem.entry(e).as_ptr() as usize;
            assert_eq!(entry, base + e * stride);
            assert_eq!(entry % page, 0);
            assert_eq!(
                (entry + mem.gap() - size_of::<abi::fuse_in_header>()) % 8,
                0
            );
        }
        assert!(is_mapped(base));
        let vm_flags = vm_flags(base);
        let flags: Vec<&str> = vm_flags.split(' ').collect();
        assert!(flags.contains(&"dc"), "MADV_DONTFORK: {flags:?}");
        assert!(flags.contains(&"nr"), "MAP_NORESERVE: {flags:?}");
        drop(mem);
        assert!(!is_mapped(base));
    }

    #[test]
    fn payload_cap_rounds_up_to_a_page() {
        let page = page_size::get();
        let mem = RingMemory::new(1, 8192 + 1).unwrap();
        assert_eq!(mem.payload_cap(), 8193usize.next_multiple_of(page));
        assert_eq!(RingMemory::new(1, page).unwrap().payload_cap(), page);
    }

    /// CONSTELLATION PATCH (io-uring): a pool's queue slices and buffers sit where the kernel
    /// reports them, and only whole buffers of a slice are ever handed out.
    #[test]
    fn pool_slices_and_buffers() {
        let page = page_size::get();
        let pool = PoolMemory::new(3, 4, 2 * page).unwrap();
        assert_eq!(pool.len(), 3 * 4 * 2 * page);
        assert_eq!(pool.buf_size(), 2 * page);
        let base = pool.iovec().iov_base as usize;
        assert_eq!(pool.iovec().iov_len, pool.len());
        for q in 0..3 {
            let (start, len) = pool.slice(q);
            assert_eq!(start.as_ptr() as usize, base + q * 8 * page);
            assert_eq!(len, 8 * page);
            for i in 0..4u32 {
                let at = pool.buffer(q, i * 2 * page as u32).unwrap().as_ptr() as usize;
                assert_eq!(at, base + q * 8 * page + i as usize * 2 * page);
            }
            assert!(
                pool.buffer(q, 4 * 2 * page as u32).is_none(),
                "past the slice"
            );
            assert!(
                pool.buffer(q, page as u32).is_none(),
                "not a buffer's start"
            );
        }
        let flags = vm_flags(base);
        assert!(
            flags.split(' ').any(|f| f == "dc"),
            "MADV_DONTFORK: {flags}"
        );
        assert!(
            PoolMemory::new(1, 1, page + 1).is_err(),
            "not a page multiple"
        );
        assert!(
            PoolMemory::new(1, 1 << 20, 1 << 20).is_err(),
            "a slice past 4 GiB"
        );
        assert!(PoolMemory::new(0, 1, page).is_err());
    }

    #[test]
    fn rejects_sizes_that_do_not_fit() {
        assert!(RingMemory::new(usize::MAX, 4096).is_err());
        assert!(RingMemory::new(0, 4096).is_err());
        assert!(RingMemory::new(1, usize::MAX - 4096).is_err());
        assert!(RingMemory::new(1, 1 << 32).is_err(), "payload_sz is a u32");
    }
}
