// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! File systems for file drivers.
//!
//! PROTOTYPE (file-driver proposal): this module mirrors
//! `arrow-adbc/adbc_filesystem.h` in safe Rust. Names and semantics may
//! change.
//!
//! A file driver never opens files itself; it reads all of its bytes
//! through a [FileSystem] handed to it by the host. `adbc_ffi` converts
//! between these traits and the C tables in both directions.

use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use arrow_array::RecordBatchReader;

use crate::error::{Error, Result, Status};

/// Open hint: the file will be read mostly front to back.
pub const OPEN_SEQUENTIAL: u32 = 1;
/// Open hint: the file will be read at scattered offsets.
pub const OPEN_RANDOM: u32 = 2;

/// Metadata about a file.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FileInfo {
    /// The size in bytes, if known. Unknown is not the same as zero.
    pub size: Option<u64>,
    /// An opaque version token (ETag, generation, mtime...), if any.
    pub version: Option<Vec<u8>>,
}

/// One byte range to read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadRange {
    /// The absolute offset. Ignored if `suffix` is set.
    pub offset: u64,
    /// The number of bytes to read.
    pub length: u64,
    /// Read the final `length` bytes of the file.
    pub suffix: bool,
    /// Permit a short result, but only because the file ended.
    pub up_to: bool,
}

impl ReadRange {
    /// Read exactly `length` bytes at `offset`.
    pub fn exact(offset: u64, length: u64) -> Self {
        Self {
            offset,
            length,
            suffix: false,
            up_to: false,
        }
    }

    /// Read the final `length` bytes of the file (or the whole file, if
    /// it is shorter and `up_to` is set).
    pub fn suffix(length: u64) -> Self {
        Self {
            offset: 0,
            length,
            suffix: true,
            up_to: false,
        }
    }

    /// Resolve this range against a file of `size` bytes.
    ///
    /// Returns the absolute offset and the number of bytes to read, or an
    /// error if the range cannot be satisfied exactly and `up_to` is not
    /// set.
    pub fn resolve(&self, size: u64) -> Result<(u64, u64)> {
        let (offset, available) = if self.suffix {
            let length = self.length.min(size);
            (size - length, length)
        } else if self.offset >= size {
            (self.offset, 0)
        } else {
            (self.offset, (size - self.offset).min(self.length))
        };
        if available < self.length && !self.up_to {
            return Err(Error::with_message_and_status(
                format!(
                    "read past end of file: range {self:?} on a file of {size} bytes \
                     (use up_to to allow a short read)"
                ),
                Status::InvalidArguments,
            ));
        }
        Ok((offset, available))
    }
}

/// A blocking read into a borrowed buffer.
#[derive(Debug)]
pub struct ReadRequest<'a> {
    /// The range to read. `buffer.len()` must be at least `range.length`.
    pub range: ReadRange,
    /// The destination.
    pub buffer: &'a mut [u8],
    /// On success, the number of bytes written to `buffer`.
    pub bytes_read: u64,
}

/// An asynchronous read into an owned buffer.
///
/// The buffer is handed back through [ReadCallback::complete], which is
/// how safe Rust expresses "the buffer stays valid until its callback
/// fires".
#[derive(Debug)]
pub struct AsyncRead {
    /// The range to read. `buffer.len()` must be at least `range.length`.
    pub range: ReadRange,
    /// The destination; returned to the callback.
    pub buffer: Vec<u8>,
    /// Echoed to the callback; also the key for [File::cancel].
    pub user_data: u64,
}

/// The result of one [AsyncRead].
#[derive(Debug)]
pub struct ReadCompletion {
    /// The `user_data` of the read.
    pub user_data: u64,
    /// The buffer of the read, always returned (its contents are
    /// unspecified on error).
    pub buffer: Vec<u8>,
    /// The number of bytes read, or the error.
    pub result: Result<u64>,
}

/// Receives the completions of [File::submit_reads].
///
/// Implementations must be tiny: they may run on any provider thread and
/// must not block, wait for another read, or close the file. They may
/// submit follow-up reads.
pub trait ReadCallback: Send + Sync {
    /// Called exactly once per accepted read, never on the thread inside
    /// `submit_reads`.
    fn complete(&self, completion: ReadCompletion);
}

impl<F: Fn(ReadCompletion) + Send + Sync> ReadCallback for F {
    fn complete(&self, completion: ReadCompletion) {
        self(completion)
    }
}

/// An open file.
///
/// Reads may be issued concurrently from any thread; there is no shared
/// cursor. Dropping the file closes it: implementations must ensure that
/// no callback for this file is running or will run after drop returns.
pub trait File: Send + Sync {
    /// Metadata about the file, as observed at open.
    fn info(&self) -> Result<FileInfo>;

    /// Read ranges, blocking until every range has completed.
    ///
    /// Async-only providers can implement this with
    /// [read_ranges_via_submit].
    fn read_ranges(&self, requests: &mut [ReadRequest<'_>]) -> Result<()>;

    /// Submit reads; returns at once.
    ///
    /// Admission is all-or-nothing: on error, no callback fires. On
    /// success, each read gets exactly one callback, never on the calling
    /// thread.
    ///
    /// The default runs [File::read_ranges] on a small shared pool, so
    /// blocking-only providers get an async path for free. Because the
    /// pool job borrows `self`, it requires an `Arc`; use
    /// [submit_reads_via_pool] from an `Arc<dyn File>`.
    fn submit_reads(&self, reads: Vec<AsyncRead>, callback: Arc<dyn ReadCallback>) -> Result<()> {
        let _ = (reads, callback);
        Err(Error::with_message_and_status(
            "this file does not implement submit_reads natively; use submit_reads_via_pool",
            Status::NotImplemented,
        ))
    }

    /// Whether [File::submit_reads] is implemented natively.
    fn has_native_submit(&self) -> bool {
        false
    }

    /// Request cancellation of outstanding reads with this `user_data`.
    /// Best-effort; the callback stays authoritative.
    fn cancel(&self, user_data: u64) {
        let _ = user_data;
    }
}

/// A store of named, immutable files.
///
/// Names are store-relative UTF-8. Implementations must confine names to
/// their store. Methods may be called concurrently.
pub trait FileSystem: Send + Sync {
    /// Open a file. May be lazy.
    fn open(&self, name: &str, hints: u32) -> Result<Arc<dyn File>>;

    /// Get metadata about a file without opening it.
    fn stat(&self, name: &str) -> Result<FileInfo>;

    /// List the entries under a prefix.
    ///
    /// The schema is `name: utf8 not null, size: uint64, version: binary,
    /// is_directory: bool not null`; see [list_schema].
    fn list(&self, prefix: &str) -> Result<Box<dyn RecordBatchReader + Send>> {
        let _ = prefix;
        Err(Error::with_message_and_status(
            "list is not supported by this file system",
            Status::NotImplemented,
        ))
    }
}

/// The schema of [FileSystem::list] results.
pub fn list_schema() -> arrow_schema::SchemaRef {
    use arrow_schema::{DataType, Field, Schema};
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("size", DataType::UInt64, true),
        Field::new("version", DataType::Binary, true),
        Field::new("is_directory", DataType::Boolean, false),
    ]))
}

/// Submit reads to a file, using its native async path if it has one and
/// the shared pool otherwise.
pub fn submit_reads(
    file: &Arc<dyn File>,
    reads: Vec<AsyncRead>,
    callback: Arc<dyn ReadCallback>,
) -> Result<()> {
    if file.has_native_submit() {
        file.submit_reads(reads, callback)
    } else {
        submit_reads_via_pool(file.clone(), reads, callback)
    }
}

/// Run a blocking file's reads on the shared pool, one job per read.
///
/// The pool never runs a job on the submitting thread, which satisfies
/// the no-inline-completion rule.
pub fn submit_reads_via_pool(
    file: Arc<dyn File>,
    reads: Vec<AsyncRead>,
    callback: Arc<dyn ReadCallback>,
) -> Result<()> {
    for read in &reads {
        check_buffer(&read.range, read.buffer.len())?;
    }
    let pool = pool();
    for mut read in reads {
        let file = file.clone();
        let callback = callback.clone();
        pool.execute(Box::new(move || {
            let mut request = [ReadRequest {
                range: read.range,
                buffer: &mut read.buffer,
                bytes_read: 0,
            }];
            let result = file
                .read_ranges(&mut request)
                .map(|_| request[0].bytes_read);
            callback.complete(ReadCompletion {
                user_data: read.user_data,
                buffer: read.buffer,
                result,
            });
        }));
    }
    Ok(())
}

/// Implement [File::read_ranges] for an async-only file by submitting the
/// reads and waiting for all of them.
pub fn read_ranges_via_submit(file: &dyn File, requests: &mut [ReadRequest<'_>]) -> Result<()> {
    struct Waiter {
        state: Mutex<(usize, Vec<ReadCompletion>)>,
        done: Condvar,
    }
    impl ReadCallback for Waiter {
        fn complete(&self, completion: ReadCompletion) {
            let mut state = self.state.lock().unwrap();
            state.1.push(completion);
            if state.1.len() == state.0 {
                self.done.notify_all();
            }
        }
    }

    let reads = requests
        .iter()
        .enumerate()
        .map(|(i, r)| {
            check_buffer(&r.range, r.buffer.len())?;
            Ok(AsyncRead {
                range: r.range,
                buffer: vec![0; r.range.length as usize],
                user_data: i as u64,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let waiter = Arc::new(Waiter {
        state: Mutex::new((reads.len(), Vec::with_capacity(reads.len()))),
        done: Condvar::new(),
    });
    file.submit_reads(reads, waiter.clone())?;

    let mut state = waiter.state.lock().unwrap();
    while state.1.len() < state.0 {
        state = waiter.done.wait(state).unwrap();
    }
    let mut first_error = None;
    for completion in state.1.drain(..) {
        let request = &mut requests[completion.user_data as usize];
        match completion.result {
            Ok(n) => {
                request.buffer[..n as usize].copy_from_slice(&completion.buffer[..n as usize]);
                request.bytes_read = n;
            }
            Err(e) => {
                first_error.get_or_insert(e);
            }
        }
    }
    match first_error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Check that a buffer can hold a range.
pub fn check_buffer(range: &ReadRange, buffer_len: usize) -> Result<()> {
    if (buffer_len as u64) < range.length {
        return Err(Error::with_message_and_status(
            format!("buffer of {buffer_len} bytes is too small for range {range:?}"),
            Status::InvalidArguments,
        ));
    }
    Ok(())
}

/// A [std::io::Read] + [std::io::Seek] view of a [File], for format
/// libraries that expect a stream (calamine, zip, ...).
///
/// Reads are served from a read-ahead window, so small sequential reads
/// do not each cost a round trip to the provider.
pub struct FileReader {
    file: Arc<dyn File>,
    size: u64,
    position: u64,
    window: Vec<u8>,
    window_start: u64,
    read_ahead: usize,
}

impl FileReader {
    /// The default read-ahead window.
    pub const DEFAULT_READ_AHEAD: usize = 256 * 1024;

    /// Wrap a file. Its size must be known.
    pub fn new(file: Arc<dyn File>) -> Result<Self> {
        Self::with_read_ahead(file, Self::DEFAULT_READ_AHEAD)
    }

    /// Wrap a file with a given read-ahead window (at least one byte).
    pub fn with_read_ahead(file: Arc<dyn File>, read_ahead: usize) -> Result<Self> {
        let size = file.info()?.size.ok_or_else(|| {
            Error::with_message_and_status(
                "FileReader needs a file whose size is known",
                Status::InvalidState,
            )
        })?;
        Ok(Self {
            file,
            size,
            position: 0,
            window: Vec::new(),
            window_start: 0,
            read_ahead: read_ahead.max(1),
        })
    }

    /// The size of the file.
    pub fn size(&self) -> u64 {
        self.size
    }

    fn fill(&mut self, wanted: usize) -> std::io::Result<()> {
        let length = (wanted.max(self.read_ahead) as u64).min(self.size - self.position);
        let mut window = vec![0; length as usize];
        let mut request = [ReadRequest {
            range: ReadRange::exact(self.position, length),
            buffer: &mut window,
            bytes_read: 0,
        }];
        self.file
            .read_ranges(&mut request)
            .map_err(std::io::Error::other)?;
        self.window = window;
        self.window_start = self.position;
        Ok(())
    }
}

impl std::io::Read for FileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() || self.position >= self.size {
            return Ok(0);
        }
        let window_end = self.window_start + self.window.len() as u64;
        if self.position < self.window_start || self.position >= window_end {
            self.fill(buf.len())?;
        }
        let start = (self.position - self.window_start) as usize;
        let n = buf.len().min(self.window.len() - start);
        buf[..n].copy_from_slice(&self.window[start..start + n]);
        self.position += n as u64;
        Ok(n)
    }
}

impl std::io::Seek for FileReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let target = match pos {
            std::io::SeekFrom::Start(p) => Some(p),
            std::io::SeekFrom::End(d) => self.size.checked_add_signed(d),
            std::io::SeekFrom::Current(d) => self.position.checked_add_signed(d),
        };
        match target {
            Some(p) => {
                self.position = p;
                Ok(p)
            }
            None => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "seek to a negative position",
            )),
        }
    }
}

type Job = Box<dyn FnOnce() + Send>;

struct Pool {
    sender: Mutex<mpsc::Sender<Job>>,
}

impl Pool {
    fn execute(&self, job: Job) {
        // The receiver lives in the worker threads for the life of the
        // process, so this cannot fail.
        self.sender.lock().unwrap().send(job).unwrap();
    }
}

/// The shared pool behind [submit_reads_via_pool].
fn pool() -> &'static Pool {
    static POOL: OnceLock<Pool> = OnceLock::new();
    POOL.get_or_init(|| {
        let (sender, receiver) = mpsc::channel::<Job>();
        let receiver = Arc::new(Mutex::new(receiver));
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .clamp(2, 8);
        for i in 0..workers {
            let receiver = receiver.clone();
            std::thread::Builder::new()
                .name(format!("adbc-fs-{i}"))
                .spawn(move || {
                    loop {
                        let job = receiver.lock().unwrap().recv();
                        match job {
                            Ok(job) => {
                                // A panicking job must not take the worker down.
                                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                            }
                            Err(_) => break,
                        }
                    }
                })
                .expect("failed to spawn file-system pool thread");
        }
        Pool {
            sender: Mutex::new(sender),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_ranges() {
        assert_eq!(ReadRange::exact(2, 3).resolve(10).unwrap(), (2, 3));
        assert!(ReadRange::exact(8, 3).resolve(10).is_err());
        let up_to = ReadRange {
            up_to: true,
            ..ReadRange::exact(8, 3)
        };
        assert_eq!(up_to.resolve(10).unwrap(), (8, 2));
        assert_eq!(ReadRange::suffix(4).resolve(10).unwrap(), (6, 4));
        assert!(ReadRange::suffix(12).resolve(10).is_err());
        let up_to = ReadRange {
            up_to: true,
            ..ReadRange::suffix(12)
        };
        assert_eq!(up_to.resolve(10).unwrap(), (0, 10));
    }

    struct Bytes(Vec<u8>);

    impl File for Bytes {
        fn info(&self) -> Result<FileInfo> {
            Ok(FileInfo {
                size: Some(self.0.len() as u64),
                version: None,
            })
        }

        fn read_ranges(&self, requests: &mut [ReadRequest<'_>]) -> Result<()> {
            for r in requests {
                let (offset, n) = r.range.resolve(self.0.len() as u64)?;
                r.buffer[..n as usize]
                    .copy_from_slice(&self.0[offset as usize..(offset + n) as usize]);
                r.bytes_read = n;
            }
            Ok(())
        }
    }

    #[test]
    fn pool_never_completes_inline() {
        let file: Arc<dyn File> = Arc::new(Bytes((0..100).collect()));
        let caller = std::thread::current().id();
        let (tx, rx) = mpsc::channel();
        let tx = Mutex::new(tx);
        let callback = Arc::new(move |c: ReadCompletion| {
            tx.lock()
                .unwrap()
                .send((std::thread::current().id(), c))
                .unwrap();
        });
        let reads = (0..10)
            .map(|i| AsyncRead {
                range: ReadRange::exact(i * 10, 10),
                buffer: vec![0; 10],
                user_data: i,
            })
            .collect();
        submit_reads(&file, reads, callback).unwrap();
        let mut seen = Vec::new();
        for _ in 0..10 {
            let (thread, c) = rx.recv().unwrap();
            assert_ne!(thread, caller);
            assert_eq!(c.result.unwrap(), 10);
            assert_eq!(c.buffer[0], (c.user_data * 10) as u8);
            seen.push(c.user_data);
        }
        seen.sort();
        assert_eq!(seen, (0..10).collect::<Vec<_>>());
        assert!(rx.try_recv().is_err());
    }

    /// An async-only file that completes on a fresh thread.
    struct AsyncBytes(Arc<Vec<u8>>);

    impl File for AsyncBytes {
        fn info(&self) -> Result<FileInfo> {
            Ok(FileInfo {
                size: Some(self.0.len() as u64),
                version: None,
            })
        }

        fn read_ranges(&self, requests: &mut [ReadRequest<'_>]) -> Result<()> {
            read_ranges_via_submit(self, requests)
        }

        fn submit_reads(
            &self,
            reads: Vec<AsyncRead>,
            callback: Arc<dyn ReadCallback>,
        ) -> Result<()> {
            let data = self.0.clone();
            std::thread::spawn(move || {
                for mut read in reads {
                    let result = read.range.resolve(data.len() as u64).map(|(offset, n)| {
                        read.buffer[..n as usize]
                            .copy_from_slice(&data[offset as usize..(offset + n) as usize]);
                        n
                    });
                    callback.complete(ReadCompletion {
                        user_data: read.user_data,
                        buffer: read.buffer,
                        result,
                    });
                }
            });
            Ok(())
        }

        fn has_native_submit(&self) -> bool {
            true
        }
    }

    /// Counts the reads that reach the provider.
    struct Counting(Bytes, std::sync::atomic::AtomicUsize);

    impl File for Counting {
        fn info(&self) -> Result<FileInfo> {
            self.0.info()
        }

        fn read_ranges(&self, requests: &mut [ReadRequest<'_>]) -> Result<()> {
            self.1
                .fetch_add(requests.len(), std::sync::atomic::Ordering::Relaxed);
            self.0.read_ranges(requests)
        }
    }

    #[test]
    fn file_reader_reads_and_seeks() {
        use std::io::{Read, Seek, SeekFrom};
        let data: Vec<u8> = (0..=255).cycle().take(10_000).map(|b| b as u8).collect();
        let file = Arc::new(Counting(
            Bytes(data.clone()),
            std::sync::atomic::AtomicUsize::new(0),
        ));
        let mut reader = FileReader::with_read_ahead(file.clone(), 1024).unwrap();
        assert_eq!(reader.size(), 10_000);

        let mut all = Vec::new();
        reader.read_to_end(&mut all).unwrap();
        assert_eq!(all, data);
        // The window is a minimum: 10 000 bytes take at most ten 1 KiB
        // reads (fewer when the caller asks for more at once).
        let reads = file.1.load(std::sync::atomic::Ordering::Relaxed);
        assert!((1..=10).contains(&reads), "{reads} provider reads");

        // Small sequential reads are served from the window.
        let before = file.1.load(std::sync::atomic::Ordering::Relaxed);
        reader.seek(SeekFrom::Start(0)).unwrap();
        let mut byte = [0u8; 1];
        for _ in 0..1024 {
            reader.read_exact(&mut byte).unwrap();
        }
        let after = file.1.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(after - before, 1, "1024 one-byte reads hit the provider");

        // A footer read, as a ZIP reader does first.
        reader.seek(SeekFrom::End(-22)).unwrap();
        let mut footer = [0u8; 22];
        reader.read_exact(&mut footer).unwrap();
        assert_eq!(&footer[..], &data[10_000 - 22..]);

        reader.seek(SeekFrom::Start(5000)).unwrap();
        reader.seek(SeekFrom::Current(-10)).unwrap();
        let mut b = [0u8; 4];
        reader.read_exact(&mut b).unwrap();
        assert_eq!(&b[..], &data[4990..4994]);
        assert!(reader.seek(SeekFrom::Current(-100_000)).is_err());

        // Past the end reads nothing; it is not an error.
        reader.seek(SeekFrom::Start(20_000)).unwrap();
        assert_eq!(reader.read(&mut b).unwrap(), 0);
    }

    #[test]
    fn blocking_over_async() {
        let file = AsyncBytes(Arc::new((0..100).collect()));
        let mut a = [0u8; 4];
        let mut b = [0u8; 8];
        let mut requests = [
            ReadRequest {
                range: ReadRange::exact(10, 4),
                buffer: &mut a,
                bytes_read: 0,
            },
            ReadRequest {
                range: ReadRange {
                    up_to: true,
                    ..ReadRange::suffix(8)
                },
                buffer: &mut b,
                bytes_read: 0,
            },
        ];
        file.read_ranges(&mut requests).unwrap();
        assert_eq!(requests[0].bytes_read, 4);
        assert_eq!(requests[1].bytes_read, 8);
        assert_eq!(a, [10, 11, 12, 13]);
        assert_eq!(b, [92, 93, 94, 95, 96, 97, 98, 99]);

        let mut c = [0u8; 4];
        let mut bad = [ReadRequest {
            range: ReadRange::exact(98, 4),
            buffer: &mut c,
            bytes_read: 0,
        }];
        assert!(file.read_ranges(&mut bad).is_err());
    }
}
