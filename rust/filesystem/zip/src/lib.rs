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

//! A file system over a ZIP archive, layered over another file system.
//!
//! PROTOTYPE for the ADBC file-driver proposal: archives are file systems,
//! not formats. A CSV driver reading `zip:s3://bucket/a.zip!/orders/` needs
//! to know nothing about ZIP or S3.
//!
//! - The archive's central directory is read once, through the parent.
//! - **Stored** entries are windows into the archive: range reads (blocking
//!   and async) pass straight through to the parent with an offset.
//! - **Deflated** entries cannot be read at random offsets; v0 inflates the
//!   whole entry into memory when it is opened (and checks its CRC).
//! - `list` returns the immediate children of a directory, with directories
//!   that are only implied by entry names included.
//!
//! Use [factory] to register the `zip` scheme with a driver manager:
//! `zip:<archive location>!/<path in archive>`.

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::{Arc, Mutex};

use arrow_array::builder::{BinaryBuilder, BooleanBuilder, StringBuilder, UInt64Builder};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use zip::{CompressionMethod, ZipArchive};

use adbc_core::error::{Error, Result, Status};
use adbc_core::filesystem::{
    self, AsyncRead, File, FileInfo, FileReader, FileSystem, Location, OPEN_RANDOM, ReadCallback,
    ReadRange, ReadRequest, list_schema,
};

/// Creates `zip` stores for a driver manager's registry. `resolve` finds the
/// file system holding the archive (typically the registry itself, so that
/// `zip:` composes with any scheme, including `zip:`).
pub fn factory<R>(resolve: R) -> impl Fn(&Location) -> Result<Arc<dyn FileSystem>> + Send + Sync
where
    R: Fn(&Location) -> Result<Arc<dyn FileSystem>> + Send + Sync,
{
    move |location: &Location| {
        if location.authority.is_empty() {
            return Err(Error::with_message_and_status(
                "a zip location names its archive: zip:<archive location>!/<path>",
                Status::InvalidArguments,
            ));
        }
        let archive = Location::parse(&location.authority)?;
        let parent = resolve(&archive)?;
        Ok(Arc::new(ZipFileSystem::open(parent.as_ref(), &archive.path)?) as Arc<dyn FileSystem>)
    }
}

#[derive(Clone, Debug)]
struct Entry {
    index: usize,
    method: CompressionMethod,
    data_start: u64,
    size: u64,
    crc32: u32,
}

/// A ZIP archive as a file system.
pub struct ZipFileSystem {
    archive: Arc<dyn File>,
    version: Vec<u8>,
    /// Files by normalized name.
    entries: BTreeMap<String, Entry>,
    /// Directory names (explicit or implied), without a trailing slash.
    directories: std::collections::BTreeSet<String>,
    /// For inflating entries; the central directory is already parsed.
    reader: Mutex<ZipArchive<FileReader>>,
}

fn zip_error(e: zip::result::ZipError) -> Error {
    let status = match &e {
        zip::result::ZipError::FileNotFound => Status::NotFound,
        zip::result::ZipError::Io(_) => Status::IO,
        zip::result::ZipError::UnsupportedArchive(_) => Status::NotImplemented,
        _ => Status::InvalidData,
    };
    Error::with_message_and_status(format!("zip: {e}"), status)
}

/// Normalize a name: forward slashes, no leading or trailing slash, and no
/// `.` or `..` components (a name may not escape the archive).
fn normalize(name: &str) -> Result<String> {
    let mut parts = Vec::new();
    for part in name.split(['/', '\\']) {
        match part {
            "" | "." => {}
            ".." => {
                return Err(Error::with_message_and_status(
                    format!("name '{name}' escapes the archive"),
                    Status::InvalidArguments,
                ));
            }
            part => parts.push(part),
        }
    }
    Ok(parts.join("/"))
}

impl ZipFileSystem {
    /// Open the archive `name` in `parent`.
    pub fn open(parent: &dyn FileSystem, name: &str) -> Result<Self> {
        let archive = parent.open(name, OPEN_RANDOM)?;
        let info = archive.info()?;
        let mut reader = ZipArchive::new(FileReader::new(archive.clone())?).map_err(zip_error)?;

        let mut entries = BTreeMap::new();
        let mut directories = std::collections::BTreeSet::new();
        for index in 0..reader.len() {
            let file = reader.by_index_raw(index).map_err(zip_error)?;
            // Entries whose names would escape the archive are not exposed.
            if file.enclosed_name().is_none() {
                continue;
            }
            let entry_name = normalize(file.name())?;
            // Every ancestor is a directory, whether or not it has an entry.
            let mut ancestor = entry_name.as_str();
            while let Some((parent_dir, _)) = ancestor.rsplit_once('/') {
                directories.insert(parent_dir.to_string());
                ancestor = parent_dir;
            }
            if file.is_dir() {
                directories.insert(entry_name);
                continue;
            }
            let data_start = file.data_start().ok_or_else(|| {
                Error::with_message_and_status(
                    format!("zip: no data offset for '{entry_name}'"),
                    Status::InvalidData,
                )
            })?;
            entries.insert(
                entry_name,
                Entry {
                    index,
                    method: file.compression(),
                    data_start,
                    size: file.size(),
                    crc32: file.crc32(),
                },
            );
        }
        Ok(Self {
            archive,
            version: info.version.unwrap_or_default(),
            entries,
            directories,
            reader: Mutex::new(reader),
        })
    }

    fn entry(&self, name: &str) -> Result<(String, &Entry)> {
        let name = normalize(name)?;
        match self.entries.get(&name) {
            Some(entry) => Ok((name, entry)),
            None if self.directories.contains(&name) || name.is_empty() => {
                Err(Error::with_message_and_status(
                    format!("zip: '{name}' is a directory"),
                    Status::InvalidArguments,
                ))
            }
            None => Err(Error::with_message_and_status(
                format!("zip: no entry '{name}'"),
                Status::NotFound,
            )),
        }
    }

    /// The version token of an entry: the archive's, plus the entry's CRC.
    fn entry_version(&self, entry: &Entry) -> Vec<u8> {
        let mut version = self.version.clone();
        version.extend_from_slice(&entry.crc32.to_le_bytes());
        version
    }
}

impl FileSystem for ZipFileSystem {
    fn open(&self, name: &str, _hints: u32) -> Result<Arc<dyn File>> {
        let (name, entry) = self.entry(name)?;
        let info = FileInfo {
            size: Some(entry.size),
            version: Some(self.entry_version(entry)),
        };
        match entry.method {
            CompressionMethod::Stored => Ok(Arc::new(StoredFile {
                archive: self.archive.clone(),
                start: entry.data_start,
                info,
            })),
            _ => {
                // Inflate the whole entry; the zip crate checks the CRC.
                let mut reader = self.reader.lock().unwrap();
                let mut file = reader.by_index(entry.index).map_err(zip_error)?;
                let mut data = Vec::with_capacity(entry.size as usize);
                file.read_to_end(&mut data).map_err(|e| {
                    Error::with_message_and_status(
                        format!("zip: inflating '{name}': {e}"),
                        Status::InvalidData,
                    )
                })?;
                Ok(Arc::new(MemoryFile { data, info }))
            }
        }
    }

    fn stat(&self, name: &str) -> Result<FileInfo> {
        let (_, entry) = self.entry(name)?;
        Ok(FileInfo {
            size: Some(entry.size),
            version: Some(self.entry_version(entry)),
        })
    }

    fn list(&self, prefix: &str) -> Result<Box<dyn RecordBatchReader + Send>> {
        let prefix = normalize(prefix)?;
        if !prefix.is_empty() && !self.directories.contains(&prefix) {
            return Err(Error::with_message_and_status(
                format!("zip: no directory '{prefix}'"),
                Status::NotFound,
            ));
        }
        let is_child = |name: &str| -> bool {
            let rest = if prefix.is_empty() {
                Some(name)
            } else {
                name.strip_prefix(&prefix).and_then(|r| r.strip_prefix('/'))
            };
            rest.is_some_and(|r| !r.is_empty() && !r.contains('/'))
        };

        let mut names = StringBuilder::new();
        let mut sizes = UInt64Builder::new();
        let mut versions = BinaryBuilder::new();
        let mut is_dir = BooleanBuilder::new();
        for dir in self.directories.iter().filter(|d| is_child(d)) {
            names.append_value(dir);
            sizes.append_null();
            versions.append_null();
            is_dir.append_value(true);
        }
        for (name, entry) in self.entries.iter().filter(|(n, _)| is_child(n)) {
            names.append_value(name);
            sizes.append_value(entry.size);
            versions.append_value(self.entry_version(entry));
            is_dir.append_value(false);
        }
        let batch = RecordBatch::try_new(
            list_schema(),
            vec![
                Arc::new(names.finish()),
                Arc::new(sizes.finish()),
                Arc::new(versions.finish()),
                Arc::new(is_dir.finish()),
            ],
        )?;
        Ok(Box::new(RecordBatchIterator::new(
            vec![Ok(batch)],
            list_schema(),
        )))
    }
}

/// A stored entry: a window into the archive, read through the parent.
struct StoredFile {
    archive: Arc<dyn File>,
    start: u64,
    info: FileInfo,
}

impl StoredFile {
    /// Translate a range on the entry to an exact range on the archive.
    fn translate(&self, range: &ReadRange) -> Result<ReadRange> {
        let (offset, length) = range.resolve(self.info.size.unwrap_or(0))?;
        Ok(ReadRange::exact(self.start + offset, length))
    }
}

impl File for StoredFile {
    fn info(&self) -> Result<FileInfo> {
        Ok(self.info.clone())
    }

    fn read_ranges(&self, requests: &mut [ReadRequest<'_>]) -> Result<()> {
        let mut translated = requests
            .iter_mut()
            .map(|r| {
                let range = self.translate(&r.range)?;
                Ok(ReadRequest {
                    range,
                    buffer: &mut r.buffer[..range.length as usize],
                    bytes_read: 0,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.archive.read_ranges(&mut translated)?;
        let lengths: Vec<u64> = translated.iter().map(|t| t.bytes_read).collect();
        drop(translated);
        for (request, n) in requests.iter_mut().zip(lengths) {
            request.bytes_read = n;
        }
        Ok(())
    }

    fn submit_reads(&self, reads: Vec<AsyncRead>, callback: Arc<dyn ReadCallback>) -> Result<()> {
        // A range that cannot be satisfied (e.g. an exact read past the end)
        // fails on its own callback, like any other read error; it does not
        // make the whole submission inadmissible.
        let mut valid = Vec::with_capacity(reads.len());
        let mut failed = Vec::new();
        for read in reads {
            match self.translate(&read.range) {
                Ok(range) => valid.push(AsyncRead { range, ..read }),
                Err(e) => failed.push((read, e)),
            }
        }
        if !valid.is_empty() {
            filesystem::submit_reads(&self.archive, valid, callback.clone())?;
        }
        if !failed.is_empty() {
            // Never complete inline.
            std::thread::spawn(move || {
                for (read, error) in failed {
                    callback.complete(filesystem::ReadCompletion {
                        user_data: read.user_data,
                        buffer: read.buffer,
                        result: Err(error),
                    });
                }
            });
        }
        Ok(())
    }

    fn has_native_submit(&self) -> bool {
        // Pass through whatever the parent offers (natively or pooled).
        true
    }

    fn cancel(&self, user_data: u64) {
        self.archive.cancel(user_data)
    }
}

/// An inflated entry, held in memory.
struct MemoryFile {
    data: Vec<u8>,
    info: FileInfo,
}

impl File for MemoryFile {
    fn info(&self) -> Result<FileInfo> {
        Ok(self.info.clone())
    }

    fn read_ranges(&self, requests: &mut [ReadRequest<'_>]) -> Result<()> {
        for r in requests {
            let (offset, n) = r.range.resolve(self.data.len() as u64)?;
            r.buffer[..n as usize]
                .copy_from_slice(&self.data[offset as usize..(offset + n) as usize]);
            r.bytes_read = n;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use zip::write::SimpleFileOptions;

    /// An in-memory parent file system.
    struct Memory(BTreeMap<String, Arc<Vec<u8>>>);

    impl FileSystem for Memory {
        fn open(&self, name: &str, _hints: u32) -> Result<Arc<dyn File>> {
            let data = self.0.get(name).ok_or_else(|| {
                Error::with_message_and_status(format!("no file {name}"), Status::NotFound)
            })?;
            Ok(Arc::new(MemoryFile {
                data: data.as_ref().clone(),
                info: FileInfo {
                    size: Some(data.len() as u64),
                    version: Some(b"v1".to_vec()),
                },
            }))
        }

        fn stat(&self, name: &str) -> Result<FileInfo> {
            self.open(name, 0)?.info()
        }
    }

    fn csv(rows: usize, seed: u32) -> Vec<u8> {
        let mut out = String::from("id,value\n");
        for i in 0..rows {
            out.push_str(&format!("{},{}\n", i, (i as u32 * 7 + seed) % 100));
        }
        out.into_bytes()
    }

    fn make_zip(files: &[(&str, Vec<u8>, CompressionMethod)]) -> Vec<u8> {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        zip.add_directory("empty/", SimpleFileOptions::default())
            .unwrap();
        for (name, data, method) in files {
            zip.start_file(
                *name,
                SimpleFileOptions::default().compression_method(*method),
            )
            .unwrap();
            zip.write_all(data).unwrap();
        }
        zip.finish().unwrap().into_inner()
    }

    fn fixture() -> (Memory, Vec<Vec<u8>>) {
        let files = vec![csv(50, 1), csv(200, 2), csv(10, 3)];
        let archive = make_zip(&[
            (
                "orders/2024-01.csv",
                files[0].clone(),
                CompressionMethod::Stored,
            ),
            (
                "orders/2024-02.csv",
                files[1].clone(),
                CompressionMethod::Deflated,
            ),
            ("readme.csv", files[2].clone(), CompressionMethod::Deflated),
        ]);
        let parent = Memory(BTreeMap::from([(
            "data/a.zip".to_string(),
            Arc::new(archive),
        )]));
        (parent, files)
    }

    fn read_all(file: &Arc<dyn File>) -> Vec<u8> {
        let mut reader = FileReader::new(file.clone()).unwrap();
        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        out
    }

    #[test]
    fn reads_stored_and_deflated_entries() {
        let (parent, files) = fixture();
        let zip = ZipFileSystem::open(&parent, "data/a.zip").unwrap();
        assert_eq!(
            read_all(&zip.open("orders/2024-01.csv", 0).unwrap()),
            files[0]
        );
        assert_eq!(
            read_all(&zip.open("/orders/2024-02.csv", 0).unwrap()),
            files[1]
        );
        assert_eq!(read_all(&zip.open("readme.csv", 0).unwrap()), files[2]);
        assert_eq!(
            zip.stat("orders/2024-02.csv").unwrap().size,
            Some(files[1].len() as u64)
        );
        assert_eq!(
            zip.open("nope.csv", 0).err().unwrap().status,
            Status::NotFound
        );
        assert_eq!(
            zip.open("orders", 0).err().unwrap().status,
            Status::InvalidArguments
        );
        assert_eq!(
            zip.open("../data/a.zip", 0).err().unwrap().status,
            Status::InvalidArguments
        );
    }

    fn list_names(zip: &ZipFileSystem, prefix: &str) -> Vec<(String, bool)> {
        let mut out = Vec::new();
        for batch in zip.list(prefix).unwrap() {
            let batch = batch.unwrap();
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            let dirs = batch
                .column(3)
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                out.push((names.value(i).to_string(), dirs.value(i)));
            }
        }
        out
    }

    #[test]
    fn lists_immediate_children() {
        let (parent, _) = fixture();
        let zip = ZipFileSystem::open(&parent, "data/a.zip").unwrap();
        assert_eq!(
            list_names(&zip, ""),
            [
                ("empty".to_string(), true),
                ("orders".to_string(), true),
                ("readme.csv".to_string(), false)
            ]
        );
        assert_eq!(
            list_names(&zip, "orders/"),
            [
                ("orders/2024-01.csv".to_string(), false),
                ("orders/2024-02.csv".to_string(), false)
            ]
        );
        assert!(list_names(&zip, "empty").is_empty());
        assert_eq!(zip.list("missing").err().unwrap().status, Status::NotFound);
    }

    #[test]
    fn stored_entries_pass_ranges_through() {
        let (parent, files) = fixture();
        let zip = ZipFileSystem::open(&parent, "data/a.zip").unwrap();
        let file = zip.open("orders/2024-01.csv", 0).unwrap();
        let mut tail = [0u8; 8];
        let mut requests = [ReadRequest {
            range: ReadRange::suffix(8),
            buffer: &mut tail,
            bytes_read: 0,
        }];
        file.read_ranges(&mut requests).unwrap();
        assert_eq!(&tail[..], &files[0][files[0].len() - 8..]);
    }

    #[test]
    fn factory_composes_with_a_resolver() {
        let (parent, files) = fixture();
        let parent: Arc<dyn FileSystem> = Arc::new(parent);
        let resolve = move |location: &Location| -> Result<Arc<dyn FileSystem>> {
            assert_eq!(location.scheme, "file");
            Ok(parent.clone())
        };
        let make = factory(resolve);
        let location = Location::parse("zip:data/a.zip!/orders/2024-02.csv").unwrap();
        let zip = make(&location).unwrap();
        assert_eq!(read_all(&zip.open(&location.path, 0).unwrap()), files[1]);
    }

    #[test]
    fn conforms_through_c() {
        // Both kinds of entry, exported through the C ABI and checked by the
        // conformance kit.
        let (parent, files) = fixture();
        let zip: Arc<dyn FileSystem> =
            Arc::new(ZipFileSystem::open(&parent, "data/a.zip").unwrap());
        for (name, data) in [
            ("orders/2024-01.csv", &files[0]),
            ("orders/2024-02.csv", &files[1]),
        ] {
            let mut raw = adbc_ffi::filesystem::export_file_system(zip.clone());
            unsafe {
                adbc_ffi::filesystem::conformance::check_file_system(
                    &mut raw,
                    name,
                    "orders/missing.csv",
                    data,
                )
                .unwrap_or_else(|e| panic!("{name}: {e}"));
                (raw.release.unwrap())(&mut raw);
            }
        }
    }
}
