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

//! File systems for file drivers: the built-in local provider and the
//! scheme registry.
//!
//! PROTOTYPE (file-driver proposal).
//!
//! When a file driver's connection is created without a file system, and its
//! database has none either, the driver manager parses the database `uri`
//! into a [Location] and asks the registry for a provider for its scheme.
//! Only `file` is built in. Other providers are shared libraries described
//! by *provider manifests* and loaded with [load_provider_manifest] or
//! [register_providers_in]; a scheme that is not registered yet is also
//! looked up in the directories of `ADBC_FILESYSTEM_PROVIDER_PATH`. In-process
//! providers can be registered directly with [register_scheme].
//!
//! A provider manifest:
//!
//! ```toml
//! manifest_version = 1
//! name = "ADBC S3 provider"
//!
//! [ADBC.filesystem]
//! schemes = ["s3"]
//! # true for archive-style providers (`zip:<inner location>!/<path>`): the
//! # driver manager resolves the inner location and hands it over.
//! layered = false
//!
//! [Provider]
//! # A path (relative to the manifest), or a table keyed by platform.
//! shared = "adbc_filesystem_object_store.dll"
//! entrypoint = "AdbcFileSystemProviderInit"  # the default
//!
//! [options]  # provider configuration
//! aws_region = "us-east-1"
//! ```

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::UNIX_EPOCH;

use arrow_array::builder::{BooleanBuilder, StringBuilder, UInt64Builder};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};

use adbc_core::error::{Error, Result, Status};
use adbc_core::filesystem::{
    File, FileInfo, FileSystem, FileSystemProvider, Location, ReadRequest, list_schema,
};
use adbc_ffi::filesystem::provider::{FFI_AdbcFileSystemProviderInitFunc, import_provider};
use toml::de::{DeTable, DeValue};

use crate::error::libloading_error_to_adbc_error;
use crate::search::DriverLibrary;

/// Creates a provider for a [Location]'s store (its scheme and authority).
pub type FileSystemFactory = Arc<dyn Fn(&Location) -> Result<Arc<dyn FileSystem>> + Send + Sync>;

static REGISTRY: LazyLock<RwLock<HashMap<String, FileSystemFactory>>> = LazyLock::new(|| {
    let mut registry: HashMap<String, FileSystemFactory> = HashMap::new();
    registry.insert(
        "file".to_string(),
        Arc::new(|location: &Location| {
            if !location.authority.is_empty() && location.authority != "localhost" {
                return Err(Error::with_message_and_status(
                    format!(
                        "file:// locations on another host ('{}') are not supported",
                        location.authority
                    ),
                    Status::NotImplemented,
                ));
            }
            Ok(Arc::new(LocalFileSystem::new()) as Arc<dyn FileSystem>)
        }),
    );
    RwLock::new(registry)
});

/// Register (or replace) the provider factory for a URI scheme.
pub fn register_scheme(scheme: &str, factory: FileSystemFactory) {
    REGISTRY
        .write()
        .unwrap()
        .insert(scheme.to_ascii_lowercase(), factory);
}

/// Resolve a provider for a location through the registry.
pub fn resolve(location: &Location) -> Result<Arc<dyn FileSystem>> {
    let registered = || REGISTRY.read().unwrap().get(&location.scheme).cloned();
    let factory = match registered() {
        Some(factory) => factory,
        None => {
            find_provider_on_path(&location.scheme)?;
            registered().ok_or_else(|| {
                Error::with_message_and_status(
                    format!(
                        "No file system is registered for scheme '{}' (and no provider \
                         manifest for it was found on {PROVIDER_PATH_ENV})",
                        location.scheme
                    ),
                    Status::NotFound,
                )
            })?
        }
    };
    factory(location)
}

/// Directories searched for provider manifests when a scheme is not
/// registered (a path list, like `PATH`).
pub const PROVIDER_PATH_ENV: &str = "ADBC_FILESYSTEM_PROVIDER_PATH";

const DEFAULT_PROVIDER_ENTRYPOINT: &[u8] = b"AdbcFileSystemProviderInit";

/// Load a provider library, configure it, and register it for `schemes`.
///
/// `layered` providers (archives) receive the store of the inner location
/// of a nested `scheme:<inner location>!/<path>` location, resolved through
/// this registry, so they compose with every other provider.
///
/// The library stays loaded for the life of the process: file systems it
/// opened may outlive any registration.
pub fn load_provider(
    library: impl AsRef<OsStr>,
    entrypoint: Option<&[u8]>,
    schemes: &[String],
    layered: bool,
    options: &[(String, String)],
) -> Result<()> {
    let library = DriverLibrary::load_library(library)?;
    let entrypoint = entrypoint.unwrap_or(DEFAULT_PROVIDER_ENTRYPOINT);
    let init: FFI_AdbcFileSystemProviderInitFunc = unsafe {
        *library
            .get::<FFI_AdbcFileSystemProviderInitFunc>(entrypoint)
            .map_err(libloading_error_to_adbc_error)?
    };
    std::mem::forget(library);
    let mut provider = unsafe { import_provider(init)? };
    for (key, value) in options {
        provider.set_option(key, value)?;
    }
    let provider = Arc::new(provider);
    for scheme in schemes {
        let provider = provider.clone();
        register_scheme(
            scheme,
            Arc::new(move |location: &Location| {
                let parent = if layered {
                    if location.authority.is_empty() {
                        return Err(Error::with_message_and_status(
                            format!(
                                "a '{0}' location names an inner location: \
                                 {0}:<location>!/<path>",
                                location.scheme
                            ),
                            Status::InvalidArguments,
                        ));
                    }
                    let inner = Location::parse(&location.authority)?;
                    Some((resolve(&inner)?, inner.path))
                } else {
                    None
                };
                provider.open(&location.scheme, &location.authority, parent)
            }),
        );
    }
    Ok(())
}

/// A parsed provider manifest.
#[derive(Clone, Debug)]
struct ProviderManifest {
    library: PathBuf,
    entrypoint: Option<Vec<u8>>,
    schemes: Vec<String>,
    layered: bool,
    options: Vec<(String, String)>,
}

fn manifest_error(path: &Path, message: impl std::fmt::Display) -> Error {
    Error::with_message_and_status(
        format!("Provider manifest '{}': {message}", path.display()),
        Status::InvalidArguments,
    )
}

fn read_provider_manifest(path: &Path) -> Result<ProviderManifest> {
    let contents = std::fs::read_to_string(path).map_err(|e| manifest_error(path, e))?;
    let manifest = DeTable::parse(&contents).map_err(|e| manifest_error(path, e))?;
    let manifest = manifest.get_ref();
    let table = |key: &str| manifest.get(key).and_then(|v| v.get_ref().as_table());

    let provider = table("Provider").ok_or_else(|| manifest_error(path, "no [Provider]"))?;
    let library = match provider.get("shared").map(|v| v.get_ref()) {
        Some(DeValue::String(s)) => PathBuf::from(s.as_ref()),
        Some(DeValue::Table(by_platform)) => {
            let (os, arch, extra) = crate::search::arch_triplet();
            let key = format!("{os}_{arch}{extra}");
            by_platform
                .get(key.as_str())
                .and_then(|v| v.get_ref().as_str())
                .map(PathBuf::from)
                .ok_or_else(|| manifest_error(path, format!("no Provider.shared.{key}")))?
        }
        _ => {
            return Err(manifest_error(
                path,
                "Provider.shared must be a string or a table",
            ));
        }
    };
    // Relative library paths are relative to the manifest.
    let library = if library.is_relative() {
        path.parent().unwrap_or(Path::new(".")).join(library)
    } else {
        library
    };
    let entrypoint = provider
        .get("entrypoint")
        .and_then(|v| v.get_ref().as_str())
        .map(|s| s.as_bytes().to_vec());

    let filesystem = table("ADBC")
        .and_then(|adbc| adbc.get("filesystem"))
        .and_then(|v| v.get_ref().as_table())
        .ok_or_else(|| manifest_error(path, "no [ADBC.filesystem]"))?;
    let schemes: Vec<String> = filesystem
        .get("schemes")
        .and_then(|v| v.get_ref().as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.get_ref().as_str())
                .map(str::to_ascii_lowercase)
                .collect()
        })
        .unwrap_or_default();
    if schemes.is_empty() {
        return Err(manifest_error(
            path,
            "ADBC.filesystem.schemes names no scheme",
        ));
    }
    let layered = filesystem
        .get("layered")
        .and_then(|v| v.get_ref().as_bool())
        .unwrap_or(false);

    let mut options = Vec::new();
    if let Some(table) = table("options") {
        for (key, value) in table.iter() {
            let value = match value.get_ref() {
                DeValue::String(s) => s.to_string(),
                DeValue::Integer(i) => i.as_str().to_string(),
                DeValue::Boolean(b) => b.to_string(),
                _ => {
                    return Err(manifest_error(
                        path,
                        format!("option '{}' must be a scalar", key.get_ref()),
                    ));
                }
            };
            options.push((key.get_ref().to_string(), value));
        }
    }
    Ok(ProviderManifest {
        library,
        entrypoint,
        schemes,
        layered,
        options,
    })
}

/// Load the provider a manifest describes and register its schemes.
/// `options` are applied after the manifest's own `[options]`, so a host
/// can override or add configuration (endpoints, credentials). Returns the
/// schemes registered.
pub fn load_provider_manifest(
    manifest: impl AsRef<Path>,
    options: &[(String, String)],
) -> Result<Vec<String>> {
    let mut parsed = read_provider_manifest(manifest.as_ref())?;
    parsed.options.extend(options.iter().cloned());
    load_provider(
        &parsed.library,
        parsed.entrypoint.as_deref(),
        &parsed.schemes,
        parsed.layered,
        &parsed.options,
    )?;
    Ok(parsed.schemes)
}

/// Load every provider manifest (a `*.toml` with a `[Provider]` table) in a
/// directory. Returns the schemes registered.
pub fn register_providers_in(dir: impl AsRef<Path>) -> Result<Vec<String>> {
    let mut schemes = Vec::new();
    for manifest in provider_manifests_in(dir.as_ref())? {
        schemes.extend(load_provider_manifest(&manifest, &[])?);
    }
    Ok(schemes)
}

fn provider_manifests_in(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut manifests: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(|e| io_error(dir, e))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "toml"))
        .filter(|p| {
            std::fs::read_to_string(p).ok().is_some_and(|c| {
                DeTable::parse(&c).is_ok_and(|t| t.get_ref().contains_key("Provider"))
            })
        })
        .collect();
    manifests.sort();
    Ok(manifests)
}

/// Find and load a provider for `scheme` on [PROVIDER_PATH_ENV], if any.
fn find_provider_on_path(scheme: &str) -> Result<()> {
    let Some(path) = std::env::var_os(PROVIDER_PATH_ENV) else {
        return Ok(());
    };
    for dir in std::env::split_paths(&path) {
        let Ok(manifests) = provider_manifests_in(&dir) else {
            continue;
        };
        for manifest in manifests {
            let parsed = read_provider_manifest(&manifest)?;
            if parsed.schemes.iter().any(|s| s == scheme) {
                load_provider_manifest(&manifest, &[])?;
                return Ok(());
            }
        }
    }
    Ok(())
}

/// The built-in local file system.
///
/// Unrooted (the default the registry hands out), names are local paths as
/// [Location] produces them, absolute or relative to the working
/// directory. Rooted, names are relative to the root and may not escape it.
#[derive(Clone, Debug, Default)]
pub struct LocalFileSystem {
    root: Option<PathBuf>,
}

impl LocalFileSystem {
    /// An unrooted local file system.
    pub fn new() -> Self {
        Self { root: None }
    }

    /// A local file system confined to `root`.
    pub fn rooted(root: impl Into<PathBuf>) -> Self {
        Self {
            root: Some(root.into()),
        }
    }

    fn path(&self, name: &str) -> Result<PathBuf> {
        let Some(root) = &self.root else {
            return Ok(PathBuf::from(name));
        };
        let relative = Path::new(name);
        for component in relative.components() {
            match component {
                Component::Normal(_) | Component::CurDir => {}
                _ => {
                    return Err(Error::with_message_and_status(
                        format!("name '{name}' escapes the file system root"),
                        Status::InvalidArguments,
                    ));
                }
            }
        }
        Ok(root.join(relative))
    }
}

fn io_error(path: &Path, e: std::io::Error) -> Error {
    let status = match e.kind() {
        std::io::ErrorKind::NotFound => Status::NotFound,
        std::io::ErrorKind::PermissionDenied => Status::Unauthorized,
        _ => Status::IO,
    };
    Error::with_message_and_status(format!("{}: {e}", path.display()), status)
}

/// A version token from the size and modification time.
fn version_token(metadata: &std::fs::Metadata) -> Option<Vec<u8>> {
    let modified = metadata.modified().ok()?.duration_since(UNIX_EPOCH).ok()?;
    let mut token = modified.as_nanos().to_le_bytes().to_vec();
    token.extend_from_slice(&metadata.len().to_le_bytes());
    Some(token)
}

fn file_info(metadata: &std::fs::Metadata) -> FileInfo {
    FileInfo {
        size: Some(metadata.len()),
        version: version_token(metadata),
    }
}

impl FileSystem for LocalFileSystem {
    fn open(&self, name: &str, _hints: u32) -> Result<Arc<dyn File>> {
        let path = self.path(name)?;
        let file = std::fs::File::open(&path).map_err(|e| io_error(&path, e))?;
        let metadata = file.metadata().map_err(|e| io_error(&path, e))?;
        if metadata.is_dir() {
            return Err(Error::with_message_and_status(
                format!("{} is a directory", path.display()),
                Status::InvalidArguments,
            ));
        }
        Ok(Arc::new(LocalFile {
            info: file_info(&metadata),
            file,
            path,
        }))
    }

    fn stat(&self, name: &str) -> Result<FileInfo> {
        let path = self.path(name)?;
        let metadata = std::fs::metadata(&path).map_err(|e| io_error(&path, e))?;
        Ok(file_info(&metadata))
    }

    fn list(&self, prefix: &str) -> Result<Box<dyn RecordBatchReader + Send>> {
        let path = self.path(if prefix.is_empty() { "." } else { prefix })?;
        let mut names = StringBuilder::new();
        let mut sizes = UInt64Builder::new();
        let mut version_values: Vec<Option<Vec<u8>>> = Vec::new();
        let mut directories = BooleanBuilder::new();
        let mut entries: Vec<_> = std::fs::read_dir(&path)
            .map_err(|e| io_error(&path, e))?
            .collect::<std::io::Result<_>>()
            .map_err(|e| io_error(&path, e))?;
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let metadata = entry.metadata().map_err(|e| io_error(&entry.path(), e))?;
            let file_name = entry.file_name().to_string_lossy().into_owned();
            let name = if prefix.is_empty() {
                file_name
            } else {
                format!("{}/{file_name}", prefix.trim_end_matches('/'))
            };
            names.append_value(name);
            if metadata.is_dir() {
                sizes.append_null();
                version_values.push(None);
            } else {
                sizes.append_value(metadata.len());
                version_values.push(version_token(&metadata));
            }
            directories.append_value(metadata.is_dir());
        }
        let versions = arrow_array::BinaryArray::from(
            version_values
                .iter()
                .map(|v| v.as_deref())
                .collect::<Vec<_>>(),
        );
        let batch = RecordBatch::try_new(
            list_schema(),
            vec![
                Arc::new(names.finish()),
                Arc::new(sizes.finish()),
                Arc::new(versions),
                Arc::new(directories.finish()),
            ],
        )?;
        Ok(Box::new(RecordBatchIterator::new(
            vec![Ok(batch)],
            list_schema(),
        )))
    }
}

/// An open local file. Reads are positional, so they may run concurrently.
struct LocalFile {
    file: std::fs::File,
    info: FileInfo,
    path: PathBuf,
}

impl LocalFile {
    /// Read into `buf` at `offset` until it is full or the file ends.
    fn read_at(&self, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<usize> {
        let mut total = 0;
        while !buf.is_empty() {
            #[cfg(unix)]
            let n = std::os::unix::fs::FileExt::read_at(&self.file, buf, offset)?;
            #[cfg(windows)]
            let n = std::os::windows::fs::FileExt::seek_read(&self.file, buf, offset)?;
            if n == 0 {
                break;
            }
            total += n;
            offset += n as u64;
            buf = &mut buf[n..];
        }
        Ok(total)
    }
}

impl File for LocalFile {
    fn info(&self) -> Result<FileInfo> {
        Ok(self.info.clone())
    }

    fn read_ranges(&self, requests: &mut [ReadRequest<'_>]) -> Result<()> {
        let size = self.info.size.unwrap_or(0);
        for request in requests {
            let (offset, length) = request.range.resolve(size)?;
            let n = self
                .read_at(&mut request.buffer[..length as usize], offset)
                .map_err(|e| io_error(&self.path, e))?;
            if (n as u64) < length {
                // The file shrank: it was not immutable while open.
                return Err(Error::with_message_and_status(
                    format!(
                        "{}: short read ({n} of {length} bytes at {offset})",
                        self.path.display()
                    ),
                    Status::IO,
                ));
            }
            request.bytes_read = n as u64;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::filesystem::ReadRange;

    #[test]
    fn rooted_names_cannot_escape() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.txt"), b"hello").unwrap();
        let fs = LocalFileSystem::rooted(dir.path());
        assert_eq!(fs.stat("a.txt").unwrap().size, Some(5));
        assert_eq!(fs.stat("./a.txt").unwrap().size, Some(5));
        for bad in [
            "../a.txt",
            "sub/../../a.txt",
            "/etc/passwd",
            "C:/Windows/win.ini",
        ] {
            let err = fs.stat(bad).unwrap_err();
            assert_eq!(err.status, Status::InvalidArguments, "{bad}");
        }
    }

    #[test]
    fn read_and_list() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("b.bin"), (0..=255u8).collect::<Vec<_>>()).unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        let fs = LocalFileSystem::rooted(dir.path());

        let file = fs.open("b.bin", 0).unwrap();
        let mut buf = [0u8; 4];
        let mut requests = [ReadRequest {
            range: ReadRange::suffix(4),
            buffer: &mut buf,
            bytes_read: 0,
        }];
        file.read_ranges(&mut requests).unwrap();
        assert_eq!(buf, [252, 253, 254, 255]);
        assert_eq!(
            fs.open("missing", 0).err().unwrap().status,
            Status::NotFound
        );

        let batches: Vec<RecordBatch> = fs.list("").unwrap().map(|b| b.unwrap()).collect();
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "b.bin");
        assert_eq!(names.value(1), "sub");
    }

    #[test]
    fn local_provider_conforms_through_c() {
        let dir = tempfile::TempDir::new().unwrap();
        let data: Vec<u8> = (0..5000u32).map(|i| (i * 13 % 251) as u8).collect();
        std::fs::write(dir.path().join("data.bin"), &data).unwrap();
        for fs in [LocalFileSystem::rooted(dir.path()), LocalFileSystem::new()] {
            let (name, missing) = if fs.root.is_some() {
                ("data.bin".to_string(), "missing.bin".to_string())
            } else {
                let path = |f: &str| {
                    Location::parse(dir.path().join(f).to_str().unwrap())
                        .unwrap()
                        .path
                };
                (path("data.bin"), path("missing.bin"))
            };
            let mut raw = adbc_ffi::filesystem::export_file_system(Arc::new(fs));
            unsafe {
                adbc_ffi::filesystem::conformance::check_file_system(
                    &mut raw, &name, &missing, &data,
                )
                .unwrap();
                (raw.release.unwrap())(&mut raw);
            }
        }
    }

    /// What the no-inline-completion rule costs on local reads (file-drivers.md
    /// §6). Run with `cargo test --release -p adbc_driver_manager --lib
    /// thread_hop -- --ignored --nocapture`.
    #[test]
    #[ignore = "benchmark"]
    fn thread_hop_cost() {
        use adbc_core::filesystem::{AsyncRead, ReadCompletion, submit_reads};
        use std::sync::mpsc;
        use std::time::Instant;

        let dir = tempfile::TempDir::new().unwrap();
        let data = vec![7u8; 4 << 20];
        std::fs::write(dir.path().join("data.bin"), &data).unwrap();
        let fs = LocalFileSystem::rooted(dir.path());
        let mut raw = adbc_ffi::filesystem::export_file_system(Arc::new(fs.clone()));
        let through_c = unsafe { adbc_ffi::filesystem::import_file_system(&mut raw).unwrap() };
        let iterations = 20_000u64;
        let block = 4096u64;
        let offset = |i: u64| (i * 7919 * block) % (data.len() as u64 - block);

        for (label, fs) in [
            ("rust", Arc::new(fs) as Arc<dyn FileSystem>),
            ("via C", through_c),
        ] {
            let file = fs.open("data.bin", 0).unwrap();
            let mut buf = vec![0u8; block as usize];

            let start = Instant::now();
            for i in 0..iterations {
                let mut request = [ReadRequest {
                    range: ReadRange::exact(offset(i), block),
                    buffer: &mut buf,
                    bytes_read: 0,
                }];
                file.read_ranges(&mut request).unwrap();
            }
            let blocking = start.elapsed() / iterations as u32;

            let (tx, rx) = mpsc::channel::<ReadCompletion>();
            let tx = std::sync::Mutex::new(tx);
            let callback = Arc::new(move |c: ReadCompletion| tx.lock().unwrap().send(c).unwrap());
            let start = Instant::now();
            for i in 0..iterations {
                let read = AsyncRead {
                    range: ReadRange::exact(offset(i), block),
                    buffer: vec![0; block as usize],
                    user_data: i,
                };
                submit_reads(&file, vec![read], callback.clone()).unwrap();
                rx.recv().unwrap().result.unwrap();
            }
            let async_one = start.elapsed() / iterations as u32;

            let start = Instant::now();
            let reads = (0..iterations)
                .map(|i| AsyncRead {
                    range: ReadRange::exact(offset(i), block),
                    buffer: vec![0; block as usize],
                    user_data: i,
                })
                .collect();
            submit_reads(&file, reads, callback.clone()).unwrap();
            for _ in 0..iterations {
                rx.recv().unwrap().result.unwrap();
            }
            let async_batch = start.elapsed() / iterations as u32;

            println!(
                "{label:>6}: 4 KiB read: blocking {blocking:?}, async one-at-a-time \
                 {async_one:?}, async batched {async_batch:?} (per read)"
            );
        }
    }

    #[test]
    fn registry_resolves_file_scheme() {
        let fs = resolve(&Location::parse("C:/x.xlsx").unwrap()).unwrap();
        // Unrooted: a missing absolute path is simply not found.
        let dir = tempfile::TempDir::new().unwrap();
        let missing = dir.path().join("missing.xlsx");
        let name = Location::parse(missing.to_str().unwrap()).unwrap().path;
        assert_eq!(fs.stat(&name).unwrap_err().status, Status::NotFound);
        let err = resolve(&Location::parse("nope://x/y").unwrap())
            .err()
            .unwrap();
        assert_eq!(err.status, Status::NotFound);
    }
}
