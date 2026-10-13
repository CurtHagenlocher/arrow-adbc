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

//! An S3 file-system provider over the `object_store` crate.
//!
//! PROTOTYPE for the ADBC file-driver proposal. It is the first provider
//! that is *async-native*: every read is an HTTP range request on the
//! provider's own Tokio runtime, completed through the read callback.
//! Blocking reads are built on top with
//! [read_ranges_via_submit](adbc_core::filesystem::read_ranges_via_submit).
//!
//! - One store per bucket: `s3://bucket/key` opens the store for `bucket`;
//!   names are keys.
//! - A file is pinned to the ETag seen at open: reads send `If-Match`, so an
//!   object replaced mid-scan fails instead of mixing versions.
//! - Configuration comes from the environment (`AWS_*`) and from provider
//!   options, which take `object_store`'s S3 keys (`aws_endpoint`,
//!   `aws_region`, `aws_allow_http`, `aws_access_key_id`, ...). The option
//!   `trace = true` logs every request to stderr.
//!
//! Loaded from a provider manifest with the entrypoint
//! `AdbcFileSystemProviderInit`.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use arrow_array::builder::{BinaryBuilder, BooleanBuilder, StringBuilder, UInt64Builder};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};
use tokio::sync::Notify;

use adbc_core::error::{Error, Result, Status};
use adbc_core::filesystem::{
    AsyncRead, File, FileInfo, FileSystem, FileSystemProvider, ReadCallback, ReadCompletion,
    ReadRequest, list_schema, read_ranges_via_submit,
};

/// The provider's runtime: all requests run here, whoever submits them.
fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("adbc-s3")
            .enable_all()
            .build()
            .expect("cannot start the S3 provider's runtime")
    })
}

fn store_error(e: object_store::Error) -> Error {
    let status = match &e {
        object_store::Error::NotFound { .. } => Status::NotFound,
        object_store::Error::Precondition { .. } => Status::InvalidState,
        object_store::Error::PermissionDenied { .. }
        | object_store::Error::Unauthenticated { .. } => Status::Unauthorized,
        object_store::Error::NotImplemented { .. } => Status::NotImplemented,
        _ => Status::IO,
    };
    Error::with_message_and_status(format!("s3: {e}"), status)
}

/// The `s3` provider: configuration shared by the stores it opens.
#[derive(Default)]
pub struct S3Provider {
    options: Vec<(AmazonS3ConfigKey, String)>,
    trace: bool,
}

impl FileSystemProvider for S3Provider {
    fn set_option(&mut self, key: &str, value: &str) -> Result<()> {
        if key == "trace" {
            self.trace = value == "true";
            return Ok(());
        }
        let key: AmazonS3ConfigKey = key.parse().map_err(|_| {
            Error::with_message_and_status(
                format!("s3: unknown option '{key}'"),
                Status::NotImplemented,
            )
        })?;
        self.options.push((key, value.to_string()));
        Ok(())
    }

    fn open(
        &self,
        scheme: &str,
        authority: &str,
        _parent: Option<(Arc<dyn FileSystem>, String)>,
    ) -> Result<Arc<dyn FileSystem>> {
        if scheme != "s3" {
            return Err(Error::with_message_and_status(
                format!("s3: cannot open scheme '{scheme}'"),
                Status::InvalidArguments,
            ));
        }
        if authority.is_empty() {
            return Err(Error::with_message_and_status(
                "s3: the location names no bucket (s3://bucket/key)",
                Status::InvalidArguments,
            ));
        }
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(authority);
        for (key, value) in &self.options {
            builder = builder.with_config(*key, value);
        }
        let store = builder.build().map_err(store_error)?;
        Ok(Arc::new(S3FileSystem {
            store: Arc::new(store),
            bucket: authority.to_string(),
            trace: self.trace,
        }))
    }
}

adbc_ffi::export_file_system_provider!(AdbcFileSystemProviderInit, S3Provider);

/// One bucket.
pub struct S3FileSystem {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    trace: bool,
}

fn key(name: &str) -> Path {
    Path::from(name.trim_start_matches('/'))
}

fn version_of(meta: &object_store::ObjectMeta) -> Option<Vec<u8>> {
    meta.e_tag
        .as_ref()
        .or(meta.version.as_ref())
        .map(|v| v.as_bytes().to_vec())
}

impl S3FileSystem {
    fn head(&self, name: &str) -> Result<object_store::ObjectMeta> {
        let path = key(name);
        if self.trace {
            eprintln!("[s3] HEAD s3://{}/{path}", self.bucket);
        }
        runtime()
            .block_on(self.store.head(&path))
            .map_err(store_error)
    }
}

impl FileSystem for S3FileSystem {
    fn open(&self, name: &str, _hints: u32) -> Result<Arc<dyn File>> {
        let meta = self.head(name)?;
        Ok(Arc::new(S3File {
            store: self.store.clone(),
            label: format!("s3://{}/{}", self.bucket, meta.location),
            info: FileInfo {
                size: Some(meta.size),
                version: version_of(&meta),
            },
            e_tag: meta.e_tag.clone(),
            path: meta.location,
            trace: self.trace,
            in_flight: Arc::new(InFlight::default()),
        }))
    }

    fn stat(&self, name: &str) -> Result<FileInfo> {
        let meta = self.head(name)?;
        Ok(FileInfo {
            size: Some(meta.size),
            version: version_of(&meta),
        })
    }

    fn list(&self, prefix: &str) -> Result<Box<dyn RecordBatchReader + Send>> {
        let prefix = prefix.trim_matches('/');
        let path = (!prefix.is_empty()).then(|| key(prefix));
        if self.trace {
            eprintln!("[s3] LIST s3://{}/{prefix}", self.bucket);
        }
        let listing = runtime()
            .block_on(self.store.list_with_delimiter(path.as_ref()))
            .map_err(store_error)?;
        let mut names = StringBuilder::new();
        let mut sizes = UInt64Builder::new();
        let mut versions = BinaryBuilder::new();
        let mut is_dir = BooleanBuilder::new();
        for dir in &listing.common_prefixes {
            names.append_value(dir.as_ref());
            sizes.append_null();
            versions.append_null();
            is_dir.append_value(true);
        }
        for object in &listing.objects {
            names.append_value(object.location.as_ref());
            sizes.append_value(object.size);
            versions.append_option(version_of(object));
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

/// Reads not yet completed, so that dropping a file can wait for them; and
/// their cancellation signals.
#[derive(Default)]
struct InFlight {
    reads: Mutex<HashMap<u64, Vec<Arc<Notify>>>>,
    count: Mutex<usize>,
    idle: Condvar,
}

impl InFlight {
    fn start(&self, user_data: u64) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        self.reads
            .lock()
            .unwrap()
            .entry(user_data)
            .or_default()
            .push(notify.clone());
        *self.count.lock().unwrap() += 1;
        notify
    }

    fn finish(&self, user_data: u64, notify: &Arc<Notify>) {
        {
            let mut reads = self.reads.lock().unwrap();
            if let Some(list) = reads.get_mut(&user_data) {
                list.retain(|n| !Arc::ptr_eq(n, notify));
                if list.is_empty() {
                    reads.remove(&user_data);
                }
            }
        }
        let mut count = self.count.lock().unwrap();
        *count -= 1;
        if *count == 0 {
            self.idle.notify_all();
        }
    }

    fn cancel(&self, user_data: Option<u64>) {
        let reads = self.reads.lock().unwrap();
        for (key, list) in reads.iter() {
            if user_data.is_none_or(|u| u == *key) {
                list.iter().for_each(|n| n.notify_one());
            }
        }
    }

    fn wait_idle(&self) {
        let mut count = self.count.lock().unwrap();
        while *count > 0 {
            count = self.idle.wait(count).unwrap();
        }
    }
}

/// One object, pinned to the ETag seen at open.
struct S3File {
    store: Arc<dyn ObjectStore>,
    path: Path,
    label: String,
    info: FileInfo,
    e_tag: Option<String>,
    trace: bool,
    in_flight: Arc<InFlight>,
}

impl Drop for S3File {
    fn drop(&mut self) {
        // No callback may run after the file is gone.
        self.in_flight.cancel(None);
        self.in_flight.wait_idle();
    }
}

impl File for S3File {
    fn info(&self) -> Result<FileInfo> {
        Ok(self.info.clone())
    }

    fn read_ranges(&self, requests: &mut [ReadRequest<'_>]) -> Result<()> {
        // Blocking over async.
        read_ranges_via_submit(self, requests)
    }

    fn submit_reads(&self, reads: Vec<AsyncRead>, callback: Arc<dyn ReadCallback>) -> Result<()> {
        let size = self.info.size.unwrap_or(0);
        for read in reads {
            let resolved = read.range.resolve(size);
            let notify = self.in_flight.start(read.user_data);
            let in_flight = self.in_flight.clone();
            let store = self.store.clone();
            let path = self.path.clone();
            let e_tag = self.e_tag.clone();
            let callback = callback.clone();
            let trace = self.trace.then(|| self.label.clone());
            runtime().spawn(async move {
                let AsyncRead {
                    mut buffer,
                    user_data,
                    ..
                } = read;
                let fetch = async {
                    let (offset, length) = resolved?;
                    if length == 0 {
                        return Ok(0);
                    }
                    if let Some(label) = &trace {
                        eprintln!(
                            "[s3] GET {label} bytes={offset}-{} ({length} bytes)",
                            offset + length - 1
                        );
                    }
                    let options = GetOptions {
                        range: Some(GetRange::Bounded(offset..offset + length)),
                        if_match: e_tag,
                        ..Default::default()
                    };
                    let bytes = store
                        .get_opts(&path, options)
                        .await
                        .map_err(store_error)?
                        .bytes()
                        .await
                        .map_err(store_error)?;
                    if bytes.len() as u64 != length {
                        return Err(Error::with_message_and_status(
                            format!("s3: short read ({} of {length} bytes)", bytes.len()),
                            Status::IO,
                        ));
                    }
                    buffer[..bytes.len()].copy_from_slice(&bytes);
                    Ok(length)
                };
                let result = tokio::select! {
                    result = fetch => result,
                    _ = notify.notified() => Err(Error::with_message_and_status(
                        "s3: read cancelled",
                        Status::Cancelled,
                    )),
                };
                callback.complete(ReadCompletion {
                    user_data,
                    buffer,
                    result,
                });
                in_flight.finish(user_data, &notify);
            });
        }
        Ok(())
    }

    fn has_native_submit(&self) -> bool {
        true
    }

    fn cancel(&self, user_data: u64) {
        self.in_flight.cancel(Some(user_data));
    }
}
