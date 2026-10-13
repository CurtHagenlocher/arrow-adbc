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

//! The S3 and ZIP providers loaded as shared libraries from provider
//! manifests, composed by the driver manager's registry (PROTOTYPE).
//!
//! Needs an S3-compatible endpoint that accepts any credentials, e.g.
//! `moto_server -p 5555`, named by `ADBC_S3_TEST_ENDPOINT`
//! (`http://127.0.0.1:5555`); otherwise the tests are skipped. The provider
//! libraries are found next to the test executable (build them with
//! `cargo build -p adbc_filesystem_object_store -p adbc_filesystem_zip`).

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use adbc_core::error::Status;
use adbc_core::filesystem::{FileReader, FileSystem, Location};
use adbc_driver_manager::filesystem;
use arrow_array::Array;
use arrow_array::cast::AsArray;
use object_store::aws::AmazonS3Builder;
use object_store::{ObjectStoreExt, PutPayload};

const BUCKET: &str = "adbc-provider-test";

/// The provider library `name` from this build.
fn library(name: &str) -> PathBuf {
    let file = libloading_name(name);
    let exe = std::env::current_exe().unwrap();
    let deps = exe.parent().unwrap();
    for dir in [deps.parent().unwrap(), deps] {
        if dir.join(&file).exists() {
            return dir.join(&file);
        }
    }
    panic!("{file} not found; run `cargo build -p {name}` first")
}

fn libloading_name(name: &str) -> String {
    if cfg!(windows) {
        format!("{name}.dll")
    } else if cfg!(target_os = "macos") {
        format!("lib{name}.dylib")
    } else {
        format!("lib{name}.so")
    }
}

/// Create the bucket with a bare HTTP request (object_store cannot).
fn create_bucket(endpoint: &str) {
    let host = endpoint.trim_start_matches("http://").trim_end_matches('/');
    let mut stream = std::net::TcpStream::connect(host).unwrap();
    write!(
        stream,
        "PUT /{BUCKET} HTTP/1.1\r\nHost: {host}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(
        response.starts_with("HTTP/1.1 200") || response.contains("BucketAlreadyOwnedByYou"),
        "{response}"
    );
}

fn archive() -> Vec<u8> {
    use zip_writer::*;
    build(&[
        ("orders/2024-01.csv", b"id,qty\n1,5\n2,7\n".to_vec()),
        ("orders/2024-02.csv", b"id,qty\n3,1\n".to_vec()),
    ])
}

/// A minimal stored-only ZIP writer, so the test needs no zip crate.
mod zip_writer {
    fn crc32(data: &[u8]) -> u32 {
        let mut crc = 0xFFFF_FFFFu32;
        for &b in data {
            crc ^= b as u32;
            for _ in 0..8 {
                crc = if crc & 1 != 0 {
                    (crc >> 1) ^ 0xEDB8_8320
                } else {
                    crc >> 1
                };
            }
        }
        !crc
    }

    pub fn build(files: &[(&str, Vec<u8>)]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut central = Vec::new();
        for (name, data) in files {
            let offset = out.len() as u32;
            let crc = crc32(data);
            let header = |sig: u32, central: bool| {
                let mut h = Vec::new();
                h.extend_from_slice(&sig.to_le_bytes());
                if central {
                    h.extend_from_slice(&20u16.to_le_bytes()); // made by
                }
                h.extend_from_slice(&20u16.to_le_bytes()); // needed
                h.extend_from_slice(&0u16.to_le_bytes()); // flags
                h.extend_from_slice(&0u16.to_le_bytes()); // stored
                h.extend_from_slice(&0u32.to_le_bytes()); // time, date
                h.extend_from_slice(&crc.to_le_bytes());
                h.extend_from_slice(&(data.len() as u32).to_le_bytes());
                h.extend_from_slice(&(data.len() as u32).to_le_bytes());
                h.extend_from_slice(&(name.len() as u16).to_le_bytes());
                h.extend_from_slice(&0u16.to_le_bytes()); // extra
                if central {
                    h.extend_from_slice(&[0; 6]); // comment, disk, internal attrs
                    h.extend_from_slice(&0u32.to_le_bytes()); // external attrs
                    h.extend_from_slice(&offset.to_le_bytes());
                }
                h.extend_from_slice(name.as_bytes());
                h
            };
            out.extend(header(0x0403_4b50, false));
            out.extend_from_slice(data);
            central.extend(header(0x0201_4b50, true));
        }
        let central_offset = out.len() as u32;
        out.extend_from_slice(&central);
        out.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
        out.extend_from_slice(&[0; 4]);
        out.extend_from_slice(&(files.len() as u16).to_le_bytes());
        out.extend_from_slice(&(files.len() as u16).to_le_bytes());
        out.extend_from_slice(&(central.len() as u32).to_le_bytes());
        out.extend_from_slice(&central_offset.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out
    }
}

struct Setup {
    _dir: tempfile::TempDir,
    data: Vec<u8>,
}

/// Upload the fixtures and load both providers from manifests, once.
fn setup() -> Option<&'static Setup> {
    let endpoint = std::env::var("ADBC_S3_TEST_ENDPOINT").ok()?;
    static SETUP: OnceLock<Setup> = OnceLock::new();
    Some(SETUP.get_or_init(|| {
        create_bucket(&endpoint);
        let store = AmazonS3Builder::new()
            .with_endpoint(&endpoint)
            .with_allow_http(true)
            .with_region("us-east-1")
            .with_access_key_id("test")
            .with_secret_access_key("test")
            .with_bucket_name(BUCKET)
            .build()
            .unwrap();
        let data: Vec<u8> = (0..300_000u32).map(|i| (i * 31 % 251) as u8).collect();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            store
                .put(&"data.bin".into(), PutPayload::from(data.clone()))
                .await
                .unwrap();
            store
                .put(&"nested/a.zip".into(), PutPayload::from(archive()))
                .await
                .unwrap();
        });

        let dir = tempfile::TempDir::new().unwrap();
        write_manifest(
            dir.path(),
            "s3",
            &library("adbc_filesystem_object_store"),
            false,
            &format!(
                "aws_endpoint = \"{endpoint}\"\naws_allow_http = \"true\"\n\
                 aws_region = \"us-east-1\"\naws_access_key_id = \"test\"\n\
                 aws_secret_access_key = \"test\"\n"
            ),
        );
        write_manifest(dir.path(), "zip", &library("adbc_filesystem_zip"), true, "");
        let mut schemes = filesystem::register_providers_in(dir.path()).unwrap();
        schemes.sort();
        assert_eq!(schemes, ["s3", "zip"]);
        Setup { _dir: dir, data }
    }))
}

fn write_manifest(dir: &Path, scheme: &str, library: &Path, layered: bool, options: &str) {
    let library = library.to_str().unwrap().replace('\\', "/");
    std::fs::write(
        dir.join(format!("{scheme}.toml")),
        format!(
            "manifest_version = 1\nname = \"{scheme} provider\"\n\n\
             [ADBC.filesystem]\nschemes = [\"{scheme}\"]\nlayered = {layered}\n\n\
             [Provider]\nshared = \"{library}\"\n\n[options]\n{options}"
        ),
    )
    .unwrap();
}

fn read_all(fs: &dyn FileSystem, name: &str) -> Vec<u8> {
    let mut out = Vec::new();
    FileReader::new(fs.open(name, 0).unwrap())
        .unwrap()
        .read_to_end(&mut out)
        .unwrap();
    out
}

#[test]
fn s3_provider_conforms_through_c() {
    let Some(setup) = setup() else {
        eprintln!("skipping: set ADBC_S3_TEST_ENDPOINT");
        return;
    };
    let location = Location::parse(&format!("s3://{BUCKET}/data.bin")).unwrap();
    let fs = filesystem::resolve(&location).unwrap();
    assert_eq!(read_all(fs.as_ref(), &location.path), setup.data);
    assert_eq!(fs.stat("missing.bin").unwrap_err().status, Status::NotFound);

    // The loaded provider, exported again and run through the kit: exact,
    // suffix and up-to reads, exactly-once callbacks, close waits.
    let mut raw = adbc_ffi::filesystem::export_file_system(fs);
    unsafe {
        adbc_ffi::filesystem::conformance::check_file_system(
            &mut raw,
            "data.bin",
            "missing.bin",
            &setup.data,
        )
        .unwrap();
        (raw.release.unwrap())(&mut raw);
    }
}

#[test]
fn zip_over_s3_composes_through_the_registry() {
    if setup().is_none() {
        return;
    }
    // Neither provider knows about the other: the driver manager resolves
    // the archive's location with the s3 provider and hands it to zip.
    let location = Location::parse(&format!("zip:s3://{BUCKET}/nested/a.zip!/orders")).unwrap();
    let fs: Arc<dyn FileSystem> = filesystem::resolve(&location).unwrap();
    let batches: Vec<_> = fs
        .list(&location.path)
        .unwrap()
        .map(|b| b.unwrap())
        .collect();
    let names = batches[0].column(0).as_string::<i32>();
    assert_eq!(
        (0..names.len()).map(|i| names.value(i)).collect::<Vec<_>>(),
        ["orders/2024-01.csv", "orders/2024-02.csv"]
    );
    assert_eq!(
        read_all(fs.as_ref(), "orders/2024-01.csv"),
        b"id,qty\n1,5\n2,7\n"
    );
}
