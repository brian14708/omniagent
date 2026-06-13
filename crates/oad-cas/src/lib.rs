//! Content-addressed storage (CAS) for `oad`'s large binary artifacts.
//!
//! `oad` produces two large, node-local binary artifacts that must become
//! portable for a distributed runtime: the `EROFS` rootfs image and the `runsc`
//! checkpoint image. This crate splits such a file into variable-size chunks
//! with content-defined chunking (`FastCDC`), addresses each chunk by the
//! `blake3` hash of its *uncompressed* bytes, compresses chunks with `zstd`, and
//! stores them in a [`ChunkStore`]. A [`ChunkRecipe`] records the ordered list
//! of chunks needed to reassemble the exact original file on any node.
//!
//! Two operations form the core of the crate:
//! - [`chunk_and_upload`] chunks a file, asks the store which chunks are already
//!   present (so only the delta is transferred), and uploads the rest.
//! - [`materialize`] reassembles a file from its recipe, pulling each chunk from
//!   the store, verifying integrity, and publishing the result atomically.
//!
//! Addressing by the *uncompressed* hash means identical plaintext deduplicates
//! regardless of compression settings. Stored chunk objects are self-describing
//! (a one-byte codec header), so a consumer decodes a chunk without consulting
//! the recipe's codec — robust to chunks shared across recipes built with
//! different settings.

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{StreamExt, TryStreamExt, stream};
use reqwest::StatusCode;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use url::Url;

/// Hash algorithm used to content-address chunks and whole files.
pub const CAS_ALGO: &str = "blake3";

/// Version tag recorded in recipes. Bump when the chunking or encoding format
/// changes in a way that should invalidate reuse of previously built recipes.
pub const CAS_BUILDER_VERSION: &str = "oad-cas-1";

/// Default minimum `FastCDC` chunk size (256 `KiB`).
pub const DEFAULT_MIN_SIZE: u32 = 256 * 1024;
/// Default average (target) `FastCDC` chunk size (1 `MiB`).
pub const DEFAULT_AVG_SIZE: u32 = 1024 * 1024;
/// Default maximum `FastCDC` chunk size (4 `MiB`).
pub const DEFAULT_MAX_SIZE: u32 = 4 * 1024 * 1024;

/// Maximum number of chunk uploads in flight at once.
const UPLOAD_CONCURRENCY: usize = 8;

/// Stored-object codec header byte: payload is the raw chunk bytes.
const CODEC_NONE: u8 = 0;
/// Stored-object codec header byte: payload is `zstd`-compressed chunk bytes.
const CODEC_ZSTD: u8 = 1;

/// Errors produced by chunking, storage, and reassembly.
#[derive(Debug, thiserror::Error)]
pub enum CasError {
    /// An underlying I/O operation failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// The `FastCDC` chunker reported an error while reading the source.
    #[error("chunking failed: {0}")]
    Chunk(String),
    /// A hex string was not a valid 32-byte `blake3` hash.
    #[error("invalid chunk hash: {0}")]
    InvalidHash(String),
    /// A value did not fit in its on-recipe representation.
    #[error("value too large: {0}")]
    TooLarge(String),
    /// A requested chunk was not present in the store.
    #[error("chunk not found: {0}")]
    NotFound(ChunkHash),
    /// A requested named object (recipe/descriptor) was not present.
    #[error("object not found: {0}")]
    ObjectNotFound(String),
    /// A fetched chunk did not hash to the address it was stored under.
    #[error("integrity check failed for chunk {0}")]
    Integrity(ChunkHash),
    /// A reassembled file did not match the recipe's whole-file hash.
    #[error("reassembled file hash mismatch: expected {expected}, got {actual}")]
    FileHashMismatch {
        /// Hash recorded in the recipe.
        expected: ChunkHash,
        /// Hash of the bytes actually reassembled.
        actual: ChunkHash,
    },
    /// A recipe was structurally invalid (e.g. non-contiguous chunk offsets).
    #[error("invalid recipe: {0}")]
    InvalidRecipe(String),
    /// The backing store reported a failure.
    #[error("store error: {0}")]
    Store(String),
}

/// A `blake3` hash addressing a chunk (of its uncompressed bytes) or a whole
/// reassembled file.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkHash([u8; 32]);

impl ChunkHash {
    /// Wraps raw hash bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrows the raw hash bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns the lowercase hex encoding of the hash.
    #[must_use]
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parses a hash from its lowercase hex encoding.
    ///
    /// # Errors
    ///
    /// Returns [`CasError::InvalidHash`] if `value` is not valid hex or does not
    /// decode to exactly 32 bytes.
    pub fn from_hex(value: &str) -> Result<Self, CasError> {
        let bytes = hex::decode(value).map_err(|err| CasError::InvalidHash(err.to_string()))?;
        let len = bytes.len();
        let array: [u8; 32] = bytes.try_into().map_err(|_| {
            CasError::InvalidHash(format!("expected 32-byte hash, got {len} bytes"))
        })?;
        Ok(Self(array))
    }
}

impl fmt::Display for ChunkHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for ChunkHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChunkHash({})", self.to_hex())
    }
}

impl FromStr for ChunkHash {
    type Err = CasError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::from_hex(value)
    }
}

impl Serialize for ChunkHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for ChunkHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::from_hex(&value).map_err(serde::de::Error::custom)
    }
}

/// Computes the `blake3` hash of `data`.
#[must_use]
fn blake3_of(data: &[u8]) -> ChunkHash {
    ChunkHash(*blake3::hash(data).as_bytes())
}

/// One entry in a [`ChunkRecipe`]: a chunk's address and its placement in the
/// reassembled file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRef {
    /// `blake3` hash of the chunk's uncompressed bytes (its address).
    pub hash: ChunkHash,
    /// Byte offset of the chunk within the reassembled file.
    pub offset: u64,
    /// Uncompressed length of the chunk in bytes.
    pub len: u32,
}

/// The ordered set of chunks needed to reassemble one file, plus a whole-file
/// hash for end-to-end verification.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRecipe {
    /// Hash algorithm (always [`CAS_ALGO`] for now).
    pub algo: String,
    /// Builder version tag ([`CAS_BUILDER_VERSION`]).
    pub builder_version: String,
    /// Total uncompressed length of the reassembled file.
    pub total_len: u64,
    /// `blake3` hash of the whole reassembled file.
    pub file_hash: ChunkHash,
    /// Chunks in file order; offsets are contiguous and cover `[0, total_len)`.
    pub chunks: Vec<ChunkRef>,
}

impl ChunkRecipe {
    /// Returns the chunk hashes in file order, including duplicates.
    #[must_use]
    pub fn chunk_hashes(&self) -> Vec<ChunkHash> {
        self.chunks.iter().map(|chunk| chunk.hash).collect()
    }

    /// Returns the distinct chunk hashes in first-seen order.
    #[must_use]
    pub fn unique_chunk_hashes(&self) -> Vec<ChunkHash> {
        let mut seen = std::collections::HashSet::new();
        self.chunks
            .iter()
            .filter_map(|chunk| seen.insert(chunk.hash).then_some(chunk.hash))
            .collect()
    }
}

/// `FastCDC` chunk-size parameters.
#[derive(Clone, Copy, Debug)]
pub struct ChunkerParams {
    /// Minimum chunk size in bytes.
    pub min: u32,
    /// Average (target) chunk size in bytes.
    pub avg: u32,
    /// Maximum chunk size in bytes.
    pub max: u32,
}

impl Default for ChunkerParams {
    fn default() -> Self {
        Self {
            min: DEFAULT_MIN_SIZE,
            avg: DEFAULT_AVG_SIZE,
            max: DEFAULT_MAX_SIZE,
        }
    }
}

/// A content-addressed store of opaque chunk objects.
///
/// Implementors map a [`ChunkHash`] to a stored object (a codec header plus a
/// possibly-compressed payload). Objects are immutable and idempotent to write:
/// the same hash always maps to bytes that decode to the same plaintext.
pub trait ChunkStore: Send + Sync {
    /// Reports, for each input hash, whether the store already holds it.
    ///
    /// The returned vector is parallel to `hashes`.
    ///
    /// # Errors
    ///
    /// Returns [`CasError`] if the store cannot be queried.
    fn has_chunks(
        &self,
        hashes: &[ChunkHash],
    ) -> impl std::future::Future<Output = Result<Vec<bool>, CasError>> + Send;

    /// Stores `body` (an encoded chunk object) under `hash`. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`CasError`] if the object cannot be written.
    fn put_chunk(
        &self,
        hash: &ChunkHash,
        body: &[u8],
    ) -> impl std::future::Future<Output = Result<(), CasError>> + Send;

    /// Fetches the encoded chunk object stored under `hash`.
    ///
    /// # Errors
    ///
    /// Returns [`CasError::NotFound`] if the chunk is absent, or another
    /// [`CasError`] on transport failure.
    fn get_chunk(
        &self,
        hash: &ChunkHash,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, CasError>> + Send;

    /// Stores a named object (e.g. a recipe or snapshot descriptor) at `key`.
    /// Unlike chunks, named objects are mutable-by-key and not content-addressed.
    ///
    /// # Errors
    ///
    /// Returns [`CasError`] if the object cannot be written.
    fn put_object(
        &self,
        key: &str,
        body: &[u8],
    ) -> impl std::future::Future<Output = Result<(), CasError>> + Send;

    /// Fetches the named object stored at `key`.
    ///
    /// # Errors
    ///
    /// Returns [`CasError::ObjectNotFound`] if the object is absent, or another
    /// [`CasError`] on failure.
    fn get_object(
        &self,
        key: &str,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, CasError>> + Send;
}

/// Object key for a chunk recipe.
///
/// Content-addressed by the recipe's whole-file hash:
/// `recipes/blake3/<aa>/<file-hash>.json`. Identical files (same chunker)
/// produce identical recipes, so recipes deduplicate too.
#[must_use]
pub fn recipe_object_key(recipe: &ChunkRecipe) -> String {
    let hex = recipe.file_hash.to_hex();
    format!("recipes/blake3/{}/{hex}.json", &hex[0..2])
}

/// Object key for a snapshot descriptor: `descriptors/<name>.json`. `name` must
/// be a validated snapshot name (path-safe).
#[must_use]
pub fn descriptor_object_key(name: &str) -> String {
    format!("descriptors/{name}.json")
}

/// A [`ChunkStore`] backed by a local directory tree.
///
/// Chunks are stored at `<root>/chunks/blake3/<aa>/<full-hex>`, where `aa` is
/// the first byte of the hash in hex — a fan-out that keeps directories small.
/// Used directly in tests and as the basis for a node's local chunk cache.
#[derive(Clone, Debug)]
pub struct FsChunkStore {
    root: PathBuf,
}

impl FsChunkStore {
    /// Creates a store rooted at `root` (created lazily on first write).
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Resolves the on-disk path for a chunk.
    #[must_use]
    fn chunk_path(&self, hash: &ChunkHash) -> PathBuf {
        let hex = hash.to_hex();
        self.root
            .join("chunks")
            .join("blake3")
            .join(&hex[0..2])
            .join(&hex)
    }
}

impl ChunkStore for FsChunkStore {
    async fn has_chunks(&self, hashes: &[ChunkHash]) -> Result<Vec<bool>, CasError> {
        let mut present = Vec::with_capacity(hashes.len());
        for hash in hashes {
            present.push(tokio::fs::try_exists(self.chunk_path(hash)).await?);
        }
        Ok(present)
    }

    async fn put_chunk(&self, hash: &ChunkHash, body: &[u8]) -> Result<(), CasError> {
        oad_core::write_atomic_file(&self.chunk_path(hash), body).await?;
        Ok(())
    }

    async fn get_chunk(&self, hash: &ChunkHash) -> Result<Vec<u8>, CasError> {
        match tokio::fs::read(self.chunk_path(hash)).await {
            Ok(body) => Ok(body),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Err(CasError::NotFound(*hash))
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn put_object(&self, key: &str, body: &[u8]) -> Result<(), CasError> {
        oad_core::write_atomic_file(&self.root.join(key), body).await?;
        Ok(())
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>, CasError> {
        match tokio::fs::read(self.root.join(key)).await {
            Ok(body) => Ok(body),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Err(CasError::ObjectNotFound(key.to_string()))
            }
            Err(err) => Err(err.into()),
        }
    }
}

/// Configuration for an [`S3ChunkStore`].
#[derive(Clone)]
pub struct S3Config {
    /// S3 (or S3-compatible) endpoint URL, e.g. `http://rustfs:9000`.
    pub endpoint: String,
    /// Region label (`RustFS` accepts any; AWS requires the real region).
    pub region: String,
    /// Bucket holding the chunk objects.
    pub bucket: String,
    /// Access key id.
    pub access_key_id: String,
    /// Secret access key.
    pub secret_access_key: String,
    /// Key prefix under which chunk objects live (may be empty).
    pub prefix: String,
}

impl fmt::Debug for S3Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("prefix", &self.prefix)
            .finish()
    }
}

/// A [`ChunkStore`] backed by an S3-compatible object store (e.g. `RustFS`).
///
/// Requests are presigned with `rusty-s3` (`SigV4`) and executed over a shared
/// `reqwest` client, reusing the workspace's `rustls` stack. Path-style URLs are
/// used so the same code works against `RustFS`/`MinIO` and AWS.
///
/// [`ChunkStore::has_chunks`] issues one `HEAD` per hash. The `oad` daemon
/// prefers a single batched existence check against the control-plane chunk
/// index, but `HEAD` keeps this store usable on its own.
#[derive(Debug, Clone)]
pub struct S3ChunkStore {
    bucket: Bucket,
    credentials: Credentials,
    http: reqwest::Client,
    prefix: String,
    presign_ttl: Duration,
}

impl S3ChunkStore {
    /// Default presigned-URL lifetime.
    const PRESIGN_TTL: Duration = Duration::from_mins(15);

    /// Builds a store from `config`, reusing the provided `reqwest` client.
    ///
    /// # Errors
    ///
    /// Returns [`CasError::Store`] if the endpoint URL or bucket is invalid.
    pub fn new(config: S3Config, http: reqwest::Client) -> Result<Self, CasError> {
        let endpoint = Url::parse(&config.endpoint)
            .map_err(|err| CasError::Store(format!("invalid s3 endpoint: {err}")))?;
        let bucket = Bucket::new(endpoint, UrlStyle::Path, config.bucket, config.region)
            .map_err(|err| CasError::Store(format!("invalid s3 bucket: {err}")))?;
        let credentials = Credentials::new(config.access_key_id, config.secret_access_key);
        Ok(Self {
            bucket,
            credentials,
            http,
            prefix: config.prefix.trim_matches('/').to_string(),
            presign_ttl: Self::PRESIGN_TTL,
        })
    }

    /// Prepends the configured prefix (if any) to a store-relative key.
    #[must_use]
    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{key}", self.prefix)
        }
    }

    /// Object key for a chunk: `[<prefix>/]chunks/blake3/<aa>/<hex>`.
    #[must_use]
    fn object_key(&self, hash: &ChunkHash) -> String {
        let hex = hash.to_hex();
        let fanout = &hex[0..2];
        self.full_key(&format!("chunks/blake3/{fanout}/{hex}"))
    }
}

impl ChunkStore for S3ChunkStore {
    async fn has_chunks(&self, hashes: &[ChunkHash]) -> Result<Vec<bool>, CasError> {
        // `buffered` (not `buffer_unordered`) preserves input order so the
        // returned vector stays parallel to `hashes`.
        stream::iter(hashes.iter().copied().map(|hash| {
            let key = self.object_key(&hash);
            let url = self
                .bucket
                .head_object(Some(&self.credentials), &key)
                .sign(self.presign_ttl);
            async move {
                let resp = self
                    .http
                    .head(url)
                    .send()
                    .await
                    .map_err(|err| CasError::Store(format!("HEAD {key}: {err}")))?;
                match resp.status() {
                    status if status.is_success() => Ok(true),
                    StatusCode::NOT_FOUND => Ok(false),
                    status => Err(CasError::Store(format!("HEAD {key}: status {status}"))),
                }
            }
        }))
        .buffered(UPLOAD_CONCURRENCY)
        .try_collect()
        .await
    }

    async fn put_chunk(&self, hash: &ChunkHash, body: &[u8]) -> Result<(), CasError> {
        let key = self.object_key(hash);
        let url = self
            .bucket
            .put_object(Some(&self.credentials), &key)
            .sign(self.presign_ttl);
        let resp = self
            .http
            .put(url)
            .body(body.to_vec())
            .send()
            .await
            .map_err(|err| CasError::Store(format!("PUT {key}: {err}")))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(CasError::Store(format!(
                "PUT {key}: status {}",
                resp.status()
            )))
        }
    }

    async fn get_chunk(&self, hash: &ChunkHash) -> Result<Vec<u8>, CasError> {
        let key = self.object_key(hash);
        let url = self
            .bucket
            .get_object(Some(&self.credentials), &key)
            .sign(self.presign_ttl);
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|err| CasError::Store(format!("GET {key}: {err}")))?;
        match resp.status() {
            status if status.is_success() => {
                let bytes = resp
                    .bytes()
                    .await
                    .map_err(|err| CasError::Store(format!("GET {key}: body: {err}")))?;
                Ok(bytes.to_vec())
            }
            StatusCode::NOT_FOUND => Err(CasError::NotFound(*hash)),
            status => Err(CasError::Store(format!("GET {key}: status {status}"))),
        }
    }

    async fn put_object(&self, key: &str, body: &[u8]) -> Result<(), CasError> {
        let key = self.full_key(key);
        let url = self
            .bucket
            .put_object(Some(&self.credentials), &key)
            .sign(self.presign_ttl);
        let resp = self
            .http
            .put(url)
            .body(body.to_vec())
            .send()
            .await
            .map_err(|err| CasError::Store(format!("PUT {key}: {err}")))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(CasError::Store(format!(
                "PUT {key}: status {}",
                resp.status()
            )))
        }
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>, CasError> {
        let full = self.full_key(key);
        let url = self
            .bucket
            .get_object(Some(&self.credentials), &full)
            .sign(self.presign_ttl);
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|err| CasError::Store(format!("GET {full}: {err}")))?;
        match resp.status() {
            status if status.is_success() => {
                let bytes = resp
                    .bytes()
                    .await
                    .map_err(|err| CasError::Store(format!("GET {full}: body: {err}")))?;
                Ok(bytes.to_vec())
            }
            StatusCode::NOT_FOUND => Err(CasError::ObjectNotFound(key.to_string())),
            status => Err(CasError::Store(format!("GET {full}: status {status}"))),
        }
    }
}

/// Encodes a raw chunk into a stored object: a one-byte codec header followed by
/// the payload. Falls back to [`CODEC_NONE`] when compression does not shrink
/// the data (e.g. already-compressed content).
fn encode_chunk(data: &[u8], zstd_level: i32) -> Result<Vec<u8>, CasError> {
    let compressed = zstd::encode_all(data, zstd_level)?;
    if compressed.len() < data.len() {
        let mut out = Vec::with_capacity(compressed.len() + 1);
        out.push(CODEC_ZSTD);
        out.extend_from_slice(&compressed);
        Ok(out)
    } else {
        let mut out = Vec::with_capacity(data.len() + 1);
        out.push(CODEC_NONE);
        out.extend_from_slice(data);
        Ok(out)
    }
}

/// Decodes a stored object back to raw chunk bytes and verifies that they match
/// the expected address and length.
///
/// Decompression output is bounded by `expected_len`: the bytes are read through
/// `take(expected_len + 1)`, so a corrupt or hostile object (a tiny `zstd`
/// "bomb" that expands to gigabytes) fails the length check below instead of
/// exhausting memory. The decoder's *window* is bounded by `zstd`'s own default
/// cap (`ZSTD_WINDOWLOG_LIMIT_DEFAULT`, ~128 MiB), which rejects frames
/// declaring an oversized window before any output is produced.
fn decode_chunk(body: &[u8], expected: ChunkHash, expected_len: u32) -> Result<Vec<u8>, CasError> {
    use std::io::Read;

    let (&codec, payload) = body
        .split_first()
        .ok_or_else(|| CasError::Store("empty chunk object".to_string()))?;
    let data = match codec {
        CODEC_NONE => payload.to_vec(),
        CODEC_ZSTD => {
            // Read one byte past the claimed length so the check below detects
            // (and rejects) an over-long stream without buffering all of it.
            let cap = u64::from(expected_len).saturating_add(1);
            let mut data = Vec::with_capacity(expected_len as usize);
            zstd::stream::read::Decoder::new(payload)?
                .take(cap)
                .read_to_end(&mut data)?;
            data
        }
        other => return Err(CasError::Store(format!("unknown chunk codec {other}"))),
    };
    if u32::try_from(data.len()) != Ok(expected_len) {
        return Err(CasError::Integrity(expected));
    }
    let actual = blake3_of(&data);
    if actual != expected {
        return Err(CasError::Integrity(expected));
    }
    Ok(data)
}

/// Chunks `file` into a [`ChunkRecipe`] without uploading anything.
///
/// Runs the synchronous, memory-bounded `FastCDC` stream chunker on a blocking
/// thread so multi-`GiB` files do not buffer in memory.
async fn chunk_file(file: &Path, params: ChunkerParams) -> Result<ChunkRecipe, CasError> {
    let path = file.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<ChunkRecipe, CasError> {
        use fastcdc::v2020::StreamCDC;

        let handle = std::fs::File::open(&path)?;
        let total_len = handle.metadata()?.len();
        let chunker = StreamCDC::new(handle, params.min, params.avg, params.max);

        let mut chunks = Vec::new();
        let mut file_hasher = blake3::Hasher::new();
        for result in chunker {
            let chunk = result.map_err(|err| CasError::Chunk(err.to_string()))?;
            file_hasher.update(&chunk.data);
            let len = u32::try_from(chunk.length).map_err(|_| {
                CasError::TooLarge(format!("chunk length {} exceeds u32", chunk.length))
            })?;
            chunks.push(ChunkRef {
                hash: blake3_of(&chunk.data),
                offset: chunk.offset,
                len,
            });
        }

        Ok(ChunkRecipe {
            algo: CAS_ALGO.to_string(),
            builder_version: CAS_BUILDER_VERSION.to_string(),
            total_len,
            file_hash: ChunkHash(*file_hasher.finalize().as_bytes()),
            chunks,
        })
    })
    .await
    .map_err(|err| CasError::Store(format!("chunk task panicked: {err}")))?
}

/// Outcome of [`chunk_and_upload`].
#[derive(Clone, Debug)]
pub struct UploadOutcome {
    /// Recipe describing how to reassemble the file.
    pub recipe: ChunkRecipe,
    /// Number of distinct chunks in the file.
    pub unique_chunks: usize,
    /// Number of distinct chunks that were missing and therefore uploaded.
    pub chunks_uploaded: usize,
    /// Total bytes uploaded (sum of encoded object sizes).
    pub bytes_uploaded: u64,
}

/// Chunks `file`, uploads only the chunks the store does not already hold, and
/// returns the recipe plus transfer statistics.
///
/// Deduplication is driven by a single batched [`ChunkStore::has_chunks`] call,
/// so a re-upload of an unchanged or near-identical file transfers only the
/// delta.
///
/// # Errors
///
/// Returns [`CasError`] if the file cannot be read or chunked, the store cannot
/// be queried, or an upload fails.
pub async fn chunk_and_upload<S: ChunkStore + ?Sized>(
    file: &Path,
    store: &S,
    params: ChunkerParams,
    zstd_level: i32,
) -> Result<UploadOutcome, CasError> {
    let recipe = chunk_file(file, params).await?;
    let unique = recipe.unique_chunk_hashes();
    let present = store.has_chunks(&unique).await?;

    let missing: Vec<ChunkHash> = unique
        .iter()
        .zip(present)
        .filter_map(|(hash, present)| (!present).then_some(*hash))
        .collect();

    // First-seen offset/length per distinct hash; any occurrence yields the
    // same bytes, so the first is sufficient to read and upload the chunk.
    let mut locations: HashMap<ChunkHash, (u64, u32)> = HashMap::new();
    for chunk in &recipe.chunks {
        locations
            .entry(chunk.hash)
            .or_insert((chunk.offset, chunk.len));
    }

    let unique_chunks = unique.len();
    let chunks_uploaded = missing.len();

    if missing.is_empty() {
        return Ok(UploadOutcome {
            recipe,
            unique_chunks,
            chunks_uploaded,
            bytes_uploaded: 0,
        });
    }

    let path = file.to_path_buf();
    let handle = Arc::new(
        tokio::task::spawn_blocking(move || std::fs::File::open(&path))
            .await
            .map_err(|err| CasError::Store(format!("open task panicked: {err}")))??,
    );

    let bytes: usize = stream::iter(missing.into_iter().map(|hash| {
        let handle = Arc::clone(&handle);
        let (offset, len) = locations[&hash];
        async move {
            let encoded = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, CasError> {
                use std::os::unix::fs::FileExt;
                let mut buf = vec![0u8; len as usize];
                handle.read_exact_at(&mut buf, offset)?;
                encode_chunk(&buf, zstd_level)
            })
            .await
            .map_err(|err| CasError::Store(format!("encode task panicked: {err}")))??;
            let size = encoded.len();
            store.put_chunk(&hash, &encoded).await?;
            Ok::<usize, CasError>(size)
        }
    }))
    .buffer_unordered(UPLOAD_CONCURRENCY)
    .try_fold(0usize, |acc, size| async move { Ok(acc + size) })
    .await?;

    Ok(UploadOutcome {
        recipe,
        unique_chunks,
        chunks_uploaded,
        bytes_uploaded: u64::try_from(bytes).unwrap_or(u64::MAX),
    })
}

/// Reassembles the file described by `recipe` at `dest`, pulling chunks from the
/// store, verifying each chunk's integrity and the whole-file hash, then
/// publishing the result atomically.
///
/// # Errors
///
/// Returns [`CasError`] if a chunk is missing or corrupt, the recipe is not
/// contiguous, the reassembled file hash does not match, or an I/O operation
/// fails. On any error the partial output is removed and `dest` is left
/// untouched.
pub async fn materialize<S: ChunkStore + ?Sized>(
    recipe: &ChunkRecipe,
    dest: &Path,
    store: &S,
) -> Result<(), CasError> {
    use tokio::io::AsyncWriteExt;

    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = oad_core::temp_path(dest);

    let outcome = async {
        let mut file = tokio::fs::File::create(&tmp).await?;
        let mut file_hasher = blake3::Hasher::new();
        let mut pos: u64 = 0;
        for chunk in &recipe.chunks {
            if chunk.offset != pos {
                return Err(CasError::InvalidRecipe(format!(
                    "non-contiguous recipe: expected offset {pos}, got {}",
                    chunk.offset
                )));
            }
            let body = store.get_chunk(&chunk.hash).await?;
            let data = decode_chunk(&body, chunk.hash, chunk.len)?;
            file.write_all(&data).await?;
            file_hasher.update(&data);
            pos += u64::from(chunk.len);
        }
        file.flush().await?;
        file.sync_all().await?;
        drop(file);

        if pos != recipe.total_len {
            return Err(CasError::InvalidRecipe(format!(
                "recipe covers {pos} bytes, expected {}",
                recipe.total_len
            )));
        }
        let actual = ChunkHash(*file_hasher.finalize().as_bytes());
        if actual != recipe.file_hash {
            return Err(CasError::FileHashMismatch {
                expected: recipe.file_hash,
                actual,
            });
        }
        Ok(())
    }
    .await;

    match outcome {
        Ok(()) => {
            oad_core::publish_atomic_file(&tmp, dest).await?;
            Ok(())
        }
        Err(err) => {
            let _ = tokio::fs::remove_file(&tmp).await;
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic pseudo-random bytes (xorshift64) — avoids a dev-dependency
    /// on a RNG while exercising incompressible content.
    fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed | 1;
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            out.extend_from_slice(&state.to_le_bytes());
        }
        out.truncate(len);
        out
    }

    /// A mix of incompressible and compressible (zeroed) regions, so both codec
    /// branches are exercised.
    fn mixed_data(len: usize, seed: u64) -> Vec<u8> {
        let mut data = pseudo_random(len, seed);
        let zero_start = len / 3;
        let zero_end = (2 * len) / 3;
        data[zero_start..zero_end].fill(0);
        data
    }

    /// Small chunk sizes keep tests fast while producing many chunks.
    fn test_params() -> ChunkerParams {
        ChunkerParams {
            min: 2 * 1024,
            avg: 8 * 1024,
            max: 32 * 1024,
        }
    }

    #[tokio::test]
    async fn round_trip_reassembles_identical_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsChunkStore::new(dir.path().join("cas"));
        let src = dir.path().join("rootfs.erofs");
        let data = mixed_data(1024 * 1024, 0x1234_5678);
        tokio::fs::write(&src, &data).await.unwrap();

        let outcome = chunk_and_upload(&src, &store, test_params(), 3)
            .await
            .unwrap();
        assert!(outcome.unique_chunks > 1, "expected multiple chunks");
        assert_eq!(outcome.chunks_uploaded, outcome.unique_chunks);
        assert!(outcome.bytes_uploaded > 0);

        let dest = dir.path().join("restored.erofs");
        materialize(&outcome.recipe, &dest, &store).await.unwrap();

        let restored = tokio::fs::read(&dest).await.unwrap();
        assert_eq!(restored, data);
        assert_eq!(blake3_of(&restored), outcome.recipe.file_hash);
    }

    #[tokio::test]
    async fn re_upload_of_identical_file_transfers_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsChunkStore::new(dir.path().join("cas"));
        let src = dir.path().join("a.bin");
        tokio::fs::write(&src, mixed_data(512 * 1024, 0xabcd))
            .await
            .unwrap();

        let first = chunk_and_upload(&src, &store, test_params(), 3)
            .await
            .unwrap();
        assert!(first.chunks_uploaded > 0);

        let second = chunk_and_upload(&src, &store, test_params(), 3)
            .await
            .unwrap();
        assert_eq!(second.chunks_uploaded, 0);
        assert_eq!(second.bytes_uploaded, 0);
        assert_eq!(first.recipe, second.recipe);
    }

    #[tokio::test]
    async fn near_identical_file_uploads_only_the_delta() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsChunkStore::new(dir.path().join("cas"));

        let base = mixed_data(1024 * 1024, 0x55aa);
        let src_a = dir.path().join("a.bin");
        tokio::fs::write(&src_a, &base).await.unwrap();
        let outcome_a = chunk_and_upload(&src_a, &store, test_params(), 3)
            .await
            .unwrap();

        // Flip a small region in the middle; content-defined chunking should
        // keep all chunks outside the edited region identical.
        let mut edited = base.clone();
        let mid = edited.len() / 2;
        for byte in &mut edited[mid..mid + 256] {
            *byte ^= 0xff;
        }
        let src_b = dir.path().join("b.bin");
        tokio::fs::write(&src_b, &edited).await.unwrap();
        let outcome_b = chunk_and_upload(&src_b, &store, test_params(), 3)
            .await
            .unwrap();

        assert!(outcome_b.chunks_uploaded > 0, "the edit must upload data");
        assert!(
            outcome_b.chunks_uploaded * 4 < outcome_b.unique_chunks,
            "expected heavy reuse: uploaded {} of {} chunks",
            outcome_b.chunks_uploaded,
            outcome_b.unique_chunks
        );

        // Both files reassemble correctly from the shared store.
        let dest_a = dir.path().join("a.out");
        let dest_b = dir.path().join("b.out");
        materialize(&outcome_a.recipe, &dest_a, &store)
            .await
            .unwrap();
        materialize(&outcome_b.recipe, &dest_b, &store)
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(&dest_a).await.unwrap(), base);
        assert_eq!(tokio::fs::read(&dest_b).await.unwrap(), edited);
    }

    #[tokio::test]
    async fn corrupt_chunk_fails_integrity_check() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsChunkStore::new(dir.path().join("cas"));
        let src = dir.path().join("a.bin");
        tokio::fs::write(&src, mixed_data(256 * 1024, 0x9999))
            .await
            .unwrap();
        let outcome = chunk_and_upload(&src, &store, test_params(), 3)
            .await
            .unwrap();

        // Overwrite one chunk object with garbage of the same address.
        let victim = outcome.recipe.chunks[0].hash;
        store
            .put_chunk(&victim, &[CODEC_NONE, 1, 2, 3])
            .await
            .unwrap();

        let dest = dir.path().join("a.out");
        let err = materialize(&outcome.recipe, &dest, &store)
            .await
            .unwrap_err();
        assert!(matches!(err, CasError::Integrity(_)));
        assert!(!tokio::fs::try_exists(&dest).await.unwrap());
    }

    #[tokio::test]
    async fn empty_file_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsChunkStore::new(dir.path().join("cas"));
        let src = dir.path().join("empty.bin");
        tokio::fs::write(&src, b"").await.unwrap();

        let outcome = chunk_and_upload(&src, &store, test_params(), 3)
            .await
            .unwrap();
        assert_eq!(outcome.unique_chunks, 0);

        let dest = dir.path().join("empty.out");
        materialize(&outcome.recipe, &dest, &store).await.unwrap();
        assert_eq!(tokio::fs::read(&dest).await.unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn chunk_hash_hex_round_trips() {
        let hash = blake3_of(b"hello oad-cas");
        let hex = hash.to_hex();
        assert_eq!(ChunkHash::from_hex(&hex).unwrap(), hash);
        assert_eq!(hex.parse::<ChunkHash>().unwrap(), hash);
        assert!(ChunkHash::from_hex("nothex").is_err());
        assert!(ChunkHash::from_hex("abcd").is_err());
    }

    #[test]
    fn recipe_serde_round_trips() {
        let recipe = ChunkRecipe {
            algo: CAS_ALGO.to_string(),
            builder_version: CAS_BUILDER_VERSION.to_string(),
            total_len: 10,
            file_hash: blake3_of(b"x"),
            chunks: vec![ChunkRef {
                hash: blake3_of(b"y"),
                offset: 0,
                len: 10,
            }],
        };
        let json = serde_json::to_string(&recipe).unwrap();
        let parsed: ChunkRecipe = serde_json::from_str(&json).unwrap();
        assert_eq!(recipe, parsed);
    }

    #[tokio::test]
    async fn object_put_get_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsChunkStore::new(dir.path().join("cas"));
        let key = "recipes/blake3/ab/abcd.json";
        assert!(matches!(
            store.get_object(key).await.unwrap_err(),
            CasError::ObjectNotFound(_)
        ));
        store.put_object(key, br#"{"hello":1}"#).await.unwrap();
        assert_eq!(store.get_object(key).await.unwrap(), br#"{"hello":1}"#);
    }

    #[test]
    fn object_key_helpers() {
        let recipe = ChunkRecipe {
            algo: CAS_ALGO.to_string(),
            builder_version: CAS_BUILDER_VERSION.to_string(),
            total_len: 0,
            file_hash: blake3_of(b"f"),
            chunks: vec![],
        };
        let hex = recipe.file_hash.to_hex();
        assert_eq!(
            recipe_object_key(&recipe),
            format!("recipes/blake3/{}/{hex}.json", &hex[0..2])
        );
        assert_eq!(descriptor_object_key("ws-v3"), "descriptors/ws-v3.json");
    }

    #[tokio::test]
    async fn decompression_bomb_is_rejected_not_oom() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsChunkStore::new(dir.path().join("cas"));

        // A chunk that legitimately holds 10 bytes...
        let real = b"ten-bytes!";
        assert_eq!(real.len(), 10);
        let hash = blake3_of(real);
        let recipe = ChunkRecipe {
            algo: CAS_ALGO.to_string(),
            builder_version: CAS_BUILDER_VERSION.to_string(),
            total_len: 10,
            file_hash: blake3_of(real),
            chunks: vec![ChunkRef {
                hash,
                offset: 0,
                len: 10,
            }],
        };

        // ...but the stored object is a zstd "bomb": 16 MiB of zeros that
        // compresses to a tiny payload, filed under the chunk's address.
        let compressed = zstd::encode_all(&vec![0u8; 16 * 1024 * 1024][..], 3).unwrap();
        assert!(compressed.len() < 4096, "bomb payload should be tiny");
        let mut object = Vec::with_capacity(compressed.len() + 1);
        object.push(CODEC_ZSTD);
        object.extend_from_slice(&compressed);
        store.put_chunk(&hash, &object).await.unwrap();

        // materialize must reject it via a bounded read — never OOM, never
        // publish. Rejection surfaces as Io (window cap) or Integrity (length).
        let dest = dir.path().join("out.bin");
        let err = materialize(&recipe, &dest, &store).await.unwrap_err();
        assert!(
            matches!(err, CasError::Integrity(_) | CasError::Io(_)),
            "expected bounded rejection, got {err:?}"
        );
        assert!(!tokio::fs::try_exists(&dest).await.unwrap());
    }

    #[test]
    fn s3_object_key_layout() {
        let store = S3ChunkStore::new(
            S3Config {
                endpoint: "http://rustfs:9000".to_string(),
                region: "us-east-1".to_string(),
                bucket: "omniagent-cas".to_string(),
                access_key_id: "key".to_string(),
                secret_access_key: "secret".to_string(),
                prefix: "/cas/".to_string(),
            },
            reqwest::Client::new(),
        )
        .unwrap();
        let hash = blake3_of(b"abc");
        let hex = hash.to_hex();
        assert_eq!(
            store.object_key(&hash),
            format!("cas/chunks/blake3/{}/{hex}", &hex[0..2])
        );
    }

    #[test]
    fn s3_new_rejects_bad_endpoint() {
        let err = S3ChunkStore::new(
            S3Config {
                endpoint: "not a url".to_string(),
                region: "r".to_string(),
                bucket: "b".to_string(),
                access_key_id: "k".to_string(),
                secret_access_key: "s".to_string(),
                prefix: String::new(),
            },
            reqwest::Client::new(),
        )
        .unwrap_err();
        assert!(matches!(err, CasError::Store(_)));
    }
}
