use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
#[cfg(test)]
use std::io::Cursor;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use bytes::{Bytes, BytesMut};
use flate2::read::GzDecoder;
use futures_util::{StreamExt, TryStreamExt, stream};
use oad_core::{
    ContainerSpec, EnvVar, MountSpec, OadPaths, PAUSE_CONTAINER, ResourceSpec, SandboxId,
    publish_atomic_file, write_atomic_file,
};
use reqwest::header::{ACCEPT, AUTHORIZATION, WWW_AUTHENTICATE};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tar::{Archive, EntryType};
use thiserror::Error;
use tokio::fs as async_fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tracing::{debug, info, warn};
use url::{Host, Url};

const DOCKER_HUB_REGISTRY: &str = "registry-1.docker.io";
const DEFAULT_TAG: &str = "latest";
const DEFAULT_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const GVISOR_ROOTFS_SOURCE_ANNOTATION: &str = "dev.gvisor.spec.rootfs.source";
const GVISOR_ROOTFS_TYPE_ANNOTATION: &str = "dev.gvisor.spec.rootfs.type";
const GVISOR_ROOTFS_OVERLAY_ANNOTATION: &str = "dev.gvisor.spec.rootfs.overlay";
const MAX_PARALLEL_LAYER_DOWNLOADS: usize = 4;
const REGISTRY_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REGISTRY_REQUEST_TIMEOUT: Duration = Duration::from_mins(5);
const DEFAULT_MANIFEST_CACHE_TTL: Duration = Duration::from_mins(5);
const MAX_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;
const MAX_CONFIG_BYTES: u64 = 16 * 1024 * 1024;
const MAX_TOKEN_BYTES: u64 = 1024 * 1024;
const MAX_LAYER_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Total unpacked bytes allowed across *all* layers of one image. Enforced as a
/// single image-wide budget (not per layer) so a manifest with many layers
/// cannot multiply the cap and fill the host filesystem.
const MAX_UNPACKED_IMAGE_BYTES: u64 = 16 * 1024 * 1024 * 1024;
/// Maximum number of layer descriptors accepted in a single image manifest.
const MAX_LAYERS: usize = 512;
const MAX_REGISTRY_REDIRECTS: usize = 5;

const MANIFEST_ACCEPT: &str = concat!(
    "application/vnd.oci.image.manifest.v1+json, ",
    "application/vnd.docker.distribution.manifest.v2+json, ",
    "application/vnd.oci.image.index.v1+json, ",
    "application/vnd.docker.distribution.manifest.list.v2+json"
);

#[derive(Debug, Clone)]
pub struct GvisorManager {
    client: RegistryClient,
}

impl GvisorManager {
    #[must_use]
    pub fn new() -> Self {
        Self {
            client: RegistryClient::new(),
        }
    }

    /// Prepares the runsc JSON config for the sandbox's pause container.
    ///
    /// # Errors
    ///
    /// Returns [`OciError`] if pulling the image, building its EROFS rootfs, or
    /// writing the runsc config JSON fails.
    pub async fn prepare_pause_bundle(
        &self,
        paths: &OadPaths,
        sandbox_id: &SandboxId,
        image: &str,
        network_namespace: Option<&Path>,
        resolv_conf: Option<&Path>,
    ) -> Result<(), OciError> {
        let staging = paths.rootfs_staging_dir(sandbox_id, PAUSE_CONTAINER);
        let pulled = self
            .client
            .pull_image_rootfs(
                image,
                &paths.layer_cache_dir(),
                &paths.manifest_cache_dir(),
                &paths.rootfs_cache_dir(),
                &staging,
            )
            .await?;
        async_fs::create_dir_all(paths.rootfs_dir(sandbox_id, PAUSE_CONTAINER)).await?;
        let rootfs_overlay = prepare_rootfs_overlay_dir(paths, sandbox_id, PAUSE_CONTAINER).await?;
        let rootfs = async_fs::canonicalize(pulled.rootfs).await?;
        let image_config = pulled.config;
        let config = GvisorConfig {
            container_name: PAUSE_CONTAINER,
            args: vec!["/pause".to_string()],
            env: merged_env(&image_config.env(), &[]),
            cwd: cwd_or_root(image_config.working_dir()),
            rootfs,
            rootfs_overlay,
            network_namespace: network_namespace.map(Path::to_path_buf),
            resolv_conf: resolv_conf.map(Path::to_path_buf),
            mounts: Vec::new(),
            resources: None,
            annotations: BTreeMap::from([
                (
                    "io.kubernetes.cri.container-type".to_string(),
                    "sandbox".to_string(),
                ),
                (
                    "io.kubernetes.cri.container-name".to_string(),
                    PAUSE_CONTAINER.to_string(),
                ),
            ]),
        };
        write_gvisor_config_json(&paths.config_json(sandbox_id, PAUSE_CONTAINER), &config).await
    }

    /// Prepares the runsc JSON config for a user-defined container.
    ///
    /// # Errors
    ///
    /// Returns [`OciError`] if pulling the image, building its EROFS rootfs, or
    /// writing the runsc config JSON fails.
    pub async fn prepare_container_bundle(
        &self,
        paths: &OadPaths,
        sandbox_id: &SandboxId,
        container: &ContainerSpec,
        network_namespace: Option<&Path>,
        resolv_conf: Option<&Path>,
        static_mounts: &[MountSpec],
    ) -> Result<(), OciError> {
        let staging = paths.rootfs_staging_dir(sandbox_id, &container.name);
        let pulled = self
            .client
            .pull_image_rootfs(
                &container.image,
                &paths.layer_cache_dir(),
                &paths.manifest_cache_dir(),
                &paths.rootfs_cache_dir(),
                &staging,
            )
            .await?;
        async_fs::create_dir_all(paths.rootfs_dir(sandbox_id, &container.name)).await?;
        let rootfs_overlay = prepare_rootfs_overlay_dir(paths, sandbox_id, &container.name).await?;
        let rootfs = async_fs::canonicalize(pulled.rootfs).await?;
        let image_config = pulled.config;
        let args = resolve_args(container, &image_config)?;
        let env = merged_env(&image_config.env(), &container.env);
        let config = GvisorConfig {
            container_name: &container.name,
            args,
            env,
            cwd: cwd_or_root(image_config.working_dir()),
            rootfs,
            rootfs_overlay,
            network_namespace: network_namespace.map(Path::to_path_buf),
            resolv_conf: resolv_conf.map(Path::to_path_buf),
            mounts: static_mounts.to_vec(),
            resources: container.resources.clone(),
            annotations: BTreeMap::from([
                (
                    "io.kubernetes.cri.container-type".to_string(),
                    "container".to_string(),
                ),
                (
                    "io.kubernetes.cri.sandbox-id".to_string(),
                    PAUSE_CONTAINER.to_string(),
                ),
                (
                    "io.kubernetes.cri.container-name".to_string(),
                    container.name.clone(),
                ),
            ]),
        };
        write_gvisor_config_json(&paths.config_json(sandbox_id, &container.name), &config).await
    }
}

impl Default for GvisorManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
struct RegistryClient {
    http: reqwest::Client,
    manifest_cache_ttl: Duration,
}

impl RegistryClient {
    fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .connect_timeout(REGISTRY_CONNECT_TIMEOUT)
                .timeout(REGISTRY_REQUEST_TIMEOUT)
                .redirect(registry_redirect_policy())
                .build()
                .expect("registry HTTP client configuration is valid"),
            manifest_cache_ttl: DEFAULT_MANIFEST_CACHE_TTL,
        }
    }

    async fn pull_image_rootfs(
        &self,
        image: &str,
        layer_cache: &Path,
        manifest_cache: &Path,
        rootfs_cache: &Path,
        staging: &Path,
    ) -> Result<PulledImage, OciError> {
        let reference = ImageReference::parse(image)?;
        debug!(
            image,
            staging = %staging.display(),
            layer_cache = %layer_cache.display(),
            rootfs_cache = %rootfs_cache.display(),
            "pulling OCI image"
        );

        let expected_top_digest = reference
            .reference_is_digest
            .then_some(reference.reference.as_str());
        let manifest = self
            .fetch_manifest(
                &reference,
                &reference.reference,
                expected_top_digest,
                manifest_cache,
            )
            .await?;
        let manifest = match manifest.manifests {
            Some(manifests) => {
                let descriptor = select_platform_manifest(&manifests)?;
                self.fetch_manifest(
                    &reference,
                    &descriptor.digest,
                    Some(&descriptor.digest),
                    manifest_cache,
                )
                .await?
            }
            None => manifest,
        };

        if manifest.layers.len() > MAX_LAYERS {
            return Err(OciError::InvalidManifest(
                "image manifest has too many layers",
            ));
        }

        let config_descriptor = manifest.config.ok_or(OciError::InvalidManifest(
            "image manifest is missing config descriptor",
        ))?;
        let config_limit = descriptor_body_limit(&config_descriptor, MAX_CONFIG_BYTES, "config")?;
        let config_blob = self
            .fetch_blob_cached(
                &reference,
                &config_descriptor.digest,
                layer_cache,
                config_limit,
                "config",
            )
            .await?;
        let image_config: ImageConfig =
            parse_json_body("image config", &config_descriptor.digest, &config_blob)?;

        // The built rootfs is fully determined by its ordered layer digests, so
        // reuse a previously built EROFS image for the same layer set.
        let rootfs_key = rootfs_cache_key(&manifest.layers);
        async_fs::create_dir_all(rootfs_cache).await?;
        let rootfs = rootfs_cache.join(format!("{rootfs_key}.erofs"));
        if valid_rootfs_cache_entry(&rootfs).await? {
            info!(image, rootfs = %rootfs.display(), "reusing cached rootfs");
            return Ok(PulledImage {
                config: image_config,
                rootfs,
            });
        }

        async_fs::create_dir_all(layer_cache).await?;

        info!(
            image,
            layers = manifest.layers.len(),
            "fetching image layers"
        );
        let blobs = self
            .fetch_layers_cached(&reference, &manifest.layers, layer_cache)
            .await?;

        // Build into a unique temp path then atomically publish into the cache,
        // so a crashed build or a concurrent builder never yields a partial
        // image. A losing race just overwrites with byte-identical content.
        let tmp_rootfs = rootfs_cache.join(temp_name(&rootfs_key));
        // Cleans up the partial EROFS image on any error or cancellation;
        // disarmed once it has been published into the cache.
        let tmp_guard = TempPathGuard::new(tmp_rootfs.clone());
        // Extract every layer into a staging directory (resolving ordering and
        // whiteouts), then build the EROFS image in one pass. We do NOT use
        // `mkfs.erofs --tar`: erofs-utils 1.9.x's incremental tar build corrupts
        // or segfaults past two layers, and a single pass over concatenated tars
        // silently stops at the first layer's end-of-archive marker, dropping
        // every later layer. The staging build normalizes everything to root
        // (`--all-root`), which matches the in-container root (uid 0) ownership.
        let build = build_erofs_from_staging(&blobs, staging, &tmp_rootfs).await;
        // Staging is scratch space regardless of outcome.
        let _ = remove_dir_if_exists(staging).await;
        if let Err(err) = build {
            warn!(image, error = %err, "failed to build rootfs from image layers");
            return Err(err);
        }
        publish_atomic_file(&tmp_rootfs, &rootfs).await?;
        tmp_guard.disarm();
        info!(
            image,
            layers = manifest.layers.len(),
            rootfs = %rootfs.display(),
            "built rootfs"
        );

        Ok(PulledImage {
            config: image_config,
            rootfs,
        })
    }

    async fn fetch_manifest(
        &self,
        reference: &ImageReference,
        manifest_ref: &str,
        expected_digest: Option<&str>,
        manifest_cache: &Path,
    ) -> Result<ImageManifest, OciError> {
        let cached = manifest_cache_path(manifest_cache, reference, manifest_ref);
        if let Some(manifest) = self
            .read_manifest_cache(reference, manifest_ref, expected_digest, &cached)
            .await?
        {
            return Ok(manifest);
        }

        let url = reference.registry_url(&format!(
            "/v2/{}/manifests/{}",
            reference.repository, manifest_ref
        ))?;
        let response = self
            .get_with_auth(url, &reference.repository, Some(MANIFEST_ACCEPT))
            .await?;
        let bytes = read_response_limited(response, "manifest", MAX_MANIFEST_BYTES).await?;
        if let Some(digest) = expected_digest {
            verify_digest(&bytes, digest)?;
        }
        let manifest = parse_json_body("manifest", manifest_ref, &bytes)?;
        self.write_manifest_cache(&cached, &bytes).await;
        Ok(manifest)
    }

    async fn read_manifest_cache(
        &self,
        reference: &ImageReference,
        manifest_ref: &str,
        expected_digest: Option<&str>,
        cached: &Path,
    ) -> Result<Option<ImageManifest>, OciError> {
        let metadata = match async_fs::metadata(cached).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(OciError::Io(err)),
        };
        let Ok(modified) = metadata.modified() else {
            return Ok(None);
        };
        if !manifest_cache_entry_is_fresh(modified, SystemTime::now(), self.manifest_cache_ttl) {
            return Ok(None);
        }
        if metadata.len() > MAX_MANIFEST_BYTES {
            debug!(
                cached = %cached.display(),
                "discarding oversized cached manifest"
            );
            let _ = async_fs::remove_file(cached).await;
            return Ok(None);
        }

        let bytes = async_fs::read(cached).await?;
        if let Some(digest) = expected_digest
            && let Err(err) = verify_digest(&bytes, digest)
        {
            match err {
                OciError::DigestMismatch { .. } => {
                    debug!(
                        cached = %cached.display(),
                        digest,
                        "discarding cached manifest with mismatched digest"
                    );
                    let _ = async_fs::remove_file(cached).await;
                    return Ok(None);
                }
                err => return Err(err),
            }
        }

        match parse_json_body("manifest", manifest_ref, &bytes) {
            Ok(manifest) => {
                debug!(
                    registry = reference.registry,
                    repository = reference.repository,
                    manifest_ref,
                    cached = %cached.display(),
                    "reusing cached OCI manifest"
                );
                Ok(Some(manifest))
            }
            Err(err) => {
                debug!(
                    cached = %cached.display(),
                    %err,
                    "discarding invalid cached manifest"
                );
                let _ = async_fs::remove_file(cached).await;
                Ok(None)
            }
        }
    }

    async fn write_manifest_cache(&self, cached: &Path, bytes: &[u8]) {
        if let Err(err) = write_atomic_file(cached, bytes).await {
            debug!(
                cached = %cached.display(),
                %err,
                "failed to write OCI manifest cache entry"
            );
        }
    }

    async fn fetch_blob(
        &self,
        reference: &ImageReference,
        digest: &str,
        max_bytes: u64,
        kind: &'static str,
    ) -> Result<Bytes, OciError> {
        let url =
            reference.registry_url(&format!("/v2/{}/blobs/{digest}", reference.repository))?;
        let response = self.get_with_auth(url, &reference.repository, None).await?;
        read_response_limited(response, kind, max_bytes).await
    }

    async fn fetch_blob_cached(
        &self,
        reference: &ImageReference,
        digest: &str,
        blob_cache: &Path,
        max_bytes: u64,
        kind: &'static str,
    ) -> Result<Bytes, OciError> {
        let cached = blob_cache.join(blob_filename(digest)?);
        if let Some(bytes) = read_blob_cache(&cached, digest, max_bytes, kind).await? {
            debug!(
                cached = %cached.display(),
                digest,
                kind,
                "reusing cached OCI blob"
            );
            return Ok(bytes);
        }

        let bytes = self.fetch_blob(reference, digest, max_bytes, kind).await?;
        verify_digest(&bytes, digest)?;
        self.write_blob_cache(&cached, &bytes, kind).await;
        Ok(bytes)
    }

    async fn write_blob_cache(&self, cached: &Path, bytes: &[u8], kind: &'static str) {
        if let Err(err) = write_atomic_file(cached, bytes).await {
            debug!(
                cached = %cached.display(),
                kind,
                %err,
                "failed to write OCI blob cache entry"
            );
        }
    }

    /// Fetches every layer blob into the persistent cache with bounded
    /// concurrency. The returned vector preserves manifest layer order, which
    /// extraction still needs for correct OCI overlay semantics.
    async fn fetch_layers_cached(
        &self,
        reference: &ImageReference,
        layers: &[Descriptor],
        layer_cache: &Path,
    ) -> Result<Vec<(PathBuf, String)>, OciError> {
        let downloads = layers
            .iter()
            .enumerate()
            .map(|(index, layer)| {
                Ok((
                    index,
                    layer_cache.join(blob_filename(&layer.digest)?),
                    layer.digest.clone(),
                    layer.media_type.clone().unwrap_or_default(),
                    descriptor_body_limit(layer, MAX_LAYER_BYTES, "layer")?,
                ))
            })
            .collect::<Result<Vec<_>, OciError>>()?;

        let mut blobs = stream::iter(downloads)
            .map(
                |(index, cached, digest, media_type, max_bytes)| async move {
                    // Layers are content-addressed by digest and verified on
                    // download, so an existing cache entry is safe to reuse without
                    // re-fetching.
                    if async_fs::try_exists(&cached).await? {
                        match verify_blob_file(&cached, &digest).await {
                            Ok(()) => {}
                            Err(OciError::DigestMismatch { .. }) => {
                                debug!(
                                    cached = %cached.display(),
                                    digest,
                                    "discarding cached layer with mismatched digest"
                                );
                                let _ = async_fs::remove_file(&cached).await;
                                self.download_blob_cached(
                                    reference,
                                    &digest,
                                    layer_cache,
                                    &cached,
                                    max_bytes,
                                )
                                .await?;
                            }
                            Err(err) => return Err(err),
                        }
                    } else {
                        self.download_blob_cached(
                            reference,
                            &digest,
                            layer_cache,
                            &cached,
                            max_bytes,
                        )
                        .await?;
                    }
                    Ok::<_, OciError>((index, cached, media_type))
                },
            )
            .buffer_unordered(MAX_PARALLEL_LAYER_DOWNLOADS)
            .try_collect::<Vec<_>>()
            .await?;

        blobs.sort_by_key(|(index, _, _)| *index);
        Ok(blobs
            .into_iter()
            .map(|(_, cached, media_type)| (cached, media_type))
            .collect())
    }

    async fn download_blob(
        &self,
        reference: &ImageReference,
        digest: &str,
        path: &Path,
        max_bytes: u64,
        kind: &'static str,
    ) -> Result<(), OciError> {
        let url =
            reference.registry_url(&format!("/v2/{}/blobs/{digest}", reference.repository))?;
        let mut response = self.get_with_auth(url, &reference.repository, None).await?;
        let mut file = async_fs::File::create(path).await?;
        let mut hasher = Sha256::new();
        let mut downloaded = 0_u64;

        while let Some(chunk) = response.chunk().await? {
            downloaded =
                downloaded
                    .checked_add(chunk.len() as u64)
                    .ok_or(OciError::BodyTooLarge {
                        kind,
                        limit: max_bytes,
                    })?;
            if downloaded > max_bytes {
                return Err(OciError::BodyTooLarge {
                    kind,
                    limit: max_bytes,
                });
            }
            hasher.update(&chunk);
            file.write_all(&chunk).await?;
        }
        file.flush().await?;

        verify_digest_hex(&hex::encode(hasher.finalize()), digest)
    }

    /// Downloads a layer blob to a unique temp path then atomically renames it
    /// into the persistent cache, so an interrupted or unverified download is
    /// never observed as a valid cache entry.
    async fn download_blob_cached(
        &self,
        reference: &ImageReference,
        digest: &str,
        layer_cache: &Path,
        cached: &Path,
        max_bytes: u64,
    ) -> Result<(), OciError> {
        let tmp = layer_cache.join(temp_name(&blob_filename(digest)?));
        // Removes `tmp` on any early return *or* cancellation; disarmed only
        // once the verified blob has been renamed into the cache.
        let guard = TempPathGuard::new(tmp.clone());
        if let Err(err) = self
            .download_blob(reference, digest, &tmp, max_bytes, "layer")
            .await
        {
            warn!(digest, error = %err, "failed to download layer blob");
            return Err(err);
        }
        async_fs::rename(&tmp, cached).await?;
        guard.disarm();
        Ok(())
    }

    async fn get_with_auth(
        &self,
        url: Url,
        repository: &str,
        accept: Option<&str>,
    ) -> Result<reqwest::Response, OciError> {
        let mut request = self.http.get(url.clone());
        if let Some(accept) = accept {
            request = request.header(ACCEPT, accept);
        }
        let response = request.send().await?;
        if response.status() != reqwest::StatusCode::UNAUTHORIZED {
            return response.error_for_status().map_err(OciError::Http);
        }

        let auth_header = response
            .headers()
            .get(WWW_AUTHENTICATE)
            .ok_or(OciError::MissingAuthChallenge)?
            .to_str()
            .map_err(|_| OciError::InvalidAuthChallenge)?
            .to_string();
        let challenge = BearerChallenge::parse(&auth_header)?;
        let token = self.fetch_token(&challenge, repository).await?;

        // Retry with the bearer token. Use the builder pattern to mirror the
        // first request, avoiding a manual HeaderMap and the fallible
        // HeaderValue::from_str calls.
        let mut retry = self
            .http
            .get(url)
            .header(AUTHORIZATION, format!("Bearer {token}"));
        if let Some(accept) = accept {
            retry = retry.header(ACCEPT, accept);
        }
        retry
            .send()
            .await?
            .error_for_status()
            .map_err(OciError::Http)
    }

    async fn fetch_token(
        &self,
        challenge: &BearerChallenge,
        repository: &str,
    ) -> Result<String, OciError> {
        let mut url = Url::parse(&challenge.realm).map_err(|_| OciError::InvalidAuthChallenge)?;
        validate_token_realm(&url)?;
        {
            let mut query = url.query_pairs_mut();
            if let Some(service) = &challenge.service {
                query.append_pair("service", service);
            }
            let scope = challenge
                .scope
                .clone()
                .unwrap_or_else(|| format!("repository:{repository}:pull"));
            query.append_pair("scope", &scope);
        }
        let response = self.http.get(url).send().await?.error_for_status()?;
        let bytes = read_response_limited(response, "token", MAX_TOKEN_BYTES).await?;
        let token: TokenResponse = parse_json_body("token", &challenge.realm, &bytes)?;
        token
            .token
            .or(token.access_token)
            .ok_or(OciError::MissingToken)
    }
}

async fn read_response_limited(
    mut response: reqwest::Response,
    kind: &'static str,
    limit: u64,
) -> Result<Bytes, OciError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(OciError::BodyTooLarge { kind, limit });
    }

    let mut downloaded = 0_u64;
    let mut body = BytesMut::new();
    while let Some(chunk) = response.chunk().await? {
        downloaded = downloaded
            .checked_add(chunk.len() as u64)
            .ok_or(OciError::BodyTooLarge { kind, limit })?;
        if downloaded > limit {
            return Err(OciError::BodyTooLarge { kind, limit });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

fn parse_json_body<T: for<'de> Deserialize<'de>>(
    kind: &'static str,
    source: &str,
    body: &[u8],
) -> Result<T, OciError> {
    serde_json::from_slice(body).map_err(|err| OciError::JsonBody {
        kind,
        body_source: source.to_string(),
        error: err,
        preview: body_preview(body),
    })
}

fn body_preview(body: &[u8]) -> String {
    const MAX_PREVIEW_BYTES: usize = 256;
    let end = body.len().min(MAX_PREVIEW_BYTES);
    String::from_utf8_lossy(&body[..end]).into_owned()
}

fn descriptor_body_limit(
    descriptor: &Descriptor,
    default_limit: u64,
    kind: &'static str,
) -> Result<u64, OciError> {
    if let Some(size) = descriptor.size
        && size > default_limit
    {
        return Err(OciError::BodyTooLarge {
            kind,
            limit: default_limit,
        });
    }
    Ok(descriptor.size.unwrap_or(default_limit))
}

fn validate_token_realm(url: &Url) -> Result<(), OciError> {
    validate_remote_https_url(url).map_err(|()| OciError::InvalidAuthChallenge)
}

fn validate_registry_url(url: &Url) -> Result<(), OciError> {
    validate_remote_https_url(url).map_err(|()| OciError::InvalidReference(url.to_string()))
}

fn registry_redirect_policy() -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(|attempt| {
        if attempt.previous().len() > MAX_REGISTRY_REDIRECTS {
            return attempt.error("registry redirect limit exceeded");
        }
        if validate_remote_https_url(attempt.url()).is_err() {
            let url = attempt.url().to_string();
            return attempt.error(format!("registry redirect is invalid or unsafe: {url}"));
        }
        attempt.follow()
    })
}

fn validate_remote_https_url(url: &Url) -> Result<(), ()> {
    if url.scheme() != "https" {
        return Err(());
    }

    let Some(host) = url.host() else {
        return Err(());
    };
    match host {
        Host::Domain(domain)
            if domain.eq_ignore_ascii_case("localhost")
                || domain.to_ascii_lowercase().ends_with(".localhost") =>
        {
            Err(())
        }
        Host::Ipv4(addr) if forbidden_ipv4_host(addr) => Err(()),
        Host::Ipv6(addr) if forbidden_ipv6_host(addr) => Err(()),
        Host::Domain(_) | Host::Ipv4(_) | Host::Ipv6(_) => Ok(()),
    }
}

const fn forbidden_ipv4_host(addr: Ipv4Addr) -> bool {
    let [a, b, c, _] = addr.octets();
    addr.is_loopback()
        || addr.is_private()
        || addr.is_link_local()
        || addr.is_unspecified()
        || addr.is_multicast()
        || addr.is_broadcast()
        // 0.0.0.0/8 "this host on this network"
        || a == 0
        // 100.64.0.0/10 carrier-grade NAT
        || (a == 100 && (b & 0xc0) == 0x40)
        // 192.0.0.0/24 IETF protocol assignments
        || (a == 192 && b == 0 && c == 0)
}

const fn forbidden_ipv6_host(addr: Ipv6Addr) -> bool {
    // An IPv4-mapped (`::ffff:a.b.c.d`) or IPv4-compatible host resolves to its
    // embedded IPv4 address, so apply the IPv4 denylist to it — otherwise
    // `[::ffff:169.254.169.254]` would bypass the link-local/metadata block.
    if let Some(v4) = addr.to_ipv4() {
        return forbidden_ipv4_host(v4);
    }
    addr.is_loopback()
        || addr.is_unspecified()
        || addr.is_unique_local()
        || addr.is_unicast_link_local()
        || addr.is_multicast()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImageReference {
    registry: String,
    repository: String,
    reference: String,
    reference_is_digest: bool,
}

impl ImageReference {
    fn parse(input: &str) -> Result<Self, OciError> {
        if input.is_empty() {
            return Err(OciError::InvalidReference(input.to_string()));
        }
        let reference_is_digest = input.rsplit_once('@').is_some();
        let (name, reference) = split_reference(input);
        let parts: Vec<&str> = name.split('/').collect();
        let (registry, repository) = if parts.len() > 1 && looks_like_registry(parts[0]) {
            (parts[0].to_string(), parts[1..].join("/"))
        } else {
            let repository = if parts.len() == 1 {
                format!("library/{}", parts[0])
            } else {
                parts.join("/")
            };
            (DOCKER_HUB_REGISTRY.to_string(), repository)
        };

        if repository.is_empty() {
            return Err(OciError::InvalidReference(input.to_string()));
        }

        Ok(Self {
            registry,
            repository,
            reference: reference.unwrap_or_else(|| DEFAULT_TAG.to_string()),
            reference_is_digest,
        })
    }

    fn registry_url(&self, path: &str) -> Result<Url, OciError> {
        let url = Url::parse(&format!("https://{}{}", self.registry, path))
            .map_err(|_| OciError::InvalidReference(self.registry.clone()))?;
        validate_registry_url(&url)?;
        Ok(url)
    }
}

fn split_reference(input: &str) -> (&str, Option<String>) {
    if let Some((name, digest)) = input.rsplit_once('@') {
        return (strip_tag(name), Some(digest.to_string()));
    }

    tag_colon(input).map_or((input, None), |colon| {
        (&input[..colon], Some(input[colon + 1..].to_string()))
    })
}

/// Returns the index of the tag-separating `:` in an image name, ignoring a
/// `:` that belongs to a registry `host:port` (i.e. one before the last `/`).
fn tag_colon(name: &str) -> Option<usize> {
    let last_slash = name.rfind('/').map_or(0, |index| index + 1);
    name.rfind(':').filter(|&colon| colon > last_slash)
}

fn strip_tag(name: &str) -> &str {
    tag_colon(name).map_or(name, |colon| &name[..colon])
}

fn looks_like_registry(value: &str) -> bool {
    value == "localhost" || value.contains('.') || value.contains(':')
}

#[derive(Debug, Deserialize)]
struct ImageManifest {
    #[serde(default)]
    manifests: Option<Vec<Descriptor>>,
    #[serde(default)]
    config: Option<Descriptor>,
    #[serde(default)]
    layers: Vec<Descriptor>,
}

#[derive(Debug, Deserialize)]
struct Descriptor {
    #[serde(rename = "mediaType")]
    media_type: Option<String>,
    digest: String,
    #[serde(default)]
    size: Option<u64>,
    platform: Option<Platform>,
}

#[derive(Debug, Deserialize)]
struct Platform {
    os: String,
    architecture: String,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    token: Option<String>,
    access_token: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ImageConfig {
    #[serde(default)]
    config: ImageConfigBody,
}

#[derive(Debug, Default, Deserialize)]
struct ImageConfigBody {
    #[serde(default, rename = "Entrypoint")]
    entrypoint: Option<Vec<String>>,
    #[serde(default, rename = "Cmd")]
    cmd: Option<Vec<String>>,
    #[serde(default, rename = "Env")]
    env: Option<Vec<String>>,
    #[serde(default, rename = "WorkingDir")]
    working_dir: Option<String>,
}

impl ImageConfig {
    fn entrypoint(&self) -> &[String] {
        self.config.entrypoint.as_deref().unwrap_or_default()
    }

    fn cmd(&self) -> &[String] {
        self.config.cmd.as_deref().unwrap_or_default()
    }

    fn env(&self) -> Vec<EnvVar> {
        self.config
            .env
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter_map(|entry| {
                let (name, value) = entry.split_once('=')?;
                Some(EnvVar {
                    name: name.to_string(),
                    value: value.to_string(),
                })
            })
            .collect()
    }

    fn working_dir(&self) -> Option<&str> {
        self.config
            .working_dir
            .as_deref()
            .filter(|value| !value.is_empty())
    }
}

#[derive(Debug)]
struct GvisorConfig<'a> {
    container_name: &'a str,
    args: Vec<String>,
    env: Vec<String>,
    cwd: String,
    rootfs: PathBuf,
    rootfs_overlay: PathBuf,
    network_namespace: Option<PathBuf>,
    resolv_conf: Option<PathBuf>,
    /// Extra host bind mounts (e.g. static assets like the `omniagent` binary).
    mounts: Vec<MountSpec>,
    /// CPU/memory cgroup limits emitted under `linux.resources`; `None` leaves
    /// the container unconstrained.
    resources: Option<ResourceSpec>,
    annotations: BTreeMap<String, String>,
}

/// Builds the OCI `linux.resources` JSON for a [`ResourceSpec`], emitting only
/// the cpu/memory fields that were set. Returns `None` when nothing is set.
fn linux_resources_json(resources: &ResourceSpec) -> Option<serde_json::Value> {
    let mut out = serde_json::Map::new();

    if let Some(cpu) = &resources.cpu {
        let mut cpu_json = serde_json::Map::new();
        if let Some(quota) = cpu.quota {
            cpu_json.insert("quota".to_string(), json!(quota));
        }
        if let Some(period) = cpu.period {
            cpu_json.insert("period".to_string(), json!(period));
        }
        if let Some(shares) = cpu.shares {
            cpu_json.insert("shares".to_string(), json!(shares));
        }
        if !cpu_json.is_empty() {
            out.insert("cpu".to_string(), json!(cpu_json));
        }
    }

    if let Some(memory) = &resources.memory
        && let Some(limit) = memory.limit
    {
        out.insert("memory".to_string(), json!({ "limit": limit }));
    }

    if out.is_empty() {
        None
    } else {
        Some(json!(out))
    }
}

async fn write_gvisor_config_json(path: &Path, config: &GvisorConfig<'_>) -> Result<(), OciError> {
    if config.args.is_empty() {
        return Err(OciError::NoCommand(config.container_name.to_string()));
    }
    if let Some(parent) = path.parent() {
        async_fs::create_dir_all(parent).await?;
    }

    let mut namespaces = vec![
        json!({"type": "pid"}),
        json!({"type": "ipc"}),
        json!({"type": "uts"}),
        json!({"type": "mount"}),
    ];

    // An explicit network namespace path is honored when configured (the
    // operator pre-created one); otherwise the entry is omitted and runsc sets
    // up its own netstack network.
    if let Some(netns) = &config.network_namespace {
        namespaces.push(json!({"type": "network", "path": netns}));
    }

    let mut linux = json!({ "namespaces": namespaces });

    // CPU/memory cgroup limits (honored by runsc), emitting only the fields set.
    if let Some(resources) = config.resources.as_ref().and_then(linux_resources_json) {
        linux["resources"] = resources;
    }

    let mut annotations = config.annotations.clone();
    annotations.insert(
        GVISOR_ROOTFS_SOURCE_ANNOTATION.to_string(),
        config.rootfs.display().to_string(),
    );
    annotations.insert(
        GVISOR_ROOTFS_TYPE_ANNOTATION.to_string(),
        "erofs".to_string(),
    );
    annotations.insert(
        GVISOR_ROOTFS_OVERLAY_ANNOTATION.to_string(),
        format!("dir={}", config.rootfs_overlay.display()),
    );
    let resolv_conf = config
        .resolv_conf
        .as_deref()
        .unwrap_or_else(|| Path::new("/etc/resolv.conf"));
    let config_json = json!({
        "ociVersion": "1.0.2",
        "process": {
            "terminal": false,
            "user": {"uid": 0, "gid": 0},
            "args": config.args,
            "env": config.env,
            "cwd": config.cwd,
            "capabilities": {
                "bounding": ["CAP_AUDIT_WRITE", "CAP_KILL", "CAP_NET_BIND_SERVICE"],
                "effective": ["CAP_AUDIT_WRITE", "CAP_KILL", "CAP_NET_BIND_SERVICE"],
                "inheritable": ["CAP_AUDIT_WRITE", "CAP_KILL", "CAP_NET_BIND_SERVICE"],
                "permitted": ["CAP_AUDIT_WRITE", "CAP_KILL", "CAP_NET_BIND_SERVICE"]
            },
            "rlimits": [{"type": "RLIMIT_NOFILE", "hard": 1024, "soft": 1024}],
            "noNewPrivileges": true
        },
        "root": {"path": "rootfs", "readonly": false},
        "hostname": "runsc",
        "mounts": [
            {"destination": "/proc", "type": "proc", "source": "proc"},
            {"destination": "/dev", "type": "tmpfs", "source": "tmpfs"},
            {
                "destination": "/sys",
                "type": "sysfs",
                "source": "sysfs",
                "options": ["nosuid", "noexec", "nodev", "ro"]
            },
            {
                "destination": "/etc/resolv.conf",
                "type": "bind",
                "source": resolv_conf,
                "options": ["ro"]
            }
        ],
        "linux": linux,
        "annotations": annotations
    });

    // Append any configured static bind mounts (e.g. the `omniagent` binary dir).
    let mut config_json = config_json;
    if let Some(mounts) = config_json.get_mut("mounts").and_then(|m| m.as_array_mut()) {
        for mount in &config.mounts {
            let mut options = vec!["rbind".to_string()];
            options.push(if mount.read_only { "ro" } else { "rw" }.to_string());
            mounts.push(json!({
                "destination": mount.destination,
                "type": "bind",
                "source": mount.source,
                "options": options,
            }));
        }
    }

    let body = serde_json::to_vec_pretty(&config_json).map_err(OciError::Json)?;
    async_fs::write(path, body).await?;
    Ok(())
}

async fn prepare_rootfs_overlay_dir(
    paths: &OadPaths,
    sandbox_id: &SandboxId,
    container: &str,
) -> Result<PathBuf, OciError> {
    let overlay = paths.rootfs_overlay_dir(sandbox_id, container);
    async_fs::create_dir_all(&overlay).await?;
    Ok(async_fs::canonicalize(overlay).await?)
}

fn resolve_args(
    container: &ContainerSpec,
    image_config: &ImageConfig,
) -> Result<Vec<String>, OciError> {
    let args = if !container.command.is_empty() {
        container.argv()
    } else if !container.args.is_empty() {
        image_config
            .entrypoint()
            .iter()
            .cloned()
            .chain(container.args.iter().cloned())
            .collect()
    } else {
        image_config
            .entrypoint()
            .iter()
            .cloned()
            .chain(image_config.cmd().iter().cloned())
            .collect()
    };

    if args.is_empty() {
        Err(OciError::NoCommand(container.name.clone()))
    } else {
        Ok(args)
    }
}

fn merged_env(image_env: &[EnvVar], request_env: &[EnvVar]) -> Vec<String> {
    let mut merged = BTreeMap::new();
    merged.insert("PATH".to_string(), DEFAULT_PATH.to_string());
    for item in image_env.iter().chain(request_env.iter()) {
        merged.insert(item.name.clone(), item.value.clone());
    }
    merged
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect()
}

fn cwd_or_root(value: Option<&str>) -> String {
    match value {
        Some(value) if value.starts_with('/') => value.to_string(),
        _ => "/".to_string(),
    }
}

fn select_platform_manifest(manifests: &[Descriptor]) -> Result<&Descriptor, OciError> {
    let arch = std::env::consts::ARCH;
    let oci_arch = match arch {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    };

    manifests
        .iter()
        .find(|descriptor| {
            descriptor
                .platform
                .as_ref()
                .is_some_and(|platform| platform.os == "linux" && platform.architecture == oci_arch)
        })
        .ok_or_else(|| OciError::PlatformUnavailable(format!("linux/{oci_arch}")))
}

fn verify_digest(bytes: &[u8], expected: &str) -> Result<(), OciError> {
    let actual = hex::encode(Sha256::digest(bytes));
    verify_digest_hex(&actual, expected)
}

fn verify_digest_hex(actual: &str, expected: &str) -> Result<(), OciError> {
    let Some(hex_expected) = expected.strip_prefix("sha256:") else {
        return Err(OciError::UnsupportedDigest(expected.to_string()));
    };
    if actual == hex_expected {
        Ok(())
    } else {
        Err(OciError::DigestMismatch {
            expected: expected.to_string(),
            actual: format!("sha256:{actual}"),
        })
    }
}

fn blob_filename(digest: &str) -> Result<String, OciError> {
    let Some(hex_digest) = digest.strip_prefix("sha256:") else {
        return Err(OciError::UnsupportedDigest(digest.to_string()));
    };
    // The digest comes from an untrusted manifest and is used to build a cache
    // file path; reject anything that is not a bare sha256 hex string so it can
    // never contain path separators or `..` traversal components.
    if hex_digest.len() != 64 || !hex_digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(OciError::UnsupportedDigest(digest.to_string()));
    }
    Ok(format!("sha256-{hex_digest}.blob"))
}

async fn read_blob_cache(
    path: &Path,
    expected_digest: &str,
    max_bytes: u64,
    kind: &'static str,
) -> Result<Option<Bytes>, OciError> {
    if !async_fs::try_exists(path).await? {
        return Ok(None);
    }

    match read_blob_file_limited(path, expected_digest, max_bytes, kind).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(OciError::DigestMismatch { .. }) => {
            debug!(
                cached = %path.display(),
                digest = expected_digest,
                "discarding cached blob with mismatched digest"
            );
            let _ = async_fs::remove_file(path).await;
            Ok(None)
        }
        Err(OciError::BodyTooLarge { .. }) => {
            debug!(
                cached = %path.display(),
                digest = expected_digest,
                "discarding oversized cached blob"
            );
            let _ = async_fs::remove_file(path).await;
            Ok(None)
        }
        Err(err) => Err(err),
    }
}

async fn read_blob_file_limited(
    path: &Path,
    expected_digest: &str,
    max_bytes: u64,
    kind: &'static str,
) -> Result<Bytes, OciError> {
    let mut file = async_fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut body = BytesMut::new();
    let mut downloaded = 0_u64;
    let mut buf = [0_u8; 8 * 1024];

    loop {
        let read = file.read(&mut buf).await?;
        if read == 0 {
            break;
        }
        downloaded = downloaded
            .checked_add(read as u64)
            .ok_or(OciError::BodyTooLarge {
                kind,
                limit: max_bytes,
            })?;
        if downloaded > max_bytes {
            return Err(OciError::BodyTooLarge {
                kind,
                limit: max_bytes,
            });
        }
        hasher.update(&buf[..read]);
        body.extend_from_slice(&buf[..read]);
    }

    verify_digest_hex(&hex::encode(hasher.finalize()), expected_digest)?;
    Ok(body.freeze())
}

async fn verify_blob_file(path: &Path, expected_digest: &str) -> Result<(), OciError> {
    let mut file = async_fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = [0_u8; 8 * 1024];
    loop {
        let read = file.read(&mut buf).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    verify_digest_hex(&hex::encode(hasher.finalize()), expected_digest)
}

/// A pulled image's resolved config and the path to its (cached) EROFS rootfs.
struct PulledImage {
    config: ImageConfig,
    rootfs: PathBuf,
}

/// Content key for a built rootfs: the EROFS image is determined by its ordered
/// layer digests *and* how we build it. The builder version is mixed in so that
/// changing the build (flags, mount-point layer, pipeline) invalidates rootfs
/// images cached by a previous version instead of silently reusing them.
fn rootfs_cache_key(layers: &[Descriptor]) -> String {
    /// Bump whenever the produced rootfs bytes can change for the same layers.
    const ROOTFS_BUILDER_VERSION: &str = "2026-05-31";

    let mut hasher = Sha256::new();
    hasher.update(ROOTFS_BUILDER_VERSION.as_bytes());
    hasher.update(b"\n");
    for layer in layers {
        hasher.update(layer.digest.as_bytes());
        hasher.update(b"\n");
    }
    hex::encode(hasher.finalize())
}

fn manifest_cache_path(
    cache_dir: &Path,
    reference: &ImageReference,
    manifest_ref: &str,
) -> PathBuf {
    cache_dir.join(format!(
        "{}.json",
        manifest_cache_key(reference, manifest_ref)
    ))
}

fn manifest_cache_key(reference: &ImageReference, manifest_ref: &str) -> String {
    const MANIFEST_CACHE_VERSION: &str = "2026-05-31";

    let mut hasher = Sha256::new();
    hasher.update(MANIFEST_CACHE_VERSION.as_bytes());
    hasher.update(b"\0");
    hasher.update(reference.registry.as_bytes());
    hasher.update(b"\0");
    hasher.update(reference.repository.as_bytes());
    hasher.update(b"\0");
    hasher.update(manifest_ref.as_bytes());
    hex::encode(hasher.finalize())
}

fn manifest_cache_entry_is_fresh(modified: SystemTime, now: SystemTime, ttl: Duration) -> bool {
    now.duration_since(modified).map_or(true, |age| age <= ttl)
}

/// Builds a process-unique temp filename for atomic publish-by-rename. Avoids
/// wall-clock/random sources; uniqueness comes from the PID and a counter.
fn temp_name(stem: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(".{stem}.{}.{n}.tmp", std::process::id())
}

/// Removes a temp path on drop unless `disarm`ed. A layer download or rootfs
/// build can be *cancelled* mid-flight — e.g. `buffer_unordered` drops the
/// other in-flight layer futures the moment one sibling fails, so their
/// explicit error-path cleanup never runs — which would leak partial `.tmp`
/// files into the cache. This guard cleans them up on every exit path
/// (error or cancellation) so only fully-downloaded, verified files remain.
struct TempPathGuard {
    path: Option<PathBuf>,
}

impl TempPathGuard {
    const fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    /// Keeps the file (the download/build succeeded and was published).
    fn disarm(mut self) {
        self.path = None;
    }
}

impl Drop for TempPathGuard {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            // Best-effort, synchronous: Drop cannot await. A missing file
            // (already renamed into place) is fine.
            let _ = fs::remove_file(&path);
        }
    }
}

async fn build_erofs(source: &Path, image: &Path) -> Result<(), OciError> {
    if let Some(parent) = image.parent() {
        async_fs::create_dir_all(parent).await?;
    }

    let output = Command::new("mkfs.erofs")
        .arg("-Enoinline_data")
        .arg("--all-root")
        .arg(image)
        .arg(source)
        .output()
        .await
        .map_err(OciError::MkfsErofsSpawn)?;

    if output.status.success() {
        Ok(())
    } else {
        Err(OciError::MkfsErofsExit {
            status: output.status.to_string(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

async fn valid_rootfs_cache_entry(rootfs: &Path) -> Result<bool, OciError> {
    Ok(async_fs::try_exists(rootfs).await?)
}

/// Extracts every layer into a staging directory (applying ordering and
/// whiteouts), then builds the EROFS image from it in one pass.
async fn build_erofs_from_staging(
    blobs: &[(PathBuf, String)],
    staging: &Path,
    image: &Path,
) -> Result<(), OciError> {
    remove_dir_if_exists(staging).await?;
    async_fs::create_dir_all(staging).await?;

    // One budget shared across every layer of this image, so the total unpacked
    // size is bounded regardless of how many layers the manifest lists.
    let budget = ExtractionBudget::new(MAX_UNPACKED_IMAGE_BYTES);

    for (path, media_type) in blobs {
        let staging = staging.to_path_buf();
        let media_type = media_type.clone();
        let path = path.clone();
        let budget = budget.clone();
        tokio::task::spawn_blocking(move || {
            extract_layer_from_file(&staging, &media_type, &path, &budget)
        })
        .await
        .map_err(|err| OciError::Join(err.to_string()))??;
    }

    ensure_mount_points(staging).await?;
    build_erofs(staging, image).await?;
    remove_dir_if_exists(staging).await?;
    Ok(())
}

async fn ensure_mount_points(rootfs: &Path) -> Result<(), OciError> {
    let rootfs = rootfs.to_path_buf();
    tokio::task::spawn_blocking(move || ensure_mount_points_sync(&rootfs))
        .await
        .map_err(|err| OciError::Join(err.to_string()))?
}

fn ensure_mount_points_sync(rootfs: &Path) -> Result<(), OciError> {
    for dir in ["dev", "proc", "sys", "etc", "tmp"] {
        ensure_directory(rootfs, &rootfs.join(dir), None)?;
    }
    ensure_regular_file_exists(rootfs, &rootfs.join("etc/resolv.conf"), 0o644)
}

#[cfg(test)]
fn extract_layer(rootfs: &Path, media_type: &str, bytes: Bytes) -> Result<(), OciError> {
    let reader: Box<dyn Read> = if media_type.contains("gzip") || bytes.starts_with(&[0x1f, 0x8b]) {
        Box::new(GzDecoder::new(Cursor::new(bytes)))
    } else {
        Box::new(Cursor::new(bytes))
    };
    extract_layer_from_reader(
        rootfs,
        reader,
        &ExtractionBudget::new(MAX_UNPACKED_IMAGE_BYTES),
    )
}

fn extract_layer_from_file(
    rootfs: &Path,
    media_type: &str,
    path: &Path,
    budget: &ExtractionBudget,
) -> Result<(), OciError> {
    let mut file = File::open(path)?;
    let mut magic = [0_u8; 2];
    let read = file.read(&mut magic)?;
    file.seek(SeekFrom::Start(0))?;

    let reader: Box<dyn Read> =
        if media_type.contains("gzip") || (read == 2 && magic == [0x1f, 0x8b]) {
            Box::new(GzDecoder::new(file))
        } else {
            Box::new(file)
        };
    extract_layer_from_reader(rootfs, reader, budget)
}

fn extract_layer_from_reader(
    rootfs: &Path,
    reader: Box<dyn Read>,
    budget: &ExtractionBudget,
) -> Result<(), OciError> {
    let mut archive = Archive::new(reader);
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.to_path_buf();
        if path == Path::new(".") {
            continue;
        }

        if handle_whiteout(rootfs, &path)? {
            continue;
        }

        let target = safe_join(rootfs, &path)?;
        ensure_parent_safe(rootfs, &target)?;

        match entry.header().entry_type() {
            EntryType::Directory => {
                ensure_directory(rootfs, &target, Some(entry.header().mode()?))?;
            }
            EntryType::Regular => {
                write_regular_file(rootfs, &target, entry.header().mode()?, &mut entry, budget)?;
            }
            EntryType::Symlink => {
                let link_name = entry
                    .link_name()?
                    .ok_or_else(|| OciError::UnsafeArchivePath(path.display().to_string()))?;
                // A symlink's target is stored verbatim and only resolved
                // inside the container at runtime, so absolute and parent
                // (`..`) targets are safe to write here. Attempts to write
                // *through* a symlink to escape rootfs are blocked separately
                // by ensure_parent_safe on every subsequent entry.
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent)?;
                }
                remove_path_if_exists(&target)?;
                std::os::unix::fs::symlink(link_name, &target)?;
            }
            EntryType::Link => {
                let link_name = entry
                    .link_name()?
                    .ok_or_else(|| OciError::UnsafeArchivePath(path.display().to_string()))?;
                // A hard link is resolved at extraction time (it shares an
                // inode with its target), so the target must stay within
                // rootfs. An absolute target is interpreted as relative to
                // rootfs (the conventional OCI/Docker behavior); safe_join
                // then rejects any `..` traversal, and the symlink check below
                // prevents linking through a symlinked path.
                let relative = link_name
                    .strip_prefix("/")
                    .unwrap_or_else(|_| link_name.as_ref());
                let source = safe_join(rootfs, relative)?;
                ensure_parent_safe(rootfs, &source)?;
                if fs::symlink_metadata(&source)?.file_type().is_symlink() {
                    return Err(OciError::UnsafeArchivePath(source.display().to_string()));
                }
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent)?;
                }
                remove_path_if_exists(&target)?;
                fs::hard_link(source, target)?;
            }
            EntryType::Char | EntryType::Block | EntryType::Fifo => {
                debug!(
                    path = %path.display(),
                    entry_type = ?entry.header().entry_type(),
                    "skipping unsupported special tar entry"
                );
            }
            other => {
                return Err(OciError::UnsupportedTarEntry(format!("{other:?}")));
            }
        }
    }

    Ok(())
}

fn handle_whiteout(rootfs: &Path, path: &Path) -> Result<bool, OciError> {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return Ok(false);
    };

    if file_name == ".wh..wh..opq" {
        let parent = safe_join(rootfs, path.parent().unwrap_or_else(|| Path::new("")))?;
        ensure_parent_safe(rootfs, &parent.join(".opq-check"))?;
        if parent.exists() {
            for entry in fs::read_dir(parent)? {
                let entry = entry?;
                remove_path_if_exists(&entry.path())?;
            }
        }
        return Ok(true);
    }

    if let Some(stripped) = file_name.strip_prefix(".wh.") {
        let parent = path.parent().unwrap_or_else(|| Path::new(""));
        let target = safe_join(rootfs, &parent.join(stripped))?;
        ensure_parent_safe(rootfs, &target)?;
        remove_path_if_exists(&target)?;
        return Ok(true);
    }

    Ok(false)
}

fn safe_join(rootfs: &Path, path: &Path) -> Result<PathBuf, OciError> {
    let mut target = rootfs.to_path_buf();
    for component in path.components() {
        match component {
            Component::Normal(value) => target.push(value),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(OciError::UnsafeArchivePath(path.display().to_string()));
            }
        }
    }
    Ok(target)
}

fn ensure_parent_safe(rootfs: &Path, target: &Path) -> Result<(), OciError> {
    let parent = target.parent().unwrap_or(rootfs);
    let relative = parent.strip_prefix(rootfs).map_err(|_| {
        OciError::UnsafeArchivePath(format!("{} escapes {}", target.display(), rootfs.display()))
    })?;

    let mut cursor = rootfs.to_path_buf();
    for component in relative.components() {
        if let Component::Normal(value) = component {
            cursor.push(value);
            if let Ok(metadata) = fs::symlink_metadata(&cursor)
                && metadata.file_type().is_symlink()
            {
                return Err(OciError::UnsafeArchivePath(cursor.display().to_string()));
            }
        }
    }
    Ok(())
}

fn set_mode(path: &Path, mode: u32) {
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(mode));
}

fn ensure_directory(rootfs: &Path, target: &Path, mode: Option<u32>) -> Result<(), OciError> {
    ensure_parent_safe(rootfs, target)?;
    match fs::symlink_metadata(target) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            remove_path_if_exists(target)?;
        }
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(OciError::Io(err)),
    }
    fs::create_dir_all(target)?;
    if let Some(mode) = mode {
        set_mode(target, mode);
    }
    Ok(())
}

fn ensure_regular_file_exists(rootfs: &Path, target: &Path, mode: u32) -> Result<(), OciError> {
    ensure_parent_safe(rootfs, target)?;
    match fs::symlink_metadata(target) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => Ok(()),
        Err(err) if err.kind() != io::ErrorKind::NotFound => Err(OciError::Io(err)),
        // Missing, or present but not a regular file: (re)create it empty.
        _ => write_regular_file(
            rootfs,
            target,
            mode,
            &mut io::empty(),
            &ExtractionBudget::new(MAX_UNPACKED_IMAGE_BYTES),
        ),
    }
}

fn write_regular_file(
    rootfs: &Path,
    target: &Path,
    mode: u32,
    contents: &mut dyn Read,
    budget: &ExtractionBudget,
) -> Result<(), OciError> {
    ensure_parent_safe(rootfs, target)?;
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    remove_path_if_exists(target)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(mode & 0o7777)
        .custom_flags(libc::O_NOFOLLOW)
        .open(target)?;
    budget.copy(contents, &mut file)?;
    let _ = file.set_permissions(fs::Permissions::from_mode(mode & 0o7777));
    Ok(())
}

/// Tracks the total unpacked bytes remaining for one image's extraction. Cheap
/// to `clone` (shared counter) so it can be threaded through the per-layer
/// `spawn_blocking` tasks while enforcing a single image-wide cap.
#[derive(Clone)]
struct ExtractionBudget {
    remaining: std::sync::Arc<AtomicU64>,
}

impl ExtractionBudget {
    fn new(limit: u64) -> Self {
        Self {
            remaining: std::sync::Arc::new(AtomicU64::new(limit)),
        }
    }

    fn take(&self, amount: u64) -> Result<(), OciError> {
        let mut current = self.remaining.load(Ordering::Relaxed);
        loop {
            let Some(next) = current.checked_sub(amount) else {
                return Err(OciError::BodyTooLarge {
                    kind: "unpacked image",
                    limit: MAX_UNPACKED_IMAGE_BYTES,
                });
            };
            match self.remaining.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    fn copy(&self, reader: &mut dyn Read, writer: &mut File) -> Result<(), OciError> {
        let mut buf = vec![0_u8; 128 * 1024].into_boxed_slice();
        loop {
            let read = reader.read(&mut buf)?;
            if read == 0 {
                return Ok(());
            }
            self.take(read as u64)?;
            writer.write_all(&buf[..read])?;
        }
    }
}

fn remove_path_if_exists(path: &Path) -> Result<(), OciError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            fs::remove_dir_all(path)?;
        }
        Ok(_) => {
            fs::remove_file(path)?;
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(OciError::Io(err)),
    }
    Ok(())
}

async fn remove_dir_if_exists(path: &Path) -> Result<(), OciError> {
    match async_fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(OciError::Io(err)),
    }
}

#[derive(Debug, Default)]
struct BearerChallenge {
    realm: String,
    service: Option<String>,
    scope: Option<String>,
}

impl BearerChallenge {
    fn parse(value: &str) -> Result<Self, OciError> {
        let Some(rest) = value.strip_prefix("Bearer ") else {
            return Err(OciError::InvalidAuthChallenge);
        };
        let mut challenge = Self::default();
        for piece in rest.split(',') {
            let Some((key, value)) = piece.trim().split_once('=') else {
                continue;
            };
            let value = value.trim_matches('"').to_string();
            match key {
                "realm" => challenge.realm = value,
                "service" => challenge.service = Some(value),
                "scope" => challenge.scope = Some(value),
                _ => {}
            }
        }
        if challenge.realm.is_empty() {
            Err(OciError::InvalidAuthChallenge)
        } else {
            Ok(challenge)
        }
    }
}

#[derive(Debug, Error)]
pub enum OciError {
    #[error("invalid image reference {0:?}")]
    InvalidReference(String),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
    #[error("json error: {0}")]
    Json(serde_json::Error),
    #[error("{kind} JSON from {body_source:?} is invalid: {error}; body starts with {preview:?}")]
    JsonBody {
        kind: &'static str,
        body_source: String,
        error: serde_json::Error,
        preview: String,
    },
    #[error("registry auth challenge is missing")]
    MissingAuthChallenge,
    #[error("registry auth challenge is invalid")]
    InvalidAuthChallenge,
    #[error("registry token response is missing a token")]
    MissingToken,
    #[error("manifest is invalid: {0}")]
    InvalidManifest(&'static str),
    #[error("platform {0} is not available in image index")]
    PlatformUnavailable(String),
    #[error("unsupported digest {0:?}")]
    UnsupportedDigest(String),
    #[error("digest mismatch: expected {expected}, got {actual}")]
    DigestMismatch { expected: String, actual: String },
    #[error("{kind} body exceeded {limit} bytes")]
    BodyTooLarge { kind: &'static str, limit: u64 },
    #[error("unsupported tar entry {0}")]
    UnsupportedTarEntry(String),
    #[error("archive path is unsafe: {0}")]
    UnsafeArchivePath(String),
    #[error("container {0:?} has no command to execute")]
    NoCommand(String),
    #[error("failed to spawn mkfs.erofs: {0}")]
    MkfsErofsSpawn(io::Error),
    #[error("mkfs.erofs failed with {status}: stdout={stdout:?}, stderr={stderr:?}")]
    MkfsErofsExit {
        status: String,
        stdout: String,
        stderr: String,
    },
    #[error("background task failed: {0}")]
    Join(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_docker_hub_short_reference() {
        let reference = ImageReference::parse("alpine:3.20").unwrap();
        assert_eq!(reference.registry, DOCKER_HUB_REGISTRY);
        assert_eq!(reference.repository, "library/alpine");
        assert_eq!(reference.reference, "3.20");
        assert!(!reference.reference_is_digest);
    }

    #[test]
    fn parses_digest_reference_as_digest() {
        let reference =
            ImageReference::parse("example.com/team/app@sha512:abcdef0123456789").unwrap();
        assert_eq!(reference.registry, "example.com");
        assert_eq!(reference.repository, "team/app");
        assert_eq!(reference.reference, "sha512:abcdef0123456789");
        assert!(reference.reference_is_digest);
    }

    #[test]
    fn parses_tagged_digest_reference_as_digest() {
        let reference =
            ImageReference::parse("example.com/team/app:stable@sha256:abcdef0123456789").unwrap();
        assert_eq!(reference.registry, "example.com");
        assert_eq!(reference.repository, "team/app");
        assert_eq!(reference.reference, "sha256:abcdef0123456789");
        assert!(reference.reference_is_digest);
    }

    #[test]
    fn parses_localhost_reference_without_port_as_registry() {
        let reference = ImageReference::parse("localhost/app").unwrap();
        assert_eq!(reference.registry, "localhost");
        assert_eq!(reference.repository, "app");
        assert_eq!(reference.reference, DEFAULT_TAG);
    }

    #[test]
    fn rejects_path_traversal() {
        let root = Path::new("/tmp/root");
        assert!(matches!(
            safe_join(root, Path::new("../etc/passwd")),
            Err(OciError::UnsafeArchivePath(_))
        ));
    }

    fn layer(digest: &str) -> Descriptor {
        Descriptor {
            media_type: Some("application/vnd.oci.image.layer.v1.tar+gzip".to_string()),
            digest: digest.to_string(),
            size: None,
            platform: None,
        }
    }

    #[test]
    fn rootfs_cache_key_depends_on_ordered_layers() {
        let a = rootfs_cache_key(&[layer("sha256:aa"), layer("sha256:bb")]);
        let same = rootfs_cache_key(&[layer("sha256:aa"), layer("sha256:bb")]);
        let reordered = rootfs_cache_key(&[layer("sha256:bb"), layer("sha256:aa")]);
        let extra = rootfs_cache_key(&[layer("sha256:aa"), layer("sha256:bb"), layer("sha256:cc")]);

        assert_eq!(a, same, "same ordered layers must yield the same key");
        assert_ne!(a, reordered, "layer order must affect the key");
        assert_ne!(a, extra, "an added layer must change the key");
    }

    #[test]
    fn manifest_cache_key_depends_on_reference() {
        let reference = ImageReference::parse("example.com/team/app:latest").unwrap();
        let same = ImageReference::parse("example.com/team/app:latest").unwrap();
        let different_repository = ImageReference::parse("example.com/team/other:latest").unwrap();

        assert_eq!(
            manifest_cache_key(&reference, &reference.reference),
            manifest_cache_key(&same, &same.reference)
        );
        assert_ne!(
            manifest_cache_key(&reference, &reference.reference),
            manifest_cache_key(&different_repository, &different_repository.reference)
        );
        assert_ne!(
            manifest_cache_key(&reference, "latest"),
            manifest_cache_key(&reference, "sha256:abc")
        );
    }

    #[test]
    fn manifest_cache_freshness_uses_ttl() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_mins(2);
        assert!(manifest_cache_entry_is_fresh(
            now - Duration::from_mins(1),
            now,
            Duration::from_mins(1)
        ));
        assert!(!manifest_cache_entry_is_fresh(
            now - Duration::from_secs(61),
            now,
            Duration::from_mins(1)
        ));
        assert!(manifest_cache_entry_is_fresh(
            now + Duration::from_secs(1),
            now,
            Duration::from_mins(1)
        ));
    }

    #[tokio::test]
    async fn reads_fresh_manifest_cache_entry() {
        let temp = tempfile::tempdir().unwrap();
        let reference = ImageReference::parse("example.com/team/app:latest").unwrap();
        let cached = manifest_cache_path(temp.path(), &reference, &reference.reference);
        let body = br#"{
            "schemaVersion": 2,
            "config": {"digest": "sha256:1111"},
            "layers": []
        }"#;
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(body)));
        write_atomic_file(&cached, body).await.unwrap();

        let client = RegistryClient::new();
        let manifest = client
            .read_manifest_cache(&reference, &reference.reference, Some(&digest), &cached)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(manifest.config.unwrap().digest, "sha256:1111");
        assert!(manifest.layers.is_empty());
    }

    #[tokio::test]
    async fn discards_cached_manifest_digest_mismatch() {
        let temp = tempfile::tempdir().unwrap();
        let reference = ImageReference::parse("example.com/team/app@sha256:bad").unwrap();
        let cached = manifest_cache_path(temp.path(), &reference, &reference.reference);
        write_atomic_file(&cached, br#"{"layers":[]}"#)
            .await
            .unwrap();

        let client = RegistryClient::new();
        let manifest = client
            .read_manifest_cache(
                &reference,
                &reference.reference,
                Some("sha256:0000"),
                &cached,
            )
            .await
            .unwrap();

        assert!(manifest.is_none());
        assert!(!cached.exists());
    }

    #[tokio::test]
    async fn cached_blob_bytes_are_reused() {
        let temp = tempfile::tempdir().unwrap();
        let body = br#"{"config":{}}"#;
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(body)));
        let cached = temp.path().join(blob_filename(&digest).unwrap());
        write_atomic_file(&cached, body).await.unwrap();

        let bytes = read_blob_cache(&cached, &digest, MAX_CONFIG_BYTES, "config")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(bytes.as_ref(), body);
    }

    #[tokio::test]
    async fn oversized_cached_blob_is_discarded() {
        let temp = tempfile::tempdir().unwrap();
        let body = b"oversized";
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(body)));
        let cached = temp.path().join(blob_filename(&digest).unwrap());
        write_atomic_file(&cached, body).await.unwrap();

        let bytes = read_blob_cache(&cached, &digest, 2, "config")
            .await
            .unwrap();

        assert!(bytes.is_none());
        assert!(!cached.exists());
    }

    #[test]
    fn extracts_absolute_symlink_target_verbatim() {
        let tmp = tempfile::tempdir().unwrap();
        let layer = build_tar(|builder| {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(EntryType::Symlink);
            header.set_size(0);
            header.set_cksum();
            // Absolute target, as used by alpine/ubuntu (e.g. /bin/sh -> /bin/busybox).
            builder
                .append_link(&mut header, "bin/sh", "/bin/busybox")
                .unwrap();
        });

        extract_layer(tmp.path(), "", Bytes::from(layer)).unwrap();

        let link = tmp.path().join("bin/sh");
        let target = std::fs::read_link(&link).unwrap();
        assert_eq!(target, Path::new("/bin/busybox"));
    }

    #[test]
    fn rejects_hardlink_parent_escape() {
        let tmp = tempfile::tempdir().unwrap();
        let layer = build_tar(|builder| {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(EntryType::Link);
            header.set_size(0);
            header.set_cksum();
            // A hard link whose target escapes rootfs via `..` must be rejected.
            builder
                .append_link(&mut header, "escape", "../../../../etc/passwd")
                .unwrap();
        });

        assert!(matches!(
            extract_layer(tmp.path(), "", Bytes::from(layer)),
            Err(OciError::UnsafeArchivePath(_))
        ));
    }

    #[test]
    fn image_config_accepts_null_entrypoint_and_cmd() {
        let config: ImageConfig = serde_json::from_str(
            r#"{"config":{"Entrypoint":null,"Cmd":null,"Env":null,"WorkingDir":null}}"#,
        )
        .unwrap();

        assert!(config.entrypoint().is_empty());
        assert!(config.cmd().is_empty());
        assert!(config.env().is_empty());
        assert_eq!(config.working_dir(), None);
    }

    #[test]
    fn extraction_rejects_parent_symlink_escape() {
        let tmp = tempfile::tempdir().unwrap();
        let layer = build_tar(|builder| {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(EntryType::Symlink);
            header.set_size(0);
            header.set_cksum();
            builder
                .append_link(&mut header, "link", "safe-target")
                .unwrap();

            let mut file_header = tar::Header::new_gnu();
            file_header.set_entry_type(EntryType::Regular);
            file_header.set_size(4);
            file_header.set_mode(0o644);
            file_header.set_cksum();
            builder
                .append_data(&mut file_header, "link/file", Cursor::new(b"data"))
                .unwrap();
        });

        assert!(matches!(
            extract_layer(tmp.path(), "", Bytes::from(layer)),
            Err(OciError::UnsafeArchivePath(_))
        ));
    }

    #[test]
    fn regular_file_replaces_leaf_symlink_without_following_it() {
        let rootfs = tempfile::tempdir().unwrap();
        let host = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(host.path(), b"host").unwrap();
        let layer = build_tar(|builder| {
            let mut link_header = tar::Header::new_gnu();
            link_header.set_entry_type(EntryType::Symlink);
            link_header.set_size(0);
            link_header.set_cksum();
            builder
                .append_link(&mut link_header, "target", host.path())
                .unwrap();

            let mut file_header = tar::Header::new_gnu();
            file_header.set_entry_type(EntryType::Regular);
            file_header.set_size(5);
            file_header.set_mode(0o644);
            file_header.set_cksum();
            builder
                .append_data(&mut file_header, "target", Cursor::new(b"image"))
                .unwrap();
        });

        extract_layer(rootfs.path(), "", Bytes::from(layer)).unwrap();

        assert_eq!(std::fs::read(host.path()).unwrap(), b"host");
        assert_eq!(
            std::fs::read(rootfs.path().join("target")).unwrap(),
            b"image"
        );
        assert!(
            !std::fs::symlink_metadata(rootfs.path().join("target"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[test]
    fn directory_replaces_leaf_symlink_without_following_it() {
        let rootfs = tempfile::tempdir().unwrap();
        let host_dir = tempfile::tempdir().unwrap();
        std::fs::write(host_dir.path().join("marker"), b"host").unwrap();
        let layer = build_tar(|builder| {
            let mut link_header = tar::Header::new_gnu();
            link_header.set_entry_type(EntryType::Symlink);
            link_header.set_size(0);
            link_header.set_cksum();
            builder
                .append_link(&mut link_header, "etc", host_dir.path())
                .unwrap();

            let mut dir_header = tar::Header::new_gnu();
            dir_header.set_entry_type(EntryType::Directory);
            dir_header.set_size(0);
            dir_header.set_mode(0o755);
            dir_header.set_cksum();
            builder
                .append_data(&mut dir_header, "etc", Cursor::new(Vec::<u8>::new()))
                .unwrap();
        });

        extract_layer(rootfs.path(), "", Bytes::from(layer)).unwrap();

        assert_eq!(
            std::fs::read(host_dir.path().join("marker")).unwrap(),
            b"host"
        );
        let metadata = std::fs::symlink_metadata(rootfs.path().join("etc")).unwrap();
        assert!(metadata.is_dir());
        assert!(!metadata.file_type().is_symlink());
    }

    #[tokio::test]
    async fn mount_point_resolv_conf_replaces_leaf_symlink_without_following_it() {
        let rootfs = tempfile::tempdir().unwrap();
        let host = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(host.path(), b"host").unwrap();
        std::fs::create_dir(rootfs.path().join("etc")).unwrap();
        std::os::unix::fs::symlink(host.path(), rootfs.path().join("etc/resolv.conf")).unwrap();

        ensure_mount_points(rootfs.path()).await.unwrap();

        assert_eq!(std::fs::read(host.path()).unwrap(), b"host");
        let metadata = std::fs::symlink_metadata(rootfs.path().join("etc/resolv.conf")).unwrap();
        assert!(metadata.is_file());
        assert!(!metadata.file_type().is_symlink());
    }

    #[test]
    fn token_realm_rejects_internal_metadata_endpoint() {
        let url = Url::parse("https://169.254.169.254/latest/meta-data/").unwrap();
        assert!(matches!(
            validate_token_realm(&url),
            Err(OciError::InvalidAuthChallenge)
        ));
    }

    #[test]
    fn registry_url_rejects_internal_hosts() {
        let reference = ImageReference {
            registry: "10.0.0.5:5000".to_string(),
            repository: "team/app".to_string(),
            reference: DEFAULT_TAG.to_string(),
            reference_is_digest: false,
        };
        assert!(matches!(
            reference.registry_url("/v2/team/app/manifests/latest"),
            Err(OciError::InvalidReference(_))
        ));
    }

    #[test]
    fn ssrf_guard_rejects_ipv4_mapped_ipv6_metadata_host() {
        // ::ffff:169.254.169.254 must be blocked just like the bare IPv4 form.
        for host in [
            "https://[::ffff:169.254.169.254]/v2/",
            "https://[::ffff:127.0.0.1]/v2/",
            "https://[::ffff:10.0.0.1]/v2/",
        ] {
            let url = Url::parse(host).unwrap();
            assert!(
                validate_remote_https_url(&url).is_err(),
                "expected {host} to be rejected"
            );
        }
    }

    #[test]
    fn ssrf_guard_rejects_extra_ipv4_ranges() {
        for host in [
            "https://255.255.255.255/v2/", // limited broadcast
            "https://100.64.0.1/v2/",      // CGNAT 100.64.0.0/10
            "https://0.1.2.3/v2/",         // 0.0.0.0/8
            "https://192.0.0.1/v2/",       // 192.0.0.0/24
        ] {
            let url = Url::parse(host).unwrap();
            assert!(
                validate_remote_https_url(&url).is_err(),
                "expected {host} to be rejected"
            );
        }
        // A genuinely public host is still allowed.
        assert!(
            validate_remote_https_url(&Url::parse("https://93.184.216.34/v2/").unwrap()).is_ok()
        );
    }

    #[test]
    fn blob_filename_rejects_non_hex_digest() {
        // Path-traversal and non-sha256 digests must not become cache paths.
        for digest in [
            "sha256:../../../../etc/cron.d/x",
            "sha256:not-hex",
            "sha256:",
            "sha512:0000000000000000000000000000000000000000000000000000000000000000",
        ] {
            assert!(
                matches!(blob_filename(digest), Err(OciError::UnsupportedDigest(_))),
                "expected {digest} to be rejected"
            );
        }
        let valid = "sha256:".to_string() + &"a".repeat(64);
        assert_eq!(
            blob_filename(&valid).unwrap(),
            format!("sha256-{}.blob", "a".repeat(64))
        );
    }

    #[test]
    fn descriptor_size_caps_large_bodies() {
        let descriptor = Descriptor {
            media_type: None,
            digest: "sha256:abc".to_string(),
            size: Some(MAX_CONFIG_BYTES + 1),
            platform: None,
        };
        assert!(matches!(
            descriptor_body_limit(&descriptor, MAX_CONFIG_BYTES, "config"),
            Err(OciError::BodyTooLarge { kind: "config", .. })
        ));
    }

    #[tokio::test]
    async fn cached_blob_digest_is_verified() {
        let blob = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(blob.path(), b"layer").unwrap();
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(b"layer")));

        verify_blob_file(blob.path(), &digest).await.unwrap();

        std::fs::write(blob.path(), b"tampered").unwrap();
        assert!(matches!(
            verify_blob_file(blob.path(), &digest).await,
            Err(OciError::DigestMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn rootfs_cache_entry_is_the_published_rootfs() {
        let temp = tempfile::tempdir().unwrap();
        let rootfs = temp.path().join("cached.erofs");

        assert!(!valid_rootfs_cache_entry(&rootfs).await.unwrap());

        std::fs::write(&rootfs, b"complete").unwrap();
        assert!(valid_rootfs_cache_entry(&rootfs).await.unwrap());
    }

    fn build_tar(write_entries: impl FnOnce(&mut tar::Builder<Vec<u8>>)) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        write_entries(&mut builder);
        builder.finish().unwrap();
        builder.into_inner().unwrap()
    }

    async fn write_config_json(config: &GvisorConfig<'_>) -> serde_json::Value {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("config.json");
        write_gvisor_config_json(&path, config).await.unwrap();
        let body = std::fs::read(&path).unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    fn namespace_types(config_json: &serde_json::Value) -> Vec<String> {
        config_json["linux"]["namespaces"]
            .as_array()
            .unwrap()
            .iter()
            .map(|ns| ns["type"].as_str().unwrap().to_string())
            .collect()
    }

    #[tokio::test]
    async fn spec_has_no_user_namespace_or_mappings() {
        let config = GvisorConfig {
            container_name: "web",
            args: vec!["/bin/sh".to_string()],
            env: vec![],
            cwd: "/".to_string(),
            rootfs: PathBuf::from("/tmp/rootfs.erofs"),
            rootfs_overlay: PathBuf::from("/tmp/rootfs-overlay"),
            network_namespace: None,
            resolv_conf: None,
            mounts: Vec::new(),
            resources: None,
            annotations: BTreeMap::new(),
        };
        let config_json = write_config_json(&config).await;

        let types = namespace_types(&config_json);
        assert!(
            !types.contains(&"user".to_string()),
            "namespaces: {types:?}"
        );
        // With no explicit netns configured, no network namespace entry is added.
        assert!(
            !types.contains(&"network".to_string()),
            "namespaces: {types:?}"
        );
        assert!(config_json["linux"]["uidMappings"].is_null());
        assert!(config_json["linux"]["gidMappings"].is_null());
    }

    #[tokio::test]
    async fn spec_keeps_explicit_network_namespace() {
        let config = GvisorConfig {
            container_name: "web",
            args: vec!["/bin/sh".to_string()],
            env: vec![],
            cwd: "/".to_string(),
            rootfs: PathBuf::from("/tmp/rootfs.erofs"),
            rootfs_overlay: PathBuf::from("/tmp/rootfs-overlay"),
            network_namespace: Some(PathBuf::from("/run/netns/oad")),
            resolv_conf: None,
            mounts: Vec::new(),
            resources: None,
            annotations: BTreeMap::new(),
        };
        let config_json = write_config_json(&config).await;

        let types = namespace_types(&config_json);
        assert!(
            types.contains(&"network".to_string()),
            "explicit netns should be honored: {types:?}"
        );
    }

    #[tokio::test]
    async fn spec_points_gvisor_at_erofs_rootfs() {
        let config = GvisorConfig {
            container_name: "web",
            args: vec!["/bin/sh".to_string()],
            env: vec![],
            cwd: "/".to_string(),
            rootfs: PathBuf::from("/tmp/rootfs.erofs"),
            rootfs_overlay: PathBuf::from("/tmp/rootfs-overlay"),
            network_namespace: None,
            resolv_conf: None,
            mounts: Vec::new(),
            resources: None,
            annotations: BTreeMap::new(),
        };
        let config_json = write_config_json(&config).await;

        assert_eq!(
            config_json["annotations"][GVISOR_ROOTFS_SOURCE_ANNOTATION],
            "/tmp/rootfs.erofs"
        );
        assert_eq!(
            config_json["annotations"][GVISOR_ROOTFS_TYPE_ANNOTATION],
            "erofs"
        );
        assert_eq!(
            config_json["annotations"][GVISOR_ROOTFS_OVERLAY_ANNOTATION], "dir=/tmp/rootfs-overlay",
            "rootfs overlay should be backed by the prepared host directory"
        );
    }

    #[tokio::test]
    async fn spec_can_bind_custom_resolv_conf() {
        let config = GvisorConfig {
            container_name: "web",
            args: vec!["/bin/sh".to_string()],
            env: vec![],
            cwd: "/".to_string(),
            rootfs: PathBuf::from("/tmp/rootfs.erofs"),
            rootfs_overlay: PathBuf::from("/tmp/rootfs-overlay"),
            network_namespace: None,
            resolv_conf: Some(PathBuf::from("/run/omniagent/sandboxes/s/resolv.conf")),
            mounts: Vec::new(),
            resources: None,
            annotations: BTreeMap::new(),
        };
        let config_json = write_config_json(&config).await;

        let resolv = config_json["mounts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|mount| mount["destination"] == "/etc/resolv.conf")
            .unwrap();
        assert_eq!(resolv["source"], "/run/omniagent/sandboxes/s/resolv.conf");
    }

    #[tokio::test]
    async fn spec_appends_static_bind_mounts() {
        let config = GvisorConfig {
            container_name: "web",
            args: vec!["/bin/sh".to_string()],
            env: vec![],
            cwd: "/".to_string(),
            rootfs: PathBuf::from("/tmp/rootfs.erofs"),
            rootfs_overlay: PathBuf::from("/tmp/rootfs-overlay"),
            network_namespace: None,
            resolv_conf: None,
            mounts: vec![MountSpec {
                source: PathBuf::from("/opt/omniagent"),
                destination: "/opt/omniagent".to_string(),
                read_only: true,
            }],
            resources: None,
            annotations: BTreeMap::new(),
        };
        let config_json = write_config_json(&config).await;

        let mount = config_json["mounts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|mount| mount["destination"] == "/opt/omniagent")
            .expect("static mount present");
        assert_eq!(mount["type"], "bind");
        assert_eq!(mount["source"], "/opt/omniagent");
        let options: Vec<&str> = mount["options"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| o.as_str().unwrap())
            .collect();
        assert!(options.contains(&"rbind"));
        assert!(options.contains(&"ro"));
    }

    #[tokio::test]
    async fn spec_emits_linux_resources_when_set() {
        let config = GvisorConfig {
            container_name: "web",
            args: vec!["/bin/sh".to_string()],
            env: vec![],
            cwd: "/".to_string(),
            rootfs: PathBuf::from("/tmp/rootfs.erofs"),
            rootfs_overlay: PathBuf::from("/tmp/rootfs-overlay"),
            network_namespace: None,
            resolv_conf: None,
            mounts: Vec::new(),
            resources: Some(ResourceSpec {
                cpu: Some(oad_core::CpuSpec {
                    quota: Some(200_000),
                    period: Some(100_000),
                    shares: None,
                }),
                memory: Some(oad_core::MemorySpec {
                    limit: Some(536_870_912),
                }),
            }),
            annotations: BTreeMap::new(),
        };
        let config_json = write_config_json(&config).await;

        assert_eq!(config_json["linux"]["resources"]["cpu"]["quota"], 200_000);
        assert_eq!(config_json["linux"]["resources"]["cpu"]["period"], 100_000);
        // Unset fields are omitted rather than emitted as null.
        assert!(config_json["linux"]["resources"]["cpu"]["shares"].is_null());
        assert_eq!(
            config_json["linux"]["resources"]["memory"]["limit"],
            536_870_912i64
        );
    }

    #[tokio::test]
    async fn spec_omits_linux_resources_when_unset() {
        let config = GvisorConfig {
            container_name: "web",
            args: vec!["/bin/sh".to_string()],
            env: vec![],
            cwd: "/".to_string(),
            rootfs: PathBuf::from("/tmp/rootfs.erofs"),
            rootfs_overlay: PathBuf::from("/tmp/rootfs-overlay"),
            network_namespace: None,
            resolv_conf: None,
            mounts: Vec::new(),
            resources: None,
            annotations: BTreeMap::new(),
        };
        let config_json = write_config_json(&config).await;

        assert!(config_json["linux"]["resources"].is_null());
    }
}
