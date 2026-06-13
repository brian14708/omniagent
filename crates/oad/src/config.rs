use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use oad_core::{
    CasConfig, ControlPlaneConfig, DaemonConfig, HttpConfig, ManagedNetworkBackend, MountSpec,
    NetworkRuntimeConfig, RuntimeConfig,
};
use serde::Deserialize;

const DEFAULT_BIND: &str = "127.0.0.1:8080";
const DEFAULT_BASE_DIR: &str = "/var/lib/omniagent";
const DEFAULT_PAUSE_IMAGE: &str = "registry.k8s.io/pause:3.10";
const DEFAULT_OMNIAGENT_PATH: &str = "/opt/omniagent/bin/omniagent";

const DEFAULT_S3_REGION: &str = "us-east-1";
const DEFAULT_CHUNK_MIN: u32 = 256 * 1024;
const DEFAULT_CHUNK_AVG: u32 = 1024 * 1024;
const DEFAULT_CHUNK_MAX: u32 = 4 * 1024 * 1024;
const DEFAULT_ZSTD_LEVEL: i32 = 3;

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    #[serde(default)]
    http: FileHttpConfig,
    #[serde(default)]
    runtime: FileRuntimeConfig,
    #[serde(default)]
    control_plane: FileControlPlaneConfig,
    #[serde(default)]
    cas: FileCasConfig,
}

#[derive(Debug, Default, Deserialize)]
struct FileCasConfig {
    endpoint: Option<String>,
    region: Option<String>,
    bucket: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    prefix: Option<String>,
    chunk_min: Option<u32>,
    chunk_avg: Option<u32>,
    chunk_max: Option<u32>,
    zstd_level: Option<i32>,
    cache_max_bytes: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct FileControlPlaneConfig {
    url: Option<String>,
    register_token: Option<String>,
    advertise_url: Option<String>,
    name: Option<String>,
    omniagent_path: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct FileHttpConfig {
    bind: Option<String>,
    bearer_token: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct FileRuntimeConfig {
    base_dir: Option<PathBuf>,
    pause_image: Option<String>,
    network_namespace: Option<PathBuf>,
    #[serde(default)]
    network: FileNetworkRuntimeConfig,
    #[serde(default)]
    static_mounts: Vec<MountSpec>,
}

#[derive(Debug, Default, Deserialize)]
struct FileNetworkRuntimeConfig {
    enabled: Option<bool>,
    backend: Option<ManagedNetworkBackend>,
    sandbox_cidr: Option<String>,
    envoy_listener: Option<String>,
    dns_listener: Option<String>,
    dns_upstream: Option<String>,
}

pub async fn load_config(path: Option<PathBuf>) -> Result<DaemonConfig> {
    let mut file_config = match path {
        Some(path) => {
            let body = tokio::fs::read_to_string(&path)
                .await
                .with_context(|| format!("failed to read {}", path.display()))?;
            toml::from_str::<FileConfig>(&body)
                .with_context(|| format!("failed to parse {}", path.display()))?
        }
        None => match tokio::fs::read_to_string("oad.toml").await {
            Ok(body) => toml::from_str::<FileConfig>(&body).context("failed to parse oad.toml")?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => FileConfig::default(),
            Err(err) => return Err(err).context("failed to read oad.toml"),
        },
    };

    apply_env_overrides(&mut file_config)?;

    let network = resolve_network_config(&file_config)?;

    let bearer_token = file_config.http.bearer_token.unwrap_or_default();
    if bearer_token.is_empty() {
        bail!("missing bearer token; set http.bearer_token or OAD_BEARER_TOKEN");
    }

    let bind = file_config
        .http
        .bind
        .unwrap_or_else(|| DEFAULT_BIND.to_string());
    let control_plane = resolve_control_plane_config(&file_config.control_plane, &bind)?;
    let cas = resolve_cas_config(&file_config.cas)?;

    Ok(DaemonConfig {
        http: HttpConfig { bind, bearer_token },
        runtime: RuntimeConfig {
            base_dir: file_config
                .runtime
                .base_dir
                .unwrap_or_else(default_base_dir),
            pause_image: file_config
                .runtime
                .pause_image
                .unwrap_or_else(|| DEFAULT_PAUSE_IMAGE.to_string()),
            network_namespace: file_config.runtime.network_namespace,
            network,
            static_mounts: file_config.runtime.static_mounts,
        },
        control_plane,
        cas,
    })
}

/// Builds the CAS config, or `None` when no S3 endpoint is configured (legacy
/// single-node mode). Endpoint, bucket, and credentials must be provided
/// together; a partial configuration is an error.
fn resolve_cas_config(file: &FileCasConfig) -> Result<Option<CasConfig>> {
    let endpoint = file.endpoint.clone().filter(|v| !v.is_empty());
    let bucket = file.bucket.clone().filter(|v| !v.is_empty());
    let access_key_id = file.access_key_id.clone().filter(|v| !v.is_empty());
    let secret_access_key = file.secret_access_key.clone().filter(|v| !v.is_empty());

    match (endpoint, bucket, access_key_id, secret_access_key) {
        (None, None, None, None) => Ok(None),
        (Some(endpoint), Some(bucket), Some(access_key_id), Some(secret_access_key)) => {
            Ok(Some(CasConfig {
                endpoint,
                region: file
                    .region
                    .clone()
                    .filter(|v| !v.is_empty())
                    .unwrap_or_else(|| DEFAULT_S3_REGION.to_string()),
                bucket,
                access_key_id,
                secret_access_key,
                prefix: file.prefix.clone().unwrap_or_default(),
                chunk_min: file.chunk_min.unwrap_or(DEFAULT_CHUNK_MIN),
                chunk_avg: file.chunk_avg.unwrap_or(DEFAULT_CHUNK_AVG),
                chunk_max: file.chunk_max.unwrap_or(DEFAULT_CHUNK_MAX),
                zstd_level: file.zstd_level.unwrap_or(DEFAULT_ZSTD_LEVEL),
                cache_max_bytes: file.cache_max_bytes.unwrap_or(0),
            }))
        }
        _ => bail!(
            "incomplete CAS config: set OAD_S3_ENDPOINT_URL, OAD_S3_BUCKET, \
             OAD_S3_ACCESS_KEY_ID, and OAD_S3_SECRET_ACCESS_KEY together, or none"
        ),
    }
}

/// Builds the control-plane registration config, or `None` when no control-plane
/// URL is configured. A URL without a register token is an error.
fn resolve_control_plane_config(
    file: &FileControlPlaneConfig,
    bind: &str,
) -> Result<Option<ControlPlaneConfig>> {
    let Some(url) = file.url.clone().filter(|u| !u.is_empty()) else {
        return Ok(None);
    };
    let register_token = file
        .register_token
        .clone()
        .filter(|t| !t.is_empty())
        .context("control_plane.url is set but control_plane.register_token is missing")?;
    let advertise_url = file
        .advertise_url
        .clone()
        .filter(|u| !u.is_empty())
        .unwrap_or_else(|| format!("http://{bind}"));
    Ok(Some(ControlPlaneConfig {
        url,
        register_token,
        advertise_url,
        name: file.name.clone(),
        omniagent_path: file
            .omniagent_path
            .clone()
            .unwrap_or_else(|| DEFAULT_OMNIAGENT_PATH.to_string()),
    }))
}

fn resolve_network_config(config: &FileConfig) -> Result<NetworkRuntimeConfig> {
    let defaults = NetworkRuntimeConfig::default();
    let legacy_netns = config.runtime.network_namespace.is_some();
    let enabled = config.runtime.network.enabled.unwrap_or(!legacy_netns);
    if enabled && legacy_netns {
        bail!("runtime.network.enabled cannot be true when runtime.network_namespace is set");
    }
    Ok(NetworkRuntimeConfig {
        enabled,
        backend: config.runtime.network.backend.unwrap_or(defaults.backend),
        sandbox_cidr: config
            .runtime
            .network
            .sandbox_cidr
            .clone()
            .unwrap_or(defaults.sandbox_cidr),
        envoy_listener: config
            .runtime
            .network
            .envoy_listener
            .clone()
            .unwrap_or(defaults.envoy_listener),
        dns_listener: config
            .runtime
            .network
            .dns_listener
            .clone()
            .unwrap_or(defaults.dns_listener),
        dns_upstream: config
            .runtime
            .network
            .dns_upstream
            .clone()
            .or_else(host_dns_upstream)
            .unwrap_or(defaults.dns_upstream),
    })
}

fn host_dns_upstream() -> Option<String> {
    let body = std::fs::read_to_string("/etc/resolv.conf").ok()?;
    body.lines().find_map(|line| {
        let line = line.trim();
        let rest = line.strip_prefix("nameserver")?.trim();
        let nameserver = rest.split_whitespace().next()?;
        if nameserver.contains(':') {
            Some(format!("[{nameserver}]:53"))
        } else {
            Some(format!("{nameserver}:53"))
        }
    })
}

/// Default base directory when none is configured. The daemon writes
/// persistent state (sandbox records, the OCI layer/rootfs cache, snapshots)
/// here, so it lives under `/var/lib` to survive reboots — `/run` is tmpfs and
/// would discard the cache on every boot.
fn default_base_dir() -> PathBuf {
    PathBuf::from(DEFAULT_BASE_DIR)
}

pub fn config_path_from_args() -> Result<Option<PathBuf>> {
    let mut args = std::env::args().skip(1);
    let mut config_path = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let Some(path) = args.next() else {
                    bail!("--config requires a path");
                };
                config_path = Some(PathBuf::from(path));
            }
            "--help" | "-h" => {
                println!("usage: oad [--config PATH]");
                std::process::exit(0);
            }
            other => bail!("unknown argument {other:?}"),
        }
    }
    Ok(config_path)
}

fn apply_env_overrides(config: &mut FileConfig) -> Result<()> {
    if let Ok(value) = std::env::var("OAD_HTTP_BIND") {
        config.http.bind = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_BEARER_TOKEN") {
        config.http.bearer_token = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_BASE_DIR") {
        config.runtime.base_dir = Some(PathBuf::from(value));
    }
    if let Ok(value) = std::env::var("OAD_PAUSE_IMAGE") {
        config.runtime.pause_image = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_NETWORK_NAMESPACE") {
        config.runtime.network_namespace = Some(PathBuf::from(value));
    }
    if let Ok(value) = std::env::var("OAD_NETWORK_ENABLED") {
        config.runtime.network.enabled = Some(parse_bool_env("OAD_NETWORK_ENABLED", &value)?);
    }
    if let Ok(value) = std::env::var("OAD_NETWORK_BACKEND") {
        config.runtime.network.backend = Some(match value.to_ascii_lowercase().as_str() {
            "hostinet" => ManagedNetworkBackend::Hostinet,
            "netstack" => ManagedNetworkBackend::Netstack,
            _ => bail!("OAD_NETWORK_BACKEND must be hostinet or netstack, got {value:?}"),
        });
    }
    if let Ok(value) = std::env::var("OAD_NETWORK_SANDBOX_CIDR") {
        config.runtime.network.sandbox_cidr = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_ENVOY_LISTENER") {
        config.runtime.network.envoy_listener = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_DNS_LISTENER") {
        config.runtime.network.dns_listener = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_DNS_UPSTREAM") {
        config.runtime.network.dns_upstream = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_STATIC_MOUNT") {
        config
            .runtime
            .static_mounts
            .push(parse_static_mount(&value)?);
    }
    if let Ok(value) = std::env::var("OAD_CONTROL_PLANE_URL") {
        config.control_plane.url = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_CONTROL_PLANE_TOKEN") {
        config.control_plane.register_token = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_ADVERTISE_URL") {
        config.control_plane.advertise_url = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_INSTANCE_NAME") {
        config.control_plane.name = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_OMNIAGENT_PATH") {
        config.control_plane.omniagent_path = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_S3_ENDPOINT_URL") {
        config.cas.endpoint = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_S3_REGION") {
        config.cas.region = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_S3_BUCKET") {
        config.cas.bucket = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_S3_ACCESS_KEY_ID") {
        config.cas.access_key_id = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_S3_SECRET_ACCESS_KEY") {
        config.cas.secret_access_key = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_S3_PREFIX") {
        config.cas.prefix = Some(value);
    }
    if let Ok(value) = std::env::var("OAD_CAS_CHUNK_MIN") {
        config.cas.chunk_min = Some(parse_u32_env("OAD_CAS_CHUNK_MIN", &value)?);
    }
    if let Ok(value) = std::env::var("OAD_CAS_CHUNK_AVG") {
        config.cas.chunk_avg = Some(parse_u32_env("OAD_CAS_CHUNK_AVG", &value)?);
    }
    if let Ok(value) = std::env::var("OAD_CAS_CHUNK_MAX") {
        config.cas.chunk_max = Some(parse_u32_env("OAD_CAS_CHUNK_MAX", &value)?);
    }
    if let Ok(value) = std::env::var("OAD_CAS_ZSTD_LEVEL") {
        config.cas.zstd_level = Some(parse_i32_env("OAD_CAS_ZSTD_LEVEL", &value)?);
    }
    if let Ok(value) = std::env::var("OAD_CACHE_MAX_BYTES") {
        config.cas.cache_max_bytes = Some(parse_u64_env("OAD_CACHE_MAX_BYTES", &value)?);
    }
    Ok(())
}

fn parse_u64_env(name: &str, value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .with_context(|| format!("{name} must be a non-negative integer, got {value:?}"))
}

fn parse_u32_env(name: &str, value: &str) -> Result<u32> {
    value
        .parse::<u32>()
        .with_context(|| format!("{name} must be a non-negative integer, got {value:?}"))
}

fn parse_i32_env(name: &str, value: &str) -> Result<i32> {
    value
        .parse::<i32>()
        .with_context(|| format!("{name} must be an integer, got {value:?}"))
}

/// Parses an `OAD_STATIC_MOUNT` value `SRC:DST[:ro|:rw]` (read-only default).
fn parse_static_mount(value: &str) -> Result<MountSpec> {
    let parts: Vec<&str> = value.splitn(3, ':').collect();
    let (source, destination, read_only) = match parts.as_slice() {
        [src, dst] => (*src, *dst, true),
        [src, dst, mode] => {
            let read_only = match mode.to_ascii_lowercase().as_str() {
                "ro" => true,
                "rw" => false,
                _ => bail!("OAD_STATIC_MOUNT mode must be ro or rw, got {mode:?}"),
            };
            (*src, *dst, read_only)
        }
        _ => bail!("OAD_STATIC_MOUNT must be SRC:DST[:ro|:rw], got {value:?}"),
    };
    if source.is_empty() || destination.is_empty() {
        bail!("OAD_STATIC_MOUNT source and destination must be non-empty");
    }
    Ok(MountSpec {
        source: PathBuf::from(source),
        destination: destination.to_string(),
        read_only,
    })
}

fn parse_bool_env(name: &str, value: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => bail!("{name} must be a boolean, got {value:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_base_dir_is_var_lib_omniagent() {
        assert_eq!(default_base_dir(), PathBuf::from("/var/lib/omniagent"));
    }

    #[test]
    fn cas_config_absent_when_unset() {
        assert!(
            resolve_cas_config(&FileCasConfig::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn cas_config_resolves_with_defaults() {
        let file = FileCasConfig {
            endpoint: Some("http://rustfs:9000".to_string()),
            bucket: Some("omniagent-cas".to_string()),
            access_key_id: Some("key".to_string()),
            secret_access_key: Some("secret".to_string()),
            ..Default::default()
        };
        let cas = resolve_cas_config(&file).unwrap().unwrap();
        assert_eq!(cas.region, "us-east-1");
        assert_eq!(cas.chunk_min, 256 * 1024);
        assert_eq!(cas.chunk_avg, 1024 * 1024);
        assert_eq!(cas.chunk_max, 4 * 1024 * 1024);
        assert_eq!(cas.zstd_level, 3);
        assert!(cas.prefix.is_empty());
    }

    #[test]
    fn cas_config_partial_is_error() {
        let file = FileCasConfig {
            endpoint: Some("http://rustfs:9000".to_string()),
            ..Default::default()
        };
        assert!(resolve_cas_config(&file).is_err());
    }
}
