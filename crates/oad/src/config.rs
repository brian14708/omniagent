use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use oad_core::{
    ControlPlaneConfig, DaemonConfig, HttpConfig, ManagedNetworkBackend, MountSpec,
    NetworkRuntimeConfig, ObservabilityConfig, RuntimeConfig,
};
use serde::Deserialize;

const DEFAULT_BIND: &str = "127.0.0.1:8080";
const DEFAULT_BASE_DIR: &str = "/var/lib/omniagent";
const DEFAULT_PAUSE_IMAGE: &str = "registry.k8s.io/pause:3.10";
const DEFAULT_OMNIAGENT_PATH: &str = "/opt/omniagent/bin/omniagent";

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    #[serde(default)]
    http: FileHttpConfig,
    #[serde(default)]
    runtime: FileRuntimeConfig,
    #[serde(default)]
    control_plane: FileControlPlaneConfig,
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
    observability: FileObservabilityConfig,
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

#[derive(Debug, Default, Deserialize)]
struct FileObservabilityConfig {
    enabled: Option<bool>,
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

    let observability = resolve_observability_config(&file_config);
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
            observability,
            static_mounts: file_config.runtime.static_mounts,
        },
        control_plane,
    })
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

fn resolve_observability_config(config: &FileConfig) -> ObservabilityConfig {
    let defaults = ObservabilityConfig::default();
    ObservabilityConfig {
        enabled: config
            .runtime
            .observability
            .enabled
            .unwrap_or(defaults.enabled),
    }
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
    if let Ok(value) = std::env::var("OAD_OBSERVABILITY_ENABLED") {
        config.runtime.observability.enabled =
            Some(parse_bool_env("OAD_OBSERVABILITY_ENABLED", &value)?);
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
    Ok(())
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
}
