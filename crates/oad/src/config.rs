use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use oad_core::{
    DaemonConfig, HttpConfig, ManagedNetworkBackend, NetworkRuntimeConfig, ObservabilityConfig,
    RuntimeConfig,
};
use serde::Deserialize;

const DEFAULT_BIND: &str = "127.0.0.1:8080";
const DEFAULT_BASE_DIR: &str = "/run/omniagent";
const DEFAULT_PAUSE_IMAGE: &str = "registry.k8s.io/pause:3.10";

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    #[serde(default)]
    http: FileHttpConfig,
    #[serde(default)]
    runtime: FileRuntimeConfig,
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

    Ok(DaemonConfig {
        http: HttpConfig {
            bind: file_config
                .http
                .bind
                .unwrap_or_else(|| DEFAULT_BIND.to_string()),
            bearer_token,
        },
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
        },
    })
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

/// Default base directory when none is configured. The daemon runs privileged
/// and writes runtime state under `/run`.
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
    Ok(())
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
    fn default_base_dir_is_run_omniagent() {
        assert_eq!(default_base_dir(), PathBuf::from(DEFAULT_BASE_DIR));
    }
}
