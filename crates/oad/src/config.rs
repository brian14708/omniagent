use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use oad_core::{DaemonConfig, HttpConfig, RuntimeConfig};
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

    apply_env_overrides(&mut file_config);

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
        },
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

fn apply_env_overrides(config: &mut FileConfig) {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_base_dir_is_run_omniagent() {
        assert_eq!(default_base_dir(), PathBuf::from(DEFAULT_BASE_DIR));
    }
}
