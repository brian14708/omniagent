use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use oad_core::{
    EgressDestination, EgressPolicy, EgressRule, ManagedNetworkBackend, NetworkRuntimeConfig,
    OadPaths, PortRange, Protocol, SandboxId, SandboxNetworkSpec, TrafficShapingPolicy,
    write_atomic_file,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tracing::{debug, warn};

const NETNS_DIR: &str = "/run/netns";
const HOST_VETH_PREFIX: &str = "oadh";
const PEER_VETH_PREFIX: &str = "oadp";
const NFT_TABLE: &str = "oad";
const NFT_FAMILY: &str = "inet";
const ENVOY: &str = "envoy";
const IP: &str = "ip";
const IPTABLES: &str = "iptables";
const IPTABLES_NFT: &str = "iptables-nft";
const NFT: &str = "nft";
const TC: &str = "tc";
/// Token-bucket defaults applied when a shaping policy omits them, shared by the
/// sandbox-side and host-side `tc` qdisc setup.
const DEFAULT_SHAPING_BURST_BYTES: u64 = 262_144;
const DEFAULT_SHAPING_LATENCY_MS: u32 = 50;

macro_rules! cmd_args {
    ($($arg:expr),* $(,)?) => {
        vec![$($arg.to_string()),*]
    };
}

#[derive(Debug, Error)]
pub enum NetworkError {
    #[error("{0}")]
    InvalidConfig(String),
    #[error(
        "network command failed: {program} {args:?}; status={status}; stdout={stdout:?}; stderr={stderr:?}"
    )]
    Command {
        program: String,
        args: Vec<String>,
        status: String,
        stdout: String,
        stderr: String,
    },
    #[error(transparent)]
    AddrParse(#[from] std::net::AddrParseError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Manages the daemon's egress networking: per-sandbox veth/netns setup, the
/// shared Envoy egress proxy, DNS forwarding, and host firewall rules. Cheap to
/// clone (shared state lives behind `Arc`).
#[derive(Clone)]
pub struct NetworkManager {
    config: NetworkRuntimeConfig,
    lock: Arc<Mutex<()>>,
    dns_started: Arc<Mutex<bool>>,
    envoy_child: Arc<Mutex<Option<Child>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SandboxNetworkState {
    sandbox_id: SandboxId,
    token: String,
    netns_name: String,
    host_veth: String,
    sandbox_ip: Ipv4Addr,
    host_ip: Ipv4Addr,
    prefix_len: u8,
    subnet_index: u32,
    spec: SandboxNetworkSpec,
}

#[derive(Debug, Clone)]
pub struct SandboxNetworkInfo {
    pub host_gateway_ip: Ipv4Addr,
    pub sandbox_ip: Ipv4Addr,
}

struct HostFirewallRule {
    chain: &'static str,
    args: Vec<String>,
}

impl HostFirewallRule {
    fn new(chain: &'static str, args: Vec<&str>) -> Self {
        Self {
            chain,
            args: args.into_iter().map(str::to_string).collect(),
        }
    }

    /// Builds an `iptables` invocation `-w 2 <op> <chain> [extra...] <args...>`
    /// for this rule. `op` is the operation flag (`-C`, `-I`, `-D`); `extra`
    /// holds any tokens that sit between the chain and the rule body (e.g. the
    /// insert position `1`).
    fn invocation(&self, op: &str, extra: &[&str]) -> Vec<String> {
        let mut out = vec![
            "-w".to_string(),
            "2".to_string(),
            op.to_string(),
            self.chain.to_string(),
        ];
        out.extend(extra.iter().map(|token| (*token).to_string()));
        out.extend(self.args.iter().cloned());
        out
    }
}

fn host_input_accept_rule(
    state: &SandboxNetworkState,
    source: &str,
    destination: &str,
    protocol: &str,
    port: &str,
    comment: &str,
) -> HostFirewallRule {
    HostFirewallRule::new(
        "INPUT",
        vec![
            "-i",
            state.host_veth.as_str(),
            "-s",
            source,
            "-d",
            destination,
            "-p",
            protocol,
            "--dport",
            port,
            "-m",
            "comment",
            "--comment",
            comment,
            "-j",
            "ACCEPT",
        ],
    )
}

fn host_forward_out_rule(
    state: &SandboxNetworkState,
    source: &str,
    comment: &str,
) -> HostFirewallRule {
    HostFirewallRule::new(
        "FORWARD",
        vec![
            "-i",
            state.host_veth.as_str(),
            "-s",
            source,
            "-m",
            "comment",
            "--comment",
            comment,
            "-j",
            "ACCEPT",
        ],
    )
}

fn host_forward_in_rule(
    state: &SandboxNetworkState,
    sandbox_destination: &str,
    comment: &str,
) -> HostFirewallRule {
    HostFirewallRule::new(
        "FORWARD",
        vec![
            "-o",
            state.host_veth.as_str(),
            "-d",
            sandbox_destination,
            "-m",
            "conntrack",
            "--ctstate",
            "RELATED,ESTABLISHED",
            "-m",
            "comment",
            "--comment",
            comment,
            "-j",
            "ACCEPT",
        ],
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Ipv4Cidr {
    network: u32,
    prefix: u8,
}

impl NetworkManager {
    pub fn new(config: NetworkRuntimeConfig) -> Result<Self, NetworkError> {
        if config.enabled {
            parse_ipv4_cidr(&config.sandbox_cidr)?;
            config.envoy_listener.parse::<SocketAddr>()?;
            config.dns_listener.parse::<SocketAddr>()?;
            config.dns_upstream.parse::<SocketAddr>()?;
        }
        Ok(Self {
            config,
            lock: Arc::new(Mutex::new(())),
            dns_started: Arc::new(Mutex::new(false)),
            envoy_child: Arc::new(Mutex::new(None)),
        })
    }

    pub const fn enabled(&self) -> bool {
        self.config.enabled
    }

    pub const fn backend(&self) -> ManagedNetworkBackend {
        self.config.backend
    }

    pub async fn sandbox_info(
        &self,
        paths: &OadPaths,
        id: &SandboxId,
    ) -> Result<Option<SandboxNetworkInfo>, NetworkError> {
        if !self.config.enabled {
            return Ok(None);
        }
        Ok(read_state(paths, id)
            .await?
            .map(|state| SandboxNetworkInfo {
                host_gateway_ip: state.host_ip,
                sandbox_ip: state.sandbox_ip,
            }))
    }

    pub async fn reconcile_sandbox(
        &self,
        paths: &OadPaths,
        id: &SandboxId,
        spec: &SandboxNetworkSpec,
    ) -> Result<Option<PathBuf>, NetworkError> {
        if !self.config.enabled {
            return Ok(None);
        }
        validate_spec(spec)?;
        let _guard = self.lock.lock().await;
        self.ensure_services(paths).await?;
        let mut state = match read_state(paths, id).await? {
            Some(mut state) => {
                state.spec.clone_from(spec);
                state
            }
            None => self.allocate_state(paths, id, spec).await?,
        };
        state.spec.clone_from(spec);
        write_state(paths, &state).await?;
        write_resolv_conf(paths, &state).await?;
        self.ensure_namespace(&state).await?;
        self.apply_shaping(&state).await?;
        self.apply_nft_ruleset(paths).await?;
        self.ensure_host_firewall_rules(&state).await?;
        Ok(Some(netns_path(&state)))
    }

    pub async fn reconcile_all(&self, paths: &OadPaths) -> Result<(), NetworkError> {
        if !self.config.enabled {
            return Ok(());
        }
        let _guard = self.lock.lock().await;
        self.ensure_services(paths).await?;
        let states = read_states(paths).await?;
        for state in &states {
            if fs::try_exists(netns_path(state)).await.unwrap_or(false)
                && let Err(err) = self.apply_shaping(state).await
            {
                warn!(
                    sandbox_id = %state.sandbox_id,
                    error = %err,
                    "failed to restore traffic shaping during network reconciliation"
                );
            }
            self.ensure_host_firewall_rules(state).await?;
        }
        self.apply_nft_states(&states).await
    }

    pub async fn delete_sandbox(
        &self,
        paths: &OadPaths,
        id: &SandboxId,
    ) -> Result<(), NetworkError> {
        if !self.config.enabled {
            return Ok(());
        }
        let _guard = self.lock.lock().await;
        let Some(state) = read_state(paths, id).await? else {
            self.apply_nft_ruleset(paths).await?;
            return Ok(());
        };
        let _ = self.delete_host_firewall_rules(&state).await;
        let _ = self.delete_qdisc_host(&state).await;
        let _ = self.delete_qdisc_sandbox(&state).await;
        let _ = run(IP, cmd_args!["link", "delete", state.host_veth.as_str()]).await;
        let _ = run(IP, cmd_args!["netns", "delete", state.netns_name.as_str()]).await;
        match fs::remove_file(paths.sandbox_network_state(id)).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
        self.apply_nft_ruleset(paths).await
    }

    async fn allocate_state(
        &self,
        paths: &OadPaths,
        id: &SandboxId,
        spec: &SandboxNetworkSpec,
    ) -> Result<SandboxNetworkState, NetworkError> {
        let cidr = parse_ipv4_cidr(&self.config.sandbox_cidr)?;
        let block_count = cidr.block_count_30()?;
        let existing = read_states(paths).await?;
        let used = existing
            .iter()
            .map(|state| state.subnet_index)
            .collect::<BTreeSet<_>>();
        let used_tokens = existing
            .iter()
            .map(|state| state.token.as_str())
            .collect::<BTreeSet<_>>();
        let start = u32::try_from(stable_hash(id.as_str()) % u64::from(block_count))
            .expect("hash modulo block_count always fits in u32");
        let subnet_index = (0..block_count)
            .map(|offset| (start + offset) % block_count)
            .find(|index| !used.contains(index))
            .ok_or_else(|| {
                NetworkError::InvalidConfig(format!(
                    "sandbox network CIDR {:?} has no free /30 subnets",
                    self.config.sandbox_cidr
                ))
            })?;
        let (host_ip, sandbox_ip) = cidr.nth_30_pair(subnet_index)?;
        // The token feeds the netns/veth names, which must be unique per
        // sandbox. It is only a truncated hash of the id, so probe for a token
        // not already taken by another sandbox to avoid two ids that collide in
        // the low hash bits sharing (and tearing down) each other's netns/veth.
        let token = (0..u32::MAX)
            .map(|attempt| salted_token(id, attempt))
            .find(|candidate| !used_tokens.contains(candidate.as_str()))
            .expect("48-bit token space is never exhausted by live sandboxes");
        Ok(SandboxNetworkState {
            sandbox_id: id.clone(),
            token: token.clone(),
            netns_name: format!("oad-{token}"),
            host_veth: format!("{HOST_VETH_PREFIX}{token}"),
            sandbox_ip,
            host_ip,
            prefix_len: 30,
            subnet_index,
            spec: spec.clone(),
        })
    }

    async fn ensure_namespace(&self, state: &SandboxNetworkState) -> Result<(), NetworkError> {
        self.ensure_netns(state).await?;
        self.ensure_veth(state).await?;
        self.configure_host_interface(state).await?;
        self.configure_sandbox_interface(state).await?;
        if let Err(err) = fs::write("/proc/sys/net/ipv4/ip_forward", b"1\n").await {
            warn!(error = %err, "failed to enable IPv4 forwarding");
        }
        Ok(())
    }

    async fn ensure_netns(&self, state: &SandboxNetworkState) -> Result<(), NetworkError> {
        fs::create_dir_all(NETNS_DIR).await?;
        if !fs::try_exists(netns_path(state)).await? {
            run(IP, cmd_args!["netns", "add", state.netns_name.as_str()]).await?;
        }
        Ok(())
    }

    async fn ensure_veth(&self, state: &SandboxNetworkState) -> Result<(), NetworkError> {
        if !command_succeeds(IP, ["link", "show", state.host_veth.as_str()]).await {
            let peer = peer_veth(&state.token);
            run(
                IP,
                cmd_args![
                    "link",
                    "add",
                    state.host_veth.as_str(),
                    "type",
                    "veth",
                    "peer",
                    "name",
                    peer.as_str(),
                ],
            )
            .await?;
            run(
                IP,
                cmd_args![
                    "link",
                    "set",
                    peer.as_str(),
                    "netns",
                    state.netns_name.as_str(),
                ],
            )
            .await?;
        }
        Ok(())
    }

    async fn configure_host_interface(
        &self,
        state: &SandboxNetworkState,
    ) -> Result<(), NetworkError> {
        run(
            IP,
            cmd_args![
                "addr",
                "replace",
                format!("{}/{}", state.host_ip, state.prefix_len),
                "dev",
                state.host_veth.as_str(),
            ],
        )
        .await?;
        run(IP, cmd_args!["link", "set", state.host_veth.as_str(), "up"]).await?;
        Ok(())
    }

    async fn configure_sandbox_interface(
        &self,
        state: &SandboxNetworkState,
    ) -> Result<(), NetworkError> {
        if !command_succeeds(
            IP,
            ["-n", state.netns_name.as_str(), "link", "show", "eth0"],
        )
        .await
        {
            let peer = peer_veth(&state.token);
            run(
                IP,
                cmd_args![
                    "-n",
                    state.netns_name.as_str(),
                    "link",
                    "set",
                    peer.as_str(),
                    "name",
                    "eth0",
                ],
            )
            .await?;
        }
        run(
            IP,
            cmd_args!["-n", state.netns_name.as_str(), "link", "set", "lo", "up"],
        )
        .await?;
        run(
            IP,
            cmd_args![
                "-n",
                state.netns_name.as_str(),
                "addr",
                "replace",
                format!("{}/{}", state.sandbox_ip, state.prefix_len),
                "dev",
                "eth0",
            ],
        )
        .await?;
        run(
            IP,
            cmd_args!["-n", state.netns_name.as_str(), "link", "set", "eth0", "up"],
        )
        .await?;
        run(
            IP,
            cmd_args![
                "-n",
                state.netns_name.as_str(),
                "route",
                "replace",
                "default",
                "via",
                state.host_ip.to_string(),
                "dev",
                "eth0",
            ],
        )
        .await?;
        Ok(())
    }

    async fn apply_nft_ruleset(&self, paths: &OadPaths) -> Result<(), NetworkError> {
        let states = read_states(paths).await?;
        self.apply_nft_states(&states).await
    }

    async fn apply_nft_states(&self, states: &[SandboxNetworkState]) -> Result<(), NetworkError> {
        let ruleset = self.render_nft_ruleset(states)?;
        // The ruleset replaces the table atomically within a single `nft -f`
        // transaction (see render_nft_ruleset's add/delete preamble), so a
        // rejected rule leaves the previous table intact instead of tearing
        // down every sandbox's firewall.
        run_with_stdin(NFT, ["-f", "-"], ruleset.as_bytes()).await
    }

    async fn ensure_host_firewall_rules(
        &self,
        state: &SandboxNetworkState,
    ) -> Result<(), NetworkError> {
        let Some(program) = iptables_program().await else {
            warn!(
                "iptables-compatible command not found; host firewall may drop sandbox gateway traffic"
            );
            return Ok(());
        };
        let rules = self.host_firewall_rules(state)?;
        for rule in rules {
            if command_succeeds(program, rule.invocation("-C", &[])).await {
                continue;
            }
            run(program, rule.invocation("-I", &["1"])).await?;
        }
        Ok(())
    }

    async fn delete_host_firewall_rules(
        &self,
        state: &SandboxNetworkState,
    ) -> Result<(), NetworkError> {
        let Some(program) = iptables_program().await else {
            return Ok(());
        };
        for rule in self.host_firewall_rules(state)? {
            let _ = run(program, rule.invocation("-D", &[])).await;
        }
        Ok(())
    }

    fn host_firewall_rules(
        &self,
        state: &SandboxNetworkState,
    ) -> Result<Vec<HostFirewallRule>, NetworkError> {
        let dns_port = self
            .config
            .dns_listener
            .parse::<SocketAddr>()?
            .port()
            .to_string();
        let envoy_port = self
            .config
            .envoy_listener
            .parse::<SocketAddr>()?
            .port()
            .to_string();
        let source = format!("{}/32", state.sandbox_ip);
        let destination = format!("{}/32", state.host_ip);
        let sandbox_destination = source.clone();
        let comment_prefix = format!("oad:{}", state.sandbox_id);
        Ok(vec![
            host_input_accept_rule(
                state,
                source.as_str(),
                destination.as_str(),
                "udp",
                dns_port.as_str(),
                &format!("{comment_prefix}:dns-udp"),
            ),
            host_input_accept_rule(
                state,
                source.as_str(),
                destination.as_str(),
                "tcp",
                dns_port.as_str(),
                &format!("{comment_prefix}:dns-tcp"),
            ),
            host_input_accept_rule(
                state,
                source.as_str(),
                destination.as_str(),
                "tcp",
                envoy_port.as_str(),
                &format!("{comment_prefix}:envoy-tcp"),
            ),
            host_forward_out_rule(
                state,
                source.as_str(),
                &format!("{comment_prefix}:forward-out"),
            ),
            host_forward_in_rule(
                state,
                sandbox_destination.as_str(),
                &format!("{comment_prefix}:forward-in"),
            ),
        ])
    }

    fn render_nft_ruleset(&self, states: &[SandboxNetworkState]) -> Result<String, NetworkError> {
        let cidr = parse_ipv4_cidr(&self.config.sandbox_cidr)?;
        let envoy_port = self.config.envoy_listener.parse::<SocketAddr>()?.port();
        let dns_port = self.config.dns_listener.parse::<SocketAddr>()?.port();
        let mut out = String::new();
        // Replace the table atomically: `nft -f` applies the whole script as a
        // single transaction. `add table` (idempotent) ensures the subsequent
        // `delete table` always succeeds, and the fresh `table { ... }` below
        // rebuilds it. If any rule fails to parse the entire transaction rolls
        // back, leaving the previous ruleset intact rather than flushed.
        writeln!(out, "add table {NFT_FAMILY} {NFT_TABLE}").unwrap();
        writeln!(out, "delete table {NFT_FAMILY} {NFT_TABLE}").unwrap();
        writeln!(out, "table {NFT_FAMILY} {NFT_TABLE} {{").unwrap();
        writeln!(
            out,
            "  chain prerouting {{ type nat hook prerouting priority dstnat; policy accept;"
        )
        .unwrap();
        for state in states {
            if state.spec.udp.dns_redirect {
                writeln!(
                    out,
                    "    iifname \"{}\" udp dport 53 redirect to :{}",
                    state.host_veth, dns_port
                )
                .unwrap();
                writeln!(
                    out,
                    "    iifname \"{}\" tcp dport 53 redirect to :{}",
                    state.host_veth, dns_port
                )
                .unwrap();
            }
            if state.spec.l7.transparent_tcp {
                writeln!(
                    out,
                    "    iifname \"{}\" ip daddr {} tcp dport {{ {}, {} }} accept",
                    state.host_veth, state.host_ip, envoy_port, dns_port
                )
                .unwrap();
                Self::write_tcp_redirect_rules(&mut out, state, envoy_port)?;
            }
        }
        writeln!(out, "  }}").unwrap();
        writeln!(
            out,
            "  chain input {{ type filter hook input priority -300; policy accept;"
        )
        .unwrap();
        for state in states {
            writeln!(
                out,
                "    iifname \"{}\" ip saddr {} ip daddr {} udp dport {} accept",
                state.host_veth, state.sandbox_ip, state.host_ip, dns_port
            )
            .unwrap();
            writeln!(
                out,
                "    iifname \"{}\" ip saddr {} ip daddr {} tcp dport {{ {}, {} }} accept",
                state.host_veth, state.sandbox_ip, state.host_ip, envoy_port, dns_port
            )
            .unwrap();
        }
        writeln!(out, "  }}").unwrap();
        writeln!(
            out,
            "  chain forward {{ type filter hook forward priority -300; policy accept;"
        )
        .unwrap();
        for state in states {
            if state.spec.udp.block_quic {
                writeln!(
                    out,
                    "    iifname \"{}\" udp dport 443 reject",
                    state.host_veth
                )
                .unwrap();
            }
            Self::write_forward_rules(&mut out, state)?;
        }
        writeln!(out, "  }}").unwrap();
        writeln!(
            out,
            "  chain postrouting {{ type nat hook postrouting priority srcnat; policy accept;"
        )
        .unwrap();
        writeln!(out, "    ip saddr {cidr} masquerade").unwrap();
        writeln!(out, "  }}").unwrap();
        writeln!(out, "}}").unwrap();
        Ok(out)
    }

    fn write_tcp_redirect_rules(
        out: &mut String,
        state: &SandboxNetworkState,
        envoy_port: u16,
    ) -> Result<(), NetworkError> {
        match &state.spec.egress {
            EgressPolicy::AllowAll => {
                writeln!(
                    out,
                    "    iifname \"{}\" meta l4proto tcp redirect to :{}",
                    state.host_veth, envoy_port
                )
                .unwrap();
            }
            EgressPolicy::DenyAll => {}
            EgressPolicy::Rules { rules } => {
                for rule in rules {
                    if matches!(rule.protocol, Protocol::Tcp | Protocol::All) {
                        write_nft_rule(
                            out,
                            &state.host_veth,
                            "tcp",
                            &rule.destination,
                            &rule.ports,
                            &format!("redirect to :{envoy_port}"),
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn write_forward_rules(
        out: &mut String,
        state: &SandboxNetworkState,
    ) -> Result<(), NetworkError> {
        for rule in &state.spec.udp.allow {
            write_nft_rule(
                out,
                &state.host_veth,
                "udp",
                &rule.destination,
                &rule.ports,
                "accept",
            )?;
        }

        match &state.spec.egress {
            EgressPolicy::AllowAll => {
                writeln!(out, "    iifname \"{}\" accept", state.host_veth).unwrap();
            }
            EgressPolicy::DenyAll => {
                writeln!(out, "    iifname \"{}\" reject", state.host_veth).unwrap();
            }
            EgressPolicy::Rules { rules } => {
                for rule in rules {
                    match rule.protocol {
                        Protocol::Tcp if state.spec.l7.transparent_tcp => {}
                        Protocol::Tcp => write_nft_rule(
                            out,
                            &state.host_veth,
                            "tcp",
                            &rule.destination,
                            &rule.ports,
                            "accept",
                        )?,
                        Protocol::Udp => write_nft_rule(
                            out,
                            &state.host_veth,
                            "udp",
                            &rule.destination,
                            &rule.ports,
                            "accept",
                        )?,
                        Protocol::Icmp => write_nft_rule(
                            out,
                            &state.host_veth,
                            "icmp",
                            &rule.destination,
                            &[],
                            "accept",
                        )?,
                        Protocol::All => {
                            if !state.spec.l7.transparent_tcp {
                                write_nft_rule(
                                    out,
                                    &state.host_veth,
                                    "tcp",
                                    &rule.destination,
                                    &rule.ports,
                                    "accept",
                                )?;
                            }
                            write_nft_rule(
                                out,
                                &state.host_veth,
                                "udp",
                                &rule.destination,
                                &rule.ports,
                                "accept",
                            )?;
                            write_nft_rule(
                                out,
                                &state.host_veth,
                                "icmp",
                                &rule.destination,
                                &[],
                                "accept",
                            )?;
                        }
                    }
                }
                writeln!(out, "    iifname \"{}\" reject", state.host_veth).unwrap();
            }
        }
        Ok(())
    }

    async fn apply_shaping(&self, state: &SandboxNetworkState) -> Result<(), NetworkError> {
        if state.spec.shaping.upload_bps.is_some() {
            self.apply_qdisc_sandbox(state, &state.spec.shaping).await?;
        } else {
            let _ = self.delete_qdisc_sandbox(state).await;
        }
        if state.spec.shaping.download_bps.is_some() {
            self.apply_qdisc_host(state, &state.spec.shaping).await?;
        } else {
            let _ = self.delete_qdisc_host(state).await;
        }
        Ok(())
    }

    async fn apply_qdisc_sandbox(
        &self,
        state: &SandboxNetworkState,
        shaping: &TrafficShapingPolicy,
    ) -> Result<(), NetworkError> {
        let Some(rate) = shaping.upload_bps else {
            return Ok(());
        };
        run(
            IP,
            cmd_args![
                "netns",
                "exec",
                state.netns_name.as_str(),
                TC,
                "qdisc",
                "replace",
                "dev",
                "eth0",
                "root",
                "tbf",
                "rate",
                format!("{rate}bit"),
                "burst",
                format!(
                    "{}b",
                    shaping.burst_bytes.unwrap_or(DEFAULT_SHAPING_BURST_BYTES)
                ),
                "latency",
                format!(
                    "{}ms",
                    shaping.latency_ms.unwrap_or(DEFAULT_SHAPING_LATENCY_MS)
                ),
            ],
        )
        .await
    }

    async fn apply_qdisc_host(
        &self,
        state: &SandboxNetworkState,
        shaping: &TrafficShapingPolicy,
    ) -> Result<(), NetworkError> {
        let Some(rate) = shaping.download_bps else {
            return Ok(());
        };
        run(
            TC,
            cmd_args![
                "qdisc",
                "replace",
                "dev",
                state.host_veth.as_str(),
                "root",
                "tbf",
                "rate",
                format!("{rate}bit"),
                "burst",
                format!(
                    "{}b",
                    shaping.burst_bytes.unwrap_or(DEFAULT_SHAPING_BURST_BYTES)
                ),
                "latency",
                format!(
                    "{}ms",
                    shaping.latency_ms.unwrap_or(DEFAULT_SHAPING_LATENCY_MS)
                ),
            ],
        )
        .await
    }

    async fn delete_qdisc_sandbox(&self, state: &SandboxNetworkState) -> Result<(), NetworkError> {
        run(
            IP,
            cmd_args![
                "netns",
                "exec",
                state.netns_name.as_str(),
                TC,
                "qdisc",
                "del",
                "dev",
                "eth0",
                "root",
            ],
        )
        .await
    }

    async fn delete_qdisc_host(&self, state: &SandboxNetworkState) -> Result<(), NetworkError> {
        run(
            TC,
            cmd_args!["qdisc", "del", "dev", state.host_veth.as_str(), "root",],
        )
        .await
    }

    async fn ensure_services(&self, paths: &OadPaths) -> Result<(), NetworkError> {
        self.ensure_dns_proxy().await?;
        self.ensure_envoy(paths).await
    }

    async fn ensure_dns_proxy(&self) -> Result<(), NetworkError> {
        let mut started = self.dns_started.lock().await;
        if *started {
            return Ok(());
        }
        let listener = self.config.dns_listener.parse::<SocketAddr>()?;
        let upstream = self.config.dns_upstream.parse::<SocketAddr>()?;
        let udp = Arc::new(UdpSocket::bind(listener).await?);
        let tcp = TcpListener::bind(listener).await?;
        tokio::spawn(run_udp_dns_proxy(Arc::clone(&udp), upstream));
        tokio::spawn(run_tcp_dns_proxy(tcp, upstream));
        *started = true;
        drop(started);
        Ok(())
    }

    async fn ensure_envoy(&self, paths: &OadPaths) -> Result<(), NetworkError> {
        let mut child = self.envoy_child.lock().await;
        if let Some(existing) = child.as_mut() {
            match existing.try_wait() {
                Ok(None) => return Ok(()),
                Ok(Some(status)) => {
                    warn!(%status, "managed Envoy exited; restarting");
                }
                Err(err) => {
                    warn!(error = %err, "failed to inspect managed Envoy; restarting");
                }
            }
        }

        fs::create_dir_all(paths.network_dir()).await?;
        let config = self.render_envoy_config()?;
        write_atomic_file(&paths.envoy_config(), config.as_bytes()).await?;
        let stdout = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(paths.envoy_log())?;
        let stderr = stdout.try_clone()?;
        let spawned = Command::new(ENVOY)
            .arg("--config-path")
            .arg(paths.envoy_config())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()?;
        *child = Some(spawned);
        drop(child);
        Ok(())
    }

    fn render_envoy_config(&self) -> Result<String, NetworkError> {
        let listener_addr = self.config.envoy_listener.parse::<SocketAddr>()?;

        let listener_filters = vec![json!({
            "name": "envoy.filters.listener.original_dst",
            "typed_config": {
                "@type": "type.googleapis.com/envoy.extensions.filters.listener.original_dst.v3.OriginalDst"
            }
        })];

        // The catch-all L4 chain (no `filter_chain_match`) transparently proxies
        // every connection to its original destination.
        let tcp_proxy = json!({
            "@type": "type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy",
            "stat_prefix": "oad_tcp",
            "cluster": "original_dst"
        });

        let clusters = vec![json!({
            "name": "original_dst",
            "type": "ORIGINAL_DST",
            "connect_timeout": "5s",
            "lb_policy": "CLUSTER_PROVIDED"
        })];

        let mut listener = json!({
            "name": "oad_transparent_tcp",
            "address": {
                "socket_address": {
                    "address": listener_addr.ip().to_string(),
                    "port_value": listener_addr.port()
                }
            }
        });

        let filter_chains: Vec<Value> = vec![json!({
            "filters": [{
                "name": "envoy.filters.network.tcp_proxy",
                "typed_config": tcp_proxy
            }]
        })];

        listener["listener_filters"] = Value::Array(listener_filters);
        listener["filter_chains"] = Value::Array(filter_chains);

        // Envoy's `--config-path` loader sniffs content (JSON is valid YAML), so
        // we emit JSON — structural by construction, no significant-whitespace
        // templating to get wrong.
        let config = json!({
            "static_resources": {
                "listeners": [listener],
                "clusters": clusters
            }
        });
        Ok(serde_json::to_string_pretty(&config)?)
    }
}

pub fn validate_spec(spec: &SandboxNetworkSpec) -> Result<(), NetworkError> {
    match &spec.egress {
        EgressPolicy::AllowAll | EgressPolicy::DenyAll => {}
        EgressPolicy::Rules { rules } => validate_rules(rules)?,
    }
    validate_rules(&spec.udp.allow)?;
    if let Some(latency) = spec.shaping.latency_ms
        && latency == 0
    {
        return Err(NetworkError::InvalidConfig(
            "network.shaping.latency_ms must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn validate_rules(rules: &[EgressRule]) -> Result<(), NetworkError> {
    for rule in rules {
        let EgressDestination::Cidr { cidr } = &rule.destination;
        parse_ipv4_cidr(cidr)?;
        for port in &rule.ports {
            if port.start > port.end {
                return Err(NetworkError::InvalidConfig(format!(
                    "invalid port range {}-{}",
                    port.start, port.end
                )));
            }
        }
    }
    Ok(())
}

async fn run_udp_dns_proxy(socket: Arc<UdpSocket>, upstream: SocketAddr) {
    let mut buf = vec![0_u8; 4096];
    loop {
        let Ok((len, peer)) = socket.recv_from(&mut buf).await else {
            continue;
        };
        let query = buf[..len].to_vec();
        let socket = Arc::clone(&socket);
        tokio::spawn(async move {
            let bind_addr = if upstream.is_ipv6() {
                "[::]:0"
            } else {
                "0.0.0.0:0"
            };
            let Ok(upstream_socket) = UdpSocket::bind(bind_addr).await else {
                warn!(%peer, %bind_addr, "failed to bind UDP DNS upstream socket");
                return;
            };
            // Connect the socket to the upstream so the kernel only delivers
            // datagrams whose source is the upstream; this drops spoofed replies
            // that race the real response on the ephemeral port.
            if let Err(err) = upstream_socket.connect(upstream).await {
                warn!(%peer, %upstream, error = %err, "failed to connect UDP DNS upstream socket");
                return;
            }
            if let Err(err) = upstream_socket.send(&query).await {
                warn!(%peer, %upstream, error = %err, "failed to forward UDP DNS query upstream");
                return;
            }
            let mut response = vec![0_u8; 4096];
            match tokio::time::timeout(Duration::from_secs(5), upstream_socket.recv(&mut response))
                .await
            {
                Ok(Ok(len)) => {
                    if let Err(err) = socket.send_to(&response[..len], peer).await {
                        warn!(%peer, error = %err, "failed to return UDP DNS response to sandbox");
                    }
                }
                Ok(Err(err)) => {
                    warn!(%peer, %upstream, error = %err, "failed to read UDP DNS upstream response");
                }
                Err(_) => {
                    warn!(%peer, %upstream, "timed out waiting for UDP DNS upstream response");
                }
            }
        });
    }
}

async fn run_tcp_dns_proxy(listener: TcpListener, upstream: SocketAddr) {
    loop {
        let Ok((mut inbound, _)) = listener.accept().await else {
            continue;
        };
        tokio::spawn(async move {
            let Ok(mut outbound) = TcpStream::connect(upstream).await else {
                return;
            };
            let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
        });
    }
}

async fn read_state(
    paths: &OadPaths,
    id: &SandboxId,
) -> Result<Option<SandboxNetworkState>, NetworkError> {
    match fs::read(paths.sandbox_network_state(id)).await {
        Ok(body) => Ok(Some(serde_json::from_slice(&body)?)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

async fn read_states(paths: &OadPaths) -> Result<Vec<SandboxNetworkState>, NetworkError> {
    let mut out = Vec::new();
    let Ok(mut sandboxes) = fs::read_dir(paths.sandboxes_dir()).await else {
        return Ok(out);
    };
    while let Some(entry) = sandboxes.next_entry().await? {
        let state_path = entry.path().join("network.json");
        match fs::read(state_path).await {
            Ok(body) => out.push(serde_json::from_slice(&body)?),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(out)
}

async fn write_state(paths: &OadPaths, state: &SandboxNetworkState) -> Result<(), NetworkError> {
    crate::registry::write_json_atomic(&paths.sandbox_network_state(&state.sandbox_id), state)
        .await?;
    Ok(())
}

async fn write_resolv_conf(
    paths: &OadPaths,
    state: &SandboxNetworkState,
) -> Result<(), NetworkError> {
    let body = format!("nameserver {}\noptions edns0 trust-ad\n", state.host_ip);
    write_atomic_file(
        &paths.sandbox_resolv_conf(&state.sandbox_id),
        body.as_bytes(),
    )
    .await?;
    Ok(())
}

fn write_nft_rule(
    out: &mut String,
    iifname: &str,
    protocol: &str,
    destination: &EgressDestination,
    ports: &[PortRange],
    verdict: &str,
) -> Result<(), NetworkError> {
    let EgressDestination::Cidr { cidr } = destination;
    parse_ipv4_cidr(cidr)?;
    match protocol {
        "icmp" => {
            writeln!(
                out,
                "    iifname \"{iifname}\" ip daddr {cidr} icmp type echo-request {verdict}"
            )
            .unwrap();
        }
        "tcp" | "udp" if ports.is_empty() => {
            writeln!(
                out,
                "    iifname \"{iifname}\" ip daddr {cidr} meta l4proto {protocol} {verdict}"
            )
            .unwrap();
        }
        "tcp" | "udp" => {
            let ports = nft_port_set(ports);
            writeln!(
                out,
                "    iifname \"{iifname}\" ip daddr {cidr} {protocol} dport {ports} {verdict}"
            )
            .unwrap();
        }
        other => {
            return Err(NetworkError::InvalidConfig(format!(
                "unsupported nft protocol {other:?}"
            )));
        }
    }
    Ok(())
}

fn nft_port_set(ports: &[PortRange]) -> String {
    if ports.len() == 1 && ports[0].start == ports[0].end {
        return ports[0].start.to_string();
    }
    let mut out = String::from("{ ");
    for (idx, port) in ports.iter().enumerate() {
        if idx > 0 {
            out.push_str(", ");
        }
        if port.start == port.end {
            write!(out, "{}", port.start).unwrap();
        } else {
            write!(out, "{}-{}", port.start, port.end).unwrap();
        }
    }
    out.push_str(" }");
    out
}

async fn command_succeeds<I, S>(program: &str, args: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    Command::new(program)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .is_ok_and(|status| status.success())
}

async fn iptables_program() -> Option<&'static str> {
    if command_succeeds(IPTABLES_NFT, ["--version"]).await {
        Some(IPTABLES_NFT)
    } else if command_succeeds(IPTABLES, ["--version"]).await {
        Some(IPTABLES)
    } else {
        None
    }
}

async fn run(program: &str, args: Vec<String>) -> Result<(), NetworkError> {
    let output = Command::new(program).args(&args).output().await?;
    if output.status.success() {
        debug!(program, "network command succeeded");
        return Ok(());
    }
    Err(NetworkError::Command {
        program: program.to_string(),
        args,
        status: output.status.to_string(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

async fn run_with_stdin<const N: usize>(
    program: &str,
    args: [&str; N],
    stdin: &[u8],
) -> Result<(), NetworkError> {
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let Some(mut child_stdin) = child.stdin.take() else {
        return Err(NetworkError::InvalidConfig(format!(
            "{program} did not provide stdin"
        )));
    };
    child_stdin.write_all(stdin).await?;
    drop(child_stdin);
    let output = child.wait_with_output().await?;
    if output.status.success() {
        return Ok(());
    }
    Err(NetworkError::Command {
        program: program.to_string(),
        args: args.iter().map(|arg| (*arg).to_string()).collect(),
        status: output.status.to_string(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

fn netns_path(state: &SandboxNetworkState) -> PathBuf {
    Path::new(NETNS_DIR).join(&state.netns_name)
}

fn peer_veth(token: &str) -> String {
    format!("{PEER_VETH_PREFIX}{token}")
}

/// Derives the interface/netns token for a sandbox. `attempt` 0 reproduces the
/// canonical token; higher attempts deterministically rehash with a salt so a
/// hash collision between two distinct sandbox ids can be resolved to a unique
/// token at allocation time.
fn salted_token(id: &SandboxId, attempt: u32) -> String {
    let value = if attempt == 0 {
        stable_hash(id.as_str())
    } else {
        stable_hash(&format!("{}#{attempt}", id.as_str()))
    };
    format!("{:010x}", value & 0x00ff_ffff_ffff)
}

fn stable_hash(value: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

fn parse_ipv4_cidr(value: &str) -> Result<Ipv4Cidr, NetworkError> {
    let (ip, prefix) = value
        .split_once('/')
        .ok_or_else(|| NetworkError::InvalidConfig(format!("invalid IPv4 CIDR {value:?}")))?;
    let ip = ip.parse::<Ipv4Addr>().map_err(|err| {
        NetworkError::InvalidConfig(format!("invalid IPv4 CIDR {value:?}: {err}"))
    })?;
    let prefix = prefix.parse::<u8>().map_err(|err| {
        NetworkError::InvalidConfig(format!("invalid IPv4 CIDR prefix in {value:?}: {err}"))
    })?;
    if prefix > 32 {
        return Err(NetworkError::InvalidConfig(format!(
            "invalid IPv4 CIDR prefix in {value:?}"
        )));
    }
    let mask = if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    };
    Ok(Ipv4Cidr {
        network: u32::from(ip) & mask,
        prefix,
    })
}

impl Ipv4Cidr {
    fn block_count_30(self) -> Result<u32, NetworkError> {
        if self.prefix > 30 {
            return Err(NetworkError::InvalidConfig(
                "sandbox CIDR must be /30 or larger".to_string(),
            ));
        }
        Ok(1_u32 << (30 - self.prefix))
    }

    fn nth_30_pair(self, index: u32) -> Result<(Ipv4Addr, Ipv4Addr), NetworkError> {
        let block_count = self.block_count_30()?;
        if index >= block_count {
            return Err(NetworkError::InvalidConfig(format!(
                "subnet index {index} exceeds CIDR capacity {block_count}"
            )));
        }
        let base = self.network + (index * 4);
        Ok((Ipv4Addr::from(base + 1), Ipv4Addr::from(base + 2)))
    }
}

impl std::fmt::Display for Ipv4Cidr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", Ipv4Addr::from(self.network), self.prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn salted_token_is_stable_and_collision_resolvable() {
        let id = SandboxId::new("sandbox-one").unwrap();
        // attempt 0 is deterministic and the canonical 10-hex token.
        let canonical = salted_token(&id, 0);
        assert_eq!(canonical, salted_token(&id, 0));
        assert_eq!(canonical.len(), 10);
        assert!(canonical.bytes().all(|b| b.is_ascii_hexdigit()));
        // Higher attempts deterministically yield different tokens, so a
        // collision on attempt 0 can be resolved by probing.
        let next = salted_token(&id, 1);
        assert_ne!(canonical, next);
        assert_eq!(next, salted_token(&id, 1));
    }

    #[test]
    fn allocates_hosts_from_30_blocks() {
        let cidr = parse_ipv4_cidr("10.90.0.0/16").unwrap();
        let (host, sandbox) = cidr.nth_30_pair(2).unwrap();
        assert_eq!(host, Ipv4Addr::new(10, 90, 0, 9));
        assert_eq!(sandbox, Ipv4Addr::new(10, 90, 0, 10));
    }

    #[test]
    fn validates_port_ranges() {
        let spec = SandboxNetworkSpec {
            egress: EgressPolicy::Rules {
                rules: vec![EgressRule {
                    destination: EgressDestination::Cidr {
                        cidr: "203.0.113.0/24".to_string(),
                    },
                    protocol: Protocol::Tcp,
                    ports: vec![PortRange {
                        start: 443,
                        end: 80,
                    }],
                }],
            },
            ..SandboxNetworkSpec::default()
        };

        assert!(validate_spec(&spec).is_err());
    }

    #[test]
    fn renders_transparent_tcp_redirect_for_allow_all() {
        let manager = NetworkManager::new(NetworkRuntimeConfig::default()).unwrap();
        let state = test_state();

        let ruleset = manager.render_nft_ruleset(&[state]).unwrap();

        assert!(ruleset.contains("udp dport 53 redirect to :15053"));
        assert!(ruleset.contains("tcp dport 53 redirect to :15053"));
        assert!(ruleset.contains("ip daddr 10.90.0.1 tcp dport { 15001, 15053 } accept"));
        assert!(ruleset.contains("meta l4proto tcp redirect to :15001"));
        assert!(ruleset.contains("udp dport 15053 accept"));
        assert!(ruleset.contains("tcp dport { 15001, 15053 } accept"));
        assert!(ruleset.contains("udp dport 443 reject"));
        assert!(ruleset.contains("ip saddr 10.90.0.0/16 masquerade"));
    }

    #[test]
    fn builds_host_firewall_rules_for_gateway_and_forwarding() {
        let manager = NetworkManager::new(NetworkRuntimeConfig::default()).unwrap();
        let rules = manager.host_firewall_rules(&test_state()).unwrap();

        assert_eq!(rules.len(), 5);
        assert!(rules.iter().any(|rule| {
            rule.chain == "INPUT"
                && rule.args.windows(2).any(|pair| pair == ["-p", "udp"])
                && rule
                    .args
                    .windows(2)
                    .any(|pair| pair == ["--dport", "15053"])
                && rule.args.iter().any(|arg| arg == "oad:s1:dns-udp")
        }));
        assert!(rules.iter().any(|rule| {
            rule.chain == "INPUT"
                && rule.args.windows(2).any(|pair| pair == ["-p", "tcp"])
                && rule
                    .args
                    .windows(2)
                    .any(|pair| pair == ["--dport", "15001"])
                && rule.args.iter().any(|arg| arg == "oad:s1:envoy-tcp")
        }));
        assert!(rules.iter().any(|rule| {
            rule.chain == "FORWARD" && rule.args.iter().any(|arg| arg == "oad:s1:forward-out")
        }));
        assert!(rules.iter().any(|rule| {
            rule.chain == "FORWARD"
                && rule
                    .args
                    .windows(2)
                    .any(|pair| pair == ["--ctstate", "RELATED,ESTABLISHED"])
                && rule.args.iter().any(|arg| arg == "oad:s1:forward-in")
        }));
    }

    #[test]
    fn renders_envoy_passthrough_config() {
        let manager = NetworkManager::new(NetworkRuntimeConfig::default()).unwrap();

        let rendered = manager.render_envoy_config().unwrap();
        let config: Value = serde_json::from_str(&rendered).expect("rendered config is valid JSON");

        let listener = &config["static_resources"]["listeners"][0];
        // No inspectors and no listener-filter timeout override: the proxy is a
        // pure transparent passthrough.
        assert!(
            listener
                .get("continue_on_listener_filters_timeout")
                .is_none()
        );
        let listener_filters = listener["listener_filters"].as_array().unwrap();
        assert_eq!(listener_filters.len(), 1);
        assert!(
            listener_filters[0]["typed_config"]["@type"]
                .as_str()
                .unwrap()
                .contains("OriginalDst")
        );

        // A single catch-all TCP-proxy chain, no HCM, no access log.
        let chains = listener["filter_chains"].as_array().unwrap();
        assert_eq!(chains.len(), 1);
        let tcp = &chains[0]["filters"][0]["typed_config"];
        assert!(tcp["@type"].as_str().unwrap().contains("TcpProxy"));
        assert!(tcp.get("access_log").is_none());

        // Only the original_dst cluster; no access-log cluster.
        let clusters = config["static_resources"]["clusters"].as_array().unwrap();
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0]["name"], json!("original_dst"));
    }

    fn test_state() -> SandboxNetworkState {
        SandboxNetworkState {
            sandbox_id: SandboxId::new("s1").unwrap(),
            token: "1234567890".to_string(),
            netns_name: "oad-1234567890".to_string(),
            host_veth: "oadh1234567890".to_string(),
            sandbox_ip: Ipv4Addr::new(10, 90, 0, 2),
            host_ip: Ipv4Addr::new(10, 90, 0, 1),
            prefix_len: 30,
            subnet_index: 0,
            spec: SandboxNetworkSpec::default(),
        }
    }
}
