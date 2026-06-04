//! Egress observability: re-emits Envoy access logs as OpenTelemetry spans.
//!
//! Envoy proxies every sandbox's outbound TCP/HTTP connection and streams a
//! gRPC access-log entry (one per completed connection) to this daemon over a
//! Unix socket. We decode each entry, resolve the sandbox identity from the
//! source IP, and emit a single OTLP span describing the connection. Querying
//! and retention live in the OTLP backend (Tempo/Jaeger/etc.); the daemon is
//! emit-only and keeps no local store.

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use oad_core::{OadPaths, ObservabilityConfig};
use opentelemetry::trace::{Span, SpanKind, Status, TraceContextExt, Tracer, TracerProvider};
use opentelemetry::{InstrumentationScope, KeyValue};
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::runtime;
use opentelemetry_sdk::trace::{SdkTracerProvider, span_processor_with_async_runtime};
use opentelemetry_semantic_conventions::attribute;
use thiserror::Error;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status as TonicStatus};
use tracing::{info, warn};

use crate::network::SandboxIpMap;

/// Instrumentation scope name reported on every emitted span.
const SCOPE_NAME: &str = "oad.egress";
const SERVICE_NAME: &str = "oad";

mod envoy {
    pub mod service {
        pub mod accesslog {
            #[allow(clippy::all, clippy::nursery, clippy::pedantic, warnings)]
            pub mod v3 {
                tonic::include_proto!("envoy.service.accesslog.v3");
            }
        }
    }
}

use envoy::service::accesslog::v3::{
    AccessLogCommon, Address, HttpAccessLogEntry, HttpRequestProperties, HttpResponseProperties,
    ResponseFlags, SocketAddress, StreamAccessLogsMessage, StreamAccessLogsResponse,
    TcpAccessLogEntry,
    access_log_service_server::{AccessLogService, AccessLogServiceServer},
    address, socket_address, stream_access_logs_message,
};

#[derive(Debug, Error)]
pub enum ObservabilityError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    #[error("failed to build OTLP span exporter: {0}")]
    Exporter(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Abstracts the span sink so production code emits to an OTLP tracer while
/// tests can capture spans with an in-memory exporter. Each call corresponds to
/// one finished egress connection represented as a sandbox root span and a child
/// egress span.
trait SpanEmitter: Send + Sync {
    fn emit(&self, trace: EgressTrace);
}

/// Handle to the egress telemetry pipeline. Cheap to clone; `disabled()`
/// produces a no-op handle so the daemon runs without an OTLP collector.
#[derive(Clone)]
pub struct EgressTelemetry {
    inner: Option<Arc<TelemetryInner>>,
}

struct TelemetryInner {
    emitter: Arc<dyn SpanEmitter>,
    provider: Option<SdkTracerProvider>,
}

/// Production span sink: an OTLP-backed SDK tracer.
struct OtlpEmitter {
    tracer: opentelemetry_sdk::trace::SdkTracer,
}

impl SpanEmitter for OtlpEmitter {
    fn emit(&self, trace: EgressTrace) {
        let sandbox_builder = self
            .tracer
            .span_builder(trace.sandbox.name)
            .with_kind(SpanKind::Internal)
            .with_start_time(trace.sandbox.start_time)
            .with_attributes(trace.sandbox.attributes);
        let sandbox = self
            .tracer
            .build_with_context(sandbox_builder, &opentelemetry::Context::new());
        let sandbox_cx = opentelemetry::Context::new().with_span(sandbox);
        sandbox_cx.span().set_status(trace.sandbox.status);

        let builder = self
            .tracer
            .span_builder(trace.egress.name)
            .with_kind(SpanKind::Client)
            .with_start_time(trace.egress.start_time)
            .with_attributes(trace.egress.attributes);
        // Egress spans are deliberate children of the synthetic sandbox span, so
        // bind to that explicit context rather than the ambient `Context::current`.
        let mut started = self.tracer.build_with_context(builder, &sandbox_cx);
        started.set_status(trace.egress.status);
        started.end_with_timestamp(trace.egress.end_time);
        sandbox_cx.span().end_with_timestamp(trace.sandbox.end_time);
    }
}

impl EgressTelemetry {
    /// Builds the OTLP exporter and tracer provider from config. Returns a
    /// disabled handle when observability is turned off. Must be called from
    /// within the Tokio runtime (the async-runtime batch processor and the gRPC
    /// exporter run on it).
    pub fn new(config: &ObservabilityConfig) -> Result<Self, ObservabilityError> {
        if !config.enabled {
            return Ok(Self::disabled());
        }

        let exporter = build_span_exporter()?;
        // The async-runtime batch processor drives exports on the Tokio runtime,
        // so both the gRPC (tonic/hyper) and HTTP (async reqwest) transports have
        // a live reactor. The default thread-based processor would `block_on`
        // these futures off-runtime and deadlock/panic.
        let processor = span_processor_with_async_runtime::BatchSpanProcessor::builder(
            exporter,
            runtime::Tokio,
        )
        .build();
        let provider = SdkTracerProvider::builder()
            .with_span_processor(processor)
            .with_resource(build_resource())
            .build();
        let scope = InstrumentationScope::builder(SCOPE_NAME).build();
        let tracer = provider.tracer_with_scope(scope);

        Ok(Self {
            inner: Some(Arc::new(TelemetryInner {
                emitter: Arc::new(OtlpEmitter { tracer }),
                provider: Some(provider),
            })),
        })
    }

    #[must_use]
    pub const fn disabled() -> Self {
        Self { inner: None }
    }

    #[cfg(test)]
    fn with_emitter(emitter: Arc<dyn SpanEmitter>) -> Self {
        Self {
            inner: Some(Arc::new(TelemetryInner {
                emitter,
                provider: None,
            })),
        }
    }

    /// Starts the Envoy access-log gRPC server on the daemon's Unix socket. A
    /// no-op when telemetry is disabled. `ip_map` resolves a connection's source
    /// IP to its sandbox id without touching disk.
    pub fn spawn_envoy_access_log_server(&self, paths: OadPaths, ip_map: SandboxIpMap) {
        if self.inner.is_none() {
            return;
        }
        let telemetry = self.clone();
        tokio::spawn(async move {
            if let Err(err) = serve_envoy_access_log_server(telemetry.clone(), paths, ip_map).await
            {
                warn!(%err, "Envoy access log gRPC server stopped");
            }
        });
    }

    /// Flushes batched spans and shuts the exporter down. Best-effort: called on
    /// daemon shutdown so the final connection spans are not lost. The provider's
    /// shutdown blocks on the async batch processor's flush, so run it on the
    /// blocking pool rather than parking a core runtime worker (which could stall
    /// on a single-worker runtime).
    pub async fn shutdown(&self) {
        let Some(provider) = self.inner.as_ref().and_then(|inner| inner.provider.clone()) else {
            return;
        };
        match tokio::task::spawn_blocking(move || provider.shutdown()).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(%err, "failed to flush egress telemetry on shutdown"),
            Err(err) => warn!(%err, "egress telemetry shutdown task panicked"),
        }
    }

    fn emit(&self, trace: EgressTrace) {
        if let Some(inner) = &self.inner {
            inner.emitter.emit(trace);
        }
    }
}

/// Builds the OTLP span exporter. The transport (gRPC vs HTTP) is a compile-time
/// typestate in the SDK, so we select it from the canonical
/// `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` / `OTEL_EXPORTER_OTLP_PROTOCOL` env vars
/// ourselves (defaulting to gRPC). Everything else — endpoint, headers, timeout,
/// compression — the builder resolves from the standard `OTEL_EXPORTER_OTLP_*`
/// environment on `build()`, so we deliberately do not set it here.
fn build_span_exporter() -> Result<SpanExporter, ObservabilityError> {
    let exporter = if otlp_uses_http_from_env() {
        SpanExporter::builder().with_http().build()
    } else {
        SpanExporter::builder().with_tonic().build()
    };
    exporter.map_err(|err| ObservabilityError::Exporter(err.to_string()))
}

/// Returns whether the canonical OTLP protocol environment selects an HTTP
/// transport, mirroring the SDK's precedence (signal-specific var wins over the
/// generic one). Unset or unrecognized values keep the daemon's gRPC default.
fn otlp_uses_http_from_env() -> bool {
    for var in [
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
    ] {
        if let Ok(value) = std::env::var(var) {
            match value.trim() {
                "http/protobuf" | "http/json" => return true,
                "grpc" => return false,
                _ => {}
            }
        }
    }
    false
}

/// Builds the resource describing this service. The standard env detectors
/// (`OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`) take precedence; we only
/// supply a default `service.name` when neither set one. We must inspect the
/// environment directly: `Resource::builder().build()` *always* populates
/// `service.name` (the SDK's provided-resource detector falls back to
/// `unknown_service:<exe>`), so the built resource cannot tell us whether the
/// value actually came from the environment.
fn build_resource() -> Resource {
    if env_sets_service_name() {
        Resource::builder().build()
    } else {
        Resource::builder().with_service_name(SERVICE_NAME).build()
    }
}

/// Whether the environment provides `service.name`, via `OTEL_SERVICE_NAME` or a
/// `service.name=` entry in `OTEL_RESOURCE_ATTRIBUTES`, mirroring the SDK's
/// detector precedence. An explicitly-empty `OTEL_SERVICE_NAME` counts as unset.
fn env_sets_service_name() -> bool {
    if std::env::var("OTEL_SERVICE_NAME").is_ok_and(|value| !value.is_empty()) {
        return true;
    }
    std::env::var("OTEL_RESOURCE_ATTRIBUTES")
        .ok()
        .is_some_and(|attrs| {
            attrs.split_terminator(',').any(|entry| {
                entry
                    .split_once('=')
                    .is_some_and(|(key, _)| key.trim() == attribute::SERVICE_NAME)
            })
        })
}

#[derive(Clone)]
struct EnvoyAccessLogService {
    telemetry: EgressTelemetry,
    ip_map: SandboxIpMap,
}

#[tonic::async_trait]
impl AccessLogService for EnvoyAccessLogService {
    async fn stream_access_logs(
        &self,
        request: Request<tonic::Streaming<StreamAccessLogsMessage>>,
    ) -> Result<Response<StreamAccessLogsResponse>, TonicStatus> {
        let mut stream = request.into_inner();
        while let Some(message) = stream.message().await? {
            match message.log_entries {
                Some(stream_access_logs_message::LogEntries::TcpLogs(entries)) => {
                    for entry in entries.log_entry {
                        if let Err(err) =
                            ingest_envoy_tcp_access_log_entry(&self.telemetry, &self.ip_map, &entry)
                                .await
                        {
                            warn!(%err, "failed to ingest Envoy TCP access log entry");
                        }
                    }
                }
                Some(stream_access_logs_message::LogEntries::HttpLogs(entries)) => {
                    for entry in entries.log_entry {
                        if let Err(err) = ingest_envoy_http_access_log_entry(
                            &self.telemetry,
                            &self.ip_map,
                            &entry,
                        )
                        .await
                        {
                            warn!(%err, "failed to ingest Envoy HTTP access log entry");
                        }
                    }
                }
                None => {}
            }
        }

        Ok(Response::new(StreamAccessLogsResponse {}))
    }
}

async fn serve_envoy_access_log_server(
    telemetry: EgressTelemetry,
    paths: OadPaths,
    ip_map: SandboxIpMap,
) -> Result<(), ObservabilityError> {
    let socket = paths.envoy_access_log_socket();
    if let Some(parent) = socket.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    remove_stale_socket(&socket).await?;
    let listener = UnixListener::bind(&socket)?;
    info!(path = %socket.display(), "Envoy access log gRPC server listening");

    let result = Server::builder()
        .add_service(AccessLogServiceServer::new(EnvoyAccessLogService {
            telemetry: telemetry.clone(),
            ip_map,
        }))
        .serve_with_incoming(UnixListenerStream::new(listener))
        .await;
    cleanup_socket(&socket).await;
    result?;
    Ok(())
}

async fn remove_stale_socket(path: &std::path::Path) -> Result<(), ObservabilityError> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

async fn cleanup_socket(path: &std::path::Path) {
    if let Err(err) = tokio::fs::remove_file(path).await
        && err.kind() != std::io::ErrorKind::NotFound
    {
        warn!(path = %path.display(), %err, "failed to remove Envoy access log socket");
    }
}

/// An egress connection rendered as an OTLP span, independent of the sink.
struct EgressTrace {
    sandbox: SandboxSpan,
    egress: EgressSpan,
}

struct SandboxSpan {
    name: &'static str,
    start_time: SystemTime,
    end_time: SystemTime,
    attributes: Vec<KeyValue>,
    status: Status,
}

struct EgressSpan {
    name: &'static str,
    start_time: SystemTime,
    end_time: SystemTime,
    attributes: Vec<KeyValue>,
    status: Status,
}

/// Fields shared by every Envoy access-log entry, regardless of protocol.
/// Returns `None` when the entry lacks a usable source address.
struct EgressBase {
    sandbox_id: String,
    src_ip: String,
    dst_ip: Option<String>,
    dst_port: Option<u16>,
    start_time: SystemTime,
    end_time: SystemTime,
    tls_sni: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeField<T> {
    Valid(T),
    Missing,
    Invalid,
}

async fn resolve_egress_base(
    ip_map: &SandboxIpMap,
    common: &AccessLogCommon,
) -> Option<EgressBase> {
    let src_ip = address_endpoint(common.downstream_remote_address.as_ref()).0?;
    let (dst_ip, dst_port) = address_endpoint(common.upstream_remote_address.as_ref());
    // Resolve the sandbox id from the source IP via the in-memory index; fall
    // back to the raw IP when it's unknown or unparseable.
    let sandbox_id = match src_ip.parse::<Ipv4Addr>() {
        Ok(ip) => ip_map
            .get(&ip)
            .await
            .map_or_else(|| src_ip.clone(), |id| id.as_str().to_string()),
        Err(_) => src_ip.clone(),
    };
    let (start_time, end_time) = resolve_span_timing(common);
    Some(EgressBase {
        sandbox_id,
        src_ip,
        dst_ip,
        dst_port,
        start_time,
        end_time,
        tls_sni: envoy_tls_sni(common),
    })
}

fn resolve_span_timing(common: &AccessLogCommon) -> (SystemTime, SystemTime) {
    resolve_span_timing_at(common, SystemTime::now())
}

fn resolve_span_timing_at(
    common: &AccessLogCommon,
    observed_at: SystemTime,
) -> (SystemTime, SystemTime) {
    let start = start_time_from_proto(common.start_time.as_ref());
    let duration = span_duration_from_proto(common);

    match (start, duration) {
        (TimeField::Valid(start_time), TimeField::Valid(duration)) => {
            let end_time = start_time
                .checked_add(duration)
                .unwrap_or_else(|| end_at_or_after_start(start_time, observed_at));
            (start_time, end_time)
        }
        (TimeField::Valid(start_time), TimeField::Missing | TimeField::Invalid) => {
            (start_time, end_at_or_after_start(start_time, observed_at))
        }
        (TimeField::Missing | TimeField::Invalid, TimeField::Valid(duration)) => {
            let end_time = observed_at;
            let start_time = end_time.checked_sub(duration).unwrap_or(end_time);
            (start_time, end_time)
        }
        (TimeField::Missing | TimeField::Invalid, TimeField::Missing | TimeField::Invalid) => {
            (observed_at, observed_at)
        }
    }
}

fn end_at_or_after_start(start_time: SystemTime, observed_at: SystemTime) -> SystemTime {
    if observed_at.duration_since(start_time).is_ok() {
        observed_at
    } else {
        start_time
    }
}

fn sandbox_span_attributes(base: &EgressBase) -> Vec<KeyValue> {
    vec![
        KeyValue::new("sandbox.id", base.sandbox_id.clone()),
        KeyValue::new(attribute::CLIENT_ADDRESS, base.src_ip.clone()),
    ]
}

/// Pushes the attributes shared by TCP and HTTP egress spans.
fn push_base_attributes(attributes: &mut Vec<KeyValue>, base: &EgressBase) {
    attributes.push(KeyValue::new("sandbox.id", base.sandbox_id.clone()));
    attributes.push(KeyValue::new(
        attribute::CLIENT_ADDRESS,
        base.src_ip.clone(),
    ));
    attributes.push(KeyValue::new(attribute::NETWORK_TRANSPORT, "tcp"));
    if let Some(dst_ip) = &base.dst_ip {
        attributes.push(KeyValue::new(attribute::SERVER_ADDRESS, dst_ip.clone()));
    }
    if let Some(dst_port) = base.dst_port {
        attributes.push(KeyValue::new(attribute::SERVER_PORT, i64::from(dst_port)));
    }
    if let Some(tls_sni) = &base.tls_sni {
        attributes.push(KeyValue::new("tls.server.name", tls_sni.clone()));
    }
}

/// Emits one egress span from an access-log entry's common properties. The
/// `push_protocol_attrs` closure adds the TCP- or HTTP-specific attributes; the
/// shared base attributes, reason, and status are handled here. Entries without
/// usable common properties / source address are dropped.
async fn ingest_egress_entry(
    telemetry: &EgressTelemetry,
    ip_map: &SandboxIpMap,
    common: Option<&AccessLogCommon>,
    span_name: &'static str,
    reason_prefix: &'static str,
    push_protocol_attrs: impl FnOnce(&mut Vec<KeyValue>),
) {
    let Some(common) = common else { return };
    let Some(base) = resolve_egress_base(ip_map, common).await else {
        return;
    };

    let (reason, status) = envoy_reason_and_status(reason_prefix, common);
    let mut attributes = Vec::new();
    push_base_attributes(&mut attributes, &base);
    attributes.push(KeyValue::new("egress.reason", reason));
    push_protocol_attrs(&mut attributes);

    telemetry.emit(EgressTrace {
        sandbox: SandboxSpan {
            name: "sandbox",
            start_time: base.start_time,
            end_time: base.end_time,
            attributes: sandbox_span_attributes(&base),
            status: status.clone(),
        },
        egress: EgressSpan {
            name: span_name,
            start_time: base.start_time,
            end_time: base.end_time,
            attributes,
            status,
        },
    });
}

async fn ingest_envoy_tcp_access_log_entry(
    telemetry: &EgressTelemetry,
    ip_map: &SandboxIpMap,
    entry: &TcpAccessLogEntry,
) -> Result<(), ObservabilityError> {
    ingest_egress_entry(
        telemetry,
        ip_map,
        entry.common_properties.as_ref(),
        "egress.tcp",
        "envoy_tcp_proxy",
        |attributes| {
            if let Some(conn) = entry.connection_properties.as_ref() {
                // Envoy's ConnectionProperties is downstream-relative:
                // `received_bytes` is what Envoy received *from* the sandbox (its
                // egress/upload) and `sent_bytes` is what Envoy sent *to* the
                // sandbox (its download). The span is sandbox-centric, so map
                // them to the sandbox's perspective.
                attributes.push(KeyValue::new(
                    "egress.sent_bytes",
                    i64::try_from(conn.received_bytes).unwrap_or(i64::MAX),
                ));
                attributes.push(KeyValue::new(
                    "egress.received_bytes",
                    i64::try_from(conn.sent_bytes).unwrap_or(i64::MAX),
                ));
            }
        },
    )
    .await;
    Ok(())
}

async fn ingest_envoy_http_access_log_entry(
    telemetry: &EgressTelemetry,
    ip_map: &SandboxIpMap,
    entry: &HttpAccessLogEntry,
) -> Result<(), ObservabilityError> {
    ingest_egress_entry(
        telemetry,
        ip_map,
        entry.common_properties.as_ref(),
        "egress.http",
        "envoy_http_proxy",
        |attributes| {
            let request = entry.request.as_ref();
            if let Some(method) = request.and_then(http_method) {
                attributes.push(KeyValue::new(attribute::HTTP_REQUEST_METHOD, method));
            }
            if let Some(scheme) = request.and_then(|request| clean_envoy_value(&request.scheme)) {
                attributes.push(KeyValue::new(attribute::URL_SCHEME, scheme));
            }
            if let Some(path) = request.and_then(|request| clean_envoy_value(&request.path)) {
                attributes.push(KeyValue::new(attribute::URL_PATH, path));
            }
            if let Some(user_agent) =
                request.and_then(|request| clean_envoy_value(&request.user_agent))
            {
                attributes.push(KeyValue::new(attribute::USER_AGENT_ORIGINAL, user_agent));
            }
            if let Some(status) = entry.response.as_ref().and_then(http_status) {
                attributes.push(KeyValue::new(
                    attribute::HTTP_RESPONSE_STATUS_CODE,
                    i64::from(status),
                ));
            }
        },
    )
    .await;
    Ok(())
}

fn address_endpoint(address: Option<&Address>) -> (Option<String>, Option<u16>) {
    let Some(address::Address::SocketAddress(socket_address)) =
        address.and_then(|address| address.address.as_ref())
    else {
        return (None, None);
    };
    socket_endpoint(socket_address)
}

fn socket_endpoint(socket_address: &SocketAddress) -> (Option<String>, Option<u16>) {
    let host = clean_envoy_value(&socket_address.address);
    let port = match socket_address.port_specifier {
        Some(socket_address::PortSpecifier::PortValue(port)) => u16::try_from(port).ok(),
        None => None,
    };
    (host, port)
}

fn start_time_from_proto(timestamp: Option<&prost_types::Timestamp>) -> TimeField<SystemTime> {
    let Some(timestamp) = timestamp else {
        return TimeField::Missing;
    };
    // A present-but-unset Envoy timestamp (the proto3 default) decodes to the
    // 1970 epoch; treat it as missing so the caller can approximate from ingest
    // time rather than pinning spans to Unix epoch.
    if timestamp.seconds == 0 && timestamp.nanos == 0 {
        return TimeField::Missing;
    }
    if timestamp.seconds < 0 || !(0..=999_999_999).contains(&timestamp.nanos) {
        return TimeField::Invalid;
    }
    let Ok(seconds) = u64::try_from(timestamp.seconds) else {
        return TimeField::Invalid;
    };
    let Ok(nanos) = u32::try_from(timestamp.nanos) else {
        return TimeField::Invalid;
    };
    match SystemTime::UNIX_EPOCH.checked_add(Duration::new(seconds, nanos)) {
        Some(time) => TimeField::Valid(time),
        None => TimeField::Invalid,
    }
}

fn span_duration_from_proto(common: &AccessLogCommon) -> TimeField<Duration> {
    let duration = duration_from_proto(common.duration.as_ref());
    let downstream_tx_duration =
        duration_from_proto(common.time_to_last_downstream_tx_byte.as_ref());
    match (duration, downstream_tx_duration) {
        (TimeField::Valid(duration), _) | (_, TimeField::Valid(duration)) => {
            TimeField::Valid(duration)
        }
        (TimeField::Invalid, _) | (_, TimeField::Invalid) => TimeField::Invalid,
        (TimeField::Missing, TimeField::Missing) => TimeField::Missing,
    }
}

fn duration_from_proto(duration: Option<&prost_types::Duration>) -> TimeField<Duration> {
    let Some(duration) = duration else {
        return TimeField::Missing;
    };
    if duration.seconds < 0 || !(0..=999_999_999).contains(&duration.nanos) {
        return TimeField::Invalid;
    }
    let Ok(seconds) = u64::try_from(duration.seconds) else {
        return TimeField::Invalid;
    };
    let Ok(nanos) = u32::try_from(duration.nanos) else {
        return TimeField::Invalid;
    };
    TimeField::Valid(Duration::new(seconds, nanos))
}

fn clean_envoy_value(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty() && value != "-").then(|| value.to_string())
}

/// Derives the `egress.reason` attribute and the span status from a single scan
/// of the entry's failure signals (response flags + transport failure reason),
/// so the 28-entry flag table is walked once per entry rather than twice.
fn envoy_reason_and_status(prefix: &str, common: &AccessLogCommon) -> (String, Status) {
    // The suffix is non-empty exactly when there is a flag or failure signal, so
    // it doubles as the "is this an error?" predicate and the error message.
    let suffix = envoy_reason_suffix(common);
    let trimmed = suffix.trim();
    let status = if trimmed.is_empty() {
        Status::Unset
    } else {
        Status::error(trimmed.to_string())
    };
    (format!("{prefix}{suffix}"), status)
}

fn envoy_tls_sni(common: &AccessLogCommon) -> Option<String> {
    common
        .tls_properties
        .as_ref()
        .and_then(|tls| clean_envoy_value(&tls.tls_sni_hostname))
}

/// Maps Envoy's failure signals (response flags + transport failure reason) to
/// a `" flags=...,... failure=..."` suffix; empty when the connection is clean.
fn envoy_reason_suffix(common: &AccessLogCommon) -> String {
    let mut reason = String::new();
    if let Some(flags) = common.response_flags.as_ref().map(response_flag_names)
        && !flags.is_empty()
    {
        reason.push_str(" flags=");
        reason.push_str(&flags.join(","));
    }
    if let Some(failure) = clean_envoy_value(&common.upstream_transport_failure_reason) {
        reason.push_str(" failure=");
        reason.push_str(&failure);
    }
    reason
}

fn http_method(request: &HttpRequestProperties) -> Option<String> {
    let value = match request.request_method {
        1 => "GET",
        2 => "HEAD",
        3 => "POST",
        4 => "PUT",
        5 => "DELETE",
        6 => "CONNECT",
        7 => "OPTIONS",
        8 => "TRACE",
        9 => "PATCH",
        _ => return None,
    };
    Some(value.to_string())
}

fn http_status(response: &HttpResponseProperties) -> Option<u16> {
    response
        .response_code
        .and_then(|status| u16::try_from(status).ok())
}

fn response_flag_names(flags: &ResponseFlags) -> Vec<&'static str> {
    // (accessor, Envoy flag name), in proto field order. Driving the set from a
    // table keeps the push order and the names from drifting out of sync the way
    // 28 parallel `if`/`push` arms would.
    type Flag = (fn(&ResponseFlags) -> bool, &'static str);
    const FLAGS: &[Flag] = &[
        (|f| f.failed_local_healthcheck, "failed_local_healthcheck"),
        (|f| f.no_healthy_upstream, "no_healthy_upstream"),
        (|f| f.upstream_request_timeout, "upstream_request_timeout"),
        (|f| f.local_reset, "local_reset"),
        (|f| f.upstream_remote_reset, "upstream_remote_reset"),
        (
            |f| f.upstream_connection_failure,
            "upstream_connection_failure",
        ),
        (
            |f| f.upstream_connection_termination,
            "upstream_connection_termination",
        ),
        (|f| f.upstream_overflow, "upstream_overflow"),
        (|f| f.no_route_found, "no_route_found"),
        (|f| f.delay_injected, "delay_injected"),
        (|f| f.fault_injected, "fault_injected"),
        (|f| f.rate_limited, "rate_limited"),
        (|f| f.rate_limit_service_error, "rate_limit_service_error"),
        (
            |f| f.downstream_connection_termination,
            "downstream_connection_termination",
        ),
        (
            |f| f.upstream_retry_limit_exceeded,
            "upstream_retry_limit_exceeded",
        ),
        (|f| f.stream_idle_timeout, "stream_idle_timeout"),
        (
            |f| f.invalid_envoy_request_headers,
            "invalid_envoy_request_headers",
        ),
        (|f| f.downstream_protocol_error, "downstream_protocol_error"),
        (
            |f| f.upstream_max_stream_duration_reached,
            "upstream_max_stream_duration_reached",
        ),
        (
            |f| f.response_from_cache_filter,
            "response_from_cache_filter",
        ),
        (|f| f.no_filter_config_found, "no_filter_config_found"),
        (|f| f.duration_timeout, "duration_timeout"),
        (|f| f.upstream_protocol_error, "upstream_protocol_error"),
        (|f| f.no_cluster_found, "no_cluster_found"),
        (|f| f.overload_manager, "overload_manager"),
        (|f| f.dns_resolution_failure, "dns_resolution_failure"),
        (|f| f.downstream_remote_reset, "downstream_remote_reset"),
    ];
    FLAGS
        .iter()
        .filter(|(get, _)| get(flags))
        .map(|(_, name)| *name)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::envoy::service::accesslog::v3::{ConnectionProperties, TlsProperties};
    use super::*;
    use std::sync::Mutex;

    /// Captures emitted spans so assertions can inspect attributes/status
    /// without standing up an OTLP collector.
    #[derive(Default)]
    struct CapturingEmitter {
        traces: Mutex<Vec<CapturedTrace>>,
    }

    struct CapturedTrace {
        sandbox: CapturedSpan,
        egress: CapturedSpan,
    }

    struct CapturedSpan {
        name: &'static str,
        start_time: SystemTime,
        end_time: SystemTime,
        attributes: Vec<KeyValue>,
        status: Status,
    }

    impl SpanEmitter for CapturingEmitter {
        fn emit(&self, trace: EgressTrace) {
            self.traces.lock().unwrap().push(CapturedTrace {
                sandbox: CapturedSpan {
                    name: trace.sandbox.name,
                    start_time: trace.sandbox.start_time,
                    end_time: trace.sandbox.end_time,
                    attributes: trace.sandbox.attributes,
                    status: trace.sandbox.status,
                },
                egress: CapturedSpan {
                    name: trace.egress.name,
                    start_time: trace.egress.start_time,
                    end_time: trace.egress.end_time,
                    attributes: trace.egress.attributes,
                    status: trace.egress.status,
                },
            });
        }
    }

    impl CapturingEmitter {
        /// Drains captured spans into an owned vec so assertions don't hold the
        /// lock guard (which trips `clippy::significant_drop_tightening`).
        fn take(&self) -> Vec<CapturedTrace> {
            std::mem::take(&mut *self.traces.lock().unwrap())
        }
    }

    fn attr<'a>(span: &'a CapturedSpan, key: &str) -> Option<&'a opentelemetry::Value> {
        span.attributes
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| &kv.value)
    }

    fn epoch(seconds: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
    }

    fn common_with_timing(
        start_time: Option<prost_types::Timestamp>,
        duration: Option<prost_types::Duration>,
        downstream_tx_duration: Option<prost_types::Duration>,
    ) -> AccessLogCommon {
        AccessLogCommon {
            downstream_remote_address: None,
            tls_properties: None,
            start_time,
            time_to_last_downstream_tx_byte: downstream_tx_duration,
            upstream_remote_address: None,
            response_flags: None,
            upstream_transport_failure_reason: String::new(),
            duration,
        }
    }

    /// Builds an IP→sandbox index mapping the test source IP to sandbox `s1`,
    /// mirroring what `NetworkManager` would populate at reconcile time.
    async fn test_ip_map() -> SandboxIpMap {
        let ip_map = SandboxIpMap::default();
        ip_map
            .insert(
                "10.90.0.2".parse().unwrap(),
                oad_core::SandboxId::new("s1").unwrap(),
            )
            .await;
        ip_map
    }

    #[tokio::test]
    async fn ingests_envoy_tcp_access_log_entry_as_egress_span() {
        let ip_map = test_ip_map().await;

        let emitter = Arc::new(CapturingEmitter::default());
        let telemetry = EgressTelemetry::with_emitter(emitter.clone());

        ingest_envoy_tcp_access_log_entry(
            &telemetry,
            &ip_map,
            &TcpAccessLogEntry {
                common_properties: Some(AccessLogCommon {
                    downstream_remote_address: Some(Address {
                        address: Some(address::Address::SocketAddress(SocketAddress {
                            address: "10.90.0.2".to_string(),
                            port_specifier: None,
                        })),
                    }),
                    tls_properties: Some(TlsProperties {
                        tls_sni_hostname: "example.test".to_string(),
                    }),
                    start_time: Some(prost_types::Timestamp {
                        seconds: 1_780_531_200,
                        nanos: 0,
                    }),
                    time_to_last_downstream_tx_byte: None,
                    duration: Some(prost_types::Duration {
                        seconds: 1,
                        nanos: 500_000_000,
                    }),
                    upstream_remote_address: Some(Address {
                        address: Some(address::Address::SocketAddress(SocketAddress {
                            address: "203.0.113.10".to_string(),
                            port_specifier: Some(socket_address::PortSpecifier::PortValue(443)),
                        })),
                    }),
                    response_flags: Some(ResponseFlags {
                        upstream_connection_failure: true,
                        ..ResponseFlags::default()
                    }),
                    upstream_transport_failure_reason: "test_failure".to_string(),
                }),
                connection_properties: Some(ConnectionProperties {
                    received_bytes: 1234,
                    sent_bytes: 5678,
                }),
            },
        )
        .await
        .unwrap();

        let traces = emitter.take();
        assert_eq!(traces.len(), 1);
        let trace = &traces[0];
        assert_eq!(trace.sandbox.name, "sandbox");
        assert_eq!(
            attr(&trace.sandbox, "sandbox.id"),
            Some(&opentelemetry::Value::from("s1"))
        );
        assert_eq!(trace.sandbox.start_time, trace.egress.start_time);
        assert_eq!(trace.sandbox.end_time, trace.egress.end_time);
        assert!(matches!(trace.sandbox.status, Status::Error { .. }));
        let span = &trace.egress;
        assert_eq!(span.name, "egress.tcp");
        assert_eq!(
            attr(span, "sandbox.id"),
            Some(&opentelemetry::Value::from("s1"))
        );
        assert_eq!(
            attr(span, attribute::SERVER_ADDRESS),
            Some(&opentelemetry::Value::from("203.0.113.10"))
        );
        assert_eq!(
            attr(span, attribute::SERVER_PORT),
            Some(&opentelemetry::Value::from(443_i64))
        );
        assert_eq!(
            attr(span, "tls.server.name"),
            Some(&opentelemetry::Value::from("example.test"))
        );
        // Envoy received_bytes (1234) = bytes from the sandbox = egress.sent_bytes;
        // Envoy sent_bytes (5678) = bytes to the sandbox = egress.received_bytes.
        assert_eq!(
            attr(span, "egress.sent_bytes"),
            Some(&opentelemetry::Value::from(1234_i64))
        );
        assert_eq!(
            attr(span, "egress.received_bytes"),
            Some(&opentelemetry::Value::from(5678_i64))
        );
        assert_eq!(
            attr(span, "egress.reason"),
            Some(&opentelemetry::Value::from(
                "envoy_tcp_proxy flags=upstream_connection_failure failure=test_failure"
            ))
        );
        assert!(matches!(span.status, Status::Error { .. }));
        // start + 1.5s duration
        assert_eq!(
            span.end_time.duration_since(span.start_time).unwrap(),
            Duration::new(1, 500_000_000)
        );
    }

    #[test]
    fn span_timing_uses_ingest_time_when_duration_is_missing() {
        let common = common_with_timing(
            Some(prost_types::Timestamp {
                seconds: 100,
                nanos: 0,
            }),
            None,
            None,
        );

        let (start_time, end_time) = resolve_span_timing_at(&common, epoch(105));

        assert_eq!(start_time, epoch(100));
        assert_eq!(end_time, epoch(105));
    }

    #[test]
    fn span_timing_back_computes_start_when_start_time_is_missing() {
        let common = common_with_timing(
            None,
            Some(prost_types::Duration {
                seconds: 5,
                nanos: 0,
            }),
            None,
        );

        let (start_time, end_time) = resolve_span_timing_at(&common, epoch(105));

        assert_eq!(start_time, epoch(100));
        assert_eq!(end_time, epoch(105));
    }

    #[test]
    fn span_timing_falls_back_to_downstream_tx_duration() {
        let common = common_with_timing(
            Some(prost_types::Timestamp {
                seconds: 100,
                nanos: 0,
            }),
            None,
            Some(prost_types::Duration {
                seconds: 2,
                nanos: 0,
            }),
        );

        let (start_time, end_time) = resolve_span_timing_at(&common, epoch(105));

        assert_eq!(start_time, epoch(100));
        assert_eq!(end_time, epoch(102));
    }

    #[test]
    fn span_timing_prefers_total_duration() {
        let common = common_with_timing(
            Some(prost_types::Timestamp {
                seconds: 100,
                nanos: 0,
            }),
            Some(prost_types::Duration {
                seconds: 5,
                nanos: 0,
            }),
            Some(prost_types::Duration {
                seconds: 2,
                nanos: 0,
            }),
        );

        let (start_time, end_time) = resolve_span_timing_at(&common, epoch(110));

        assert_eq!(start_time, epoch(100));
        assert_eq!(end_time, epoch(105));
    }

    #[tokio::test]
    async fn ingests_envoy_http_access_log_entry_with_http_attributes() {
        let ip_map = test_ip_map().await;

        let emitter = Arc::new(CapturingEmitter::default());
        let telemetry = EgressTelemetry::with_emitter(emitter.clone());

        ingest_envoy_http_access_log_entry(
            &telemetry,
            &ip_map,
            &HttpAccessLogEntry {
                common_properties: Some(AccessLogCommon {
                    downstream_remote_address: Some(Address {
                        address: Some(address::Address::SocketAddress(SocketAddress {
                            address: "10.90.0.2".to_string(),
                            port_specifier: None,
                        })),
                    }),
                    tls_properties: None,
                    start_time: Some(prost_types::Timestamp {
                        seconds: 100,
                        nanos: 0,
                    }),
                    time_to_last_downstream_tx_byte: None,
                    duration: None,
                    upstream_remote_address: Some(Address {
                        address: Some(address::Address::SocketAddress(SocketAddress {
                            address: "203.0.113.10".to_string(),
                            port_specifier: Some(socket_address::PortSpecifier::PortValue(8080)),
                        })),
                    }),
                    response_flags: None,
                    upstream_transport_failure_reason: String::new(),
                }),
                protocol_version: 2,
                request: Some(HttpRequestProperties {
                    request_method: 1,
                    scheme: "http".to_string(),
                    authority: "example.test".to_string(),
                    path: "/health".to_string(),
                    user_agent: "oad-test".to_string(),
                    ..HttpRequestProperties::default()
                }),
                response: Some(HttpResponseProperties {
                    response_code: Some(200),
                    response_code_details: "via_upstream".to_string(),
                    ..HttpResponseProperties::default()
                }),
            },
        )
        .await
        .unwrap();

        let traces = emitter.take();
        assert_eq!(traces.len(), 1);
        let trace = &traces[0];
        assert_eq!(trace.sandbox.name, "sandbox");
        assert_eq!(
            attr(&trace.sandbox, "sandbox.id"),
            Some(&opentelemetry::Value::from("s1"))
        );
        assert_eq!(trace.sandbox.start_time, trace.egress.start_time);
        assert_eq!(trace.sandbox.end_time, trace.egress.end_time);
        assert!(matches!(trace.sandbox.status, Status::Unset));
        let span = &trace.egress;
        assert_eq!(span.name, "egress.http");
        assert_eq!(
            attr(span, attribute::HTTP_REQUEST_METHOD),
            Some(&opentelemetry::Value::from("GET"))
        );
        assert_eq!(
            attr(span, attribute::URL_SCHEME),
            Some(&opentelemetry::Value::from("http"))
        );
        assert_eq!(
            attr(span, attribute::URL_PATH),
            Some(&opentelemetry::Value::from("/health"))
        );
        assert_eq!(
            attr(span, attribute::USER_AGENT_ORIGINAL),
            Some(&opentelemetry::Value::from("oad-test"))
        );
        assert_eq!(
            attr(span, attribute::HTTP_RESPONSE_STATUS_CODE),
            Some(&opentelemetry::Value::from(200_i64))
        );
        assert_eq!(
            attr(span, "egress.reason"),
            Some(&opentelemetry::Value::from("envoy_http_proxy"))
        );
        assert!(matches!(span.status, Status::Unset));
        // No duration on the wire: use access-log ingest time as an approximate
        // end rather than collapsing the span to zero length.
        assert!(
            span.end_time
                .duration_since(span.start_time)
                .is_ok_and(|elapsed| !elapsed.is_zero())
        );
    }

    #[tokio::test]
    async fn disabled_telemetry_drops_spans_without_panicking() {
        let telemetry = EgressTelemetry::disabled();
        // Emitting through a disabled handle is a no-op.
        let now = SystemTime::now();
        telemetry.emit(EgressTrace {
            sandbox: SandboxSpan {
                name: "sandbox",
                start_time: now,
                end_time: now,
                attributes: Vec::new(),
                status: Status::Unset,
            },
            egress: EgressSpan {
                name: "egress.tcp",
                start_time: now,
                end_time: now,
                attributes: Vec::new(),
                status: Status::Unset,
            },
        });
    }
}
