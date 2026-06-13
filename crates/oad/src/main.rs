mod background_exec;
mod cas;
mod config;
mod network;
mod registration;
mod registry;
mod snapshots;

use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::error::Error as StdError;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use background_exec::BackgroundExecStore;
use futures_util::{StreamExt, stream};
use oad_api::{
    BackgroundExecEvent, BackgroundExecEventKind, BackgroundExecInfo, BackgroundExecResponse,
    BackgroundExecStatus, BackgroundExecStdinRequest, BackgroundExecStdinResponse, CasSnapshotInfo,
    CreateSandboxRequest, CreateSnapshotRequest, DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS, ErrorBody,
    ErrorResponse, ExecRequest, ExecResponse, ListSnapshotsResponse, SandboxNetworkResponse,
    SandboxResponse, SnapshotInfo, SnapshotResponse, StartBackgroundExecRequest,
};
use oad_core::{
    ContainerSpec, EgressDestination, EgressPolicy, EgressRule, EnvVar, L7EgressPolicy,
    ManagedNetworkBackend, OadPaths, PAUSE_CONTAINER, PortRange, Protocol, SandboxId,
    SandboxNetworkSpec, SandboxRecord, SandboxSpec, SandboxStatus, TrafficShapingPolicy,
    UdpEgressPolicy, validate_container_name, validate_snapshot_name,
};
use oad_oci::GvisorManager;
use oad_runtime::{
    RestoreSpecValidation, RunscNetworkMode, checkpoint_image_complete_for_containers,
    checkpoint_sandbox, copy_checkpoint_image, delete_visible_container_sequence, restore_sandbox,
    snapshot_sandbox, start_container_sequence,
};
use registry::{NamedLocks, SandboxRegistry};
use thiserror::Error;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};

#[derive(Clone)]
struct AppState {
    config: Arc<oad_core::DaemonConfig>,
    paths: OadPaths,
    registry: SandboxRegistry,
    snapshot_locks: NamedLocks,
    gvisor: GvisorManager,
    network: network::NetworkManager,
    background_execs: BackgroundExecStore,
    /// When set, captured snapshots are published to the content-addressed store
    /// so they become portable across nodes.
    cas: Option<Arc<cas::CasPublisher>>,
}

const BACKGROUND_EXEC_KILL_RESPONSE_TIMEOUT: Duration = Duration::from_secs(1);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    ensure_started_as_root()?;

    let config_path = config::config_path_from_args()?;
    let config = Arc::new(config::load_config(config_path).await?);
    let paths = OadPaths::new(config.runtime.base_dir.clone());

    tokio::fs::create_dir_all(paths.sandboxes_dir())
        .await
        .with_context(|| format!("failed to create {}", paths.sandboxes_dir().display()))?;

    let registry = SandboxRegistry::recover(&paths).await;
    reconcile_sandboxes(&registry, &paths).await;
    let gvisor = GvisorManager::new();
    let network = network::NetworkManager::new(config.runtime.network.clone())
        .context("failed to initialize network manager")?;
    let cas = config
        .cas
        .as_ref()
        .map(cas::CasPublisher::from_config)
        .transpose()
        .context("failed to initialize CAS publisher")?
        .map(Arc::new);
    let state = AppState {
        config: config.clone(),
        paths,
        registry,
        snapshot_locks: NamedLocks::default(),
        gvisor,
        network,
        background_execs: BackgroundExecStore::default(),
        cas,
    };
    if let Err(err) = state.network.reconcile_all(&state.paths).await {
        warn!(error = %err, "failed to reconcile managed network state at startup");
    }

    let shutdown_state = state.clone();
    let app = router(state);
    let addr: SocketAddr = config
        .http
        .bind
        .parse()
        .with_context(|| format!("invalid bind address {:?}", config.http.bind))?;
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;

    // Self-register with the control plane (if configured) so it can discover
    // this instance and call our `/v1` API directly.
    let registration = config.control_plane.clone().map(|cp| {
        Arc::new(registration::Registration::new(
            cp,
            config.http.bearer_token.clone(),
            uuid::Uuid::new_v4().to_string(),
        ))
    });
    let registration_task = registration.as_ref().map(|r| Arc::clone(r).spawn());

    info!(%addr, "oad listening");
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("http server failed");
    if let Some(task) = registration_task {
        task.abort();
    }
    if let Some(registration) = registration {
        registration.deregister().await;
    }
    checkpoint_running_sandboxes_for_shutdown(&shutdown_state).await;
    result
}

fn ensure_started_as_root() -> anyhow::Result<()> {
    if !rustix::process::geteuid().is_root() {
        bail!("oad must be started as root; try running it with sudo");
    }
    Ok(())
}

/// Reconciles recovered sandbox records against live `runsc` state at startup.
///
/// A restart can't trust the persisted `Running`/`Pending`/`Unknown` status, so
/// each such sandbox is judged by its `pause` container, which *is* the gVisor
/// sandbox: alive → `Running`, exited → `Stopped` (or `Suspended` when a
/// checkpoint image is available), unqueryable → `Unknown` (or checkpoint-backed
/// `Suspended`). Anything reconciled as non-running has any remaining visible
/// `runsc` containers deleted so later restore/delete operations don't collide
/// with stale runtime IDs. App containers may legitimately exit (a finished
/// workload) while the sandbox stays up, so their individual states do not make
/// the sandbox an `Error`.
/// Dead sandbox directories are left in place for an explicit `DELETE` rather
/// than removed here, to avoid destructive surprises on boot.
async fn reconcile_sandboxes(registry: &SandboxRegistry, paths: &OadPaths) {
    for record in registry.list().await {
        if !matches!(
            record.status,
            SandboxStatus::Pending
                | SandboxStatus::Running
                | SandboxStatus::Stopping
                | SandboxStatus::Unknown
        ) {
            continue;
        }
        let checkpoint_available = checkpoint_image_complete_for_containers(
            &paths.checkpoint_dir(&record.id),
            &record.containers,
        )
        .await;
        let (mut status, mut last_error) = match oad_runtime::container_running_result(
            paths,
            &record.id,
            PAUSE_CONTAINER,
        )
        .await
        {
            Ok(true) => (SandboxStatus::Running, None),
            Ok(false) if checkpoint_available => (SandboxStatus::Suspended, None),
            Ok(false) => (SandboxStatus::Stopped, None),
            Err(err) if checkpoint_available => {
                warn!(
                    sandbox_id = %record.id,
                    %err,
                    "failed to query pause container state during reconciliation; keeping checkpoint resumable"
                );
                (SandboxStatus::Suspended, None)
            }
            Err(err) => {
                warn!(
                    sandbox_id = %record.id,
                    %err,
                    "failed to query pause container state during reconciliation"
                );
                (
                    SandboxStatus::Unknown,
                    Some("failed to query runtime state during startup reconciliation".to_string()),
                )
            }
        };
        delete_reconciled_non_running_containers(paths, &record, &mut status, &mut last_error)
            .await;
        info!(sandbox_id = %record.id, ?status, "reconciled sandbox status after restart");
        if let Err(err) = registry
            .update(paths, &record.id, |record| {
                record.set_status(status.clone());
                record.last_error.clone_from(&last_error);
            })
            .await
        {
            warn!(sandbox_id = %record.id, %err, "failed to persist reconciled status");
        }
    }
}

async fn delete_reconciled_non_running_containers(
    paths: &OadPaths,
    record: &SandboxRecord,
    status: &mut SandboxStatus,
    last_error: &mut Option<String>,
) {
    if matches!(status, SandboxStatus::Running) {
        return;
    }

    match oad_runtime::delete_visible_container_sequence(paths, &record.id, &record.containers)
        .await
    {
        Ok(failures) if failures.is_empty() => {}
        Ok(failures) => {
            let message = format!(
                "failed to delete stale containers during startup reconciliation: {failures:?}"
            );
            warn!(sandbox_id = %record.id, ?failures, "failed to delete stale containers during reconciliation");
            *status = SandboxStatus::Unknown;
            *last_error = Some(message);
        }
        Err(err) => {
            let message =
                format!("failed to clean up stale containers during startup reconciliation: {err}");
            warn!(sandbox_id = %record.id, %err, "failed to clean up stale containers during reconciliation");
            *status = SandboxStatus::Unknown;
            *last_error = Some(message);
        }
    }
}

fn router(state: AppState) -> Router {
    // Authenticated routes share a single bearer-token middleware layer, so a
    // new handler is covered by auth automatically rather than relying on every
    // handler remembering to call `require_auth`.
    let protected = Router::new()
        .route("/v1/sandboxes", post(create_sandbox))
        .route(
            "/v1/sandboxes/{id}",
            get(get_sandbox).delete(delete_sandbox),
        )
        .route("/v1/sandboxes/{id}/network", get(get_sandbox_network))
        .route("/v1/sandboxes/{id}/exec", post(exec_in_sandbox))
        .route("/v1/sandboxes/{id}/execs", post(start_background_exec))
        .route(
            "/v1/sandboxes/{id}/execs/{exec_id}",
            get(get_background_exec).delete(kill_background_exec),
        )
        .route(
            "/v1/sandboxes/{id}/execs/{exec_id}/stdin",
            post(write_background_exec_stdin),
        )
        .route(
            "/v1/sandboxes/{id}/execs/{exec_id}/events",
            get(stream_background_exec_events),
        )
        .route("/v1/sandboxes/{id}/suspend", post(suspend_sandbox))
        .route("/v1/sandboxes/{id}/resume", post(resume_sandbox))
        .route(
            "/v1/sandboxes/{id}/snapshot",
            post(snapshot_sandbox_handler),
        )
        .route("/v1/snapshots", get(list_snapshots))
        .route(
            "/v1/snapshots/{name}",
            axum::routing::delete(delete_snapshot),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_auth,
        ));

    Router::new()
        .route("/healthz", get(healthz))
        .route("/openapi.json", get(openapi_spec))
        .merge(protected)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// `OpenAPI` 3.1 specification for the oad HTTP API.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "oad",
        description = "HTTP API for the oad sandbox daemon, which runs gVisor-isolated container sandboxes."
    ),
    paths(
        healthz,
        openapi_spec,
        create_sandbox,
        get_sandbox,
        delete_sandbox,
        get_sandbox_network,
        exec_in_sandbox,
        start_background_exec,
        get_background_exec,
        kill_background_exec,
        write_background_exec_stdin,
        stream_background_exec_events,
        suspend_sandbox,
        resume_sandbox,
        snapshot_sandbox_handler,
        list_snapshots,
        delete_snapshot,
    ),
    components(schemas(
        CreateSandboxRequest,
        SandboxResponse,
        SandboxNetworkResponse,
        ExecRequest,
        ExecResponse,
        StartBackgroundExecRequest,
        BackgroundExecStatus,
        BackgroundExecInfo,
        BackgroundExecResponse,
        BackgroundExecStdinRequest,
        BackgroundExecStdinResponse,
        BackgroundExecEvent,
        BackgroundExecEventKind,
        ErrorResponse,
        ErrorBody,
        SandboxRecord,
        SandboxStatus,
        ContainerSpec,
        EnvVar,
        SandboxNetworkSpec,
        EgressPolicy,
        EgressRule,
        EgressDestination,
        Protocol,
        PortRange,
        TrafficShapingPolicy,
        L7EgressPolicy,
        UdpEgressPolicy,
        CreateSnapshotRequest,
        SnapshotInfo,
        CasSnapshotInfo,
        SnapshotResponse,
        ListSnapshotsResponse,
    )),
    tags(
        (name = "sandboxes", description = "Sandbox lifecycle management"),
        (name = "snapshots", description = "Snapshot management"),
        (name = "system", description = "Service health and metadata"),
    ),
    modifiers(&SecurityAddon),
)]
struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearer_token",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("opaque")
                    .build(),
            ),
        );
    }
}

/// Return the generated `OpenAPI` document.
#[utoipa::path(
    get,
    path = "/openapi.json",
    responses((status = 200, description = "OpenAPI specification document")),
    tag = "system",
)]
async fn openapi_spec() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}

/// Liveness probe.
#[utoipa::path(
    get,
    path = "/healthz",
    responses((status = 200, description = "Service is healthy")),
    tag = "system",
)]
async fn healthz() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

/// Create a sandbox and start its containers.
#[utoipa::path(
    post,
    path = "/v1/sandboxes",
    request_body = CreateSandboxRequest,
    responses(
        (status = 201, description = "Sandbox forked from a snapshot and running", body = SandboxResponse),
        (status = 202, description = "Fresh sandbox accepted; booting asynchronously (poll GET for status)", body = SandboxResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 409, description = "Sandbox already exists", body = ErrorResponse),
        (status = 500, description = "Failed to start sandbox", body = ErrorResponse),
    ),
    security(("bearer_token" = [])),
    tag = "sandboxes",
)]
async fn create_sandbox(
    State(state): State<AppState>,
    Json(request): Json<CreateSandboxRequest>,
) -> Result<axum::response::Response, AppError> {
    let from_snapshot = request.from_snapshot.clone();
    if from_snapshot.is_none() {
        validate_containers(&request.containers)?;
    }

    let id = match request.id.as_ref() {
        Some(value) => {
            SandboxId::new(value.as_str()).map_err(|err| AppError::BadRequest(err.to_string()))?
        }
        None => SandboxId::generate(),
    };

    let _guard = state.registry.acquire_lifecycle(&id).await;

    if state.registry.contains(&id).await {
        return Err(AppError::Conflict(format!("sandbox {id} already exists")));
    }

    if tokio::fs::try_exists(state.paths.sandbox_dir(&id)).await? {
        return Err(AppError::Conflict(format!(
            "sandbox directory for {id} already exists"
        )));
    }

    // A snapshot fork is comparatively quick (cached rootfs + checkpoint
    // restore), so it stays synchronous and returns the running record.
    if let Some(snapshot) = from_snapshot {
        let record = fork_from_snapshot(
            &state,
            &id,
            request.network.clone(),
            request.resources.clone(),
            &snapshot,
        )
        .await?;
        return Ok((
            StatusCode::CREATED,
            Json(SandboxResponse { sandbox: record }),
        )
            .into_response());
    }

    // A fresh boot pulls the container image, which for a cold image over a slow
    // registry can take minutes — far longer than a client's request timeout.
    // Validate the network synchronously (so bad input is a 400, not an async
    // failure), insert a Pending record so an immediate poll succeeds, then boot
    // in the background and return 202. Clients poll GET /v1/sandboxes/{id}
    // until the status is `running` (ready) or `error` (failed, with last_error).
    effective_fresh_network(&state, &request)?;
    let container_names = oad_core::container_names(&request.containers);
    let pending = SandboxRecord::new_pending(id.clone(), container_names.clone());
    state.registry.insert(&state.paths, pending.clone()).await?;

    let boot_state = state.clone();
    let boot_id = id.clone();
    tokio::spawn(async move {
        // Re-acquire the lifecycle lock inside the task (the handler's guard is
        // released when it returns 202). The Pending record inserted above keeps
        // the id reserved until this lock is held.
        let _guard = boot_state.registry.acquire_lifecycle(&boot_id).await;
        if let Err(err) = boot_fresh_sandbox(&boot_state, &boot_id, &request).await {
            error!(id = %boot_id, error = %err, "asynchronous sandbox boot failed");
            // boot_fresh_sandbox's cleanup removes the record on a normal
            // failure; re-persist a terminal Error record so a polling client
            // observes the failure rather than a 404. If a record survives (the
            // cleanup-failure retain path already set Error with a richer
            // message), leave it untouched.
            if boot_state.registry.get(&boot_id).await.is_none() {
                let mut record = SandboxRecord::new_pending(boot_id.clone(), container_names);
                record.set_error(format!("sandbox boot failed: {err}"));
                if let Err(persist_err) =
                    boot_state.registry.insert(&boot_state.paths, record).await
                {
                    error!(id = %boot_id, error = %persist_err, "failed to persist failed sandbox state");
                }
            }
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(SandboxResponse { sandbox: pending }),
    )
        .into_response())
}

/// Boots a sandbox fresh from its container images.
async fn boot_fresh_sandbox(
    state: &AppState,
    id: &SandboxId,
    request: &CreateSandboxRequest,
) -> Result<SandboxRecord, AppError> {
    let container_names = oad_core::container_names(&request.containers);
    let network = effective_fresh_network(state, request)?;

    let record = SandboxRecord::new_pending(id.clone(), container_names.clone());
    state.registry.insert(&state.paths, record).await?;
    let mut cleanup = SandboxCreateGuard::new(state, id, container_names.clone());
    info!(id = %id, "preparing sandbox network");

    let network_namespace = match prepare_sandbox_network(state, id, &network).await {
        Ok(network_namespace) => network_namespace,
        Err(err) => {
            cleanup.cleanup_now().await;
            return Err(err);
        }
    };

    let runsc_network_mode = runsc_network_mode(state, network_namespace.as_deref());
    let spec = SandboxSpec {
        pause_image: state.config.runtime.pause_image.clone(),
        containers: request.containers.clone(),
        network: network.clone(),
        network_mode: Some(runsc_network_mode.into()),
    };
    if let Err(err) = persist_sandbox_spec(state, id, &spec).await {
        cleanup.cleanup_now().await;
        return Err(err);
    }

    info!(id = %id, "preparing container bundles");
    if let Err(err) = prepare_bundles(
        state,
        id,
        &state.config.runtime.pause_image,
        &request.containers,
        network_namespace.as_deref(),
    )
    .await
    {
        cleanup.cleanup_now().await;
        return Err(err);
    }

    cleanup.delete_containers();
    info!(id = %id, containers = ?container_names, "starting containers");
    if let Err(err) =
        start_container_sequence(&state.paths, id, &container_names, runsc_network_mode).await
    {
        cleanup.cleanup_now().await;
        return Err(err.into());
    }

    match running_record(state, id).await {
        Ok(record) => {
            cleanup.disarm();
            info!(id = %id, "sandbox running");
            Ok(record)
        }
        Err(err) => {
            cleanup.cleanup_now().await;
            Err(err)
        }
    }
}

/// Forks a sandbox from a snapshot: rebuilds bundles from the snapshot manifest
/// (reusing cached EROFS rootfs images with fresh, per-fork overlays) and
/// restores every container from the snapshot's checkpoint image. Each fork
/// diverges through its own writable overlay.
async fn fork_from_snapshot(
    state: &AppState,
    id: &SandboxId,
    request_network: Option<SandboxNetworkSpec>,
    request_resources: Option<oad_core::ResourceSpec>,
    snapshot_name: &str,
) -> Result<SandboxRecord, AppError> {
    validate_snapshot_name(snapshot_name).map_err(|err| AppError::BadRequest(err.to_string()))?;
    let _snapshot_guard = state.snapshot_locks.acquire(snapshot_name).await;
    if !snapshots::exists(&state.paths, snapshot_name).await {
        return Err(AppError::NotFound(format!(
            "snapshot {snapshot_name} not found"
        )));
    }
    let manifest = snapshots::read_manifest(&state.paths, snapshot_name).await?;
    let container_names = manifest.container_names();
    let network = effective_fork_network(state, request_network, &manifest.network)?;

    // A request-level resource override replaces the limits on every container
    // restored from the snapshot, so resources are runtime data set per fork
    // rather than baked into the immutable snapshot.
    let mut containers = manifest.containers.clone();
    if let Some(resources) = &request_resources {
        for container in &mut containers {
            container.resources = Some(resources.clone());
        }
    }

    let mut record = SandboxRecord::new_pending(id.clone(), container_names.clone());
    record.origin_snapshot = Some(snapshot_name.to_string());
    state.registry.insert(&state.paths, record).await?;
    let mut cleanup = SandboxCreateGuard::new(state, id, container_names.clone());

    let network_namespace = match prepare_sandbox_network(state, id, &network).await {
        Ok(network_namespace) => network_namespace,
        Err(err) => {
            cleanup.cleanup_now().await;
            return Err(err);
        }
    };

    // Persist the spec (with the resolved network mode) so a fork can itself be
    // snapshotted/resumed later with the same runsc network mode.
    let runsc_network_mode = runsc_network_mode(state, network_namespace.as_deref());
    let spec = SandboxSpec {
        pause_image: manifest.pause_image.clone(),
        containers: containers.clone(),
        network: network.clone(),
        network_mode: Some(runsc_network_mode.into()),
    };
    if let Err(err) = persist_sandbox_spec(state, id, &spec).await {
        cleanup.cleanup_now().await;
        return Err(err);
    }

    if let Err(err) = prepare_bundles(
        state,
        id,
        &manifest.pause_image,
        &containers,
        network_namespace.as_deref(),
    )
    .await
    {
        cleanup.cleanup_now().await;
        return Err(err);
    }

    let image_dir = state.paths.snapshot_checkpoint_dir(snapshot_name);
    cleanup.delete_containers();
    if let Err(err) = restore_sandbox(
        &state.paths,
        id,
        &container_names,
        &image_dir,
        RestoreSpecValidation::Warning,
        runsc_network_mode,
    )
    .await
    {
        cleanup.cleanup_now().await;
        return Err(err.into());
    }

    match running_record(state, id).await {
        Ok(record) => {
            cleanup.disarm();
            Ok(record)
        }
        Err(err) => {
            cleanup.cleanup_now().await;
            Err(err)
        }
    }
}

/// Marks a sandbox `Running`, clears its last error, and returns the record.
async fn running_record(state: &AppState, id: &SandboxId) -> Result<SandboxRecord, AppError> {
    state
        .registry
        .update(&state.paths, id, |record| {
            record.set_status(SandboxStatus::Running);
            record.last_error = None;
        })
        .await?
        .ok_or_else(|| AppError::NotFound(format!("sandbox {id} not found")))
}

/// Persists a sandbox's spec manifest (pause image + container specs) so a
/// later snapshot can rebuild bundles for forks.
async fn persist_sandbox_spec(
    state: &AppState,
    id: &SandboxId,
    spec: &SandboxSpec,
) -> Result<(), AppError> {
    registry::write_json_atomic(&state.paths.sandbox_spec(id), spec)
        .await
        .map_err(AppError::from)
}

/// Reads a sandbox's persisted spec manifest.
async fn read_sandbox_spec(state: &AppState, id: &SandboxId) -> Result<SandboxSpec, AppError> {
    let body = tokio::fs::read(state.paths.sandbox_spec(id)).await?;
    serde_json::from_slice(&body).map_err(|err| {
        AppError::Io(std::io::Error::other(format!(
            "invalid sandbox spec: {err}"
        )))
    })
}

fn effective_fresh_network(
    state: &AppState,
    request: &CreateSandboxRequest,
) -> Result<SandboxNetworkSpec, AppError> {
    let network = request.network.clone().unwrap_or_default();
    validate_network_available(state, &network)?;
    network::validate_spec(&network)?;
    Ok(network)
}

fn effective_fork_network(
    state: &AppState,
    requested: Option<SandboxNetworkSpec>,
    inherited: &SandboxNetworkSpec,
) -> Result<SandboxNetworkSpec, AppError> {
    let network = requested.unwrap_or_else(|| inherited.clone());
    validate_network_available(state, &network)?;
    network::validate_spec(&network)?;
    Ok(network)
}

/// Rejects any sandbox whose effective network policy needs managed networking
/// to enforce it while managed networking is disabled. We check the *resolved*
/// spec (not merely whether the request set one) so a restrictive policy
/// inherited from a snapshot can't silently run with unrestricted egress.
fn validate_network_available(
    state: &AppState,
    network: &SandboxNetworkSpec,
) -> Result<(), AppError> {
    if !state.network.enabled() && *network != SandboxNetworkSpec::default() {
        return Err(AppError::BadRequest(
            "per-sandbox network policy requires managed networking; remove runtime.network_namespace or enable runtime.network.enabled".to_string(),
        ));
    }
    Ok(())
}

const fn runsc_network_mode(
    state: &AppState,
    network_namespace: Option<&std::path::Path>,
) -> RunscNetworkMode {
    if state.network.enabled() {
        return match state.network.backend() {
            ManagedNetworkBackend::Hostinet => RunscNetworkMode::Host,
            ManagedNetworkBackend::Netstack => RunscNetworkMode::Sandbox,
        };
    }
    if network_namespace.is_some() {
        RunscNetworkMode::Host
    } else {
        RunscNetworkMode::Sandbox
    }
}

fn runtime_supports_checkpoints(state: &AppState) -> bool {
    if state.network.enabled() {
        return matches!(state.network.backend(), ManagedNetworkBackend::Netstack);
    }
    state.config.runtime.network_namespace.is_none()
}

fn hostinet_checkpoint_unsupported_message() -> String {
    "checkpoint, suspend, and live snapshot require managed network backend \"netstack\" or default runsc sandbox networking".to_string()
}

async fn prepare_sandbox_network(
    state: &AppState,
    id: &SandboxId,
    network: &SandboxNetworkSpec,
) -> Result<Option<std::path::PathBuf>, AppError> {
    if state.network.enabled() {
        return Ok(state
            .network
            .reconcile_sandbox(&state.paths, id, network)
            .await?);
    }
    Ok(state.config.runtime.network_namespace.clone())
}

async fn prepare_bundles(
    state: &AppState,
    id: &SandboxId,
    pause_image: &str,
    containers: &[ContainerSpec],
    network_namespace: Option<&std::path::Path>,
) -> Result<(), AppError> {
    let resolv_conf = state
        .network
        .enabled()
        .then(|| state.paths.sandbox_resolv_conf(id));
    state
        .gvisor
        .prepare_pause_bundle(
            &state.paths,
            id,
            pause_image,
            network_namespace,
            resolv_conf.as_deref(),
        )
        .await?;

    for container in containers {
        state
            .gvisor
            .prepare_container_bundle(
                &state.paths,
                id,
                container,
                network_namespace,
                resolv_conf.as_deref(),
                &state.config.runtime.static_mounts,
            )
            .await?;
    }
    Ok(())
}

/// Best-effort teardown after a failed create: stop any containers that were
/// started, delete the sandbox directory, and drop the in-memory record so the
/// id can be reused. Each step is independent and its failure is logged rather
/// than propagated, since the caller is already returning the original error.
async fn cleanup_failed_sandbox(
    state: &AppState,
    id: &SandboxId,
    container_names: &[String],
    delete_containers: bool,
) {
    let mut cleanup_failures = Vec::new();
    if delete_containers {
        match delete_visible_container_sequence(&state.paths, id, container_names).await {
            Ok(failures) => {
                for (container, error) in failures {
                    error!(
                        %id,
                        container,
                        error,
                        "failed to stop container while cleaning up failed sandbox"
                    );
                    cleanup_failures.push(format!("{container}: {error}"));
                }
            }
            Err(err) => {
                error!(%id, %err, "failed to stop containers while cleaning up failed sandbox");
                cleanup_failures.push(err.to_string());
            }
        }
    }

    if !cleanup_failures.is_empty() {
        let message = format!(
            "cleanup failed after create error; sandbox retained: {}",
            cleanup_failures.join("; ")
        );
        if let Err(err) = state
            .registry
            .update(&state.paths, id, |record| record.set_error(message.clone()))
            .await
        {
            error!(%id, %err, "failed to persist failed sandbox cleanup status");
        }
        return;
    }

    if let Err(err) = state.network.delete_sandbox(&state.paths, id).await {
        error!(%id, %err, "failed to remove sandbox network while cleaning up failed sandbox");
    }

    if let Err(err) = tokio::fs::remove_dir_all(state.paths.sandbox_dir(id)).await
        && err.kind() != std::io::ErrorKind::NotFound
    {
        error!(%id, %err, "failed to remove directory of failed sandbox");
    }
    state.registry.remove(id).await;
}

struct SandboxCreateGuard {
    state: AppState,
    id: SandboxId,
    containers: Vec<String>,
    delete_containers: bool,
    active: bool,
}

impl SandboxCreateGuard {
    fn new(state: &AppState, id: &SandboxId, containers: Vec<String>) -> Self {
        Self {
            state: state.clone(),
            id: id.clone(),
            containers,
            delete_containers: false,
            active: true,
        }
    }

    const fn delete_containers(&mut self) {
        self.delete_containers = true;
    }

    const fn disarm(&mut self) {
        self.active = false;
    }

    async fn cleanup_now(mut self) {
        cleanup_failed_sandbox(
            &self.state,
            &self.id,
            &self.containers,
            self.delete_containers,
        )
        .await;
        self.active = false;
    }
}

impl Drop for SandboxCreateGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let state = self.state.clone();
        let id = self.id.clone();
        let containers = self.containers.clone();
        let delete_containers = self.delete_containers;
        tokio::spawn(async move {
            let _guard = state.registry.acquire_lifecycle(&id).await;
            cleanup_failed_sandbox(&state, &id, &containers, delete_containers).await;
        });
    }
}

/// Fetch a single sandbox by id.
#[utoipa::path(
    get,
    path = "/v1/sandboxes/{id}",
    params(("id" = String, Path, description = "Sandbox id")),
    responses(
        (status = 200, description = "Sandbox record", body = SandboxResponse),
        (status = 400, description = "Invalid sandbox id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Sandbox not found", body = ErrorResponse),
    ),
    security(("bearer_token" = [])),
    tag = "sandboxes",
)]
async fn get_sandbox(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<SandboxResponse>, AppError> {
    let id = parse_sandbox_id(id)?;
    let sandbox = state
        .registry
        .get(&id)
        .await
        .ok_or_else(|| AppError::NotFound(format!("sandbox {id} not found")))?;
    Ok(Json(SandboxResponse { sandbox }))
}

/// Stop and delete a sandbox and all of its containers.
#[utoipa::path(
    delete,
    path = "/v1/sandboxes/{id}",
    params(("id" = String, Path, description = "Sandbox id")),
    responses(
        (status = 200, description = "Sandbox stopped and deleted", body = SandboxResponse),
        (status = 400, description = "Invalid sandbox id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Sandbox not found", body = ErrorResponse),
        (status = 500, description = "Failed to delete sandbox", body = ErrorResponse),
    ),
    security(("bearer_token" = [])),
    tag = "sandboxes",
)]
async fn delete_sandbox(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<SandboxResponse>, AppError> {
    let id = parse_sandbox_id(id)?;
    let _guard = state.registry.acquire_lifecycle(&id).await;

    let current = state
        .registry
        .get(&id)
        .await
        .ok_or_else(|| AppError::NotFound(format!("sandbox {id} not found")))?;
    state.background_execs.kill_for_sandbox(id.as_str()).await;

    state
        .registry
        .update(&state.paths, &id, |record| {
            record.set_status(SandboxStatus::Stopping);
        })
        .await?;

    // A suspended sandbox has no live containers to tear down — its state lives
    // only in the checkpoint image, which the directory removal below reclaims.
    let failures = if matches!(current.status, SandboxStatus::Suspended) {
        Vec::new()
    } else {
        delete_visible_container_sequence(&state.paths, &id, &current.containers).await?
    };

    if !failures.is_empty() {
        let message = format!("delete failures: {failures:?}");
        state
            .registry
            .update(&state.paths, &id, |record| {
                record.set_error(message.clone());
            })
            .await?;
        error!(%id, ?failures, "sandbox delete left containers running; retaining state");
        return Err(AppError::TeardownFailed(message));
    }

    if let Err(err) = state.network.delete_sandbox(&state.paths, &id).await {
        let message = format!("failed to delete sandbox network: {err}");
        state
            .registry
            .update(&state.paths, &id, |record| {
                record.set_error(message.clone());
            })
            .await?;
        return Err(err.into());
    }

    // Remove the sandbox directory (state.json, bundles, EROFS images, logs) so
    // its disk footprint is freed and the id can be recreated later.
    match tokio::fs::remove_dir_all(state.paths.sandbox_dir(&id)).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            let message = format!("failed to remove sandbox directory: {err}");
            state
                .registry
                .update(&state.paths, &id, |record| {
                    record.set_error(message.clone());
                })
                .await?;
            return Err(err.into());
        }
    }

    let mut record = state.registry.remove(&id).await.unwrap_or(current);
    record.set_status(SandboxStatus::Stopped);
    record.last_error = None;

    Ok(Json(SandboxResponse { sandbox: record }))
}

/// Return managed-network addresses for a sandbox.
#[utoipa::path(
    get,
    path = "/v1/sandboxes/{id}/network",
    params(("id" = String, Path, description = "Sandbox id")),
    responses(
        (status = 200, description = "Sandbox managed-network addresses", body = SandboxNetworkResponse),
        (status = 400, description = "Invalid sandbox id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Sandbox or network state not found", body = ErrorResponse),
        (status = 409, description = "Managed networking is disabled", body = ErrorResponse),
    ),
    security(("bearer_token" = [])),
    tag = "sandboxes",
)]
async fn get_sandbox_network(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<SandboxNetworkResponse>, AppError> {
    let id = parse_sandbox_id(id)?;
    if !state.registry.contains(&id).await {
        return Err(AppError::NotFound(format!("sandbox {id} not found")));
    }
    if !state.network.enabled() {
        return Err(AppError::Conflict(
            "managed networking is disabled".to_string(),
        ));
    }
    let info = state
        .network
        .sandbox_info(&state.paths, &id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("network state for sandbox {id} not found")))?;

    Ok(Json(SandboxNetworkResponse {
        sandbox_id: id.to_string(),
        host_gateway_ip: info.host_gateway_ip.to_string(),
        sandbox_ip: info.sandbox_ip.to_string(),
    }))
}

fn parse_sandbox_id(id: String) -> Result<SandboxId, AppError> {
    SandboxId::new(id).map_err(|err| AppError::BadRequest(err.to_string()))
}

/// Picks the default container to act on: the first non-pause container, falling
/// back to the first container overall.
fn default_log_container(record: &SandboxRecord) -> Option<String> {
    record
        .containers
        .iter()
        .find(|name| name.as_str() != PAUSE_CONTAINER)
        .or_else(|| record.containers.first())
        .cloned()
}

async fn resolve_running_container(
    state: &AppState,
    record: &SandboxRecord,
    id: &SandboxId,
    requested: Option<String>,
) -> Result<String, AppError> {
    if !matches!(record.status, SandboxStatus::Running) {
        return Err(AppError::Conflict(format!(
            "sandbox {id} is {:?}, not running",
            record.status
        )));
    }

    let container = requested
        .or_else(|| default_log_container(record))
        .ok_or_else(|| AppError::NotFound(format!("sandbox {id} has no containers")))?;
    if !record.containers.iter().any(|known| known == &container) {
        return Err(AppError::NotFound(format!(
            "container {container:?} not found in sandbox {id}"
        )));
    }
    if !oad_runtime::container_running(&state.paths, id, &container).await {
        return Err(AppError::Conflict(format!(
            "container {container:?} in sandbox {id} is not running"
        )));
    }
    Ok(container)
}

/// Render exec environment variables as `KEY=VALUE` strings for the runtime.
fn render_env(env: &[EnvVar]) -> Vec<String> {
    env.iter()
        .map(|var| format!("{}={}", var.name, var.value))
        .collect()
}

/// Run a one-off command inside a running container and return its output.
#[utoipa::path(
    post,
    path = "/v1/sandboxes/{id}/exec",
    params(("id" = String, Path, description = "Sandbox id")),
    request_body = ExecRequest,
    responses(
        (status = 200, description = "Command executed; output captured", body = ExecResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Sandbox or container not found", body = ErrorResponse),
        (status = 500, description = "Failed to execute command", body = ErrorResponse),
    ),
    security(("bearer_token" = [])),
    tag = "sandboxes",
)]
async fn exec_in_sandbox(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<ExecRequest>,
) -> Result<Json<ExecResponse>, AppError> {
    let id = parse_sandbox_id(id)?;
    if request.command.is_empty() {
        return Err(AppError::BadRequest(
            "command must not be empty".to_string(),
        ));
    }
    let _guard = state.registry.acquire_lifecycle(&id).await;
    let record = state
        .registry
        .get(&id)
        .await
        .ok_or_else(|| AppError::NotFound(format!("sandbox {id} not found")))?;
    // Transparently resume a suspended sandbox before running the command.
    let record = ensure_running(&state, &id, record).await?;
    let container =
        resolve_running_container(&state, &record, &id, request.container.clone()).await?;

    let env = render_env(&request.env);
    let output = oad_runtime::exec_in_container(
        &state.paths,
        &id,
        &container,
        &request.command,
        &env,
        request.cwd.as_deref(),
    )
    .await?;

    Ok(Json(ExecResponse {
        sandbox_id: id.to_string(),
        container,
        exit_code: output.exit_code,
        stdout: output.stdout,
        stderr: output.stderr,
    }))
}

/// Start a long-running command inside a container and keep its streams
/// attached to the daemon for later control.
#[utoipa::path(
    post,
    path = "/v1/sandboxes/{id}/execs",
    params(("id" = String, Path, description = "Sandbox id")),
    request_body = StartBackgroundExecRequest,
    responses(
        (status = 201, description = "Background exec started", body = BackgroundExecResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Sandbox or container not found", body = ErrorResponse),
        (status = 409, description = "Sandbox or container is not running", body = ErrorResponse),
        (status = 500, description = "Failed to start background exec", body = ErrorResponse),
    ),
    security(("bearer_token" = [])),
    tag = "sandboxes",
)]
async fn start_background_exec(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<StartBackgroundExecRequest>,
) -> Result<(StatusCode, Json<BackgroundExecResponse>), AppError> {
    let id = parse_sandbox_id(id)?;
    if request.command.is_empty() {
        return Err(AppError::BadRequest(
            "command must not be empty".to_string(),
        ));
    }
    if request.pty && (request.rows == Some(0) || request.cols == Some(0)) {
        return Err(AppError::BadRequest(
            "PTY rows and cols must be greater than zero".to_string(),
        ));
    }
    let _guard = state.registry.acquire_lifecycle(&id).await;
    let record = state
        .registry
        .get(&id)
        .await
        .ok_or_else(|| AppError::NotFound(format!("sandbox {id} not found")))?;
    let record = ensure_running(&state, &id, record).await?;
    let container =
        resolve_running_container(&state, &record, &id, request.container.clone()).await?;

    let env = render_env(&request.env);
    let exec_id = uuid::Uuid::new_v4().to_string();
    let log_path = state
        .paths
        .logs_dir(&id)
        .join(format!("exec-{exec_id}.jsonl"));
    let info = BackgroundExecInfo {
        id: exec_id,
        sandbox_id: id.to_string(),
        container,
        command: request.command,
        pty: request.pty,
        status: BackgroundExecStatus::Running,
        exit_code: None,
        last_error: None,
    };
    let session = if request.pty {
        let process = oad_runtime::spawn_pty_exec_in_container(
            &state.paths,
            &id,
            oad_runtime::PtyExecConfig {
                container: &info.container,
                command: &info.command,
                env: &env,
                cwd: request.cwd.as_deref(),
                log_path: Some(&log_path),
                size: oad_runtime::PtySizeSpec {
                    rows: request.rows.unwrap_or(DEFAULT_PTY_ROWS),
                    cols: request.cols.unwrap_or(DEFAULT_PTY_COLS),
                },
            },
        )
        .await?;
        state.background_execs.insert_pty(info, process).await
    } else {
        let process = oad_runtime::spawn_exec_in_container(
            &state.paths,
            &id,
            &info.container,
            &info.command,
            &env,
            request.cwd.as_deref(),
            Some(&log_path),
        )
        .await?;
        state.background_execs.insert(info, process).await
    };

    Ok((
        StatusCode::CREATED,
        Json(BackgroundExecResponse {
            exec: session.info().await,
        }),
    ))
}

/// Fetch a single background exec session.
#[utoipa::path(
    get,
    path = "/v1/sandboxes/{id}/execs/{exec_id}",
    params(
        ("id" = String, Path, description = "Sandbox id"),
        ("exec_id" = String, Path, description = "Background exec session id"),
    ),
    responses(
        (status = 200, description = "Background exec session", body = BackgroundExecResponse),
        (status = 400, description = "Invalid sandbox id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Sandbox or background exec not found", body = ErrorResponse),
    ),
    security(("bearer_token" = [])),
    tag = "sandboxes",
)]
async fn get_background_exec(
    State(state): State<AppState>,
    AxumPath((id, exec_id)): AxumPath<(String, String)>,
) -> Result<Json<BackgroundExecResponse>, AppError> {
    let id = parse_sandbox_id(id)?;
    let session = background_exec_session(&state, &id, &exec_id).await?;
    Ok(Json(BackgroundExecResponse {
        exec: session.info().await,
    }))
}

/// Kill a running background exec session.
#[utoipa::path(
    delete,
    path = "/v1/sandboxes/{id}/execs/{exec_id}",
    params(
        ("id" = String, Path, description = "Sandbox id"),
        ("exec_id" = String, Path, description = "Background exec session id"),
    ),
    responses(
        (status = 200, description = "Background exec kill requested", body = BackgroundExecResponse),
        (status = 400, description = "Invalid sandbox id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Sandbox or background exec not found", body = ErrorResponse),
    ),
    security(("bearer_token" = [])),
    tag = "sandboxes",
)]
async fn kill_background_exec(
    State(state): State<AppState>,
    AxumPath((id, exec_id)): AxumPath<(String, String)>,
) -> Result<Json<BackgroundExecResponse>, AppError> {
    let id = parse_sandbox_id(id)?;
    let session = background_exec_session(&state, &id, &exec_id).await?;
    if session.kill().await {
        let _ = tokio::time::timeout(
            BACKGROUND_EXEC_KILL_RESPONSE_TIMEOUT,
            session.wait_finished(),
        )
        .await;
    }
    Ok(Json(BackgroundExecResponse {
        exec: session.info().await,
    }))
}

/// Write bytes to a background exec session's stdin.
#[utoipa::path(
    post,
    path = "/v1/sandboxes/{id}/execs/{exec_id}/stdin",
    params(
        ("id" = String, Path, description = "Sandbox id"),
        ("exec_id" = String, Path, description = "Background exec session id"),
    ),
    request_body = BackgroundExecStdinRequest,
    responses(
        (status = 200, description = "Stdin write accepted", body = BackgroundExecStdinResponse),
        (status = 400, description = "Invalid sandbox id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Sandbox or background exec not found", body = ErrorResponse),
        (status = 409, description = "Background exec stdin is closed", body = ErrorResponse),
        (status = 500, description = "Failed to write stdin", body = ErrorResponse),
    ),
    security(("bearer_token" = [])),
    tag = "sandboxes",
)]
async fn write_background_exec_stdin(
    State(state): State<AppState>,
    AxumPath((id, exec_id)): AxumPath<(String, String)>,
    Json(request): Json<BackgroundExecStdinRequest>,
) -> Result<Json<BackgroundExecStdinResponse>, AppError> {
    let id = parse_sandbox_id(id)?;
    if request.data.is_empty() && !request.close {
        return Err(AppError::BadRequest(
            "stdin request must include data or close=true".to_string(),
        ));
    }
    let session = background_exec_session(&state, &id, &exec_id).await?;
    let accepted = session
        .write_stdin(&request.data, request.close)
        .await
        .map_err(AppError::Io)?;
    if !accepted {
        return Err(AppError::Conflict(format!(
            "stdin for background exec {exec_id} is closed"
        )));
    }
    Ok(Json(BackgroundExecStdinResponse { accepted }))
}

/// Stream background exec session events as Server-Sent Events.
#[utoipa::path(
    get,
    path = "/v1/sandboxes/{id}/execs/{exec_id}/events",
    params(
        ("id" = String, Path, description = "Sandbox id"),
        ("exec_id" = String, Path, description = "Background exec session id"),
        BackgroundExecEventsQuery,
    ),
    responses(
        (status = 200, description = "SSE stream of background exec events"),
        (status = 400, description = "Invalid sandbox id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Sandbox or background exec not found", body = ErrorResponse),
    ),
    security(("bearer_token" = [])),
    tag = "sandboxes",
)]
async fn stream_background_exec_events(
    State(state): State<AppState>,
    AxumPath((id, exec_id)): AxumPath<(String, String)>,
    Query(query): Query<BackgroundExecEventsQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, AppError> {
    let id = parse_sandbox_id(id)?;
    let session = background_exec_session(&state, &id, &exec_id).await?;
    let from = query.from.unwrap_or(1);
    let receiver = session.subscribe();
    let replay = session.events_since(from).await;
    let replay_terminal = replay.last().is_some_and(|event| event.event.is_terminal());
    let next_live_sequence = replay
        .last()
        .map_or(from, |event| event.sequence.saturating_add(1));
    let live = if replay_terminal {
        stream::empty::<BackgroundExecEvent>().left_stream()
    } else {
        stream::unfold(
            (
                session.clone(),
                receiver,
                next_live_sequence,
                VecDeque::new(),
                false,
            ),
            |(session, mut receiver, mut next_sequence, mut pending, done): (
                Arc<background_exec::BackgroundExecSession>,
                tokio::sync::broadcast::Receiver<BackgroundExecEvent>,
                u64,
                VecDeque<BackgroundExecEvent>,
                bool,
            )| async move {
                if done {
                    return None;
                }
                loop {
                    // Drain any events recovered from the in-memory buffer after
                    // a lag before going back to the live channel.
                    if let Some(event) = pending.pop_front() {
                        if event.sequence < next_sequence {
                            continue;
                        }
                        next_sequence = event.sequence.saturating_add(1);
                        let done = event.event.is_terminal();
                        return Some((event, (session, receiver, next_sequence, pending, done)));
                    }
                    match receiver.recv().await {
                        Ok(event) if event.sequence < next_sequence => {}
                        Ok(event) => {
                            next_sequence = event.sequence.saturating_add(1);
                            let done = event.event.is_terminal();
                            return Some((
                                event,
                                (session, receiver, next_sequence, pending, done),
                            ));
                        }
                        // A slow consumer fell behind the broadcast buffer.
                        // Rather than silently dropping events, refill from the
                        // session's retained buffer; only events already evicted
                        // from that buffer are unrecoverable.
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            pending = session.events_since(next_sequence).await.into();
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                    }
                }
            },
        )
        .right_stream()
    };
    let events = stream::iter(replay).chain(live).map(|event| {
        Ok(Event::default()
            .event(background_exec_event_name(&event.event))
            .id(event.sequence.to_string())
            .json_data(event)
            .expect("background exec event serializes to JSON"))
    });

    Ok(Sse::new(events).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

#[derive(Debug, Clone, serde::Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
struct BackgroundExecEventsQuery {
    /// First event sequence to replay before live streaming (default 1).
    from: Option<u64>,
}

async fn background_exec_session(
    state: &AppState,
    id: &SandboxId,
    exec_id: &str,
) -> Result<Arc<background_exec::BackgroundExecSession>, AppError> {
    if !state.registry.contains(id).await {
        return Err(AppError::NotFound(format!("sandbox {id} not found")));
    }
    let session = state
        .background_execs
        .get(exec_id)
        .await
        .ok_or_else(|| AppError::NotFound(format!("background exec {exec_id} not found")))?;
    let info = session.info().await;
    if info.sandbox_id != id.as_str() {
        return Err(AppError::NotFound(format!(
            "background exec {exec_id} not found in sandbox {id}"
        )));
    }
    Ok(session)
}

const fn background_exec_event_name(event: &BackgroundExecEventKind) -> &'static str {
    match event {
        BackgroundExecEventKind::Stdout { .. } => "stdout",
        BackgroundExecEventKind::Stderr { .. } => "stderr",
        BackgroundExecEventKind::Exited { .. } => "exited",
        BackgroundExecEventKind::Failed { .. } => "failed",
    }
}

/// Suspend a running sandbox: checkpoint it to disk and tear down its
/// containers, freeing memory while preserving in-container state.
#[utoipa::path(
    post,
    path = "/v1/sandboxes/{id}/suspend",
    params(("id" = String, Path, description = "Sandbox id")),
    responses(
        (status = 200, description = "Sandbox suspended", body = SandboxResponse),
        (status = 400, description = "Invalid sandbox id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Sandbox not found", body = ErrorResponse),
        (status = 409, description = "Sandbox is not running", body = ErrorResponse),
        (status = 500, description = "Failed to checkpoint sandbox", body = ErrorResponse),
    ),
    security(("bearer_token" = [])),
    tag = "sandboxes",
)]
async fn suspend_sandbox(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<SandboxResponse>, AppError> {
    let id = parse_sandbox_id(id)?;
    let _guard = state.registry.acquire_lifecycle(&id).await;
    let record = state
        .registry
        .get(&id)
        .await
        .ok_or_else(|| AppError::NotFound(format!("sandbox {id} not found")))?;
    if !matches!(record.status, SandboxStatus::Running) {
        return Err(AppError::Conflict(format!(
            "sandbox {id} is {:?}, not running",
            record.status
        )));
    }
    if !runtime_supports_checkpoints(&state) {
        return Err(AppError::Conflict(hostinet_checkpoint_unsupported_message()));
    }
    state.background_execs.kill_for_sandbox(id.as_str()).await;

    let record = suspend_locked(&state, &id, &record).await?;
    Ok(Json(SandboxResponse { sandbox: record }))
}

/// Checkpoints a running sandbox and flips its record to `Suspended`.
///
/// The caller must already hold the sandbox's lifecycle lock.
async fn suspend_locked(
    state: &AppState,
    id: &SandboxId,
    record: &SandboxRecord,
) -> Result<SandboxRecord, AppError> {
    let image_dir = state.paths.checkpoint_dir(id);
    if let Err(err) = checkpoint_sandbox(&state.paths, id, &record.containers, &image_dir).await {
        let checkpoint_available = checkpoint_image_complete_for_containers(
            &state.paths.checkpoint_dir(id),
            &record.containers,
        )
        .await;
        let message = format!("checkpoint failed: {err}");
        let status = if checkpoint_available {
            SandboxStatus::Suspended
        } else if oad_runtime::container_running(&state.paths, id, PAUSE_CONTAINER).await {
            SandboxStatus::Running
        } else {
            SandboxStatus::Unknown
        };
        if let Err(persist_err) = state
            .registry
            .update(&state.paths, id, |record| {
                record.set_status(status.clone());
                record.last_error = Some(message.clone());
            })
            .await
        {
            error!(
                sandbox_id = %id,
                %persist_err,
                "failed to persist sandbox status after checkpoint failure"
            );
        }
        return Err(err.into());
    }

    state
        .registry
        .update(&state.paths, id, |record| {
            record.set_status(SandboxStatus::Suspended);
            record.last_error = None;
        })
        .await?
        .ok_or_else(|| AppError::NotFound(format!("sandbox {id} not found")))
}

/// Resume a suspended sandbox by restoring it from its checkpoint image.
#[utoipa::path(
    post,
    path = "/v1/sandboxes/{id}/resume",
    params(("id" = String, Path, description = "Sandbox id")),
    responses(
        (status = 200, description = "Sandbox resumed", body = SandboxResponse),
        (status = 400, description = "Invalid sandbox id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Sandbox not found", body = ErrorResponse),
        (status = 409, description = "Sandbox is not suspended", body = ErrorResponse),
        (status = 500, description = "Failed to restore sandbox", body = ErrorResponse),
    ),
    security(("bearer_token" = [])),
    tag = "sandboxes",
)]
async fn resume_sandbox(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<SandboxResponse>, AppError> {
    let id = parse_sandbox_id(id)?;
    let _guard = state.registry.acquire_lifecycle(&id).await;
    let record = state
        .registry
        .get(&id)
        .await
        .ok_or_else(|| AppError::NotFound(format!("sandbox {id} not found")))?;
    if !matches!(record.status, SandboxStatus::Suspended) {
        return Err(AppError::Conflict(format!(
            "sandbox {id} is {:?}, not suspended",
            record.status
        )));
    }

    let record = resume_locked(&state, &id, &record).await?;
    Ok(Json(SandboxResponse { sandbox: record }))
}

/// Restores a suspended sandbox and flips its status to `Running`.
///
/// The caller must already hold the sandbox's lifecycle lock; that lock
/// deduplicates concurrent transparent resumes (the second caller observes
/// `Running` once the first finishes).
async fn resume_locked(
    state: &AppState,
    id: &SandboxId,
    record: &SandboxRecord,
) -> Result<SandboxRecord, AppError> {
    let spec = read_sandbox_spec(state, id).await?;
    let network_namespace = prepare_sandbox_network(state, id, &spec.network).await?;
    let image_dir = state.paths.checkpoint_dir(id);
    // Replay the network mode the checkpoint was taken with. Recomputing it from
    // current daemon config would mismatch the checkpoint if the config changed
    // since create/suspend; fall back only for specs persisted before the field
    // existed.
    let network_mode = spec.network_mode.map_or_else(
        || runsc_network_mode(state, network_namespace.as_deref()),
        RunscNetworkMode::from,
    );
    if let Err(err) = restore_sandbox(
        &state.paths,
        id,
        &record.containers,
        &image_dir,
        RestoreSpecValidation::Enforce,
        network_mode,
    )
    .await
    {
        cleanup_partial_resume(state, id, record, &err).await;
        return Err(err.into());
    }
    let record = running_record(state, id).await?;
    if let Err(err) = tokio::fs::remove_dir_all(&image_dir).await
        && err.kind() != std::io::ErrorKind::NotFound
    {
        warn!(
            sandbox_id = %id,
            error = %err,
            path = %image_dir.display(),
            "failed to remove consumed checkpoint image"
        );
    }
    Ok(record)
}

async fn cleanup_partial_resume(
    state: &AppState,
    id: &SandboxId,
    record: &SandboxRecord,
    restore_error: &oad_runtime::RuntimeError,
) {
    match delete_visible_container_sequence(&state.paths, id, &record.containers).await {
        Ok(failures) if failures.is_empty() => {
            warn!(
                sandbox_id = %id,
                error = %restore_error,
                "resume failed; cleaned up any partially restored containers"
            );
        }
        Ok(failures) => {
            let message = format!(
                "resume failed and partial restore cleanup failed: restore={restore_error}; cleanup={failures:?}"
            );
            error!(
                sandbox_id = %id,
                ?failures,
                error = %restore_error,
                "resume failed and partial restore cleanup left containers behind"
            );
            if let Err(err) = state
                .registry
                .update(&state.paths, id, |record| record.set_error(message.clone()))
                .await
            {
                error!(sandbox_id = %id, %err, "failed to persist failed resume cleanup status");
            }
        }
        Err(cleanup_error) => {
            let message = format!(
                "resume failed and partial restore cleanup failed: restore={restore_error}; cleanup={cleanup_error}"
            );
            error!(
                sandbox_id = %id,
                error = %restore_error,
                %cleanup_error,
                "resume failed and partial restore cleanup failed"
            );
            if let Err(err) = state
                .registry
                .update(&state.paths, id, |record| record.set_error(message.clone()))
                .await
            {
                error!(sandbox_id = %id, %err, "failed to persist failed resume cleanup status");
            }
        }
    }
}

/// Returns the record, transparently resuming the sandbox first if it is
/// suspended. Must be called while holding the sandbox's lifecycle lock.
async fn ensure_running(
    state: &AppState,
    id: &SandboxId,
    record: SandboxRecord,
) -> Result<SandboxRecord, AppError> {
    if matches!(record.status, SandboxStatus::Suspended) {
        info!(%id, "auto-resuming suspended sandbox on demand");
        return resume_locked(state, id, &record).await;
    }
    Ok(record)
}

/// Capture a snapshot of a sandbox as a forkable image. The snapshot's
/// containers and pause image come from the source sandbox's persisted spec.
///
/// A running sandbox is checkpointed live with `--leave-running`, so it keeps
/// executing. A suspended sandbox already has a checkpoint image on disk, which
/// is reused directly, leaving the source suspended.
#[utoipa::path(
    post,
    path = "/v1/sandboxes/{id}/snapshot",
    params(("id" = String, Path, description = "Source sandbox id")),
    request_body = CreateSnapshotRequest,
    responses(
        (status = 201, description = "Snapshot created", body = SnapshotResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Sandbox not found", body = ErrorResponse),
        (status = 409, description = "Sandbox not running/suspended or snapshot exists", body = ErrorResponse),
        (status = 500, description = "Failed to capture snapshot", body = ErrorResponse),
    ),
    security(("bearer_token" = [])),
    tag = "snapshots",
)]
async fn snapshot_sandbox_handler(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<CreateSnapshotRequest>,
) -> Result<(StatusCode, Json<SnapshotResponse>), AppError> {
    let id = parse_sandbox_id(id)?;
    let name = match request.name {
        Some(name) => {
            validate_snapshot_name(&name).map_err(|err| AppError::BadRequest(err.to_string()))?;
            name
        }
        // No name supplied: generate a unique, path-safe one.
        None => format!("snap-{}", uuid::Uuid::new_v4()),
    };

    let _guard = state.registry.acquire_lifecycle(&id).await;
    let record = state
        .registry
        .get(&id)
        .await
        .ok_or_else(|| AppError::NotFound(format!("sandbox {id} not found")))?;
    if !matches!(
        record.status,
        SandboxStatus::Running | SandboxStatus::Suspended
    ) {
        return Err(AppError::Conflict(format!(
            "sandbox {id} is {:?}; must be running or suspended to snapshot",
            record.status
        )));
    }
    let spec = read_sandbox_spec(&state, &id).await?;

    let _snapshot_guard = state.snapshot_locks.acquire(&name).await;
    if !snapshots::reserve(&state.paths, &name).await? {
        return Err(AppError::Conflict(format!(
            "snapshot {name} already exists"
        )));
    }
    let mut snapshot_cleanup = SnapshotReservationGuard::new(&state, name.clone());

    let image_dir = state.paths.snapshot_checkpoint_dir(&name);
    let captured: Result<(), AppError> = match record.status {
        SandboxStatus::Running if runtime_supports_checkpoints(&state) => {
            snapshot_sandbox(&state.paths, &id, &record.containers, &image_dir)
                .await
                .map_err(Into::into)
        }
        SandboxStatus::Running => {
            Err(AppError::Conflict(hostinet_checkpoint_unsupported_message()))
        }
        _ => copy_checkpoint_image(
            &state.paths.checkpoint_dir(&id),
            &image_dir,
            &record.containers,
        )
        .await
        .map_err(Into::into),
    };
    if let Err(err) = captured {
        snapshot_cleanup.cleanup_now().await;
        return Err(err);
    }

    let manifest =
        snapshots::SnapshotManifest::new(name, spec.pause_image, spec.containers, spec.network);
    if let Err(err) = snapshots::write_manifest(&state.paths, &manifest).await {
        snapshot_cleanup.cleanup_now().await;
        return Err(err.into());
    }
    snapshot_cleanup.disarm();

    // Publish the snapshot to the content-addressed store so it becomes portable
    // across nodes. A publish failure is non-fatal: the snapshot is still valid
    // locally, it just stays node-local (no portable descriptor).
    let cas = match &state.cas {
        Some(publisher) => match publisher.publish_snapshot(&state.paths, &manifest).await {
            Ok(outcome) => Some(CasSnapshotInfo {
                descriptor_key: outcome.descriptor_key,
                total_bytes: outcome.total_bytes,
                uploaded_bytes: outcome.uploaded_bytes,
                chunk_hashes: outcome.chunk_hashes,
            }),
            Err(err) => {
                warn!(
                    error = %err,
                    snapshot = %manifest.name,
                    "CAS publish failed; snapshot remains node-local"
                );
                None
            }
        },
        None => None,
    };

    let mut info = snapshot_info(&manifest);
    info.cas = cas;
    Ok((
        StatusCode::CREATED,
        Json(SnapshotResponse { snapshot: info }),
    ))
}

/// List all stored snapshots.
#[utoipa::path(
    get,
    path = "/v1/snapshots",
    responses(
        (status = 200, description = "Known snapshots", body = ListSnapshotsResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
    ),
    security(("bearer_token" = [])),
    tag = "snapshots",
)]
async fn list_snapshots(
    State(state): State<AppState>,
) -> Result<Json<ListSnapshotsResponse>, AppError> {
    let snapshots = snapshots::list(&state.paths)
        .await
        .iter()
        .map(snapshot_info)
        .collect();
    Ok(Json(ListSnapshotsResponse { snapshots }))
}

/// Delete a snapshot and its checkpoint image.
#[utoipa::path(
    delete,
    path = "/v1/snapshots/{name}",
    params(("name" = String, Path, description = "Snapshot name")),
    responses(
        (status = 204, description = "Snapshot deleted"),
        (status = 400, description = "Invalid snapshot name", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Snapshot not found", body = ErrorResponse),
        (status = 500, description = "Failed to delete snapshot", body = ErrorResponse),
    ),
    security(("bearer_token" = [])),
    tag = "snapshots",
)]
async fn delete_snapshot(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<StatusCode, AppError> {
    validate_snapshot_name(&name).map_err(|err| AppError::BadRequest(err.to_string()))?;
    let _snapshot_guard = state.snapshot_locks.acquire(&name).await;
    if snapshots::delete(&state.paths, &name).await? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(AppError::NotFound(format!("snapshot {name} not found")))
    }
}

fn snapshot_info(manifest: &snapshots::SnapshotManifest) -> SnapshotInfo {
    SnapshotInfo {
        name: manifest.name.clone(),
        containers: manifest.container_names(),
        created_at: manifest.created_at_rfc3339(),
        cas: None,
    }
}

struct SnapshotReservationGuard {
    state: AppState,
    name: String,
    active: bool,
}

impl SnapshotReservationGuard {
    fn new(state: &AppState, name: String) -> Self {
        Self {
            state: state.clone(),
            name,
            active: true,
        }
    }

    const fn disarm(&mut self) {
        self.active = false;
    }

    async fn cleanup_now(mut self) {
        let _ = snapshots::delete(&self.state.paths, &self.name).await;
        self.active = false;
    }
}

impl Drop for SnapshotReservationGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let state = self.state.clone();
        let name = self.name.clone();
        tokio::spawn(async move {
            let _snapshot_guard = state.snapshot_locks.acquire(&name).await;
            let _ = snapshots::delete(&state.paths, &name).await;
        });
    }
}

/// Validates a non-empty container list: each name is legal and unique, and
/// each image is non-empty.
fn validate_containers(containers: &[ContainerSpec]) -> Result<(), AppError> {
    if containers.is_empty() {
        return Err(AppError::BadRequest(
            "containers must not be empty".to_string(),
        ));
    }

    let mut names = BTreeSet::new();
    for container in containers {
        validate_container_name(&container.name)
            .map_err(|err| AppError::BadRequest(err.to_string()))?;
        if !names.insert(container.name.clone()) {
            return Err(AppError::BadRequest(format!(
                "container {:?} is duplicated",
                container.name
            )));
        }
        if container.image.is_empty() {
            return Err(AppError::BadRequest(format!(
                "container {:?} image must not be empty",
                container.name
            )));
        }
    }

    Ok(())
}

/// Bearer-token auth middleware. Rejects the request with `401` unless the
/// `Authorization` header carries the configured token.
async fn require_auth(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<Response, AppError> {
    let expected = format!("Bearer {}", state.config.http.bearer_token);
    let provided = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    if provided.is_some_and(|provided| constant_time_eq(provided.as_bytes(), expected.as_bytes())) {
        Ok(next.run(request).await)
    } else {
        Err(AppError::Unauthorized)
    }
}

/// Compares two byte slices without short-circuiting on the first differing
/// byte, so request timing does not leak how much of the bearer token matched.
/// The length comparison is not itself secret.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

#[derive(Debug, Error)]
enum AppError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    TeardownFailed(String),
    #[error(transparent)]
    Oci(#[from] oad_oci::OciError),
    #[error(transparent)]
    Runtime(#[from] oad_runtime::RuntimeError),
    #[error(transparent)]
    Network(#[from] network::NetworkError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code, message) = match &self {
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "missing or invalid bearer token".to_string(),
            ),
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, "bad_request", message.clone()),
            Self::NotFound(message) => (StatusCode::NOT_FOUND, "not_found", message.clone()),
            Self::Conflict(message) => (StatusCode::CONFLICT, "conflict", message.clone()),
            Self::TeardownFailed(message) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "delete_failed",
                message.clone(),
            ),
            Self::Oci(
                oad_oci::OciError::InvalidReference(_)
                | oad_oci::OciError::InvalidManifest(_)
                | oad_oci::OciError::JsonBody { .. }
                | oad_oci::OciError::NoCommand(_)
                | oad_oci::OciError::PlatformUnavailable(_)
                | oad_oci::OciError::UnsupportedDigest(_)
                | oad_oci::OciError::UnsupportedTarEntry(_)
                | oad_oci::OciError::UnsafeArchivePath(_),
            ) => (StatusCode::BAD_REQUEST, "bad_request", self.to_string()),
            Self::Oci(oad_oci::OciError::BodyTooLarge { .. }) => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                self.to_string(),
            ),
            Self::Network(network::NetworkError::InvalidConfig(_)) => {
                (StatusCode::BAD_REQUEST, "bad_request", self.to_string())
            }
            // Internal-class errors keep their typed source for the log but
            // return a generic message so internals are not leaked to clients.
            Self::Oci(_) | Self::Runtime(_) | Self::Network(_) | Self::Io(_) => {
                error!(error = %self, source_chain = %error_source_chain(&self), "request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    "internal server error".to_string(),
                )
            }
        };
        (status, Json(ErrorResponse::new(code, message))).into_response()
    }
}

fn error_source_chain(error: &dyn StdError) -> String {
    let mut sources = Vec::new();
    let mut current = error.source();
    while let Some(source) = current {
        sources.push(source.to_string());
        current = source.source();
    }
    sources.join(": ")
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(true)
        .init();
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(err) => {
                    error!(%err, "failed to install SIGTERM handler");
                    if let Err(err) = tokio::signal::ctrl_c().await {
                        error!(%err, "failed to install ctrl-c handler");
                    }
                    return;
                }
            };

        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(err) = result {
                    error!(%err, "failed to install ctrl-c handler");
                } else {
                    info!("received SIGINT, shutting down");
                }
            }
            _ = terminate.recv() => {
                info!("received SIGTERM, shutting down");
            }
        }
    }

    #[cfg(not(unix))]
    {
        if let Err(err) = tokio::signal::ctrl_c().await {
            error!(%err, "failed to install ctrl-c handler");
        }
    }
}

async fn checkpoint_running_sandboxes_for_shutdown(state: &AppState) {
    let mut failed = 0usize;

    if !runtime_supports_checkpoints(state) {
        let running = state
            .registry
            .list()
            .await
            .into_iter()
            .filter(|record| matches!(record.status, SandboxStatus::Running))
            .count();
        if running > 0 {
            info!(
                running,
                "skipping daemon-shutdown checkpoints because runsc host networking does not support checkpoint"
            );
        }
        return;
    }

    for recovered in state.registry.list().await {
        if !matches!(recovered.status, SandboxStatus::Running) {
            continue;
        }

        let id = recovered.id.clone();
        let _guard = state.registry.acquire_lifecycle(&id).await;
        let Some(record) = state.registry.get(&id).await else {
            continue;
        };
        if !matches!(record.status, SandboxStatus::Running) {
            continue;
        }

        info!(sandbox_id = %id, "checkpointing running sandbox for daemon shutdown");
        match suspend_locked(state, &id, &record).await {
            Ok(_) => info!(sandbox_id = %id, "sandbox checkpointed for daemon shutdown"),
            Err(err) => {
                failed += 1;
                error!(
                    sandbox_id = %id,
                    %err,
                    "failed to checkpoint sandbox for daemon shutdown"
                );
            }
        }
    }

    if failed > 0 {
        warn!(
            failed,
            "some running sandboxes were not checkpointed during shutdown"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openapi_spec_includes_all_routes() {
        let spec = ApiDoc::openapi();
        for path in [
            "/healthz",
            "/openapi.json",
            "/v1/sandboxes",
            "/v1/sandboxes/{id}",
            "/v1/sandboxes/{id}/exec",
            "/v1/sandboxes/{id}/execs",
            "/v1/sandboxes/{id}/execs/{exec_id}",
            "/v1/sandboxes/{id}/execs/{exec_id}/stdin",
            "/v1/sandboxes/{id}/execs/{exec_id}/events",
            "/v1/sandboxes/{id}/suspend",
            "/v1/sandboxes/{id}/resume",
            "/v1/sandboxes/{id}/snapshot",
            "/v1/snapshots",
            "/v1/snapshots/{name}",
        ] {
            assert!(
                spec.paths.paths.contains_key(path),
                "spec missing path {path}"
            );
        }

        let components = spec.components.expect("components present");
        assert!(components.security_schemes.contains_key("bearer_token"));
        assert!(components.schemas.contains_key("SandboxRecord"));
    }
}
