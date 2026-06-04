use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex as StdMutex};

use oad_core::{OadPaths, SandboxId, SandboxRecord, write_atomic_file};
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};
use tracing::{debug, warn};

#[derive(Debug, Clone)]
pub struct SandboxRegistry {
    inner: Arc<RegistryInner>,
}

#[derive(Debug, Clone, Default)]
pub struct NamedLocks {
    inner: Arc<StdMutex<BTreeMap<String, NamedLockSlot>>>,
}

#[derive(Debug)]
struct NamedLockSlot {
    lock: Arc<Mutex<()>>,
    holders: usize,
}

pub struct NamedLockGuard {
    inner: Arc<StdMutex<BTreeMap<String, NamedLockSlot>>>,
    key: String,
    _guard: OwnedMutexGuard<()>,
}

impl NamedLocks {
    pub async fn acquire(&self, key: &str) -> NamedLockGuard {
        let lock = {
            let mut locks = self.inner.lock().expect("named lock map poisoned");
            let slot = locks
                .entry(key.to_string())
                .or_insert_with(|| NamedLockSlot {
                    lock: Arc::new(Mutex::new(())),
                    holders: 0,
                });
            slot.holders += 1;
            slot.lock.clone()
        };
        // If the caller is cancelled while waiting on the contended lock below,
        // this releases the holder count we just incremented so the slot can
        // still be pruned. Disarmed once the real guard is built.
        let pending = PendingHolder {
            inner: Some(self.inner.clone()),
            key,
        };
        let guard = lock.lock_owned().await;
        pending.disarm();
        NamedLockGuard {
            inner: self.inner.clone(),
            key: key.to_string(),
            _guard: guard,
        }
    }
}

/// Decrements a lock slot's holder count (pruning the slot at zero) unless
/// `disarm`ed. Guards the window between incrementing `holders` and acquiring
/// the owned mutex, where a cancelled future would otherwise leak the count.
struct PendingHolder<'a> {
    inner: Option<Arc<StdMutex<BTreeMap<String, NamedLockSlot>>>>,
    key: &'a str,
}

impl PendingHolder<'_> {
    fn disarm(mut self) {
        self.inner = None;
    }
}

impl Drop for PendingHolder<'_> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let mut locks = inner.lock().expect("named lock map poisoned");
            if let Some(slot) = locks.get_mut(self.key) {
                slot.holders -= 1;
                if slot.holders == 0 {
                    locks.remove(self.key);
                }
            }
        }
    }
}

impl Drop for NamedLockGuard {
    fn drop(&mut self) {
        let mut locks = self.inner.lock().expect("named lock map poisoned");
        if let Some(slot) = locks.get_mut(&self.key) {
            slot.holders -= 1;
            if slot.holders == 0 {
                locks.remove(&self.key);
            }
        }
    }
}

#[derive(Debug)]
struct RegistryInner {
    records: RwLock<BTreeMap<String, SandboxRecord>>,
    locks: StdMutex<BTreeMap<String, LockSlot>>,
}

/// A per-sandbox lifecycle mutex plus a count of outstanding guards, so the
/// entry can be removed deterministically once the last guard is dropped.
#[derive(Debug)]
struct LockSlot {
    lock: Arc<Mutex<()>>,
    holders: usize,
}

/// Held for the duration of a sandbox lifecycle operation (create/delete).
///
/// Dropping it releases the per-sandbox mutex and decrements the slot's holder
/// count, removing the slot from the map when it reaches zero. This keeps the
/// lock map bounded to in-flight operations without relying on `Arc` strong
/// counts.
pub struct LifecycleGuard {
    inner: Arc<RegistryInner>,
    id: String,
    // Dropped after the explicit `Drop` impl runs; releases the tokio mutex.
    _guard: OwnedMutexGuard<()>,
}

impl Drop for LifecycleGuard {
    fn drop(&mut self) {
        let mut locks = self
            .inner
            .locks
            .lock()
            .expect("lifecycle lock map poisoned");
        if let Some(slot) = locks.get_mut(&self.id) {
            slot.holders -= 1;
            if slot.holders == 0 {
                locks.remove(&self.id);
            }
        }
    }
}

/// Cancellation-safety counterpart to [`LifecycleGuard`]: decrements the slot's
/// holder count (pruning at zero) unless `disarm`ed after the owned mutex is
/// acquired.
struct PendingLifecycleHolder<'a> {
    inner: Option<Arc<RegistryInner>>,
    id: &'a str,
}

impl PendingLifecycleHolder<'_> {
    fn disarm(mut self) {
        self.inner = None;
    }
}

impl Drop for PendingLifecycleHolder<'_> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let mut locks = inner.locks.lock().expect("lifecycle lock map poisoned");
            if let Some(slot) = locks.get_mut(self.id) {
                slot.holders -= 1;
                if slot.holders == 0 {
                    locks.remove(self.id);
                }
            }
        }
    }
}

impl SandboxRegistry {
    pub async fn recover(paths: &OadPaths) -> Self {
        let registry = Self {
            inner: Arc::new(RegistryInner {
                records: RwLock::new(BTreeMap::new()),
                locks: StdMutex::new(BTreeMap::new()),
            }),
        };

        let sandboxes = paths.sandboxes_dir();
        let mut entries = match tokio::fs::read_dir(&sandboxes).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return registry,
            Err(err) => {
                warn!(path = %sandboxes.display(), error = %err, "failed to recover sandboxes");
                return registry;
            }
        };

        loop {
            let entry = match entries.next_entry().await {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(err) => {
                    warn!(path = %sandboxes.display(), error = %err, "failed to read sandbox directory entry");
                    break;
                }
            };
            let state_file = entry.path().join("state.json");
            match read_record(&state_file).await {
                Ok(record) => {
                    // Status is reconciled against live `runsc` state by a
                    // separate pass after recovery; load it verbatim here.
                    debug!(sandbox_id = %record.id, "recovered sandbox state");
                    registry.insert_memory(record).await;
                }
                Err(err) => {
                    warn!(path = %state_file.display(), error = %err, "failed to recover sandbox");
                }
            }
        }

        registry
    }

    /// Acquires the lifecycle lock for `id`, returning a guard that releases it
    /// (and prunes the lock entry when no other operation holds it) on drop.
    pub async fn acquire_lifecycle(&self, id: &SandboxId) -> LifecycleGuard {
        let lock = {
            let mut locks = self
                .inner
                .locks
                .lock()
                .expect("lifecycle lock map poisoned");
            let slot = locks.entry(id.to_string()).or_insert_with(|| LockSlot {
                lock: Arc::new(Mutex::new(())),
                holders: 0,
            });
            slot.holders += 1;
            slot.lock.clone()
        };
        // Release the holder count if the caller is cancelled while waiting on
        // the contended lifecycle lock; disarmed once the guard is built.
        let pending = PendingLifecycleHolder {
            inner: Some(self.inner.clone()),
            id: id.as_str(),
        };
        let guard = lock.lock_owned().await;
        pending.disarm();
        LifecycleGuard {
            inner: self.inner.clone(),
            id: id.to_string(),
            _guard: guard,
        }
    }

    pub async fn insert(&self, paths: &OadPaths, record: SandboxRecord) -> io::Result<()> {
        self.persist(paths, &record).await?;
        self.insert_memory(record).await;
        Ok(())
    }

    pub async fn insert_memory(&self, record: SandboxRecord) {
        self.inner
            .records
            .write()
            .await
            .insert(record.id.to_string(), record);
    }

    pub async fn update(
        &self,
        paths: &OadPaths,
        id: &SandboxId,
        update: impl FnOnce(&mut SandboxRecord),
    ) -> io::Result<Option<SandboxRecord>> {
        let records = self.inner.records.read().await;
        let Some(record) = records.get(id.as_str()) else {
            return Ok(None);
        };
        let mut updated = record.clone();
        drop(records);

        update(&mut updated);
        self.persist(paths, &updated).await?;

        let mut records = self.inner.records.write().await;
        let Some(record) = records.get_mut(id.as_str()) else {
            return Ok(None);
        };
        *record = updated.clone();
        drop(records);
        Ok(Some(updated))
    }

    pub async fn get(&self, id: &SandboxId) -> Option<SandboxRecord> {
        self.inner.records.read().await.get(id.as_str()).cloned()
    }

    pub async fn contains(&self, id: &SandboxId) -> bool {
        self.inner.records.read().await.contains_key(id.as_str())
    }

    /// Removes a sandbox record from memory, returning it if present. The
    /// on-disk state is owned by the sandbox directory and removed separately.
    pub async fn remove(&self, id: &SandboxId) -> Option<SandboxRecord> {
        self.inner.records.write().await.remove(id.as_str())
    }

    pub async fn list(&self) -> Vec<SandboxRecord> {
        self.inner.records.read().await.values().cloned().collect()
    }

    async fn persist(&self, paths: &OadPaths, record: &SandboxRecord) -> io::Result<()> {
        write_json_atomic(&paths.state_file(&record.id), record).await
    }
}

async fn read_record(path: &Path) -> io::Result<SandboxRecord> {
    let body = tokio::fs::read(path).await?;
    serde_json::from_slice(&body).map_err(io::Error::other)
}

/// Serializes `value` as pretty JSON and writes it to `path` atomically,
/// creating the parent directory if needed.
pub async fn write_json_atomic<T: serde::Serialize + Sync>(
    path: &Path,
    value: &T,
) -> io::Result<()> {
    let body = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
    write_atomic_file(path, &body).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use oad_core::SandboxStatus;

    fn empty_registry() -> SandboxRegistry {
        SandboxRegistry {
            inner: Arc::new(RegistryInner {
                records: RwLock::new(BTreeMap::new()),
                locks: StdMutex::new(BTreeMap::new()),
            }),
        }
    }

    fn lock_count(registry: &SandboxRegistry) -> usize {
        registry.inner.locks.lock().unwrap().len()
    }

    #[tokio::test]
    async fn lifecycle_guard_prunes_on_last_drop() {
        let registry = empty_registry();
        let id = SandboxId::new("sandbox").unwrap();

        let guard = registry.acquire_lifecycle(&id).await;
        assert_eq!(lock_count(&registry), 1);
        drop(guard);
        assert_eq!(lock_count(&registry), 0, "slot should be pruned on drop");
    }

    #[tokio::test]
    async fn overlapping_holders_keep_slot_until_last_drop() {
        let registry = empty_registry();
        let id = SandboxId::new("sandbox").unwrap();

        // A second acquire must wait on the mutex, but it still registers as a
        // holder immediately, so the slot survives the first guard's drop.
        let first = registry.acquire_lifecycle(&id).await;
        let registry2 = registry.clone();
        let id2 = id.clone();
        let waiter = tokio::spawn(async move { registry2.acquire_lifecycle(&id2).await });

        // Give the waiter time to register as a holder before releasing `first`.
        tokio::task::yield_now().await;
        while registry.inner.locks.lock().unwrap()[id.as_str()].holders < 2 {
            tokio::task::yield_now().await;
        }

        drop(first);
        let second = waiter.await.unwrap();
        assert_eq!(lock_count(&registry), 1, "slot held by the second guard");
        drop(second);
        assert_eq!(lock_count(&registry), 0);
    }

    #[tokio::test]
    async fn failed_update_persist_does_not_mutate_memory() {
        let registry = empty_registry();
        let id = SandboxId::new("sandbox").unwrap();
        let record = SandboxRecord::new_pending(id.clone(), vec!["pause".to_string()]);
        registry.insert_memory(record).await;

        let blocker = tempfile::NamedTempFile::new().unwrap();
        let paths = OadPaths::new(blocker.path());
        let result = registry
            .update(&paths, &id, |record| {
                record.set_status(SandboxStatus::Running);
            })
            .await;

        assert!(result.is_err());
        let stored = registry.get(&id).await.unwrap();
        assert_eq!(stored.status, SandboxStatus::Pending);
    }

    #[tokio::test]
    async fn persist_writes_state_atomically() {
        let registry = empty_registry();
        let temp = tempfile::tempdir().unwrap();
        let paths = OadPaths::new(temp.path());
        let id = SandboxId::new("sandbox").unwrap();
        let record = SandboxRecord::new_pending(id.clone(), vec!["pause".to_string()]);

        registry.insert(&paths, record.clone()).await.unwrap();

        let stored = read_record(&paths.state_file(&id)).await.unwrap();
        assert_eq!(stored, record);
        let mut entries = tokio::fs::read_dir(paths.sandbox_dir(&id)).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            assert!(
                !entry.file_name().to_string_lossy().ends_with(".tmp"),
                "temporary state file should not remain"
            );
        }
    }
}
