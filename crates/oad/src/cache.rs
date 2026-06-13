//! Bounded local cache accounting for the content-addressed store.
//!
//! A node materializes chunks and snapshot artifacts from object storage onto
//! local disk under [`oad_core::OadPaths`]. Left unchecked that cache grows
//! without bound; this index tracks each cached artifact's size, access order,
//! and pin state so the cache can be held under a byte budget
//! (`OAD_CACHE_MAX_BYTES`) by evicting the coldest entries — never one pinned by
//! a running or restorable sandbox.
//!
//! The index is in-memory and authoritative for accounting; the daemon records
//! entries as it pulls/materializes them, pins them across a fork's lifetime,
//! and applies [`CacheIndex::evict_plan`] (the pure eviction policy) to decide
//! what to delete when the high watermark is crossed.

use std::collections::HashMap;

/// What kind of cached artifact an entry tracks.
///
/// Materialized artifacts are evicted before raw chunks: they are larger and can
/// be rebuilt cheaply from chunks that may still be resident, whereas evicting a
/// chunk forces an object-store pull on next use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    /// A reassembled artifact (EROFS rootfs or checkpoint image).
    Materialized,
    /// A raw chunk pulled from object storage. Recorded once chunk-level
    /// pull-through caching is wired; today only the materialized tier is
    /// tracked, so the daemon never constructs this outside tests.
    #[allow(dead_code)]
    Chunk,
}

#[derive(Debug, Clone)]
struct Entry {
    kind: EntryKind,
    size: u64,
    last_access: u64,
    pins: u32,
}

/// Tracks cached entries by key and plans eviction under a byte budget.
#[derive(Debug, Default)]
pub struct CacheIndex {
    entries: HashMap<String, Entry>,
    clock: u64,
    total_bytes: u64,
}

impl CacheIndex {
    /// Creates an empty index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    const fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    /// Records (or updates the size of) an entry and marks it most-recently-used.
    pub fn record(&mut self, key: impl Into<String>, kind: EntryKind, size: u64) {
        let key = key.into();
        let access = self.tick();
        if let Some(entry) = self.entries.get_mut(&key) {
            let old = entry.size;
            entry.size = size;
            entry.kind = kind;
            entry.last_access = access;
            self.total_bytes = self.total_bytes - old + size;
        } else {
            self.entries.insert(
                key,
                Entry {
                    kind,
                    size,
                    last_access: access,
                    pins: 0,
                },
            );
            self.total_bytes += size;
        }
    }

    /// Marks an entry most-recently-used (a cache hit). No-op if absent.
    pub fn touch(&mut self, key: &str) {
        let access = self.tick();
        if let Some(entry) = self.entries.get_mut(key) {
            entry.last_access = access;
        }
    }

    /// Pins an entry so it is never evicted while in use. Balanced by [`Self::unpin`].
    pub fn pin(&mut self, key: &str) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.pins += 1;
        }
    }

    /// Releases one pin acquired by [`Self::pin`].
    pub fn unpin(&mut self, key: &str) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.pins = entry.pins.saturating_sub(1);
        }
    }

    /// Drops an entry (e.g. after it has been evicted from disk).
    pub fn remove(&mut self, key: &str) {
        if let Some(entry) = self.entries.remove(key) {
            self.total_bytes -= entry.size;
        }
    }

    /// Total tracked bytes across all entries.
    #[must_use]
    pub const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Whether `key` is currently tracked.
    #[must_use]
    pub fn contains(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    /// Whether `key` is currently pinned.
    #[must_use]
    pub fn is_pinned(&self, key: &str) -> bool {
        self.entries.get(key).is_some_and(|entry| entry.pins > 0)
    }

    /// Plans eviction to bring the cache to at most `low_watermark` bytes,
    /// returning the keys to evict, coldest-first.
    ///
    /// Pinned entries are never chosen. Materialized artifacts are evicted before
    /// chunks (cheaper to rebuild); within a kind, least-recently-used first.
    /// Returns an empty plan when `max_bytes` is 0 (unbounded) or the cache is
    /// already at or under `max_bytes`.
    #[must_use]
    pub fn evict_plan(&self, max_bytes: u64, low_watermark: u64) -> Vec<String> {
        if max_bytes == 0 || self.total_bytes <= max_bytes {
            return Vec::new();
        }

        let mut candidates: Vec<(&String, &Entry)> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.pins == 0)
            .collect();
        candidates.sort_by(|(_, a), (_, b)| {
            kind_rank(a.kind)
                .cmp(&kind_rank(b.kind))
                .then(a.last_access.cmp(&b.last_access))
        });

        let target = self.total_bytes.saturating_sub(low_watermark);
        let mut freed = 0u64;
        let mut plan = Vec::new();
        for (key, entry) in candidates {
            if freed >= target {
                break;
            }
            plan.push(key.clone());
            freed += entry.size;
        }
        plan
    }
}

const fn kind_rank(kind: EntryKind) -> u8 {
    match kind {
        EntryKind::Materialized => 0,
        EntryKind::Chunk => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_total_bytes_across_record_and_remove() {
        let mut index = CacheIndex::new();
        index.record("a", EntryKind::Chunk, 100);
        index.record("b", EntryKind::Chunk, 250);
        assert_eq!(index.total_bytes(), 350);

        // Re-recording updates the size in place.
        index.record("a", EntryKind::Chunk, 150);
        assert_eq!(index.total_bytes(), 400);

        index.remove("b");
        assert_eq!(index.total_bytes(), 150);
        assert!(!index.contains("b"));
    }

    #[test]
    fn unbounded_or_under_budget_evicts_nothing() {
        let mut index = CacheIndex::new();
        index.record("a", EntryKind::Chunk, 100);
        assert!(index.evict_plan(0, 0).is_empty(), "0 max = unbounded");
        assert!(index.evict_plan(1000, 500).is_empty(), "under budget");
    }

    #[test]
    fn evicts_coldest_first_down_to_low_watermark() {
        let mut index = CacheIndex::new();
        index.record("oldest", EntryKind::Chunk, 100);
        index.record("middle", EntryKind::Chunk, 100);
        index.record("newest", EntryKind::Chunk, 100);
        // total 300; cap 250, low watermark 100 -> must free >= 200 -> evict the
        // two coldest.
        let plan = index.evict_plan(250, 100);
        assert_eq!(plan, vec!["oldest".to_string(), "middle".to_string()]);
    }

    #[test]
    fn touch_updates_recency() {
        let mut index = CacheIndex::new();
        index.record("a", EntryKind::Chunk, 100);
        index.record("b", EntryKind::Chunk, 100);
        index.touch("a"); // a is now most-recent; b is coldest
        let plan = index.evict_plan(150, 100);
        assert_eq!(plan, vec!["b".to_string()]);
    }

    #[test]
    fn never_evicts_pinned_entries() {
        let mut index = CacheIndex::new();
        index.record("pinned", EntryKind::Chunk, 100);
        index.record("free", EntryKind::Chunk, 100);
        index.pin("pinned");
        // Need to free 150 but only "free" (100) is evictable.
        let plan = index.evict_plan(50, 0);
        assert_eq!(plan, vec!["free".to_string()]);

        index.unpin("pinned");
        let plan = index.evict_plan(50, 0);
        assert!(plan.contains(&"pinned".to_string()));
    }

    #[test]
    fn evicts_materialized_before_chunks() {
        let mut index = CacheIndex::new();
        // Record the chunk first (coldest by access), then the materialized one.
        index.record("chunk", EntryKind::Chunk, 100);
        index.record("rootfs", EntryKind::Materialized, 100);
        // Freeing 100 should drop the materialized artifact despite it being
        // newer — it is cheaper to rebuild from the still-resident chunk.
        let plan = index.evict_plan(150, 100);
        assert_eq!(plan, vec!["rootfs".to_string()]);
    }
}
