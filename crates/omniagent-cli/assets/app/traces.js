// Trace pane controller: ingests the SSE span stream, renders each span node
// exactly once (cached in `nodeById`), and handles filter/search by toggling
// visibility instead of rebuilding. Also owns scroll-lock (don't yank the view
// when new spans arrive mid-scroll) and keyboard navigation.

import {
  spans,
  seenIds,
  nodeById,
  totals,
  ui,
} from "./state.js";
import { spanSearchHaystack, readUsage } from "./parse.js";
import { buildNode, openSpanModal } from "./render.js";
import { updateMetrics } from "./metrics.js";

const tracesEl = document.getElementById("traces");
const emptyEl = document.getElementById("empty");
const countEl = document.getElementById("count");
const pillEl = document.getElementById("new-pill");
const pillNEl = document.getElementById("new-pill-n");

let pendingNew = 0;

function passes(span) {
  if (!ui.search) return true;
  return spanSearchHaystack(span).includes(ui.search);
}

function nodeFor(span) {
  let node = nodeById.get(span.id);
  if (!node) {
    node = buildNode(span);
    nodeById.set(span.id, node);
  }
  return node;
}

// Re-evaluate which existing nodes are visible. No rebuild, no re-highlight.
function applyVisibility() {
  let shown = 0;
  for (const span of spans) {
    const node = nodeById.get(span.id);
    if (!node) continue;
    const ok = passes(span);
    node.classList.toggle("hidden", !ok);
    if (ok) shown += 1;
  }
  emptyEl.style.display = shown ? "none" : "flex";
}

function atTop() {
  return tracesEl.scrollTop <= 4;
}

function showPill() {
  pendingNew += 1;
  pillNEl.textContent = String(pendingNew);
  pillEl.hidden = false;
}

function clearPill() {
  pendingNew = 0;
  pillEl.hidden = true;
}

function ingest(span) {
  if (!span || seenIds.has(span.id)) return;
  seenIds.add(span.id);
  spans.unshift(span);
  const u = readUsage(span);
  totals.in += u.inTok || 0;
  totals.out += u.outTok || 0;
  totals.cacheR += u.cacheR || 0;
  totals.cacheW += u.cacheW || 0;
  totals.lat += span.latency_ms || 0;
  if (span.error || (span.status && span.status >= 400)) totals.err += 1;
  updateMetrics();
  if (countEl) countEl.textContent = String(spans.length);

  const node = nodeFor(span);
  const visible = passes(span);
  node.classList.toggle("hidden", !visible);

  // Newest-first: insert above the first existing span node (after #empty).
  const firstSpan = tracesEl.querySelector(".span");
  const pinned = atTop();
  tracesEl.insertBefore(node, firstSpan);

  if (visible) {
    emptyEl.style.display = "none";
    if (pinned) {
      tracesEl.scrollTop = 0;
    } else {
      // Scroll lock: keep the user's current view from jumping.
      tracesEl.scrollTop += node.offsetHeight;
      showPill();
    }
  }
}

function moveSelection(dir) {
  const visible = spans.filter(passes);
  if (!visible.length) return;
  let idx = visible.findIndex((s) => s.id === ui.selectedId);
  idx =
    idx < 0
      ? dir > 0
        ? 0
        : visible.length - 1
      : Math.min(visible.length - 1, Math.max(0, idx + dir));
  selectSpan(visible[idx].id);
}

function selectSpan(id) {
  if (ui.selectedId && ui.selectedId !== id)
    nodeById.get(ui.selectedId)?.classList.remove("selected");
  ui.selectedId = id;
  const node = nodeById.get(id);
  if (node) {
    node.classList.add("selected");
    node.scrollIntoView({ block: "nearest" });
  }
}

function openSelected() {
  const span = spans.find((s) => s.id === ui.selectedId);
  const node = ui.selectedId && nodeById.get(ui.selectedId);
  if (span && node) openSpanModal(span, node);
}

function typingTarget(t) {
  return (
    t &&
    (t.tagName === "INPUT" ||
      t.tagName === "TEXTAREA" ||
      t.isContentEditable ||
      t.closest?.("#term-pane"))
  );
}

function initKeys() {
  const search = document.getElementById("trace-search");
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      if (document.activeElement === search) search.blur();
      return;
    }
    if (typingTarget(e.target)) return;
    if (ui.tab !== "traces") return;
    if (e.key === "/") {
      e.preventDefault();
      search.focus();
      search.select();
    } else if (e.key === "j") {
      e.preventDefault();
      moveSelection(1);
    } else if (e.key === "k") {
      e.preventDefault();
      moveSelection(-1);
    } else if (e.key === "Enter") {
      e.preventDefault();
      openSelected();
    }
  });
}

export function initTraces() {
  let searchTimer;
  document.getElementById("trace-search").addEventListener("input", (e) => {
    const value = e.target.value.trim().toLowerCase();
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      ui.search = value;
      applyVisibility();
    }, 120);
  });

  pillEl?.addEventListener("click", () => {
    tracesEl.scrollTo({ top: 0, behavior: "smooth" });
    clearPill();
  });
  tracesEl.addEventListener("scroll", () => {
    if (atTop() && pendingNew) clearPill();
  });

  initKeys();

  const es = new EventSource("/api/traces/events?from=1");
  es.addEventListener("span", (ev) => {
    try {
      ingest(JSON.parse(ev.data));
    } catch (_) {}
  });
  es.onopen = () => {
    document.body.dataset.sse = "up";
  };
  es.onerror = () => {
    if (es.readyState === EventSource.CLOSED)
      document.body.dataset.sse = "down";
  };
}
