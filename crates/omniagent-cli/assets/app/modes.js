// Workflow modes + right-pane views.
//
// MODES set `body[data-mode]` (agent | inspection | side-by-side) and drive the
// macro layout via CSS; the choice persists in localStorage. VIEWS switch the
// right pane between main / file / diff and are available in every mode. The
// main view itself has traces/review/compare sub-tabs (`setTab`).

import { ui } from "./state.js";
import { refit } from "./pty.js";

const MODES = ["agent", "inspection", "side-by-side"];
const VIEWS = ["main", "file", "diff"];
const TABS = ["traces", "review", "compare"];
const MODE_KEY = "omniagent.mode";

// Each mode's default view when you switch into it.
const DEFAULT_VIEW = { agent: "file", inspection: "main", "side-by-side": "main" };

export function setTab(name) {
  if (!TABS.includes(name)) return;
  // Sub-tabs live inside the main view; make sure it's showing.
  if (ui.view !== "main") setView("main");
  applyTab(name);
}

// Toggles the active sub-tab without forcing the main view (used when a mode
// pins a tab, e.g. agent mode → traces only).
function applyTab(name) {
  ui.tab = name;
  for (const btn of document.querySelectorAll(".tab"))
    btn.classList.toggle("is-active", btn.dataset.tab === name);
  for (const panel of document.querySelectorAll(".tab-panel"))
    panel.classList.toggle("is-active", panel.dataset.panel === name);
}

export function setView(name) {
  if (!VIEWS.includes(name)) return;
  ui.view = name;
  for (const btn of document.querySelectorAll(".view"))
    btn.classList.toggle("is-active", btn.dataset.view === name);
  for (const panel of document.querySelectorAll(".view-panel"))
    panel.classList.toggle("is-active", panel.dataset.view === name);
}

export function setMode(name) {
  if (!MODES.includes(name)) name = "agent";
  ui.mode = name;
  document.body.dataset.mode = name;
  for (const btn of document.querySelectorAll("#modes .mode"))
    btn.classList.toggle("is-active", btn.dataset.mode === name);
  try {
    localStorage.setItem(MODE_KEY, name);
  } catch (_) {}
  // Agent mode has no review/compare — the main view shows only traces.
  if (name === "agent") applyTab("traces");
  setView(DEFAULT_VIEW[name] || "main");
  // Layout changed — re-fit the terminal canvas on the next frame.
  requestAnimationFrame(() => refit());
}

export function initModes() {
  // Mode is chosen on the startup screen and locked for the session — the
  // header switcher is a read-only indicator, so it has no click handler.
  document.getElementById("view-switch")?.addEventListener("click", (e) => {
    const btn = e.target.closest(".view");
    if (btn) setView(btn.dataset.view);
  });
  document.getElementById("tablist")?.addEventListener("click", (e) => {
    const btn = e.target.closest(".tab");
    if (btn) setTab(btn.dataset.tab);
  });
  document.getElementById("trace-collapse")?.addEventListener("click", () => {
    const collapsed = document.body.toggleAttribute("data-trace-collapsed");
    document.getElementById("trace-collapse").textContent = collapsed
      ? "«"
      : "»";
    requestAnimationFrame(() => refit());
  });
  // Full-screen the terminal (most useful in inspection mode's small pane).
  const fullBtn = document.getElementById("term-full");
  const toggleFull = (on) => {
    const full =
      on === undefined
        ? document.body.toggleAttribute("data-term-full")
        : (document.body.toggleAttribute("data-term-full", on), on);
    if (fullBtn) fullBtn.classList.toggle("is-active", full);
    requestAnimationFrame(() => refit());
  };
  fullBtn?.addEventListener("click", () => toggleFull());
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && document.body.hasAttribute("data-term-full"))
      toggleFull(false);
  });
}

// Applies the mode chosen on the startup screen (called post-launch).
export function applyMode(name) {
  setMode(MODES.includes(name) ? name : "agent");
}
