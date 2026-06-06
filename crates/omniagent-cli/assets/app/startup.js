// Startup screen + deferred launch.
//
// On boot we ask the backend whether an agent is already running. If not, we
// show the startup overlay; picking a mode + review setting and hitting Start
// posts /api/launch (which spawns the agent), then boots the runtime modules.
// On reload of an already-running session we skip straight to the runtime.

import { ui } from "./state.js";
import { applyMode } from "./modes.js";

let runtimeStarted = false;

// Lazily import + run the runtime modules exactly once (post-launch).
async function startRuntime(mode) {
  if (runtimeStarted) return;
  runtimeStarted = true;

  const [{ initPty }, { initTraces }, { initReviews }, { initFiles }, { initDiff }] =
    await Promise.all([
      import("./pty.js"),
      import("./traces.js"),
      import("./reviews.js"),
      import("./files.js"),
      import("./diff.js"),
    ]);

  applyMode(mode || "agent");
  initPty().catch((err) => {
    console.error("failed to initialize terminal", err);
    const s = document.getElementById("status");
    if (s) s.textContent = "terminal init failed";
  });
  initTraces();
  initReviews();
  initFiles();
  initDiff();
}

function showStartup() {
  const overlay = document.getElementById("startup");
  const modesEl = document.getElementById("startup-modes");
  const reviewEl = document.getElementById("startup-review");
  const startEl = document.getElementById("startup-start");
  if (!overlay) return;
  overlay.hidden = false;

  let mode = "agent";
  const selectMode = (btn) => {
    if (!btn || btn.disabled) return;
    mode = btn.dataset.mode;
    for (const b of modesEl.querySelectorAll(".smode"))
      b.classList.toggle("is-active", b === btn);
    // Default the review toggle to the mode's recommendation.
    reviewEl.checked = btn.dataset.review === "1";
  };

  modesEl.addEventListener("click", (e) =>
    selectMode(e.target.closest(".smode")),
  );

  startEl.addEventListener("click", async () => {
    startEl.disabled = true;
    startEl.textContent = "launching…";
    try {
      const res = await fetch("/api/launch", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ mode, review: reviewEl.checked }),
      });
      if (!res.ok && res.status !== 409) throw new Error(String(res.status));
      ui.reviewEnabled = reviewEl.checked;
      overlay.hidden = true;
      startRuntime(mode);
    } catch (err) {
      startEl.disabled = false;
      startEl.textContent = "launch failed — retry";
    }
  });
}

export async function initStartup() {
  try {
    const status = await fetch("/api/status").then((r) => r.json());
    // Populate the read-only launch details.
    const cmd = document.getElementById("startup-cmd");
    const cwd = document.getElementById("startup-cwd");
    if (cmd) cmd.textContent = status.agent_cmd || "—";
    if (cwd) cwd.textContent = status.cwd || "—";

    if (status.launched) {
      // Already running (e.g. page reload, or oad mode) — skip the screen.
      ui.reviewEnabled = false;
      startRuntime(status.mode || "agent");
    } else {
      showStartup();
    }
  } catch (_) {
    // Backend unreachable — fall back to the startup screen.
    showStartup();
  }
}
