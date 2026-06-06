// Diff view: the agent's changed files (git status) and a per-file unified diff
// (git diff). Backed by GET /api/diff. Refreshes whenever the Diff view is
// opened so it tracks the agent's ongoing edits.

import { escapeHtml } from "./format.js";

const STATUS_LABEL = {
  M: "modified",
  A: "added",
  D: "deleted",
  R: "renamed",
  C: "copied",
  U: "conflict",
  "??": "untracked",
};

const filesEl = () => document.getElementById("diff-files");
const viewEl = () => document.getElementById("diff-view");

function renderDiffText(text) {
  if (!text || !text.trim())
    return '<div class="review-empty">no textual diff (new or binary file)</div>';
  return text
    .split("\n")
    .map((line) => {
      let cls = "";
      if (line.startsWith("@@")) cls = "hunk";
      else if (line.startsWith("+++") || line.startsWith("---")) cls = "meta";
      else if (line.startsWith("diff ") || line.startsWith("index ")) cls = "meta";
      else if (line.startsWith("+")) cls = "add";
      else if (line.startsWith("-")) cls = "del";
      return `<span class="dl ${cls}">${escapeHtml(line) || "&nbsp;"}</span>`;
    })
    .join("");
}

async function openDiff(path, row) {
  for (const r of filesEl().querySelectorAll(".fx-row.sel"))
    r.classList.remove("sel");
  row.classList.add("sel");
  const el = viewEl();
  el.textContent = "loading…";
  try {
    const res = await fetch("/api/diff?path=" + encodeURIComponent(path));
    const data = await res.json();
    el.innerHTML = renderDiffText(data.diff || "");
  } catch (_) {
    el.textContent = "failed to load diff";
  }
}

async function refresh() {
  const list = filesEl();
  if (!list) return;
  let data = { files: [] };
  try {
    data = await fetch("/api/diff").then((r) => r.json());
  } catch (_) {}
  list.innerHTML = "";
  if (!data.files || !data.files.length) {
    list.innerHTML = '<div class="review-empty">no changes</div>';
    return;
  }
  for (const file of data.files) {
    const row = document.createElement("div");
    row.className = "fx-row is-file";
    const label = STATUS_LABEL[file.status] || file.status || "?";
    row.innerHTML = `<span class="diff-badge s-${escapeHtml(
      file.status.replace(/[^a-zA-Z]/g, "") || "x",
    )}">${escapeHtml(label)}</span><span class="fx-name"></span>`;
    row.querySelector(".fx-name").textContent = file.path;
    row.addEventListener("click", () => openDiff(file.path, row));
    list.appendChild(row);
  }
}

export function initDiff() {
  refresh();
  // Re-pull when the user opens the Diff view so it tracks live edits.
  document
    .querySelector('.view[data-view="diff"]')
    ?.addEventListener("click", refresh);
}
