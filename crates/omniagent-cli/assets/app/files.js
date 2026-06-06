// File view: a lazy-loaded read-only tree of the workspace plus a content
// viewer. Backed by GET /api/files (listing) and GET /api/file (contents),
// both sandboxed to the workspace root server-side.

import { hl } from "./format.js";

const treeEl = () => document.getElementById("file-tree");
const viewEl = () => document.getElementById("file-view");

async function fetchDir(path) {
  try {
    const res = await fetch("/api/files?path=" + encodeURIComponent(path));
    if (!res.ok) return [];
    return await res.json();
  } catch (_) {
    return [];
  }
}

async function openFile(path) {
  const el = viewEl();
  el.textContent = "loading…";
  try {
    const res = await fetch("/api/file?path=" + encodeURIComponent(path));
    if (!res.ok) {
      el.textContent =
        res.status === 413
          ? "file too large to display"
          : res.status === 415
            ? "binary / non-text file"
            : "cannot open file";
      return;
    }
    const text = await res.text();
    if (path.endsWith(".json")) {
      try {
        el.innerHTML = hl(JSON.parse(text));
        return;
      } catch (_) {}
    }
    el.textContent = text;
  } catch (_) {
    el.textContent = "failed to load file";
  }
}

function rowFor(entry, parent, depth) {
  const full = parent ? `${parent}/${entry.name}` : entry.name;
  const row = document.createElement("div");
  row.className = "fx-row " + (entry.dir ? "is-dir" : "is-file");
  row.style.paddingLeft = `${8 + depth * 14}px`;
  row.innerHTML = `<span class="fx-ic">${entry.dir ? "▸" : "·"}</span><span class="fx-name"></span>`;
  row.querySelector(".fx-name").textContent = entry.name;

  if (entry.dir) {
    let open = false;
    let childWrap = null;
    row.addEventListener("click", async () => {
      open = !open;
      row.querySelector(".fx-ic").textContent = open ? "▾" : "▸";
      if (open) {
        if (!childWrap) {
          childWrap = document.createElement("div");
          row.after(childWrap);
          for (const kid of await fetchDir(full))
            childWrap.appendChild(rowFor(kid, full, depth + 1));
        } else {
          childWrap.hidden = false;
        }
      } else if (childWrap) {
        childWrap.hidden = true;
      }
    });
  } else {
    row.addEventListener("click", () => {
      for (const r of treeEl().querySelectorAll(".fx-row.sel"))
        r.classList.remove("sel");
      row.classList.add("sel");
      openFile(full);
    });
  }
  return row;
}

export async function initFiles() {
  const tree = treeEl();
  if (!tree) return;
  tree.innerHTML = "";
  const entries = await fetchDir("");
  if (!entries.length) {
    tree.innerHTML = '<div class="review-empty">empty workspace</div>';
    return;
  }
  for (const entry of entries) tree.appendChild(rowFor(entry, "", 0));
}
