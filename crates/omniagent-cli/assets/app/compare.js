// Multi-model comparison panel: replays a captured request against several
// models and renders their responses side-by-side. Runs are created from the
// "compare" button on a trace span; results stream in over SSE as each model
// finishes. Analysis only — the agent's own response is untouched.

import { escapeHtml, fmtLat, fmtTok, compactPath } from "./format.js";
import { responseContent, previewText, readUsage } from "./parse.js";
import { comparisons, ui } from "./state.js";
import { setTab } from "./modes.js";

// Reuse the span-shaped parsers by wrapping a variant in a pseudo-span.
function variantBody(variant) {
  return { response: variant.response, usage: variant.usage ?? {} };
}

function variantPreview(variant) {
  if (variant.state === "pending") return "…running";
  if (variant.error) return `error: ${variant.error}`;
  const text = previewText(responseContent(variantBody(variant)), 1600);
  return text || "(no textual content)";
}

function variantStatus(variant) {
  if (variant.state === "pending") return "running";
  if (variant.state === "error") return variant.error ? "failed" : "error";
  const lat =
    variant.latency_ms != null ? ` · ${fmtLat(variant.latency_ms)}` : "";
  return `${variant.status ?? "?"}${lat}`;
}

function variantCard(variant) {
  const { inTok, outTok } = readUsage(variantBody(variant));
  const tokens =
    variant.state === "done"
      ? `<span class="compare-tok">${fmtTok(inTok)}→${fmtTok(outTok)}</span>`
      : "";
  return `
    <div class="compare-col" data-state="${escapeHtml(variant.state)}">
      <div class="compare-col-head">
        <span class="compare-model">${escapeHtml(variant.model)}</span>
        <span class="compare-state">${escapeHtml(variantStatus(variant))}</span>
        ${tokens}
      </div>
      <pre class="compare-body">${escapeHtml(variantPreview(variant))}</pre>
    </div>`;
}

function renderRun(run) {
  const div = document.createElement("div");
  div.className = "compare-run";
  div.style.setProperty("--pc", `var(--${run.provider})`);
  const cols = run.variants.map(variantCard).join("");
  div.innerHTML = `
    <div class="compare-run-head">
      <span class="badge">${escapeHtml(run.provider)}</span>
      <span class="compare-run-title">${escapeHtml(compactPath(run.path))}</span>
      <span class="seq" style="margin-left:auto">${escapeHtml(run.created_at?.slice(11, 19) ?? "")}</span>
    </div>
    <div class="compare-cols">${cols}</div>`;
  return div;
}

function renderCompare() {
  const list = document.getElementById("compare-list");
  const count = document.getElementById("compare-count");
  if (!list) return;
  const runs = Array.from(comparisons.values()).sort(
    (a, b) => b.sequence - a.sequence,
  );
  if (count) {
    count.textContent = String(runs.length);
    count.hidden = runs.length === 0;
  }
  list.innerHTML = "";
  if (!runs.length) {
    const empty = document.createElement("div");
    empty.className = "review-empty";
    empty.textContent = "no comparison runs yet";
    list.appendChild(empty);
    return;
  }
  for (const run of runs) list.appendChild(renderRun(run));
}

// Opens (or toggles) the inline model-picker form on a trace span node, then
// POSTs a comparison run for that span.
export function openCompareForm(span, node) {
  const existing = node.querySelector(".compare-form");
  if (existing) {
    existing.remove();
    return;
  }
  const models = [span.model, ...(ui.compareDefaults ?? [])]
    .map((m) => (m ?? "").trim())
    .filter(Boolean);
  const prefill = Array.from(new Set(models)).join(", ");

  const form = document.createElement("div");
  form.className = "compare-form";
  form.innerHTML = `
    <input class="compare-input" placeholder="models, comma-separated" value="${escapeHtml(prefill)}" />
    <button class="compare-go" type="button">compare</button>`;
  node.insertBefore(form, node.querySelector(".body"));

  const input = form.querySelector(".compare-input");
  const go = form.querySelector(".compare-go");
  const submit = async () => {
    const list = input.value
      .split(",")
      .map((m) => m.trim())
      .filter(Boolean);
    if (!list.length) return;
    go.disabled = true;
    go.textContent = "running…";
    try {
      const res = await fetch("/api/compare/run", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ span_id: span.id, models: list }),
      });
      if (!res.ok) throw new Error(String(res.status));
      form.remove();
      setTab("compare");
    } catch (err) {
      go.disabled = false;
      go.textContent = err.message === "404" ? "unavailable" : "failed";
      setTimeout(() => (go.textContent = "compare"), 1500);
    }
  };
  go.addEventListener("click", submit);
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter") submit();
  });
  input.focus();
  input.select();
}

export async function initCompare() {
  try {
    const data = await fetch("/api/compare").then((r) => r.json());
    ui.compareDefaults = data.default_models ?? [];
    comparisons.clear();
    for (const run of data.runs ?? []) comparisons.set(run.id, run);
    renderCompare();
    const es = new EventSource("/api/compare/events");
    es.addEventListener("compare", (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (msg.type === "reset") {
          comparisons.clear();
          for (const run of msg.runs ?? []) comparisons.set(run.id, run);
        } else if (msg.type === "upsert" && msg.run) {
          comparisons.set(msg.run.id, msg.run);
        }
        renderCompare();
      } catch (_) {}
    });
  } catch (_) {
    renderCompare();
  }
}
