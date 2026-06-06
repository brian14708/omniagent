// Human-review panel: pending response approvals streamed over SSE. Each item
// includes request context, but only the response is gated.

import { escapeHtml, compactPath, fmtLat, safeJson } from "./format.js";
import {
  requestMessages,
  requestTools,
  previewText,
  responseContent,
} from "./parse.js";
import { reviews, ui } from "./state.js";
import { setTab } from "./modes.js";

function requestPreview(item) {
  const messages = requestMessages(item);
  const tools = requestTools(item);
  const text = messages
    .slice(-4)
    .map((m) => `${m.role}${m.tool ? " · " + m.tool : ""}\n${m.text}`)
    .join("\n\n");
  const toolText = tools.length
    ? "\n\nTOOLS\n" + tools.map((t) => t.name).join(", ")
    : "";
  return (text + toolText).trim() || safeJson(item.request).slice(0, 1200);
}

function responsePreview(item) {
  return (
    previewText(responseContent(item), 1200) ||
    safeJson(item.response).slice(0, 1200)
  );
}

function renderReviewItem(item) {
  const div = document.createElement("div");
  div.className = "review-item";
  div.style.setProperty("--pc", `var(--${item.provider})`);
  const labels = { approve: "release", retry: "regenerate", reject: "reject" };
  const model = item.model ?? "";
  const attempt = item.attempt > 1 ? ` · attempt ${item.attempt}` : "";
  const status =
    `${item.status ?? "?"} · ${item.latency_ms != null ? fmtLat(item.latency_ms) : "pending"}`;
  div.innerHTML = `
    <div class="review-top">
      <span class="badge">${escapeHtml(item.provider)}</span>
      <span class="pill method"><span class="lc"></span>${escapeHtml(item.method ?? "HTTP")}</span>
      <span class="review-title">response approval${attempt} · ${escapeHtml(item.model ?? item.path)}</span>
    </div>
    <div class="row2">
      <span class="pill lat mid"><span class="lc"></span>${escapeHtml(status)}</span>
      ${item.streaming ? '<span class="pill stream"><span class="lc"></span>stream</span>' : ""}
      <span class="seq" style="margin-left:auto">${escapeHtml(compactPath(item.path))}</span>
    </div>
    <div class="review-preview">
      <div class="review-subhead">request context</div>
      <pre>${escapeHtml(requestPreview(item))}</pre>
      <div class="review-subhead">response to approve</div>
      <pre>${escapeHtml(responsePreview(item))}</pre>
    </div>
    <div class="review-actions">
      <input class="model-edit" placeholder="retry model override" value="${escapeHtml(model)}" />
      <button class="approve" data-a="approve">${labels.approve}</button>
      <button class="retry" data-a="retry">${labels.retry}</button>
      <button class="reject" data-a="reject">${labels.reject}</button>
    </div>`;
  div.querySelectorAll("button[data-a]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const action = btn.dataset.a;
      const model = div.querySelector(".model-edit")?.value?.trim() || null;
      const payload =
        action === "reject"
          ? { action: "reject", message: "rejected by operator" }
          : action === "retry"
            ? { action: "retry", model }
            : { action: "approve" };
      btn.disabled = true;
      try {
        const res = await fetch(
          `/api/review/${encodeURIComponent(item.id)}/decision`,
          {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(payload),
          },
        );
        if (!res.ok) throw new Error(String(res.status));
        reviews.delete(item.id);
        renderReviews();
      } catch (err) {
        btn.disabled = false;
        btn.textContent = "failed";
        setTimeout(() => (btn.textContent = labels[action] ?? action), 1200);
      }
    });
  });
  return div;
}

function renderReviews() {
  const list = document.getElementById("review-list");
  const count = document.getElementById("review-count");
  const items = Array.from(reviews.values()).sort(
    (a, b) => a.sequence - b.sequence,
  );
  count.textContent = String(items.length);
  count.hidden = items.length === 0;
  list.innerHTML = "";
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "review-empty";
    empty.textContent = ui.reviewEnabled
      ? "no pending response approvals"
      : "review disabled";
    list.appendChild(empty);
    return;
  }
  for (const item of items) list.appendChild(renderReviewItem(item));
  // Newest approvals are appended at the bottom — keep them in view.
  list.scrollTop = list.scrollHeight;
}

export async function initReviews() {
  try {
    const data = await fetch("/api/review").then((r) => r.json());
    ui.reviewEnabled = !!data.enabled;
    reviews.clear();
    for (const item of data.items ?? []) reviews.set(item.id, item);
    renderReviews();
    if (!ui.reviewEnabled && reviews.size === 0) return;
    const reviewEs = new EventSource("/api/review/events");
    reviewEs.addEventListener("review", (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        let arrived = false;
        if (msg.type === "reset") {
          reviews.clear();
          for (const item of msg.items ?? []) reviews.set(item.id, item);
        } else if (msg.type === "upsert") {
          arrived = !reviews.has(msg.item.id);
          reviews.set(msg.item.id, msg.item);
        } else if (msg.type === "remove" && msg.id) reviews.delete(msg.id);
        renderReviews();
        // A new approval gates live traffic — pull focus to the queue.
        if (arrived && ui.reviewEnabled) setTab("review");
      } catch (_) {}
    });
  } catch (_) {
    ui.reviewEnabled = false;
    renderReviews();
  }
}
