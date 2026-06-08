// LiveView hook: trace pane controller. Renders each LLM span node exactly once
// (cached in `nodeById`) and handles search by toggling visibility rather than
// rebuilding. Spans arrive from the server: a `trace_init` batch on mount, then
// incremental `trace_span` events. Adapted from the 282d628 SSE controller.

import { spanSearchHaystack } from "./parse.js";
import { buildNode } from "./render.js";

export const Traces = {
  mounted() {
    this.spans = [];
    this.seen = new Set();
    this.nodeById = new Map();
    this.search = "";
    this.emptyEl = this.el.querySelector("[data-trace-empty]");

    this.handleEvent("trace_init", ({ spans }) => {
      for (const span of spans ?? []) this.ingest(span);
    });
    this.handleEvent("trace_span", (span) => this.ingest(span));

    // Search box lives outside the hook element (in the section header).
    this.searchEl = document.getElementById("trace-search");
    if (this.searchEl) {
      this.onSearch = () => {
        clearTimeout(this.searchTimer);
        this.searchTimer = setTimeout(() => {
          this.search = this.searchEl.value.trim().toLowerCase();
          this.applyVisibility();
        }, 120);
      };
      this.searchEl.addEventListener("input", this.onSearch);
    }

    this.buildModal();
    // Spans dispatch `trace:open` (bubbling) when clicked; show it in the popup.
    this.onOpen = (e) => {
      const span = e.target.closest(".span");
      if (span) this.openModal(span);
    };
    this.el.addEventListener("trace:open", this.onOpen);
  },

  // A single popup overlay, appended to <body> so it isn't clipped by the
  // trace pane's overflow. The clicked span's detail body is moved in on open
  // and moved back on close, preserving its copy-button listeners.
  buildModal() {
    this.overlay = document.createElement("div");
    this.overlay.className = "oa-trace-modal-overlay";
    this.overlay.hidden = true;
    this.overlay.innerHTML = `
      <div class="oa-trace-modal oa-traces" role="dialog" aria-modal="true">
        <div class="oa-trace-modal-head">
          <span class="oa-trace-modal-title"></span>
          <button class="oa-trace-modal-close" type="button" aria-label="Close">esc</button>
        </div>
        <div class="oa-trace-modal-summary"></div>
        <div class="oa-trace-modal-content"></div>
      </div>`;
    document.body.appendChild(this.overlay);
    this.modalTitle = this.overlay.querySelector(".oa-trace-modal-title");
    this.modalSummary = this.overlay.querySelector(".oa-trace-modal-summary");
    this.modalContent = this.overlay.querySelector(".oa-trace-modal-content");

    this.overlay.addEventListener("click", (e) => {
      if (e.target === this.overlay) this.closeModal();
    });
    this.overlay
      .querySelector(".oa-trace-modal-close")
      .addEventListener("click", () => this.closeModal());
    this.onKey = (e) => {
      if (e.key === "Escape") this.closeModal();
    };
  },

  openModal(span) {
    // The detail body is built lazily (see render.js) the first time a span is
    // opened, then kept on the span (hidden in the list) for re-opens.
    let body = span.querySelector(":scope > .body");
    if (!body) {
      body = span._buildBody?.();
      if (!body) return;
      span.appendChild(body);
    }
    this.restoreBody();
    this.activeSpan = span;
    this.activeBody = body;
    this.modalTitle.textContent = span.dataset.title || "trace";
    // Clone the status/flow rows for context (no listeners on them).
    this.modalSummary.innerHTML = "";
    for (const sel of [":scope > .row2", ":scope > .flow"]) {
      const el = span.querySelector(sel);
      if (el) this.modalSummary.appendChild(el.cloneNode(true));
    }
    this.modalContent.appendChild(body);
    this.overlay.hidden = false;
    document.addEventListener("keydown", this.onKey);
  },

  // Return the detail body to its span so the span can be reopened later.
  restoreBody() {
    if (this.activeSpan && this.activeBody)
      this.activeSpan.appendChild(this.activeBody);
    this.activeSpan = null;
    this.activeBody = null;
  },

  closeModal() {
    if (this.overlay.hidden) return;
    this.overlay.hidden = true;
    this.restoreBody();
    document.removeEventListener("keydown", this.onKey);
  },

  spanKey(span) {
    return span.id ?? span.external_id ?? `${span.sequence}`;
  },

  passes(span) {
    if (!this.search) return true;
    return spanSearchHaystack(span).includes(this.search);
  },

  ingest(span) {
    if (!span) return;
    const key = this.spanKey(span);
    if (this.seen.has(key)) return;
    this.seen.add(key);

    // Keep newest-first by sequence.
    this.spans.unshift(span);
    const node = buildNode(span);
    this.nodeById.set(key, node);
    node.classList.toggle("hidden", !this.passes(span));

    // Insert above the first existing span node (newest on top).
    const firstSpan = this.el.querySelector(".span");
    this.el.insertBefore(node, firstSpan);
    this.updateEmpty();
  },

  applyVisibility() {
    for (const span of this.spans) {
      const node = this.nodeById.get(this.spanKey(span));
      if (node) node.classList.toggle("hidden", !this.passes(span));
    }
    this.updateEmpty();
  },

  updateEmpty() {
    if (!this.emptyEl) return;
    const anyVisible = this.spans.some((s) => this.passes(s));
    this.emptyEl.style.display = anyVisible ? "none" : "";
  },

  destroyed() {
    if (this.searchEl && this.onSearch)
      this.searchEl.removeEventListener("input", this.onSearch);
    clearTimeout(this.searchTimer);
    document.removeEventListener("keydown", this.onKey);
    this.overlay?.remove();
  },
};
