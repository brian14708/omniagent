// LiveView hook: native codex app-server conversation renderer.
//
// The codex backend speaks structured events (not a PTY byte stream), so this
// hook owns the whole middle pane — a transcript of conversation items, a
// status/token strip, an error banner, and a composer — driven entirely by
// push_event (mirroring how the Terminal and Traces hooks work, with
// phx-update="ignore" on the container so LiveView never patches inside it).
//
// Events from the server:
//   codex_init        {events:[{type, payload}]}   replay persisted backlog
//   codex_item        {phase, item}                 item lifecycle (started/completed)
//   codex_delta       {kind, item_id, delta}        streaming text (ephemeral)
//   codex_turn        {phase, turn|diff|plan}       turn lifecycle / aggregated diff / plan
//   codex_token_usage {...}                         token accounting
//   codex_error       {error:{message}, ...}        turn error
//   codex_status      {status}                      session online/offline

import { renderMarkdown } from "../markdown";

export const Codex = {
  mounted() {
    this.items = new Map(); // item_id -> { el, body, type }
    this.online = this.el.dataset.status === "online";
    this.build();

    this.handleEvent("codex_init", ({ events }) => {
      for (const { type, payload } of events || []) this.replay(type, payload);
      this.scrollToBottom();
    });
    this.handleEvent("codex_item", (p) => {
      this.onItem(p);
      this.scrollToBottom();
    });
    this.handleEvent("codex_delta", (p) => {
      this.onDelta(p);
      this.scrollToBottom();
    });
    this.handleEvent("codex_turn", (p) => this.onTurn(p));
    this.handleEvent("codex_token_usage", (p) => this.onTokenUsage(p));
    this.handleEvent("codex_error", (p) => this.onError(p));
    this.handleEvent("codex_status", ({ status }) =>
      this.setOnline(status === "online"),
    );
  },

  // ── Layout ────────────────────────────────────────────────────────────────
  build() {
    this.el.innerHTML = "";

    const strip = el("div", "oa-codex-strip");
    this.turnEl = el("span", "oa-codex-turn");
    this.turnEl.textContent = "idle";
    this.tokensEl = el("span", "oa-codex-tokens");
    strip.append(this.turnEl, this.tokensEl);

    this.errorEl = el("div", "oa-codex-error");
    this.errorEl.style.display = "none";

    this.log = el("div", "oa-codex-log");

    const form = document.createElement("form");
    form.className = "oa-codex-composer";
    this.input = document.createElement("textarea");
    this.input.className = "oa-input oa-codex-input";
    this.input.rows = 2;
    this.input.placeholder =
      "Message codex…  (Enter to send, Shift+Enter for newline)";
    this.sendBtn = document.createElement("button");
    this.sendBtn.type = "submit";
    this.sendBtn.className = "oa-btn primary";
    this.sendBtn.textContent = "Send";
    this.interruptBtn = document.createElement("button");
    this.interruptBtn.type = "button";
    this.interruptBtn.className = "oa-btn danger";
    this.interruptBtn.textContent = "Interrupt";
    this.interruptBtn.style.display = "none";
    form.append(this.input, this.sendBtn, this.interruptBtn);

    form.addEventListener("submit", (e) => {
      e.preventDefault();
      this.send();
    });
    this.input.addEventListener("keydown", (e) => {
      // Ignore Enter while an IME composition is active (CJK candidate
      // selection) so confirming a candidate doesn't send a half-composed line.
      if (
        e.key === "Enter" &&
        !e.shiftKey &&
        !e.isComposing &&
        e.keyCode !== 229
      ) {
        e.preventDefault();
        this.send();
      }
    });
    this.interruptBtn.addEventListener("click", () =>
      this.pushEvent("codex_interrupt", {}),
    );

    this.el.append(strip, this.errorEl, this.log, form);
    this.setOnline(this.online);
  },

  send() {
    const text = this.input.value.trim();
    if (text === "" || !this.online) return;
    // Don't echo locally: codex app-server emits the user input back as its own
    // `userMessage` item, which renders via onItem. A local echo would duplicate
    // it (and disagree with the persisted backlog on reconnect).
    this.pushEvent("codex_send", { text });
    this.input.value = "";
  },

  setOnline(online) {
    this.online = online;
    this.input.disabled = !online;
    this.sendBtn.disabled = !online;
    if (!online) this.interruptBtn.style.display = "none";
  },

  // ── Event handling ──────────────────────────────────────────────────────────
  replay(type, payload) {
    if (type === "codex_item") this.onItem(payload);
    else if (type === "codex_turn") this.onTurn(payload);
    else if (type === "codex_token_usage") this.onTokenUsage(payload);
    else if (type === "codex_error") this.onError(payload);
  },

  onItem({ phase, item } = {}) {
    if (!item || !item.id) return;
    const card = this.card(item.id, item.type);
    card.label.textContent = itemLabel(item.type);
    card.el.className = `oa-codex-item kind-${item.type || "unknown"}`;
    const done = phase === "completed";

    if (item.type === "agentMessage" || item.type === "reasoning") {
      // Stream raw text live (deltas append to a <pre>); on completion re-render
      // the final text as Markdown.
      const text =
        item.type === "agentMessage" ? item.text || "" : reasoningText(item);
      if (done) {
        const finalText = text || (card.stream ? card.stream.textContent : "");
        card.body.replaceChildren(renderMarkdown(finalText));
        card.stream = null;
      } else {
        this.setStreamText(card, text);
      }
    } else if (item.type === "commandExecution") {
      // Render the command immediately (on started too) so in-progress command
      // cards aren't empty; output streams in / is finalized from aggregatedOutput.
      this.renderCommand(card, item, done);
    } else {
      this.renderItem(card, item);
    }
    card.done = done;
    this.refreshVisibility(card);
  },

  onDelta({ kind, item_id, delta } = {}) {
    if (!item_id || kind === "file_change") return; // patches land on item/completed
    if (typeof delta !== "string" || !delta.length) return;
    const card = this.card(item_id, deltaType(kind));
    if (card.done) return; // already finalized; ignore late deltas
    if (kind === "command_output") {
      if (!card.outStream) {
        card.outStream = preBlock("", "oa-codex-output");
        card.body.append(card.outStream);
      }
      card.outStream.textContent += delta;
    } else {
      if (!card.stream) {
        card.stream = preBlock("", "oa-codex-text");
        card.body.append(card.stream);
      }
      card.stream.textContent += delta;
    }
    this.refreshVisibility(card);
  },

  // Hide a card whose body has no visible content (e.g. an empty agentMessage,
  // or a reasoning item codex never populated) and reveal it once text arrives.
  refreshVisibility(card) {
    card.el.classList.toggle("is-empty", card.body.textContent.trim() === "");
  },

  onTurn({ phase, turn } = {}) {
    if (phase === "started") {
      // A new turn supersedes any prior turn's error banner.
      this.clearError();
      this.turnEl.textContent = "running…";
      this.turnEl.dataset.state = "running";
      if (this.online) this.interruptBtn.style.display = "";
    } else if (phase === "completed") {
      const status = (turn && turn.status) || "completed";
      this.turnEl.textContent = status;
      this.turnEl.dataset.state = status;
      this.interruptBtn.style.display = "none";
    }
    // phase "diff"/"plan" carry aggregated turn state we don't separately card;
    // the per-item fileChange/plan cards already show the detail.
  },

  onTokenUsage(payload) {
    const total = findTokenTotal(payload);
    this.tokensEl.textContent =
      total == null ? "" : `${total.toLocaleString()} tokens`;
  },

  onError({ error } = {}) {
    const message = (error && error.message) || "codex error";
    this.errorEl.textContent = message;
    this.errorEl.style.display = "";
  },

  clearError() {
    this.errorEl.textContent = "";
    this.errorEl.style.display = "none";
  },

  // ── Item cards ──────────────────────────────────────────────────────────────
  card(itemId, type) {
    let entry = this.items.get(itemId);
    if (entry) return entry;
    const el_ = el("div", `oa-codex-item kind-${type || "unknown"}`);
    el_.dataset.itemId = itemId;
    const label = el("div", "oa-codex-item-label");
    label.textContent = itemLabel(type);
    const body = el("div", "oa-codex-item-body");
    el_.append(label, body);
    this.log.append(el_);
    entry = {
      el: el_,
      body,
      label,
      type,
      stream: null,
      outStream: null,
      done: false,
    };
    this.items.set(itemId, entry);
    return entry;
  },

  // Ensures the card holds a single text <pre> and sets its content (non-empty
  // only, so an empty completed payload doesn't wipe streamed deltas).
  setStreamText(card, text) {
    if (!card.stream) {
      card.stream = preBlock("", "oa-codex-text");
      card.body.replaceChildren(card.stream);
    }
    if (text) card.stream.textContent = text;
  },

  // commandExecution: command line + output (streamed live, or aggregatedOutput
  // on completion) + a status/exit footer once done.
  renderCommand(card, item, done) {
    const streamed = card.outStream ? card.outStream.textContent : "";
    card.body.replaceChildren();
    card.body.append(codeBlock(commandLine(item.command)));
    if (done) {
      const out = item.aggregatedOutput || streamed;
      if (out)
        card.body.append(
          collapsible(
            `output · ${lineCount(out)} lines`,
            preBlock(out, "oa-codex-output"),
          ),
        );
      const exit = item.exitCode;
      card.body.append(
        metaLine(
          `${item.status || "done"}${exit == null ? "" : ` · exit ${exit}`}`,
        ),
      );
      card.outStream = null;
    } else {
      card.outStream = preBlock(streamed, "oa-codex-output");
      card.body.append(card.outStream);
    }
  },

  // Structured (non-streaming) items rendered on both started and completed.
  renderItem(card, item) {
    card.body.replaceChildren();
    switch (item.type) {
      case "userMessage":
        card.body.append(renderMarkdown(itemText(item)));
        break;
      case "fileChange":
        for (const change of item.changes || []) {
          card.body.append(
            metaLine(`${change.kind || "edit"} ${change.path || ""}`),
          );
          if (change.diff)
            card.body.append(
              collapsible(
                `diff · ${lineCount(change.diff)} lines`,
                preBlock(change.diff, "oa-codex-diff"),
              ),
            );
        }
        break;
      case "plan": {
        const list = el("ul", "oa-codex-plan");
        for (const step of item.plan || item.steps || []) {
          const li = document.createElement("li");
          li.textContent =
            typeof step === "string"
              ? step
              : `${step.status ? `[${step.status}] ` : ""}${step.step || step.text || ""}`;
          list.append(li);
        }
        card.body.append(list);
        break;
      }
      default:
        card.body.append(
          collapsible("raw", preBlock(safeJson(item), "oa-codex-raw")),
        );
    }
  },

  scrollToBottom() {
    // Coalesce to one layout read/write per frame: deltas arrive token-by-token,
    // and reading scrollHeight per delta would thrash layout on the hot path.
    if (this._scrollQueued) return;
    this._scrollQueued = true;
    requestAnimationFrame(() => {
      this._scrollQueued = false;
      this.log.scrollTop = this.log.scrollHeight;
    });
  },
};

// ── Helpers ───────────────────────────────────────────────────────────────────
function el(tag, className) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  return node;
}

function codeBlock(text) {
  return preBlock(text, "oa-codex-cmd");
}

function preBlock(text, className) {
  const pre = el("pre", className);
  pre.textContent = typeof text === "string" ? text : safeJson(text);
  return pre;
}

function metaLine(text) {
  const node = el("div", "oa-codex-meta");
  node.textContent = text;
  return node;
}

// Wraps a verbose node (command output, diff, raw payload) in a native
// collapsed-by-default <details> so long tool results don't dominate the
// transcript. `summaryText` is the clickable header.
function collapsible(summaryText, node, open = false) {
  const details = el("details", "oa-codex-collapse");
  details.open = open;
  const summary = document.createElement("summary");
  summary.textContent = summaryText;
  details.append(summary, node);
  return details;
}

function lineCount(text) {
  if (typeof text !== "string" || text === "") return 0;
  return text.split("\n").length;
}

function deltaType(kind) {
  if (kind === "agent_message") return "agentMessage";
  if (kind === "reasoning" || kind === "reasoning_summary") return "reasoning";
  if (kind === "command_output") return "commandExecution";
  if (kind === "file_change") return "fileChange";
  return "unknown";
}

function itemLabel(type) {
  switch (type) {
    case "agentMessage":
      return "codex";
    case "userMessage":
      return "you";
    case "reasoning":
      return "reasoning";
    case "commandExecution":
      return "command";
    case "fileChange":
      return "file change";
    case "plan":
      return "plan";
    default:
      return type || "event";
  }
}

function commandLine(command) {
  if (Array.isArray(command)) return command.join(" ");
  return typeof command === "string" ? command : safeJson(command);
}

function itemText(item) {
  if (typeof item.text === "string") return item.text;
  if (Array.isArray(item.content)) {
    return item.content.map(partText).join("");
  }
  return "";
}

function reasoningText(item) {
  const parts = [];
  for (const field of [item.summary, item.content]) {
    if (Array.isArray(field)) {
      for (const c of field) parts.push(partText(c));
    }
  }
  return parts.join("\n");
}

// A content-array element may be a string, an object {text}, or null/other.
// Guard the object case: `typeof null === "object"`, so `c.text` on null throws.
function partText(c) {
  if (typeof c === "string") return c;
  return (c && c.text) || "";
}

// Token-usage payload shape varies across codex versions; pull the first
// plausible total we can find rather than hard-coding one spelling.
function findTokenTotal(payload) {
  const usage = payload?.tokenUsage || payload?.token_usage || payload;
  if (!usage || typeof usage !== "object") return null;
  const candidates = [
    usage.total?.totalTokens,
    usage.total?.total_tokens,
    usage.total,
    usage.totalTokens,
    usage.total_tokens,
  ];
  for (const c of candidates) if (typeof c === "number") return c;
  return null;
}

function safeJson(value) {
  try {
    return JSON.stringify(value, null, 2);
  } catch (_) {
    return String(value);
  }
}
