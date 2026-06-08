// Builds the DOM node for one LLM span: header row, token flow bar, and the
// detail body (rendered prompt/assistant cards plus raw request/response).
//
// Clicking a span dispatches a `trace:open` event; the Traces hook moves the
// detail body into a popup overlay rather than expanding it inline.

import {
  escapeHtml,
  latClass,
  fmtLat,
  fmtTok,
  compactPath,
  hl,
} from "./format.js";
import {
  readUsage,
  requestMessages,
  requestTools,
  responseContent,
} from "./parse.js";

function renderRequest(span, allMessages) {
  const messages = allMessages.slice(-8);
  const tools = requestTools(span);
  if (!messages.length && !tools.length) return "";
  const cards = [];
  for (const m of messages) {
    const title = `${m.role}${m.tool ? " · " + m.tool : ""}`;
    cards.push(
      `<div class="cblock req"><div class="ch">${escapeHtml(title)}</div><pre class="ctext">${escapeHtml(m.text)}</pre></div>`,
    );
  }
  if (tools.length) {
    const toolText = tools
      .map((t) => `${t.name}\n${JSON.stringify(t.raw, null, 2)}`)
      .join("\n\n");
    cards.push(
      `<div class="cblock tool"><div class="ch">tools<span class="tname">${tools.length}</span></div><pre class="ctext code">${escapeHtml(toolText)}</pre></div>`,
    );
  }
  return cards.join("");
}

// Render reconstructed content blocks as readable cards. Returns "" when
// there is nothing renderable (e.g. a raw/streamed fallback body), so the
// raw response JSON below still tells the whole story.
function renderMessage(blocks) {
  if (!blocks.length) return "";
  const cards = [];
  for (const b of blocks) {
    const type = b?.type;
    if (type === "text" && b.text) {
      cards.push(
        `<div class="cblock"><div class="ch">text</div><pre class="ctext">${escapeHtml(b.text)}</pre></div>`,
      );
    } else if (type === "thinking" && b.thinking) {
      cards.push(
        `<div class="cblock think"><div class="ch">thinking</div><pre class="ctext">${escapeHtml(b.thinking)}</pre></div>`,
      );
    } else if (type === "tool_use") {
      const input =
        typeof b.input === "string"
          ? b.input
          : JSON.stringify(b.input ?? {}, null, 2);
      cards.push(
        `<div class="cblock tool"><div class="ch">tool call<span class="tname">${escapeHtml(b.name ?? "")}</span></div><pre class="ctext code">${escapeHtml(input)}</pre></div>`,
      );
    } else if (type === "tool_result") {
      const output =
        typeof b.content === "string"
          ? b.content
          : JSON.stringify(b.content ?? {}, null, 2);
      cards.push(
        `<div class="cblock result"><div class="ch">tool result<span class="tname">${escapeHtml(b.tool_use_id ?? "")}</span></div><pre class="ctext code">${escapeHtml(output)}</pre></div>`,
      );
    }
  }
  return cards.join("");
}

export function buildNode(span) {
  const div = document.createElement("div");
  div.className = "span";
  div.style.setProperty(
    "--pc",
    `var(--prov-${span.provider}, var(--prov-default))`,
  );

  const { inTok, outTok, totalTok, reasonTok, cacheR, cacheW } =
    readUsage(span);
  const flowTotal = (inTok || 0) + (outTok || 0);
  const inPct = flowTotal ? ((inTok || 0) / flowTotal) * 100 : 0;
  const hasFlow =
    inTok != null || outTok != null || totalTok != null || reasonTok != null;
  const hasCache = cacheR != null || cacheW != null;

  const status = span.error
    ? `<span class="err-tag"><span class="lc"></span>${escapeHtml(span.error)}</span>`
    : `<span class="pill lat ${latClass(span.latency_ms)}"><span class="lc"></span>${span.status} · ${fmtLat(span.latency_ms)}</span>`;

  // Compact stat shown inline in the collapsed (single-line) row. The full
  // pills + flow bar live in row2/.flow below, hidden in the list but cloned
  // into the popup on open.
  const row1Stat = span.error
    ? `<span class="r1stat err">${escapeHtml(span.error)}</span>`
    : `<span class="r1stat"><span class="lat ${latClass(span.latency_ms)}">${span.status ?? ""} · ${fmtLat(span.latency_ms)}</span>${hasFlow ? `<span class="tok">${fmtTok(inTok)}→${fmtTok(outTok)}</span>` : ""}</span>`;

  // Badges are precomputed server-side (by the proxy) and arrive on the span as
  // `labels`, so the row needs no response parsing.
  const labels = Array.isArray(span.labels) ? span.labels : [];

  div.innerHTML = `
  <div class="row1">
    <span class="model">${escapeHtml(span.model ?? span.path)}</span>
    ${labels
      .map(
        (l) =>
          `<span class="rtype ${escapeHtml(l.cls ?? "")}" title="${escapeHtml(l.text ?? "")}">${escapeHtml(l.text ?? "")}</span>`,
      )
      .join("")}
    ${row1Stat}
  </div>
  <div class="row2">
    ${status}
    <span class="pill method"><span class="lc"></span>${escapeHtml(span.method ?? "HTTP")}</span>
    ${span.streaming ? '<span class="pill stream"><span class="lc"></span>stream</span>' : ""}
    ${reasonTok != null ? `<span class="pill" style="color:var(--prov-anthropic)"><span class="lc"></span>reason ${fmtTok(reasonTok)}</span>` : ""}
    ${hasCache ? `<span class="pill" style="color:var(--prov-openai)"><span class="lc"></span>cache ${fmtTok(cacheR || 0)}r${cacheW ? ` ${fmtTok(cacheW)}w` : ""}</span>` : ""}
    <span class="seq" style="margin-left:auto">${escapeHtml(compactPath(span.path))}</span>
  </div>
  ${
    hasFlow
      ? `
  <div class="flow">
    <span class="io">in <b>${fmtTok(inTok)}</b></span>
    <span class="bar"><i class="in" style="width:${inPct}%"></i><i class="out" style="width:${100 - inPct}%"></i></span>
    <span class="io"><b>${fmtTok(outTok)}</b> out</span>
    ${totalTok != null ? `<span class="io">total <b>${fmtTok(totalTok)}</b></span>` : ""}
  </div>`
      : ""
  }`;

  // Title shown in the popup header (plain text; the modal uses textContent).
  div.dataset.title = `#${span.sequence} · ${span.model ?? span.path ?? ""}${span.provider ? " · " + span.provider : ""}`;

  // The detail body — rendered prompt/assistant cards plus the (highlighted)
  // raw request/response — is hidden in the list and only shown when the span
  // is opened in the popup. Building it (especially `hl()` over full
  // request/response bodies) is expensive, so defer it to first open. This is
  // what keeps `trace_init`/session switches cheap for sessions with many or
  // large spans, where eagerly building every body froze the UI.
  div._buildBody = () => buildBody(span);

  // Clicking the span opens its detail in a popup (handled by the Traces hook,
  // which listens for this bubbling event).
  div.addEventListener("click", (e) => {
    if (e.target.closest(".copy")) return;
    div.dispatchEvent(new CustomEvent("trace:open", { bubbles: true }));
  });
  return div;
}

// Builds the (heavy) detail body for one span: prompt/assistant cards and the
// collapsible, syntax-highlighted raw request/response/stream segments. Called
// lazily by the Traces hook the first time a span is opened.
function buildBody(span) {
  const reqMessages = requestMessages(span);
  const resBlocks = responseContent(span);
  const requestSummary = renderRequest(span, reqMessages);
  const message = renderMessage(resBlocks);
  const streamEvents = span.stream_events ?? [];
  const isError = !!span.error || (span.status != null && span.status >= 400);
  // Raw request/response are always available (collapsible), but expanded by
  // default for errors or when there's nothing reconstructed to show.
  const rawOpen = isError || (!requestSummary && !message);

  const body = document.createElement("div");
  body.className = "body";
  body.innerHTML = `
    ${requestSummary ? `<div class="seg"><div class="seg-head">prompt<span class="line"></span><span class="mini">${reqMessages.length} msgs</span></div><div class="msg">${requestSummary}</div></div>` : ""}
    ${message ? `<div class="seg"><div class="seg-head">assistant<span class="line"></span><span class="mini">${resBlocks.length} blocks</span></div><div class="msg">${message}</div></div>` : ""}
    <details class="seg raw"${rawOpen ? " open" : ""}>
      <summary class="seg-head">request<span class="line"></span><button class="copy" data-c="req">copy</button></summary>
      <pre class="req"></pre>
    </details>
    <details class="seg raw"${rawOpen ? " open" : ""}>
      <summary class="seg-head">response<span class="line"></span><button class="copy" data-c="res">copy</button></summary>
      <pre class="res"></pre>
    </details>
    ${
      streamEvents.length
        ? `<details class="seg raw">
      <summary class="seg-head">stream events<span class="line"></span><span class="mini">${streamEvents.length}</span><button class="copy" data-c="evt">copy</button></summary>
      <pre class="evt"></pre>
    </details>`
        : ""
    }`;

  const request = {
    method: span.method ?? "",
    request_base_url: span.request_base_url,
    upstream_base_url: span.upstream_base_url,
    path: span.path,
    headers: span.request_headers ?? {},
    body: span.request,
  };
  const response = {
    status: span.status,
    latency_ms: span.latency_ms,
    headers: span.response_headers ?? {},
    body: span.response,
    usage: span.usage ?? {},
    stream_events: streamEvents,
  };

  const reqPre = body.querySelector("pre.req");
  if (reqPre) reqPre.innerHTML = hl(request);
  const resPre = body.querySelector("pre.res");
  if (resPre) resPre.innerHTML = hl(response);
  const evtPre = body.querySelector("pre.evt");
  if (evtPre) evtPre.innerHTML = hl(streamEvents);

  body.querySelectorAll(".copy").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      // The copy button lives inside a <summary>; stop the click from toggling
      // the <details> open/closed.
      e.preventDefault();
      e.stopPropagation();
      const payload =
        btn.dataset.c === "req"
          ? request
          : btn.dataset.c === "evt"
            ? streamEvents
            : response;
      const text = JSON.stringify(payload, null, 2);
      navigator.clipboard?.writeText(text);
      const old = btn.textContent;
      btn.textContent = "copied";
      btn.style.color = "var(--prov-green, #58d3a0)";
      setTimeout(() => {
        btn.textContent = old;
        btn.style.color = "";
      }, 1100);
    });
  });
  return body;
}
