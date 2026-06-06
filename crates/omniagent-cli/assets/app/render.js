// Builds the DOM node for one LLM span: header row, token flow bar, and the
// expandable body (rendered prompt/assistant cards plus raw request/response).

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
import { openIds } from "./state.js";

// --- span detail popup -------------------------------------------------------
// A span's detail opens in a modal rather than expanding inline (the trace pane
// is often a narrow rail). We MOVE the span's `.body` element into the modal
// (preserving its event listeners) and move it back on close.
const modal = document.getElementById("span-modal");
const modalBody = document.getElementById("span-modal-body");
const modalTitle = document.getElementById("span-modal-title");
let modalReturn = null; // { node, body }

function closeSpanModal() {
  if (modalReturn) {
    modalReturn.node.appendChild(modalReturn.body);
    modalReturn = null;
  }
  if (modal) modal.hidden = true;
}

export function openSpanModal(span, node) {
  // Clicking the same span again closes it.
  if (modalReturn && modalReturn.node === node) {
    closeSpanModal();
    return;
  }
  closeSpanModal();
  const body = node.querySelector(".body");
  if (!body || !modal) return;
  if (modalTitle) modalTitle.textContent = span.model ?? span.path;
  modalBody.appendChild(body);
  modalReturn = { node, body };
  modal.hidden = false;
}

document.getElementById("span-modal-close")?.addEventListener("click", closeSpanModal);
modal?.addEventListener("click", (e) => {
  if (e.target === modal) closeSpanModal();
});
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && modal && !modal.hidden) closeSpanModal();
});

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
  div.className = "span" + (openIds.has(span.id) ? " open" : "");
  div.style.setProperty("--pc", `var(--${span.provider})`);

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

  const reqMessages = requestMessages(span);
  const resBlocks = responseContent(span);
  const requestSummary = renderRequest(span, reqMessages);
  const message = renderMessage(resBlocks);
  const streamEvents = span.stream_events ?? [];
  // Successful spans show only the rendered prompt/assistant blocks; the raw
  // request/response dump is reserved for errors (and as a fallback when there
  // is nothing rendered to show, so the detail is never empty).
  const isError = !!span.error || (span.status != null && span.status >= 400);
  const showRaw = isError || (!requestSummary && !message);

  div.innerHTML = `
  <div class="row1">
    <span class="seq">#${span.sequence}</span>
    <span class="model">${escapeHtml(span.model ?? span.path)}</span>
    <span class="badge">${span.provider}</span>
  </div>
  <div class="row2">
    ${status}
    <span class="pill method"><span class="lc"></span>${escapeHtml(span.method ?? "HTTP")}</span>
    ${span.streaming ? '<span class="pill stream"><span class="lc"></span>stream</span>' : ""}
    ${reasonTok != null ? `<span class="pill" style="color:var(--anthropic)"><span class="lc"></span>reason ${fmtTok(reasonTok)}</span>` : ""}
    ${hasCache ? `<span class="pill" style="color:var(--openai)"><span class="lc"></span>cache ${fmtTok(cacheR || 0)}r${cacheW ? ` ${fmtTok(cacheW)}w` : ""}</span>` : ""}
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
  }
  <div class="body">
    ${requestSummary ? `<div class="seg"><div class="seg-head">prompt<span class="line"></span><span class="mini">${reqMessages.length} msgs</span></div><div class="msg">${requestSummary}</div></div>` : ""}
    ${message ? `<div class="seg"><div class="seg-head">assistant<span class="line"></span><span class="mini">${resBlocks.length} blocks</span></div><div class="msg">${message}</div></div>` : ""}
    ${
      showRaw
        ? `<div class="seg">
      <div class="seg-head">request<span class="line"></span><button class="copy" data-c="req">copy</button></div>
      <pre class="req"></pre>
    </div>
    <div class="seg">
      <div class="seg-head">response<span class="line"></span><button class="copy" data-c="res">copy</button></div>
      <pre class="res"></pre>
    </div>
    ${
      streamEvents.length
        ? `<div class="seg">
      <div class="seg-head">stream events<span class="line"></span><span class="mini">${streamEvents.length}</span><button class="copy" data-c="evt">copy</button></div>
      <pre class="evt"></pre>
    </div>`
        : ""
    }`
        : ""
    }
</div>`;

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
  const reqPre = div.querySelector("pre.req");
  if (reqPre) reqPre.innerHTML = hl(request);
  const resPre = div.querySelector("pre.res");
  if (resPre) resPre.innerHTML = hl(response);
  const evtPre = div.querySelector("pre.evt");
  if (evtPre) evtPre.innerHTML = hl(streamEvents);

  div.addEventListener("click", (e) => {
    if (e.target.closest(".copy")) return;
    openSpanModal(span, div);
  });
  div.querySelectorAll(".copy").forEach((btn) => {
    btn.addEventListener("click", () => {
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
      btn.style.color = "var(--green)";
      setTimeout(() => {
        btn.textContent = old;
        btn.style.color = "";
      }, 1100);
    });
  });
  return div;
}
