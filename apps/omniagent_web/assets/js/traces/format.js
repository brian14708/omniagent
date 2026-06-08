// Pure formatting + escaping helpers. No DOM, no shared state.
// Ported verbatim from the 282d628 self-hosted frontend.

export const fmtTok = (n) =>
  n == null
    ? "?"
    : n >= 1000
      ? (n / 1000).toFixed(n >= 10000 ? 0 : 1) + "k"
      : "" + n;

export const fmtLat = (ms) =>
  ms == null
    ? "?"
    : ms >= 1000
      ? (ms / 1000).toFixed(ms >= 10000 ? 0 : 1) + "s"
      : ms + "ms";

export const latClass = (ms) =>
  ms == null ? "" : ms < 1500 ? "fast" : ms < 6000 ? "mid" : "slow";

export const compactPath = (path) =>
  String(path || "")
    .replace(/^\/v1\//, "/")
    .replace(/^\/v1beta\//, "/")
    .replace(/^\/v1alpha\//, "/");

export function safeJson(value) {
  try {
    return JSON.stringify(value);
  } catch (_) {
    return String(value ?? "");
  }
}

export function escapeHtml(s) {
  return String(s).replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
}

// minimal JSON syntax highlighter
export function hl(value) {
  let json = JSON.stringify(value, null, 2);
  if (json === undefined) return "";
  json = json
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
  return json.replace(
    /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?)/g,
    (m) => {
      let cls = "n";
      if (/^"/.test(m)) cls = /:$/.test(m) ? "k" : "s";
      else if (/true|false|null/.test(m)) cls = "b";
      return `<span class="${cls}">${m}</span>`;
    },
  );
}
