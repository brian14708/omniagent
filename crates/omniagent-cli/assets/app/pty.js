// Terminal over WebSocket: ghostty-web bridged to the agent PTY.
import {
  FitAddon,
  Terminal,
  init,
} from "https://cdn.jsdelivr.net/npm/ghostty-web@0.4.0/dist/ghostty-web.js";

// Module-scoped so `refit()` can be called from mode/layout changes.
let activeFit = null;
let activeResize = null;

// Re-fit the terminal to its container and tell the PTY the new size. Safe to
// call before the terminal is initialized.
export function refit() {
  try {
    activeFit?.fit();
    activeResize?.();
  } catch (_) {}
}

export async function initPty() {
  await init();

  const term = new Terminal({
    cursorBlink: true,
    fontSize: 13,
    fontFamily: '"JetBrains Mono", ui-monospace, Menlo, monospace',
    convertEol: false,
    theme: {
      background: "#060708",
      foreground: "#e2e6ee",
      cursor: "#ffb454",
      cursorAccent: "#060708",
      selectionBackground: "rgba(255,180,84,.22)",
      black: "#11141b",
      brightBlack: "#535b6b",
      red: "#ff6b6b",
      brightRed: "#ff8f8f",
      green: "#58d3a0",
      brightGreen: "#7ee8bd",
      yellow: "#ffb454",
      brightYellow: "#ffd9a3",
      blue: "#7aa2ff",
      brightBlue: "#a3c0ff",
      magenta: "#d98b6a",
      brightMagenta: "#e8a98f",
      cyan: "#4fc8a3",
      brightCyan: "#7ee8bd",
      white: "#c5cbd6",
      brightWhite: "#e2e6ee",
    },
  });
  const fit = new FitAddon();
  term.loadAddon(fit);
  term.open(document.getElementById("term"));
  fit.fit();
  fit.observeResize?.();
  activeFit = fit;

  const wsProto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${wsProto}://${location.host}/ws/pty`);
  ws.binaryType = "arraybuffer";
  const statusEl = document.getElementById("status");
  const pulseEl = document.getElementById("pulse");
  const wsDot = document.getElementById("ws-dot");

  let opened = false;
  ws.onopen = () => {
    opened = true;
    statusEl.textContent = "connected";
    pulseEl.className = "pulse ok";
    wsDot.classList.add("live");
    document.body.dataset.ws = "up";
    sendResize();
  };
  ws.onclose = () => {
    wsDot.classList.remove("live");
    document.body.dataset.ws = "down";
    pulseEl.className = "pulse bad";
    if (opened) {
      // The PTY bridge closing means the agent process ended the session.
      statusEl.textContent = "agent exited";
      term.write("\r\n\x1b[38;5;245m──────── agent exited ────────\x1b[0m\r\n");
    } else {
      statusEl.textContent = "disconnected";
    }
  };
  ws.onmessage = (ev) => {
    if (typeof ev.data === "string") term.write(ev.data);
    else term.write(new Uint8Array(ev.data));
  };
  term.onData((data) => {
    if (ws.readyState === WebSocket.OPEN)
      ws.send(new TextEncoder().encode(data));
  });
  term.onResize?.(() => sendResize());
  function sendResize() {
    if (ws.readyState !== WebSocket.OPEN) return;
    ws.send(
      JSON.stringify({ type: "resize", rows: term.rows, cols: term.cols }),
    );
  }
  activeResize = sendResize;
  window.addEventListener("resize", () => {
    fit.fit();
    sendResize();
  });
}
