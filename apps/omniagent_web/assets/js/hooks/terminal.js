// LiveView hook: ghostty-web terminal emulator bridged to the agent PTY.
//
// Output (live `pty_output`, the `pty_backlog` replay, and `pty_exit`) is
// pushed from the server via push_event and written to the emulator as raw
// bytes — ghostty interprets the ANSI/alt-screen control sequences. Input and
// resize are pushed back up to the LiveView, which relays them to the CLI.
//
// ghostty-web is bundled from npm (assets/package.json); its ESM build inlines
// the VT WASM as a data: URL, so no separate .wasm needs to be served.
import { Terminal as GhosttyTerminal, FitAddon, init } from "ghostty-web";

const THEME = {
  background: "#0a0b0d",
  foreground: "#e7e9ee",
  cursor: "#7c86ff",
  cursorAccent: "#0a0b0d",
  // Solid iris highlight with dark text. Without an explicit
  // selectionForeground, selected glyphs keep their own (often dark) colour and
  // become unreadable over a dark highlight.
  selectionBackground: "#7c86ff",
  selectionForeground: "#0a0b0d",
  black: "#161a20",
  brightBlack: "#535b6b",
  red: "#f0616d",
  brightRed: "#ff8f8f",
  green: "#4ec9a0",
  brightGreen: "#7ee8bd",
  yellow: "#e3a24a",
  brightYellow: "#f0bd72",
  blue: "#7aa2ff",
  brightBlue: "#a3c0ff",
  magenta: "#d99873",
  brightMagenta: "#e8a98f",
  cyan: "#4fc8a3",
  brightCyan: "#7ee8bd",
  white: "#c5cbd6",
  brightWhite: "#e7e9ee",
};

// ghostty-web's keydown fast-path emits the *unmodified* sequence for these
// special keys even when Shift is held, so Shift+<key> silently behaves like the
// bare key (e.g. Shift+Tab sends "\t" instead of back-tab). We send the correct
// xterm modifier-encoded sequences ourselves. Only pure Shift is affected:
// Ctrl/Alt/Meta combinations and the arrow keys already route through ghostty's
// real key encoder. Shift+Enter/Backspace/Escape are intentionally omitted —
// they have no standard legacy sequence and conventionally act as the bare key.
const SHIFT_KEY_SEQUENCES = {
  Tab: "\x1b[Z",
  Home: "\x1b[1;2H",
  End: "\x1b[1;2F",
  Insert: "\x1b[2;2~",
  Delete: "\x1b[3;2~",
  PageUp: "\x1b[5;2~",
  PageDown: "\x1b[6;2~",
  F1: "\x1b[1;2P",
  F2: "\x1b[1;2Q",
  F3: "\x1b[1;2R",
  F4: "\x1b[1;2S",
  F5: "\x1b[15;2~",
  F6: "\x1b[17;2~",
  F7: "\x1b[18;2~",
  F8: "\x1b[19;2~",
  F9: "\x1b[20;2~",
  F10: "\x1b[21;2~",
  F11: "\x1b[23;2~",
  F12: "\x1b[24;2~",
};

export const Terminal = {
  mounted() {
    // The dynamic import resolves asynchronously, so output that arrives before
    // the emulator is ready is buffered and flushed once it is.
    this.ready = false;
    this.pending = [];
    this.term = null;
    this.fit = null;

    const write = (data) => {
      if (typeof data !== "string" || data.length === 0) return;
      if (this.ready) this.term.write(data);
      else this.pending.push(data);
    };

    this.handleEvent("pty_backlog", ({ data }) => write(data));
    this.handleEvent("pty_output", ({ data }) => write(data));
    this.handleEvent("pty_exit", ({ code }) =>
      write(
        `\r\n\x1b[38;5;245m──────── agent exited ${code ?? ""} ────────\x1b[0m\r\n`,
      ),
    );

    this.boot(write);
  },

  async boot(write) {
    await init();

    const term = new GhosttyTerminal({
      cursorBlink: true,
      fontSize: 13,
      fontFamily: '"JetBrains Mono", ui-monospace, Menlo, monospace',
      convertEol: false,
      theme: THEME,
    });
    const fit = new FitAddon();
    term.loadAddon(fit);

    // Focus indicator: ghostty mounts a hidden <textarea> for keyboard input as
    // a child of this.el, so focusin/focusout bubble up. Toggle a class the CSS
    // uses to highlight the pane. Listeners go on *before* open() so the initial
    // focus() that open() performs is captured.
    this.onFocusIn = () => this.el.classList.add("is-focused");
    this.onFocusOut = () => this.el.classList.remove("is-focused");
    this.el.addEventListener("focusin", this.onFocusIn);
    this.el.addEventListener("focusout", this.onFocusOut);

    term.open(this.el);
    fit.fit();
    fit.observeResize?.();

    term.onData((data) => this.pushEvent("pty_input", { data }));
    term.onResize?.(() => this.sendResize());

    // Work around ghostty-web dropping Shift on special keys (see
    // SHIFT_KEY_SEQUENCES). Returning true suppresses ghostty's default handling
    // (and the browser's focus shift for Shift+Tab).
    term.attachCustomKeyEventHandler?.((e) => {
      if (
        e.type === "keydown" &&
        e.shiftKey &&
        !e.ctrlKey &&
        !e.altKey &&
        !e.metaKey &&
        Object.prototype.hasOwnProperty.call(SHIFT_KEY_SEQUENCES, e.key)
      ) {
        this.pushEvent("pty_input", { data: SHIFT_KEY_SEQUENCES[e.key] });
        return true;
      }
      return false;
    });

    this.term = term;
    this.fit = fit;
    this.ready = true;

    // Flush anything that arrived during the async import, then size the PTY.
    for (const chunk of this.pending) term.write(chunk);
    this.pending = [];
    this.sendResize();

    this.onWindowResize = () => {
      try {
        fit.fit();
        this.sendResize();
      } catch (_) {}
    };
    window.addEventListener("resize", this.onWindowResize);
  },

  sendResize() {
    if (!this.term) return;
    this.pushEvent("resize", { rows: this.term.rows, cols: this.term.cols });
  },

  destroyed() {
    if (this.onWindowResize)
      window.removeEventListener("resize", this.onWindowResize);
    if (this.onFocusIn) this.el.removeEventListener("focusin", this.onFocusIn);
    if (this.onFocusOut)
      this.el.removeEventListener("focusout", this.onFocusOut);
    try {
      this.term?.dispose?.();
    } catch (_) {}
  },
};
