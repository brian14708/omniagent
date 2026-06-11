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
