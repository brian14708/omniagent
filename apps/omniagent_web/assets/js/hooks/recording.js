// LiveView hook: asciinema-player for a session's terminal recording.
//
// The element carries `data-src` — the artifact download URL serving the
// asciicast v2 file. The player fetches and renders it client-side; the inlined
// WASM VT (bundled from npm) runs in the main thread, so no worker or separate
// .wasm asset needs to be served. The player CSS is imported in `app.js`.
import { create } from "asciinema-player";

export const Recording = {
  mounted() {
    const src = this.el.dataset.src;
    if (!src) return;

    this.player = create(src, this.el, {
      autoPlay: true,
      fit: "width",
      terminalFontFamily: '"JetBrains Mono", ui-monospace, Menlo, monospace',
      theme: "asciinema",
    });
  },

  destroyed() {
    try {
      this.player?.dispose?.();
    } catch (_) {}
  },
};
