// LiveView hook: persists cosmetic UI preferences (panel collapse state, the
// active right-pane tab) in the browser. On mount it hydrates the server from
// localStorage via `prefs_restore`; thereafter the server pushes `prefs_save`
// whenever a preference changes and we write it back. Per-browser, no DB.
const KEY = "oa:prefs";

export const Prefs = {
  mounted() {
    let saved = {};
    try {
      saved = JSON.parse(localStorage.getItem(KEY) || "{}");
    } catch (_) {
      saved = {};
    }
    this.pushEvent("prefs_restore", saved);

    this.handleEvent("prefs_save", (prefs) => {
      try {
        localStorage.setItem(KEY, JSON.stringify(prefs));
      } catch (_) {}
    });
  },
};
