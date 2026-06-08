// LiveView hook: drag-resizable panel divider ("gutter"). Each gutter mutates a
// CSS custom property on #console (the layout root) during the drag for instant
// feedback, then reports the final value to the server on pointerup, which
// clamps + persists it via the Prefs hook.
//
// data-* attributes:
//   axis   "x" (width) | "y" (height %)
//   var    CSS custom property to drive, e.g. "--left-w"
//   prop   server-side key sent in resize_panel ("left_w" | "right_w" | "term_pct")
//   unit   "px" (default) | "%"
//   min/max clamp bounds (in the same unit)
//   invert "true" when dragging left should grow the panel (right-side gutter)
export const Resize = {
  mounted() {
    const root = document.getElementById("console");
    const { axis, var: cssVar, prop, unit = "px", invert } = this.el.dataset;
    const min = parseFloat(this.el.dataset.min);
    const max = parseFloat(this.el.dataset.max);
    const clamp = (v) => Math.max(min, Math.min(max, v));

    this.onDown = (e) => {
      e.preventDefault();
      const startPos = axis === "x" ? e.clientX : e.clientY;
      const startVal =
        parseFloat(getComputedStyle(root).getPropertyValue(cssVar)) || 0;
      // For "%" we need the container size to convert a pixel delta to percent.
      const span =
        axis === "x"
          ? root.clientWidth
          : this.el.parentElement?.clientHeight || 1;

      const onMove = (ev) => {
        const pos = axis === "x" ? ev.clientX : ev.clientY;
        let delta = pos - startPos;
        if (invert === "true") delta = -delta;
        const next = clamp(
          unit === "%" ? startVal + (delta / span) * 100 : startVal + delta,
        );
        root.style.setProperty(cssVar, unit === "%" ? `${next}%` : `${next}px`);
        this.current = next;
      };

      const onUp = () => {
        window.removeEventListener("pointermove", onMove);
        window.removeEventListener("pointerup", onUp);
        document.body.style.userSelect = "";
        this.el.classList.remove("dragging");
        if (this.current != null)
          this.pushEvent("resize_panel", {
            prop,
            value: Math.round(this.current),
          });
      };

      this.current = null;
      this.el.classList.add("dragging");
      document.body.style.userSelect = "none";
      window.addEventListener("pointermove", onMove);
      window.addEventListener("pointerup", onUp);
    };

    this.el.addEventListener("pointerdown", this.onDown);
  },

  destroyed() {
    if (this.onDown) this.el.removeEventListener("pointerdown", this.onDown);
  },
};
