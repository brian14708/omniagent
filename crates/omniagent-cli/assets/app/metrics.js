// Header instrument gauges — recomputed from the running totals on each span.

import { fmtTok } from "./format.js";
import { spans, totals } from "./state.js";

export function updateMetrics() {
  document.getElementById("m-spans").textContent = spans.length;
  document.getElementById("m-tin").textContent = fmtTok(totals.in);
  document.getElementById("m-tout").textContent = fmtTok(totals.out);
  document.getElementById("m-cread").textContent = fmtTok(totals.cacheR);
  document.getElementById("m-ccreate").textContent = fmtTok(totals.cacheW);
  document.getElementById("m-lat").innerHTML = spans.length
    ? Math.round(totals.lat / spans.length) + "<small>ms</small>"
    : "—<small>ms</small>";
  document.getElementById("m-err").textContent = totals.err;
  document.getElementById("m-err-box").classList.toggle("hot", totals.err > 0);
  document.getElementById("count").textContent = spans.length;
}
