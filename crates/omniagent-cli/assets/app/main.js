// Entry point: wire the mode/view controls and the startup screen, then run the
// footer mission clock. The runtime modules (terminal, traces, reviews,
// compare, files, diff) are booted by startup.js after the agent is launched.

import { initModes } from "./modes.js";
import { initStartup } from "./startup.js";

initModes();
initStartup();

// Footer telemetry: endpoint + mission clock.
document.getElementById("host").textContent = location.host;
const clockEl = document.getElementById("clock");
function tick() {
  const t = new Date().toISOString().slice(11, 19);
  clockEl.innerHTML = `${t} <b>utc</b>`;
}
tick();
setInterval(tick, 1000);
