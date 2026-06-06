// Shared mutable UI state. Collections (Set/Map/array/object) are exported by
// reference so every module mutates the same instance; primitives that get
// reassigned live inside `totals` / `ui` objects so updates stay visible
// across modules.

export const openIds = new Set();
export const seenIds = new Set();
export const spans = []; // newest-first
export const nodeById = new Map(); // span.id -> built DOM node (rendered once)
export const reviews = new Map();
export const comparisons = new Map();

export const totals = {
  in: 0,
  out: 0,
  cacheR: 0,
  cacheW: 0,
  lat: 0,
  err: 0,
};

export const ui = {
  search: "",
  reviewEnabled: false,
  compareDefaults: [],
  mode: "agent", // agent | inspection | side-by-side
  view: "main", // main | file | diff (right-pane view, available in all modes)
  tab: "traces", // traces | review | compare (sub-tabs of the main view)
  selectedId: null, // keyboard-selected span id
};
