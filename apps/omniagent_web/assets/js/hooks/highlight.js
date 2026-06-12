// DiffHighlight: renders a unified diff with per-line +/- gutter signs and
// syntax-highlighted code. The server hands us the raw diff text via data
// attributes and leaves the element empty (phx-update="ignore"); this hook owns
// the DOM. The container's id is keyed by file path, so switching files mounts
// a fresh element and re-runs render().
//
// highlight.js escapes its input, so feeding `.value` into innerHTML is safe
// even though the file contents are untrusted agent output.
import hljs from "highlight.js/lib/core";
import bash from "highlight.js/lib/languages/bash";
import css from "highlight.js/lib/languages/css";
import elixir from "highlight.js/lib/languages/elixir";
import go from "highlight.js/lib/languages/go";
import ini from "highlight.js/lib/languages/ini";
import javascript from "highlight.js/lib/languages/javascript";
import json from "highlight.js/lib/languages/json";
import markdown from "highlight.js/lib/languages/markdown";
import python from "highlight.js/lib/languages/python";
import ruby from "highlight.js/lib/languages/ruby";
import rust from "highlight.js/lib/languages/rust";
import sql from "highlight.js/lib/languages/sql";
import typescript from "highlight.js/lib/languages/typescript";
import xml from "highlight.js/lib/languages/xml";
import yaml from "highlight.js/lib/languages/yaml";

for (const [name, lang] of [
  ["bash", bash],
  ["css", css],
  ["elixir", elixir],
  ["go", go],
  ["ini", ini],
  ["javascript", javascript],
  ["json", json],
  ["markdown", markdown],
  ["python", python],
  ["ruby", ruby],
  ["rust", rust],
  ["sql", sql],
  ["typescript", typescript],
  ["xml", xml],
  ["yaml", yaml],
]) {
  hljs.registerLanguage(name, lang);
}

const EXT_LANG = {
  bash: "bash",
  sh: "bash",
  zsh: "bash",
  css: "css",
  scss: "css",
  ex: "elixir",
  exs: "elixir",
  eex: "xml",
  heex: "xml",
  go: "go",
  ini: "ini",
  toml: "ini",
  cfg: "ini",
  cjs: "javascript",
  js: "javascript",
  jsx: "javascript",
  mjs: "javascript",
  json: "json",
  md: "markdown",
  markdown: "markdown",
  py: "python",
  rb: "ruby",
  rs: "rust",
  sql: "sql",
  ts: "typescript",
  tsx: "typescript",
  html: "xml",
  svg: "xml",
  xml: "xml",
  vue: "xml",
  yaml: "yaml",
  yml: "yaml",
};

function langForPath(path) {
  const name = (path || "").split("/").pop() || "";
  const dot = name.lastIndexOf(".");
  if (dot < 0) return null;
  return EXT_LANG[name.slice(dot + 1).toLowerCase()] || null;
}

// `inHunk` disambiguates body lines from file headers: once a `@@` hunk has
// started, a line beginning with `---`/`+++` is removed/added content (e.g. a
// deleted YAML `---` or SQL `--` comment), not a `--- a/path` / `+++ b/path`
// header. Headers only appear before the first hunk of each file.
function kindOf(line, inHunk) {
  if (line.startsWith("@@")) return "hunk";
  if (!inHunk) {
    if (line.startsWith("+++") || line.startsWith("---")) return "meta";
    if (
      /^(diff |index |old mode|new mode|new file|deleted file|rename |similarity |Binary )/.test(
        line,
      )
    ) {
      return "meta";
    }
  }
  if (line.startsWith("+")) return "add";
  if (line.startsWith("-")) return "del";
  return "ctx";
}

function setCode(el, text, lang) {
  if (text === "") {
    el.innerHTML = "&nbsp;";
    return;
  }
  if (lang) {
    try {
      el.innerHTML = hljs.highlight(text, {
        language: lang,
        ignoreIllegal: true,
      }).value;
      return;
    } catch (_e) {
      // fall through to plain text
    }
  }
  el.textContent = text;
}

export const DiffHighlight = {
  mounted() {
    this.render();
  },

  render() {
    const diff = this.el.dataset.diff || "";
    const lang = langForPath(this.el.dataset.path);
    const frag = document.createDocumentFragment();

    let inHunk = false;
    for (const raw of diff.split("\n")) {
      const kind = kindOf(raw, inHunk);
      if (kind === "hunk") inHunk = true;
      else if (kind === "meta") inHunk = false;
      const line = document.createElement("div");
      line.className = `diff-line diff-${kind}`;

      const sign = document.createElement("span");
      sign.className = "diff-sign";

      const code = document.createElement("span");
      code.className = "diff-code";

      if (kind === "add" || kind === "del") {
        sign.textContent = raw[0];
        setCode(code, raw.slice(1), lang);
      } else if (kind === "ctx") {
        sign.textContent = " ";
        setCode(code, raw.startsWith(" ") ? raw.slice(1) : raw, lang);
      } else {
        // hunk / meta headers: shown verbatim, not language-highlighted.
        sign.textContent = " ";
        code.textContent = raw === "" ? " " : raw;
      }

      line.appendChild(sign);
      line.appendChild(code);
      frag.appendChild(line);
    }

    this.el.replaceChildren(frag);
  },
};
