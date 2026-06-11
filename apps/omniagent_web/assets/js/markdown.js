// Minimal, dependency-free, XSS-safe Markdown → DOM renderer.
//
// Agent/LLM output is untrusted, so this builds nodes via the DOM
// (createElement + textContent) and NEVER assigns innerHTML — markup in the
// source can't execute. Links are restricted to http/https/mailto. It covers
// the subset LLM replies actually use: fenced code, ATX headings, blockquotes,
// ordered/unordered lists, horizontal rules, paragraphs, and inline
// code/bold/italic/links. Anything unrecognized renders as literal text.

export function renderMarkdown(text) {
  const frag = document.createDocumentFragment();
  if (typeof text !== "string" || text === "") return frag;
  const lines = text.replace(/\r\n?/g, "\n").split("\n");
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];

    // Fenced code block (``` or ~~~)
    const fence = line.match(/^(```|~~~)/);
    if (fence) {
      const marker = fence[1];
      const buf = [];
      i++;
      while (i < lines.length && !lines[i].startsWith(marker)) {
        buf.push(lines[i]);
        i++;
      }
      i++; // skip closing fence
      const pre = el("pre", "oa-md-code");
      const code = document.createElement("code");
      code.textContent = buf.join("\n");
      pre.append(code);
      frag.append(pre);
      continue;
    }

    // Blank line
    if (line.trim() === "") {
      i++;
      continue;
    }

    // ATX heading
    const h = line.match(/^(#{1,6})\s+(.*)$/);
    if (h) {
      const heading = el("div", `oa-md-h oa-md-h${h[1].length}`);
      appendInline(heading, h[2]);
      frag.append(heading);
      i++;
      continue;
    }

    // Horizontal rule
    if (/^(\*{3,}|-{3,}|_{3,})$/.test(line.trim())) {
      frag.append(el("hr", "oa-md-hr"));
      i++;
      continue;
    }

    // Blockquote
    if (line.startsWith(">")) {
      const buf = [];
      while (i < lines.length && lines[i].startsWith(">")) {
        buf.push(lines[i].replace(/^>\s?/, ""));
        i++;
      }
      const bq = el("blockquote", "oa-md-quote");
      bq.append(renderMarkdown(buf.join("\n")));
      frag.append(bq);
      continue;
    }

    // List (unordered -/*/+ or ordered 1. / 1))
    const ordered = /^\s*\d+[.)]\s+/.test(line);
    const itemRe = ordered ? /^\s*\d+[.)]\s+(.*)$/ : /^\s*[-*+]\s+(.*)$/;
    if (itemRe.test(line)) {
      const list = el(ordered ? "ol" : "ul", "oa-md-list");
      while (i < lines.length) {
        const m = lines[i].match(itemRe);
        if (!m) break;
        const li = document.createElement("li");
        appendInline(li, m[1]);
        list.append(li);
        i++;
      }
      frag.append(list);
      continue;
    }

    // Paragraph: gather consecutive plain lines.
    const buf = [line];
    i++;
    while (i < lines.length && isParagraphLine(lines[i])) {
      buf.push(lines[i]);
      i++;
    }
    const p = el("p", "oa-md-p");
    appendInline(p, buf.join("\n"));
    frag.append(p);
  }

  return frag;
}

function isParagraphLine(line) {
  if (line.trim() === "") return false;
  if (/^(```|~~~|#{1,6}\s|>)/.test(line)) return false;
  if (/^\s*([-*+]|\d+[.)])\s+/.test(line)) return false;
  if (/^(\*{3,}|-{3,}|_{3,})$/.test(line.trim())) return false;
  return true;
}

// Inline spans: `code`, **bold**, __bold__, *italic*, _italic_, [text](url).
// A single newline becomes a <br>; everything else is literal text.
function appendInline(parent, text) {
  text.split("\n").forEach((seg, idx) => {
    if (idx > 0) parent.append(document.createElement("br"));
    appendInlineSegment(parent, seg);
  });
}

function appendInlineSegment(parent, text) {
  const re =
    /(`[^`]+`)|(\*\*[^*]+\*\*)|(__[^_]+__)|(\*[^*]+\*)|(_[^_]+_)|(\[[^\]]+\]\([^)]+\))/g;
  let last = 0;
  let m;
  while ((m = re.exec(text)) !== null) {
    if (m.index > last)
      parent.append(document.createTextNode(text.slice(last, m.index)));
    const tok = m[0];
    if (tok.startsWith("`")) {
      const c = el("code", "oa-md-inlinecode");
      c.textContent = tok.slice(1, -1);
      parent.append(c);
    } else if (tok.startsWith("**") || tok.startsWith("__")) {
      const b = document.createElement("strong");
      b.textContent = tok.slice(2, -2);
      parent.append(b);
    } else if (tok.startsWith("*") || tok.startsWith("_")) {
      const it = document.createElement("em");
      it.textContent = tok.slice(1, -1);
      parent.append(it);
    } else {
      const lm = tok.match(/^\[([^\]]+)\]\(([^)]+)\)$/);
      const a = document.createElement("a");
      a.textContent = lm[1];
      a.href = /^(https?:|mailto:)/i.test(lm[2]) ? lm[2] : "#";
      a.target = "_blank";
      a.rel = "noopener noreferrer";
      parent.append(a);
    }
    last = re.lastIndex;
  }
  if (last < text.length)
    parent.append(document.createTextNode(text.slice(last)));
}

function el(tag, className) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  return node;
}
