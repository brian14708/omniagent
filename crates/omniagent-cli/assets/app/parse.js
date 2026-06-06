// Provider-shape normalization: turn raw request/response bodies from
// Anthropic / OpenAI / Gemini into uniform message + content structures.

import { safeJson } from "./format.js";

function parseMaybeJson(value) {
  if (typeof value !== "string") return value ?? {};
  try {
    return JSON.parse(value);
  } catch (_) {
    return value;
  }
}

export function responseToolName(item) {
  if (item?.type === "tool_search_call") return "tool_search";
  if (item?.name) return item.name;
  if (typeof item?.type === "string" && item.type.endsWith("_call"))
    return item.type.slice(0, -5);
  return "";
}

export function responseToolInput(item) {
  if (!item || typeof item !== "object") return {};
  if (Object.prototype.hasOwnProperty.call(item, "arguments"))
    return parseMaybeJson(item.arguments);
  const out = {};
  for (const [k, v] of Object.entries(item)) {
    if (["id", "type", "status", "call_id", "name", "execution"].includes(k))
      continue;
    out[k] = v;
  }
  return out;
}

export function textFromContent(content) {
  if (content == null) return "";
  if (typeof content === "string") return content;
  if (Array.isArray(content))
    return content
      .map((part) => {
        if (typeof part === "string") return part;
        if (part?.type === "tool_use")
          return `[tool_use ${part.name ?? ""}]\n${JSON.stringify(part.input ?? {}, null, 2)}`;
        if (part?.type === "tool_result")
          return `[tool_result ${part.tool_use_id ?? ""}]\n${textFromContent(part.content)}`;
        return (
          part?.text ??
          part?.input_text ??
          part?.output_text ??
          part?.refusal ??
          (part?.type ? `[${part.type}]` : "") ??
          ""
        );
      })
      .filter(Boolean)
      .join("");
  if (typeof content === "object")
    return (
      content.text ??
      content.input_text ??
      content.output_text ??
      content.refusal ??
      JSON.stringify(content)
    );
  return String(content);
}

function responseBlocksFromOutput(output) {
  const blocks = [];
  if (!Array.isArray(output)) return blocks;
  for (const item of output) {
    if (!item || typeof item !== "object") continue;
    if (item.type === "message") {
      for (const c of item.content ?? []) {
        if (c?.type === "output_text" || c?.type === "input_text")
          blocks.push({ type: "text", text: c.text ?? "" });
        else if (c?.type === "refusal")
          blocks.push({ type: "text", text: c.refusal ?? "" });
        else if (c?.text) blocks.push({ type: "text", text: c.text });
      }
    } else if (item.type === "reasoning") {
      const thinking = (item.summary ?? [])
        .map((s) => s?.text ?? s?.summary_text ?? "")
        .join("");
      if (thinking) blocks.push({ type: "thinking", thinking });
    } else if (typeof item.type === "string" && item.type.endsWith("_call")) {
      blocks.push({
        type: "tool_use",
        id: item.call_id ?? item.id ?? "",
        name: responseToolName(item),
        input: responseToolInput(item),
      });
    } else if (
      item.type === "tool_search_output" ||
      (typeof item.type === "string" && item.type.endsWith("_call_output"))
    ) {
      blocks.push({
        type: "tool_result",
        tool_use_id: item.call_id ?? "",
        content: item.output ?? item,
      });
    }
  }
  return blocks;
}

// Pull the reconstructed assistant content out of a span's response,
// normalizing the providers into one array of {type,...} blocks. The proxy
// mirrors OpenAI/Gemini output into an Anthropic-shape `content` array, so
// reading body.content covers every provider; fallbacks cover non-streamed
// OpenAI Responses/Chat and Gemini shapes.
export function responseContent(span) {
  const body = span.response;
  if (!body || typeof body !== "object") return [];
  if (Array.isArray(body.content) && body.content.length) return body.content;
  const outputBlocks = responseBlocksFromOutput(body.output);
  if (outputBlocks.length) return outputBlocks;
  // Non-streamed OpenAI chat completion.
  const msg = body.choices?.[0]?.message;
  if (msg) {
    const out = [];
    if (msg.reasoning_content)
      out.push({ type: "thinking", thinking: msg.reasoning_content });
    if (msg.content) out.push({ type: "text", text: msg.content });
    for (const tc of msg.tool_calls ?? [])
      out.push({
        type: "tool_use",
        name: tc.function?.name,
        input: parseMaybeJson(tc.function?.arguments),
      });
    return out;
  }
  // Non-streamed Gemini.
  const parts = body.candidates?.[0]?.content?.parts;
  if (Array.isArray(parts))
    return parts
      .filter((p) => p?.text != null)
      .map((p) => ({ type: "text", text: p.text }));
  return [];
}

export function requestMessages(span) {
  const body = span.request;
  if (!body || typeof body !== "object") return [];
  const out = [];
  if (body.system)
    out.push({ role: "system", text: textFromContent(body.system) });
  if (body.instructions)
    out.push({
      role: "instructions",
      text: textFromContent(body.instructions),
    });
  if (Array.isArray(body.messages)) {
    for (const m of body.messages)
      out.push({
        role: m?.role ?? "message",
        text: textFromContent(m?.content),
        raw: m,
      });
  }
  if (Array.isArray(body.input)) {
    for (const item of body.input) {
      if (!item || typeof item !== "object") continue;
      if (item.type && String(item.type).endsWith("_call")) {
        out.push({
          role: "assistant",
          tool: responseToolName(item),
          text: JSON.stringify(responseToolInput(item), null, 2),
          raw: item,
        });
      } else if (
        item.type === "function_call_output" ||
        String(item.type ?? "").endsWith("_call_output")
      ) {
        out.push({
          role: "tool",
          tool: item.call_id ?? "",
          text: textFromContent(item.output ?? item.content ?? item),
          raw: item,
        });
      } else {
        out.push({
          role: item.role ?? item.type ?? "input",
          text: textFromContent(item.content ?? item.text ?? item),
          raw: item,
        });
      }
    }
  } else if (typeof body.input === "string") {
    out.push({ role: "user", text: body.input });
  }
  if (Array.isArray(body.contents)) {
    for (const c of body.contents)
      out.push({
        role: c?.role ?? "user",
        text: textFromContent(c?.parts ?? c?.content ?? c),
        raw: c,
      });
  }
  return out.filter((m) => m.text || m.tool);
}

export function requestTools(span) {
  const body = span.request;
  if (!body || typeof body !== "object") return [];
  const tools = [];
  const add = (name, raw) => {
    if (name) tools.push({ name, raw });
  };
  for (const tool of body.tools ?? []) {
    add(tool?.name ?? tool?.function?.name ?? tool?.type, tool);
  }
  for (const tool of body.tool_choice?.tools ?? []) {
    add(tool?.name ?? tool?.function?.name ?? tool?.type, tool);
  }
  return tools;
}

export function previewText(blocks, limit = 900) {
  const text = blocks
    .map((b) => {
      if (b?.type === "text") return b.text ?? "";
      if (b?.type === "thinking") return `[thinking]\n${b.thinking ?? ""}`;
      if (b?.type === "tool_use")
        return `[tool ${b.name ?? ""}]\n${JSON.stringify(b.input ?? {}, null, 2)}`;
      if (b?.type === "tool_result")
        return `[tool result]\n${textFromContent(b.content)}`;
      return "";
    })
    .filter(Boolean)
    .join("\n\n");
  return text.length > limit ? text.slice(0, limit) + "\n…" : text;
}

export function spanSearchHaystack(span) {
  // Spans are immutable once ingested, so the (relatively expensive) JSON
  // stringify + lowercase is computed once and cached for later keystrokes.
  if (span._haystack != null) return span._haystack;
  span._haystack = [
    span.provider,
    span.model,
    span.method,
    span.path,
    span.status,
    span.error,
    safeJson(span.request),
    safeJson(span.response),
  ]
    .filter(Boolean)
    .join("\n")
    .toLowerCase();
  return span._haystack;
}

function usageNum(usage, ...keys) {
  for (const key of keys) {
    const value = usage?.[key];
    if (typeof value === "number") return value;
  }
  const details =
    usage?.input_tokens_details ?? usage?.prompt_tokens_details ?? {};
  if (
    keys.includes("cache_read_tokens") &&
    typeof details.cached_tokens === "number"
  )
    return details.cached_tokens;
  const outDetails =
    usage?.output_tokens_details ?? usage?.completion_tokens_details ?? {};
  if (
    keys.includes("reasoning_tokens") &&
    typeof outDetails.reasoning_tokens === "number"
  )
    return outDetails.reasoning_tokens;
  return undefined;
}

// Normalizes a span's usage object into the canonical token counts,
// tolerating every provider field spelling (see `Usage::from_value`).
export function readUsage(span) {
  const usage = span.usage ?? {};
  return {
    inTok: usageNum(
      usage,
      "input_tokens",
      "prompt_tokens",
      "promptTokenCount",
      "inputTokens",
    ),
    outTok: usageNum(
      usage,
      "output_tokens",
      "completion_tokens",
      "candidatesTokenCount",
      "outputTokens",
    ),
    totalTok: usageNum(usage, "total_tokens", "totalTokenCount", "totalTokens"),
    reasonTok: usageNum(usage, "reasoning_tokens", "reasoningTokens"),
    cacheR: usageNum(
      usage,
      "cache_read_tokens",
      "cache_read_input_tokens",
      "cached_tokens",
      "cachedContentTokenCount",
      "cacheReadInputTokens",
    ),
    cacheW: usageNum(
      usage,
      "cache_creation_tokens",
      "cache_creation_input_tokens",
      "cacheWriteInputTokens",
    ),
  };
}
