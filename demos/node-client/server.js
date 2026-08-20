#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0
//
// Web demo: a dependency-light HTTP bridge in front of grpc-ebcdic.
//
// The interesting part is that there is only one request. The browser POSTs an
// EBCDIC file and reads Server-Sent Events off the *same* response, so the
// HTTP call has the same shape as the gRPC call underneath it: bytes flowing
// one way while decoded rows flow the other. Nothing here buffers the file;
// each upload chunk is written into the gRPC call as it lands, and each row is
// flushed to the browser as the Rust server decodes it.
//
// That is what the page is for. Rows land in the table while the upload bar is
// still filling, and the "first row" readout says how far in they started.
//
//   node server.js            # http://127.0.0.1:8089
//
// Environment: EBCDIC_ADDR (default 127.0.0.1:50063), PORT (default 8089),
// UI_BASE (default empty; serve everything under this path prefix instead).

import { createServer } from "node:http";
import { readFile, readdir, stat } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { EbcdicClient } from "./lib/ebcdic.js";

const PORT = Number(process.env.PORT ?? 8089);
const ADDR = process.env.EBCDIC_ADDR ?? "127.0.0.1:50063";
const client = new EbcdicClient(ADDR);
const publicDir = path.join(path.dirname(fileURLToPath(import.meta.url)), "public");

/**
 * Path prefix the whole bridge is served under, e.g. `/ui/ebcdic` behind a
 * reverse proxy that forwards without stripping. Empty means the bridge
 * answers at the root, byte-for-byte as it always has.
 */
const UI_BASE = normalizeBase(process.env.UI_BASE ?? "");

function normalizeBase(base) {
  if (!base) return "";
  const withSlash = base.startsWith("/") ? base : `/${base}`;
  return withSlash.replace(/\/+$/, "");
}

/** Largest artificial upload delay accepted, in ms per chunk. */
const MAX_DELAY_MS = 2000;

/**
 * Bytes per chunk fed into the gRPC call.
 *
 * The bridge re-slices the incoming body rather than forwarding whatever
 * Node's HTTP layer happened to hand it, because for a small file that is one
 * buffer, and a single chunk makes the whole demo invisible: the upload bar
 * would jump to full before the first row. Chunk size does not change the
 * rows, which are a property of the record length alone: a record is emitted
 * the moment its last byte arrives, however the bytes were sliced. It only
 * changes how much of this you get to watch.
 */
const DEFAULT_CHUNK_BYTES = 256;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Where the fixtures live. Resolved once, and used to bound path lookups. */
const sampleDir = path.resolve(publicDir, "..", "..", "sample-data");

/**
 * The fixtures, with their sizes, plus anything dropped in `sample-data/large`.
 *
 * Each `.ebc` file travels with a companion layout: `name.cpy` (COBOL copybook
 * source) or `name.layout.json` (Docling's EbcdicLayout JSON), which the page
 * loads into its layout editor when the sample is picked.
 *
 * `large/` is gitignored and meant for real extracts worth megabytes, which do
 * not belong in the repository but are the only way to see this service do
 * something a small fixture cannot show.
 */
async function listSamples() {
  const found = [];
  for (const dir of ["", "large"]) {
    const full = path.join(sampleDir, dir);
    let entries;
    try {
      entries = await readdir(full);
    } catch {
      continue; // `large/` is optional.
    }
    for (const entry of entries.filter((e) => e.endsWith(".ebc")).sort()) {
      const name = dir ? `${dir}/${entry}` : entry;
      const { size } = await stat(path.join(full, entry));
      const stem = entry.slice(0, -".ebc".length);
      const layout = await companion(full, `${stem}.cpy`, "copybook")
        ?? await companion(full, `${stem}.layout.json`, "json");
      found.push({ name, size, layout });
    }
  }
  return found;
}

/** The kind of layout companion that exists next to a fixture, if any. */
async function companion(dir, file, kind) {
  try {
    await stat(path.join(dir, file));
    return kind;
  } catch {
    return null;
  }
}

function sendJson(res, status, body) {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
}

/** Format one SSE event frame. */
function frame(event, data) {
  return `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
}

/**
 * Read the layout source from the `x-ebcdic-layout` header and build
 * ParseOptions from it plus the query string.
 *
 * The layout is text the user may have edited (COBOL source or JSON), so it
 * travels base64url-encoded in a header rather than in the URL. The body
 * belongs to the file bytes, and a query string belongs in a proxy log, so
 * neither holds the layout. Everything else is small and goes in the query.
 */
async function optionsFrom(req, url) {
  const raw = req.headers["x-ebcdic-layout"];
  if (!raw) return { error: "missing x-ebcdic-layout header" };

  let spec;
  try {
    spec = JSON.parse(Buffer.from(raw, "base64url").toString("utf8"));
  } catch {
    return { error: "x-ebcdic-layout is not base64url JSON" };
  }

  const options = {
    encoding: url.searchParams.get("encoding") ?? "",
    maxRecords: Math.max(0, Number(url.searchParams.get("maxRecords") ?? 0) || 0),
    emitDocument: url.searchParams.has("emitDocument"),
  };

  if (typeof spec.copybook === "string") {
    options.copybook = spec.copybook;
  } else if (spec.layoutJson !== undefined) {
    options.layoutJson = Buffer.from(JSON.stringify(spec.layoutJson), "utf8");
  } else if (spec.layout !== undefined) {
    options.layout = spec.layout;
  } else {
    return { error: "layout spec needs copybook, layoutJson, or layout" };
  }
  return { options };
}

/**
 * Pipe one upload through ParseEbcdic, streaming events back on the same
 * response.
 *
 * `delayMs` sleeps between upload chunks. It is there to make the streaming
 * visible on a small local file, where the whole thing would otherwise be
 * uploaded and decoded inside a single frame of animation. It slows the upload
 * only; the decoder is never waiting on anything but bytes.
 */
async function bridge(req, res, url) {
  const { options, error } = await optionsFrom(req, url);
  if (error) return sendJson(res, 400, { error });

  const delayMs = Math.min(Number(url.searchParams.get("delayMs") ?? 0) || 0, MAX_DELAY_MS);

  res.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    connection: "keep-alive",
    // The page is watching for the first row; a proxy that buffers would hide
    // the only thing this demo exists to show.
    "x-accel-buffering": "no",
  });

  const call = client.openParse(options);
  let closed = false;

  // proto-loader's `oneofs: true` sets `message.event` to the name of the
  // active arm, so an arm this bridge has never heard of still forwards
  // rather than being dropped on the floor.
  call.on("data", (message) => {
    const kind = message.event;
    if (!kind) return;
    if (!res.write(frame(kind, message[kind] ?? {}))) {
      // The browser is behind. Stop pulling events until it drains, rather
      // than queueing the whole file's worth of rows in this process. The
      // pause propagates back through gRPC flow control to the decoder itself.
      call.pause();
      res.once("drain", () => call.resume());
    }
  });
  call.on("error", (err) => {
    if (!closed) {
      closed = true;
      res.write(frame("grpc-error", { message: err.details ?? err.message }));
      res.end();
    }
  });
  call.on("end", () => {
    if (!closed) {
      closed = true;
      res.write(frame("done", {}));
      res.end();
    }
  });

  // If the browser goes away, stop decoding rather than finish into a void.
  res.on("close", () => {
    if (!closed) {
      closed = true;
      call.cancel();
    }
  });

  const chunkBytes = Math.max(
    1,
    Number(url.searchParams.get("chunkBytes") ?? 0) || DEFAULT_CHUNK_BYTES,
  );

  let fed = 0;
  try {
    for await (const buffer of req) {
      for (let at = 0; at < buffer.length; at += chunkBytes) {
        if (closed) break;
        const chunk = buffer.subarray(at, at + chunkBytes);
        call.write({ chunk });
        fed += chunk.length;
        // The page draws its upload bar from this, so it measures what the
        // decoder has actually been handed, not what the browser has queued.
        res.write(frame("fed", { bytes: fed }));
        if (delayMs > 0) await sleep(delayMs);
      }
      if (closed) break;
    }
    if (!closed) call.end();
  } catch {
    if (!closed) {
      closed = true;
      call.cancel();
      res.end();
    }
  }
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  // The router sees the path with the base stripped, so every route below is
  // written against the root and works identically with or without UI_BASE.
  let pathname = url.pathname;
  if (UI_BASE && (pathname === UI_BASE || pathname.startsWith(`${UI_BASE}/`))) {
    pathname = pathname.slice(UI_BASE.length) || "/";
  }
  try {
    if (req.method === "POST" && pathname === "/api/parse") {
      return await bridge(req, res, url);
    }

    if (req.method === "GET" && pathname === "/api/info") {
      return sendJson(res, 200, await client.getServiceInfo());
    }

    if (req.method === "GET" && pathname === "/api/samples") {
      return sendJson(res, 200, { files: await listSamples() });
    }

    // A name may contain one `large/` segment, so the path is resolved and
    // then checked to be inside the sample directory rather than pattern
    // matched. Prefix checks on unresolved strings are how directory
    // traversal gets through.
    const sample = pathname.match(/^\/api\/samples\/(.+)$/);
    if (req.method === "GET" && sample) {
      const file = path.resolve(sampleDir, decodeURIComponent(sample[1]));
      if (!file.startsWith(sampleDir + path.sep)) {
        return sendJson(res, 403, { error: "outside the sample directory" });
      }
      const { size } = await stat(file);
      res.writeHead(200, {
        "content-type": file.endsWith(".ebc")
          ? "application/octet-stream"
          : "text/plain; charset=utf-8",
        "content-length": size,
      });
      return res.end(await readFile(file));
    }

    // Static front end. no-store: this is a live demo page, never let the
    // browser run a stale copy of it. When a base is configured the page is
    // told about it through a meta tag, and prefixes its own calls with it.
    if (req.method === "GET" && (pathname === "/" || pathname === "/index.html")) {
      let html = await readFile(path.join(publicDir, "index.html"), "utf8");
      if (UI_BASE) {
        html = html.replace("</head>", `<meta name="ui-base" content="${UI_BASE}">\n</head>`);
      }
      res.writeHead(200, {
        "content-type": "text/html; charset=utf-8",
        "cache-control": "no-store",
      });
      return res.end(html);
    }
    if (req.method === "GET" && pathname === "/favicon.ico") {
      res.writeHead(204);
      return res.end();
    }

    sendJson(res, 404, { error: "not found" });
  } catch (err) {
    sendJson(res, 502, { error: err.details ?? err.message });
  }
});

server.on("error", (err) => {
  if (err.code === "EADDRINUSE") {
    console.error(`port ${PORT} is already in use, another bridge instance?`);
    console.error(`stop it, or run with a different port: PORT=8090 npm start`);
    process.exit(1);
  }
  throw err;
});

server.listen(PORT, () => {
  console.log(`ebcdic web demo on http://127.0.0.1:${PORT}`);
  console.log(`forwarding to grpc-ebcdic at ${ADDR}`);
});
