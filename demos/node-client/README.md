# Node demo client

A live web viewer for grpc-ebcdic. Stubs are loaded dynamically from
[`../../proto`](../../proto) at run time, so nothing generated is checked in.

```bash
npm install
npm start   # then open http://127.0.0.1:8089
```

The viewer honours `EBCDIC_ADDR` (default `127.0.0.1:50063`) and `PORT`
(default 8089).

### Serving under a base path

Set `UI_BASE` and the whole viewer moves under that prefix, for example behind
a reverse proxy that forwards `/ui/ebcdic/*` unchanged:

```bash
UI_BASE=/ui/ebcdic npm start   # page at http://127.0.0.1:8089/ui/ebcdic/
```

The bridge strips the prefix before routing, so every endpoint lives at
`$UI_BASE/api/*`, and it injects a `<meta name="ui-base">` tag into the served
page, which the page reads to prefix its own `fetch()` calls. Unset, nothing
changes: the bridge answers at the root exactly as before.

## The web viewer

The viewer exists to make one property visible: **rows arrive before the
upload finishes**.

It is a single HTTP request. The browser POSTs the file and reads Server-Sent
Events off the *same* response, which is deliberately the same shape as the
gRPC call underneath it: bytes going one way while decoded rows come back the
other. Nothing buffers the file. Each upload slice is written into the gRPC
call as it lands, and each `RecordRow` is flushed to the page as the Rust
server decodes it.

The page shows an upload bar with a green marker where the first row landed,
and says so in words:

> First row after **48 B of 370 B** (13% uploaded). The rest of the file had
> not been sent yet.

Two controls make that observable rather than theoretical:

- **Upload throttle** sleeps between upload slices. It slows the *upload*
  only. The decoder is never waiting on anything but bytes.
- **Chunk size** is derived from the file, aiming for about 40 upload steps
  whatever the size, and shown next to the throttle. A 300 byte fixture and a
  50 MiB extract then look the same.

Neither changes the rows. Records are fixed-length, so "this record is
complete" is a property of the bytes, not of the slicing: a row is emitted the
moment its record's last byte arrives, however the upload was chunked.

The layout travels with the request, not the file: the page's layout box holds
either COBOL copybook source or Docling `EbcdicLayout` JSON (it sniffs the
leading `{`), and the browser sends it base64url-encoded in the
`x-ebcdic-layout` header, leaving the POST body for the file bytes alone. The
box is editable, so a fixture is a starting point, not a cage.

### The fixtures

Each `.ebc` in [`../sample-data`](../sample-data) is real cp037 bytes with a
companion layout (`name.cpy` or `name.layout.json`) that the page loads into
the editor when the sample is picked. `generate.py` in that directory rebuilds
them, field by field.

| File | What you see |
|---|---|
| `customer-master.ebc` | the README's worked example: zoned id, packed balance with a sign nibble, COMP order count, and a FILLER that never becomes a column |
| `sales-week.ebc` | a COBOL `OCCURS 7` arriving as seven packed-decimal columns |
| `statement.ebc` | two schemas routed by a one-byte type prefix into two tables, from a Docling JSON layout |

Negative amounts render red. That nibble is the sign: the difference between a
credit and a debit is one hex digit, and seeing `-12,345.67` next to the bytes
that produced it is the point.

### A real extract

The small fixtures each pin one idea; none of them show what the service is
actually for. Drop any fixed-width extract worth megabytes into
[`../sample-data/large/`](../sample-data/large) (gitignored) with a companion
layout and it appears in the dropdown, or use the file picker for something on
your disk and paste its copybook into the layout box.

## Things that bite

**Write the options frame before you read the response.** The server sends
`layout_info` before it reads a single data byte, but a client that awaits the
call before sending options deadlocks. `lib/ebcdic.js` sends options inside
`openParse()` for exactly this reason, so the ordering cannot be got wrong by
a caller.

**Handle the oneof by name, not by guessing.** With `oneofs: true`,
proto-loader sets `message.event` to the name of the active arm. The bridge
forwards `message[message.event]` rather than sniffing which key is populated,
so an arm added to the contract later is passed through to the page instead of
being dropped silently.

**Backpressure is real and worth keeping.** `res.write()` returning false
means the browser is behind. The bridge pauses the gRPC call and resumes on
`drain`, which propagates through gRPC flow control back to the decoder.
Without it, a large extract queues the whole row stream in this process.

**Fillers are layout, not data.** `layout_info` lists `FIELD_TYPE_SKIP` fields
in its resolved schema, but no row ever carries a cell for one. The page
builds its table headers from the schema minus the fillers, and aligns cells
by field name rather than by position, so the two can never drift apart.
