# grpc-ebcdic architecture

**Status:** spec (no implementation yet)
**Updated:** 2026-08-13

## Where this sits

EBCDIC is not “just text.” A `.ebc` file is a stream of **fixed-width
records** whose fields only mean something together with the COBOL
copybook (or equivalent layout) that produced them. Docling's
`EbcdicBackend` takes an `EbcdicLayout` and emits one table per record
schema. This service is that backend over gRPC.

```text
.ebc bytes  +  copybook / EbcdicLayout
        │
        ▼
   grpc-ebcdic
        │
        ▼
   gRParse coordinator (COLLECTOR_EBCDIC)
        ▼
   Document (tables)
```

Without a layout the bytes are an opaque code page. We refuse to
guess.

## What this process owns

- Decoding character fields with a named EBCDIC codec (`cp037`,
  `cp500`, `cp1140`, …).
- Unpacking COBOL numeric usages: `DISPLAY`, `COMP-3` packed, zoned
  decimal, signs in nibbles — the same set Docling implements.
- One `TableItem` per record type in the layout. Rows stream.
- `max_records`, `strip_control_characters` — Docling's knobs.

## What this process does not own

| Concern | Owner |
|---|---|
| Inventing a copybook from the file | out of scope |
| VSAM / IMS / mainframe connectivity | a connector, not a parser |
| EBCDIC inside a PDF or office doc | not this format |
| Export to CSV | protomolt sink, from the table |

## Language

**Rust**. The decode is a tight byte walk; no GC on the per-record
path. Codecs from `encoding_rs` / a small EBCDIC table. Layout is a
protobuf message (and optionally JSON, matching Docling's
`layout_file`, parsed in-process — the client sends the bytes, we do
not read a server path).
