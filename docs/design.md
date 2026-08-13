# grpc-ebcdic design

## 1. Goals

- Feature parity with Docling `InputFormat.EBCDIC` and
  `EbcdicBackendOptions`.
- Stream rows as soon as a record is decoded. A multi-gigabyte dump
  must not become one protobuf table in RAM.
- Layout is required and is part of the request, never a file on the
  server.

## 2. Non-goals (v1)

- Compiling arbitrary COBOL source. We accept the already-normalized
  layout (field name, offset, length, type, pic) that Docling calls
  `EbcdicLayout`. A later helper may compile copybooks offline.
- Variable-length records (RDW) in v1 unless the layout marks them;
  default is fixed width = sum of field lengths.
- EBCDIC as a charset for some other format's text layer.

## 3. Wire API (sketch)

`ai.pipestream.ebcdic.v1.EbcdicParseService`

```text
rpc ParseEbcdic(stream ParseEbcdicRequest) returns (stream ParseEbcdicEvent);
rpc GetServiceInfo(GetServiceInfoRequest) returns (ServiceInfo);
```

Options (first message):

- `encoding` — default `cp037`
- `layout` — protobuf `EbcdicLayout` (record selectors + fields)
- `layout_json` — mutually exclusive, Docling's JSON shape
- `max_records`
- `strip_control_characters` — default true
- `max_document_mib`

Missing both layout forms is `INVALID_ARGUMENT`.

Events:

1. `LayoutInfo` — record length, field count, codec
2. `RecordRow` — record type name, 0-based index, typed cells
3. `ParseStatus` — records kept, bytes consumed, trailing-byte warning
   if the file is not a multiple of the record length

## 4. Mapping to Document

One `TableItem` per record schema. Header row = field names. Cell
types: string, decimal (as string to preserve scale), integer. Packed
and zoned numbers become decimals, never floats.

`CollectorSource.collector = "ebcdic"`. No pages.

Docling emits the whole table at once. We stream rows and let gRParse
assemble the `TableItem`, so a 10 M-row dump does not inflate one
page event.

## 5. Layout message

Mirror Docling's `EbcdicField` / `EbcdicRecordLayout`:

- `name`, `offset`, `length`, `field_type` (`ALPHANUMERIC`,
  `PACKED_DECIMAL`, `ZONED_DECIMAL`, `BINARY`, …)
- record `selector` when multiple schemas share a file (byte at
  offset equals a tag)

Overlapping fields or a selector that matches two schemas →
`INVALID_ARGUMENT` at request validation, before any row.

## 6. Tests

- Hand-built buffer of known packed/zoned/display fields; assert cell
  values exactly (including negative nibble `0xd`).
- Wrong codec produces different strings; test both cp037 and cp500
  on the same bytes.
- No layout → `INVALID_ARGUMENT`.
- Trailing partial record → warning, not a failed parse, unless
  `abort_on_error`.
- Control bytes in an alphanumeric field stripped when the option is
  on, preserved when off.
