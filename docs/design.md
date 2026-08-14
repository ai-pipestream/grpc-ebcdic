# grpc-ebcdic design

## 1. Goals

- Feature parity with Docling `InputFormat.EBCDIC` and
  `EbcdicBackendOptions`.
- Stream rows as soon as a record is decoded. A multi-gigabyte dump
  must not become one protobuf table in RAM. UIs merge rows live;
  they do not wait for `ParseStatus`. Docling returns the finished
  tables. We do not.
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

## 4. Mapping to Document — implemented in-repo (`src/document_fold.rs`)

One `TableItem` per record schema. Header row = field names. Cell
types: string, decimal (as string to preserve scale), integer. Packed
and zoned numbers become decimals, never floats.

`CollectorSource.collector = "ebcdic"`. No pages.

Docling emits the whole table at once. We stream rows and let gRParse
assemble the `TableItem`, so a 10 M-row dump does not inflate one
page event.

That last paragraph was the whole answer until the fleet's collectors
each grew a Document fold. It is still the *default* answer — the row
stream is the product and nothing changes about it — but "stream rows,
don't inflate one event" now has a bounded counterpart for callers who
want the Document plane from the collector itself:

- `ParseOptions.emit_document` (off by default) turns on the fold.
- The fold consumes the collector's **own** response events, in wire
  order, and produces one `ai.pipestream.document.v1.Document`, emitted
  as a fourth `ParseEbcdicResponse.event` variant immediately before the
  `status` trailer. The trailer stays last; the `record` events are
  untouched and remain the lossless result.
- The bound is a per-schema row cap (100 000). Past it rows are counted,
  not folded, and the trailer carries
  `WARNING_CODE_DOCUMENT_ROWS_TRUNCATED` naming the schema and the
  dropped count, with the same count in the table's
  `meta.custom_fields["ebcdic.rows_truncated"]`. There is no silent cap.
  Pair `emit_document` with `max_records` for a whole Document.

### Shape

| Document | Source |
|---|---|
| `schema_name` | `"docling_document_v2"` |
| `name` | `LayoutInfo.description`, else the first schema name |
| `origin` | unset — the stream carries bytes and a layout, never a filename or a media type |
| `body.meta.custom_fields` | `ebcdic.encoding`, `ebcdic.layout_source`, `ebcdic.header_size`, `ebcdic.footer_size`, `ebcdic.prefix_size` |
| one `GroupItem` per record schema | `GROUP_LABEL_SHEET`, named by the schema, child of `#/body`, holding exactly its own table |
| one `TableItem` per record schema | grid row 0 is the field names with `column_header = true`, one non-`SKIP` field per column |
| one grid row per `RecordRow` | cells aligned by name against the schema's fields; `text` verbatim, `Decimal` as `Decimal.text` exactly, `integer` as its decimal string |
| `table.meta.custom_fields` | `ebcdic.record_length`, `ebcdic.selector` (when the layout has one), `ebcdic.rows`, `ebcdic.rows_truncated` (only when rows were dropped) |
| `CollectorSource` on every table | `collector = "ebcdic"`, `model` = the layout form (`proto` / `json` / `copybook`), `version` = the server's crate version |

### Deliberately not mapped

- **`prov`, `bbox`, `pages`.** An EBCDIC record has a byte offset, not a
  page and not a rectangle. Inventing either would make a downstream
  viewer draw a box over nothing. The offsets are in the `record`
  events, which is where a caller that needs them should look.
- **`confidence` on the source.** A copybook is a declaration and the
  mapping is deterministic, so a confidence would be noise.
- **`field_regions` / `field_items`.** The coordinator's additive merge
  does not renumber them and drops them silently.
- **Filler fields.** `FIELD_TYPE_SKIP` produces no cell on the wire and
  no column in the table; the bytes are consumed and that is all.
- **The trailer's counts.** `ParseStatus` is folded last so the fold
  knows nothing else is coming, but its numbers are counts of the parse.
  A table's `ebcdic.rows` counts what is actually in that table, which is
  the number a reader of the Document can check.

The fold ships an integrity checker (`document_fold::integrity_errors`,
the idea ported from gRParse's `docling_integrity_errors`) and every fold
test asserts it is clean: dense local numbering, unique `self_ref`,
symmetric parent/child links, no reference to anything the fold did not
create. That is what makes the fragment safe for the coordinator's
additive merge.

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
