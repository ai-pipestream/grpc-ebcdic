# grpc-ebcdic design

## 1. Goals

- Decode the COBOL data descriptions a record layout is made of, exactly:
  character fields through a named code page, packed and zoned decimals with
  their scale preserved, binary integers. No floats, no lossy text.
- Stream rows as soon as a record is decoded. A multi-gigabyte dump must not
  become one protobuf table in RAM. UIs merge rows live; they do not wait for
  `ParseStatus`. A batch parse returns the finished tables at the end. We do
  not.
- Layout is required and is part of the request, never a file on the server.

## 2. Non-goals (v1)

- Compiling arbitrary COBOL source. We accept the already-normalized layout
  (field name, size, type, scale, picture) as `EbcdicLayout`. A later helper
  may compile copybooks offline.
- Variable-length records (RDW) in v1 unless the layout marks them; default is
  fixed width = sum of field lengths.
- EBCDIC as a charset for some other format's text layer.

## 3. Wire API (sketch)

`ai.pipestream.ebcdic.v1.EbcdicParseService`

```text
rpc ParseEbcdic(stream ParseEbcdicRequest) returns (stream ParseEbcdicEvent);
rpc GetServiceInfo(GetServiceInfoRequest) returns (ServiceInfo);
```

Options (first message):

| Field | Default | Meaning |
|---|---|---|
| `encoding` | `cp037` | EBCDIC code page for character data |
| `layout` | one of the two required | protobuf `EbcdicLayout` (record selectors + fields) |
| `layout_json` | one of the two required | the same layout model as JSON bytes |
| `max_records` | `0` (all) | stop after this many records |
| `strip_control_characters` | `true` | drop Unicode control characters from text |
| `max_document_mib` | server default | per-stream byte cap |

Missing both layout forms is `INVALID_ARGUMENT`.

Events:

1. `LayoutInfo`: record length, field count, codec
2. `RecordRow`: record type name, 0-based index, typed cells
3. `ParseStatus`: records kept, bytes consumed, trailing-byte warning if the
   file is not a multiple of the record length

## 4. Mapping to Document: implemented in-repo (`src/document_fold.rs`)

One `TableItem` per record schema. Header row = field names. Cell types:
string, decimal (as string to preserve scale), integer. Packed and zoned
numbers become decimals, never floats.

`CollectorSource.collector = "ebcdic"`. No pages.

A batch parse emits the whole table at once. We stream rows and let gRParse
assemble the `TableItem`, so a 10 M-row dump does not inflate one page event.

That last paragraph was the whole answer until the fleet's collectors each
grew a Document fold. It is still the default answer (the row stream is the
product and nothing changes about it), but "stream rows, don't inflate one
event" now has a bounded counterpart for callers who want the Document plane
from the collector itself:

- `ParseOptions.emit_document` (off by default) turns on the fold.
- The fold consumes the collector's own response events, in wire order, and
  produces one `ai.pipestream.document.v1.Document`, emitted as a fourth
  `ParseEbcdicResponse.event` variant immediately before the `status` trailer.
  The trailer stays last; the `record` events are untouched and remain the
  lossless result.
- The bound is a per-schema row cap (100,000). Past it rows are counted, not
  folded, and the trailer carries `WARNING_CODE_DOCUMENT_ROWS_TRUNCATED`
  naming the schema and the dropped count, with the same count in the table's
  `data.record_layout.rows_truncated`. There is no silent cap. Pair
  `emit_document` with `max_records` for a whole Document. Dropping rows is
  not the same as matching none: a schema whose rows were all dropped still
  gets its table, carrying the count that says so, while a schema the input
  never mentioned is left out of the document.

### Shape

The fold builds a flat document: no groups, the description first, a heading
per schema only when the layout declares more than one, and the tables as
siblings of those headings rather than children. Everything hangs off
`#/body`.

| Document | Source |
|---|---|
| `schema_name` | the pipestream document schema v2 identifier |
| `name` | `LayoutInfo.description`, else the first schema name |
| `origin` | unset: the stream carries bytes and a layout, never a filename or a media type |
| `body.meta.custom_fields` | `ebcdic.layout_source`, plus `ebcdic.encoding`, `ebcdic.header_size`, `ebcdic.footer_size` and `ebcdic.prefix_size` as duplicates of `record_layout`, kept for one release |
| `groups` | empty: this mapping uses none |
| one `TextItem` (`TEXT`) | the layout description, when there is one, first on `#/body` |
| one `SectionHeaderItem` per record schema | the schema name, level 1, on `#/body`, only when the layout declares more than one schema |
| one `TableItem` per record schema | on `#/body`, the heading's sibling; grid row 0 is the field names with `column_header = true`, one non-`SKIP` field per column |
| a schema that matched no record | nothing at all: no heading, no table |
| one grid row per `RecordRow` | cells aligned by name against the schema's fields; `text` verbatim, `Decimal` as `Decimal.text` exactly, `integer` as its decimal string |
| `TableCell.value` | the cell's number when the field is numeric, beside the rendering in `text`; a character field has none |
| `TableData.columns` | one `TableColumnSchema` per column in layout order: declared type, picture clause, byte offset and width in the record body, COBOL level, the dotted qualification path with its `OCCURS` subscript, the occurrence as `occurs_index`, and the level-88 `conditions` |
| `TableColumnSchema.conditions` | one `ValueCondition` per level-88 name; one `ValueRange` per literal, `low` alone for a bare `VALUE`, `low` and `high` for a `THRU`, several ranges for several literals, all keeping the copybook's own literals |
| `TableData.record_layout` | the code page, the schema's record length, the header, footer and prefix byte trims, and the rows the cap dropped, as typed numbers |
| `TableData.row_prov` | one entry per grid row carrying the record's `ByteSpan` in the input; the header row's entry carries no location, because the header came from the copybook and not from the input |
| `table.meta.custom_fields` | `ebcdic.schema` and `ebcdic.selector` (when the layout has one), plus `ebcdic.record_length`, `ebcdic.rows` and `ebcdic.rows_truncated` (only when rows were dropped) as duplicates of the typed fields, kept for one release |
| `CollectorSource` on every item | `collector = "ebcdic"`, `model` = the layout form (`proto` / `json` / `copybook`), `version` = the server's crate version |

One field goes past the minimum: `table.meta.custom_fields["ebcdic.schema"]`
always names the record schema. A heading names a schema only when there is
more than one, so a single-schema document would otherwise lose the name
entirely; naming it in the table's own metadata keeps every table
self-describing without changing the item shape.

### Deliberately not mapped

- `bbox`, `pages`, and item-level `prov`. An EBCDIC record has a byte offset,
  not a page and not a rectangle. Inventing either would make a downstream
  viewer draw a box over nothing. The byte offsets now reach the Document as
  `TableData.row_prov`, one `ByteSpan` per row; a table carries no `prov` of
  its own because the records of one schema are interleaved with every other
  schema's and span no single range.
- `confidence` on the source. A copybook is a declaration and the mapping is
  deterministic, so a confidence would be noise.
- `field_regions` / `field_items`. The coordinator's additive merge does not
  renumber them and drops them silently.
- Filler fields. `FIELD_TYPE_SKIP` produces no cell on the wire and no column
  in the table; the bytes are consumed and that is all. Nothing is hidden by
  it: a filler is the gap between the byte offsets of the columns on either
  side of it, and a trailing one is the difference between the last column's
  end and `record_layout.record_length`.
- Nothing about the copybook, any more. Level-88 condition names and the
  numeric `OCCURS` index were the two the document schema had no slot for;
  `TableColumnSchema.conditions` and `TableColumnSchema.occurs_index` are those
  slots. Both are still first-class on this collector's own contract as well,
  in `FieldSchema.conditions` and `FieldSchema.occurs_index`, which is the
  lossless view, and the column path still carries the COBOL subscript that
  names the occurrence.
- The trailer's counts. `ParseStatus` is folded last so the fold knows nothing
  else is coming, but its numbers are counts of the parse. A table's
  `num_rows` counts what is actually in that table, which is the number a
  reader of the Document can check.

The fold ships an integrity checker (`document_fold::integrity_errors`, the
idea ported from gRParse's document integrity checker) and every fold test
asserts it is clean: dense local numbering, unique `self_ref`, symmetric
parent/child links, no reference to anything the fold did not create. That is
what makes the fragment safe for the coordinator's additive merge.

## 5. Layout message

The layout model, `EbcdicField` / `EbcdicRecordLayout`:

- `name`, `offset`, `length`, `field_type` (`ALPHANUMERIC`, `PACKED_DECIMAL`,
  `ZONED_DECIMAL`, `BINARY`, and friends)
- record `selector` when multiple schemas share a file (byte at offset equals
  a tag)

Overlapping fields or a selector that matches two schemas is
`INVALID_ARGUMENT` at request validation, before any row.

## 6. Tests

- Hand-built buffer of known packed/zoned/display fields; assert cell values
  exactly (including negative nibble `0xd`).
- Wrong codec produces different strings; test both cp037 and cp500 on the
  same bytes.
- No layout gives `INVALID_ARGUMENT`.
- Trailing partial record gives a warning, not a failed parse, unless
  `abort_on_error`.
- Control bytes in an alphanumeric field stripped when the option is on,
  preserved when off.
