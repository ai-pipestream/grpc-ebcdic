// SPDX-License-Identifier: Apache-2.0

//! The Document fold: this collector's own event stream projected into a
//! single `ai.pipestream.document.v1.Document`.
//!
//! The typed event stream is the product and stays lossless; this is the lossy
//! structural projection for callers that want the gRParse Document plane. It
//! is a fold rather than a second parser: it consumes exactly the
//! [`ParseEbcdicResponse`](crate::proto::v1::ParseEbcdicResponse) events the
//! service writes to the wire, in the order it writes them, so there is no way
//! for the Document to describe a parse that did not happen.
//!
//! The shape is `docs/design.md` §4 made literal, and §4 in turn mirrors
//! docling's own EBCDIC backend (`docling/backend/ebcdic_backend.py`), which
//! builds a flat document with no groups at all:
//!
//! - the layout description, when there is one, is the first item: a
//!   [`TextItem`](crate::proto::document::v1::TextItem) labelled `TEXT` on
//!   `#/body`;
//! - then, per record schema in layout order, a
//!   [`SectionHeaderItem`](crate::proto::document::v1::SectionHeaderItem)
//!   naming the schema — only when the layout declares more than one, exactly
//!   as upstream — followed by the schema's
//!   [`TableItem`](crate::proto::document::v1::TableItem). Both hang directly
//!   off `#/body`: the table is the heading's *sibling*, not its child;
//! - a schema that matched no record produces nothing, so a Document never
//!   shows a table a reader would find empty;
//! - each table's first grid row is the schema's field names, then one grid row
//!   per [`RecordRow`](crate::proto::v1::RecordRow), cells in field order,
//!   decimals carried across as their exact canonical text.
//!
//! What the flat shape cannot say, the extended schema now can, and the fold
//! fills all of it in:
//!
//! - `TableData.columns` is one
//!   [`TableColumnSchema`](crate::proto::document::v1::TableColumnSchema) per
//!   column in layout order, carrying the declared type, the picture clause,
//!   the byte offset and width inside the record body, the COBOL level number,
//!   and the qualification path. A grid of strings becomes a grid a consumer
//!   can parse without guessing from the rendering.
//! - `TableCell.value` is the cell's number when it has one, alongside the
//!   rendering in `text` that every existing reader already reads.
//! - `TableData.row_prov` is one
//!   [`ProvenanceItem`](crate::proto::document::v1::ProvenanceItem) per grid
//!   row carrying the record's byte extent, which is the only location a
//!   record has.
//! - `TableData.record_layout` is the fixed-record layout the table was
//!   decoded with, typed: the code page, the record length, the header,
//!   footer and prefix byte trims, and the number of rows the fold's own cap
//!   dropped. Those facts used to be stringly-typed `ebcdic.*` custom fields
//!   on the body and the table, which are still written for one release and
//!   are no longer the truth.
//! - `TableColumnSchema.conditions` is the level-88 condition names of the
//!   column, each one a named list of literals and inclusive ranges with the
//!   bounds apart and spelled the way the copybook spelled them.
//!   `TableColumnSchema.occurs_index` is the `OCCURS` occurrence as a number,
//!   beside the COBOL subscript the column path already carries.
//!
//! Nothing the copybook declares stops at this boundary any more. The
//! conditions still ride this collector's own contract too, in
//! `FieldSchema.conditions`, which is the lossless view and the one a client
//! of the row stream reads.
//!
//! One thing is deliberately kept past docling: every table carries its schema
//! name in `meta.custom_fields["ebcdic.schema"]`. Upstream loses that name
//! entirely in the single-schema case, where it emits no heading, and a table
//! that cannot say which copybook record it holds is worth less than the two
//! dozen bytes the field costs.
//!
//! Two properties are load-bearing and are asserted in the tests:
//!
//! - **Bounded.** A Document is one protobuf message and a mainframe extract
//!   is not. The fold holds every row it folds, so it caps the rows per schema
//!   at [`DEFAULT_ROW_CAP`] and reports what it dropped through the parse's own
//!   warning channel rather than quietly shortening a table.
//! - **Merge-safe.** The coordinator merges fragments additively and renumbers
//!   refs, so the fragment must be self-contained: dense local numbering, every
//!   `self_ref` unique, every parent/child link symmetric, no reference to
//!   anything this fold did not create. [`integrity_errors`] checks exactly
//!   that and every fold test runs it.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use prost_types::Value;
use prost_types::value::Kind;

use crate::proto::document::v1 as doc;
use crate::proto::v1 as pb;

/// Value of `CollectorSource.collector` on every item this fold creates.
pub const COLLECTOR: &str = "ebcdic";

/// Value of `Document.schema_name`: the upstream docling schema this shape
/// tracks.
pub const SCHEMA_NAME: &str = "docling_document_v2";

/// Rows folded per record schema before the fold starts counting instead.
///
/// Clients commonly cap a received message at 4 MiB and a Document cannot be
/// streamed, so "fold every row" is not an option a ten-million-row extract
/// leaves open. A hundred thousand rows of a typical copybook is a Document in
/// the tens of megabytes: past what a default client accepts, which is the
/// point — the cap exists so the failure is a counted, reported truncation
/// rather than a message nobody can receive.
pub const DEFAULT_ROW_CAP: u64 = 100_000;

/// JSON-Pointer self reference of the body group.
const BODY_REF: &str = "#/body";

/// JSON-Pointer self reference of the furniture group.
const FURNITURE_REF: &str = "#/furniture";

/// One record schema's rows, and the counters the finalizer needs.
///
/// The rows are held here rather than in the document arena because a schema
/// that matches no record is not written to the document at all: nothing can be
/// appended to the arenas until the last event has been seen and the empty
/// schemas are known.
struct TableState {
    /// Schema name, as it appears on every row and in the warning message.
    name: String,
    /// Non-filler columns in field order: the header row, the column order
    /// every data row is aligned to, and the declarations behind both.
    columns: Vec<Column>,
    /// Column position by field name, so a row's named cells can be placed
    /// without trusting their order.
    column_index: BTreeMap<String, usize>,
    /// Body length the layout resolved for this schema, in bytes.
    record_length: u32,
    /// Record-type value that selects this schema, when the layout has more
    /// than one.
    selector: Option<String>,
    /// Data rows folded so far, header row excluded.
    rows: u64,
    /// Rows seen past the cap and therefore not folded.
    dropped: u64,
    /// Input offset of the first dropped record, for the warning.
    first_dropped_offset: u64,
    /// The folded data rows, in arrival order, header row excluded.
    grid: Vec<doc::TableRow>,
    /// Where each folded row sat in the input, index-aligned with `grid`.
    row_prov: Vec<doc::ProvenanceItem>,
}

/// One column of a folded table: what its header cell says, and what the
/// layout declared about the bytes behind it.
struct Column {
    /// Field name as the layout declared it, which is the header cell's text
    /// and the name a row's cells are placed by.
    name: String,
    /// The declaration that lands in `TableData.columns`.
    schema: doc::TableColumnSchema,
}

/// A single-pass fold from parse events to one Document.
///
/// Feed it every outbound event in wire order with [`consume`](Self::consume),
/// take the truncation warnings with
/// [`truncation_warnings`](Self::truncation_warnings) so they can be merged
/// into the trailer, then finish with [`take`](Self::take).
pub struct DocumentFold {
    /// The document under construction.
    document: doc::Document,
    /// Server version, stamped as `CollectorSource.version`.
    version: String,
    /// Layout form the request used, stamped as `CollectorSource.model`.
    model: Option<String>,
    /// Rows folded per schema before the surplus is counted instead.
    row_cap: u64,
    /// The parse-wide half of every table's `TableData.record_layout`: the
    /// code page and the three byte trims, which the layout states once and
    /// every schema decoded with it shares. The per-schema half, the record
    /// length and the truncation count, is filled in as each table is written.
    layout: doc::RecordLayoutMeta,
    /// Per-schema state, in layout order.
    tables: Vec<TableState>,
    /// Table state index by schema name.
    index_by_schema: BTreeMap<String, usize>,
    /// The layout's description, which becomes the document's first text item.
    description: String,
    /// Whether the opening `layout_info` has been folded.
    started: bool,
}

impl DocumentFold {
    /// Start a fold that stamps `version` on every item it creates.
    #[must_use]
    pub fn new(version: impl Into<String>) -> Self {
        Self {
            document: doc::Document {
                schema_name: Some(SCHEMA_NAME.to_string()),
                // No origin: this stream carries bytes and a layout, never a
                // filename or a media type. Inventing either would be a claim
                // the collector cannot support.
                origin: None,
                body: Some(doc::GroupItem {
                    self_ref: BODY_REF.to_string(),
                    content_layer: doc::ContentLayer::Body as i32,
                    ..Default::default()
                }),
                furniture: Some(doc::GroupItem {
                    self_ref: FURNITURE_REF.to_string(),
                    content_layer: doc::ContentLayer::Furniture as i32,
                    ..Default::default()
                }),
                ..Default::default()
            },
            version: version.into(),
            model: None,
            row_cap: DEFAULT_ROW_CAP,
            layout: doc::RecordLayoutMeta::default(),
            tables: Vec::new(),
            index_by_schema: BTreeMap::new(),
            description: String::new(),
            started: false,
        }
    }

    /// Override how many rows per schema are folded.
    ///
    /// A test knob first and an operator knob second: the tests prove the cap
    /// reports itself without assembling a hundred thousand records.
    #[must_use]
    pub const fn with_row_cap(mut self, rows: u64) -> Self {
        self.row_cap = rows;
        self
    }

    /// Fold one outbound event.
    ///
    /// Cheap and stateful: `layout_info` works out the columns of every record
    /// schema; each `record` appends one grid row to its schema's rows or
    /// counts itself as dropped.
    pub fn consume(&mut self, event: &pb::parse_ebcdic_response::Event) {
        match event {
            pb::parse_ebcdic_response::Event::LayoutInfo(info) => self.begin(info),
            pb::parse_ebcdic_response::Event::Record(row) => self.row(row),
            // Neither of these changes the Document. The trailer is counts of
            // the *parse*, while a table's `ebcdic.rows` is the count of what
            // is actually in front of the reader, so folding the trailer's
            // numbers in would state the same thing twice and sometimes
            // disagree with itself; it is folded only because it is the last
            // event, which is the fold's cue that nothing else is coming. The
            // document event is the fold's own output and it never sees it:
            // the server builds it after taking the fold apart.
            pb::parse_ebcdic_response::Event::Status(_)
            | pb::parse_ebcdic_response::Event::Document(_) => {}
        }
    }

    /// Warnings for every schema whose rows did not all fit.
    ///
    /// Call before [`take`](Self::take) and merge the result into the trailer:
    /// a cap that shortens a table without saying so is the silent-truncation
    /// failure this whole channel exists to avoid.
    #[must_use]
    pub fn truncation_warnings(&self) -> Vec<pb::ParseWarning> {
        self.tables
            .iter()
            .filter(|state| state.dropped > 0)
            .map(|state| pb::ParseWarning {
                code: pb::WarningCode::DocumentRowsTruncated as i32,
                message: format!(
                    "record schema {:?} produced {} rows past the {}-row Document fold cap; its \
                     table holds the first {} and {} were dropped. Every row was sent as a \
                     `record` event, so re-run with a smaller max_records for a whole Document.",
                    state.name, state.dropped, self.row_cap, state.rows, state.dropped
                ),
                byte_offset: state.first_dropped_offset,
            })
            .collect()
    }

    /// Finish the fold and hand back the Document.
    ///
    /// This is where the document is written: the description, then the
    /// schemas in layout order, each one a heading and a table or — when it
    /// matched no record — nothing at all. Which schemas those are is only
    /// knowable once the last row has been seen, so the arenas stay empty
    /// until now.
    #[must_use]
    pub fn take(mut self) -> doc::Document {
        let description = std::mem::take(&mut self.description);
        if !description.is_empty() {
            self.add_description(&description);
        }
        let states = std::mem::take(&mut self.tables);
        // Upstream keys the headings off the declared schema count, not the
        // surviving one, so a two-schema layout with one empty schema still
        // says which schema the surviving table is.
        let headings = states.len() > 1;
        for state in states {
            // Empty means "no record of this schema was in the input". A
            // schema whose rows were all dropped by the cap did occur, and its
            // table carries the truncation count that says so.
            if state.rows == 0 && state.dropped == 0 {
                continue;
            }
            if headings {
                self.add_heading(&state.name);
            }
            self.add_table(state);
        }
        self.document
    }

    /// Work out the columns of every record schema and the document's facts.
    fn begin(&mut self, info: &pb::LayoutInfo) {
        if self.started {
            return;
        }
        self.started = true;
        self.model = layout_source_name(info.source).map(str::to_string);
        self.description.clone_from(&info.description);

        // The layout's own description is the best name available; a file of
        // EBCDIC bytes has no title of its own. Failing that, the first record
        // schema at least says what the rows are.
        self.document.name = if info.description.is_empty() {
            info.records
                .first()
                .map_or_else(String::new, |schema| schema.name.clone())
        } else {
            info.description.clone()
        };

        // The code page and the three byte trims the layout states once. Every
        // table decoded with this layout carries them typed, in
        // `TableData.record_layout`, beside the record length only that schema
        // knows.
        self.layout = doc::RecordLayoutMeta {
            encoding: Some(info.encoding.clone()),
            record_length: None,
            header_bytes: Some(u64::from(info.header_size)),
            footer_bytes: Some(u64::from(info.footer_size)),
            prefix_bytes: Some(u64::from(info.prefix_size)),
            rows_truncated: None,
        };

        // Four of the body's five keys now have that typed home and are the
        // duplicates kept for one release; `ebcdic.layout_source` remains the
        // duplicate it always was of `CollectorSource.model`. See
        // `table_custom_fields` for the window this repo is in.
        let mut custom_fields = HashMap::new();
        custom_fields.insert("ebcdic.encoding".to_string(), string_value(&info.encoding));
        if let Some(source) = self.model.clone() {
            custom_fields.insert("ebcdic.layout_source".to_string(), string_value(&source));
        }
        custom_fields.insert(
            "ebcdic.header_size".to_string(),
            number_value(u64::from(info.header_size)),
        );
        custom_fields.insert(
            "ebcdic.footer_size".to_string(),
            number_value(u64::from(info.footer_size)),
        );
        custom_fields.insert(
            "ebcdic.prefix_size".to_string(),
            number_value(u64::from(info.prefix_size)),
        );
        if let Some(body) = self.document.body.as_mut() {
            body.meta = Some(doc::BaseMeta {
                custom_fields,
                ..Default::default()
            });
        }

        for schema in &info.records {
            // Fillers are not columns: they name nothing and decode to
            // nothing. They are not lost either, because every column declares
            // its byte offset and width, so a filler is the gap between two
            // neighbours and the last column's end is short of
            // `ebcdic.record_length` by exactly a trailing filler's width.
            let columns: Vec<Column> = schema
                .fields
                .iter()
                .filter(|field| field.r#type != pb::FieldType::Skip as i32)
                .map(column)
                .collect();
            let column_index = columns
                .iter()
                .enumerate()
                .map(|(index, column)| (column.name.clone(), index))
                .collect();
            self.index_by_schema
                .insert(schema.name.clone(), self.tables.len());
            self.tables.push(TableState {
                name: schema.name.clone(),
                columns,
                column_index,
                record_length: schema.record_length,
                selector: schema.selector.clone(),
                rows: 0,
                dropped: 0,
                first_dropped_offset: 0,
                grid: Vec::new(),
                row_prov: Vec::new(),
            });
        }
    }

    /// Append one decoded record as a grid row, or count it as dropped.
    fn row(&mut self, row: &pb::RecordRow) {
        let Some(&index) = self.index_by_schema.get(&row.record_type) else {
            // Unreachable through the service: the walk only emits rows for
            // schemas the layout declared, and the layout arrived first.
            return;
        };
        let state = &mut self.tables[index];
        if state.rows >= self.row_cap {
            if state.dropped == 0 {
                state.first_dropped_offset = row.byte_offset;
            }
            state.dropped += 1;
            return;
        }
        let grid_row = state.rows + 1;
        state.rows += 1;
        // Cells arrive named, so they are placed by name rather than by
        // position: a short row leaves its columns empty instead of shifting
        // every later value one column to the left.
        let mut values: Vec<(String, Option<doc::CellValue>)> =
            vec![(String::new(), None); state.columns.len()];
        for cell in &row.cells {
            if let Some(&column) = state.column_index.get(&cell.name) {
                values[column] = (cell_text(cell), cell_value(cell));
            }
        }

        let cells: Vec<doc::TableCell> = values
            .into_iter()
            .enumerate()
            .map(|(column, (text, value))| table_cell(text, value, grid_row, column, false))
            .collect();
        state.grid.push(doc::TableRow { cells });
        // The record's own extent in the input. A row of a mainframe extract
        // has no page and no box, and this is the location an auditor of one
        // actually asks for.
        state.row_prov.push(doc::ProvenanceItem {
            byte_range: Some(doc::ByteSpan {
                start: row.byte_offset,
                end: row.byte_offset.saturating_add(row.byte_length),
            }),
            ..Default::default()
        });
    }

    /// Append the layout description as the document's opening text item.
    fn add_description(&mut self, text: &str) {
        let base = self.text_base(doc::DocItemLabel::Text, text);
        self.document.texts.push(doc::BaseTextItem {
            item: Some(doc::base_text_item::Item::Text(doc::TextItem {
                base: Some(base),
            })),
        });
    }

    /// Append a section header naming a record schema.
    ///
    /// A sibling of its table rather than its parent, which is upstream's
    /// shape: `DoclingDocument.add_heading` appends to the current parent and
    /// the table that follows it appends to the same one.
    fn add_heading(&mut self, name: &str) {
        let base = self.text_base(doc::DocItemLabel::SectionHeader, name);
        self.document.texts.push(doc::BaseTextItem {
            item: Some(doc::base_text_item::Item::SectionHeader(
                doc::SectionHeaderItem {
                    base: Some(base),
                    // The schemas of a layout are siblings, so their headings
                    // are all at the one level upstream's add_heading defaults
                    // to.
                    level: 1,
                },
            )),
        });
    }

    /// The base of one text item on `#/body`, linked in both directions.
    ///
    /// The self ref is the next free slot of the text arena and the body is
    /// already pointing at it, so the caller has to push the item it wraps
    /// straight away.
    fn text_base(&mut self, label: doc::DocItemLabel, text: &str) -> doc::TextItemBase {
        let self_ref = format!("#/texts/{}", self.document.texts.len());
        self.link_body_child(&self_ref);
        doc::TextItemBase {
            self_ref,
            parent: Some(ref_item(BODY_REF)),
            content_layer: doc::ContentLayer::Body as i32,
            label: label as i32,
            // No prov, for the reason the tables carry none.
            orig: text.to_string(),
            text: text.to_string(),
            source: vec![self.collector_source()],
            ..Default::default()
        }
    }

    /// Append one schema's table to the body: header row, data rows, and the
    /// facts that are only knowable now the rows are all in.
    fn add_table(&mut self, mut state: TableState) {
        let self_ref = format!("#/tables/{}", self.document.tables.len());
        let custom_fields = table_custom_fields(&state);
        // The layout this table was decoded with, typed: the parse-wide code
        // page and byte trims, this schema's own record length, and the count
        // of what the row cap dropped. The count is always set, because zero
        // is the answer to "was anything dropped" and not the absence of one.
        let record_layout = doc::RecordLayoutMeta {
            record_length: Some(u64::from(state.record_length)),
            rows_truncated: Some(state.dropped),
            ..self.layout.clone()
        };
        let header: Vec<doc::TableCell> = state
            .columns
            .iter()
            .enumerate()
            .map(|(column, declared)| table_cell(declared.name.clone(), None, 0, column, true))
            .collect();
        // Both arenas, always: a consumer may read the flat cells or walk the
        // grid, and a document where those two disagree is worse than one that
        // carries neither.
        let mut table_cells = header.clone();
        for row in &state.grid {
            table_cells.extend(row.cells.iter().cloned());
        }
        let mut grid = vec![doc::TableRow { cells: header }];
        grid.append(&mut state.grid);
        // Row provenance is index-aligned with the grid, and the grid's first
        // row is the header. The header was assembled from the copybook rather
        // than read from the input, so its entry names no location at all: an
        // empty entry is the only honest one, and dropping it would put every
        // later row one place out.
        let mut row_prov = vec![doc::ProvenanceItem::default()];
        row_prov.append(&mut state.row_prov);
        let columns: Vec<doc::TableColumnSchema> = std::mem::take(&mut state.columns)
            .into_iter()
            .map(|column| column.schema)
            .collect();
        let num_cols = clamp(columns.len() as u64);

        let source = self.collector_source();
        self.link_body_child(&self_ref);
        self.document.tables.push(doc::TableItem {
            self_ref,
            parent: Some(ref_item(BODY_REF)),
            content_layer: doc::ContentLayer::Body as i32,
            label: doc::DocItemLabel::Table as i32,
            // No prov on the table itself: the records of one schema are
            // interleaved with every other schema's in the input, so a table
            // spans no single range of bytes. Its rows do, and each one says
            // so in `data.row_prov`.
            meta: Some(doc::FloatingMeta {
                custom_fields,
                ..Default::default()
            }),
            data: Some(doc::TableData {
                // The header row is a row of the grid, so the count includes it.
                num_rows: clamp(state.rows.saturating_add(1)),
                num_cols,
                table_cells,
                grid,
                columns,
                row_prov,
                record_layout: Some(record_layout),
                ..Default::default()
            }),
            source: vec![source],
            ..Default::default()
        });
    }

    /// Record `child_ref` in the body's children list.
    ///
    /// The other half of the parent pointer: the merge walks children, the
    /// integrity check walks both, and a link written in one direction only is
    /// a fragment that silently loses items downstream.
    fn link_body_child(&mut self, child_ref: &str) {
        if let Some(body) = self.document.body.as_mut() {
            body.children.push(ref_item(child_ref));
        }
    }

    /// The attribution stamped on every item this fold creates.
    fn collector_source(&self) -> doc::SourceType {
        doc::SourceType {
            source: Some(doc::source_type::Source::Collector(doc::CollectorSource {
                collector: COLLECTOR.to_string(),
                model: self.model.clone(),
                version: Some(self.version.clone()),
                // Omitted on purpose: a copybook is a declaration and this
                // mapping is deterministic, so a confidence would be noise,
                // and an uncalibrated signal behind a confidence that does not
                // exist would be noise about noise.
                confidence: None,
                raw_score: None,
                raw_score_kind: None,
                raw_score_samples: None,
            })),
        }
    }
}

/// The custom fields of one schema's table.
///
/// `ebcdic.schema` is the one field upstream has no equivalent of: a schema is
/// named only through the heading emitted when there is more than one, so a
/// single-schema document loses the name altogether. Naming it here costs
/// nothing and keeps every table self-describing.
///
/// Where the extended schema has grown a home, that home is now the truth and
/// the key is a duplicate kept for one release so that no reader breaks on the
/// same day the fold starts filling it in:
///
/// - `ebcdic.rows` duplicates `TableData.num_rows` less the header row.
/// - `ebcdic.record_length` duplicates `record_layout.record_length`, and
///   `ebcdic.rows_truncated` duplicates `record_layout.rows_truncated`. The
///   typed count is always set and reads zero for a complete table; the custom
///   field is written only when rows were dropped, which is what it has always
///   done.
/// - on the body, `ebcdic.encoding`, `ebcdic.header_size`,
///   `ebcdic.footer_size` and `ebcdic.prefix_size` duplicate
///   `record_layout.encoding`, `.header_bytes`, `.footer_bytes` and
///   `.prefix_bytes` on every table.
/// - the per-column facts these keys used to stand in for - the field type,
///   the picture, the byte offset and width, the level, the path - are now
///   `TableData.columns`, and the per-row byte offsets are
///   `TableData.row_prov`.
///
/// `ebcdic.schema` and `ebcdic.selector` have no first-class home in the
/// extended schema, so they stay custom fields rather than being dropped.
fn table_custom_fields(state: &TableState) -> HashMap<String, Value> {
    let mut custom_fields = HashMap::new();
    custom_fields.insert("ebcdic.schema".to_string(), string_value(&state.name));
    custom_fields.insert(
        "ebcdic.record_length".to_string(),
        number_value(u64::from(state.record_length)),
    );
    if let Some(selector) = state.selector.as_ref() {
        custom_fields.insert("ebcdic.selector".to_string(), string_value(selector));
    }
    custom_fields.insert("ebcdic.rows".to_string(), number_value(state.rows));
    if state.dropped > 0 {
        custom_fields.insert(
            "ebcdic.rows_truncated".to_string(),
            number_value(state.dropped),
        );
    }
    custom_fields
}

/// One column of a folded table, from the field schema behind it.
///
/// The header cell keeps the flat field name, which is what a reader sees and
/// what a row's cells are placed by. The column declaration carries the name
/// the copybook actually gives the field: the dotted qualification path with
/// its subscript, `CUSTOMER-RECORD.TOTALS.MONTH(2)` rather than `MONTH(2)`.
/// The flat layout forms have no groups, so for them the two are the same
/// string.
///
/// `byte_offset` is measured from the start of the record body, as
/// `FieldSchema.offset` is, so the two never disagree. The body starts
/// `ebcdic.prefix_size` bytes into the record extent that `row_prov` reports.
fn column(field: &pb::FieldSchema) -> Column {
    let path = if field.path.is_empty() {
        field.name.clone()
    } else {
        field.path.clone()
    };
    Column {
        name: field.name.clone(),
        schema: doc::TableColumnSchema {
            name: Some(path),
            declared_type: field_type_name(field.r#type).map(str::to_string),
            picture: if field.picture.is_empty() {
                None
            } else {
                Some(field.picture.clone())
            },
            byte_offset: Some(u64::from(field.offset)),
            byte_size: Some(u64::from(field.size)),
            // Only a copybook has levels; the flat forms leave this unset
            // rather than claim an 01.
            level: field.level.and_then(|level| i32::try_from(level).ok()),
            // The occurrence as a number, beside the COBOL subscript that
            // `name` already carries in the path. Unset for a field that
            // occurs once, which is not the same claim as occurrence one.
            occurs_index: field
                .occurs_index
                .and_then(|index| i32::try_from(index).ok()),
            // No display width: a fixed-record field has a width in bytes,
            // which is `byte_size`, and no width on any page.
            width: None,
            conditions: field.conditions.iter().map(condition).collect(),
            ..Default::default()
        },
    }
}

/// One level-88 condition name as the document schema declares it.
///
/// The typed `ranges` of the collector's own event are the source, not the
/// flat `values` beside them: a bare literal is a `ValueRange` with only
/// `low`, a `VALUE low THRU high` keeps both bounds apart, and a condition
/// with several literals is several ranges under the one name. The bounds stay
/// the copybook's own literals, because a condition on a `PIC X` field has no
/// numeric reading and one on a `PIC 9` field is still written the way the
/// copybook wrote it.
fn condition(declared: &pb::ConditionName) -> doc::ValueCondition {
    doc::ValueCondition {
        name: declared.name.clone(),
        values: declared
            .ranges
            .iter()
            .map(|range| doc::ValueRange {
                low: range.low.clone(),
                high: range.high.clone(),
            })
            .collect(),
    }
}

/// The name of a field type, in the vocabulary the layout forms use.
///
/// The same spelling the JSON layout form accepts, so a consumer reading
/// `declared_type` off a column can write the layout that produced it. `None`
/// for a type this build has no name for, which the server never emits.
fn field_type_name(field_type: i32) -> Option<&'static str> {
    match pb::FieldType::try_from(field_type) {
        Ok(pb::FieldType::String) => Some("string"),
        Ok(pb::FieldType::Integer) => Some("integer"),
        Ok(pb::FieldType::UnsignedInteger) => Some("unsigned_integer"),
        Ok(pb::FieldType::PackedDecimal) => Some("packed_decimal"),
        Ok(pb::FieldType::ZonedDecimal) => Some("zoned_decimal"),
        Ok(pb::FieldType::Skip) => Some("skip"),
        Ok(pb::FieldType::Unspecified) | Err(_) => None,
    }
}

/// The typed value of one decoded cell, when it has one a `CellValue` can
/// hold.
///
/// Text cells get none: `CellValue` has no text arm, and `TableCell.text` is
/// already the whole of a character field. A numeric cell gets the nearest
/// double, which is what the schema's `number` arm is; the exact value stays
/// in `text`, because a scaled decimal is not generally representable in
/// binary and a `PIC S9(18)V99 COMP-3` has more digits than a double has.
/// The scale and the picture are not repeated on every cell: they belong to
/// the column and are declared once, in `TableData.columns`.
#[allow(clippy::cast_precision_loss)]
fn cell_value(cell: &pb::Cell) -> Option<doc::CellValue> {
    let number = match cell.value.as_ref()? {
        pb::cell::Value::Text(_) => return None,
        // Parsed from the canonical text rather than rebuilt from `unscaled`,
        // so the rounding happens once and against the value the reader sees.
        pb::cell::Value::Decimal(decimal) => decimal.text.parse::<f64>().ok()?,
        pb::cell::Value::Integer(integer) => *integer as f64,
    };
    Some(doc::CellValue {
        kind: Some(doc::cell_value::Kind::Number(number)),
        number_format: None,
    })
}

/// Render one decoded cell as table text.
///
/// A `Decimal` is carried across as its own canonical rendering, character for
/// character. Re-deriving it from `unscaled` and `scale` would drop a trailing
/// zero, and a float would drop rather more than that: a `PIC S9(18)V99 COMP-3`
/// has twenty significant digits and an IEEE double has fifteen.
fn cell_text(cell: &pb::Cell) -> String {
    match cell.value.as_ref() {
        Some(pb::cell::Value::Text(text)) => text.clone(),
        Some(pb::cell::Value::Decimal(decimal)) => decimal.text.clone(),
        Some(pb::cell::Value::Integer(integer)) => integer.to_string(),
        None => String::new(),
    }
}

/// One table cell at `(row, column)`. Spans are always one: a record layout is
/// a rectangle by construction.
fn table_cell(
    text: String,
    value: Option<doc::CellValue>,
    row: u64,
    column: usize,
    column_header: bool,
) -> doc::TableCell {
    let row = clamp(row);
    let column = clamp(column as u64);
    doc::TableCell {
        // No bbox: nothing here was ever on a page.
        bbox: None,
        row_span: 1,
        col_span: 1,
        start_row_offset_idx: row,
        end_row_offset_idx: row.saturating_add(1),
        start_col_offset_idx: column,
        end_col_offset_idx: column.saturating_add(1),
        text,
        column_header,
        row_header: false,
        row_section: false,
        fillable: false,
        r#ref: None,
        // The typed value alongside the rendering, for the consumer that has
        // to compute rather than display.
        value,
        // No spans: a decoded field is one run of plain text, with no
        // emphasis and no links to mark up.
        spans: Vec::new(),
        // No declared alignment: a fixed-record field states none.
        align: None,
        valign: None,
    }
}

/// A JSON-Pointer reference.
fn ref_item(reference: &str) -> doc::RefItem {
    doc::RefItem {
        r#ref: reference.to_string(),
    }
}

/// The lowercase name of a layout form, or `None` when the server did not say.
fn layout_source_name(source: i32) -> Option<&'static str> {
    match pb::LayoutSource::try_from(source) {
        Ok(pb::LayoutSource::Proto) => Some("proto"),
        Ok(pb::LayoutSource::Json) => Some("json"),
        Ok(pb::LayoutSource::Copybook) => Some("copybook"),
        Ok(pb::LayoutSource::Unspecified) | Err(_) => None,
    }
}

/// A `google.protobuf.Value` holding a string.
fn string_value(text: &str) -> Value {
    Value {
        kind: Some(Kind::StringValue(text.to_string())),
    }
}

/// A `google.protobuf.Value` holding a count.
///
/// `Value` is a double, which holds every integer below 2^53 exactly; every
/// count written here is a byte width or a row count under the fold cap.
#[allow(clippy::cast_precision_loss)]
fn number_value(number: u64) -> Value {
    Value {
        kind: Some(Kind::NumberValue(number as f64)),
    }
}

/// Narrow a count to the `int32` the document schema uses for table indices.
fn clamp(value: u64) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

/// The self ref, children, and parent of one text item, whatever variant it is.
///
/// `CodeItem` is the odd one out: it inlines the base fields instead of
/// wrapping `TextItemBase`, because in the upstream model it inherits two
/// different `meta` types and only one can be on the wire.
fn text_links(item: &doc::BaseTextItem) -> Option<(&str, &[doc::RefItem], Option<&doc::RefItem>)> {
    use doc::base_text_item::Item;
    let (self_ref, children, parent) = match item.item.as_ref()? {
        Item::Title(value) => links(value.base.as_ref()?),
        Item::SectionHeader(value) => links(value.base.as_ref()?),
        Item::ListItem(value) => links(value.base.as_ref()?),
        Item::Formula(value) => links(value.base.as_ref()?),
        Item::Text(value) => links(value.base.as_ref()?),
        Item::FieldHeading(value) => links(value.base.as_ref()?),
        Item::FieldValue(value) => links(value.base.as_ref()?),
        Item::Code(value) => (
            value.self_ref.as_str(),
            value.children.as_slice(),
            value.parent.as_ref(),
        ),
    };
    Some((self_ref, children, parent))
}

/// The three link fields of a wrapped text item.
fn links(base: &doc::TextItemBase) -> (&str, &[doc::RefItem], Option<&doc::RefItem>) {
    (
        base.self_ref.as_str(),
        base.children.as_slice(),
        base.parent.as_ref(),
    )
}

/// Everything gathered by one walk of a document's link structure.
#[derive(Default)]
struct Links {
    /// Problems found so far.
    errors: Vec<String>,
    /// Every self ref seen, plus the two roots.
    refs: BTreeSet<String>,
    /// `(item, declared parent)` pairs.
    parents: Vec<(String, String)>,
    /// Children listed by each container.
    children: BTreeMap<String, BTreeSet<String>>,
}

impl Links {
    /// Record one item's own ref, its children, and its parent pointer.
    fn collect(
        &mut self,
        self_ref: &str,
        children: &[doc::RefItem],
        parent: Option<&doc::RefItem>,
    ) {
        if self_ref.is_empty() {
            self.errors.push("item with empty self_ref".to_string());
            return;
        }
        if !self.refs.insert(self_ref.to_string()) {
            self.errors.push(format!("duplicate self_ref {self_ref}"));
        }
        for child in children {
            self.children
                .entry(self_ref.to_string())
                .or_default()
                .insert(child.r#ref.clone());
        }
        if let Some(parent) = parent {
            self.parents
                .push((self_ref.to_string(), parent.r#ref.clone()));
        }
    }
}

/// Check that a document fragment is safe for the coordinator's additive merge.
///
/// Ported from `docling_integrity_errors` in the gRParse mapper: every
/// `self_ref` present and unique, every child reference resolving, every parent
/// reference resolving, and every parent listing the child that claims it. An
/// empty result is the only acceptable one, which is why every fold test ends
/// with this call.
#[must_use]
pub fn integrity_errors(document: &doc::Document) -> Vec<String> {
    let mut links = Links {
        refs: [BODY_REF.to_string(), FURNITURE_REF.to_string()]
            .into_iter()
            .collect(),
        ..Default::default()
    };

    for (root, group) in [
        (BODY_REF, &document.body),
        (FURNITURE_REF, &document.furniture),
    ] {
        let Some(group) = group.as_ref() else {
            links.errors.push(format!("{root} is missing"));
            continue;
        };
        if group.self_ref != root {
            links
                .errors
                .push(format!("{root} carries self_ref {}", group.self_ref));
        }
        for child in &group.children {
            links
                .children
                .entry(root.to_string())
                .or_default()
                .insert(child.r#ref.clone());
        }
    }

    for group in &document.groups {
        links.collect(&group.self_ref, &group.children, group.parent.as_ref());
    }
    for item in &document.texts {
        match text_links(item) {
            Some((self_ref, children, parent)) => links.collect(self_ref, children, parent),
            None => links
                .errors
                .push("text item with unset variant".to_string()),
        }
    }
    for picture in &document.pictures {
        links.collect(
            &picture.self_ref,
            &picture.children,
            picture.parent.as_ref(),
        );
    }
    for table in &document.tables {
        links.collect(&table.self_ref, &table.children, table.parent.as_ref());
    }

    for (parent, children) in &links.children {
        for child in children {
            if !links.refs.contains(child) {
                links
                    .errors
                    .push(format!("child {child} of {parent} does not resolve"));
            }
        }
    }
    for (item, parent) in &links.parents {
        if !links.refs.contains(parent) {
            links
                .errors
                .push(format!("parent {parent} of {item} does not resolve"));
            continue;
        }
        if !links
            .children
            .get(parent)
            .is_some_and(|listed| listed.contains(item))
        {
            links
                .errors
                .push(format!("parent {parent} does not list {item} as a child"));
        }
    }
    links.errors
}

#[cfg(test)]
mod tests {
    use super::{COLLECTOR, DocumentFold, SCHEMA_NAME, integrity_errors};
    use crate::codec::Codec;
    use crate::layout;
    use crate::proto::document::v1 as doc;
    use crate::proto::v1 as pb;
    use crate::stream::{DecodeOptions, RecordStream};

    /// This build's version, which every item is stamped with.
    const VERSION: &str = "9.9.9-test";

    /// A two-schema layout: a customer with text and a scaled packed decimal
    /// behind a filler, and an order with one zoned integer.
    fn two_schema_layout() -> pb::EbcdicLayout {
        pb::EbcdicLayout {
            description: "ACCOUNTS EXTRACT".into(),
            record_type_field: Some(pb::EbcdicField {
                name: "RECTYPE".into(),
                size: 1,
                r#type: pb::FieldType::String as i32,
                ..Default::default()
            }),
            records: vec![
                pb::EbcdicRecordLayout {
                    name: "CUSTOMER".into(),
                    selector: Some("C".into()),
                    fields: vec![
                        pb::EbcdicField {
                            name: "NAME".into(),
                            size: 4,
                            r#type: pb::FieldType::String as i32,
                            ..Default::default()
                        },
                        pb::EbcdicField {
                            name: "BALANCE".into(),
                            size: 3,
                            r#type: pb::FieldType::PackedDecimal as i32,
                            scale: 2,
                            ..Default::default()
                        },
                        pb::EbcdicField {
                            name: String::new(),
                            size: 2,
                            r#type: pb::FieldType::Skip as i32,
                            ..Default::default()
                        },
                    ],
                },
                pb::EbcdicRecordLayout {
                    name: "ORDER".into(),
                    selector: Some("O".into()),
                    fields: vec![pb::EbcdicField {
                        name: "QTY".into(),
                        size: 3,
                        r#type: pb::FieldType::ZonedDecimal as i32,
                        ..Default::default()
                    }],
                },
            ],
            ..Default::default()
        }
    }

    /// Wrap a layout in the options message.
    fn options(layout: pb::EbcdicLayout) -> pb::ParseOptions {
        pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::Layout(layout)),
            ..Default::default()
        }
    }

    /// Decoder settings for these tests: cp037, strip controls, no caps.
    fn decode_options() -> DecodeOptions {
        DecodeOptions {
            codec: Codec::resolve("cp037").unwrap(),
            strip_control_characters: true,
            max_records: 0,
            abort_on_error: false,
        }
    }

    /// A customer record: `C`, a four-character name, a packed balance, filler.
    fn customer(name: &str, packed: [u8; 3]) -> Vec<u8> {
        let codec = Codec::resolve("cp037").unwrap();
        let mut bytes = codec.encode("C").unwrap();
        bytes.extend(codec.encode(name).unwrap());
        bytes.extend_from_slice(&packed);
        bytes.extend_from_slice(&[0x40, 0x40]);
        bytes
    }

    /// An order record: `O` and three zoned digits.
    fn order(quantity: &str) -> Vec<u8> {
        Codec::resolve("cp037")
            .unwrap()
            .encode(&format!("O{quantity}"))
            .unwrap()
    }

    /// Run a real walk over `data` and fold every event it produces, exactly
    /// as the service does.
    fn fold(options: &pb::ParseOptions, data: &[u8], fold: DocumentFold) -> doc::Document {
        let mut fold = fold;
        let layout = layout::resolve(options).expect("the layout resolves");
        let info = layout.to_layout_info("cp037");
        fold.consume(&pb::parse_ebcdic_response::Event::LayoutInfo(info));
        let mut walk = RecordStream::new(layout, decode_options());
        walk.push(data);
        while let Some(row) = walk.next_record().expect("no decode failure") {
            fold.consume(&pb::parse_ebcdic_response::Event::Record(row));
        }
        walk.finish_input();
        let mut status = walk.status().expect("the walk ends cleanly");
        status.warnings.extend(fold.truncation_warnings());
        fold.consume(&pb::parse_ebcdic_response::Event::Status(status));
        let document = fold.take();
        assert!(
            integrity_errors(&document).is_empty(),
            "{:?}",
            integrity_errors(&document)
        );
        document
    }

    /// The text of every cell of one grid row.
    fn row_text(table: &doc::TableItem, row: usize) -> Vec<String> {
        table.data.as_ref().unwrap().grid[row]
            .cells
            .iter()
            .map(|cell| cell.text.clone())
            .collect()
    }

    /// The column declarations of a table.
    fn columns(table: &doc::TableItem) -> &[doc::TableColumnSchema] {
        &table.data.as_ref().unwrap().columns
    }

    /// The fixed-record layout a table declares it was decoded with.
    fn record_layout(table: &doc::TableItem) -> &doc::RecordLayoutMeta {
        table
            .data
            .as_ref()
            .unwrap()
            .record_layout
            .as_ref()
            .expect("every table this fold writes declares its record layout")
    }

    /// The byte extent one grid row reports, or `None` when it reports none.
    fn row_span(table: &doc::TableItem, row: usize) -> Option<(u64, u64)> {
        table.data.as_ref().unwrap().row_prov[row]
            .byte_range
            .as_ref()
            .map(|span| (span.start, span.end))
    }

    /// The typed value of one grid cell, which for this collector is a number
    /// or nothing at all.
    fn cell_number(table: &doc::TableItem, row: usize, column: usize) -> Option<f64> {
        let value = table.data.as_ref().unwrap().grid[row].cells[column]
            .value
            .as_ref()?;
        match value.kind.as_ref().expect("a value with no arm set") {
            doc::cell_value::Kind::Number(number) => Some(*number),
            other => panic!("an EBCDIC cell is a number or nothing, not {other:?}"),
        }
    }

    /// The base of a text item, whichever variant it is.
    fn text_base(item: &doc::BaseTextItem) -> &doc::TextItemBase {
        match item.item.as_ref().expect("the variant is set") {
            doc::base_text_item::Item::Text(value) => value.base.as_ref(),
            doc::base_text_item::Item::SectionHeader(value) => value.base.as_ref(),
            other => panic!("this fold only writes text and section headers, not {other:?}"),
        }
        .expect("the base is set")
    }

    /// The `DocItemLabel` of a text item.
    fn text_label(item: &doc::BaseTextItem) -> i32 {
        text_base(item).label
    }

    /// The text of a text item, which is also its `orig`.
    fn text_of(item: &doc::BaseTextItem) -> String {
        let base = text_base(item);
        assert_eq!(base.orig, base.text, "orig and text say the same thing");
        base.text.clone()
    }

    /// The collector attribution of one item's source list.
    fn collector(source: &[doc::SourceType]) -> doc::CollectorSource {
        assert_eq!(source.len(), 1, "exactly one attribution per item");
        let Some(doc::source_type::Source::Collector(collector)) = source[0].source.as_ref() else {
            panic!("the source is a collector attribution");
        };
        collector.clone()
    }

    /// A custom field, as a string.
    fn custom_string(
        fields: &std::collections::HashMap<String, prost_types::Value>,
        key: &str,
    ) -> String {
        match fields.get(key).and_then(|value| value.kind.as_ref()) {
            Some(prost_types::value::Kind::StringValue(text)) => text.clone(),
            other => panic!("{key} is {other:?}, not a string"),
        }
    }

    /// A custom field, as a number.
    fn custom_number(
        fields: &std::collections::HashMap<String, prost_types::Value>,
        key: &str,
    ) -> f64 {
        match fields.get(key).and_then(|value| value.kind.as_ref()) {
            Some(prost_types::value::Kind::NumberValue(number)) => *number,
            other => panic!("{key} is {other:?}, not a number"),
        }
    }

    #[test]
    fn a_multi_schema_layout_is_a_flat_run_of_headings_and_their_tables() {
        let mut data = customer("JANE", [0x12, 0x34, 0x5d]);
        data.extend(order("042"));
        data.extend(customer("BOB!", [0x00, 0x10, 0x0c]));
        let document = fold(
            &options(two_schema_layout()),
            &data,
            DocumentFold::new(VERSION),
        );

        assert_eq!(document.schema_name.as_deref(), Some(SCHEMA_NAME));
        // The layout's description names the document and opens it.
        assert_eq!(document.name, "ACCOUNTS EXTRACT");
        assert!(document.origin.is_none(), "the stream carries no filename");
        assert!(document.pictures.is_empty());
        assert!(
            document.field_regions.is_empty() && document.field_items.is_empty(),
            "the coordinator's merge drops these silently"
        );
        assert!(document.pages.is_empty(), "an EBCDIC file has no pages");

        assert!(
            document.groups.is_empty(),
            "docling's EBCDIC backend uses no groups and neither does this"
        );
        assert_eq!(document.tables.len(), 2);
        // Description, then heading and table per schema, all siblings on the
        // body, in layout order.
        let body = document.body.as_ref().unwrap();
        assert_eq!(
            body.children
                .iter()
                .map(|child| child.r#ref.as_str())
                .collect::<Vec<_>>(),
            vec![
                "#/texts/0",
                "#/texts/1",
                "#/tables/0",
                "#/texts/2",
                "#/tables/1"
            ]
        );
        assert_eq!(
            document
                .texts
                .iter()
                .map(|item| (text_label(item), text_of(item)))
                .collect::<Vec<_>>(),
            vec![
                (
                    doc::DocItemLabel::Text as i32,
                    "ACCOUNTS EXTRACT".to_string()
                ),
                (
                    doc::DocItemLabel::SectionHeader as i32,
                    "CUSTOMER".to_string()
                ),
                (doc::DocItemLabel::SectionHeader as i32, "ORDER".to_string()),
            ]
        );
        for (index, name) in ["CUSTOMER", "ORDER"].iter().enumerate() {
            let table = &document.tables[index];
            assert_eq!(
                table.parent.as_ref().unwrap().r#ref,
                "#/body",
                "a table is the heading's sibling, not its child"
            );
            let fields = &table.meta.as_ref().unwrap().custom_fields;
            assert_eq!(custom_string(fields, "ebcdic.schema"), *name);
        }

        // The filler is not a column: it has no name and no cell.
        let customers = &document.tables[0];
        assert_eq!(row_text(customers, 0), vec!["NAME", "BALANCE"]);
        assert!(
            customers.data.as_ref().unwrap().grid[0]
                .cells
                .iter()
                .all(|cell| cell.column_header),
            "the first grid row is the header"
        );
        assert_eq!(row_text(customers, 1), vec!["JANE", "-123.45"]);
        assert_eq!(row_text(customers, 2), vec!["BOB!", "1.00"]);
        let orders = &document.tables[1];
        assert_eq!(row_text(orders, 0), vec!["QTY"]);
        assert_eq!(row_text(orders, 1), vec!["42"]);
    }

    #[test]
    fn a_decimal_keeps_the_exact_text_the_decoder_produced() {
        // 1.00 must not come back as 1, and -123.45 must keep its sign: the
        // fold copies `Decimal.text` and never re-renders it.
        let mut data = customer("ONE!", [0x00, 0x10, 0x0c]);
        data.extend(customer("NEG!", [0x12, 0x34, 0x5d]));
        data.extend(customer("ZERO", [0x00, 0x00, 0x0c]));
        let document = fold(
            &options(two_schema_layout()),
            &data,
            DocumentFold::new(VERSION),
        );
        let table = &document.tables[0];
        assert_eq!(
            (1..=3)
                .map(|row| row_text(table, row)[1].clone())
                .collect::<Vec<_>>(),
            vec!["1.00", "-123.45", "0.00"]
        );
    }

    #[test]
    fn the_columns_declare_what_the_grid_only_renders() {
        let document = fold(
            &options(two_schema_layout()),
            &customer("JANE", [0x12, 0x34, 0x5d]),
            DocumentFold::new(VERSION),
        );
        let table = &document.tables[0];
        let declared = columns(table);
        assert_eq!(
            declared.len(),
            usize::try_from(table.data.as_ref().unwrap().num_cols).unwrap(),
            "one declaration per grid column, or the alignment means nothing"
        );
        assert_eq!(
            declared
                .iter()
                .map(|column| (
                    column.name.as_deref().unwrap_or(""),
                    column.declared_type.as_deref(),
                    column.byte_offset,
                    column.byte_size,
                ))
                .collect::<Vec<_>>(),
            vec![
                ("NAME", Some("string"), Some(0), Some(4)),
                ("BALANCE", Some("packed_decimal"), Some(4), Some(3)),
            ]
        );
        // A protobuf layout is flat and carries no picture clauses, and the
        // fold declares neither rather than inventing them.
        assert!(
            declared
                .iter()
                .all(|column| column.level.is_none() && column.picture.is_none())
        );
        // The header row renders the same columns, in the same order.
        assert_eq!(
            declared
                .iter()
                .map(|column| column.name.clone().unwrap_or_default())
                .collect::<Vec<_>>(),
            row_text(table, 0)
        );
    }

    #[test]
    fn a_filler_is_the_gap_the_column_offsets_leave() {
        let options = pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::Copybook(
                "01 REC.\n05 A PIC X(2).\n05 FILLER PIC X(3).\n05 B PIC X(2).\n".into(),
            )),
            ..Default::default()
        };
        let document = fold(
            &options,
            &Codec::resolve("cp037").unwrap().encode("AAxxxBB").unwrap(),
            DocumentFold::new(VERSION),
        );
        let table = &document.tables[0];
        // The filler is not a column, and it did not have to be: the three
        // bytes between the end of A and the start of B are the whole of it.
        assert_eq!(
            columns(table)
                .iter()
                .map(|column| (
                    column.name.as_deref().unwrap_or(""),
                    column.byte_offset,
                    column.byte_size
                ))
                .collect::<Vec<_>>(),
            vec![("REC.A", Some(0), Some(2)), ("REC.B", Some(5), Some(2)),]
        );
        assert_eq!(row_text(table, 1), vec!["AA", "BB"]);
    }

    #[test]
    fn a_copybook_column_carries_its_path_its_level_and_its_picture() {
        let options = pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::Copybook(
                "01 REC.\n05 ADDRESS.\n10 STREET PIC X(4).\n05 MONTH PIC 9(2) OCCURS 2.\n".into(),
            )),
            ..Default::default()
        };
        let document = fold(
            &options,
            &Codec::resolve("cp037").unwrap().encode("MAIN0102").unwrap(),
            DocumentFold::new(VERSION),
        );
        let table = &document.tables[0];
        // The header keeps the flat name a reader sees; the declaration keeps
        // the name the copybook gives the field, subscript and all, so an
        // occurrence is no longer a sibling column with an index buried in a
        // string.
        assert_eq!(row_text(table, 0), vec!["STREET", "MONTH(1)", "MONTH(2)"]);
        assert_eq!(
            columns(table)
                .iter()
                .map(|column| (
                    column.name.as_deref().unwrap_or(""),
                    column.level,
                    column.picture.as_deref(),
                    column.declared_type.as_deref(),
                ))
                .collect::<Vec<_>>(),
            vec![
                ("REC.ADDRESS.STREET", Some(10), Some("X(4)"), Some("string")),
                ("REC.MONTH(1)", Some(5), Some("9(2)"), Some("zoned_decimal")),
                ("REC.MONTH(2)", Some(5), Some("9(2)"), Some("zoned_decimal")),
            ]
        );
        assert_eq!(row_text(table, 1), vec!["MAIN", "1", "2"]);
    }

    #[test]
    fn a_numeric_cell_carries_a_number_and_a_text_cell_carries_none() {
        let mut data = customer("JANE", [0x12, 0x34, 0x5d]);
        data.extend(order("042"));
        let document = fold(
            &options(two_schema_layout()),
            &data,
            DocumentFold::new(VERSION),
        );

        let customers = &document.tables[0];
        // `CellValue` has no text arm and needs none: a character field is
        // already the whole of `TableCell.text`.
        assert_eq!(cell_number(customers, 1, 0), None);
        assert_eq!(row_text(customers, 1)[0], "JANE");
        // The scaled packed decimal is a number a consumer can compute with,
        // and the exact rendering is still there beside it.
        let balance = cell_number(customers, 1, 1).expect("a packed decimal is a number");
        assert!((balance - -123.45).abs() < 1e-9, "{balance}");
        assert_eq!(row_text(customers, 1)[1], "-123.45");
        // The scale lives on the column, once, rather than on every cell.
        assert_eq!(
            columns(customers)[1].declared_type.as_deref(),
            Some("packed_decimal")
        );

        let orders = &document.tables[1];
        let quantity = cell_number(orders, 1, 0).expect("a zoned integer is a number");
        assert!((quantity - 42.0).abs() < f64::EPSILON, "{quantity}");
        // A header cell is a label, not a value.
        assert_eq!(cell_number(orders, 0, 0), None);
    }

    #[test]
    fn every_row_says_where_its_record_was_and_the_header_says_nothing() {
        // A customer is ten bytes on the wire (a one-byte selector prefix, a
        // four-byte name, three packed bytes, two filler) and an order is four.
        let mut data = customer("JANE", [0x12, 0x34, 0x5d]);
        data.extend(order("042"));
        data.extend(customer("BOB!", [0x00, 0x10, 0x0c]));
        let document = fold(
            &options(two_schema_layout()),
            &data,
            DocumentFold::new(VERSION),
        );

        for table in &document.tables {
            let data = table.data.as_ref().unwrap();
            assert_eq!(
                data.row_prov.len(),
                data.grid.len(),
                "row provenance is index-aligned with the grid or it is useless"
            );
            assert_eq!(
                row_span(table, 0),
                None,
                "the header row was assembled from the copybook, not read from the input"
            );
        }
        let customers = &document.tables[0];
        assert_eq!(row_span(customers, 1), Some((0, 10)));
        assert_eq!(row_span(customers, 2), Some((14, 24)));
        // The interleaved order record sits between the two customers, which
        // is why a table spans no single range and its rows each carry one.
        assert_eq!(row_span(&document.tables[1], 1), Some((10, 14)));
        assert!(
            document.tables.iter().all(|table| table.prov.is_empty()),
            "a table of interleaved records has no one location"
        );
    }

    #[test]
    fn cells_are_indexed_in_both_arenas_and_the_two_agree() {
        let mut data = customer("JANE", [0x12, 0x34, 0x5d]);
        data.extend(customer("BOB!", [0x00, 0x10, 0x0c]));
        let document = fold(
            &options(two_schema_layout()),
            &data,
            DocumentFold::new(VERSION),
        );
        let data_ = document.tables[0].data.as_ref().unwrap();
        assert_eq!(data_.num_rows, 3, "two rows plus the header");
        assert_eq!(data_.num_cols, 2);
        let flat: Vec<_> = data_
            .grid
            .iter()
            .flat_map(|row| row.cells.iter().cloned())
            .collect();
        assert_eq!(flat, data_.table_cells, "the flat arena mirrors the grid");
        for (row, grid_row) in data_.grid.iter().enumerate() {
            for (column, cell) in grid_row.cells.iter().enumerate() {
                assert_eq!(cell.row_span, 1);
                assert_eq!(cell.col_span, 1);
                assert_eq!(
                    (
                        cell.start_row_offset_idx,
                        cell.end_row_offset_idx,
                        cell.start_col_offset_idx,
                        cell.end_col_offset_idx
                    ),
                    (
                        i32::try_from(row).unwrap(),
                        i32::try_from(row).unwrap() + 1,
                        i32::try_from(column).unwrap(),
                        i32::try_from(column).unwrap() + 1
                    )
                );
                assert!(cell.bbox.is_none(), "nothing here was ever on a page");
            }
        }
        assert!(
            document.tables.iter().all(|table| table.prov.is_empty()),
            "no prov: an EBCDIC record has a byte offset, not a page"
        );
    }

    #[test]
    fn every_item_is_stamped_with_the_collector_the_dialect_and_the_version() {
        let mut data = customer("JANE", [0x12, 0x34, 0x5d]);
        data.extend(order("042"));
        let document = fold(
            &options(two_schema_layout()),
            &data,
            DocumentFold::new(VERSION),
        );
        // The description, both headings, and both tables: every item this
        // fold creates is attributed, not just the ones carrying rows.
        let sources: Vec<_> = document
            .texts
            .iter()
            .map(|item| collector(&text_base(item).source))
            .chain(document.tables.iter().map(|table| collector(&table.source)))
            .collect();
        assert_eq!(sources.len(), 5);
        for collector in sources {
            assert_eq!(collector.collector, COLLECTOR);
            assert_eq!(collector.model.as_deref(), Some("proto"));
            assert_eq!(collector.version.as_deref(), Some(VERSION));
            assert!(
                collector.confidence.is_none(),
                "a declarative mapping has no confidence to report"
            );
        }
    }

    #[test]
    fn the_layout_form_is_the_model_and_a_copybook_says_so() {
        let options = pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::Copybook(
                "01 R.\n05 CODE PIC X(2).\n".into(),
            )),
            ..Default::default()
        };
        let document = fold(
            &options,
            &Codec::resolve("cp037").unwrap().encode("AB").unwrap(),
            DocumentFold::new(VERSION),
        );
        // No description in a copybook, so the schema name names the document.
        assert_eq!(document.name, "R");
        assert_eq!(
            collector(&document.tables[0].source).model.as_deref(),
            Some("copybook")
        );
        let body = document.body.as_ref().unwrap();
        let fields = &body.meta.as_ref().unwrap().custom_fields;
        assert_eq!(custom_string(fields, "ebcdic.layout_source"), "copybook");
    }

    #[test]
    fn one_schema_gets_no_heading_and_the_table_still_names_it() {
        let options = pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::Copybook(
                "01 R.\n05 CODE PIC X(2).\n".into(),
            )),
            ..Default::default()
        };
        let document = fold(
            &options,
            &Codec::resolve("cp037").unwrap().encode("AB").unwrap(),
            DocumentFold::new(VERSION),
        );
        // Upstream emits a heading only when there is more than one schema,
        // and there is nothing else to write here: no description, no groups.
        assert!(document.texts.is_empty(), "{:?}", document.texts);
        assert!(document.groups.is_empty());
        assert_eq!(document.tables.len(), 1);
        assert_eq!(
            document
                .body
                .as_ref()
                .unwrap()
                .children
                .iter()
                .map(|child| child.r#ref.as_str())
                .collect::<Vec<_>>(),
            vec!["#/tables/0"]
        );
        // The name docling would have lost with the heading.
        let fields = &document.tables[0].meta.as_ref().unwrap().custom_fields;
        assert_eq!(custom_string(fields, "ebcdic.schema"), "R");
    }

    #[test]
    fn the_description_opens_the_document_as_a_plain_text_item() {
        let mut layout = two_schema_layout();
        layout.records.truncate(1);
        layout.record_type_field = None;
        layout.records[0].selector = None;
        let document = fold(
            &options(layout),
            &customer("JANE", [0x12, 0x34, 0x5d])[1..],
            DocumentFold::new(VERSION),
        );
        // One schema, so the only text is the description, and it comes first.
        assert_eq!(document.texts.len(), 1);
        assert_eq!(
            text_label(&document.texts[0]),
            doc::DocItemLabel::Text as i32
        );
        assert_eq!(text_of(&document.texts[0]), "ACCOUNTS EXTRACT");
        assert_eq!(
            document
                .body
                .as_ref()
                .unwrap()
                .children
                .iter()
                .map(|child| child.r#ref.as_str())
                .collect::<Vec<_>>(),
            vec!["#/texts/0", "#/tables/0"]
        );
    }

    #[test]
    fn a_schema_that_matched_no_record_is_left_out_of_the_document() {
        // Two schemas declared, only one of them in the input.
        let document = fold(
            &options(two_schema_layout()),
            &customer("JANE", [0x12, 0x34, 0x5d]),
            DocumentFold::new(VERSION),
        );
        assert_eq!(
            document.tables.len(),
            1,
            "an empty schema is not an empty table"
        );
        let fields = &document.tables[0].meta.as_ref().unwrap().custom_fields;
        assert_eq!(custom_string(fields, "ebcdic.schema"), "CUSTOMER");
        // The heading survives the pruning: the layout still declared two
        // schemas, so the one table that is here says which one it is.
        assert_eq!(
            document.texts.iter().map(text_of).collect::<Vec<_>>(),
            vec!["ACCOUNTS EXTRACT", "CUSTOMER"]
        );
        assert_eq!(
            document
                .body
                .as_ref()
                .unwrap()
                .children
                .iter()
                .map(|child| child.r#ref.as_str())
                .collect::<Vec<_>>(),
            vec!["#/texts/0", "#/texts/1", "#/tables/0"]
        );
    }

    #[test]
    fn the_body_and_the_tables_carry_the_layout_facts_as_custom_fields() {
        let mut layout = two_schema_layout();
        layout.header_size = 3;
        layout.footer_size = 5;
        let mut data = b"HDR".to_vec();
        data.extend(customer("JANE", [0x12, 0x34, 0x5d]));
        data.extend(b"TRAIL");
        let document = fold(&options(layout), &data, DocumentFold::new(VERSION));

        let body = document.body.as_ref().unwrap();
        let fields = &body.meta.as_ref().unwrap().custom_fields;
        assert_eq!(custom_string(fields, "ebcdic.encoding"), "cp037");
        assert_eq!(custom_string(fields, "ebcdic.layout_source"), "proto");
        assert!((custom_number(fields, "ebcdic.header_size") - 3.0).abs() < f64::EPSILON);
        assert!((custom_number(fields, "ebcdic.footer_size") - 5.0).abs() < f64::EPSILON);
        assert!((custom_number(fields, "ebcdic.prefix_size") - 1.0).abs() < f64::EPSILON);

        let fields = &document.tables[0].meta.as_ref().unwrap().custom_fields;
        assert_eq!(custom_string(fields, "ebcdic.schema"), "CUSTOMER");
        assert_eq!(custom_string(fields, "ebcdic.selector"), "C");
        assert!((custom_number(fields, "ebcdic.record_length") - 9.0).abs() < f64::EPSILON);
        assert!((custom_number(fields, "ebcdic.rows") - 1.0).abs() < f64::EPSILON);
        assert!(!fields.contains_key("ebcdic.rows_truncated"));
    }

    #[test]
    fn every_table_declares_the_record_layout_it_was_decoded_with() {
        let mut layout = two_schema_layout();
        layout.header_size = 3;
        layout.footer_size = 5;
        let mut data = b"HDR".to_vec();
        data.extend(customer("JANE", [0x12, 0x34, 0x5d]));
        data.extend(order("042"));
        data.extend(b"TRAIL");
        let document = fold(&options(layout), &data, DocumentFold::new(VERSION));

        // Numbers, not the decimal strings a custom field would have had to
        // carry them as, and the code page under its own name.
        let customers = record_layout(&document.tables[0]);
        assert_eq!(customers.encoding.as_deref(), Some("cp037"));
        assert_eq!(customers.record_length, Some(9));
        assert_eq!(customers.header_bytes, Some(3));
        assert_eq!(customers.footer_bytes, Some(5));
        assert_eq!(customers.prefix_bytes, Some(1));
        // Nothing was dropped, and zero says so: the field is set either way,
        // because an unset count is not a claim that a table is complete.
        assert_eq!(customers.rows_truncated, Some(0));

        // The trims are the layout's and every schema shares them; only the
        // record length is the schema's own.
        let orders = record_layout(&document.tables[1]);
        assert_eq!(orders.record_length, Some(3));
        assert_eq!(orders.encoding.as_deref(), Some("cp037"));
        assert_eq!(orders.header_bytes, Some(3));
        assert_eq!(orders.footer_bytes, Some(5));
        assert_eq!(orders.prefix_bytes, Some(1));
        assert_eq!(orders.rows_truncated, Some(0));
    }

    #[test]
    fn a_layout_with_no_trims_declares_them_as_zero_rather_than_leaving_them_out() {
        let document = fold(
            &options(two_schema_layout()),
            &customer("JANE", [0x12, 0x34, 0x5d]),
            DocumentFold::new(VERSION),
        );
        let declared = record_layout(&document.tables[0]);
        assert_eq!(declared.header_bytes, Some(0));
        assert_eq!(declared.footer_bytes, Some(0));
        // The record-type prefix is one byte even when nothing is trimmed at
        // the file boundaries, and it is what stands between the record extent
        // in `row_prov` and the byte offsets the columns declare.
        assert_eq!(declared.prefix_bytes, Some(1));
    }

    #[test]
    fn the_deprecated_layout_custom_fields_are_still_emitted_beside_the_typed_ones() {
        let mut layout = two_schema_layout();
        layout.header_size = 3;
        layout.footer_size = 5;
        let mut data = b"HDR".to_vec();
        data.extend(customer("JANE", [0x12, 0x34, 0x5d]));
        data.extend(b"TRAIL");
        let document = fold(&options(layout), &data, DocumentFold::new(VERSION));

        // The deprecation window: both halves are written this release, and
        // they agree. Delete either half and this fails, which is how the
        // window ends deliberately rather than by accident.
        let body = document.body.as_ref().unwrap();
        let fields = &body.meta.as_ref().unwrap().custom_fields;
        let table = &document.tables[0];
        let declared = record_layout(table);
        assert_eq!(
            custom_string(fields, "ebcdic.encoding"),
            declared.encoding.clone().unwrap()
        );
        assert!(
            (custom_number(fields, "ebcdic.header_size") - 3.0).abs() < f64::EPSILON
                && declared.header_bytes == Some(3)
        );
        assert!(
            (custom_number(fields, "ebcdic.footer_size") - 5.0).abs() < f64::EPSILON
                && declared.footer_bytes == Some(5)
        );
        assert!(
            (custom_number(fields, "ebcdic.prefix_size") - 1.0).abs() < f64::EPSILON
                && declared.prefix_bytes == Some(1)
        );

        let fields = &table.meta.as_ref().unwrap().custom_fields;
        assert!(
            (custom_number(fields, "ebcdic.record_length") - 9.0).abs() < f64::EPSILON
                && declared.record_length == Some(9)
        );
        // These two never had a typed home and are not deprecated with the
        // rest: a schema name and a record selector are this collector's, not
        // the document schema's.
        assert_eq!(custom_string(fields, "ebcdic.schema"), "CUSTOMER");
        assert_eq!(custom_string(fields, "ebcdic.selector"), "C");
    }

    #[test]
    fn a_column_declares_the_level_88_conditions_of_the_field_behind_it() {
        let options = pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::Copybook(
                "01 REC.\n\
                 05 STATUS-CODE PIC X.\n\
                 88 STATUS-OPEN VALUE 'O'.\n\
                 05 GRADE PIC 9.\n\
                 88 PASSING VALUE 1 THRU 3.\n\
                 05 REGION PIC X.\n\
                 88 DOMESTIC VALUES ARE 'N' 'S' 'E'.\n\
                 05 PLAIN PIC X.\n"
                    .into(),
            )),
            ..Default::default()
        };
        let document = fold(
            &options,
            &Codec::resolve("cp037").unwrap().encode("O2NX").unwrap(),
            DocumentFold::new(VERSION),
        );
        let declared = columns(&document.tables[0]);

        // One literal: a range with a low bound and no high one. `high` unset
        // is the difference between "equals O" and "anything from O upwards".
        assert_eq!(declared[0].conditions.len(), 1);
        assert_eq!(declared[0].conditions[0].name, "STATUS-OPEN");
        assert_eq!(declared[0].conditions[0].values.len(), 1);
        assert_eq!(declared[0].conditions[0].values[0].low, "O");
        assert_eq!(declared[0].conditions[0].values[0].high, None);

        // THRU: one range carrying both bounds, still spelled the way the
        // copybook spelled them even though the field is numeric.
        assert_eq!(declared[1].conditions[0].name, "PASSING");
        assert_eq!(declared[1].conditions[0].values.len(), 1);
        assert_eq!(declared[1].conditions[0].values[0].low, "1");
        assert_eq!(
            declared[1].conditions[0].values[0].high.as_deref(),
            Some("3")
        );

        // Several literals: several ranges under the one condition name, in
        // declaration order, rather than one joined string.
        assert_eq!(declared[2].conditions[0].name, "DOMESTIC");
        assert_eq!(
            declared[2]
                .conditions
                .iter()
                .flat_map(|condition| condition.values.iter())
                .map(|range| (range.low.as_str(), range.high.as_deref()))
                .collect::<Vec<_>>(),
            vec![("N", None), ("S", None), ("E", None)]
        );

        // A field with no level-88 under it declares no conditions, which is
        // not the same as declaring an empty one.
        assert!(declared[3].conditions.is_empty());
    }

    #[test]
    fn an_occurs_column_declares_its_occurrence_as_a_number() {
        let options = pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::Copybook(
                "01 REC.\n05 NAME PIC X(2).\n05 MONTH PIC 9(2) OCCURS 3.\n".into(),
            )),
            ..Default::default()
        };
        let document = fold(
            &options,
            &Codec::resolve("cp037").unwrap().encode("AB010203").unwrap(),
            DocumentFold::new(VERSION),
        );
        let declared = columns(&document.tables[0]);
        assert_eq!(
            declared
                .iter()
                .map(|column| (column.name.as_deref().unwrap_or(""), column.occurs_index))
                .collect::<Vec<_>>(),
            vec![
                // A field that occurs once is not occurrence one.
                ("REC.NAME", None),
                ("REC.MONTH(1)", Some(1)),
                ("REC.MONTH(2)", Some(2)),
                ("REC.MONTH(3)", Some(3)),
            ]
        );
        // A byte width is not a display width, and this collector has no page
        // to measure one on.
        assert!(declared.iter().all(|column| column.width.is_none()));
    }

    #[test]
    fn rows_past_the_cap_are_counted_warned_about_and_left_out() {
        let mut data = Vec::new();
        for name in ["AAAA", "BBBB", "CCCC", "DDDD"] {
            data.extend(customer(name, [0x00, 0x10, 0x0c]));
        }
        data.extend(order("007"));

        // The cap is a parameter so this test costs five records rather than a
        // hundred thousand and one.
        let layout = layout::resolve(&options(two_schema_layout())).unwrap();
        let mut fold = DocumentFold::new(VERSION).with_row_cap(2);
        fold.consume(&pb::parse_ebcdic_response::Event::LayoutInfo(
            layout.to_layout_info("cp037"),
        ));
        let mut walk = RecordStream::new(layout, decode_options());
        walk.push(&data);
        while let Some(row) = walk.next_record().unwrap() {
            fold.consume(&pb::parse_ebcdic_response::Event::Record(row));
        }
        let warnings = fold.truncation_warnings();
        let document = fold.take();
        assert!(integrity_errors(&document).is_empty());

        assert_eq!(
            warnings.len(),
            1,
            "only the capped schema warns: {warnings:?}"
        );
        assert_eq!(
            warnings[0].code,
            pb::WarningCode::DocumentRowsTruncated as i32
        );
        assert!(warnings[0].message.contains("CUSTOMER"), "{warnings:?}");
        assert!(warnings[0].message.contains('2'), "{warnings:?}");
        // The third customer is where the fold stopped folding.
        assert_eq!(warnings[0].byte_offset, 20);

        let table = &document.tables[0];
        assert_eq!(table.data.as_ref().unwrap().num_rows, 3, "header plus two");
        assert_eq!(row_text(table, 1)[0], "AAAA");
        assert_eq!(row_text(table, 2)[0], "BBBB");
        // A dropped row leaves no provenance behind either: what is in the
        // grid and what claims a location are the same rows.
        let data_ = table.data.as_ref().unwrap();
        assert_eq!(data_.row_prov.len(), data_.grid.len());
        assert_eq!(row_span(table, 1), Some((0, 10)));
        assert_eq!(row_span(table, 2), Some((10, 20)));
        // The typed count is the truth: two rows dropped, as a number.
        assert_eq!(record_layout(table).rows_truncated, Some(2));
        let fields = &table.meta.as_ref().unwrap().custom_fields;
        assert!((custom_number(fields, "ebcdic.rows") - 2.0).abs() < f64::EPSILON);
        // Still written beside it for one release, and still only when rows
        // were actually dropped, which is what this key has always meant.
        assert!((custom_number(fields, "ebcdic.rows_truncated") - 2.0).abs() < f64::EPSILON);
        // The other schema is under the cap. The typed count says so with a
        // zero; the custom field says so by being absent.
        assert_eq!(record_layout(&document.tables[1]).rows_truncated, Some(0));
        let fields = &document.tables[1].meta.as_ref().unwrap().custom_fields;
        assert!(!fields.contains_key("ebcdic.rows_truncated"));
        assert_eq!(row_text(&document.tables[1], 1), vec!["7"]);
    }

    #[test]
    fn the_integrity_check_catches_a_link_written_in_one_direction_only() {
        let mut document = fold(
            &options(two_schema_layout()),
            &customer("JANE", [0x12, 0x34, 0x5d]),
            DocumentFold::new(VERSION),
        );
        // A table that names a parent which does not list it back is exactly
        // what the coordinator's merge loses without saying so.
        document
            .body
            .as_mut()
            .unwrap()
            .children
            .retain(|child| child.r#ref != "#/tables/0");
        let errors = integrity_errors(&document);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("does not list"), "{errors:?}");

        let mut document = fold(
            &options(two_schema_layout()),
            &customer("JANE", [0x12, 0x34, 0x5d]),
            DocumentFold::new(VERSION),
        );
        // Two text items with the same self_ref: the description and the
        // heading are separate items and the merge has to be able to tell
        // them apart.
        let first = text_base(&document.texts[0]).self_ref.clone();
        match document.texts[1].item.as_mut().unwrap() {
            doc::base_text_item::Item::SectionHeader(heading) => {
                heading.base.as_mut().unwrap().self_ref = first;
            }
            other => panic!("the second text is the schema heading, not {other:?}"),
        }
        let errors = integrity_errors(&document);
        assert!(
            errors
                .iter()
                .any(|error| error.contains("duplicate self_ref")),
            "{errors:?}"
        );
    }
}
