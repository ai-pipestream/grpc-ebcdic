// SPDX-License-Identifier: Apache-2.0

//! The normalized layout: one internal shape that the protobuf message, the
//! Docling JSON, and the copybook compiler all reduce to before a single byte
//! of data is read.
//!
//! Validation happens here and only here, and it happens before the response
//! stream opens, so a layout that cannot describe the file fails with
//! `INVALID_ARGUMENT` instead of half a table followed by an error.

use std::collections::BTreeSet;

use serde::Deserialize;

use crate::decode::{MAX_BINARY_BYTES, MAX_PACKED_BYTES, MAX_ZONED_BYTES};
use crate::error::ParseError;
use crate::proto::v1 as pb;

/// Default schema name, matching Docling's `EbcdicRecordLayout.name` default.
const DEFAULT_RECORD_NAME: &str = "record";

/// Widest record body the server will assemble, in bytes.
///
/// A record is the unit that must be buffered whole before it can be decoded,
/// so this is the real memory bound of a parse. Sixteen mebibytes is far past
/// any copybook and small enough that a thousand concurrent streams still fit
/// in a container.
pub const MAX_RECORD_BYTES: u32 = 16 * 1024 * 1024;

/// Largest footer the server will hold back, in bytes.
///
/// Footer bytes cannot be decoded until the input ends, so they are buffered
/// for the whole parse. Same reasoning and same number as [`MAX_RECORD_BYTES`].
pub const MAX_FOOTER_BYTES: u32 = 16 * 1024 * 1024;

/// How the bytes of one field are decoded. The internal mirror of
/// [`pb::FieldType`], with the "unspecified" case already resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    /// `USAGE DISPLAY` character data.
    Text,
    /// Signed big-endian binary.
    Integer,
    /// Unsigned big-endian binary.
    UnsignedInteger,
    /// `COMP-3` packed decimal.
    PackedDecimal,
    /// Signed `USAGE DISPLAY` numeric.
    ZonedDecimal,
    /// Filler: bytes consumed, no cell emitted.
    Skip,
}

impl FieldKind {
    /// The protobuf value this kind reports as.
    #[must_use]
    pub const fn to_proto(self) -> pb::FieldType {
        match self {
            Self::Text => pb::FieldType::String,
            Self::Integer => pb::FieldType::Integer,
            Self::UnsignedInteger => pb::FieldType::UnsignedInteger,
            Self::PackedDecimal => pb::FieldType::PackedDecimal,
            Self::ZonedDecimal => pb::FieldType::ZonedDecimal,
            Self::Skip => pb::FieldType::Skip,
        }
    }

    /// Resolve a protobuf field type.
    ///
    /// An unset type is character data, which is Docling's default for a field
    /// that declares none.
    ///
    /// # Errors
    ///
    /// [`ParseError::Unsupported`] for a value this build does not know, which
    /// is how a newer client's field type surfaces.
    pub fn from_proto(value: i32) -> Result<Self, ParseError> {
        match pb::FieldType::try_from(value) {
            Ok(pb::FieldType::Unspecified | pb::FieldType::String) => Ok(Self::Text),
            Ok(pb::FieldType::Integer) => Ok(Self::Integer),
            Ok(pb::FieldType::UnsignedInteger) => Ok(Self::UnsignedInteger),
            Ok(pb::FieldType::PackedDecimal) => Ok(Self::PackedDecimal),
            Ok(pb::FieldType::ZonedDecimal) => Ok(Self::ZonedDecimal),
            Ok(pb::FieldType::Skip) => Ok(Self::Skip),
            Err(_) => Err(ParseError::unsupported(format!(
                "field type {value} is not one this build decodes"
            ))),
        }
    }

    /// Whether the kind produces a number rather than text.
    #[must_use]
    pub const fn is_numeric(self) -> bool {
        matches!(
            self,
            Self::Integer | Self::UnsignedInteger | Self::PackedDecimal | Self::ZonedDecimal
        )
    }
}

/// One COBOL level-88 condition name and the values that make it true.
///
/// A copybook has no other way to declare which values of a field are legal,
/// so this is the closest thing to an enumeration the layout can carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConditionName {
    /// The condition name as it was declared.
    pub name: String,
    /// Values of the parent field the condition is true for.
    pub values: Vec<String>,
}

impl ConditionName {
    /// The protobuf view of this condition name.
    #[must_use]
    pub fn to_proto(&self) -> pb::ConditionName {
        pb::ConditionName {
            name: self.name.clone(),
            values: self.values.clone(),
        }
    }
}

/// What a copybook says about a field beyond the bytes it occupies: where it
/// sits in the level tree, which occurrence of a repeating item it is, and the
/// condition names declared under it.
///
/// Empty for the protobuf and JSON layout forms, which describe a flat field
/// list with no hierarchy: those set only [`Declaration::path`], to the field
/// name, so a consumer never has to special-case the flat forms.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Declaration {
    /// COBOL level number of the item, when the source had levels.
    pub level: Option<u32>,
    /// Dotted qualification path, record name first.
    pub path: String,
    /// One-based occurrence index of an `OCCURS` expansion.
    pub occurs_index: Option<u32>,
    /// Unsubscripted name of the repeating item.
    pub occurs_group: Option<String>,
    /// Level-88 condition names declared under the field.
    pub conditions: Vec<ConditionName>,
}

/// One fixed-width field, with its offset already resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    /// Column name of the decoded field.
    pub name: String,
    /// Byte offset within the record body (or within the prefix).
    pub offset: u32,
    /// Field width in bytes.
    pub size: u32,
    /// How the bytes are decoded.
    pub kind: FieldKind,
    /// Implied decimal digits.
    pub scale: u32,
    /// Original COBOL picture clause, for diagnostics.
    pub picture: String,
    /// What the source declaration said beyond the bytes.
    pub declaration: Declaration,
}

impl Field {
    /// The protobuf schema view of this field.
    #[must_use]
    pub fn to_schema(&self) -> pb::FieldSchema {
        pb::FieldSchema {
            name: self.name.clone(),
            offset: self.offset,
            size: self.size,
            r#type: self.kind.to_proto() as i32,
            scale: self.scale,
            picture: self.picture.clone(),
            level: self.declaration.level,
            path: self.declaration.path.clone(),
            occurs_index: self.declaration.occurs_index,
            occurs_group: self.declaration.occurs_group.clone(),
            conditions: self
                .declaration
                .conditions
                .iter()
                .map(ConditionName::to_proto)
                .collect(),
        }
    }
}

/// One record schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordLayout {
    /// Name of the schema.
    pub name: String,
    /// Fields in physical order, fillers included.
    pub fields: Vec<Field>,
    /// Total body length in bytes.
    pub size: u32,
    /// Record-type prefix value that selects this schema.
    pub selector: Option<String>,
}

impl RecordLayout {
    /// The protobuf schema view of this record.
    #[must_use]
    pub fn to_schema(&self) -> pb::RecordSchema {
        pb::RecordSchema {
            name: self.name.clone(),
            record_length: self.size,
            fields: self.fields.iter().map(Field::to_schema).collect(),
            selector: self.selector.clone(),
        }
    }
}

/// A validated, fully resolved layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    /// Record schemas in layout order.
    pub records: Vec<RecordLayout>,
    /// Free-text description.
    pub description: String,
    /// Bytes skipped at the start of the input.
    pub header_size: u32,
    /// Bytes held back and skipped at the end of the input.
    pub footer_size: u32,
    /// Prefix field holding the total record length, prefix included.
    pub record_length_field: Option<Field>,
    /// Prefix field selecting the record schema.
    pub record_type_field: Option<Field>,
    /// Total prefix width, the sum of the prefix fields present.
    pub prefix_size: u32,
    /// Which request form the layout came from.
    pub source: pb::LayoutSource,
}

impl Layout {
    /// Resolve the schema a record-type value selects.
    ///
    /// With no record-type field there is exactly one schema and it matches
    /// everything, which is Docling's `EbcdicLayout.select` behaviour.
    #[must_use]
    pub fn select(&self, record_type: Option<&str>) -> Option<&RecordLayout> {
        match (self.record_type_field.as_ref(), record_type) {
            (None, _) => self.records.first(),
            (Some(_), value) => self
                .records
                .iter()
                .find(|record| record.selector.as_deref() == value),
        }
    }

    /// The `LayoutInfo` event this layout produces, sent before any input byte
    /// is read.
    #[must_use]
    pub fn to_layout_info(&self, encoding: &str) -> pb::LayoutInfo {
        pb::LayoutInfo {
            encoding: encoding.to_string(),
            records: self.records.iter().map(RecordLayout::to_schema).collect(),
            description: self.description.clone(),
            header_size: self.header_size,
            footer_size: self.footer_size,
            prefix_size: self.prefix_size,
            source: self.source as i32,
        }
    }
}

/// A field as it arrives, before offsets are resolved and bounds are checked.
///
/// Shared with [`crate::copybook`], which is the third producer of this shape
/// alongside the protobuf message and the Docling JSON.
#[derive(Debug)]
pub(crate) struct RawField {
    /// Column name.
    pub(crate) name: String,
    /// Declared width in bytes.
    pub(crate) size: u32,
    /// Declared type.
    pub(crate) kind: FieldKind,
    /// Declared scale.
    pub(crate) scale: u32,
    /// Picture clause, when the source carried one.
    pub(crate) picture: String,
    /// Explicit offset, when the source carried one.
    pub(crate) offset: Option<u32>,
    /// Levels, paths, occurrences and conditions, when the source had them.
    pub(crate) declaration: Declaration,
}

/// A record as it arrives.
#[derive(Debug)]
pub(crate) struct RawRecord {
    /// Schema name, empty for the default.
    pub(crate) name: String,
    /// Fields in physical order.
    pub(crate) fields: Vec<RawField>,
    /// Record-type selector.
    pub(crate) selector: Option<String>,
}

/// A whole layout as it arrives.
#[derive(Debug)]
pub(crate) struct RawLayout {
    /// Record schemas.
    pub(crate) records: Vec<RawRecord>,
    /// Free-text description.
    pub(crate) description: String,
    /// Leading bytes to skip.
    pub(crate) header_size: u32,
    /// Trailing bytes to skip.
    pub(crate) footer_size: u32,
    /// Record-length prefix field.
    pub(crate) record_length_field: Option<RawField>,
    /// Record-type prefix field.
    pub(crate) record_type_field: Option<RawField>,
}

/// Build the normalized layout from a `ParseOptions` message.
///
/// # Errors
///
/// [`ParseError::Invalid`] when no layout form is set, when the JSON does not
/// parse, or when the layout fails validation; [`ParseError::Unsupported`] for
/// a field type or copybook feature this build does not implement.
pub fn resolve(options: &pb::ParseOptions) -> Result<Layout, ParseError> {
    let Some(source) = options.layout_source.as_ref() else {
        return Err(ParseError::invalid(
            "an EBCDIC file is an opaque code page without its copybook: set exactly one of \
             ParseOptions.layout, ParseOptions.layout_json, or ParseOptions.copybook",
        ));
    };
    match source {
        pb::parse_options::LayoutSource::Layout(layout) => {
            validate(from_proto(layout), pb::LayoutSource::Proto)
        }
        pb::parse_options::LayoutSource::LayoutJson(bytes) => {
            validate(from_json(bytes)?, pb::LayoutSource::Json)
        }
        pb::parse_options::LayoutSource::Copybook(source) => validate(
            crate::copybook::compile(source)?,
            pb::LayoutSource::Copybook,
        ),
    }
}

/// Lift a protobuf layout into the raw shape.
fn from_proto(layout: &pb::EbcdicLayout) -> RawLayout {
    /// Lift one protobuf field.
    fn field(field: &pb::EbcdicField) -> RawField {
        RawField {
            name: field.name.clone(),
            size: field.size,
            // Unknown type values are caught in validation, where the field
            // name is available for the message.
            kind: FieldKind::from_proto(field.r#type).unwrap_or(FieldKind::Text),
            scale: field.scale,
            picture: field.picture.clone(),
            offset: field.offset,
            // A protobuf layout is a flat field list: no levels, no groups, no
            // OCCURS. Validation fills the path in from the name.
            declaration: Declaration::default(),
        }
    }
    RawLayout {
        records: layout
            .records
            .iter()
            .map(|record| RawRecord {
                name: record.name.clone(),
                fields: record.fields.iter().map(field).collect(),
                selector: record.selector.clone(),
            })
            .collect(),
        description: layout.description.clone(),
        header_size: layout.header_size,
        footer_size: layout.footer_size,
        record_length_field: layout.record_length_field.as_ref().map(field),
        record_type_field: layout.record_type_field.as_ref().map(field),
    }
}

/// Docling's `EbcdicFieldType`, as it appears in a serialized layout.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum JsonFieldType {
    /// `USAGE DISPLAY` character data. Docling's default.
    #[default]
    String,
    /// Signed binary.
    Integer,
    /// Unsigned binary.
    UnsignedInteger,
    /// `COMP-3`.
    PackedDecimal,
    /// Signed display numeric.
    ZonedDecimal,
    /// Filler.
    Skip,
}

impl From<JsonFieldType> for FieldKind {
    fn from(value: JsonFieldType) -> Self {
        match value {
            JsonFieldType::String => Self::Text,
            JsonFieldType::Integer => Self::Integer,
            JsonFieldType::UnsignedInteger => Self::UnsignedInteger,
            JsonFieldType::PackedDecimal => Self::PackedDecimal,
            JsonFieldType::ZonedDecimal => Self::ZonedDecimal,
            JsonFieldType::Skip => Self::Skip,
        }
    }
}

/// Docling's `EbcdicField`, as it appears in a serialized layout.
///
/// Unknown keys are ignored rather than rejected, matching pydantic's default,
/// so a layout written for a newer Docling still loads here.
#[derive(Debug, Deserialize)]
struct JsonField {
    /// Column name.
    name: String,
    /// Width in bytes.
    size: u32,
    /// Storage type; absent means character data.
    #[serde(default)]
    r#type: JsonFieldType,
    /// Implied decimal digits.
    #[serde(default)]
    scale: u32,
    /// Byte offset. Not part of Docling's model; accepted as the same optional
    /// cross-check the protobuf form offers.
    #[serde(default)]
    offset: Option<u32>,
    /// Picture clause. Not part of Docling's model; carried for diagnostics
    /// when a producer supplies one.
    #[serde(default)]
    picture: Option<String>,
}

impl From<JsonField> for RawField {
    fn from(field: JsonField) -> Self {
        Self {
            name: field.name,
            size: field.size,
            kind: field.r#type.into(),
            scale: field.scale,
            picture: field.picture.unwrap_or_default(),
            offset: field.offset,
            // Same flat shape as the protobuf form.
            declaration: Declaration::default(),
        }
    }
}

/// Docling's `EbcdicRecordLayout`.
#[derive(Debug, Deserialize)]
struct JsonRecord {
    /// Fields in physical record order.
    fields: Vec<JsonField>,
    /// Schema name.
    #[serde(default)]
    name: Option<String>,
    /// Record-type selector.
    #[serde(default)]
    selector: Option<String>,
}

/// Docling's `EbcdicLayout`.
#[derive(Debug, Deserialize)]
struct JsonLayout {
    /// Record schemas.
    records: Vec<JsonRecord>,
    /// Free-text description.
    #[serde(default)]
    description: String,
    /// Leading bytes to skip.
    #[serde(default)]
    header_size: u32,
    /// Trailing bytes to skip.
    #[serde(default)]
    footer_size: u32,
    /// Record-length prefix field.
    #[serde(default)]
    record_length_field: Option<JsonField>,
    /// Record-type prefix field.
    #[serde(default)]
    record_type_field: Option<JsonField>,
}

/// Parse Docling's JSON layout shape.
fn from_json(bytes: &[u8]) -> Result<RawLayout, ParseError> {
    let parsed: JsonLayout = serde_json::from_slice(bytes).map_err(|err| {
        ParseError::invalid(format!("layout_json is not a Docling EbcdicLayout: {err}"))
    })?;
    Ok(RawLayout {
        records: parsed
            .records
            .into_iter()
            .map(|record| RawRecord {
                name: record.name.unwrap_or_default(),
                fields: record.fields.into_iter().map(RawField::from).collect(),
                selector: record.selector,
            })
            .collect(),
        description: parsed.description,
        header_size: parsed.header_size,
        footer_size: parsed.footer_size,
        record_length_field: parsed.record_length_field.map(RawField::from),
        record_type_field: parsed.record_type_field.map(RawField::from),
    })
}

/// Check the widths a decoder can actually handle.
fn check_width(field: &RawField, context: &str) -> Result<(), ParseError> {
    let limit = match field.kind {
        FieldKind::PackedDecimal => MAX_PACKED_BYTES,
        FieldKind::ZonedDecimal => MAX_ZONED_BYTES,
        FieldKind::Integer | FieldKind::UnsignedInteger => MAX_BINARY_BYTES,
        FieldKind::Text | FieldKind::Skip => MAX_RECORD_BYTES,
    };
    if field.size > limit {
        return Err(ParseError::unsupported(format!(
            "{context} field {:?} is {} bytes; this build decodes at most {limit}",
            field.name, field.size
        )));
    }
    Ok(())
}

/// Validate a prefix field: it is read ahead of every record, so it has to be
/// something a length or a selector can be made of.
fn prefix_field(raw: RawField, role: &str) -> Result<Field, ParseError> {
    if raw.size == 0 {
        return Err(ParseError::invalid(format!(
            "the {role} field must be at least one byte"
        )));
    }
    if raw.kind == FieldKind::Skip {
        return Err(ParseError::invalid(format!(
            "the {role} field cannot be a filler: its value is what it is there for"
        )));
    }
    check_width(&raw, role)?;
    let name = if raw.name.is_empty() {
        role.to_string()
    } else {
        raw.name
    };
    Ok(Field {
        offset: 0,
        size: raw.size,
        kind: raw.kind,
        scale: raw.scale,
        picture: raw.picture,
        declaration: Declaration {
            path: name.clone(),
            ..raw.declaration
        },
        name,
    })
}

/// Turn a raw layout into a validated one, or say exactly why it cannot be.
#[allow(clippy::too_many_lines)]
fn validate(raw: RawLayout, source: pb::LayoutSource) -> Result<Layout, ParseError> {
    if raw.records.is_empty() {
        return Err(ParseError::invalid(
            "a layout needs at least one record schema",
        ));
    }
    if raw.footer_size > MAX_FOOTER_BYTES {
        return Err(ParseError::unsupported(format!(
            "footer_size {} exceeds the {MAX_FOOTER_BYTES}-byte limit; footer bytes are held \
             in memory for the whole parse",
            raw.footer_size
        )));
    }

    let record_length_field = raw
        .record_length_field
        .map(|field| prefix_field(field, "record_length"))
        .transpose()?;
    if let Some(field) = record_length_field.as_ref()
        && field.scale != 0
    {
        return Err(ParseError::invalid(
            "the record_length field must have scale 0: a record cannot be a fraction of a byte \
             long",
        ));
    }
    let mut record_type_field = raw
        .record_type_field
        .map(|field| prefix_field(field, "record_type"))
        .transpose()?;
    let prefix_size = record_length_field.as_ref().map_or(0, |field| field.size)
        + record_type_field.as_ref().map_or(0, |field| field.size);
    // The type field sits after the length field, so its offset within the
    // prefix is whatever the length field occupies.
    if let Some(field) = record_type_field.as_mut() {
        field.offset = record_length_field.as_ref().map_or(0, |length| length.size);
    }

    if raw.records.len() > 1 && record_type_field.is_none() {
        return Err(ParseError::invalid(
            "a layout with more than one record schema needs a record_type_field to choose \
             between them",
        ));
    }

    let mut names = BTreeSet::new();
    let mut selectors = BTreeSet::new();
    let mut records = Vec::with_capacity(raw.records.len());
    for (index, raw_record) in raw.records.into_iter().enumerate() {
        let name = if raw_record.name.is_empty() {
            DEFAULT_RECORD_NAME.to_string()
        } else {
            raw_record.name
        };
        if !names.insert(name.clone()) {
            return Err(ParseError::invalid(format!(
                "two record schemas are both named {name:?}; schema names identify the rows they \
                 produce and must be unique"
            )));
        }
        if record_type_field.is_some() {
            let Some(selector) = raw_record.selector.clone() else {
                return Err(ParseError::invalid(format!(
                    "record schema {name:?} needs a selector: the layout has a record_type_field \
                     and every schema must say which value picks it"
                )));
            };
            if !selectors.insert(selector.clone()) {
                return Err(ParseError::invalid(format!(
                    "record selector {selector:?} matches two schemas; a record must resolve to \
                     exactly one layout"
                )));
            }
        }
        if raw_record.fields.is_empty() {
            return Err(ParseError::invalid(format!(
                "record schema {name:?} has no fields"
            )));
        }

        let mut cursor: u32 = 0;
        let mut fields = Vec::with_capacity(raw_record.fields.len());
        let mut field_names = BTreeSet::new();
        for raw_field in raw_record.fields {
            if raw_field.size == 0 {
                return Err(ParseError::invalid(format!(
                    "field {:?} of record {name:?} has zero width",
                    raw_field.name
                )));
            }
            check_width(&raw_field, &format!("record {name:?}"))?;
            let offset = match raw_field.offset {
                None => cursor,
                Some(declared) if declared >= cursor => declared,
                Some(declared) => {
                    return Err(ParseError::invalid(format!(
                        "field {:?} of record {name:?} declares offset {declared} but the fields \
                         before it already reach {cursor}; fields may not overlap",
                        raw_field.name
                    )));
                }
            };
            cursor = offset.checked_add(raw_field.size).ok_or_else(|| {
                ParseError::invalid(format!("record {name:?} overflows a 32-bit record length"))
            })?;
            if cursor > MAX_RECORD_BYTES {
                return Err(ParseError::unsupported(format!(
                    "record {name:?} is at least {cursor} bytes; this build decodes records up \
                     to {MAX_RECORD_BYTES} bytes"
                )));
            }
            if raw_field.kind != FieldKind::Skip && !field_names.insert(raw_field.name.clone()) {
                return Err(ParseError::invalid(format!(
                    "record {name:?} has two fields named {:?}; cells are addressed by name, so \
                     qualify one of them or mark it as a filler",
                    raw_field.name
                )));
            }
            if raw_field.kind != FieldKind::Skip && raw_field.name.is_empty() {
                return Err(ParseError::invalid(format!(
                    "a field of record {name:?} has no name; only fillers may be anonymous"
                )));
            }
            if raw_field.scale != 0 && !raw_field.kind.is_numeric() {
                return Err(ParseError::invalid(format!(
                    "field {:?} of record {name:?} has scale {} but is not a numeric field",
                    raw_field.name, raw_field.scale
                )));
            }
            // A flat layout form declares no path, so the field name is the
            // whole of it: a consumer reads `path` without asking which form
            // the layout arrived in.
            let mut declaration = raw_field.declaration;
            if declaration.path.is_empty() {
                declaration.path.clone_from(&raw_field.name);
            }
            fields.push(Field {
                name: raw_field.name,
                offset,
                size: raw_field.size,
                kind: raw_field.kind,
                scale: raw_field.scale,
                picture: raw_field.picture,
                declaration,
            });
        }
        debug_assert!(!fields.is_empty(), "checked above");
        let _ = index;
        records.push(RecordLayout {
            name,
            fields,
            size: cursor,
            selector: raw_record.selector,
        });
    }

    Ok(Layout {
        records,
        description: raw.description,
        header_size: raw.header_size,
        footer_size: raw.footer_size,
        record_length_field,
        record_type_field,
        prefix_size,
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::{FieldKind, resolve};
    use crate::error::ParseError;
    use crate::proto::v1 as pb;

    /// A three-field layout: text, packed, filler.
    fn sample_layout() -> pb::EbcdicLayout {
        pb::EbcdicLayout {
            records: vec![pb::EbcdicRecordLayout {
                name: "CUSTOMER".into(),
                selector: None,
                fields: vec![
                    pb::EbcdicField {
                        name: "NAME".into(),
                        size: 8,
                        r#type: pb::FieldType::String as i32,
                        ..Default::default()
                    },
                    pb::EbcdicField {
                        name: "BALANCE".into(),
                        size: 5,
                        r#type: pb::FieldType::PackedDecimal as i32,
                        scale: 2,
                        ..Default::default()
                    },
                    pb::EbcdicField {
                        name: String::new(),
                        size: 3,
                        r#type: pb::FieldType::Skip as i32,
                        ..Default::default()
                    },
                ],
            }],
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

    #[test]
    fn offsets_are_the_running_sum_of_the_sizes() {
        let layout = resolve(&options(sample_layout())).expect("valid layout");
        let record = &layout.records[0];
        assert_eq!(record.size, 16);
        assert_eq!(
            record
                .fields
                .iter()
                .map(|f| (f.offset, f.size))
                .collect::<Vec<_>>(),
            vec![(0, 8), (8, 5), (13, 3)]
        );
        assert_eq!(record.fields[2].kind, FieldKind::Skip);
    }

    #[test]
    fn a_flat_layout_form_still_gives_every_field_a_path() {
        // The protobuf and JSON forms have no groups and no levels, so a
        // consumer of the schema reads `path` unconditionally and gets the
        // field name when that is all there is.
        let layout = resolve(&options(sample_layout())).expect("valid layout");
        let record = &layout.records[0];
        assert_eq!(
            record
                .fields
                .iter()
                .map(|f| f.declaration.path.as_str())
                .collect::<Vec<_>>(),
            vec!["NAME", "BALANCE", ""]
        );
        assert!(
            record.fields.iter().all(|f| f.declaration.level.is_none()
                && f.declaration.occurs_index.is_none()
                && f.declaration.conditions.is_empty()),
            "a flat form declares no hierarchy and must not invent one"
        );
    }

    #[test]
    fn a_missing_layout_is_invalid_argument_and_says_why() {
        let err = resolve(&pb::ParseOptions::default()).unwrap_err();
        let ParseError::Invalid(message) = &err else {
            panic!("got {err:?}")
        };
        assert!(message.contains("copybook"), "{message}");
        assert!(message.contains("layout_json"), "{message}");
    }

    #[test]
    fn an_explicit_offset_may_leave_a_gap_but_never_overlap() {
        let mut layout = sample_layout();
        layout.records[0].fields[1].offset = Some(10);
        let resolved = resolve(&options(layout.clone())).expect("a gap is a legal skip");
        assert_eq!(resolved.records[0].fields[1].offset, 10);
        assert_eq!(resolved.records[0].size, 18);

        layout.records[0].fields[1].offset = Some(4);
        let err = resolve(&options(layout)).unwrap_err();
        let ParseError::Invalid(message) = &err else {
            panic!("got {err:?}")
        };
        assert!(message.contains("overlap"), "{message}");
    }

    #[test]
    fn several_schemas_need_a_record_type_field_and_unique_selectors() {
        let mut layout = sample_layout();
        let mut second = layout.records[0].clone();
        second.name = "ORDER".into();
        layout.records.push(second);

        let err = resolve(&options(layout.clone())).unwrap_err();
        assert!(
            matches!(&err, ParseError::Invalid(m) if m.contains("record_type_field")),
            "got {err:?}"
        );

        layout.record_type_field = Some(pb::EbcdicField {
            name: "RECTYPE".into(),
            size: 1,
            r#type: pb::FieldType::String as i32,
            ..Default::default()
        });
        let err = resolve(&options(layout.clone())).unwrap_err();
        assert!(
            matches!(&err, ParseError::Invalid(m) if m.contains("selector")),
            "got {err:?}"
        );

        layout.records[0].selector = Some("C".into());
        layout.records[1].selector = Some("C".into());
        let err = resolve(&options(layout.clone())).unwrap_err();
        assert!(
            matches!(&err, ParseError::Invalid(m) if m.contains("matches two schemas")),
            "got {err:?}"
        );

        layout.records[1].selector = Some("O".into());
        let resolved = resolve(&options(layout)).expect("distinct selectors resolve");
        assert_eq!(resolved.prefix_size, 1);
        assert_eq!(
            resolved.select(Some("O")).map(|r| r.name.as_str()),
            Some("ORDER")
        );
        assert!(resolved.select(Some("Z")).is_none());
    }

    #[test]
    fn duplicate_field_names_are_rejected_but_fillers_may_repeat() {
        let mut layout = sample_layout();
        layout.records[0].fields[1].name = "NAME".into();
        let err = resolve(&options(layout)).unwrap_err();
        assert!(
            matches!(&err, ParseError::Invalid(m) if m.contains("two fields named")),
            "got {err:?}"
        );

        let mut layout = sample_layout();
        let filler = layout.records[0].fields[2].clone();
        layout.records[0].fields.push(filler);
        resolve(&options(layout)).expect("two anonymous fillers are fine");
    }

    #[test]
    fn a_field_wider_than_its_decoder_is_unimplemented() {
        let mut layout = sample_layout();
        layout.records[0].fields[1].size = 64;
        let err = resolve(&options(layout)).unwrap_err();
        assert!(matches!(err, ParseError::Unsupported(_)), "got {err:?}");
    }

    #[test]
    fn the_docling_json_shape_loads_and_matches_the_protobuf_one() {
        let json = br#"{
            "records": [{
                "name": "CUSTOMER",
                "fields": [
                    {"name": "NAME", "size": 8, "type": "string"},
                    {"name": "BALANCE", "size": 5, "type": "packed_decimal", "scale": 2},
                    {"name": "", "size": 3, "type": "skip"}
                ]
            }]
        }"#;
        let from_json = resolve(&pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::LayoutJson(json.to_vec())),
            ..Default::default()
        })
        .expect("Docling JSON loads");
        let from_proto = resolve(&options(sample_layout())).unwrap();
        assert_eq!(from_json.records, from_proto.records);
        assert_eq!(from_json.source, pb::LayoutSource::Json);
        assert_eq!(from_proto.source, pb::LayoutSource::Proto);
    }

    #[test]
    fn a_json_field_without_a_type_defaults_to_character_data() {
        let json = br#"{"records": [{"fields": [{"name": "F", "size": 4}]}]}"#;
        let layout = resolve(&pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::LayoutJson(json.to_vec())),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(layout.records[0].name, "record");
        assert_eq!(layout.records[0].fields[0].kind, FieldKind::Text);
    }

    #[test]
    fn malformed_json_is_invalid_argument_not_a_panic() {
        let err = resolve(&pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::LayoutJson(b"{".to_vec())),
            ..Default::default()
        })
        .unwrap_err();
        assert!(matches!(err, ParseError::Invalid(_)), "got {err:?}");
    }
}
