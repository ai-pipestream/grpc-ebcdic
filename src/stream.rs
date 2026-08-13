// SPDX-License-Identifier: Apache-2.0

//! The incremental record walker.
//!
//! This is where "the live stream is the product" is actually implemented, and
//! it is a state machine rather than a loop over a buffer so that it can be
//! true: [`RecordStream::push`] appends whatever bytes have arrived and
//! [`RecordStream::next_record`] hands back one record at a time, as soon as
//! that record's last byte is present and never later. The caller awaits the
//! send of each row before asking for the next, so a slow consumer applies
//! backpressure to the parse instead of building a queue.
//!
//! Nothing here is async and nothing here touches the network, which is what
//! makes the streaming contract testable without a socket.

use std::collections::BTreeMap;

use crate::codec::Codec;
use crate::decode::{
    FieldValue, decode_binary, decode_packed, decode_text, decode_zoned, render_decimal,
};
use crate::error::ParseError;
use crate::layout::{Field, FieldKind, Layout, RecordLayout};
use crate::proto::v1 as pb;

/// Decoder settings that do not come from the layout.
#[derive(Debug, Clone, Copy)]
pub struct DecodeOptions {
    /// Code page character data is decoded with.
    pub codec: Codec,
    /// Whether to drop Unicode control characters from character data.
    pub strip_control_characters: bool,
    /// Stop after this many records; zero reads them all.
    pub max_records: u64,
    /// Fail the stream on a trailing partial record instead of warning.
    pub abort_on_error: bool,
}

/// An incremental walk over an EBCDIC byte stream.
pub struct RecordStream {
    /// The validated layout.
    layout: Layout,
    /// Decoder settings.
    options: DecodeOptions,
    /// Bytes received but not yet consumed.
    buffer: Vec<u8>,
    /// Absolute offset of `buffer[0]` within the input.
    buffer_base: u64,
    /// Total bytes received so far.
    received: u64,
    /// Header bytes still to skip.
    header_left: u64,
    /// Records emitted so far.
    records_kept: u64,
    /// Rows emitted per schema name.
    rows_per_type: BTreeMap<String, u64>,
    /// Whether `max_records` has been reached.
    truncated: bool,
    /// Whether the client's half of the stream has ended.
    input_finished: bool,
}

impl RecordStream {
    /// Start a walk over a validated layout.
    #[must_use]
    pub fn new(layout: Layout, options: DecodeOptions) -> Self {
        let rows_per_type = layout
            .records
            .iter()
            .map(|record| (record.name.clone(), 0))
            .collect();
        let header_left = u64::from(layout.header_size);
        Self {
            layout,
            options,
            buffer: Vec::new(),
            buffer_base: 0,
            received: 0,
            header_left,
            records_kept: 0,
            rows_per_type,
            truncated: false,
            input_finished: false,
        }
    }

    /// Append a chunk of input.
    ///
    /// Once `max_records` has been reached the bytes are counted and dropped
    /// rather than buffered: the parse is over, and holding the rest of a
    /// ten-gigabyte extract to report a byte count would be absurd.
    pub fn push(&mut self, chunk: &[u8]) {
        self.received += chunk.len() as u64;
        if self.truncated {
            self.buffer_base = self.received;
            self.buffer.clear();
            return;
        }
        self.buffer.extend_from_slice(chunk);
    }

    /// Note that the client will send no more bytes.
    pub const fn finish_input(&mut self) {
        self.input_finished = true;
    }

    /// Total bytes received so far.
    #[must_use]
    pub const fn received(&self) -> u64 {
        self.received
    }

    /// The absolute offset one past the last byte that is certainly *not* part
    /// of the trailing footer.
    ///
    /// Correct at every point in the stream, not only at the end: the footer is
    /// the last `footer_size` bytes of the input, so any byte more than
    /// `footer_size` from the high-water mark is definitely data. That is what
    /// lets a layout with a footer still stream instead of buffering the file.
    fn decodable_end(&self) -> u64 {
        self.received
            .saturating_sub(u64::from(self.layout.footer_size))
    }

    /// Bytes available for decoding right now, starting at `buffer_base`.
    fn available(&self) -> usize {
        let end = self.decodable_end().saturating_sub(self.buffer_base);
        // The buffer never holds more than what has been received, so the
        // clamp only matters while the footer reserve is still filling.
        usize::try_from(end)
            .unwrap_or(usize::MAX)
            .min(self.buffer.len())
    }

    /// Drop `count` consumed bytes from the front of the buffer.
    fn consume(&mut self, count: usize) {
        self.buffer.drain(..count);
        self.buffer_base += count as u64;
    }

    /// Decode the next record, if all of its bytes have arrived.
    ///
    /// Returns `Ok(None)` when more input is needed, when the input is over, or
    /// when `max_records` has been reached. Call it in a loop after every
    /// [`Self::push`] until it yields `None`.
    ///
    /// # Errors
    ///
    /// [`ParseError::Invalid`] for an undecodable field, a record type no
    /// schema claims, or a record length shorter than its own prefix.
    pub fn next_record(&mut self) -> Result<Option<pb::RecordRow>, ParseError> {
        if self.truncated {
            return Ok(None);
        }
        if self.header_left > 0 {
            let available = self.available() as u64;
            let skip = self.header_left.min(available);
            self.consume(usize::try_from(skip).unwrap_or(usize::MAX));
            self.header_left -= skip;
            if self.header_left > 0 {
                return Ok(None);
            }
        }

        let prefix_size = self.layout.prefix_size as usize;
        if self.available() < prefix_size {
            return Ok(None);
        }
        // An input that ends exactly on a record boundary has nothing left,
        // and a zero-length prefix must not be mistaken for one more record.
        if prefix_size == 0 && self.available() == 0 {
            return Ok(None);
        }

        let (record, body_size) = self.read_prefix()?;
        let total = prefix_size + body_size;
        if self.available() < total {
            return Ok(None);
        }

        let byte_offset = self.buffer_base;
        let body = &self.buffer[prefix_size..total];
        let cells = decode_cells(&record, body, &self.options)?;
        let row_index = self
            .rows_per_type
            .get(&record.name)
            .copied()
            .unwrap_or_default();
        let row = pb::RecordRow {
            record_type: record.name.clone(),
            row_index,
            record_index: self.records_kept,
            byte_offset,
            cells,
        };
        self.consume(total);
        self.records_kept += 1;
        *self.rows_per_type.entry(record.name).or_default() += 1;
        if self.options.max_records != 0 && self.records_kept >= self.options.max_records {
            self.truncated = true;
            self.buffer_base = self.received;
            self.buffer.clear();
        }
        Ok(Some(row))
    }

    /// Read the record prefix and resolve the schema plus the body length.
    ///
    /// Does not consume: the caller may still be short of the body, in which
    /// case the prefix has to be re-read after the next chunk.
    fn read_prefix(&self) -> Result<(RecordLayout, usize), ParseError> {
        let mut cursor = 0usize;
        let mut declared_length: Option<i128> = None;
        if let Some(field) = self.layout.record_length_field.as_ref() {
            let bytes = &self.buffer[cursor..cursor + field.size as usize];
            let value = decode_field(field, bytes, &self.options)?;
            declared_length = Some(match value {
                FieldValue::Number { unscaled, .. } => unscaled,
                FieldValue::Text(text) => text.trim().parse::<i128>().map_err(|_| {
                    ParseError::invalid(format!(
                        "record length field {:?} holds {text:?} at byte {}, which is not a number",
                        field.name, self.buffer_base
                    ))
                })?,
            });
            cursor += field.size as usize;
        }
        let mut record_type: Option<String> = None;
        if let Some(field) = self.layout.record_type_field.as_ref() {
            let bytes = &self.buffer[cursor..cursor + field.size as usize];
            record_type = Some(decode_field(field, bytes, &self.options)?.to_display_string());
        }

        let Some(record) = self.layout.select(record_type.as_deref()) else {
            return Err(ParseError::invalid(format!(
                "no record schema has selector {:?} (record at byte {})",
                record_type.unwrap_or_default(),
                self.buffer_base
            )));
        };

        let body_size = match declared_length {
            None => i128::from(record.size),
            Some(length) => length - i128::from(self.layout.prefix_size),
        };
        if body_size < 0 {
            return Err(ParseError::invalid(format!(
                "record at byte {} declares length {}, shorter than its own {}-byte prefix",
                self.buffer_base,
                declared_length.unwrap_or_default(),
                self.layout.prefix_size
            )));
        }
        let body_size = usize::try_from(body_size).map_err(|_| {
            ParseError::invalid(format!(
                "record at byte {} declares an unusable length {body_size}",
                self.buffer_base
            ))
        })?;
        if body_size > crate::layout::MAX_RECORD_BYTES as usize {
            return Err(ParseError::unsupported(format!(
                "record at byte {} declares a {body_size}-byte body; this build decodes records \
                 up to {} bytes",
                self.buffer_base,
                crate::layout::MAX_RECORD_BYTES
            )));
        }
        Ok((record.clone(), body_size))
    }

    /// Build the trailer once the input is over and no record is left.
    ///
    /// # Errors
    ///
    /// [`ParseError::Invalid`] when `abort_on_error` is set and the input did
    /// not divide evenly into records.
    pub fn status(&self) -> Result<pb::ParseStatus, ParseError> {
        debug_assert!(
            self.input_finished,
            "the trailer is only meaningful after the input ends"
        );
        let leftover = self.decodable_end().saturating_sub(self.buffer_base);
        let mut warnings = Vec::new();

        if self.received < u64::from(self.layout.header_size) + u64::from(self.layout.footer_size) {
            warnings.push(pb::ParseWarning {
                code: pb::WarningCode::InputShorterThanBoundaries as i32,
                message: format!(
                    "the input is {} bytes, shorter than the {}-byte header plus {}-byte footer \
                     the layout declares",
                    self.received, self.layout.header_size, self.layout.footer_size
                ),
                byte_offset: self.received,
            });
        } else if self.truncated {
            warnings.push(pb::ParseWarning {
                code: pb::WarningCode::MaxRecordsReached as i32,
                message: format!(
                    "stopped after {} records; {} bytes of input were not decoded",
                    self.records_kept,
                    self.received.saturating_sub(self.buffer_base)
                ),
                byte_offset: self.buffer_base,
            });
        } else if leftover > 0 {
            let message = format!(
                "the input ends inside a record: {leftover} bytes after the last whole record at \
                 byte {}",
                self.buffer_base
            );
            if self.options.abort_on_error {
                return Err(ParseError::invalid(message));
            }
            warnings.push(pb::ParseWarning {
                code: pb::WarningCode::TrailingPartialRecord as i32,
                message,
                byte_offset: self.buffer_base,
            });
        }

        Ok(pb::ParseStatus {
            records_kept: self.records_kept,
            bytes_received: self.received,
            bytes_consumed: self.buffer_base,
            trailing_bytes: leftover,
            truncated: self.truncated,
            warnings,
            rows_per_record_type: self
                .rows_per_type
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect(),
        })
    }
}

/// Decode one field's bytes into a value.
fn decode_field(
    field: &Field,
    bytes: &[u8],
    options: &DecodeOptions,
) -> Result<FieldValue, ParseError> {
    let number = |unscaled| {
        Ok(FieldValue::Number {
            unscaled,
            scale: field.scale,
        })
    };
    match field.kind {
        FieldKind::Text => Ok(FieldValue::Text(decode_text(
            options.codec,
            bytes,
            options.strip_control_characters,
        )?)),
        FieldKind::Integer => number(decode_binary(bytes, true)?),
        FieldKind::UnsignedInteger => number(decode_binary(bytes, false)?),
        FieldKind::PackedDecimal => number(decode_packed(bytes)?),
        FieldKind::ZonedDecimal => number(decode_zoned(bytes)?),
        FieldKind::Skip => Ok(FieldValue::Text(String::new())),
    }
}

/// Decode every non-filler field of one record body into a wire cell.
fn decode_cells(
    record: &RecordLayout,
    body: &[u8],
    options: &DecodeOptions,
) -> Result<Vec<pb::Cell>, ParseError> {
    let mut cells = Vec::with_capacity(record.fields.len());
    for field in &record.fields {
        if field.kind == FieldKind::Skip {
            continue;
        }
        let start = field.offset as usize;
        let end = start + field.size as usize;
        let Some(bytes) = body.get(start..end) else {
            // Only reachable with a length-prefixed record whose declared
            // length is shorter than the schema it selected.
            return Err(ParseError::invalid(format!(
                "record {:?} is {} bytes but field {:?} needs bytes {start}..{end}",
                record.name,
                body.len(),
                field.name
            )));
        };
        let value = decode_field(field, bytes, options).map_err(|err| match err {
            ParseError::Invalid(message) => ParseError::Invalid(format!(
                "field {:?} of record {:?}: {message}",
                field.name, record.name
            )),
            other => other,
        })?;
        cells.push(pb::Cell {
            name: field.name.clone(),
            r#type: field.kind.to_proto() as i32,
            value: Some(to_cell_value(value, field.scale)),
        });
    }
    Ok(cells)
}

/// Shape a decoded value into the wire cell's oneof.
///
/// A scale of zero yields an `integer` when it fits in 64 bits. Everything else
/// is an exact [`pb::Decimal`], never a float: a `PIC S9(18)V99 COMP-3` has
/// twenty significant digits and an IEEE double has fifteen.
fn to_cell_value(value: FieldValue, scale: u32) -> pb::cell::Value {
    match value {
        FieldValue::Text(text) => pb::cell::Value::Text(text),
        FieldValue::Number { unscaled, scale: _ } if scale == 0 => match i64::try_from(unscaled) {
            Ok(fits) => pb::cell::Value::Integer(fits),
            Err(_) => pb::cell::Value::Decimal(pb::Decimal {
                text: render_decimal(unscaled, 0),
                scale: 0,
                unscaled: None,
            }),
        },
        FieldValue::Number { unscaled, scale } => pb::cell::Value::Decimal(pb::Decimal {
            text: render_decimal(unscaled, scale),
            scale,
            unscaled: i64::try_from(unscaled).ok(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{DecodeOptions, RecordStream};
    use crate::codec::Codec;
    use crate::error::ParseError;
    use crate::layout;
    use crate::proto::v1 as pb;

    /// Default decoder settings for the tests: cp037, strip controls, no caps.
    fn options() -> DecodeOptions {
        DecodeOptions {
            codec: Codec::resolve("cp037").unwrap(),
            strip_control_characters: true,
            max_records: 0,
            abort_on_error: false,
        }
    }

    /// A four-byte fixed record: two characters and a two-digit zoned number.
    fn simple_layout() -> layout::Layout {
        layout::resolve(&pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::Copybook(
                "01 R.\n05 CODE PIC X(2).\n05 QTY PIC 9(2).\n".into(),
            )),
            ..Default::default()
        })
        .expect("the copybook compiles")
    }

    /// Encode one four-byte record.
    fn record(code: &str, qty: &str) -> Vec<u8> {
        let codec = Codec::resolve("cp037").unwrap();
        let mut bytes = codec.encode(code).unwrap();
        bytes.extend(codec.encode(qty).unwrap());
        bytes
    }

    /// Drain every record the stream can produce right now.
    fn drain(stream: &mut RecordStream) -> Vec<pb::RecordRow> {
        let mut rows = Vec::new();
        while let Some(row) = stream.next_record().expect("no decode failure") {
            rows.push(row);
        }
        rows
    }

    #[test]
    fn a_record_is_emitted_the_moment_its_last_byte_arrives() {
        let mut stream = RecordStream::new(simple_layout(), options());
        let bytes = record("AB", "42");
        // Three of four bytes: nothing can be decoded yet.
        stream.push(&bytes[..3]);
        assert!(
            drain(&mut stream).is_empty(),
            "a partial record yields nothing"
        );
        // The fourth byte completes it, with no further input and no end of
        // stream. This is the property the whole service exists for.
        stream.push(&bytes[3..]);
        let rows = drain(&mut stream);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].cells[0].value,
            Some(pb::cell::Value::Text("AB".into()))
        );
        assert_eq!(rows[0].cells[1].value, Some(pb::cell::Value::Integer(42)));
        assert_eq!(rows[0].byte_offset, 0);
    }

    #[test]
    fn rows_carry_their_own_index_and_offset() {
        let mut stream = RecordStream::new(simple_layout(), options());
        for (index, code) in ["AA", "BB", "CC"].iter().enumerate() {
            stream.push(&record(code, "01"));
            let rows = drain(&mut stream);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].record_index, index as u64);
            assert_eq!(rows[0].row_index, index as u64);
            assert_eq!(rows[0].byte_offset, index as u64 * 4);
        }
    }

    #[test]
    fn a_trailing_partial_record_warns_and_is_not_an_error() {
        let mut stream = RecordStream::new(simple_layout(), options());
        stream.push(&record("AB", "42"));
        stream.push(&record("CD", "07")[..2]);
        drain(&mut stream);
        stream.finish_input();
        let status = stream.status().expect("a short tail is recoverable");
        assert_eq!(status.records_kept, 1);
        assert_eq!(status.bytes_received, 6);
        assert_eq!(status.bytes_consumed, 4);
        assert_eq!(status.trailing_bytes, 2);
        assert_eq!(status.warnings.len(), 1);
        assert_eq!(
            status.warnings[0].code,
            pb::WarningCode::TrailingPartialRecord as i32
        );
    }

    #[test]
    fn abort_on_error_turns_the_same_tail_into_invalid_argument() {
        let mut stream = RecordStream::new(
            simple_layout(),
            DecodeOptions {
                abort_on_error: true,
                ..options()
            },
        );
        stream.push(&record("AB", "42"));
        stream.push(&[0xc3, 0xc4]);
        drain(&mut stream);
        stream.finish_input();
        let err = stream.status().unwrap_err();
        assert!(matches!(err, ParseError::Invalid(_)), "got {err:?}");
    }

    #[test]
    fn max_records_stops_the_walk_and_stops_buffering() {
        let mut stream = RecordStream::new(
            simple_layout(),
            DecodeOptions {
                max_records: 2,
                ..options()
            },
        );
        for code in ["AA", "BB", "CC", "DD"] {
            stream.push(&record(code, "01"));
        }
        let rows = drain(&mut stream);
        assert_eq!(rows.len(), 2);
        stream.finish_input();
        let status = stream.status().unwrap();
        assert!(status.truncated);
        assert_eq!(status.records_kept, 2);
        assert_eq!(status.bytes_received, 16);
        assert_eq!(
            status.warnings[0].code,
            pb::WarningCode::MaxRecordsReached as i32
        );
    }

    #[test]
    fn headers_and_footers_are_skipped_without_buffering_the_file() {
        let mut layout = simple_layout();
        layout.header_size = 3;
        layout.footer_size = 5;
        let mut stream = RecordStream::new(layout, options());
        stream.push(b"HDR");
        stream.push(&record("AB", "42"));
        // The footer's bytes are indistinguishable from data until the input
        // ends, so they must not be decoded early.
        stream.push(b"TRAIL");
        let rows = drain(&mut stream);
        assert_eq!(rows.len(), 1, "the footer is held back, not decoded");
        assert_eq!(
            rows[0].cells[0].value,
            Some(pb::cell::Value::Text("AB".into()))
        );
        stream.finish_input();
        let status = stream.status().unwrap();
        assert_eq!(status.records_kept, 1);
        assert!(status.warnings.is_empty(), "{:?}", status.warnings);
        assert_eq!(status.trailing_bytes, 0);
    }

    #[test]
    fn an_input_shorter_than_its_own_boundaries_warns_rather_than_failing() {
        let mut layout = simple_layout();
        layout.header_size = 8;
        layout.footer_size = 8;
        let mut stream = RecordStream::new(layout, options());
        stream.push(b"tiny");
        drain(&mut stream);
        stream.finish_input();
        let status = stream.status().unwrap();
        assert_eq!(status.records_kept, 0);
        assert_eq!(
            status.warnings[0].code,
            pb::WarningCode::InputShorterThanBoundaries as i32
        );
    }

    #[test]
    fn a_bad_packed_nibble_fails_the_stream_with_invalid_argument() {
        let layout = layout::resolve(&pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::Copybook(
                "01 R.\n05 AMOUNT PIC S9(5)V99 COMP-3.\n".into(),
            )),
            ..Default::default()
        })
        .unwrap();
        let mut stream = RecordStream::new(layout, options());
        // 0xa in a digit position rather than the sign position.
        stream.push(&[0x1a, 0x23, 0x45, 0x6c]);
        let err = stream.next_record().unwrap_err();
        let ParseError::Invalid(message) = &err else {
            panic!("got {err:?}")
        };
        assert!(message.contains("AMOUNT"), "the field is named: {message}");
        assert!(message.contains("nibble"), "{message}");
    }

    #[test]
    fn selectors_route_records_to_their_own_schema() {
        let layout = layout::resolve(&pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::Layout(pb::EbcdicLayout {
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
                        fields: vec![pb::EbcdicField {
                            name: "NAME".into(),
                            size: 4,
                            r#type: pb::FieldType::String as i32,
                            ..Default::default()
                        }],
                    },
                    pb::EbcdicRecordLayout {
                        name: "ORDER".into(),
                        selector: Some("O".into()),
                        fields: vec![pb::EbcdicField {
                            name: "QTY".into(),
                            size: 2,
                            r#type: pb::FieldType::ZonedDecimal as i32,
                            ..Default::default()
                        }],
                    },
                ],
                ..Default::default()
            })),
            ..Default::default()
        })
        .unwrap();
        let codec = Codec::resolve("cp037").unwrap();
        let mut stream = RecordStream::new(layout, options());
        stream.push(&codec.encode("CJane").unwrap());
        stream.push(&codec.encode("O12").unwrap());
        stream.push(&codec.encode("CBob!").unwrap());
        let rows = drain(&mut stream);
        assert_eq!(
            rows.iter()
                .map(|r| (r.record_type.as_str(), r.row_index))
                .collect::<Vec<_>>(),
            vec![("CUSTOMER", 0), ("ORDER", 0), ("CUSTOMER", 1)]
        );
        stream.finish_input();
        let status = stream.status().unwrap();
        assert_eq!(status.rows_per_record_type["CUSTOMER"], 2);
        assert_eq!(status.rows_per_record_type["ORDER"], 1);
    }

    #[test]
    fn an_unmatched_selector_is_invalid_argument_because_the_length_is_unknown() {
        let layout = layout::resolve(&pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::Layout(pb::EbcdicLayout {
                record_type_field: Some(pb::EbcdicField {
                    name: "RECTYPE".into(),
                    size: 1,
                    r#type: pb::FieldType::String as i32,
                    ..Default::default()
                }),
                records: vec![pb::EbcdicRecordLayout {
                    name: "CUSTOMER".into(),
                    selector: Some("C".into()),
                    fields: vec![pb::EbcdicField {
                        name: "NAME".into(),
                        size: 4,
                        r#type: pb::FieldType::String as i32,
                        ..Default::default()
                    }],
                }],
                ..Default::default()
            })),
            ..Default::default()
        })
        .unwrap();
        let codec = Codec::resolve("cp037").unwrap();
        let mut stream = RecordStream::new(layout, options());
        stream.push(&codec.encode("XJane").unwrap());
        let err = stream.next_record().unwrap_err();
        assert!(
            matches!(&err, ParseError::Invalid(m) if m.contains("selector")),
            "got {err:?}"
        );
    }

    #[test]
    fn a_length_prefix_makes_records_variable_length() {
        let layout = layout::resolve(&pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::Layout(pb::EbcdicLayout {
                record_length_field: Some(pb::EbcdicField {
                    name: "RECLEN".into(),
                    size: 2,
                    r#type: pb::FieldType::ZonedDecimal as i32,
                    ..Default::default()
                }),
                records: vec![pb::EbcdicRecordLayout {
                    name: "NOTE".into(),
                    selector: None,
                    fields: vec![pb::EbcdicField {
                        name: "TEXT".into(),
                        size: 4,
                        r#type: pb::FieldType::String as i32,
                        ..Default::default()
                    }],
                }],
                ..Default::default()
            })),
            ..Default::default()
        })
        .unwrap();
        let codec = Codec::resolve("cp037").unwrap();
        let mut stream = RecordStream::new(layout, options());
        // Length 06 = a 2-byte prefix plus the schema's 4 bytes exactly.
        stream.push(&codec.encode("06ABCD").unwrap());
        // Length 09 = the same 4 bytes plus 3 bytes of inter-record slack,
        // which the prefix exists to let the walk step over.
        stream.push(&codec.encode("09WXYZ...").unwrap());
        stream.push(&codec.encode("06LAST").unwrap());
        let rows = drain(&mut stream);
        assert_eq!(
            rows.iter()
                .map(|r| r.cells[0].value.clone().unwrap())
                .collect::<Vec<_>>(),
            vec![
                pb::cell::Value::Text("ABCD".into()),
                pb::cell::Value::Text("WXYZ".into()),
                pb::cell::Value::Text("LAST".into()),
            ]
        );
        assert_eq!(
            rows.iter().map(|r| r.byte_offset).collect::<Vec<_>>(),
            vec![0, 6, 15]
        );
    }

    #[test]
    fn a_declared_length_shorter_than_the_schema_is_invalid_argument() {
        // Docling slices the short body with Python's forgiving indexing and
        // decodes whatever is there. Truncating a field silently is how a
        // balance comes back off by three digits, so this build refuses.
        let layout = layout::resolve(&pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::Layout(pb::EbcdicLayout {
                record_length_field: Some(pb::EbcdicField {
                    name: "RECLEN".into(),
                    size: 2,
                    r#type: pb::FieldType::ZonedDecimal as i32,
                    ..Default::default()
                }),
                records: vec![pb::EbcdicRecordLayout {
                    name: "NOTE".into(),
                    selector: None,
                    fields: vec![pb::EbcdicField {
                        name: "TEXT".into(),
                        size: 8,
                        r#type: pb::FieldType::String as i32,
                        ..Default::default()
                    }],
                }],
                ..Default::default()
            })),
            ..Default::default()
        })
        .unwrap();
        let codec = Codec::resolve("cp037").unwrap();
        let mut stream = RecordStream::new(layout, options());
        stream.push(&codec.encode("05ABC").unwrap());
        let err = stream.next_record().unwrap_err();
        assert!(
            matches!(&err, ParseError::Invalid(m) if m.contains("TEXT")),
            "got {err:?}"
        );
    }

    #[test]
    fn a_length_shorter_than_the_prefix_is_invalid_argument() {
        let layout = layout::resolve(&pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::Layout(pb::EbcdicLayout {
                record_length_field: Some(pb::EbcdicField {
                    name: "RECLEN".into(),
                    size: 2,
                    r#type: pb::FieldType::ZonedDecimal as i32,
                    ..Default::default()
                }),
                records: vec![pb::EbcdicRecordLayout {
                    name: "NOTE".into(),
                    selector: None,
                    fields: vec![pb::EbcdicField {
                        name: "TEXT".into(),
                        size: 4,
                        r#type: pb::FieldType::String as i32,
                        ..Default::default()
                    }],
                }],
                ..Default::default()
            })),
            ..Default::default()
        })
        .unwrap();
        let codec = Codec::resolve("cp037").unwrap();
        let mut stream = RecordStream::new(layout, options());
        stream.push(&codec.encode("01ABCD").unwrap());
        let err = stream.next_record().unwrap_err();
        assert!(
            matches!(&err, ParseError::Invalid(m) if m.contains("shorter than its own")),
            "got {err:?}"
        );
    }

    #[test]
    fn wide_packed_values_stay_exact_as_decimals_rather_than_integers() {
        let layout = layout::resolve(&pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::Layout(pb::EbcdicLayout {
                records: vec![pb::EbcdicRecordLayout {
                    name: "BIG".into(),
                    selector: None,
                    fields: vec![pb::EbcdicField {
                        name: "HUGE".into(),
                        size: 12,
                        r#type: pb::FieldType::PackedDecimal as i32,
                        ..Default::default()
                    }],
                }],
                ..Default::default()
            })),
            ..Default::default()
        })
        .unwrap();
        let mut stream = RecordStream::new(layout, options());
        // 23 digits of 9 with a positive sign: past i64 by five orders.
        let mut bytes = vec![0x99; 11];
        bytes.push(0x9c);
        stream.push(&bytes);
        let row = stream.next_record().unwrap().unwrap();
        let Some(pb::cell::Value::Decimal(decimal)) = &row.cells[0].value else {
            panic!(
                "a 23-digit number cannot be an int64 cell: {:?}",
                row.cells[0].value
            );
        };
        assert_eq!(decimal.text, "9".repeat(23));
        assert_eq!(decimal.scale, 0);
        assert!(decimal.unscaled.is_none(), "no lossy int64 is offered");
    }
}
