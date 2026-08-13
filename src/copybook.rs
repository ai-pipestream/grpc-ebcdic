// SPDX-License-Identifier: Apache-2.0

//! A compiler for the flat subset of COBOL data descriptions that a record
//! copybook is made of.
//!
//! ## Why this exists, given `docs/design.md` says it should not
//!
//! `design.md` §2 lists "compiling arbitrary COBOL source" as a v1 non-goal
//! and expects the caller to send the already-normalized layout. That stays
//! true: this is not a COBOL compiler, it has no `PROCEDURE DIVISION`, no
//! copy-replacing, no `REDEFINES`, and no `OCCURS DEPENDING ON`. What it does
//! is turn the flat `level / name / PIC / USAGE` subset — the only part of a
//! copybook that describes bytes — into exactly the same normalized layout the
//! protobuf and JSON forms produce. Everything outside that subset is refused
//! with `UNIMPLEMENTED` and names the clause that did it, which is the whole
//! point: a caller finds out that their copybook needs hand-normalizing before
//! they ship it, not after a table of garbage arrives.
//!
//! ## Supported subset
//!
//! - Level numbers 01–49 and 77. Level 88 condition names are skipped (they
//!   describe values, not storage); level 66 `RENAMES` is refused.
//! - Group items, nested to any depth, flattened to their leaf fields.
//! - `PIC X`/`A` character items, `PIC 9` numeric items with optional leading
//!   `S` and a single `V` implied point.
//! - `USAGE DISPLAY` (the default), `COMP-3`/`COMPUTATIONAL-3`/
//!   `PACKED-DECIMAL`, and `COMP`/`COMP-4`/`COMPUTATIONAL`/`BINARY`.
//! - `OCCURS n [TIMES]` on an elementary item, expanded to `NAME(1)` … `NAME(n)`.
//! - `FILLER`, and anonymous items, which become `FIELD_TYPE_SKIP`.
//! - Clauses that do not change the bytes (`VALUE`, `JUSTIFIED`, `BLANK WHEN
//!   ZERO`, `GLOBAL`, `EXTERNAL`) are accepted and ignored.
//! - Both fixed-format (sequence area in columns 1–6, indicator in column 7,
//!   code through column 72) and free-format source.
//!
//! Anything else is `UNIMPLEMENTED`. Source that is not a data description at
//! all is `INVALID_ARGUMENT`.

use crate::error::ParseError;
use crate::layout::{FieldKind, RawField, RawLayout, RawRecord};

/// Largest `OCCURS` count the compiler will expand.
///
/// Each occurrence becomes a real field with a real name, so this bounds the
/// work a one-line copybook can ask for.
const MAX_OCCURS: u32 = 4096;

/// Compile copybook source into the raw layout shape.
///
/// # Errors
///
/// [`ParseError::Invalid`] when the source is not a parseable data
/// description; [`ParseError::Unsupported`] when it uses a clause outside the
/// documented subset.
pub(crate) fn compile(source: &str) -> Result<RawLayout, ParseError> {
    let statements = statements(source)?;
    if statements.is_empty() {
        return Err(ParseError::invalid(
            "the copybook has no data descriptions; a copybook is a list of level-numbered items \
             such as `01 REC.  05 NAME PIC X(20).`",
        ));
    }

    let mut items = Vec::new();
    for statement in statements {
        if let Some(item) = parse_item(&statement)? {
            items.push(item);
        }
    }
    if items.is_empty() {
        return Err(ParseError::invalid(
            "the copybook declares no storage: every item in it is a level-88 condition name",
        ));
    }

    let roots = items
        .iter()
        .filter(|item| item.level == 1 || item.level == 77)
        .count();
    if roots == 0 {
        return Err(ParseError::invalid(
            "the copybook has no 01-level record description",
        ));
    }
    if roots > 1 {
        return Err(ParseError::unsupported(
            "the copybook describes more than one 01-level record. Multi-schema files need a \
             record_type_field and per-schema selectors, which a copybook cannot express; send \
             ParseOptions.layout instead",
        ));
    }
    if items[0].level != 1 && items[0].level != 77 {
        return Err(ParseError::invalid(format!(
            "the copybook starts at level {:02} rather than 01",
            items[0].level
        )));
    }

    let record_name = items[0]
        .name
        .clone()
        .unwrap_or_else(|| "record".to_string());
    let mut fields = Vec::new();
    flatten(&items, &mut fields)?;
    if fields.is_empty() {
        return Err(ParseError::invalid(format!(
            "record {record_name:?} has no elementary fields"
        )));
    }

    Ok(RawLayout {
        records: vec![RawRecord {
            name: record_name,
            fields,
            selector: None,
        }],
        description: String::new(),
        header_size: 0,
        footer_size: 0,
        record_length_field: None,
        record_type_field: None,
    })
}

/// One data-description statement, already split into tokens.
type Statement = Vec<String>;

/// Split copybook source into period-terminated statements of tokens.
///
/// Handles the fixed-format card layout (sequence area, indicator column,
/// identification area) and quoted literals, because a `VALUE 'A.B'` clause
/// would otherwise end a statement in the middle of a string.
fn statements(source: &str) -> Result<Vec<Statement>, ParseError> {
    let mut out = Vec::new();
    let mut current: Statement = Vec::new();
    for raw_line in source.lines() {
        let Some(line) = strip_card_columns(raw_line) else {
            continue;
        };
        let mut chars = line.chars().peekable();
        let mut token = String::new();
        while let Some(ch) = chars.next() {
            match ch {
                '\'' | '"' => {
                    // A literal is one token, quotes included, so a period
                    // inside it cannot terminate the statement.
                    token.push(ch);
                    let mut closed = false;
                    for inner in chars.by_ref() {
                        token.push(inner);
                        if inner == ch {
                            closed = true;
                            break;
                        }
                    }
                    if !closed {
                        return Err(ParseError::invalid(format!(
                            "unterminated literal in copybook line {line:?}"
                        )));
                    }
                }
                // COBOL's own disambiguation rule: a period ends a sentence
                // only when a space or the end of the line follows it.
                // Anywhere else it is part of the token, which is what keeps
                // the decimal point of `PIC ZZ9.99` attached to its picture
                // instead of splitting it into a bogus `99` level number.
                '.' if chars.peek().is_none_or(|next| next.is_whitespace()) => {
                    if !token.is_empty() {
                        current.push(std::mem::take(&mut token));
                    }
                    if !current.is_empty() {
                        out.push(std::mem::take(&mut current));
                    }
                }
                c if c.is_whitespace() => {
                    if !token.is_empty() {
                        current.push(std::mem::take(&mut token));
                    }
                }
                c => token.push(c),
            }
        }
        if !token.is_empty() {
            current.push(token);
        }
    }
    if !current.is_empty() {
        return Err(ParseError::invalid(format!(
            "the copybook ends without a period after {:?}",
            current.join(" ")
        )));
    }
    Ok(out)
}

/// Strip the fixed-format card columns from one line, or drop it if it is a
/// comment.
///
/// Fixed format is detected rather than assumed: columns 1–6 must be digits or
/// blanks and column 7 must be one of the indicator characters. A free-format
/// line, which most modern copybooks are, falls through untouched.
fn strip_card_columns(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    if trimmed.starts_with('*') || trimmed.starts_with('/') {
        return None;
    }
    let bytes = line.as_bytes();
    let fixed_format = bytes.len() >= 7
        && bytes[..6].iter().all(|b| b.is_ascii_digit() || *b == b' ')
        && matches!(bytes[6], b' ' | b'-' | b'*' | b'/' | b'D' | b'd');
    if !fixed_format {
        return Some(line);
    }
    if matches!(bytes[6], b'*' | b'/') {
        return None;
    }
    // Columns 73-80 are the identification area and are not code.
    let end = line.len().min(72);
    Some(&line[6..end])
}

/// One parsed data-description item, before the tree is flattened.
struct Item {
    /// Level number.
    level: u8,
    /// Item name, absent for anonymous items.
    name: Option<String>,
    /// Picture clause, absent for group items.
    picture: Option<String>,
    /// Declared usage.
    usage: Usage,
    /// `OCCURS` count, absent when the item is not a table.
    occurs: Option<u32>,
}

/// The storage usages the compiler recognizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Usage {
    /// `USAGE DISPLAY`, the default.
    Display,
    /// `COMP-3` / `PACKED-DECIMAL`.
    Packed,
    /// `COMP` / `COMP-4` / `BINARY`.
    Binary,
}

/// Clause keywords that introduce a usage.
fn usage_of(token: &str) -> Option<Usage> {
    match token {
        "COMP" | "COMPUTATIONAL" | "COMP-4" | "COMPUTATIONAL-4" | "BINARY" => Some(Usage::Binary),
        "COMP-3" | "COMPUTATIONAL-3" | "PACKED-DECIMAL" => Some(Usage::Packed),
        "DISPLAY" => Some(Usage::Display),
        _ => None,
    }
}

/// Clauses the compiler refuses, with the reason it refuses them.
fn refused(token: &str) -> Option<&'static str> {
    match token {
        "REDEFINES" => Some(
            "REDEFINES gives one run of bytes two layouts; send the layout you \
                             actually want as ParseOptions.layout",
        ),
        "RENAMES" => Some("level-66 RENAMES regroups fields that are already described"),
        "COMP-1" | "COMPUTATIONAL-1" | "COMP-2" | "COMPUTATIONAL-2" => Some(
            "COMP-1 and COMP-2 are hexadecimal floating point, which this build does not \
                  decode",
        ),
        "COMP-5" | "COMPUTATIONAL-5" => Some(
            "COMP-5 is native-endian binary, so its bytes depend on the machine that wrote \
                  them; declare the field as COMP if it is big-endian",
        ),
        "SYNCHRONIZED" | "SYNC" => Some(
            "SYNCHRONIZED inserts compiler-chosen slack bytes, so the offsets in the \
                  copybook are no longer the offsets in the file",
        ),
        "SIGN" => Some(
            "an explicit SIGN clause moves or separates the sign; only the default \
                        trailing overpunch is decoded",
        ),
        "DEPENDING" => Some(
            "OCCURS DEPENDING ON makes the record variable-length in a way the \
                             layout cannot describe",
        ),
        "POINTER" | "INDEX" | "PROCEDURE-POINTER" | "FUNCTION-POINTER" => {
            Some("pointer and index items hold machine addresses, not data")
        }
        _ => None,
    }
}

/// Clauses that describe something other than the bytes, and are ignored.
fn ignorable(token: &str) -> bool {
    matches!(
        token,
        "VALUE"
            | "VALUES"
            | "JUSTIFIED"
            | "JUST"
            | "RIGHT"
            | "LEFT"
            | "GLOBAL"
            | "EXTERNAL"
            | "BLANK"
            | "WHEN"
            | "ZERO"
            | "ZEROS"
            | "ZEROES"
            | "IS"
            | "ARE"
            | "TIMES"
            | "CHARACTERS"
    )
}

/// Parse one statement into an item, or `None` when it declares no storage.
///
/// One long match rather than a clause table: every arm either sets a field or
/// refuses, and splitting it would scatter the refusals away from the clauses
/// they refuse.
#[allow(clippy::too_many_lines)]
fn parse_item(statement: &Statement) -> Result<Option<Item>, ParseError> {
    let tokens: Vec<String> = statement.iter().map(|token| token.to_uppercase()).collect();
    let level: u8 = tokens[0].parse().map_err(|_| {
        ParseError::invalid(format!(
            "expected a level number at the start of {:?}, found {:?}",
            statement.join(" "),
            statement[0]
        ))
    })?;
    match level {
        88 => return Ok(None),
        66 => {
            return Err(ParseError::unsupported(
                "level-66 RENAMES regroups fields that are already described",
            ));
        }
        1..=49 | 77 => {}
        other => {
            return Err(ParseError::invalid(format!(
                "level {other} is not a data-description level number"
            )));
        }
    }

    let mut index = 1;
    let mut name = None;
    if let Some(token) = tokens.get(index)
        && !is_clause_keyword(token)
    {
        if token != "FILLER" {
            name = Some(statement[index].clone());
        }
        index += 1;
    }

    let mut picture = None;
    let mut usage = None;
    let mut occurs = None;
    while index < tokens.len() {
        let token = tokens[index].as_str();
        if let Some(reason) = refused(token) {
            return Err(ParseError::unsupported(format!(
                "copybook item {:?} uses {token}: {reason}",
                name.as_deref().unwrap_or("FILLER")
            )));
        }
        match token {
            "PIC" | "PICTURE" => {
                index += 1;
                if tokens.get(index).map(String::as_str) == Some("IS") {
                    index += 1;
                }
                let Some(clause) = tokens.get(index) else {
                    return Err(ParseError::invalid(format!(
                        "PICTURE clause of {:?} has no picture string",
                        name.as_deref().unwrap_or("FILLER")
                    )));
                };
                picture = Some(clause.clone());
                index += 1;
            }
            "USAGE" => {
                index += 1;
                if tokens.get(index).map(String::as_str) == Some("IS") {
                    index += 1;
                }
                let Some(clause) = tokens.get(index) else {
                    return Err(ParseError::invalid(format!(
                        "USAGE clause of {:?} names no usage",
                        name.as_deref().unwrap_or("FILLER")
                    )));
                };
                if let Some(reason) = refused(clause) {
                    return Err(ParseError::unsupported(format!(
                        "copybook item {:?} uses {clause}: {reason}",
                        name.as_deref().unwrap_or("FILLER")
                    )));
                }
                usage = Some(usage_of(clause).ok_or_else(|| {
                    ParseError::unsupported(format!(
                        "USAGE {clause} is not a usage this build \
                                                     decodes"
                    ))
                })?);
                index += 1;
            }
            "OCCURS" => {
                index += 1;
                let Some(count) = tokens.get(index).and_then(|t| t.parse::<u32>().ok()) else {
                    return Err(ParseError::invalid(format!(
                        "OCCURS clause of {:?} has no repeat count",
                        name.as_deref().unwrap_or("FILLER")
                    )));
                };
                if count == 0 || count > MAX_OCCURS {
                    return Err(ParseError::invalid(format!(
                        "OCCURS {count} is outside the supported range 1..={MAX_OCCURS}"
                    )));
                }
                occurs = Some(count);
                index += 1;
            }
            other if usage_of(other).is_some() => {
                usage = usage_of(other);
                index += 1;
            }
            other if ignorable(other) => {
                // `VALUE` swallows its literal, which may be several tokens
                // for a figurative constant such as `ALL SPACES`.
                if other == "VALUE" || other == "VALUES" {
                    index = tokens.len();
                } else {
                    index += 1;
                }
            }
            other => {
                return Err(ParseError::invalid(format!(
                    "unrecognized clause {other:?} in copybook item {:?}",
                    name.as_deref().unwrap_or("FILLER")
                )));
            }
        }
    }

    Ok(Some(Item {
        level,
        name,
        picture,
        usage: usage.unwrap_or(Usage::Display),
        occurs,
    }))
}

/// Whether a token in the name position is really the start of a clause.
fn is_clause_keyword(token: &str) -> bool {
    matches!(token, "PIC" | "PICTURE" | "USAGE" | "OCCURS" | "REDEFINES")
        || usage_of(token).is_some()
        || ignorable(token)
        || refused(token).is_some()
}

/// Walk the level tree and emit the elementary fields in physical order.
fn flatten(items: &[Item], out: &mut Vec<RawField>) -> Result<(), ParseError> {
    for (index, item) in items.iter().enumerate() {
        let is_group = items
            .get(index + 1)
            .is_some_and(|next| next.level > item.level && item.level != 77);
        if is_group {
            if item.picture.is_some() {
                return Err(ParseError::invalid(format!(
                    "item {:?} has both a PICTURE clause and subordinate items",
                    item.name.as_deref().unwrap_or("FILLER")
                )));
            }
            if item.occurs.is_some() {
                return Err(ParseError::unsupported(format!(
                    "OCCURS on the group item {:?} is not supported; put it on the elementary \
                     items instead",
                    item.name.as_deref().unwrap_or("FILLER")
                )));
            }
            // Children are emitted by their own iteration; a group contributes
            // no bytes of its own.
            continue;
        }
        let Some(picture) = item.picture.as_deref() else {
            return Err(ParseError::invalid(format!(
                "elementary item {:?} has no PICTURE clause",
                item.name.as_deref().unwrap_or("FILLER")
            )));
        };
        let described = describe(picture, item.usage, item.name.as_deref())?;
        match item.occurs {
            None => out.push(RawField {
                name: item.name.clone().unwrap_or_default(),
                size: described.size,
                kind: if item.name.is_none() {
                    FieldKind::Skip
                } else {
                    described.kind
                },
                scale: described.scale,
                picture: picture.to_string(),
                offset: None,
            }),
            Some(count) => {
                for occurrence in 1..=count {
                    out.push(RawField {
                        name: item
                            .name
                            .as_ref()
                            .map(|name| format!("{name}({occurrence})"))
                            .unwrap_or_default(),
                        size: described.size,
                        kind: if item.name.is_none() {
                            FieldKind::Skip
                        } else {
                            described.kind
                        },
                        scale: described.scale,
                        picture: picture.to_string(),
                        offset: None,
                    });
                }
            }
        }
    }
    Ok(())
}

/// What a picture clause and a usage together say about the bytes.
struct Described {
    /// Decoded type.
    kind: FieldKind,
    /// Width in bytes.
    size: u32,
    /// Implied decimal digits.
    scale: u32,
}

/// Expand a picture clause into its symbol run, resolving `(n)` repeats.
fn expand(picture: &str, name: &str) -> Result<Vec<char>, ParseError> {
    let mut out = Vec::new();
    let mut chars = picture.chars().peekable();
    while let Some(symbol) = chars.next() {
        if symbol == '(' {
            return Err(ParseError::invalid(format!(
                "picture {picture:?} of {name:?} starts a repeat count with nothing to repeat"
            )));
        }
        let mut count = 1usize;
        if chars.peek() == Some(&'(') {
            chars.next();
            let mut digits = String::new();
            let mut closed = false;
            for inner in chars.by_ref() {
                if inner == ')' {
                    closed = true;
                    break;
                }
                digits.push(inner);
            }
            if !closed || digits.is_empty() {
                return Err(ParseError::invalid(format!(
                    "picture {picture:?} of {name:?} has an unterminated repeat count"
                )));
            }
            count = digits.parse().map_err(|_| {
                ParseError::invalid(format!(
                    "picture {picture:?} of {name:?} has a non-numeric repeat count {digits:?}"
                ))
            })?;
            if count == 0 || count > MAX_RECORD_SYMBOLS {
                return Err(ParseError::invalid(format!(
                    "picture {picture:?} of {name:?} repeats {count} times, outside \
                     1..={MAX_RECORD_SYMBOLS}"
                )));
            }
        }
        out.extend(std::iter::repeat_n(symbol, count));
        if out.len() > MAX_RECORD_SYMBOLS {
            return Err(ParseError::invalid(format!(
                "picture {picture:?} of {name:?} describes more than {MAX_RECORD_SYMBOLS} \
                 character positions"
            )));
        }
    }
    Ok(out)
}

/// Largest number of character positions one picture clause may describe.
const MAX_RECORD_SYMBOLS: usize = 65_536;

/// Resolve a picture clause and a usage into a field description.
fn describe(picture: &str, usage: Usage, name: Option<&str>) -> Result<Described, ParseError> {
    let label = name.unwrap_or("FILLER");
    let symbols = expand(picture, label)?;
    if symbols.is_empty() {
        return Err(ParseError::invalid(format!(
            "picture clause of {label:?} is empty"
        )));
    }

    // One `X` or `A` anywhere makes the whole item character data, however
    // many `9`s it also contains.
    if symbols.iter().any(|c| matches!(c, 'X' | 'A')) {
        return describe_character(&symbols, picture, usage, label);
    }
    describe_numeric(&symbols, picture, usage, label)
}

/// Resolve a character picture clause.
fn describe_character(
    symbols: &[char],
    picture: &str,
    usage: Usage,
    label: &str,
) -> Result<Described, ParseError> {
    if symbols.iter().any(|c| matches!(c, 'S' | 'V' | 'P')) {
        return Err(ParseError::invalid(format!(
            "picture {picture:?} of {label:?} mixes character symbols with the numeric \
             symbols S, V or P"
        )));
    }
    if let Some(bad) = symbols.iter().find(|c| !matches!(c, 'X' | 'A' | '9')) {
        return Err(ParseError::unsupported(format!(
            "picture {picture:?} of {label:?} uses the editing symbol {bad:?}; \
             numeric-edited and alphanumeric-edited items are not decoded"
        )));
    }
    if usage != Usage::Display {
        return Err(ParseError::invalid(format!(
            "{label:?} is a character item, so it cannot have a computational usage"
        )));
    }
    Ok(Described {
        kind: FieldKind::Text,
        size: u32::try_from(symbols.len()).unwrap_or(u32::MAX),
        scale: 0,
    })
}

/// Resolve a numeric picture clause together with its usage.
///
/// Only `S`, `9` and `V` describe stored digits; every other symbol belongs to
/// an edited picture, which describes how a number is *printed* rather than how
/// it is stored, and is therefore refused rather than guessed at.
fn describe_numeric(
    symbols: &[char],
    picture: &str,
    usage: Usage,
    label: &str,
) -> Result<Described, ParseError> {
    let mut digits: u32 = 0;
    let mut scale: u32 = 0;
    let mut seen_point = false;
    let mut signed = false;
    for (position, symbol) in symbols.iter().enumerate() {
        match symbol {
            'S' => {
                if position != 0 {
                    return Err(ParseError::invalid(format!(
                        "picture {picture:?} of {label:?} puts S somewhere other than the front"
                    )));
                }
                signed = true;
            }
            'V' => {
                if seen_point {
                    return Err(ParseError::invalid(format!(
                        "picture {picture:?} of {label:?} has more than one implied decimal point"
                    )));
                }
                seen_point = true;
            }
            '9' => {
                digits += 1;
                if seen_point {
                    scale += 1;
                }
            }
            other => {
                return Err(ParseError::unsupported(format!(
                    "picture {picture:?} of {label:?} uses the symbol {other:?}; only S, 9 and V \
                     describe stored digits, everything else is an edited picture this build does \
                     not decode"
                )));
            }
        }
    }
    if digits == 0 {
        return Err(ParseError::invalid(format!(
            "picture {picture:?} of {label:?} declares no digit positions"
        )));
    }

    match usage {
        Usage::Display => Ok(Described {
            kind: FieldKind::ZonedDecimal,
            size: digits,
            scale,
        }),
        Usage::Packed => Ok(Described {
            kind: FieldKind::PackedDecimal,
            size: digits / 2 + 1,
            scale,
        }),
        Usage::Binary => {
            let size = match digits {
                1..=4 => 2,
                5..=9 => 4,
                10..=18 => 8,
                other => {
                    return Err(ParseError::unsupported(format!(
                        "{label:?} is a binary item of {other} digits; COBOL binary stops at 18"
                    )));
                }
            };
            let kind = if signed {
                FieldKind::Integer
            } else {
                FieldKind::UnsignedInteger
            };
            Ok(Described { kind, size, scale })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::compile;
    use crate::error::ParseError;
    use crate::layout::FieldKind;

    /// The worked example from the README, in fixed-format columns.
    const CUSTOMER: &str = "\
      * Customer master record.
       01  CUSTOMER-RECORD.
           05  CUST-ID              PIC 9(6).
           05  CUST-NAME            PIC X(20).
           05  CUST-BALANCE         PIC S9(7)V99 COMP-3.
           05  CUST-ORDER-COUNT     PIC S9(4) COMP.
           05  FILLER               PIC X(4).
";

    #[test]
    fn the_worked_example_compiles_to_the_expected_bytes() {
        let layout = compile(CUSTOMER).expect("the example compiles");
        assert_eq!(layout.records.len(), 1);
        let record = &layout.records[0];
        assert_eq!(record.name, "CUSTOMER-RECORD");
        let shape: Vec<_> = record
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.size, f.kind, f.scale))
            .collect();
        assert_eq!(
            shape,
            vec![
                ("CUST-ID", 6, FieldKind::ZonedDecimal, 0),
                ("CUST-NAME", 20, FieldKind::Text, 0),
                ("CUST-BALANCE", 5, FieldKind::PackedDecimal, 2),
                ("CUST-ORDER-COUNT", 2, FieldKind::Integer, 0),
                ("", 4, FieldKind::Skip, 0),
            ]
        );
        assert_eq!(record.fields.iter().map(|f| f.size).sum::<u32>(), 37);
    }

    #[test]
    fn free_format_source_compiles_the_same_way() {
        let free = "01 CUSTOMER-RECORD.\n\
                    05 CUST-ID PIC 9(6).\n\
                    05 CUST-NAME PIC X(20).\n\
                    05 CUST-BALANCE PIC S9(7)V99 COMP-3.\n\
                    05 CUST-ORDER-COUNT PIC S9(4) COMP.\n\
                    05 FILLER PIC X(4).\n";
        assert_eq!(
            compile(free).unwrap().records[0]
                .fields
                .iter()
                .map(|f| (f.name.clone(), f.size))
                .collect::<Vec<_>>(),
            compile(CUSTOMER).unwrap().records[0]
                .fields
                .iter()
                .map(|f| (f.name.clone(), f.size))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn group_items_are_flattened_and_contribute_no_bytes_of_their_own() {
        let source = "01 REC.\n\
                      05 ADDRESS.\n\
                      10 STREET PIC X(20).\n\
                      10 CITY PIC X(10).\n\
                      05 ZIP PIC 9(5).\n";
        let record = &compile(source).unwrap().records[0];
        assert_eq!(
            record
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.size))
                .collect::<Vec<_>>(),
            vec![("STREET", 20), ("CITY", 10), ("ZIP", 5)]
        );
    }

    #[test]
    fn occurs_expands_to_indexed_names() {
        let record = &compile("01 REC.\n05 MONTH-TOTAL PIC S9(5) COMP-3 OCCURS 3 TIMES.\n")
            .unwrap()
            .records[0];
        assert_eq!(
            record
                .fields
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>(),
            vec!["MONTH-TOTAL(1)", "MONTH-TOTAL(2)", "MONTH-TOTAL(3)"]
        );
        assert!(record.fields.iter().all(|f| f.size == 3));
    }

    #[test]
    fn binary_widths_follow_the_digit_count() {
        let record = &compile(
            "01 REC.\n\
             05 A PIC S9(4) COMP.\n\
             05 B PIC 9(9) COMP.\n\
             05 C PIC S9(18) BINARY.\n",
        )
        .unwrap()
        .records[0];
        assert_eq!(
            record
                .fields
                .iter()
                .map(|f| (f.size, f.kind))
                .collect::<Vec<_>>(),
            vec![
                (2, FieldKind::Integer),
                (4, FieldKind::UnsignedInteger),
                (8, FieldKind::Integer),
            ]
        );
    }

    #[test]
    fn packed_width_is_half_the_digits_plus_the_sign_nibble() {
        for (digits, bytes) in [
            (1u32, 1u32),
            (2, 2),
            (3, 2),
            (4, 3),
            (7, 4),
            (9, 5),
            (18, 10),
        ] {
            let source = format!("01 REC.\n05 N PIC S9({digits}) COMP-3.\n");
            assert_eq!(
                compile(&source).unwrap().records[0].fields[0].size,
                bytes,
                "{digits}"
            );
        }
    }

    #[test]
    fn level_88_condition_names_declare_no_storage() {
        let record = &compile(
            "01 REC.\n\
             05 STATUS-CODE PIC X.\n\
             88 STATUS-OPEN VALUE 'O'.\n\
             88 STATUS-CLOSED VALUE 'C'.\n\
             05 REST PIC X(4).\n",
        )
        .unwrap()
        .records[0];
        assert_eq!(
            record
                .fields
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>(),
            ["STATUS-CODE", "REST"]
        );
    }

    #[test]
    fn a_value_literal_containing_a_period_does_not_end_the_statement() {
        let record = &compile("01 REC.\n05 F PIC X(4) VALUE 'A.B '.\n05 G PIC X(2).\n")
            .unwrap()
            .records[0];
        assert_eq!(record.fields.len(), 2);
    }

    #[test]
    fn unsupported_clauses_are_unimplemented_and_name_themselves() {
        for (source, needle) in [
            (
                "01 REC.\n05 A PIC X(4).\n05 B REDEFINES A PIC 9(4).\n",
                "REDEFINES",
            ),
            ("01 REC.\n05 A COMP-1.\n", "COMP-1"),
            ("01 REC.\n05 A PIC S9(4) COMP-5.\n", "COMP-5"),
            ("01 REC.\n05 A PIC S9(4) COMP SYNC.\n", "SYNCHRONIZED"),
            (
                "01 REC.\n05 A PIC S9(4) SIGN IS LEADING SEPARATE.\n",
                "SIGN",
            ),
            (
                "01 REC.\n05 N PIC 9(3).\n05 T PIC X(2) OCCURS 3 DEPENDING ON N.\n",
                "DEPENDING",
            ),
            ("01 REC.\n05 A PIC ZZ9.99.\n", "edited picture"),
            ("01 REC.\n05 A PIC XXBXX.\n", "editing symbol"),
            ("01 REC.\n05 A PIC 9(3)PPP.\n", "symbol 'P'"),
            ("66 A RENAMES B.\n", "RENAMES"),
        ] {
            let err = compile(source).unwrap_err();
            assert!(
                matches!(&err, ParseError::Unsupported(m) if m.contains(needle)),
                "{source:?} should be UNIMPLEMENTED mentioning {needle}, got {err:?}"
            );
        }
    }

    #[test]
    fn malformed_source_is_invalid_argument() {
        for source in [
            "",
            "this is not a copybook",
            "01 REC.\n05 A PIC.\n",
            "01 REC.\n05 A PIC X(.\n",
            "01 REC.\n05 A PIC X(4)\n",
            "01 REC.\n05 A PIC S9(4)V9V9.\n",
            "05 A PIC X(4).\n",
            "01 REC.\n05 A PIC XV9.\n",
            "01 REC.\n05 A.\n",
        ] {
            let err = compile(source).unwrap_err();
            assert!(
                matches!(err, ParseError::Invalid(_)),
                "{source:?} should be INVALID_ARGUMENT, got {err:?}"
            );
        }
    }

    #[test]
    fn two_record_descriptions_need_the_layout_message_instead() {
        let err = compile("01 A.\n05 F PIC X(2).\n01 B.\n05 G PIC X(2).\n").unwrap_err();
        assert!(
            matches!(&err, ParseError::Unsupported(m) if m.contains("record_type_field")),
            "got {err:?}"
        );
    }

    #[test]
    fn the_identification_area_past_column_72_is_not_code() {
        // Columns 73-80 carry a program name on real punched-card source.
        let padded = format!("{:<72}{}", "       01  REC.", "IGNOREME");
        let line = format!("{padded}\n{:<72}\n", "           05  F  PIC X(3).");
        let record = &compile(&line).unwrap().records[0];
        assert_eq!(record.name, "REC");
        assert_eq!(record.fields.len(), 1);
    }
}
