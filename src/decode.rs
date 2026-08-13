// SPDX-License-Identifier: Apache-2.0

//! The byte walk: one function per COBOL storage usage.
//!
//! Everything here is total and allocation-free apart from the character
//! decoder, which has to build a `String`. Numerics are carried as `i128`
//! plus a scale rather than as a float: a COBOL `PIC S9(15)V99 COMP-3` holds
//! seventeen significant digits, an IEEE double holds fifteen, and a balance
//! that arrives off by a cent is worse than one that does not arrive.
//!
//! The rules match Docling's `_FieldDecoder` so the same bytes and the same
//! layout produce the same values through either implementation.

use crate::codec::Codec;
use crate::error::ParseError;

/// Sign nibbles that mean negative.
///
/// COBOL leaves the positive nibble implementation-defined: `0xc` is the
/// preferred positive sign, `0xf` marks an unsigned field, and `0xa` and `0xe`
/// are alternates. Only `0xb` and `0xd` are negative, so testing for negative
/// is the only test with a single answer.
const NEGATIVE_SIGNS: [u8; 2] = [0x0b, 0x0d];

/// Widest packed field the decoder accepts, in bytes.
///
/// Nineteen bytes hold thirty-seven digits, which is the most that fits an
/// `i128` for every value. Real copybooks stop at eighteen digits (COBOL-85)
/// or thirty-one (COBOL 2002), so this is not a limit anyone reaches by
/// accident.
pub const MAX_PACKED_BYTES: u32 = 19;

/// Widest zoned field the decoder accepts, in bytes, one digit per byte.
pub const MAX_ZONED_BYTES: u32 = 37;

/// Widest binary field the decoder accepts, in bytes.
pub const MAX_BINARY_BYTES: u32 = 16;

/// A decoded field value, before it is shaped into a protobuf cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldValue {
    /// Character data, already stripped and trimmed.
    Text(String),
    /// An exact base-10 number equal to `unscaled * 10^-scale`.
    Number {
        /// The value with the decimal point removed.
        unscaled: i128,
        /// Number of implied fractional digits.
        scale: u32,
    },
}

impl FieldValue {
    /// Render the value the way Docling's backend stringifies it into a table
    /// cell. Used for record selectors, which compare decoded text.
    #[must_use]
    pub fn to_display_string(&self) -> String {
        match self {
            Self::Text(text) => text.clone(),
            Self::Number { unscaled, scale } => render_decimal(*unscaled, *scale),
        }
    }
}

/// Render `unscaled * 10^-scale` as an exact decimal string.
///
/// Matches Python's `str(Decimal(n).scaleb(-scale))` for every scale a
/// copybook can express, which keeps our text identical to Docling's cell
/// text.
#[must_use]
pub fn render_decimal(unscaled: i128, scale: u32) -> String {
    if scale == 0 {
        return unscaled.to_string();
    }
    let negative = unscaled < 0;
    // `unsigned_abs` rather than `abs`, so i128::MIN does not panic.
    let digits = unscaled.unsigned_abs().to_string();
    let scale = scale as usize;
    let padded = if digits.len() <= scale {
        format!("{}{digits}", "0".repeat(scale + 1 - digits.len()))
    } else {
        digits
    };
    let split = padded.len() - scale;
    let sign = if negative { "-" } else { "" };
    format!("{sign}{}.{}", &padded[..split], &padded[split..])
}

/// Decode `USAGE DISPLAY` character data.
///
/// Control characters are dropped first when the option is on, then both ends
/// are trimmed, because EBCDIC fields are blank-padded to their declared width
/// and the padding is not part of the value. Both steps mirror Docling.
///
/// # Errors
///
/// [`ParseError::Invalid`] when the code page leaves one of the bytes
/// unassigned.
pub fn decode_text(codec: Codec, bytes: &[u8], strip_control: bool) -> Result<String, ParseError> {
    let text = codec.decode(bytes)?;
    let text = if strip_control {
        text.chars().filter(|c| !c.is_control()).collect::<String>()
    } else {
        text
    };
    Ok(text.trim().to_string())
}

/// Decode `COMP-3` packed decimal: two digits per byte, sign in the trailing
/// nibble.
///
/// # Errors
///
/// [`ParseError::Invalid`] when the field is empty or any digit nibble is
/// above nine. A nibble of `0xa`..`0xf` anywhere but the sign position is the
/// classic symptom of a layout whose offsets have drifted, so it is reported
/// with the nibble's position rather than as a generic parse failure.
pub fn decode_packed(bytes: &[u8]) -> Result<i128, ParseError> {
    if bytes.is_empty() {
        return Err(ParseError::invalid(
            "a packed decimal field needs at least one byte",
        ));
    }
    let mut value: i128 = 0;
    let last = bytes.len() - 1;
    for (index, &byte) in bytes.iter().enumerate() {
        let high = byte >> 4;
        let low = byte & 0x0f;
        for (nibble_index, nibble) in [(index * 2, high), (index * 2 + 1, low)] {
            // The very last nibble is the sign, not a digit.
            if index == last && nibble_index == last * 2 + 1 {
                continue;
            }
            if nibble > 9 {
                return Err(ParseError::invalid(format!(
                    "packed decimal nibble {nibble_index} is 0x{nibble:x}, not a digit \
                     (bytes {})",
                    hex(bytes)
                )));
            }
            value = value * 10 + i128::from(nibble);
        }
    }
    let sign = bytes[last] & 0x0f;
    Ok(if NEGATIVE_SIGNS.contains(&sign) {
        -value
    } else {
        value
    })
}

/// Decode a zoned decimal (signed `USAGE DISPLAY` numeric): one digit in the
/// low nibble of every byte, the sign overpunched into the zone nibble of the
/// last byte.
///
/// The zone nibbles of the leading bytes are deliberately not checked. They
/// are `0xf` in well-formed EBCDIC numerics, but blank-filled fields arrive as
/// `0x40` and mainframe programs read those as zeros; rejecting them would
/// refuse files that COBOL itself accepts. The digit nibbles are checked,
/// which is where real corruption shows up.
///
/// # Errors
///
/// [`ParseError::Invalid`] when the field is empty or a low nibble is above
/// nine.
pub fn decode_zoned(bytes: &[u8]) -> Result<i128, ParseError> {
    if bytes.is_empty() {
        return Err(ParseError::invalid(
            "a zoned decimal field needs at least one byte",
        ));
    }
    let mut value: i128 = 0;
    for (index, &byte) in bytes.iter().enumerate() {
        let digit = byte & 0x0f;
        if digit > 9 {
            return Err(ParseError::invalid(format!(
                "zoned decimal digit {index} is 0x{digit:x}, not a digit (bytes {})",
                hex(bytes)
            )));
        }
        value = value * 10 + i128::from(digit);
    }
    let sign = bytes[bytes.len() - 1] >> 4;
    Ok(if NEGATIVE_SIGNS.contains(&sign) {
        -value
    } else {
        value
    })
}

/// Decode a big-endian binary field (`COMP` / `COMP-4` / `BINARY`).
///
/// # Errors
///
/// [`ParseError::Invalid`] when the field is empty, and
/// [`ParseError::Unsupported`] past [`MAX_BINARY_BYTES`].
pub fn decode_binary(bytes: &[u8], signed: bool) -> Result<i128, ParseError> {
    if bytes.is_empty() {
        return Err(ParseError::invalid(
            "a binary field needs at least one byte",
        ));
    }
    if bytes.len() > MAX_BINARY_BYTES as usize {
        return Err(ParseError::unsupported(format!(
            "binary fields wider than {MAX_BINARY_BYTES} bytes are not supported (got {})",
            bytes.len()
        )));
    }
    let negative = signed && (bytes[0] & 0x80) != 0;
    // Sign-extend by starting from -1 rather than 0, so the fold produces the
    // two's-complement value without a separate branch per byte.
    let mut value: i128 = if negative { -1 } else { 0 };
    for &byte in bytes {
        value = (value << 8) | i128::from(byte);
    }
    Ok(value)
}

/// Lowercase hex of a field's bytes, for error messages.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            // Writing into a String cannot fail.
            let _ = write!(out, "{byte:02x}");
            out
        })
}

#[cfg(test)]
mod tests {
    use super::{
        FieldValue, decode_binary, decode_packed, decode_text, decode_zoned, render_decimal,
    };
    use crate::codec::Codec;
    use crate::error::ParseError;

    #[test]
    fn packed_decimal_reads_both_signs_and_the_unsigned_nibble() {
        // 12345 with the preferred positive sign, the negative sign, and the
        // unsigned filler, all three of which appear in real files.
        assert_eq!(decode_packed(&[0x12, 0x34, 0x5c]).unwrap(), 12_345);
        assert_eq!(decode_packed(&[0x12, 0x34, 0x5d]).unwrap(), -12_345);
        assert_eq!(decode_packed(&[0x12, 0x34, 0x5f]).unwrap(), 12_345);
        // The alternates: 0xa and 0xe positive, 0xb negative.
        assert_eq!(decode_packed(&[0x00, 0x1a]).unwrap(), 1);
        assert_eq!(decode_packed(&[0x00, 0x1e]).unwrap(), 1);
        assert_eq!(decode_packed(&[0x00, 0x1b]).unwrap(), -1);
    }

    #[test]
    fn packed_decimal_rejects_a_bad_nibble_and_says_where() {
        // 0xa in a digit position, not the sign position.
        let err = decode_packed(&[0x1a, 0x34, 0x5c]).unwrap_err();
        let ParseError::Invalid(message) = &err else {
            panic!("a corrupt nibble is the caller's problem, got {err:?}");
        };
        assert!(message.contains("nibble 1"), "{message}");
        assert!(message.contains("0xa"), "{message}");
        assert!(
            message.contains("1a345c"),
            "the offending bytes are quoted: {message}"
        );
    }

    #[test]
    fn packed_decimal_zero_and_single_byte_forms() {
        assert_eq!(decode_packed(&[0x0c]).unwrap(), 0);
        assert_eq!(decode_packed(&[0x9d]).unwrap(), -9);
        assert_eq!(decode_packed(&[0x00, 0x00, 0x0d]).unwrap(), 0);
    }

    #[test]
    fn packed_decimal_holds_more_digits_than_a_double() {
        // 17 significant digits: an f64 loses the last two.
        let bytes = [0x12, 0x34, 0x56, 0x78, 0x90, 0x12, 0x34, 0x56, 0x7c];
        assert_eq!(decode_packed(&bytes).unwrap(), 12_345_678_901_234_567_i128);
    }

    #[test]
    fn zoned_decimal_reads_the_overpunched_sign() {
        // EBCDIC "12345" is f1 f2 f3 f4 f5; the negative form overpunches the
        // last zone to 0xd.
        assert_eq!(
            decode_zoned(&[0xf1, 0xf2, 0xf3, 0xf4, 0xf5]).unwrap(),
            12_345
        );
        assert_eq!(
            decode_zoned(&[0xf1, 0xf2, 0xf3, 0xf4, 0xd5]).unwrap(),
            -12_345
        );
        assert_eq!(
            decode_zoned(&[0xf1, 0xf2, 0xf3, 0xf4, 0xc5]).unwrap(),
            12_345
        );
    }

    #[test]
    fn zoned_decimal_rejects_a_non_decimal_digit() {
        let err = decode_zoned(&[0xf1, 0xfa]).unwrap_err();
        let ParseError::Invalid(message) = &err else {
            panic!("got {err:?}")
        };
        assert!(message.contains("digit 1"), "{message}");
    }

    #[test]
    fn zoned_decimal_reads_blank_padding_as_zero_like_cobol_does() {
        // 0x40 is the EBCDIC space; its low nibble is zero. A blank-filled
        // numeric is what an unwritten field looks like on tape.
        assert_eq!(decode_zoned(&[0x40, 0x40, 0xf7]).unwrap(), 7);
    }

    #[test]
    fn binary_is_big_endian_and_sign_aware() {
        assert_eq!(decode_binary(&[0x00, 0x2a], true).unwrap(), 42);
        assert_eq!(decode_binary(&[0xff, 0xd6], true).unwrap(), -42);
        assert_eq!(decode_binary(&[0xff, 0xd6], false).unwrap(), 65_494);
        assert_eq!(
            decode_binary(&[0x7f, 0xff, 0xff, 0xff], true).unwrap(),
            2_147_483_647
        );
        assert_eq!(
            decode_binary(&[0x80, 0x00, 0x00, 0x00], true).unwrap(),
            -2_147_483_648
        );
        assert_eq!(decode_binary(&[0xff; 8], false).unwrap(), u64::MAX.into());
        assert_eq!(decode_binary(&[0xff; 8], true).unwrap(), -1);
    }

    #[test]
    fn binary_wider_than_the_decoder_is_unimplemented_not_invalid() {
        let err = decode_binary(&[0u8; 17], false).unwrap_err();
        assert!(matches!(err, ParseError::Unsupported(_)), "got {err:?}");
    }

    #[test]
    fn text_strips_control_characters_only_when_asked() {
        let codec = Codec::resolve("cp037").unwrap();
        // "AB", a shift-out control (0x0e), then "C", blank-padded.
        let bytes = [0xc1, 0xc2, 0x0e, 0xc3, 0x40, 0x40];
        assert_eq!(decode_text(codec, &bytes, true).unwrap(), "ABC");
        assert_eq!(decode_text(codec, &bytes, false).unwrap(), "AB\u{e}C");
    }

    #[test]
    fn text_is_trimmed_at_both_ends() {
        let codec = Codec::resolve("cp037").unwrap();
        let bytes = codec.encode("   padded   ").unwrap();
        assert_eq!(decode_text(codec, &bytes, true).unwrap(), "padded");
    }

    #[test]
    fn decimals_render_exactly_at_every_scale() {
        assert_eq!(render_decimal(12_345, 0), "12345");
        assert_eq!(render_decimal(12_345, 2), "123.45");
        assert_eq!(render_decimal(-12_345, 2), "-123.45");
        assert_eq!(render_decimal(5, 2), "0.05");
        assert_eq!(render_decimal(-5, 2), "-0.05");
        assert_eq!(render_decimal(0, 2), "0.00");
        assert_eq!(render_decimal(5, 6), "0.000005");
        assert_eq!(render_decimal(i128::MIN, 0), i128::MIN.to_string());
    }

    #[test]
    fn field_values_stringify_the_way_a_selector_compares_them() {
        assert_eq!(FieldValue::Text("A".into()).to_display_string(), "A");
        assert_eq!(
            FieldValue::Number {
                unscaled: 7,
                scale: 0
            }
            .to_display_string(),
            "7"
        );
        assert_eq!(
            FieldValue::Number {
                unscaled: 7,
                scale: 2
            }
            .to_display_string(),
            "0.07"
        );
    }
}
