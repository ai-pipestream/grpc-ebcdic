// SPDX-License-Identifier: Apache-2.0

//! EBCDIC code pages: byte-to-Unicode decoding, and the reverse map the tests
//! and the worked README example use to author record bytes.
//!
//! There is no `encoding_rs` support for EBCDIC — it implements the encodings
//! the WHATWG spec lists, and none of them are EBCDIC — so the tables in
//! `src/codepages.rs` are the whole implementation. They are lifted from the
//! Python standard library codecs of the same name, which is what Docling's
//! `EbcdicDocumentBackend` decodes with, so the two agree byte for byte.

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::codepages;

/// Code page used when a request leaves `encoding` empty.
pub const DEFAULT_ENCODING: &str = "cp037";

/// One supported code page: its canonical name and its 256-entry table.
#[derive(Debug)]
struct CodePage {
    /// Canonical name, as echoed back in `LayoutInfo.encoding`.
    name: &'static str,
    /// Byte-to-scalar table; `codepages::UNDEFINED` marks an unassigned byte.
    table: &'static [u16; 256],
}

/// Every code page this build can decode, in the order `GetServiceInfo`
/// reports them.
static CODE_PAGES: &[CodePage] = &[
    CodePage {
        name: "cp037",
        table: &codepages::CP037,
    },
    CodePage {
        name: "cp273",
        table: &codepages::CP273,
    },
    CodePage {
        name: "cp424",
        table: &codepages::CP424,
    },
    CodePage {
        name: "cp500",
        table: &codepages::CP500,
    },
    CodePage {
        name: "cp875",
        table: &codepages::CP875,
    },
    CodePage {
        name: "cp1026",
        table: &codepages::CP1026,
    },
    CodePage {
        name: "cp1140",
        table: &codepages::CP1140,
    },
];

/// Aliases accepted for a code page, beyond its canonical name.
///
/// The names people actually put in a config file: the IBM CCSID with and
/// without a prefix, and the IANA `ebcdic-cp-*` spellings. Lookup normalizes
/// away case, spaces, hyphens, and underscores first, so only genuinely
/// different spellings need an entry.
static ALIASES: &[(&str, &str)] = &[
    ("037", "cp037"),
    ("ibm037", "cp037"),
    ("ibm-037", "cp037"),
    ("ebcdiccpus", "cp037"),
    ("ebcdiccpca", "cp037"),
    ("ebcdiccpwt", "cp037"),
    ("ebcdiccpnl", "cp037"),
    ("csibm037", "cp037"),
    ("273", "cp273"),
    ("ibm273", "cp273"),
    ("csibm273", "cp273"),
    ("424", "cp424"),
    ("ibm424", "cp424"),
    ("ebcdiccphe", "cp424"),
    ("csibm424", "cp424"),
    ("500", "cp500"),
    ("ibm500", "cp500"),
    ("ebcdiccpbe", "cp500"),
    ("ebcdiccpch", "cp500"),
    ("csibm500", "cp500"),
    ("875", "cp875"),
    ("ibm875", "cp875"),
    ("ebcdicgreek", "cp875"),
    ("1026", "cp1026"),
    ("ibm1026", "cp1026"),
    ("csibm1026", "cp1026"),
    ("1140", "cp1140"),
    ("ibm1140", "cp1140"),
    ("ebcdicuscanadaeuro", "cp1140"),
];

/// A resolved code page, ready to decode field bytes.
#[derive(Clone, Copy, Debug)]
pub struct Codec {
    /// The code page this codec decodes with.
    page: &'static CodePage,
}

/// Why a code page name or a run of bytes could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// The requested code page is not one this build carries.
    UnknownEncoding {
        /// The name exactly as the caller spelled it.
        requested: String,
    },
    /// A byte the code page leaves unassigned.
    UndefinedByte {
        /// Canonical name of the code page.
        encoding: &'static str,
        /// The offending byte.
        byte: u8,
        /// Its index within the field's bytes.
        index: usize,
    },
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownEncoding { requested } => write!(
                f,
                "unknown EBCDIC code page {requested:?}; this build carries {}",
                supported_encodings().join(", ")
            ),
            Self::UndefinedByte {
                encoding,
                byte,
                index,
            } => write!(
                f,
                "byte 0x{byte:02x} at index {index} is unassigned in {encoding}"
            ),
        }
    }
}

impl std::error::Error for CodecError {}

/// Canonical names of every code page this build can decode.
#[must_use]
pub fn supported_encodings() -> Vec<&'static str> {
    CODE_PAGES.iter().map(|page| page.name).collect()
}

/// Normalize a code page name for lookup: lowercase, and without the spacing
/// characters people sprinkle through CCSID spellings.
fn normalize(name: &str) -> String {
    name.chars()
        .filter(|c| !matches!(c, '-' | '_' | ' ' | '.'))
        .flat_map(char::to_lowercase)
        .collect()
}

impl Codec {
    /// Resolve a code page by name. An empty name selects [`DEFAULT_ENCODING`].
    ///
    /// # Errors
    ///
    /// [`CodecError::UnknownEncoding`] when the name matches no code page in
    /// this build.
    pub fn resolve(name: &str) -> Result<Self, CodecError> {
        let wanted = if name.trim().is_empty() {
            DEFAULT_ENCODING
        } else {
            name.trim()
        };
        let key = normalize(wanted);
        let canonical = ALIASES
            .iter()
            .find(|(alias, _)| *alias == key)
            .map_or(key.as_str(), |(_, canonical)| *canonical);
        CODE_PAGES
            .iter()
            .find(|page| page.name == canonical)
            .map(|page| Self { page })
            .ok_or_else(|| CodecError::UnknownEncoding {
                requested: wanted.to_string(),
            })
    }

    /// Canonical name of the resolved code page.
    #[must_use]
    pub const fn name(self) -> &'static str {
        self.page.name
    }

    /// Decode field bytes into a Rust string.
    ///
    /// Strict, like Python's codecs and therefore like Docling: an unassigned
    /// byte is an error rather than a replacement character, because silently
    /// substituting U+FFFD into a mainframe account number is worse than
    /// refusing the record.
    ///
    /// # Errors
    ///
    /// [`CodecError::UndefinedByte`] on the first byte the code page does not
    /// assign.
    pub fn decode(self, bytes: &[u8]) -> Result<String, CodecError> {
        let mut out = String::with_capacity(bytes.len());
        for (index, &byte) in bytes.iter().enumerate() {
            let scalar = self.page.table[byte as usize];
            if scalar == codepages::UNDEFINED {
                return Err(CodecError::UndefinedByte {
                    encoding: self.page.name,
                    byte,
                    index,
                });
            }
            // Every table entry is a BMP scalar taken from a Python codec, so
            // it is never a surrogate and the conversion cannot fail.
            out.push(char::from_u32(u32::from(scalar)).unwrap_or(char::REPLACEMENT_CHARACTER));
        }
        Ok(out)
    }

    /// Encode text back into this code page.
    ///
    /// Not on the decode path: the server never encodes anything, because
    /// record selectors are compared against *decoded* text exactly as Docling
    /// compares them. This exists so tests and the README example can author
    /// EBCDIC record bytes from readable literals instead of committing binary
    /// fixtures.
    ///
    /// # Errors
    ///
    /// Returns the first character with no representation in this code page.
    pub fn encode(self, text: &str) -> Result<Vec<u8>, char> {
        let reverse = self.reverse_map();
        text.chars()
            .map(|ch| {
                u16::try_from(u32::from(ch))
                    .ok()
                    .and_then(|scalar| reverse.get(&scalar).copied())
                    .ok_or(ch)
            })
            .collect()
    }

    /// The lazily built scalar-to-byte map for this code page.
    ///
    /// Built once per code page and cached. Where several bytes decode to the
    /// same scalar the lowest byte wins, which is arbitrary but stable.
    fn reverse_map(self) -> &'static HashMap<u16, u8> {
        static MAPS: OnceLock<Vec<HashMap<u16, u8>>> = OnceLock::new();
        let maps = MAPS.get_or_init(|| {
            CODE_PAGES
                .iter()
                .map(|page| {
                    let mut map = HashMap::with_capacity(256);
                    for (byte, &scalar) in page.table.iter().enumerate() {
                        if scalar != codepages::UNDEFINED {
                            map.entry(scalar)
                                .or_insert_with(|| u8::try_from(byte).unwrap_or(0));
                        }
                    }
                    map
                })
                .collect()
        });
        let index = CODE_PAGES
            .iter()
            .position(|page| std::ptr::eq(page, self.page))
            .unwrap_or(0);
        &maps[index]
    }
}

#[cfg(test)]
mod tests {
    use super::{Codec, CodecError};

    #[test]
    fn cp037_maps_the_invariant_characters() {
        let codec = Codec::resolve("cp037").expect("cp037 is carried");
        // The four anchors every EBCDIC layout depends on: space, the digits,
        // and both letter runs, which are not contiguous the way ASCII's are.
        assert_eq!(codec.decode(&[0x40]).unwrap(), " ");
        assert_eq!(codec.decode(&[0xf0, 0xf5, 0xf9]).unwrap(), "059");
        assert_eq!(codec.decode(&[0xc1, 0xc9, 0xd1, 0xe2]).unwrap(), "AIJS");
        assert_eq!(codec.decode(&[0x81, 0x89, 0x91, 0xa2]).unwrap(), "aijs");
    }

    #[test]
    fn cp037_and_cp500_disagree_where_they_are_known_to() {
        // The two code pages shuffle the same seven punctuation marks around
        // seven byte positions. Decoding with the wrong one of the pair
        // produces text that looks plausible and is wrong, which is why the
        // encoding is a request option and never a guess.
        let us = Codec::resolve("cp037").unwrap();
        let international = Codec::resolve("cp500").unwrap();
        for (byte, in_us, in_international) in [
            (0x4a_u8, "\u{a2}", "["),
            (0x4f, "|", "!"),
            (0x5a, "!", "]"),
            (0x5f, "\u{ac}", "^"),
            (0xb0, "^", "\u{a2}"),
            (0xba, "[", "\u{ac}"),
            (0xbb, "]", "|"),
        ] {
            assert_eq!(us.decode(&[byte]).unwrap(), in_us, "cp037 0x{byte:02x}");
            assert_eq!(
                international.decode(&[byte]).unwrap(),
                in_international,
                "cp500 0x{byte:02x}"
            );
        }
    }

    #[test]
    fn cp1140_is_cp037_with_a_euro() {
        let legacy = Codec::resolve("cp037").unwrap();
        let euro = Codec::resolve("cp1140").unwrap();
        assert_eq!(legacy.decode(&[0x9f]).unwrap(), "\u{a4}");
        assert_eq!(euro.decode(&[0x9f]).unwrap(), "\u{20ac}");
        // And identical everywhere else.
        for byte in 0u8..=255 {
            if byte != 0x9f {
                assert_eq!(
                    legacy.decode(&[byte]),
                    euro.decode(&[byte]),
                    "byte 0x{byte:02x}"
                );
            }
        }
    }

    #[test]
    fn names_are_resolved_through_aliases_and_spacing() {
        for spelling in [
            "cp037",
            "CP037",
            "IBM-037",
            "ibm037",
            "037",
            "  cp037  ",
            "ebcdic-cp-us",
        ] {
            assert_eq!(
                Codec::resolve(spelling).unwrap().name(),
                "cp037",
                "{spelling}"
            );
        }
        assert_eq!(Codec::resolve("").unwrap().name(), "cp037");
    }

    #[test]
    fn an_unknown_code_page_is_named_in_the_error() {
        let err = Codec::resolve("utf-8").unwrap_err();
        assert!(matches!(&err, CodecError::UnknownEncoding { requested } if requested == "utf-8"));
        assert!(
            err.to_string().contains("cp1140"),
            "the error lists what is available"
        );
    }

    #[test]
    fn an_unassigned_byte_is_an_error_not_a_replacement_character() {
        // cp424 (Hebrew) leaves 38 bytes unassigned; cp037 assigns all 256.
        let hebrew = Codec::resolve("cp424").unwrap();
        let err = hebrew.decode(&[0xc1, 0x70]).unwrap_err();
        assert!(
            matches!(
                err,
                CodecError::UndefinedByte {
                    byte: 0x70,
                    index: 1,
                    ..
                }
            ),
            "{err:?}"
        );
        for byte in 0u8..=255 {
            assert!(
                Codec::resolve("cp037").unwrap().decode(&[byte]).is_ok(),
                "0x{byte:02x}"
            );
        }
    }

    #[test]
    fn encode_round_trips_through_decode() {
        for name in ["cp037", "cp500", "cp1140"] {
            let codec = Codec::resolve(name).unwrap();
            let text = "ACCT-0042 Jones, R.  ";
            let bytes = codec.encode(text).expect("all ASCII is representable");
            assert_eq!(codec.decode(&bytes).unwrap(), text, "{name}");
        }
    }

    #[test]
    fn encode_reports_the_character_it_cannot_represent() {
        let codec = Codec::resolve("cp037").unwrap();
        assert_eq!(codec.encode("\u{1f600}"), Err('\u{1f600}'));
    }
}
