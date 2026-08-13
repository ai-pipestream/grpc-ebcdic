// SPDX-License-Identifier: Apache-2.0

//! The one error type that crosses every internal boundary, and its single
//! mapping onto gRPC status codes.
//!
//! Keeping the mapping in one place is what makes the fleet's error contract
//! checkable: a reviewer reads [`ParseError`] and knows every code this
//! service can return and what earns it.

/// A failure that ends a parse stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// The request cannot be honored as written: no layout, two layouts, a
    /// malformed copybook, overlapping fields, a record length shorter than
    /// its own prefix, a bad COMP-3 nibble, a non-decimal zoned digit, or a
    /// byte the code page leaves unassigned. Becomes `INVALID_ARGUMENT`.
    Invalid(String),

    /// The request is well formed but names something this build does not
    /// implement: a copybook feature outside the supported subset, or a field
    /// wider than the decoders handle. Becomes `UNIMPLEMENTED`.
    Unsupported(String),

    /// A resource bound was hit: the byte cap, or the concurrent-parse cap.
    /// Becomes `RESOURCE_EXHAUSTED`.
    Exhausted(String),

    /// The server broke, not the request. Becomes `INTERNAL`.
    Internal(String),
}

impl ParseError {
    /// Build an [`Self::Invalid`] from anything printable.
    pub fn invalid(message: impl std::fmt::Display) -> Self {
        Self::Invalid(message.to_string())
    }

    /// Build an [`Self::Unsupported`] from anything printable.
    pub fn unsupported(message: impl std::fmt::Display) -> Self {
        Self::Unsupported(message.to_string())
    }

    /// Build an [`Self::Exhausted`] from anything printable.
    pub fn exhausted(message: impl std::fmt::Display) -> Self {
        Self::Exhausted(message.to_string())
    }

    /// Build an [`Self::Internal`] from anything printable.
    pub fn internal(message: impl std::fmt::Display) -> Self {
        Self::Internal(message.to_string())
    }

    /// The gRPC code this failure maps to.
    #[must_use]
    pub const fn code(&self) -> tonic::Code {
        match self {
            Self::Invalid(_) => tonic::Code::InvalidArgument,
            Self::Unsupported(_) => tonic::Code::Unimplemented,
            Self::Exhausted(_) => tonic::Code::ResourceExhausted,
            Self::Internal(_) => tonic::Code::Internal,
        }
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message)
            | Self::Unsupported(message)
            | Self::Exhausted(message)
            | Self::Internal(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ParseError {}

impl From<ParseError> for tonic::Status {
    fn from(error: ParseError) -> Self {
        Self::new(error.code(), error.to_string())
    }
}

impl From<crate::codec::CodecError> for ParseError {
    /// Both codec failures are the caller's: an encoding this build does not
    /// carry, and a byte the chosen code page does not assign. Neither is a
    /// server fault and neither is recoverable by retrying.
    fn from(error: crate::codec::CodecError) -> Self {
        Self::Invalid(error.to_string())
    }
}
