// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests: a real tonic server on an ephemeral port, the generated
//! client, and fixtures the tests build themselves.
//!
//! There are no fixture files. Every record in here is assembled from a
//! copybook written as a string literal plus bytes encoded by
//! [`Encoder`], so the assertions read as "these values, written this way,
//! come back as these values" rather than as "this opaque blob matches this
//! opaque expectation". Nothing touches the network beyond localhost and
//! nothing touches the disk at all.

use std::time::Duration;

use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic::transport::{Endpoint, Server};

use grpc_ebcdic::codec::Codec;
use grpc_ebcdic::proto::v1 as pb;
use grpc_ebcdic::proto::v1::ebcdic_parse_service_client::EbcdicParseServiceClient;
use grpc_ebcdic::{EbcdicGrpc, Metrics};

/// A connected client to a server running in this process.
type Client = EbcdicParseServiceClient<tonic::transport::Channel>;

/// Start the server on an ephemeral localhost port and return a client.
async fn start_server() -> Client {
    start_server_with(EbcdicGrpc::new(Metrics::new())).await
}

/// Start a specific service configuration and return a client for it.
async fn start_server_with(service: EbcdicGrpc) -> Client {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(service.into_service())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("server runs");
    });
    // The listener is bound before the spawn, so connecting cannot race it.
    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("connect");
    EbcdicParseServiceClient::new(channel)
}

/// Builds EBCDIC record bytes from readable values.
///
/// The counterpart of the decoder under test: text goes through the same code
/// page tables, and the numeric encoders here are written from the COBOL
/// definitions rather than from the decoder's code, so a round trip is a real
/// check and not a tautology.
struct Encoder {
    /// Code page for character data.
    codec: Codec,
    /// Bytes accumulated so far.
    bytes: Vec<u8>,
}

impl Encoder {
    /// Start an encoder for a named code page.
    fn new(encoding: &str) -> Self {
        Self {
            codec: Codec::resolve(encoding).expect("known code page"),
            bytes: Vec::new(),
        }
    }

    /// Append `text` padded with EBCDIC spaces to `width` bytes.
    fn text(mut self, text: &str, width: usize) -> Self {
        assert!(
            text.len() <= width,
            "{text:?} does not fit in {width} bytes"
        );
        let mut encoded = self.codec.encode(text).expect("representable");
        encoded.resize(width, 0x40);
        self.bytes.extend(encoded);
        self
    }

    /// Append raw bytes, for the deliberately corrupt cases.
    fn raw(mut self, bytes: &[u8]) -> Self {
        self.bytes.extend_from_slice(bytes);
        self
    }

    /// Append a zoned decimal of `digits` digits.
    ///
    /// Each digit becomes `0xF<digit>`; a negative value overpunches the last
    /// zone nibble to `0xD`.
    fn zoned(mut self, value: i64, digits: usize) -> Self {
        let text = format!("{:0>width$}", value.unsigned_abs(), width = digits);
        assert_eq!(
            text.len(),
            digits,
            "{value} needs more than {digits} digits"
        );
        let mut encoded: Vec<u8> = text.bytes().map(|digit| 0xF0 | (digit - b'0')).collect();
        if value < 0 {
            let last = encoded.len() - 1;
            encoded[last] = 0xD0 | (encoded[last] & 0x0F);
        }
        self.bytes.extend(encoded);
        self
    }

    /// Append a COMP-3 packed decimal of `digits` digits.
    ///
    /// `digits` counts stored digits, so the field occupies
    /// `digits / 2 + 1` bytes: two digits per byte with the sign in the final
    /// nibble.
    fn packed(mut self, value: i64, digits: usize) -> Self {
        let text = format!("{:0>width$}", value.unsigned_abs(), width = digits);
        assert_eq!(
            text.len(),
            digits,
            "{value} needs more than {digits} digits"
        );
        let mut nibbles: Vec<u8> = Vec::with_capacity(digits + 2);
        // An even digit count needs a leading zero nibble to fill the byte.
        if digits.is_multiple_of(2) {
            nibbles.push(0);
        }
        nibbles.extend(text.bytes().map(|digit| digit - b'0'));
        nibbles.push(if value < 0 { 0x0d } else { 0x0c });
        let bytes: Vec<u8> = nibbles
            .chunks(2)
            .map(|pair| (pair[0] << 4) | pair[1])
            .collect();
        assert_eq!(bytes.len(), digits / 2 + 1);
        self.bytes.extend(bytes);
        self
    }

    /// Append a big-endian binary field of `width` bytes.
    fn binary(mut self, value: i64, width: usize) -> Self {
        let full = value.to_be_bytes();
        self.bytes.extend_from_slice(&full[full.len() - width..]);
        self
    }

    /// Finish and return the bytes.
    fn build(self) -> Vec<u8> {
        self.bytes
    }
}

/// The worked example copybook, shared by most of these tests.
///
/// Fixed-format columns on purpose: this is what a copybook looks like when it
/// comes off a mainframe, sequence numbers and all.
const CUSTOMER_COPYBOOK: &str = "\
000100* Customer master record.
000200 01  CUSTOMER-RECORD.
000300     05  CUST-ID              PIC 9(6).
000400     05  CUST-NAME            PIC X(20).
000500     05  CUST-BALANCE         PIC S9(7)V99 COMP-3.
000600     05  CUST-ORDER-COUNT     PIC S9(4) COMP.
000700     05  FILLER               PIC X(4).
";

/// Width of one `CUSTOMER-RECORD`: 6 + 20 + 5 + 2 + 4.
const CUSTOMER_RECORD_BYTES: u32 = 37;

/// Encode one customer record.
fn customer(id: i64, name: &str, balance_cents: i64, orders: i64) -> Vec<u8> {
    customer_in("cp037", id, name, balance_cents, orders)
}

/// Encode one customer record with an explicit code page.
fn customer_in(encoding: &str, id: i64, name: &str, balance_cents: i64, orders: i64) -> Vec<u8> {
    let bytes = Encoder::new(encoding)
        .zoned(id, 6)
        .text(name, 20)
        .packed(balance_cents, 9)
        .binary(orders, 2)
        .text("", 4)
        .build();
    assert_eq!(bytes.len(), CUSTOMER_RECORD_BYTES as usize);
    bytes
}

/// Options that parse `CUSTOMER_COPYBOOK` with the defaults.
fn customer_options() -> pb::ParseOptions {
    pb::ParseOptions {
        layout_source: Some(pb::parse_options::LayoutSource::Copybook(
            CUSTOMER_COPYBOOK.to_string(),
        )),
        ..Default::default()
    }
}

/// Wrap options in the first request frame.
fn options_frame(options: pb::ParseOptions) -> pb::ParseEbcdicRequest {
    pb::ParseEbcdicRequest {
        frame: Some(pb::parse_ebcdic_request::Frame::Options(options)),
    }
}

/// Wrap bytes in a data frame.
fn chunk_frame(bytes: &[u8]) -> pb::ParseEbcdicRequest {
    pb::ParseEbcdicRequest {
        frame: Some(pb::parse_ebcdic_request::Frame::Chunk(bytes.to_vec())),
    }
}

/// Read the next event, failing the test rather than hanging if the server has
/// quietly turned itself into a batch parser.
async fn next_event(
    stream: &mut tonic::Streaming<pb::ParseEbcdicResponse>,
) -> pb::ParseEbcdicResponse {
    tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("the server must not wait for the end of the input")
        .expect("stream is healthy")
        .expect("stream is not finished")
}

/// Everything one parse produced, split by event kind.
#[derive(Debug)]
struct Parsed {
    /// The opening layout event.
    layout: pb::LayoutInfo,
    /// Row events in arrival order.
    rows: Vec<pb::RecordRow>,
    /// The trailer.
    status: pb::ParseStatus,
}

/// Run one parse to completion and sort the events.
async fn parse(
    client: &Client,
    options: pb::ParseOptions,
    data: &[u8],
) -> Result<Parsed, tonic::Status> {
    let mut client = client.clone();
    let mut frames = vec![options_frame(options)];
    frames.extend(data.chunks(64 * 1024).map(chunk_frame));
    let mut stream = client
        .parse_ebcdic(tokio_stream::iter(frames))
        .await?
        .into_inner();

    let mut layout = None;
    let mut rows = Vec::new();
    let mut status = None;
    while let Some(event) = stream.message().await? {
        match event.event.expect("every event carries a payload") {
            pb::parse_ebcdic_response::Event::LayoutInfo(info) => {
                assert!(layout.is_none(), "layout_info arrives exactly once");
                assert!(rows.is_empty(), "layout_info arrives before any row");
                layout = Some(info);
            }
            pb::parse_ebcdic_response::Event::Record(row) => {
                assert!(status.is_none(), "no row may follow the trailer");
                rows.push(row);
            }
            pb::parse_ebcdic_response::Event::Status(trailer) => {
                assert!(status.is_none(), "the trailer arrives exactly once");
                status = Some(trailer);
            }
        }
    }
    Ok(Parsed {
        layout: layout.expect("a successful parse always opens with layout_info"),
        rows,
        status: status.expect("a successful parse always ends with a trailer"),
    })
}

/// The value of a named cell.
fn cell<'a>(row: &'a pb::RecordRow, name: &str) -> &'a pb::cell::Value {
    row.cells
        .iter()
        .find(|cell| cell.name == name)
        .unwrap_or_else(|| panic!("row has no cell {name:?}: {:?}", row.cells))
        .value
        .as_ref()
        .expect("every cell carries a value")
}

#[tokio::test]
async fn a_copybook_and_three_records_round_trip_with_their_types_intact() {
    let client = start_server().await;
    let mut data = Vec::new();
    data.extend(customer(1, "ACME SUPPLY", -123_456_789, 42));
    data.extend(customer(2, "BETA WORKS", 0, 0));
    data.extend(customer(999_999, "GAMMA LTD", 987_654_321, -7));

    let parsed = parse(&client, customer_options(), &data)
        .await
        .expect("parse succeeds");

    assert_eq!(parsed.layout.encoding, "cp037");
    assert_eq!(parsed.layout.source, pb::LayoutSource::Copybook as i32);
    assert_eq!(parsed.layout.records.len(), 1);
    let schema = &parsed.layout.records[0];
    assert_eq!(schema.name, "CUSTOMER-RECORD");
    assert_eq!(schema.record_length, CUSTOMER_RECORD_BYTES);
    assert_eq!(
        schema
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.offset, f.size))
            .collect::<Vec<_>>(),
        vec![
            ("CUST-ID", 0, 6),
            ("CUST-NAME", 6, 20),
            ("CUST-BALANCE", 26, 5),
            ("CUST-ORDER-COUNT", 31, 2),
            ("", 33, 4),
        ]
    );

    assert_eq!(parsed.rows.len(), 3);
    // The filler is consumed and never emitted, so a row is four cells.
    assert_eq!(parsed.rows[0].cells.len(), 4);

    assert_eq!(
        cell(&parsed.rows[0], "CUST-ID"),
        &pb::cell::Value::Integer(1)
    );
    assert_eq!(
        cell(&parsed.rows[0], "CUST-NAME"),
        &pb::cell::Value::Text("ACME SUPPLY".into()),
        "trailing EBCDIC blanks are padding, not value"
    );
    assert_eq!(
        cell(&parsed.rows[0], "CUST-BALANCE"),
        &pb::cell::Value::Decimal(pb::Decimal {
            text: "-1234567.89".into(),
            scale: 2,
            unscaled: Some(-123_456_789),
        }),
        "COMP-3 keeps its sign, its scale, and every digit"
    );
    assert_eq!(
        cell(&parsed.rows[0], "CUST-ORDER-COUNT"),
        &pb::cell::Value::Integer(42)
    );

    assert_eq!(
        cell(&parsed.rows[1], "CUST-BALANCE"),
        &pb::cell::Value::Decimal(pb::Decimal {
            text: "0.00".into(),
            scale: 2,
            unscaled: Some(0),
        })
    );
    assert_eq!(
        cell(&parsed.rows[2], "CUST-ID"),
        &pb::cell::Value::Integer(999_999)
    );
    assert_eq!(
        cell(&parsed.rows[2], "CUST-ORDER-COUNT"),
        &pb::cell::Value::Integer(-7),
        "a signed COMP field is two's complement, not a magnitude"
    );

    assert_eq!(parsed.status.records_kept, 3);
    assert_eq!(parsed.status.bytes_received, data.len() as u64);
    assert_eq!(parsed.status.bytes_consumed, data.len() as u64);
    assert_eq!(parsed.status.trailing_bytes, 0);
    assert!(!parsed.status.truncated);
    assert!(
        parsed.status.warnings.is_empty(),
        "{:?}",
        parsed.status.warnings
    );
    assert_eq!(parsed.status.rows_per_record_type["CUSTOMER-RECORD"], 3);
}

/// The test that fails if anyone turns this stream back into a batch.
///
/// The client sends the options and exactly one record, then stops without
/// closing its half of the stream. If the server ever buffers the input and
/// decodes at the end, no row can arrive and this test hangs until the
/// timeout. Records here are fixed-length, so "the first record is complete"
/// is a property of the bytes and not of the upload finishing.
#[tokio::test]
async fn rows_arrive_before_the_input_is_finished() {
    let client = start_server().await;
    let mut client = client.clone();
    let (tx, rx) = tokio::sync::mpsc::channel(8);

    // Queued before the call is awaited: the server validates the options
    // frame before it opens the response stream, so awaiting first would
    // deadlock. The proto documents this on the RPC; this is what keeps it so.
    tx.send(options_frame(customer_options())).await.unwrap();
    tx.send(chunk_frame(&customer(1, "FIRST", 100, 1)))
        .await
        .unwrap();

    let mut stream = client
        .parse_ebcdic(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .expect("open the call")
        .into_inner();

    let first = next_event(&mut stream).await;
    assert!(
        matches!(
            first.event,
            Some(pb::parse_ebcdic_response::Event::LayoutInfo(_))
        ),
        "the layout is known before any data byte, got {first:?}"
    );

    // Nothing else has been sent and the request stream is still open.
    let second = next_event(&mut stream).await;
    let Some(pb::parse_ebcdic_response::Event::Record(row)) = second.event else {
        panic!("expected a row while the upload is still open, got {second:?}");
    };
    assert_eq!(
        cell(&row, "CUST-NAME"),
        &pb::cell::Value::Text("FIRST".into())
    );

    // A second record, still mid-stream, still one event per record.
    tx.send(chunk_frame(&customer(2, "SECOND", 200, 2)))
        .await
        .unwrap();
    let third = next_event(&mut stream).await;
    let Some(pb::parse_ebcdic_response::Event::Record(row)) = third.event else {
        panic!("expected the second row, got {third:?}");
    };
    assert_eq!(row.record_index, 1);
    assert_eq!(
        cell(&row, "CUST-NAME"),
        &pb::cell::Value::Text("SECOND".into())
    );

    drop(tx);
    let trailer = next_event(&mut stream).await;
    let Some(pb::parse_ebcdic_response::Event::Status(status)) = trailer.event else {
        panic!("expected the trailer, got {trailer:?}");
    };
    assert_eq!(status.records_kept, 2);
}

/// Half a record is not a record: the row appears only when its last byte does.
#[tokio::test]
async fn a_row_waits_for_its_last_byte_and_not_a_moment_longer() {
    let client = start_server().await;
    let mut client = client.clone();
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let record = customer(7, "SPLIT", 5, 1);

    tx.send(options_frame(customer_options())).await.unwrap();
    tx.send(chunk_frame(&record[..CUSTOMER_RECORD_BYTES as usize - 1]))
        .await
        .unwrap();

    let mut stream = client
        .parse_ebcdic(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .expect("open the call")
        .into_inner();

    let layout = stream.message().await.unwrap().unwrap();
    assert!(matches!(
        layout.event,
        Some(pb::parse_ebcdic_response::Event::LayoutInfo(_))
    ));

    // One byte short. Nothing may be emitted, so a read has to time out.
    let early = tokio::time::timeout(Duration::from_millis(250), stream.message()).await;
    assert!(
        early.is_err(),
        "a partial record must not produce a row: {early:?}"
    );

    tx.send(chunk_frame(&record[CUSTOMER_RECORD_BYTES as usize - 1..]))
        .await
        .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("the last byte releases the row")
        .unwrap()
        .unwrap();
    let Some(pb::parse_ebcdic_response::Event::Record(row)) = event.event else {
        panic!("expected the completed row, got {event:?}");
    };
    assert_eq!(
        cell(&row, "CUST-NAME"),
        &pb::cell::Value::Text("SPLIT".into())
    );
}

#[tokio::test]
async fn a_request_without_a_layout_is_invalid_argument() {
    let client = start_server().await;
    let status = parse(&client, pb::ParseOptions::default(), b"anything")
        .await
        .expect_err("bytes without a copybook are an opaque code page");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("layout"), "{}", status.message());
}

#[tokio::test]
async fn a_trailing_partial_record_is_a_warning_and_the_whole_rows_still_arrive() {
    let client = start_server().await;
    let mut data = customer(1, "WHOLE", 1, 1);
    data.extend_from_slice(&customer(2, "PARTIAL", 2, 2)[..10]);

    let parsed = parse(&client, customer_options(), &data)
        .await
        .expect("a short tail is not fatal");
    assert_eq!(parsed.rows.len(), 1);
    assert_eq!(parsed.status.records_kept, 1);
    assert_eq!(parsed.status.trailing_bytes, 10);
    assert_eq!(parsed.status.warnings.len(), 1);
    assert_eq!(
        parsed.status.warnings[0].code,
        pb::WarningCode::TrailingPartialRecord as i32
    );
    assert_eq!(
        parsed.status.warnings[0].byte_offset,
        u64::from(CUSTOMER_RECORD_BYTES)
    );

    // The same input with abort_on_error is a refusal instead.
    let strict = pb::ParseOptions {
        abort_on_error: true,
        ..customer_options()
    };
    let status = parse(&client, strict, &data)
        .await
        .expect_err("abort_on_error refuses the tail");
    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn a_bad_nibble_in_a_comp_3_field_is_invalid_argument() {
    let client = start_server().await;
    // A well-formed record, then one whose balance carries 0xa in a digit
    // position. This is what a layout with drifted offsets looks like.
    let mut data = customer(1, "GOOD", 100, 1);
    data.extend(
        Encoder::new("cp037")
            .zoned(2, 6)
            .text("CORRUPT", 20)
            .raw(&[0x1a, 0x23, 0x45, 0x67, 0x8c])
            .binary(1, 2)
            .text("", 4)
            .build(),
    );

    let status = parse(&client, customer_options(), &data)
        .await
        .expect_err("a packed field with a bad nibble has no value to report");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status.message().contains("CUST-BALANCE"),
        "{}",
        status.message()
    );
    assert!(status.message().contains("nibble"), "{}", status.message());
}

#[tokio::test]
async fn a_non_decimal_zoned_digit_is_invalid_argument() {
    let client = start_server().await;
    // 0xfa in the id: the zone is right, the digit nibble is not.
    let data = Encoder::new("cp037")
        .raw(&[0xf0, 0xf0, 0xf0, 0xf0, 0xf0, 0xfa])
        .text("BAD ZONE", 20)
        .packed(1, 9)
        .binary(1, 2)
        .text("", 4)
        .build();

    let status = parse(&client, customer_options(), &data)
        .await
        .expect_err("0xa is not a digit");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("CUST-ID"), "{}", status.message());
}

/// The same bytes, three code pages, three different strings.
///
/// The characters chosen are the ones cp037 and cp500 disagree about; a layout
/// decoded with the wrong page produces text that looks fine and is wrong,
/// which is exactly why `encoding` is a request option.
#[tokio::test]
async fn the_code_page_changes_what_the_same_bytes_say() {
    let client = start_server().await;
    let copybook = "01 R.\n05 MARKS PIC X(3).\n05 EURO PIC X(1).\n";
    let options = |encoding: &str| pb::ParseOptions {
        encoding: encoding.to_string(),
        layout_source: Some(pb::parse_options::LayoutSource::Copybook(
            copybook.to_string(),
        )),
        ..Default::default()
    };
    // 0x4a 0x5a 0x5f differ between cp037 and cp500; 0x9f differs between
    // cp037 and cp1140.
    let data = [0x4a, 0x5a, 0x5f, 0x9f];

    let us = parse(&client, options("cp037"), &data).await.unwrap();
    assert_eq!(us.layout.encoding, "cp037");
    assert_eq!(
        cell(&us.rows[0], "MARKS"),
        &pb::cell::Value::Text("\u{a2}!\u{ac}".into())
    );
    assert_eq!(
        cell(&us.rows[0], "EURO"),
        &pb::cell::Value::Text("\u{a4}".into())
    );

    let international = parse(&client, options("cp500"), &data).await.unwrap();
    assert_eq!(international.layout.encoding, "cp500");
    assert_eq!(
        cell(&international.rows[0], "MARKS"),
        &pb::cell::Value::Text("[]^".into()),
        "the same three bytes, a different alphabet"
    );

    let euro = parse(&client, options("cp1140"), &data).await.unwrap();
    assert_eq!(
        cell(&euro.rows[0], "EURO"),
        &pb::cell::Value::Text("\u{20ac}".into()),
        "cp1140 is cp037 with the euro at 0x9f"
    );

    // An alias resolves to the same canonical name.
    let alias = parse(&client, options("IBM-037"), &data).await.unwrap();
    assert_eq!(alias.layout.encoding, "cp037");

    // And a code page this build does not carry is refused by name.
    let status = parse(&client, options("utf-8"), &data)
        .await
        .expect_err("utf-8 is not EBCDIC");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("utf-8"), "{}", status.message());
}

#[tokio::test]
async fn control_characters_are_stripped_only_when_the_option_is_on() {
    let client = start_server().await;
    let copybook = "01 R.\n05 NOTE PIC X(6).\n";
    let build = |strip: Option<bool>| pb::ParseOptions {
        layout_source: Some(pb::parse_options::LayoutSource::Copybook(
            copybook.to_string(),
        )),
        strip_control_characters: strip,
        ..Default::default()
    };
    // "AB", shift-out (0x0e), "C", then two blanks.
    let data = [0xc1, 0xc2, 0x0e, 0xc3, 0x40, 0x40];

    let stripped = parse(&client, build(None), &data).await.unwrap();
    assert_eq!(
        cell(&stripped.rows[0], "NOTE"),
        &pb::cell::Value::Text("ABC".into()),
        "unset means true, matching Docling's default"
    );

    let kept = parse(&client, build(Some(false)), &data).await.unwrap();
    assert_eq!(
        cell(&kept.rows[0], "NOTE"),
        &pb::cell::Value::Text("AB\u{e}C".into())
    );
}

#[tokio::test]
async fn max_records_truncates_and_says_so() {
    let client = start_server().await;
    let mut data = Vec::new();
    for id in 1..=5 {
        data.extend(customer(id, "BULK", 0, 0));
    }
    let options = pb::ParseOptions {
        max_records: 2,
        ..customer_options()
    };
    let parsed = parse(&client, options, &data).await.unwrap();
    assert_eq!(parsed.rows.len(), 2);
    assert!(parsed.status.truncated);
    assert_eq!(
        parsed.status.warnings[0].code,
        pb::WarningCode::MaxRecordsReached as i32
    );
    assert_eq!(parsed.status.bytes_received, data.len() as u64);
}

#[tokio::test]
async fn the_byte_cap_is_resource_exhausted() {
    let client = start_server_with(EbcdicGrpc::new(Metrics::new()).with_max_document_mib(1)).await;
    let mut data = Vec::new();
    while data.len() <= 1024 * 1024 {
        data.extend(customer(1, "BIG", 0, 0));
    }
    let status = parse(&client, customer_options(), &data)
        .await
        .expect_err("past the cap");
    assert_eq!(status.code(), Code::ResourceExhausted);

    // The per-request cap is honoured too, and it is the tighter of the two.
    let options = pb::ParseOptions {
        max_document_mib: 1,
        ..customer_options()
    };
    let generous =
        start_server_with(EbcdicGrpc::new(Metrics::new()).with_max_document_mib(512)).await;
    let status = parse(&generous, options, &data)
        .await
        .expect_err("past the request cap");
    assert_eq!(status.code(), Code::ResourceExhausted);
}

#[tokio::test]
async fn an_unsupported_copybook_feature_is_unimplemented() {
    let client = start_server().await;
    let options = pb::ParseOptions {
        layout_source: Some(pb::parse_options::LayoutSource::Copybook(
            "01 R.\n05 A PIC X(4).\n05 B REDEFINES A PIC 9(4).\n".into(),
        )),
        ..Default::default()
    };
    let status = parse(&client, options, b"")
        .await
        .expect_err("REDEFINES is out of scope");
    assert_eq!(status.code(), Code::Unimplemented);
    assert!(
        status.message().contains("REDEFINES"),
        "{}",
        status.message()
    );
}

#[tokio::test]
async fn a_malformed_copybook_is_invalid_argument() {
    let client = start_server().await;
    let options = pb::ParseOptions {
        layout_source: Some(pb::parse_options::LayoutSource::Copybook(
            "this is a haiku\nnot a data description\nthe bytes stay opaque\n".into(),
        )),
        ..Default::default()
    };
    let status = parse(&client, options, b"")
        .await
        .expect_err("prose is not a copybook");
    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn a_multi_schema_file_routes_records_by_their_selector() {
    let client = start_server().await;
    let layout = pb::EbcdicLayout {
        description: "Mixed customer and order extract".into(),
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
                    size: 10,
                    r#type: pb::FieldType::String as i32,
                    ..Default::default()
                }],
            },
            pb::EbcdicRecordLayout {
                name: "ORDER".into(),
                selector: Some("O".into()),
                fields: vec![pb::EbcdicField {
                    name: "AMOUNT".into(),
                    size: 4,
                    r#type: pb::FieldType::PackedDecimal as i32,
                    scale: 2,
                    ..Default::default()
                }],
            },
        ],
        ..Default::default()
    };
    let options = pb::ParseOptions {
        layout_source: Some(pb::parse_options::LayoutSource::Layout(layout)),
        ..Default::default()
    };

    let mut data = Encoder::new("cp037").text("C", 1).text("ACME", 10).build();
    data.extend(Encoder::new("cp037").text("O", 1).packed(-4250, 7).build());
    data.extend(Encoder::new("cp037").text("C", 1).text("BETA", 10).build());

    let parsed = parse(&client, options, &data).await.unwrap();
    assert_eq!(parsed.layout.prefix_size, 1);
    assert_eq!(
        parsed.layout.description,
        "Mixed customer and order extract"
    );
    assert_eq!(
        parsed
            .rows
            .iter()
            .map(|r| (r.record_type.as_str(), r.row_index))
            .collect::<Vec<_>>(),
        vec![("CUSTOMER", 0), ("ORDER", 0), ("CUSTOMER", 1)]
    );
    assert_eq!(
        cell(&parsed.rows[1], "AMOUNT"),
        &pb::cell::Value::Decimal(pb::Decimal {
            text: "-42.50".into(),
            scale: 2,
            unscaled: Some(-4250),
        })
    );
    assert_eq!(parsed.status.rows_per_record_type["CUSTOMER"], 2);
    assert_eq!(parsed.status.rows_per_record_type["ORDER"], 1);
}

#[tokio::test]
async fn the_docling_json_layout_decodes_the_same_bytes_as_the_copybook() {
    let client = start_server().await;
    // Docling's EbcdicLayout serialization, field for field.
    let json = br#"{
        "description": "Customer master",
        "records": [{
            "name": "CUSTOMER-RECORD",
            "fields": [
                {"name": "CUST-ID", "size": 6, "type": "zoned_decimal"},
                {"name": "CUST-NAME", "size": 20, "type": "string"},
                {"name": "CUST-BALANCE", "size": 5, "type": "packed_decimal", "scale": 2},
                {"name": "CUST-ORDER-COUNT", "size": 2, "type": "integer"},
                {"name": "FILLER", "size": 4, "type": "skip"}
            ]
        }]
    }"#;
    let data = customer(4242, "JSON ROUTE", -50, 3);

    let from_json = parse(
        &client,
        pb::ParseOptions {
            layout_source: Some(pb::parse_options::LayoutSource::LayoutJson(json.to_vec())),
            ..Default::default()
        },
        &data,
    )
    .await
    .unwrap();
    let from_copybook = parse(&client, customer_options(), &data).await.unwrap();

    assert_eq!(from_json.layout.source, pb::LayoutSource::Json as i32);
    assert_eq!(from_json.layout.description, "Customer master");
    assert_eq!(from_json.rows[0].cells, from_copybook.rows[0].cells);
    assert_eq!(
        cell(&from_json.rows[0], "CUST-BALANCE"),
        &pb::cell::Value::Decimal(pb::Decimal {
            text: "-0.50".into(),
            scale: 2,
            unscaled: Some(-50),
        })
    );
}

#[tokio::test]
async fn setting_two_layouts_at_once_is_impossible_by_construction() {
    // The three forms are a protobuf `oneof`, so "both" cannot be expressed on
    // the wire: assigning the second clears the first. This is the test that
    // records that the mutual exclusion is structural rather than validated,
    // which is why there is no "two layouts" error to test for.
    let mut options = customer_options();
    options.layout_source = Some(pb::parse_options::LayoutSource::LayoutJson(
        br#"{"records":[]}"#.to_vec(),
    ));
    assert!(matches!(
        options.layout_source,
        Some(pb::parse_options::LayoutSource::LayoutJson(_))
    ));
}

#[tokio::test]
async fn get_service_info_reports_what_this_build_can_do() {
    let client = start_server().await;
    let mut client = client.clone();
    let info = client
        .get_service_info(pb::GetServiceInfoRequest {})
        .await
        .expect("service info is cheap")
        .into_inner();
    assert_eq!(info.service, "grpc-ebcdic");
    assert_eq!(info.default_encoding, "cp037");
    assert!(
        info.encodings.contains(&"cp500".to_string()),
        "{:?}",
        info.encodings
    );
    assert!(
        info.encodings.contains(&"cp1140".to_string()),
        "{:?}",
        info.encodings
    );
    assert!(info.copybook_compiler);
    assert!(info.max_concurrent_parses >= 1);
    assert!(
        info.supported_field_types
            .contains(&(pb::FieldType::PackedDecimal as i32))
    );
    assert!(!info.version.is_empty());
}

#[tokio::test]
async fn the_concurrency_cap_refuses_rather_than_queues() {
    let client =
        start_server_with(EbcdicGrpc::new(Metrics::new()).with_max_concurrent_parses(1)).await;

    // Hold one parse open by never closing its request stream.
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(options_frame(customer_options())).await.unwrap();
    let mut holder = client.clone();
    let mut held = holder
        .parse_ebcdic(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .expect("the first parse is admitted")
        .into_inner();
    // Reading the layout event proves the permit is taken, not merely queued.
    held.message().await.unwrap().unwrap();

    let status = parse(&client, customer_options(), &customer(1, "SECOND", 0, 0))
        .await
        .expect_err("the second parse is over the cap");
    assert_eq!(status.code(), Code::ResourceExhausted);

    // Once the first ends, the permit comes back.
    drop(tx);
    while held.message().await.unwrap().is_some() {}
    // The permit is released by the spawned task, which may still be finishing.
    for attempt in 0.. {
        match parse(&client, customer_options(), &customer(2, "THIRD", 0, 0)).await {
            Ok(parsed) => {
                assert_eq!(parsed.rows.len(), 1);
                break;
            }
            Err(status) if attempt < 50 => {
                assert_eq!(status.code(), Code::ResourceExhausted);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(status) => panic!("the permit was never released: {status}"),
        }
    }
}

#[tokio::test]
async fn a_data_frame_before_the_options_frame_is_invalid_argument() {
    let client = start_server().await;
    let mut client = client.clone();
    let status = client
        .parse_ebcdic(tokio_stream::iter(vec![chunk_frame(b"bytes first")]))
        .await
        .expect_err("the server cannot decode before it has a layout");
    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn an_empty_input_produces_a_layout_and_an_empty_trailer() {
    let client = start_server().await;
    let parsed = parse(&client, customer_options(), b"")
        .await
        .expect("no records is not an error");
    assert!(parsed.rows.is_empty());
    assert_eq!(parsed.status.records_kept, 0);
    assert_eq!(parsed.status.bytes_received, 0);
    assert!(
        parsed.status.warnings.is_empty(),
        "{:?}",
        parsed.status.warnings
    );
    assert_eq!(parsed.status.rows_per_record_type["CUSTOMER-RECORD"], 0);
}

#[tokio::test]
async fn a_header_and_footer_are_skipped_around_the_records() {
    let client = start_server().await;
    let layout = pb::EbcdicLayout {
        header_size: 8,
        footer_size: 8,
        records: vec![pb::EbcdicRecordLayout {
            name: "ROW".into(),
            selector: None,
            fields: vec![pb::EbcdicField {
                name: "VALUE".into(),
                size: 4,
                r#type: pb::FieldType::String as i32,
                ..Default::default()
            }],
        }],
        ..Default::default()
    };
    let options = pb::ParseOptions {
        layout_source: Some(pb::parse_options::LayoutSource::Layout(layout)),
        ..Default::default()
    };
    let data = Encoder::new("cp037")
        .text("HEADER01", 8)
        .text("AAAA", 4)
        .text("BBBB", 4)
        .text("TRAILER1", 8)
        .build();

    let parsed = parse(&client, options, &data).await.unwrap();
    assert_eq!(parsed.layout.header_size, 8);
    assert_eq!(parsed.layout.footer_size, 8);
    assert_eq!(
        parsed
            .rows
            .iter()
            .map(|r| cell(r, "VALUE").clone())
            .collect::<Vec<_>>(),
        vec![
            pb::cell::Value::Text("AAAA".into()),
            pb::cell::Value::Text("BBBB".into()),
        ]
    );
    assert!(
        parsed.status.warnings.is_empty(),
        "{:?}",
        parsed.status.warnings
    );
}
