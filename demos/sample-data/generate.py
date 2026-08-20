#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Regenerate the .ebc fixtures in this directory.

Every fixture is a real cp037-encoded EBCDIC file assembled field by field:
zoned DISPLAY numerics, COMP-3 packed decimals, big-endian COMP binaries and
blank-padded character fields, matching the companion .cpy copybook or
.layout.json layout byte for byte. Stdlib only; run:

    python3 generate.py
"""

import json
from pathlib import Path

HERE = Path(__file__).parent
CP = "cp037"

# COBOL signed-overpunch zone nibbles for DISPLAY numerics (not used by the
# fixtures below, which keep their DISPLAY fields unsigned, but the encoder
# would be incomplete without them).
ZONE_POSITIVE = 0xF
ZONE_NEGATIVE = 0xD


def text(value: str, size: int) -> bytes:
    """PIC X(n): cp037, right-padded with blanks."""
    raw = value.encode(CP)
    if len(raw) > size:
        raise ValueError(f"{value!r} wider than {size}")
    return raw.ljust(size, b"\x40")


def zoned(value: int, size: int) -> bytes:
    """PIC 9(n) USAGE DISPLAY, unsigned: zone nibble F over every digit."""
    digits = f"{value:0{size}d}"
    if len(digits) != size:
        raise ValueError(f"{value} does not fit {size} zoned digits")
    return bytes(0xF0 | int(d) for d in digits)


def packed(value: int, digits: int) -> bytes:
    """COMP-3: two digits per byte, sign in the trailing nibble.

    `digits` is the total number of decimal digits in the picture, V included.
    """
    sign = 0xC if value >= 0 else 0xD
    body = f"{abs(value):0{digits}d}"
    if len(body) != digits:
        raise ValueError(f"{value} does not fit {digits} packed digits")
    if digits % 2 == 0:
        body = "0" + body
    out = bytearray()
    for i in range(0, len(body) - 1, 2):
        out.append((int(body[i]) << 4) | int(body[i + 1]))
    out.append((int(body[-1]) << 4) | sign)
    return bytes(out)


def binary(value: int, size: int) -> bytes:
    """COMP / COMP-4 / BINARY: big-endian two's complement."""
    return value.to_bytes(size, "big", signed=True)


def customer_master() -> None:
    """customer-master.ebc: the README's worked example, ten rows."""
    rows = [
        (1, "ACME SUPPLY", -1234567, 42),
        (2, "NORTHWIND TRADERS", 887501, 7),
        (3, "CONTOSO LTD", 0, 0),
        (4, "FABRIKAM INC", 2500099, 113),
        (5, "WINGTIP TOYS", -43100, 3),
        (6, "ADVENTURE WORKS", 9912300, 871),
        (7, "LITWARE", 45250, 12),
        (8, "PROSEWARE", -9800005, 256),
        (9, "TAILSPIN TOYS", 6100, 1),
        (10, "COHO WINERY", 774125, 64),
    ]
    out = bytearray()
    for cust_id, name, balance, orders in rows:
        out += zoned(cust_id, 6)
        out += text(name, 20)
        out += packed(balance, 9)  # S9(7)V99: 9 digits, 5 bytes
        out += binary(orders, 2)  # S9(4) COMP
        out += b"\x40" * 4  # FILLER
    (HERE / "customer-master.ebc").write_bytes(bytes(out))


def sales_week() -> None:
    """sales-week.ebc: OCCURS 7 expanded to DAILY-TOTAL(1)..(7)."""
    rows = [
        ("N001", 34, [120150, 98425, 110020, 133900, 151275, 88010, 99900]),
        ("N002", 34, [45200, 51275, 49990, 61005, 72340, 30000, 41080]),
        ("S014", 34, [221000, 198750, 205300, 234125, 260480, 175200, 181900]),
        ("W007", 35, [15900, 17225, 16800, 19450, 21075, 12100, 13995]),
        ("E021", 35, [76500, 81250, 79900, 88750, 90225, 60400, 71300]),
        ("N001", 35, [118400, 101900, 112775, 129050, 148900, 91300, 102250]),
    ]
    out = bytearray()
    for store, week, daily in rows:
        if len(daily) != 7:
            raise ValueError("OCCURS 7 needs seven totals")
        out += text(store, 4)
        out += zoned(week, 2)
        for cents in daily:
            out += packed(cents, 7)  # S9(5)V99: 7 digits, 4 bytes
    (HERE / "sales-week.ebc").write_bytes(bytes(out))


def statement() -> None:
    """statement.ebc: multi-schema file routed by a 1-byte type prefix.

    One ACCOUNT-HEADER ('H') per block, then its TRANSACTION ('D') lines.
    """
    blocks = [
        (
            ("1002003001", "ACME SUPPLY CO", 4821550),
            [
                ("2026-07-31", "OPENING BALANCE", 4821550, 1),
                ("2026-08-03", "WIRE IN 88412", 2500000, 2),
                ("2026-08-05", "CHECK 1042", -1234567, 3),
                ("2026-08-11", "ACH DEBIT UTIL", -88210, 4),
            ],
        ),
        (
            ("1002003002", "NORTHWIND TRADE", 99125),
            [
                ("2026-07-31", "OPENING BALANCE", 99125, 1),
                ("2026-08-02", "DEPOSIT", 500000, 2),
                ("2026-08-09", "FEE", -2500, 3),
            ],
        ),
    ]
    out = bytearray()
    for (acct, name, opening), lines in blocks:
        out += text("H", 1)
        out += text(acct, 10)
        out += text(name, 24)
        out += packed(opening, 10)  # S9(8)V99: 10 digits, 6 bytes
        for date, memo, amount, seq in lines:
            out += text("D", 1)
            out += text(date, 10)
            out += text(memo, 20)
            out += packed(amount, 10)
            out += binary(seq, 2)
    (HERE / "statement.ebc").write_bytes(bytes(out))

    layout = {
        "description": "Bank statement: one account header, then its transaction lines.",
        "record_type_field": {"name": "REC-TYPE", "size": 1, "type": "string"},
        "records": [
            {
                "name": "ACCOUNT-HEADER",
                "selector": "H",
                "fields": [
                    {"name": "ACCOUNT-NO", "size": 10, "type": "string"},
                    {"name": "ACCOUNT-NAME", "size": 24, "type": "string"},
                    {"name": "OPENING-BALANCE", "size": 6,
                     "type": "packed_decimal", "scale": 2},
                ],
            },
            {
                "name": "TRANSACTION",
                "selector": "D",
                "fields": [
                    {"name": "POST-DATE", "size": 10, "type": "string"},
                    {"name": "MEMO", "size": 20, "type": "string"},
                    {"name": "AMOUNT", "size": 6,
                     "type": "packed_decimal", "scale": 2},
                    {"name": "SEQ", "size": 2, "type": "integer"},
                ],
            },
        ],
    }
    (HERE / "statement.layout.json").write_text(json.dumps(layout, indent=2) + "\n")


if __name__ == "__main__":
    customer_master()
    sales_week()
    statement()
    for path in sorted(HERE.glob("*.ebc")):
        print(f"{path.name}: {path.stat().st_size} bytes")
