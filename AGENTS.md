# AGENTS.md — grpc-ebcdic

You are implementing **grpc-ebcdic** from scratch in this repo. There is no
application code yet. Specs are the source of truth.

## Read this first, in order

1. This file
2. `docs/architecture.md` — fleet boundary, language, what we refuse to own
3. `docs/design.md` — wire API sketch, Document mapping, tests
4. `docs/guidelines.md` — fleet rules (streaming, proto, git, tests)

Do not start coding until those four are in your context. If architecture
and an existing sibling disagree on *process* (diskless, health, buf),
follow the sibling. If they disagree on *product* (live stream, Document
plane), follow architecture.md.

## This service

gRPC collector for copybook-driven EBCDIC records, projecting into the gRParse Document data plane

- **Language:** Rust (tonic). Tight byte walk, no GC on the record path.
- **Copy from:** /work/main/grpc-services/grpc-calamine (streaming rows, diskless)
- **Stack:** Layout is required on the request (protobuf or Docling-shaped JSON bytes). Never a server filesystem path. Default codec cp037.
- **Live stream:** LayoutInfo, then RecordRow as each record decodes, ParseStatus trailer.

## Definition of done (v1)

ParseEbcdic stream, packed/zoned/display fixtures, no-layout INVALID_ARGUMENT, trailing-record warning, health+reflection.

Also: README with build/run; proto lint clean; tests that fail if someone
turns the stream back into a batch (assert an event before the input is
fully consumed, or per-item events before Complete).

## Workspace

Checkout path: `/work/main/grpc-services/grpc-ebcdic`.
Git: `origin` = Forgejo (push `main` here). `github` = GitHub mirror.
Never merge GitHub `main`. See `docs/guidelines.md`.

gRParse wiring (`COLLECTOR_*` enum, endpoint env) is a **follow-up**.
Ship a working server in this repo first.
