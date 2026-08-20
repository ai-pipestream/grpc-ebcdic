// SPDX-License-Identifier: Apache-2.0
//
// Thin wrapper around the ai.pipestream.ebcdic.v1 gRPC contract.
//
// The protos are loaded dynamically from ../../../proto (the single source of
// truth in this repository) — no generated code is checked in.

import { fileURLToPath } from "node:url";
import path from "node:path";
import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";

const PROTO_ROOT = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "..", "..", "..", "proto",
);

const packageDefinition = protoLoader.loadSync(
  path.join(PROTO_ROOT, "ai", "pipestream", "ebcdic", "v1", "ebcdic_service.proto"),
  {
    includeDirs: [PROTO_ROOT],
    keepCase: false,
    longs: Number,
    enums: String,
    defaults: true,
    oneofs: true,
  },
);

const { ai } = grpc.loadPackageDefinition(packageDefinition);
const EbcdicParseService = ai.pipestream.ebcdic.v1.EbcdicParseService;

/** A connected grpc-ebcdic client. */
export class EbcdicClient {
  /** @param {string} address host:port of the grpc-ebcdic server. */
  constructor(address = process.env.EBCDIC_ADDR ?? "127.0.0.1:50063") {
    this.stub = new EbcdicParseService(
      address,
      grpc.credentials.createInsecure(),
    );
  }

  /**
   * Open a ParseEbcdic call and send the options frame.
   *
   * The caller then writes `{ chunk }` frames as the file becomes available
   * and calls `.end()`. This is the shape to use when the file is itself
   * arriving from somewhere, since it never holds the whole thing:
   * `server.js` pipes an HTTP upload straight through it.
   *
   * Note the ordering: options go out before anything reads the response. The
   * server resolves the layout and sends `layout_info` before it reads a
   * single data byte, but a caller that waits for it before sending options
   * waits forever.
   *
   * @param {object} options a ParseOptions message.
   * @returns {object} the duplex call.
   */
  openParse(options) {
    const call = this.stub.parseEbcdic();
    call.write({ options });
    return call;
  }

  /** Build identity and decode capabilities; also the shell's UiInfo block. */
  getServiceInfo() {
    return new Promise((resolve, reject) => {
      this.stub.getServiceInfo({}, (err, response) => {
        if (err) reject(err); else resolve(response);
      });
    });
  }

  close() {
    grpc.closeClient(this.stub);
  }
}
