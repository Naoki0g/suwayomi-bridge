#!/usr/bin/env python3
"""Universal Wildcard HTTP Reverse Proxy Gateway for Suwayomi Extensions on VPS.

Forwards all HTTP requests (any method, path, query, headers, body) arriving on
127.0.0.1:8192 inside the VPS container namespace to the local PC extension proxy
connected via reverse SSH tunnel (default: 172.19.0.1:18192).

This file is a universal wildcard and does NOT need to be modified when adding
new extensions, new routes, or updating extension logic on the local PC.
"""

from __future__ import annotations

import http.client
import http.server
import os
import socketserver
import time

LISTEN_HOST = os.environ.get("GATEWAY_LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("GATEWAY_LISTEN_PORT", "8192"))
UPSTREAM_HOST = os.environ.get("UPSTREAM_HOST", "172.19.0.1")
UPSTREAM_PORT = int(os.environ.get("UPSTREAM_PORT", "18192"))
UPSTREAM_TIMEOUT = float(os.environ.get("UPSTREAM_TIMEOUT", "60"))

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


class WildcardGatewayHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args) -> None:
        if len(args) >= 3:
            msg = f"{args[0]} - {args[1]} {args[2]}"
        else:
            msg = fmt % args if args else fmt
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [gateway] {msg}", flush=True)

    def _send_raw(self, status: int, content_type: str, body: bytes, head_only: bool = False) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head_only and body:
            self.wfile.write(body)

    def _proxy(self, method: str, head_only: bool = False) -> None:
        if self.path == "/health":
            self._send_raw(200, "text/plain", b"OK", head_only=head_only)
            return

        content_len = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(content_len) if content_len > 0 else None

        fwd_headers: dict[str, str] = {}
        for k, v in self.headers.items():
            if k.lower() not in HOP_BY_HOP_HEADERS:
                fwd_headers[k] = v
        fwd_headers["Connection"] = "close"

        conn: http.client.HTTPConnection | None = None
        try:
            conn = http.client.HTTPConnection(UPSTREAM_HOST, UPSTREAM_PORT, timeout=UPSTREAM_TIMEOUT)
            conn.request(method, self.path, body=body, headers=fwd_headers)
            resp = conn.getresponse()
            resp_body = b"" if head_only else resp.read()

            self.send_response(resp.status)
            sent_content_len = False
            for hk, hv in resp.getheaders():
                hkl = hk.lower()
                if hkl in HOP_BY_HOP_HEADERS:
                    continue
                if hkl == "content-length":
                    sent_content_len = True
                self.send_header(hk, hv)
            if not sent_content_len:
                self.send_header("Content-Length", str(len(resp_body)))
            self.end_headers()
            if not head_only and resp_body:
                self.wfile.write(resp_body)
        except Exception as exc:
            err_msg = f"Wildcard Gateway: tunnel to local PC ({UPSTREAM_HOST}:{UPSTREAM_PORT}) unavailable: {exc}"
            self._send_raw(502, "text/plain; charset=utf-8", err_msg.encode("utf-8"), head_only=head_only)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def do_GET(self) -> None:
        self._proxy("GET", head_only=False)

    def do_POST(self) -> None:
        self._proxy("POST", head_only=False)

    def do_HEAD(self) -> None:
        self._proxy("HEAD", head_only=True)

    def do_PUT(self) -> None:
        self._proxy("PUT", head_only=False)

    def do_DELETE(self) -> None:
        self._proxy("DELETE", head_only=False)

    def do_PATCH(self) -> None:
        self._proxy("PATCH", head_only=False)

    def do_OPTIONS(self) -> None:
        self._proxy("OPTIONS", head_only=False)


def main() -> None:
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    with socketserver.ThreadingTCPServer((LISTEN_HOST, LISTEN_PORT), WildcardGatewayHandler) as httpd:
        print(
            f"Wildcard Gateway listening on {LISTEN_HOST}:{LISTEN_PORT} -> {UPSTREAM_HOST}:{UPSTREAM_PORT}",
            flush=True,
        )
        httpd.serve_forever()


if __name__ == "__main__":
    main()
