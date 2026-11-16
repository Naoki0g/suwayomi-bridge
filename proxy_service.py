#!/usr/bin/env python3
"""Local HTTP proxy and cryptographic worker bridge."""

from __future__ import annotations

import base64
from collections import OrderedDict
import glob
import hashlib
import http.server
import importlib.util
import json
import os
import re
import select
import shutil
import socketserver
import subprocess
import sys
import threading
import time
import urllib.parse

from bs4 import BeautifulSoup
from curl_cffi import requests

PORT = int(os.environ.get("PROXY_PORT", "8192"))
CANONICAL_ORIGIN = os.environ.get("SOURCE_A_URL", "https://example-source.com")
SITE_BASE = os.environ.get("SOURCE_A_SITE_BASE", CANONICAL_ORIGIN).rstrip("/")
PATH_PREFIX = os.environ.get("SOURCE_A_PATH_PREFIX", "/dist/engine")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXTENSIONS_DIR = os.environ.get("EXTENSIONS_DIR", os.path.join(BASE_DIR, "extensions"))
os.makedirs(EXTENSIONS_DIR, exist_ok=True)
WORKER_RUNNER_PATH = os.environ.get(
    "WORKER_RUNNER_PATH",
    os.path.join(BASE_DIR, "worker_bridge.js"),
)
CACHE_DIR = os.environ.get(
    "CACHE_DIR",
    "/tmp/stream_img_cache" if os.path.isdir("/tmp") else os.path.join(BASE_DIR, "cache"),
)
os.makedirs(CACHE_DIR, exist_ok=True)

DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

MANGA_HEADERS = {
    "X-Verification-Key-1": "927c3574-4702-4ede-9d4b-70cc2d66b09b",
    "X-Verification-Key-2": "03eb3f36-f92f-4516-8d94-f74c5316a623",
    "X-Client-Signature": "MangaSig_23a09a0b13760be2",
}

CAPITULO_HEADERS = {
    "X-Verification-Key-1": "7ac3b23f-60a8-41d2-8138-dd505359d9a3",
    "X-Verification-Key-2": "9f03071a-5b63-476a-87e8-a838c6a1c5a7",
    "X-Client-Signature": "ChapterSig_037cc0882bfa7eee",
}

CHAPTER_CIPHER_KEY = "CvqaUvz9fNSJn9nQrcPabDJaaRm2usjT"
_CORE_KEYS_LOCK = threading.Lock()
_LAST_MANGA_CORE_URL = ""
_LAST_CAPITULO_CORE_URL = ""

MAX_PAGE_CACHE_BYTES = 256 * 1024 * 1024
MAX_PAGE_CACHE_ITEMS = 1500
MAX_PAGE_SPEC_ITEMS = 2500
CHAPTER_CACHE_TTL_SEC = 180.0
MANGA_CHAPTERS_TTL_SEC = 120.0
API_GET_TTL_SEC = 60.0

_THREAD_LOCAL = threading.local()
_WORKER_CODE_LOCK = threading.Lock()
_WORKER_CODE_CACHE: dict[str, str] = {}

_PAGE_CACHE_LOCK = threading.Lock()
_PAGE_IMAGE_CACHE: OrderedDict[str, tuple[bytes, str]] = OrderedDict()
_PAGE_IMAGE_TOTAL_BYTES = 0
_PAGE_SPEC_CACHE: OrderedDict[str, dict] = OrderedDict()
_PAGE_FETCH_LOCKS: dict[str, threading.Lock] = {}
_CHAPTER_PAGES_CACHE: dict[str, tuple[float, list[str]]] = {}

_META_CACHE_LOCK = threading.Lock()
_MANGA_CHAPTERS_CACHE: dict[str, tuple[float, list[dict]]] = {}
_API_GET_CACHE: dict[str, tuple[float, int, str, str, bytes]] = {}


def resolve_node_binary() -> str:
    """Locate a working Node.js binary on this machine."""
    candidates: list[str] = []
    env_bin = os.environ.get("NODE_BIN")
    if env_bin:
        candidates.append(env_bin)
    candidates.append(os.path.expanduser("~/.local/bin/node"))
    which_node = shutil.which("node")
    if which_node:
        candidates.append(which_node)
    candidates.extend(sorted(glob.glob(os.path.expanduser("~/.vscode/cli/servers/*/server/node")), reverse=True))
    candidates.append("/usr/bin/node")

    seen = set()
    for cand in candidates:
        if not cand or cand in seen or not os.path.isfile(cand):
            continue
        seen.add(cand)
        try:
            res = subprocess.run([cand, "-v"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3)
            if res.returncode == 0:
                return cand
        except Exception:
            continue
    return "node"


NODE_BINARY = resolve_node_binary()


def get_proxies() -> dict[str, str] | None:
    """Return configured curl_cffi proxy mapping. Defaults to direct connection."""
    if os.environ.get("NO_PROXY") == "1":
        return None
    custom = os.environ.get("UPSTREAM_PROXY", "").strip()
    if custom:
        return {"all": custom}
    return None


def create_session(cookies: dict[str, str] | None = None) -> requests.Session:
    """Create an isolated curl_cffi Chrome 120 session with optional cookies."""
    sess = requests.Session(
        impersonate="chrome120",
        proxies=get_proxies(),
        headers={"User-Agent": DEFAULT_UA},
    )
    if cookies:
        for k, v in cookies.items():
            sess.cookies.set(k, v)
    return sess


def get_thread_session() -> requests.Session:
    """Return a per-thread curl_cffi session for /api and /image handlers."""
    sess = getattr(_THREAD_LOCAL, "session", None)
    if sess is None:
        sess = create_session()
        _THREAD_LOCAL.session = sess
    return sess


class UpstreamHttpError(Exception):
    """Represents a mapped HTTP error status for client responses."""

    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


class WorkerClient:
    """Thread-safe persistent RPC client backed by worker_bridge.js."""

    def __init__(self, runner_path: str):
        self.runner_path = runner_path
        self._lock = threading.Lock()
        self._proc: subprocess.Popen[str] | None = None
        self._next_id = 1

    def _drain_stderr(self, pipe) -> None:
        try:
            for line in pipe:
                text = line.rstrip()
                if text:
                    print(f"[worker-stderr] {text}", file=sys.stderr, flush=True)
        except Exception:
            pass

    def _ensure_started(self) -> subprocess.Popen[str]:
        if self._proc is None or self._proc.poll() is not None:
            proc = subprocess.Popen(
                [NODE_BINARY, self.runner_path],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
            if proc.stderr is not None:
                threading.Thread(
                    target=self._drain_stderr,
                    args=(proc.stderr,),
                    daemon=True,
                ).start()
            self._proc = proc
        return self._proc

    def _reset_proc(self) -> None:
        if self._proc is not None:
            try:
                self._proc.kill()
                self._proc.wait(timeout=2)
            except Exception:
                pass
            self._proc = None

    def call(
        self,
        payload: dict,
        timeout_sec: float = 25.0,
        module_fetcher=None,
    ):
        """Send one NDJSON command to the Node worker runner and wait for its reply."""
        with self._lock:
            last_err: Exception | None = None
            for attempt in range(2):
                proc = self._ensure_started()
                req_id = self._next_id
                self._next_id += 1
                msg = dict(payload)
                msg["id"] = req_id
                upstream_err: Exception | None = None
                try:
                    assert proc.stdin is not None and proc.stdout is not None
                    proc.stdin.write(json.dumps(msg, separators=(",", ":")) + "\n")
                    proc.stdin.flush()

                    deadline = time.monotonic() + timeout_sec
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise TimeoutError(f"Worker RPC timeout for op={msg.get('op')}")
                        ready, _, _ = select.select([proc.stdout], [], [], remaining)
                        if not ready:
                            raise TimeoutError(f"Worker RPC timeout for op={msg.get('op')}")
                        line = proc.stdout.readline()
                        if not line:
                            raise EOFError("Worker runner stdout closed unexpectedly")
                        resp = json.loads(line)
                        if resp.get("type") == "fetch_module":
                            mod_url = str(resp.get("url") or "")
                            try:
                                if module_fetcher is None:
                                    raise RuntimeError(f"No module fetcher for {mod_url}")
                                mod_code = module_fetcher(mod_url)
                                reply = {"type": "module_script", "url": mod_url, "code": mod_code}
                            except Exception as exc:
                                if isinstance(exc, UpstreamHttpError):
                                    upstream_err = exc
                                reply = {"type": "module_script", "url": mod_url, "error": str(exc)}
                            proc.stdin.write(json.dumps(reply, separators=(",", ":")) + "\n")
                            proc.stdin.flush()
                            continue
                        if resp.get("id") != req_id:
                            continue
                        if not resp.get("ok"):
                            if upstream_err is not None:
                                raise upstream_err
                            raise RuntimeError(resp.get("error") or "Worker returned failure")
                        return resp.get("result")
                except (BrokenPipeError, EOFError, TimeoutError, json.JSONDecodeError) as exc:
                    last_err = exc
                    self._reset_proc()
                    if attempt == 0:
                        continue
                    raise RuntimeError(f"Worker process failed: {exc}") from exc
            raise RuntimeError(f"Worker process failed: {last_err}")


_WORKER_CLIENT = WorkerClient(WORKER_RUNNER_PATH)


def ror8(value: int, bits: int) -> int:
    return ((value >> bits) | (value << (8 - bits))) & 0xFF


def _fnv1a32(data: bytes | bytearray, offset: int, end: int, seed: int = 2166136261) -> int:
    """Compute FNV-1a 32-bit hash matching FUNC 270 (_0x2e9a74) in capitulo.core.js."""
    h = seed & 0xFFFFFFFF
    for i in range(offset, end):
        h = (h ^ data[i]) & 0xFFFFFFFF
        h = ((h << 24) + h * 403) & 0xFFFFFFFF
    return h


def decode_meta_v4(encoded: str, cipher_key: str | None = None) -> str:
    """Decode v4: base64url + xorshift32 S-box/permutation + FNV-1a32 CRC <meta> payloads."""
    if not encoded.startswith("v4:"):
        raise ValueError("Not a v4: payload")
    b64 = encoded[3:].replace("-", "+").replace("_", "/")
    b64 += "=" * (-len(b64) % 4)
    raw = base64.b64decode(b64)
    n = len(raw) - 4
    if n < 5:
        raise ValueError("v4: payload too short")

    key_str = cipher_key or CHAPTER_CIPHER_KEY
    key_bytes = key_str.encode("utf-8")
    seed = _fnv1a32(raw, 0, 4, _fnv1a32(key_bytes, 0, len(key_bytes), 2166136261))
    state = seed if seed != 0 else 2654435769
    prev = state & 0xFF

    def next_u32() -> int:
        nonlocal state
        state = (state ^ ((state << 13) & 0xFFFFFFFF)) & 0xFFFFFFFF
        state = (state ^ (state >> 17)) & 0xFFFFFFFF
        state = (state ^ ((state << 5) & 0xFFFFFFFF)) & 0xFFFFFFFF
        return state

    sbox = list(range(256))
    for i in range(255, 0, -1):
        j = next_u32() % (i + 1)
        sbox[i], sbox[j] = sbox[j], sbox[i]
    inv_sbox = [0] * 256
    for i, v in enumerate(sbox):
        inv_sbox[v] = i

    perm = list(range(n))
    for i in range(n - 1, 0, -1):
        j = next_u32() % (i + 1)
        perm[i], perm[j] = perm[j], perm[i]

    out = bytearray(n)
    for i in range(n):
        mask = (next_u32() >> 8) & 0xFF
        c = raw[4 + perm[i]]
        out[i] = ((inv_sbox[c] ^ prev) - mask) & 0xFF
        prev = c

    pt_len = n - 4
    expected_crc = (
        (out[pt_len] << 24)
        | (out[pt_len + 1] << 16)
        | (out[pt_len + 2] << 8)
        | out[pt_len + 3]
    ) & 0xFFFFFFFF
    actual_crc = _fnv1a32(out, 0, pt_len, 2166136261)
    if actual_crc != expected_crc:
        raise ValueError(f"v4: CRC mismatch ({actual_crc} != {expected_crc})")
    return bytes(out[:pt_len]).decode("utf-8")


def refresh_core_keys_from_js(sess: requests.Session, html: str, page_url: str, surface: str, force: bool = False) -> bool:
    """Extract X-Verification-Key-1/2, X-Client-Signature, and cipher_key from manga/capitulo core.js if rotated."""
    global _LAST_MANGA_CORE_URL, _LAST_CAPITULO_CORE_URL, CHAPTER_CIPHER_KEY
    pat = (
        rf"{re.escape(PATH_PREFIX)}/pages/manga/manga\.[^\"'\s>]+\.core\.js"
        if surface == "manga"
        else rf"{re.escape(PATH_PREFIX)}/pages/capitulo/capitulo\.[^\"'\s>]+\.core\.js"
    )
    m = re.search(pat, html)
    if not m:
        return False
    core_url = urllib.parse.urljoin(SITE_BASE + "/", m.group(0).lstrip("/"))
    with _CORE_KEYS_LOCK:
        last_url = _LAST_MANGA_CORE_URL if surface == "manga" else _LAST_CAPITULO_CORE_URL
        if not force and last_url == core_url:
            return False
        try:
            resp = sess.get(core_url, headers={"Referer": page_url, "Accept": "*/*"}, timeout=20)
            if resp.status_code != 200 or not resp.text:
                return False
            node_script = (
                "let fs=require('fs'),js=fs.readFileSync(0,'utf8'),orig=String.fromCharCode,b='';"
                "String.fromCharCode=function(...a){let r=orig.apply(this,a);b+=r;return r;};"
                "globalThis.window=globalThis;globalThis.self=globalThis;"
                "try{(0,eval)(js);}catch(e){}"
                "process.stdout.write(b);"
            )
            proc = subprocess.run(
                [NODE_BINARY, "-e", node_script],
                input=resp.text,
                text=True,
                capture_output=True,
                timeout=12,
            )
            out = proc.stdout or ""
            m_keys = re.search(
                r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})X-Verification-Key-1"
                r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})X-Verification-Key-2"
                r"verification_keys((?:MangaSig|ChapterSig)_[0-9a-fA-F]+)client_signature"
                r"(?:([A-Za-z0-9]{16,64})cipher_key)?",
                out,
            )
            if not m_keys:
                return False
            k1, k2, sig, ckey = m_keys.group(1), m_keys.group(2), m_keys.group(3), m_keys.group(4)
            if surface == "manga":
                MANGA_HEADERS["X-Verification-Key-1"] = k1
                MANGA_HEADERS["X-Verification-Key-2"] = k2
                MANGA_HEADERS["X-Client-Signature"] = sig
                _LAST_MANGA_CORE_URL = core_url
            else:
                CAPITULO_HEADERS["X-Verification-Key-1"] = k1
                CAPITULO_HEADERS["X-Verification-Key-2"] = k2
                CAPITULO_HEADERS["X-Client-Signature"] = sig
                if ckey:
                    CHAPTER_CIPHER_KEY = ckey
                _LAST_CAPITULO_CORE_URL = core_url
            return True
        except Exception as exc:
            print(f"[core-keys] Failed to refresh {surface} keys: {exc}", file=sys.stderr, flush=True)
            return False


def decode_meta(encoded: str, key: bytes = b"0xFF") -> str:
    """Decode v2:/v3:/v4: obfuscated <meta> payloads."""
    if encoded.startswith("v4:"):
        return decode_meta_v4(encoded)
    is_v2 = False
    is_v3 = False
    if encoded.startswith("v2:"):
        is_v2 = True
        hex_str = encoded[3:]
    elif encoded.startswith("v3:"):
        is_v3 = True
        hex_str = encoded[3:]
    else:
        hex_str = encoded

    raw = bytes.fromhex(hex_str)
    n = len(raw)
    l7 = (n + 2) // 3
    l8 = (n + 1) // 3
    s0 = raw[:l7]
    s1 = raw[l7 : l7 + l8]
    s2 = raw[l7 + l8 :]

    uninterleaved = bytearray(n)
    for i in range(n):
        m = i % 3
        q = i // 3
        uninterleaved[i] = s0[q] if m == 0 else (s1[q] if m == 1 else s2[q])

    kl = len(key)
    out = bytearray(n)
    for i in range(n):
        c = uninterleaved[i]
        k0 = (key[i % kl] + ((i * 29 + 17) & 0xFF)) & 0xFF
        k1 = (key[(i + 3) % kl] + ((i * 73 + 41) & 0xFF)) & 0xFF
        k2 = (key[(i + 5) % kl] ^ ((i * 151 + 97) & 0xFF)) & 0xFF
        k3 = (key[(i + 7) % kl] + ((i * 199 + 13) & 0xFF)) & 0xFF
        k4 = (k0 + k3) & 0xFF
        k5 = (key[(i + 2) % kl] + ((i * 43 + 61) & 0xFF)) & 0xFF
        k6 = (key[(i + 4) % kl] ^ ((i * 89 + 23) & 0xFF)) & 0xFF
        k7 = (key[(i + 6) % kl] + ((i * 113 + 7) & 0xFF)) & 0xFF
        k8 = (key[(i + 8) % kl] ^ ((i * 197 + 53) & 0xFF)) & 0xFF
        k9 = (k1 + k7) & 0xFF
        k10 = (k2 ^ k5) & 0xFF
        if not is_v2 and not is_v3:
            c = ror8(c, 3) ^ k0
        else:
            if is_v3:
                c = (c + k0) & 0xFF
                c ^= k10
                c = ror8(c, 4)
                c = (c - k9) & 0xFF
                c ^= k8
                c = ror8(c, 1)
                c = (c + k7) & 0xFF
                c ^= k6
                c = ror8(c, 7)
                c = (c - k5) & 0xFF
            c ^= k1
            c = ror8(c, 2)
            c = (c - k2) & 0xFF
            c ^= k4
            c = ror8(c, 5)
            c = (c - k3) & 0xFF
            c ^= k2
            c = ror8(c, 3)
            c = (c - k1) & 0xFF
            c ^= k0
        out[i] = c
    return bytes(out).decode("utf-8")


def decode_xor_payload(enc_b64: str) -> dict:
    """Decode base64 -> XOR cipher -> base64 -> JSON used by chapter API."""
    stage1 = base64.b64decode(enc_b64)
    key = os.environ.get("SOURCE_A_XOR_KEY", "EngineCSS").encode("utf-8")
    kl = len(key)
    xored = bytes(b ^ key[i % kl] for i, b in enumerate(stage1))
    inner = base64.b64decode(xored).decode("utf-8")
    return json.loads(inner)


def parse_header_challenge(raw_challenge: str) -> dict:
    """Decode middle base64url JSON segment of an H1.<payload>.<sig> token."""
    parts = raw_challenge.split(".")
    if len(parts) < 3 or parts[0] != "H1":
        raise UpstreamHttpError(502, "Invalid header-challenge token format")
    b64url = parts[1].replace("-", "+").replace("_", "/")
    b64url += "=" * (-len(b64url) % 4)
    return json.loads(base64.b64decode(b64url).decode("utf-8"))


def parse_poly_auth(poly_raw: str | None) -> dict[str, str]:
    """Decode poly-auth meta tag into HTTP headers required for .bin fetches."""
    if not poly_raw:
        return {}
    data = json.loads(decode_meta(poly_raw, b"0xFF"))
    raw_headers = data.get("headers") if isinstance(data, dict) else None
    out: dict[str, str] = {}
    if isinstance(raw_headers, dict):
        for k, v in raw_headers.items():
            if isinstance(k, str) and v is not None:
                out[k] = str(v)
    elif isinstance(raw_headers, list):
        for item in raw_headers:
            if isinstance(item, dict) and item.get("name") and item.get("value") is not None:
                out[str(item["name"])] = str(item["value"])
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                out[str(item[0])] = str(item[1])
    return out


def parse_kaguya_worker_meta(kaguya_raw: str | None) -> str:
    """Decode kaguya-worker meta tag to extract the worker script identifier."""
    if not kaguya_raw:
        raise UpstreamHttpError(502, "Missing meta[name=kaguya-worker]")
    decoded = decode_meta(kaguya_raw, b"0xFF").strip()
    if re.fullmatch(r"[a-z]{3,20}_[a-z0-9]{5}", decoded):
        return decoded
    obj = json.loads(decoded)
    worker = obj.get("worker") if isinstance(obj, dict) else None
    if not isinstance(worker, str) or not re.fullmatch(r"[a-z]{3,20}_[a-z0-9]{5}", worker):
        raise UpstreamHttpError(502, "Invalid kaguya-worker metadata payload")
    return worker


def normalize_site_url(raw_url: str) -> str:
    """Ensure a site URL is absolute against SITE_BASE."""
    trimmed = raw_url.strip()
    if trimmed.startswith(("http://", "https://")):
        return trimmed
    return urllib.parse.urljoin(SITE_BASE + "/", trimmed.lstrip("/"))


def to_relative_url(raw_url: str) -> str:
    """Normalize an internal site URL to a leading-slash path for Suwayomi SChapter."""
    trimmed = raw_url.strip()
    if trimmed.startswith(("http://", "https://")):
        parsed = urllib.parse.urlparse(trimmed)
        path = parsed.path or "/"
        if parsed.query:
            return f"{path}?{parsed.query}"
        return path
    return "/" + trimmed.lstrip("/")


def check_http_response(resp, is_html: bool = False) -> None:
    """Raise structured UpstreamHttpError on rate limits, captchas, or CF blocks."""
    final_url = str(getattr(resp, "url", "") or "")
    status = int(resp.status_code)
    body_preview = resp.text[:4096] if is_html or status >= 400 else ""

    if status == 429 or "/rate_limit" in final_url or "Muitas requisições" in body_preview:
        raise UpstreamHttpError(429, "Upstream source rate limit active (/rate_limit)")
    if "/manutencao/block.php" in final_url:
        raise UpstreamHttpError(403, "Upstream source access block (/manutencao/block.php)")
    if status == 403 or "Just a moment..." in body_preview or "cf-browser-verification" in body_preview:
        raise UpstreamHttpError(403, f"Upstream blocked request (HTTP {status})")
    if status >= 400:
        raise UpstreamHttpError(502, f"Upstream returned HTTP {status} for {final_url or 'target'}")


def check_api_payload(obj: dict) -> None:
    """Inspect JSON response for API-level rate_limit or captcha_required states."""
    if not isinstance(obj, dict):
        return
    status = str(obj.get("status") or "").lower()
    if status == "rate_limit" or obj.get("rate_limit"):
        raise UpstreamHttpError(429, "Upstream API rate_limit active")
    if status == "captcha_required" or obj.get("captcha_required"):
        raise UpstreamHttpError(403, "Upstream captcha_required (manual verification needed)")


def get_worker_code(sess: requests.Session, worker_url: str, referer: str) -> str:
    """Fetch and cache worker JS bundle in memory (bypassing stale disk copies)."""
    with _WORKER_CODE_LOCK:
        cached = _WORKER_CODE_CACHE.get(worker_url)
        if cached is not None:
            return cached

    resp = sess.get(
        worker_url,
        headers={"Referer": referer, "Accept": "*/*"},
        timeout=20,
    )
    check_http_response(resp, is_html=False)
    code = resp.text
    if "<html" in code[:256].lower():
        raise UpstreamHttpError(502, f"Expected JS worker at {worker_url}, received HTML")

    with _WORKER_CODE_LOCK:
        _WORKER_CODE_CACHE[worker_url] = code
    return code


def detect_image_mime(raw: bytes, fallback: str | None = None) -> str:
    """Detect image MIME type from file magic bytes."""
    if raw.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if raw.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if raw.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if len(raw) >= 12 and raw[:4] == b"RIFF" and raw[8:12] == b"WEBP":
        return "image/webp"
    if fallback and fallback.startswith("image/"):
        return fallback.split(";", 1)[0].strip()
    return "image/jpeg"


def store_page_image(page_id: str, data: bytes, mime_type: str) -> None:
    """Store decrypted page image in bounded LRU memory cache."""
    global _PAGE_IMAGE_TOTAL_BYTES
    with _PAGE_CACHE_LOCK:
        old = _PAGE_IMAGE_CACHE.pop(page_id, None)
        if old is not None:
            _PAGE_IMAGE_TOTAL_BYTES -= len(old[0])

        _PAGE_IMAGE_CACHE[page_id] = (data, mime_type)
        _PAGE_IMAGE_TOTAL_BYTES += len(data)

        while (
            len(_PAGE_IMAGE_CACHE) > MAX_PAGE_CACHE_ITEMS
            or _PAGE_IMAGE_TOTAL_BYTES > MAX_PAGE_CACHE_BYTES
        ) and len(_PAGE_IMAGE_CACHE) > 1:
            _, (evicted_data, _) = _PAGE_IMAGE_CACHE.popitem(last=False)
            _PAGE_IMAGE_TOTAL_BYTES -= len(evicted_data)


def get_cached_page_image(page_id: str) -> tuple[bytes, str] | None:
    """Retrieve and refresh LRU position of a cached page image."""
    with _PAGE_CACHE_LOCK:
        entry = _PAGE_IMAGE_CACHE.get(page_id)
        if entry is None:
            return None
        _PAGE_IMAGE_CACHE.move_to_end(page_id)
        return entry


def store_page_spec(page_id: str, spec_entry: dict) -> None:
    """Store a lazy page fetch/decryption descriptor in bounded LRU memory."""
    with _PAGE_CACHE_LOCK:
        _PAGE_SPEC_CACHE[page_id] = spec_entry
        _PAGE_SPEC_CACHE.move_to_end(page_id)
        while len(_PAGE_SPEC_CACHE) > MAX_PAGE_SPEC_ITEMS:
            evicted_id, _ = _PAGE_SPEC_CACHE.popitem(last=False)
            _PAGE_FETCH_LOCKS.pop(evicted_id, None)


def get_page_spec_and_lock(page_id: str) -> tuple[dict | None, threading.Lock | None]:
    """Look up a lazy page spec and obtain its per-page serialization lock."""
    with _PAGE_CACHE_LOCK:
        spec_entry = _PAGE_SPEC_CACHE.get(page_id)
        if spec_entry is None:
            return None, None
        _PAGE_SPEC_CACHE.move_to_end(page_id)
        lock = _PAGE_FETCH_LOCKS.get(page_id)
        if lock is None:
            lock = threading.Lock()
            _PAGE_FETCH_LOCKS[page_id] = lock
        return spec_entry, lock


def get_or_fetch_page_image(page_id: str) -> tuple[bytes, str, str]:
    """Return (bytes, mime_type, cache_status), lazily fetching & decrypting on miss."""
    cached = get_cached_page_image(page_id)
    if cached is not None:
        return cached[0], cached[1], "HIT"

    spec_entry, page_lock = get_page_spec_and_lock(page_id)
    if spec_entry is None or page_lock is None:
        raise UpstreamHttpError(404, "Page image expired or not found")

    with page_lock:
        cached = get_cached_page_image(page_id)
        if cached is not None:
            return cached[0], cached[1], "HIT"
        _, img_bytes, mime_type = fetch_and_decrypt_page(**spec_entry)
        store_page_image(page_id, img_bytes, mime_type)
        return img_bytes, mime_type, "MISS"


def parse_chapter_number(num_str: str) -> float:
    m = re.search(r"\d+(?:\.\d+)?", num_str)
    if not m:
        return -1.0
    try:
        return float(m.group(0))
    except ValueError:
        return -1.0


def format_scanlators(scans) -> str:
    if isinstance(scans, list):
        parts = []
        for s in scans:
            if isinstance(s, dict):
                name = s.get("nome") or s.get("name") or ""
            else:
                name = str(s or "")
            if name.strip():
                parts.append(name.strip())
        return ", ".join(parts)
    if isinstance(scans, str):
        return scans.strip()
    return ""


def normalize_chapter_entries(items: list) -> list[dict]:
    """Normalize Source chapter + versoes objects into flat Suwayomi chapter DTOs."""
    result: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        num_str = str(item.get("numero") or "").strip()
        ch_num = parse_chapter_number(num_str)
        base_title = str(item.get("titulo") or "").strip()
        base_date = str(item.get("data_amigavel") or "").strip()
        versoes = item.get("versoes")

        if isinstance(versoes, list) and versoes:
            for ver in versoes:
                if not isinstance(ver, dict):
                    continue
                raw_url = str(ver.get("url") or "").strip()
                if not raw_url:
                    continue
                ver_title = str(ver.get("titulo") or base_title or "").strip()
                if num_str:
                    name = f"Cap. {num_str}" + (f" - {ver_title}" if ver_title else "")
                else:
                    name = ver_title or "Capítulo"
                result.append(
                    {
                        "name": name,
                        "url": to_relative_url(raw_url),
                        "date": str(ver.get("data_amigavel") or base_date or "").strip(),
                        "scanlator": format_scanlators(ver.get("scans")),
                        "chapter_number": ch_num,
                    }
                )
        else:
            raw_url = str(item.get("url") or "").strip()
            if not raw_url:
                continue
            if num_str:
                name = f"Cap. {num_str}" + (f" - {base_title}" if base_title else "")
            else:
                name = base_title or "Capítulo"
            result.append(
                {
                    "name": name,
                    "url": to_relative_url(raw_url),
                    "date": base_date,
                    "scanlator": format_scanlators(item.get("scans")),
                    "chapter_number": ch_num,
                }
            )
    return result


def handle_manga_chapters(target_url: str) -> list[dict]:
    """Fetch full paginated chapter list for a manga via Hydra proof + cipher API."""
    full_url = normalize_site_url(target_url)
    cache_key = full_url.rstrip("/")
    now = time.monotonic()
    with _META_CACHE_LOCK:
        cached = _MANGA_CHAPTERS_CACHE.get(cache_key)
        if cached is not None and (now - cached[0]) < MANGA_CHAPTERS_TTL_SEC:
            return cached[1]

    sess = create_session()

    page_resp = sess.get(
        full_url,
        headers={
            "Referer": f"{SITE_BASE}/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        timeout=20,
    )
    check_http_response(page_resp, is_html=True)

    soup = BeautifulSoup(page_resp.text, "html.parser")
    manga_meta = soup.select_one("meta[manga-id]")
    manga_id = manga_meta.get("manga-id") if manga_meta else None
    if not manga_id:
        raise UpstreamHttpError(502, "Missing meta[manga-id] on manga page")

    chal_meta = soup.select_one('meta[name="header-challenge"]')
    raw_challenge = chal_meta.get("content") if chal_meta else None
    key_meta = soup.select_one('meta[name="security-key-name"]')
    key_name = key_meta.get("content") if key_meta else None
    csrf_meta = soup.select_one('meta[name="csrf-token"]')
    csrf = csrf_meta.get("content") if csrf_meta else None

    worker_file = None
    worker_url = None
    worker_code = None
    use_hydra = key_name == "manga_info" and isinstance(raw_challenge, str) and raw_challenge.startswith("H1.")
    if use_hydra:
        hinfo = parse_header_challenge(raw_challenge)
        worker_file = hinfo.get("workerFile")
        if not worker_file:
            raise UpstreamHttpError(502, "Missing workerFile inside manga header-challenge")
        worker_url = f"{SITE_BASE}{PATH_PREFIX}/__hydra/manga/{worker_file}.js?transport=3"
        worker_vm_url = f"{CANONICAL_ORIGIN}{PATH_PREFIX}/__hydra/manga/{worker_file}.js?transport=3"
        worker_code = get_worker_code(sess, worker_url, referer=full_url)

    api_url = f"{SITE_BASE}{PATH_PREFIX}/models/manga/..__obf__manga_capitulos.php"
    headers = {
        **MANGA_HEADERS,
        "Referer": full_url,
        "Origin": SITE_BASE,
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }
    if csrf:
        headers["X-CSRF-Token"] = csrf

    all_chapters: list[dict] = []
    offset = 0
    limit = 90
    for _ in range(100):
        payload = {
            "manga_id": str(manga_id),
            "offset": str(offset),
            "order": "desc",
            "limit": str(limit),
        }
        if use_hydra and raw_challenge and worker_file and worker_code:
            proof = _WORKER_CLIENT.call(
                {
                    "op": "hydra_prove",
                    "surface": "manga",
                    "workerFile": worker_file,
                    "workerCode": worker_code,
                    "workerUrl": worker_vm_url,
                    "challenge": raw_challenge,
                    "userAgent": DEFAULT_UA,
                }
            )
            payload["proof"] = proof
            payload["challenge"] = raw_challenge

        api_resp = sess.post(api_url, headers=headers, data=payload, timeout=20)
        if api_resp.status_code == 403 and offset == 0:
            if refresh_core_keys_from_js(sess, page_resp.text, full_url, "manga", force=True):
                headers.update(MANGA_HEADERS)
                api_resp = sess.post(api_url, headers=headers, data=payload, timeout=20)
        check_http_response(api_resp, is_html=False)
        res_json = api_resp.json()
        check_api_payload(res_json)

        if isinstance(res_json, dict) and "_enc" in res_json:
            decoded = decode_xor_payload(res_json["_enc"])
        else:
            decoded = res_json

        check_api_payload(decoded)
        if not isinstance(decoded, dict) or not decoded.get("success", False):
            raise UpstreamHttpError(
                502,
                f"manga_capitulos API failed: {decoded.get('message') if isinstance(decoded, dict) else decoded}",
            )

        items = decoded.get("data") or []
        if not isinstance(items, list):
            raise UpstreamHttpError(502, "Unexpected manga_capitulos data format")

        all_chapters.extend(normalize_chapter_entries(items))
        if not decoded.get("has_more") or len(items) == 0:
            break
        offset += len(items)

    with _META_CACHE_LOCK:
        if len(_MANGA_CHAPTERS_CACHE) > 256:
            _MANGA_CHAPTERS_CACHE.clear()
        _MANGA_CHAPTERS_CACHE[cache_key] = (time.monotonic(), all_chapters)

    return all_chapters


def expand_populated_pages(urls_obj, aes_key: str | None, base_page_url: str) -> tuple[list[dict], str | None]:
    """Expand decrypted Kaguya/Yggdrasil page structure (array or range) into fetch specs."""
    specs: list[dict] = []
    if isinstance(urls_obj, list):
        for idx, u in enumerate(urls_obj, start=1):
            specs.append(
                {
                    "url": urllib.parse.urljoin(base_page_url, str(u)),
                    "mode": 0,
                    "page": idx,
                    "is_gif": False,
                }
            )
        return specs, None

    if isinstance(urls_obj, dict) and urls_obj.get("type") == "range":
        base = str(urls_obj.get("base") or "")
        mode = 1 if urls_obj.get("mode") == 1 else 0
        ext = str(urls_obj.get("ext") or (".bin" if mode == 1 else ".jpg"))
        gif_ext = str(urls_obj.get("gifExt") or ".gif")
        pages = int(urls_obj.get("pages") or 0)
        pad = int(urls_obj.get("pad") or 1)
        gif_pages = set(urls_obj.get("gifPages") or [])
        resolved_aes = str(urls_obj.get("aesKey") or aes_key or "") if mode == 1 else None
        if mode == 1 and not resolved_aes:
            raise UpstreamHttpError(502, "Missing aesKey in encrypted range payload")
        for page_num in range(1, pages + 1):
            is_gif = page_num in gif_pages
            num_str = str(page_num).zfill(pad)
            raw_u = base + num_str + (gif_ext if is_gif else ext)
            specs.append(
                {
                    "url": urllib.parse.urljoin(base_page_url, raw_u),
                    "mode": 0 if is_gif else mode,
                    "page": page_num,
                    "is_gif": is_gif,
                }
            )
        return specs, resolved_aes

    raise UpstreamHttpError(502, "Unrecognized decrypted page payload structure")


def decrypt_read_data_payload(
    sess: requests.Session,
    res_data,
    chapter_url: str,
    subtoken: str,
    kaguya_file: str,
    kaguya_code: str,
    kaguya_url: str,
    raw_challenge: str | None,
) -> tuple[list[dict], str | None]:
    """Decrypt __tron__capitulos__read.php response data using Yggdrasil and Kaguya workers."""
    ygg_url = f"{SITE_BASE}{PATH_PREFIX}/__yggdrasil/yggdrasil-nw.js?transport=5"
    ygg_vm_url = f"{CANONICAL_ORIGIN}{PATH_PREFIX}/__yggdrasil/yggdrasil-nw.js?transport=5"
    ygg_code = get_worker_code(sess, ygg_url, referer=chapter_url)

    def _map_worker_url(mod_url: str) -> str:
        if mod_url.startswith(CANONICAL_ORIGIN):
            return SITE_BASE + mod_url[len(CANONICAL_ORIGIN) :]
        return mod_url

    def ygg_decipher(cipher_str: str, key_str: str) -> str:
        res = _WORKER_CLIENT.call(
            {
                "op": "yggdrasil_decipher",
                "workerCode": ygg_code,
                "workerUrl": ygg_vm_url,
                "operation": "decipher",
                "data": cipher_str,
                "key": key_str,
                "userAgent": DEFAULT_UA,
            },
            timeout_sec=25.0,
            module_fetcher=lambda mod_url: get_worker_code(
                sess, _map_worker_url(mod_url), referer=chapter_url
            ),
        )
        return res if isinstance(res, str) else json.dumps(res)

    kaguya_vm_url = f"{CANONICAL_ORIGIN}{PATH_PREFIX}/__kaguya/{kaguya_file}.js?transport=3"

    def kaguya_decrypt(cipher_str: str, key_str: str, mode: int) -> str:
        return _WORKER_CLIENT.call(
            {
                "op": "kaguya_transform",
                "workerFile": kaguya_file,
                "workerCode": kaguya_code,
                "workerUrl": kaguya_vm_url,
                "action": "decrypt",
                "data": cipher_str,
                "key": key_str,
                "mode": mode,
                "challenge": raw_challenge or "",
                "userAgent": DEFAULT_UA,
            }
        )

    payload = res_data
    # Handle optional openPayload envelope if the backend wraps populate payload inside one.
    if isinstance(payload, dict) and "encryptedPayload" in payload and "encryptedPayloadKey" in payload:
        key_for_envelope = subtoken
        if payload.get("encryptedEphemeralKey"):
            key_for_envelope = ygg_decipher(payload["encryptedEphemeralKey"], subtoken)
        payload_key = kaguya_decrypt(payload["encryptedPayloadKey"], key_for_envelope, 1)
        opened_raw = kaguya_decrypt(payload["encryptedPayload"], payload_key, 0)
        payload = json.loads(opened_raw)

    if isinstance(payload, dict) and (
        "encryptedEphemeralKey" in payload
        and "encryptedImageKey" in payload
        and "encryptedUrls" in payload
    ):
        ephemeral_key = ygg_decipher(payload["encryptedEphemeralKey"], subtoken)
        image_key = kaguya_decrypt(payload["encryptedImageKey"], ephemeral_key, 1)
        urls_raw = kaguya_decrypt(payload["encryptedUrls"], image_key, 0)
        urls_obj = json.loads(urls_raw)
        return expand_populated_pages(urls_obj, payload.get("aesKey"), chapter_url)

    if isinstance(payload, list) or (isinstance(payload, dict) and payload.get("type") == "range"):
        return expand_populated_pages(payload, None, chapter_url)

    raise UpstreamHttpError(502, "Unexpected __tron__capitulos__read.php data shape")


def fetch_and_decrypt_page(
    spec: dict,
    chapter_url: str,
    cookies: dict[str, str],
    poly_headers: dict[str, str],
    chapter_id_int: int,
    aes_key: str | None,
) -> tuple[str, bytes, str]:
    """Fetch a single chapter page (.bin or plain image) and decrypt SKPG if present."""
    page_url = spec["url"]
    req_headers = {
        "Referer": chapter_url,
        "Accept": "application/octet-stream" if spec["mode"] == 1 else "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }
    for k, v in poly_headers.items():
        req_headers[k] = v

    sess = create_session(cookies=cookies)
    resp = sess.get(page_url, headers=req_headers, timeout=25)
    check_http_response(resp, is_html=False)
    raw_bytes = resp.content
    if not raw_bytes:
        raise UpstreamHttpError(502, f"Empty page response from {page_url}")

    if raw_bytes.startswith((b"SKPG\x01\x01", b"SKPG\x01\x02")) or (spec["mode"] == 1 and aes_key):
        if not aes_key:
            raise UpstreamHttpError(502, "Encountered SKPG page without aesKey")
        dec = _WORKER_CLIENT.call(
            {
                "op": "skpg_decrypt",
                "binBase64": base64.b64encode(raw_bytes).decode("ascii"),
                "chapterId": chapter_id_int,
                "pageNumber": int(spec["page"]),
                "aesKeyBase64": aes_key,
            }
        )
        img_bytes = base64.b64decode(dec["dataBase64"])
        mime_type = str(dec.get("mimeType") or detect_image_mime(img_bytes))
    else:
        img_bytes = raw_bytes
        mime_type = detect_image_mime(img_bytes, resp.headers.get("content-type"))

    page_id = hashlib.sha256(f"{chapter_url}#{spec['page']}".encode("utf-8")).hexdigest()[:24]
    return page_id, img_bytes, mime_type


def handle_chapter_pages(target_url: str, host_header: str) -> list[str]:
    """Resolve and cache all decrypted page images for a chapter, returning local image URLs."""
    full_url = normalize_site_url(target_url)

    with _PAGE_CACHE_LOCK:
        cached_entry = _CHAPTER_PAGES_CACHE.get(full_url)
        if cached_entry is not None:
            cached_at, page_ids = cached_entry
            if (time.monotonic() - cached_at) < CHAPTER_CACHE_TTL_SEC and all(
                (pid in _PAGE_IMAGE_CACHE or pid in _PAGE_SPEC_CACHE) for pid in page_ids
            ):
                for pid in page_ids:
                    if pid in _PAGE_IMAGE_CACHE:
                        _PAGE_IMAGE_CACHE.move_to_end(pid)
                return [f"http://{host_header}/page_image?id={pid}" for pid in page_ids]

    sess = create_session()
    page_resp = sess.get(
        full_url,
        headers={
            "Referer": f"{SITE_BASE}/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        timeout=20,
    )
    check_http_response(page_resp, is_html=True)

    soup = BeautifulSoup(page_resp.text, "html.parser")
    ch_meta = soup.select_one("meta[chapter-id]")
    tok_meta = soup.select_one("meta[token]")
    sub_meta = soup.select_one("meta[subtoken]")
    chapter_id = ch_meta.get("chapter-id") if ch_meta else None
    token = tok_meta.get("token") if tok_meta else None
    subtoken = sub_meta.get("subtoken") if sub_meta else None
    if not chapter_id or not token or not subtoken:
        raise UpstreamHttpError(502, "Missing chapter-id/token/subtoken meta tags on chapter page")

    chapter_id_int = int(chapter_id)
    chal_meta = soup.select_one('meta[name="header-challenge"]')
    raw_challenge = chal_meta.get("content") if chal_meta else None
    key_meta = soup.select_one('meta[name="security-key-name"]')
    key_name = key_meta.get("content") if key_meta else None
    csrf_meta = soup.select_one('meta[name="csrf-token"]')
    csrf = csrf_meta.get("content") if csrf_meta else None
    poly_meta = soup.select_one('meta[name="poly-auth"]')
    poly_raw = poly_meta.get("content") if poly_meta else None
    kaguya_meta = soup.select_one('meta[name="kaguya-worker"]')
    kaguya_raw = kaguya_meta.get("content") if kaguya_meta else None

    try:
        poly_headers = parse_poly_auth(poly_raw)
    except Exception:
        if refresh_core_keys_from_js(sess, page_resp.text, full_url, "capitulo", force=True):
            poly_headers = parse_poly_auth(poly_raw)
        else:
            raise
    hinfo = parse_header_challenge(raw_challenge) if isinstance(raw_challenge, str) and raw_challenge.startswith("H1.") else {}
    if kaguya_raw:
        try:
            kaguya_file = parse_kaguya_worker_meta(kaguya_raw)
        except Exception:
            if refresh_core_keys_from_js(sess, page_resp.text, full_url, "capitulo", force=True):
                kaguya_file = parse_kaguya_worker_meta(kaguya_raw)
            else:
                raise
    elif isinstance(hinfo.get("kaguyaWorker"), str) and hinfo.get("kaguyaWorker"):
        kaguya_file = str(hinfo["kaguyaWorker"])
    else:
        raise UpstreamHttpError(502, "Missing kaguya-worker metadata on chapter page")
    kaguya_url = f"{SITE_BASE}{PATH_PREFIX}/__kaguya/{kaguya_file}.js?transport=3"
    kaguya_vm_url = f"{CANONICAL_ORIGIN}{PATH_PREFIX}/__kaguya/{kaguya_file}.js?transport=3"
    kaguya_code = get_worker_code(sess, kaguya_url, referer=full_url)

    proof = None
    if key_name == "chapter_read" and isinstance(raw_challenge, str) and raw_challenge.startswith("H1."):
        hydra_file = hinfo.get("workerFile")
        if not hydra_file:
            raise UpstreamHttpError(502, "Missing workerFile in chapter header-challenge")
        hydra_url = f"{SITE_BASE}{PATH_PREFIX}/__hydra/capitulo/{hydra_file}.js?transport=3"
        hydra_vm_url = f"{CANONICAL_ORIGIN}{PATH_PREFIX}/__hydra/capitulo/{hydra_file}.js?transport=3"
        hydra_code = get_worker_code(sess, hydra_url, referer=full_url)
        proof = _WORKER_CLIENT.call(
            {
                "op": "hydra_prove",
                "surface": "capitulo",
                "workerFile": hydra_file,
                "workerCode": hydra_code,
                "workerUrl": hydra_vm_url,
                "challenge": raw_challenge,
                "userAgent": DEFAULT_UA,
            }
        )

    signal_obj = {
        "cid": chapter_id_int,
        "ts": int(time.time()),
        "forceCaptcha": False,
        "reason": "normal",
        "count": 0,
        "touchPrimary": False,
    }
    signal_key = f"kaguya14-signal-v2:{subtoken}:{chapter_id_int}:{token}"
    client_signal_payload = _WORKER_CLIENT.call(
        {
            "op": "kaguya_transform",
            "workerFile": kaguya_file,
            "workerCode": kaguya_code,
            "workerUrl": kaguya_vm_url,
            "action": "encrypt",
            "data": json.dumps(signal_obj, separators=(",", ":")),
            "key": signal_key,
            "mode": 0,
            "challenge": raw_challenge or "",
            "userAgent": DEFAULT_UA,
        }
    )

    read_url = f"{SITE_BASE}{PATH_PREFIX}/models/capitulo/__stron__capitulos__read.php"
    headers = {
        **CAPITULO_HEADERS,
        "Referer": full_url,
        "Origin": SITE_BASE,
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }
    if csrf:
        headers["X-CSRF-Token"] = csrf

    form_data = {
        "action": "read",
        "chapter_id": str(chapter_id),
        "token": token,
        "reader_state": "",
        "client_signal_payload": client_signal_payload,
    }
    if proof and raw_challenge:
        form_data["proof"] = proof
        form_data["challenge"] = raw_challenge

    read_resp = sess.post(read_url, headers=headers, data=form_data, timeout=20)
    if read_resp.status_code == 403:
        if refresh_core_keys_from_js(sess, page_resp.text, full_url, "capitulo", force=True):
            headers.update(CAPITULO_HEADERS)
            read_resp = sess.post(read_url, headers=headers, data=form_data, timeout=20)
    if read_resp.status_code == 404:
        legacy_url = f"{SITE_BASE}{PATH_PREFIX}/models/capitulo/__tron__capitulos__read.php"
        read_resp = sess.post(legacy_url, headers=headers, data=form_data, timeout=20)
    check_http_response(read_resp, is_html=False)
    read_json = read_resp.json()
    check_api_payload(read_json)

    if not isinstance(read_json, dict) or read_json.get("status") != "success" or "data" not in read_json:
        raise UpstreamHttpError(
            502,
            f"__tron__capitulos__read.php failed: {read_json}",
        )

    page_specs, aes_key = decrypt_read_data_payload(
        sess=sess,
        res_data=read_json["data"],
        chapter_url=full_url,
        subtoken=subtoken,
        kaguya_file=kaguya_file,
        kaguya_code=kaguya_code,
        kaguya_url=kaguya_url,
        raw_challenge=raw_challenge,
    )
    if not page_specs:
        raise UpstreamHttpError(502, "No pages returned by chapter reader payload")

    cookies = dict(sess.cookies)
    page_ids: list[str] = []
    for spec in page_specs:
        page_id = hashlib.sha256(f"{full_url}#{spec['page']}".encode("utf-8")).hexdigest()[:24]
        store_page_spec(
            page_id,
            {
                "spec": spec,
                "chapter_url": full_url,
                "cookies": cookies,
                "poly_headers": poly_headers,
                "chapter_id_int": chapter_id_int,
                "aes_key": aes_key,
            },
        )
        page_ids.append(page_id)

    with _PAGE_CACHE_LOCK:
        _CHAPTER_PAGES_CACHE[full_url] = (time.monotonic(), page_ids)

    return [f"http://{host_header}/page_image?id={pid}" for pid in page_ids]


_PLUGIN_LOCK = threading.Lock()
_PLUGIN_MODULES: dict[str, tuple[float, object]] = {}


def derive_target_origin(target_url: str) -> str:
    """Extract scheme://netloc origin from target_url for multi-extension support."""
    try:
        parsed = urllib.parse.urlparse(target_url)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        pass
    return SITE_BASE


def build_upstream_headers(
    target_url: str,
    req_headers,
    qs: dict[str, list[str]],
    default_accept: str | None = None,
    include_content_type: bool = False,
) -> dict[str, str]:
    """Build upstream headers dynamically for any target site/extension."""
    origin = (
        qs.get("origin", [None])[0]
        or req_headers.get("X-Proxy-Origin")
        or derive_target_origin(target_url)
    ).rstrip("/")
    referer = (
        qs.get("referer", [None])[0]
        or req_headers.get("X-Proxy-Referer")
        or f"{origin}/"
    )
    headers: dict[str, str] = {
        "Referer": referer,
        "Origin": origin,
    }
    if default_accept:
        headers["Accept"] = default_accept
    elif req_headers.get("X-Proxy-Accept"):
        headers["Accept"] = req_headers.get("X-Proxy-Accept")

    custom_ua = qs.get("ua", [None])[0] or req_headers.get("X-Proxy-User-Agent")
    if custom_ua:
        headers["User-Agent"] = custom_ua

    custom_cookie = qs.get("cookie", [None])[0] or req_headers.get("X-Proxy-Cookie")
    if custom_cookie:
        headers["Cookie"] = custom_cookie

    if include_content_type and req_headers.get("content-type"):
        headers["Content-Type"] = req_headers.get("content-type")

    for k, v in req_headers.items():
        kl = k.lower()
        if kl.startswith("x-proxy-header-"):
            real_name = k[len("x-proxy-header-") :]
            if real_name:
                headers[real_name] = v
        elif kl.startswith("x-forward-"):
            real_name = k[len("x-forward-") :]
            if real_name:
                headers[real_name] = v

    return headers


def dispatch_extension_plugins(
    handler: "ProxyServiceHandler",
    method: str,
    parsed: urllib.parse.ParseResult,
    qs: dict[str, list[str]],
) -> bool:
    """Dynamically load and dispatch any custom extension handler in extensions/*.py."""
    if not os.path.isdir(EXTENSIONS_DIR):
        return False
    for path in sorted(glob.glob(os.path.join(EXTENSIONS_DIR, "*.py"))):
        name = os.path.basename(path)
        if name.startswith("_"):
            continue
        try:
            mtime = os.path.getmtime(path)
            with _PLUGIN_LOCK:
                cached = _PLUGIN_MODULES.get(path)
                if cached is None or cached[0] != mtime:
                    spec = importlib.util.spec_from_file_location(f"ext_plugin_{name[:-3]}", path)
                    if spec is None or spec.loader is None:
                        continue
                    mod = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(mod)
                    _PLUGIN_MODULES[path] = (mtime, mod)
                else:
                    mod = cached[1]
            fn = getattr(mod, "handle_request", None)
            if callable(fn) and fn(handler, method, parsed, qs):
                return True
        except Exception as exc:
            print(f"[plugin-error] {name}: {exc}", file=sys.stderr, flush=True)
    return False


class ProxyServiceHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "stream-proxy/1.0"

    def log_message(self, format, *args):
        if len(args) >= 3:
            msg = f"{args[0]} - {args[1]} {args[2]}"
        elif args:
            try:
                msg = format % args
            except Exception:
                msg = " ".join(str(a) for a in args)
        else:
            msg = str(format)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

    def _send_bytes(
        self,
        status: int,
        content_type: str,
        body: bytes,
        head_only: bool = False,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.end_headers()
        if not head_only and body:
            self.wfile.write(body)

    def _send_json(self, status: int, obj, head_only: bool = False) -> None:
        raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self._send_bytes(status, "application/json; charset=utf-8", raw, head_only=head_only)

    def do_HEAD(self):
        self.handle_req(method="HEAD", head_only=True)

    def do_GET(self):
        self.handle_req(method="GET", head_only=False)

    def do_POST(self):
        self.handle_req(method="POST", head_only=False)

    def do_PUT(self):
        self.handle_req(method="PUT", head_only=False)

    def do_DELETE(self):
        self.handle_req(method="DELETE", head_only=False)

    def do_PATCH(self):
        self.handle_req(method="PATCH", head_only=False)

    def handle_req(self, method: str = "GET", head_only: bool = False):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)
        host_header = self.headers.get("Host") or f"127.0.0.1:{PORT}"

        if parsed.path == "/health":
            self._send_bytes(200, "text/plain", b"OK", head_only=head_only)
            return

        if dispatch_extension_plugins(self, method, parsed, qs):
            return

        if parsed.path in ("/api", "/proxy"):
            target_url = qs.get("url", [None])[0]
            if not target_url:
                self._send_bytes(400, "text/plain", b"Missing url", head_only=head_only)
                return

            req_method = (qs.get("method", [None])[0] or method).upper()
            has_body = req_method in ("POST", "PUT", "PATCH", "DELETE")
            length = int(self.headers.get("content-length", 0)) if has_body else 0
            body = self.rfile.read(length) if length > 0 else None

            headers = build_upstream_headers(
                target_url,
                self.headers,
                qs,
                include_content_type=has_body,
            )
            no_cache = (
                req_method not in ("GET", "HEAD")
                or qs.get("no_cache", ["0"])[0] == "1"
                or "no-cache" in (self.headers.get("Cache-Control") or "").lower()
            )
            cache_key = f"{target_url}|{headers.get('Referer', '')}|{headers.get('Cookie', '')}"

            if not no_cache:
                now = time.monotonic()
                with _META_CACHE_LOCK:
                    cached = _API_GET_CACHE.get(cache_key)
                    if cached is not None and (now - cached[0]) < API_GET_TTL_SEC:
                        _, c_status, c_ctype, c_final_url, c_body = cached
                        self._send_bytes(
                            c_status,
                            c_ctype,
                            c_body,
                            head_only=head_only,
                            extra_headers={"X-Target-Url": c_final_url, "X-Cache": "HIT"},
                        )
                        return

            try:
                sess = get_thread_session()
                if req_method in ("GET", "HEAD"):
                    resp = sess.get(target_url, headers=headers, timeout=25)
                else:
                    resp = sess.request(req_method, target_url, headers=headers, data=body, timeout=25)
                ctype = resp.headers.get("content-type", "application/octet-stream")
                final_url = str(getattr(resp, "url", "") or target_url)
                is_rate_limited = (
                    resp.status_code == 429
                    or "/rate_limit" in final_url
                    or (resp.status_code == 200 and b"Muitas requisi" in resp.content[:4096])
                )
                if is_rate_limited:
                    self._send_bytes(
                        429,
                        "text/plain; charset=utf-8",
                        b"Upstream rate limit active (/rate_limit)",
                        head_only=head_only,
                    )
                    return
                if not no_cache and resp.status_code == 200 and resp.content:
                    with _META_CACHE_LOCK:
                        if len(_API_GET_CACHE) > 256:
                            _API_GET_CACHE.clear()
                        _API_GET_CACHE[cache_key] = (
                            time.monotonic(),
                            resp.status_code,
                            ctype,
                            final_url,
                            resp.content,
                        )
                self._send_bytes(
                    resp.status_code,
                    ctype,
                    resp.content,
                    head_only=head_only,
                    extra_headers={"X-Target-Url": final_url, "X-Cache": "MISS"},
                )
            except Exception as e:
                self._send_bytes(500, "text/plain", str(e).encode("utf-8"), head_only=head_only)
            return

        if parsed.path == "/manga_chapters":
            target_url = qs.get("url", [None])[0]
            if not target_url:
                self._send_bytes(400, "text/plain", b"Missing 'url' parameter", head_only=head_only)
                return
            try:
                chapters = handle_manga_chapters(target_url)
                self._send_json(200, chapters, head_only=head_only)
            except UpstreamHttpError as e:
                self._send_bytes(e.status_code, "text/plain; charset=utf-8", e.message.encode("utf-8"), head_only=head_only)
            except Exception as e:
                self._send_bytes(500, "text/plain; charset=utf-8", str(e).encode("utf-8"), head_only=head_only)
            return

        if parsed.path == "/chapter_pages":
            target_url = qs.get("url", [None])[0]
            if not target_url:
                self._send_bytes(400, "text/plain", b"Missing 'url' parameter", head_only=head_only)
                return
            try:
                urls = handle_chapter_pages(target_url, host_header=host_header)
                self._send_json(200, urls, head_only=head_only)
            except UpstreamHttpError as e:
                self._send_bytes(e.status_code, "text/plain; charset=utf-8", e.message.encode("utf-8"), head_only=head_only)
            except Exception as e:
                self._send_bytes(500, "text/plain; charset=utf-8", str(e).encode("utf-8"), head_only=head_only)
            return

        if parsed.path == "/page_image":
            page_id = qs.get("id", [None])[0]
            if not page_id:
                self._send_bytes(400, "text/plain", b"Missing 'id' parameter", head_only=head_only)
                return
            try:
                img_bytes, mime_type, cache_state = get_or_fetch_page_image(page_id)
                self._send_bytes(
                    200,
                    mime_type,
                    img_bytes,
                    head_only=head_only,
                    extra_headers={"Cache-Control": "no-store", "X-Cache": cache_state},
                )
            except UpstreamHttpError as e:
                self._send_bytes(e.status_code, "text/plain; charset=utf-8", e.message.encode("utf-8"), head_only=head_only)
            except Exception as e:
                self._send_bytes(500, "text/plain; charset=utf-8", str(e).encode("utf-8"), head_only=head_only)
            return

        if parsed.path == "/image":
            target_url = qs.get("url", [None])[0]
            if not target_url:
                self._send_bytes(400, "text/plain", b"Missing 'url' parameter", head_only=head_only)
                return
            target_parsed = urllib.parse.urlparse(target_url)
            if target_parsed.port == PORT and target_parsed.hostname in (
                "127.0.0.1",
                "localhost",
                "gateway",
                "vps-gateway",
            ):
                self._send_bytes(400, "text/plain", b"Refusing self-proxy image loop", head_only=head_only)
                return

            cache_key = hashlib.sha256(target_url.encode("utf-8")).hexdigest()
            cache_file = os.path.join(CACHE_DIR, cache_key)
            meta_file = os.path.join(CACHE_DIR, cache_key + ".meta")

            if os.path.exists(cache_file) and os.path.exists(meta_file):
                try:
                    with open(meta_file, "r", encoding="utf-8") as mf:
                        content_type = mf.read().strip()
                    with open(cache_file, "rb") as cf:
                        content = cf.read()
                    self._send_bytes(
                        200,
                        content_type or "image/jpeg",
                        content,
                        head_only=head_only,
                        extra_headers={"X-Cache": "HIT"},
                    )
                    return
                except Exception:
                    pass

            try:
                headers = build_upstream_headers(
                    target_url,
                    self.headers,
                    qs,
                    default_accept="image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                )
                resp = get_thread_session().get(target_url, headers=headers, timeout=20)
                if resp.status_code in (429, 403):
                    self._send_bytes(
                        resp.status_code,
                        "text/plain",
                        f"Upstream returned status {resp.status_code}".encode("utf-8"),
                        head_only=head_only,
                    )
                    return
                if resp.status_code == 200 and len(resp.content) > 500:
                    content_type = detect_image_mime(resp.content, resp.headers.get("content-type"))
                    try:
                        with open(cache_file, "wb") as cf:
                            cf.write(resp.content)
                        with open(meta_file, "w", encoding="utf-8") as mf:
                            mf.write(content_type)
                    except Exception:
                        pass
                    self._send_bytes(
                        200,
                        content_type,
                        resp.content,
                        head_only=head_only,
                        extra_headers={"X-Cache": "MISS"},
                    )
                else:
                    status = resp.status_code if resp.status_code != 200 else 502
                    self._send_bytes(
                        status,
                        "text/plain",
                        f"Fetch failed with status {resp.status_code}".encode("utf-8"),
                        head_only=head_only,
                    )
            except Exception as e:
                self._send_bytes(500, "text/plain", str(e).encode("utf-8"), head_only=head_only)
            return

        self._send_bytes(404, "text/plain", b"Not Found", head_only=head_only)


def main():
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    with socketserver.ThreadingTCPServer(("0.0.0.0", PORT), ProxyServiceHandler) as httpd:
        print(f"Suwayomi stream bridge proxy running on 0.0.0.0:{PORT}...", flush=True)
        httpd.serve_forever()


if __name__ == "__main__":
    main()
