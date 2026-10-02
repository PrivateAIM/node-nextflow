"""Forward selected run results to another node through node-storage-service (docs/result-handling-plan.md).

Selected files -> deterministic tar stream -> (optional gzip) -> parts of at most `part_size` bytes ->
one `PUT /intermediate` (encrypted for the target node by the service) per part. A part is spooled to a
temporary file, so at most one part lives on disk and nothing but a stream chunk lives in memory.
"""
import base64
import hashlib
import json
import re
import tarfile
import tempfile
import time
import uuid
import zlib
from typing import Callable, Iterator, Optional

import httpx

CHUNK = 1024 * 1024
TOKEN_MARGIN_S = 30


class ForwardError(Exception):
    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


# ---- selection ------------------------------------------------------------------------------------

def normalize_key(key: str) -> str:
    """Key or glob relative to results/ ('results/' prefix tolerated). Raises ValueError if it can escape."""
    key = key.strip()
    if key.startswith("results/"):
        key = key[len("results/"):]
    if not key or key.startswith("/") or ".." in key.split("/"):
        raise ValueError(f"invalid key {key!r}: must be non-empty, relative to results/, without '..'")
    return key


def glob_to_regex(pattern: str) -> re.Pattern:
    out, i = "", 0
    while i < len(pattern):
        if pattern.startswith("**/", i):
            out, i = out + "(?:.*/)?", i + 3
        elif pattern.startswith("**", i):
            out, i = out + ".*", i + 2
        elif pattern[i] == "*":
            out, i = out + "[^/]*", i + 1
        elif pattern[i] == "?":
            out, i = out + "[^/]", i + 1
        else:
            out, i = out + re.escape(pattern[i]), i + 1
    return re.compile(out + r"\Z")


def select_files(manifest: list[dict], patterns: list[str]) -> list[dict]:
    regexes = [glob_to_regex(normalize_key(p)) for p in patterns]
    return sorted((f for f in manifest if any(r.match(f["key"]) for r in regexes)), key=lambda f: f["key"])


# ---- tar stream -----------------------------------------------------------------------------------

def tar_size(files: list[dict]) -> int:
    total = 0
    for f in files:
        header = tarfile.TarInfo(f["key"])
        header.size = f["size"]
        total += len(header.tobuf(tarfile.GNU_FORMAT)) + (f["size"] + 511) // 512 * 512
    return total + 1024


def tar_stream(files: list[dict], open_object: Callable[[str], Iterator[bytes]]) -> Iterator[bytes]:
    """Deterministic (sorted keys, mtime 0, root owner) tar of the files; open_object(key) yields the content."""
    for f in files:
        header = tarfile.TarInfo(f["key"])
        header.size, header.mtime, header.mode = f["size"], 0, 0o644
        yield header.tobuf(tarfile.GNU_FORMAT)
        sent = 0
        for chunk in open_object(f["key"]):
            sent += len(chunk)
            yield chunk
        if sent != f["size"]:
            raise ForwardError("source_changed", f"{f['key']}: expected {f['size']} bytes, read {sent}")
        if sent % 512:
            yield b"\0" * (512 - sent % 512)
    yield b"\0" * 1024


def gzip_stream(stream: Iterator[bytes]) -> Iterator[bytes]:
    comp = zlib.compressobj(6, zlib.DEFLATED, 31)  # wbits 31 = gzip container, mtime 0 -> deterministic
    for chunk in stream:
        out = comp.compress(chunk)
        if out:
            yield out
    yield comp.flush()


def iter_parts(stream: Iterator[bytes], part_size: int, skip: set[int] = frozenset()):
    """Cut the stream into parts: yields (index, size, spooled file at position 0 or None if index in skip)."""
    index, size = 0, 0
    buf = None if index in skip else tempfile.TemporaryFile()
    for chunk in stream:
        view = memoryview(chunk)
        while len(view):
            take = view[:part_size - size]
            if buf is not None:
                buf.write(take)
            size += len(take)
            view = view[len(take):]
            if size == part_size:
                if buf is not None:
                    buf.seek(0)
                yield index, size, buf
                index, size = index + 1, 0
                buf = None if index in skip else tempfile.TemporaryFile()
    if size or index == 0:
        if buf is not None:
            buf.seek(0)
        yield index, size, buf


# ---- token ----------------------------------------------------------------------------------------

def token_expired(token: Optional[str]) -> bool:
    """True if the JWT is known to be expired. Anything we cannot decode is attempted anyway."""
    try:
        payload = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        return claims["exp"] < time.time() + TOKEN_MARGIN_S
    except Exception:
        return False


# ---- upload ---------------------------------------------------------------------------------------

def upload_part(client: httpx.Client, base_url: str, token: str, to: str, fileobj, name: str,
                attempts: int, backoff_s: float = 2.0) -> dict:
    """PUT /intermediate with retries; reports the status code / hop of the last failure."""
    last = ""
    for attempt in range(attempts):
        fileobj.seek(0)
        try:
            resp = client.put(f"{base_url}/intermediate", headers={"Authorization": f"Bearer {token}"},
                              files={"file": (name, fileobj)}, data={"remote_node_id": to})
            if resp.status_code == 200:
                body = resp.json()
                return {"object_id": body["object_id"], "url": body["url"]}
            last = f"storage-service answered {resp.status_code}: {resp.text[:200]}"
            if resp.status_code in (400, 401, 403, 404, 422):  # retrying cannot help
                break
        except httpx.HTTPError as e:
            last = f"{type(e).__name__} talking to storage-service: {e}"
        if attempt + 1 < attempts:
            time.sleep(backoff_s * 2 ** attempt)
    raise ForwardError("upload_failed", last)


def run_forward(*, files: list[dict], open_object: Callable[[str], Iterator[bytes]], spec: dict, token: str,
                storage_url: str, part_size: int, max_object_size: int, max_total_size: int,
                attempts: int, timeout_s: float, state: dict, save_state: Callable[[dict], None],
                backoff_s: float = 2.0, client: Optional[httpx.Client] = None) -> dict:
    """Execute a forward and return the final state (also passed to save_state after every part).

    `state` may come from an interrupted earlier attempt: parts already uploaded are not repeated.
    """
    state.update(status="running", error=None)
    state.setdefault("transfer_id", str(uuid.uuid4()))
    state.setdefault("parts", [])
    save_state(state)
    try:
        if not files:
            raise ForwardError("no_files", f"no result matches {spec['keys']}")
        compression = spec.get("compression", "none")
        part_size = min(spec.get("part_size") or part_size, max_object_size)
        raw_size = tar_size(files)
        if raw_size > max_total_size:
            raise ForwardError("too_large", f"{raw_size} bytes exceed NF_FORWARD_MAX_TOTAL_SIZE={max_total_size}")
        if token_expired(token):
            raise ForwardError("token_expired", "the token passed at /run expired before the run finished")

        sha, total = hashlib.sha256(), 0

        def hashed(stream):
            nonlocal total
            for chunk in stream:
                sha.update(chunk)
                total += len(chunk)
                yield chunk

        stream = tar_stream(files, open_object)
        if compression == "gzip":
            stream = gzip_stream(stream)
        done = {p["index"]: p for p in state["parts"]}
        own_client = client is None
        client = client or httpx.Client(timeout=timeout_s)
        try:
            for index, size, fileobj in iter_parts(hashed(stream), part_size, skip=set(done)):
                if fileobj is None:
                    continue
                try:
                    ref = upload_part(client, storage_url, token, spec["to"], fileobj,
                                      f"{state['transfer_id']}-{index}.part", attempts, backoff_s)
                finally:
                    fileobj.close()
                state["parts"].append({"index": index, "size": size, **ref})
                state["parts"].sort(key=lambda p: p["index"])
                save_state(state)
        finally:
            if own_client:
                client.close()
        state.update(status="done", total_size=total, sha256=sha.hexdigest(), compression=compression,
                     files=[{"key": f["key"], "size": f["size"]} for f in files])
    except ForwardError as e:
        state.update(status="failed", error={"reason": e.reason, "detail": e.detail})
    except Exception as e:  # never let a forward crash the conclude path
        state.update(status="failed", error={"reason": "internal_error", "detail": repr(e)[:300]})
    save_state(state)
    return state
