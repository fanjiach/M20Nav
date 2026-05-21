import asyncio
import base64
import gzip
import hashlib
import json
import os
import pathlib
import random
import string
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from comm import WebSocketClient, WebSocketClientConfig, verify_file_md5

try:
    import aiohttp
    from aiohttp import web

    _HAS_AIOHTTP = True
except Exception:
    aiohttp = None
    web = None
    _HAS_AIOHTTP = False

try:
    import websockets

    _HAS_WS = True
except Exception:
    websockets = None
    _HAS_WS = False

try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    _HAS_CRYPTO = True
except Exception:
    default_backend = None
    padding = None
    Cipher = None
    algorithms = None
    modes = None
    _HAS_CRYPTO = False


def _now_iso() -> str:
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz=tz).isoformat()


def _nonce() -> str:
    return f"{random.randint(0, 999999):06d}"


def _seq() -> str:
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    r = "".join(random.choices(string.digits, k=6))
    return f"SEQ{ts}{r}"


def _canonical_json(obj: Dict[str, Any]) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sign_message(msg: Dict[str, Any], secret: str) -> str:
    obj = dict(msg)
    obj.pop("sign", None)
    canonical = _canonical_json(obj)
    s = f"{obj.get('dog_id','')}{obj.get('timestamp','')}{canonical}{secret}"
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def decode_ws_payload(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8")
        except Exception:
            return None
    try:
        obj = json.loads(raw)
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    if obj.get("compress") == "gzip" and isinstance(obj.get("data"), str):
        try:
            zipped = base64.b64decode(obj["data"])
            plain = gzip.decompress(zipped).decode("utf-8")
            obj = json.loads(plain)
        except Exception:
            return None
    return obj


@dataclass
class WsHarness:
    host: str = "127.0.0.1"
    port: int = 8765
    dog_id: str = "X30_001"
    secret: str = "demo-secret"

    server: Any = None
    outbound: asyncio.Queue = field(default_factory=asyncio.Queue)
    inbound: List[Dict[str, Any]] = field(default_factory=list)
    connected_evt: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def url(self) -> str:
        return f"ws://{self.host}:{self.port}/robot/{self.dog_id}"

    async def start(self) -> None:
        if not _HAS_WS:
            raise RuntimeError("websockets not available")

        async def handler(ws, path=None):
            self.connected_evt.set()

            async def sender():
                while True:
                    msg = await self.outbound.get()
                    await ws.send(json.dumps(msg, ensure_ascii=False, separators=(",", ":")))

            sender_task = asyncio.create_task(sender())
            try:
                async for raw in ws:
                    msg = decode_ws_payload(raw)
                    if msg is None:
                        continue
                    self.inbound.append(msg)
                    if msg.get("msg_type") == "heartbeat":
                        ack = {
                            "msg_type": "heartbeat_ack",
                            "dog_id": msg.get("dog_id", ""),
                            "timestamp": _now_iso(),
                            "server_time": _now_iso(),
                            "seq_id": msg.get("seq_id", ""),
                            "nonce": _nonce(),
                        }
                        ack["sign"] = sign_message(ack, self.secret)
                        await ws.send(json.dumps(ack, ensure_ascii=False, separators=(",", ":")))
            finally:
                sender_task.cancel()
                try:
                    await sender_task
                except Exception:
                    pass

        self.server = await websockets.serve(handler, self.host, self.port, ping_interval=None)

    async def stop(self) -> None:
        if self.server is None:
            return
        self.server.close()
        await self.server.wait_closed()
        self.server = None

    def clear_inbound(self) -> None:
        self.inbound.clear()


@dataclass
class HttpHarness:
    host: str = "127.0.0.1"
    port: int = 9876
    content: bytes = b""
    app: Any = None
    runner: Any = None
    site: Any = None

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}/file.bin"

    async def start(self) -> None:
        if not _HAS_AIOHTTP:
            raise RuntimeError("aiohttp not available")

        async def file_handler(request):
            rng = request.headers.get("Range", "")
            start = 0
            if rng.startswith("bytes=") and rng.endswith("-"):
                try:
                    start = int(rng[len("bytes=") : -1])
                except Exception:
                    start = 0
            body = self.content[start:]
            headers = {
                "Accept-Ranges": "bytes",
                "Content-Length": str(len(body)),
            }
            if start > 0:
                headers["Content-Range"] = "bytes {}-{}/{}".format(start, len(self.content) - 1, len(self.content))
                return web.Response(status=206, body=body, headers=headers)
            return web.Response(status=200, body=body, headers=headers)

        self.app = web.Application()
        self.app.add_routes([web.get("/file.bin", file_handler)])
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()

    async def stop(self) -> None:
        if self.runner is None:
            return
        await self.runner.cleanup()
        self.runner = None
        self.site = None
        self.app = None


async def wait_for(cond, timeout_s: float) -> bool:
    start = time.monotonic()
    while time.monotonic() - start < timeout_s:
        if cond():
            return True
        await asyncio.sleep(0.05)
    return False


async def test_heartbeat(h: WsHarness) -> Tuple[bool, str]:
    h.clear_inbound()
    cfg = WebSocketClientConfig(
        dog_id=h.dog_id,
        server_url=h.url,
        token="demo-token",
        secret=h.secret,
        heartbeat_interval_s=1.0,
        heartbeat_ack_timeout_s=0.5,
        heartbeat_fail_threshold=2,
    )
    client = WebSocketClient(cfg)
    await client.start()
    ok = await client.wait_connected(timeout_s=3)
    if not ok:
        await client.stop()
        return False, "heartbeat: connect timeout"
    got = await wait_for(lambda: sum(1 for m in h.inbound if m.get("msg_type") == "heartbeat") >= 3, 4)
    alive = client.is_connected()
    await client.stop()
    if not got or not alive:
        return False, "heartbeat: missing acks or disconnected"
    return True, "heartbeat: ok"


async def test_task_download(h: WsHarness, http: HttpHarness, base_dir: pathlib.Path) -> Tuple[bool, str]:
    h.clear_inbound()
    base_dir.mkdir(parents=True, exist_ok=True)
    dest = base_dir / "task.zip"
    if dest.exists():
        dest.unlink()

    cfg = WebSocketClientConfig(
        dog_id=h.dog_id,
        server_url=h.url,
        token="demo-token",
        secret=h.secret,
        heartbeat_interval_s=1.0,
        heartbeat_ack_timeout_s=0.5,
        heartbeat_fail_threshold=2,
        download_tmp_dir=str(base_dir / "tmp"),
    )
    client = WebSocketClient(cfg)

    async def on_task_push(msg: Dict[str, Any]) -> None:
        await client.download_file(url=str(msg.get("zip_url") or ""), dest_path=str(dest), expected_md5=str(msg.get("map_md5") or ""))
        await client.send_task_ack(task_id=str(msg.get("task_id") or ""), ack_result="success", error_code=0)

    client.on("task_push", on_task_push)
    await client.start()
    ok = await client.wait_connected(timeout_s=3)
    if not ok:
        await client.stop()
        return False, "task_download: connect timeout"

    expected_md5 = hashlib.md5(http.content).hexdigest()
    task_msg = {
        "msg_type": "task_push",
        "dog_id": h.dog_id,
        "timestamp": _now_iso(),
        "seq_id": _seq(),
        "nonce": _nonce(),
        "task_id": "T1",
        "execute_time": _now_iso(),
        "zip_url": http.url,
        "map_md5": expected_md5,
        "priority": 3,
        "route_json": {"route_id": "R001", "waypoints": []},
    }
    task_msg["sign"] = sign_message(task_msg, h.secret)
    await h.outbound.put(task_msg)

    got_ack = await wait_for(lambda: any(m.get("msg_type") == "task_ack" and m.get("task_id") == "T1" for m in h.inbound), 8)
    if not got_ack:
        await client.stop()
        return False, "task_download: missing task_ack"
    if not dest.exists():
        await client.stop()
        return False, "task_download: file not downloaded"
    vr = verify_file_md5(str(dest))
    await client.stop()
    if vr.md5.lower() != expected_md5.lower():
        return False, "task_download: md5 mismatch"
    return True, "task_download: ok"


async def test_dedupe(h: WsHarness) -> Tuple[bool, str]:
    h.clear_inbound()
    cfg = WebSocketClientConfig(
        dog_id=h.dog_id,
        server_url=h.url,
        token="demo-token",
        secret=h.secret,
        heartbeat_interval_s=1.0,
        heartbeat_ack_timeout_s=0.5,
        heartbeat_fail_threshold=2,
    )
    client = WebSocketClient(cfg)
    seen: List[str] = []

    async def on_task_pause(msg: Dict[str, Any]) -> None:
        seen.append(str(msg.get("seq_id") or ""))

    client.on("task_pause", on_task_pause)
    await client.start()
    ok = await client.wait_connected(timeout_s=3)
    if not ok:
        await client.stop()
        return False, "dedupe: connect timeout"

    seq_id = _seq()
    m1 = {
        "msg_type": "task_pause",
        "dog_id": h.dog_id,
        "timestamp": _now_iso(),
        "seq_id": seq_id,
        "nonce": _nonce(),
        "task_id": "T1",
        "pause_reason": "test",
        "operator": "tester",
    }
    m1["sign"] = sign_message(m1, h.secret)
    m2 = dict(m1)
    m2["nonce"] = _nonce()
    m2["sign"] = sign_message(m2, h.secret)
    await h.outbound.put(m1)
    await h.outbound.put(m2)

    ok2 = await wait_for(lambda: len(seen) >= 1, 3)
    await asyncio.sleep(0.5)
    await client.stop()
    if not ok2:
        return False, "dedupe: handler not called"
    if len(seen) != 1:
        return False, "dedupe: duplicate delivered"
    return True, "dedupe: ok"


async def test_replay_nonce(h: WsHarness) -> Tuple[bool, str]:
    h.clear_inbound()
    cfg = WebSocketClientConfig(
        dog_id=h.dog_id,
        server_url=h.url,
        token="demo-token",
        secret=h.secret,
        heartbeat_interval_s=1.0,
        heartbeat_ack_timeout_s=0.5,
        heartbeat_fail_threshold=2,
    )
    client = WebSocketClient(cfg)
    got: List[str] = []

    async def on_time_sync(msg: Dict[str, Any]) -> None:
        got.append(str(msg.get("seq_id") or ""))

    client.on("time_sync", on_time_sync)
    await client.start()
    ok = await client.wait_connected(timeout_s=3)
    if not ok:
        await client.stop()
        return False, "replay: connect timeout"

    nonce = _nonce()
    m1 = {
        "msg_type": "time_sync",
        "dog_id": h.dog_id,
        "timestamp": _now_iso(),
        "seq_id": _seq(),
        "nonce": nonce,
        "server_time": _now_iso(),
        "local_time": _now_iso(),
        "offset": 0.0,
        "sync_result": "ok",
    }
    m1["sign"] = sign_message(m1, h.secret)
    m2 = dict(m1)
    m2["seq_id"] = _seq()
    m2["sign"] = sign_message(m2, h.secret)
    await h.outbound.put(m1)
    await h.outbound.put(m2)

    ok2 = await wait_for(lambda: len(got) >= 1, 3)
    await asyncio.sleep(0.5)
    await client.stop()
    if not ok2:
        return False, "replay: handler not called"
    if len(got) != 1:
        return False, "replay: replay message delivered"
    return True, "replay: ok"


async def test_sign_verify(h: WsHarness) -> Tuple[bool, str]:
    h.clear_inbound()
    cfg = WebSocketClientConfig(
        dog_id=h.dog_id,
        server_url=h.url,
        token="demo-token",
        secret=h.secret,
        heartbeat_interval_s=1.0,
        heartbeat_ack_timeout_s=0.5,
        heartbeat_fail_threshold=2,
    )
    client = WebSocketClient(cfg)
    got: List[str] = []

    async def on_section_check(msg: Dict[str, Any]) -> None:
        got.append(str(msg.get("seq_id") or ""))

    client.on("section_check", on_section_check)
    await client.start()
    ok = await client.wait_connected(timeout_s=3)
    if not ok:
        await client.stop()
        return False, "sign: connect timeout"

    bad = {
        "msg_type": "section_check",
        "dog_id": h.dog_id,
        "timestamp": _now_iso(),
        "seq_id": _seq(),
        "nonce": _nonce(),
        "section_id": "S1",
        "occupy_status": "free",
        "timeout": 1,
    }
    bad["sign"] = "deadbeef"
    good = dict(bad)
    good["seq_id"] = _seq()
    good["nonce"] = _nonce()
    good["sign"] = sign_message(good, h.secret)
    await h.outbound.put(bad)
    await h.outbound.put(good)

    ok2 = await wait_for(lambda: len(got) >= 1, 3)
    await asyncio.sleep(0.5)
    await client.stop()
    if not ok2:
        return False, "sign: valid signed message not delivered"
    if len(got) != 1:
        return False, "sign: invalid signature delivered"
    return True, "sign: ok"


async def test_compress_decode(h: WsHarness) -> Tuple[bool, str]:
    h.clear_inbound()
    cfg = WebSocketClientConfig(
        dog_id=h.dog_id,
        server_url=h.url,
        token="demo-token",
        secret=h.secret,
        heartbeat_interval_s=1.0,
        heartbeat_ack_timeout_s=0.5,
        heartbeat_fail_threshold=2,
    )
    client = WebSocketClient(cfg)
    got: List[str] = []

    async def on_task_cancel(msg: Dict[str, Any]) -> None:
        got.append(str(msg.get("task_id") or ""))

    client.on("task_cancel", on_task_cancel)
    await client.start()
    ok = await client.wait_connected(timeout_s=3)
    if not ok:
        await client.stop()
        return False, "compress: connect timeout"

    inner = {
        "msg_type": "task_cancel",
        "dog_id": h.dog_id,
        "timestamp": _now_iso(),
        "seq_id": _seq(),
        "nonce": _nonce(),
        "task_id": "T100",
        "cancel_reason": "x" * 2000,
        "operator": "tester",
    }
    inner["sign"] = sign_message(inner, h.secret)
    raw = json.dumps(inner, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    wrapper = {"compress": "gzip", "data": base64.b64encode(gzip.compress(raw, compresslevel=6)).decode("utf-8")}
    await h.outbound.put(wrapper)

    ok2 = await wait_for(lambda: got == ["T100"], 4)
    await client.stop()
    if not ok2:
        return False, "compress: message not decoded"
    return True, "compress: ok"


def _aes_encrypt_control_param(control_param: Dict[str, Any], key16: bytes) -> Tuple[str, str]:
    iv = os.urandom(16)
    plaintext = json.dumps(control_param, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    padder = padding.PKCS7(128).padder()
    padded = padder.update(plaintext) + padder.finalize()
    cipher = Cipher(algorithms.AES(key16), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    ct = encryptor.update(padded) + encryptor.finalize()
    return base64.b64encode(iv).decode("utf-8"), base64.b64encode(ct).decode("utf-8")


async def test_encrypt_remote_control(h: WsHarness) -> Tuple[bool, str]:
    if not _HAS_CRYPTO:
        return True, "encrypt: skipped (cryptography not available)"
    h.clear_inbound()
    cfg = WebSocketClientConfig(
        dog_id=h.dog_id,
        server_url=h.url,
        token="demo-token",
        secret=h.secret,
        heartbeat_interval_s=1.0,
        heartbeat_ack_timeout_s=0.5,
        heartbeat_fail_threshold=2,
    )
    client = WebSocketClient(cfg)
    got: List[Dict[str, Any]] = []

    async def on_remote_control(msg: Dict[str, Any]) -> None:
        got.append(msg)

    client.on("remote_control", on_remote_control)
    await client.start()
    ok = await client.wait_connected(timeout_s=3)
    if not ok:
        await client.stop()
        return False, "encrypt: connect timeout"

    key = (h.secret.encode("utf-8") * 2)[:16]
    iv_b64, data_b64 = _aes_encrypt_control_param({"speed": 0.3, "duration": 2}, key)
    msg = {
        "msg_type": "remote_control",
        "dog_id": h.dog_id,
        "timestamp": _now_iso(),
        "seq_id": _seq(),
        "nonce": _nonce(),
        "control_type": "move_forward",
        "valid_time": 2,
        "encrypt": "aes-128-cbc",
        "iv": iv_b64,
        "data": data_b64,
    }
    msg["sign"] = sign_message(msg, h.secret)
    await h.outbound.put(msg)

    ok2 = await wait_for(lambda: len(got) >= 1 and isinstance(got[0].get("control_param"), dict), 4)
    await client.stop()
    if not ok2:
        return False, "encrypt: decrypt failed"
    cp = got[0].get("control_param", {})
    if cp.get("speed") != 0.3 or cp.get("duration") != 2:
        return False, "encrypt: payload mismatch"
    return True, "encrypt: ok"


async def test_offline_flush(h: WsHarness) -> Tuple[bool, str]:
    h.clear_inbound()
    cfg = WebSocketClientConfig(
        dog_id=h.dog_id,
        server_url=h.url,
        token="demo-token",
        secret=h.secret,
        heartbeat_interval_s=1.0,
        heartbeat_ack_timeout_s=0.5,
        heartbeat_fail_threshold=2,
    )
    client = WebSocketClient(cfg)
    await client.start()
    ok = await client.wait_connected(timeout_s=3)
    if not ok:
        await client.stop()
        return False, "offline: connect timeout"

    await h.stop()
    await asyncio.sleep(0.3)
    await client.send_json("result_upload", task_id="T2", execute_result="ok", complete_rate=100, execute_duration=1, error_detail="")

    await h.start()
    ok2 = await client.wait_connected(timeout_s=5)
    if not ok2:
        await client.stop()
        return False, "offline: reconnect timeout"

    got = await wait_for(lambda: any(m.get("msg_type") == "result_upload" and m.get("task_id") == "T2" for m in h.inbound), 6)
    await client.stop()
    if not got:
        return False, "offline: cached message not flushed"
    return True, "offline: ok"


async def main() -> int:
    if not _HAS_WS:
        print("FAIL: websockets not installed")
        return 2
    if not _HAS_AIOHTTP:
        print("FAIL: aiohttp not installed")
        return 2

    base_dir = pathlib.Path(os.environ.get("TEST_TMP_DIR", "./.ws_test_tmp")).resolve()
    if base_dir.exists():
        for p in sorted(base_dir.rglob("*"), reverse=True):
            if p.is_file():
                try:
                    p.unlink()
                except Exception:
                    pass
            else:
                try:
                    p.rmdir()
                except Exception:
                    pass
    base_dir.mkdir(parents=True, exist_ok=True)

    ws = WsHarness(secret=os.environ.get("WS_SECRET", "demo-secret"))
    await ws.start()

    http = HttpHarness(content=os.urandom(512 * 1024 + 123))
    await http.start()

    tests = [
        ("heartbeat", lambda: test_heartbeat(ws)),
        ("task_download", lambda: test_task_download(ws, http, base_dir / "download")),
        ("dedupe", lambda: test_dedupe(ws)),
        ("replay", lambda: test_replay_nonce(ws)),
        ("sign", lambda: test_sign_verify(ws)),
        ("compress", lambda: test_compress_decode(ws)),
        ("encrypt", lambda: test_encrypt_remote_control(ws)),
        ("offline", lambda: test_offline_flush(ws)),
    ]

    failed = 0
    for name, fn in tests:
        try:
            ok, msg = await fn()
        except Exception as e:
            ok, msg = False, f"{name}: exception {e}"
        if ok:
            print("PASS:", msg)
        else:
            print("FAIL:", msg)
            failed += 1

    await http.stop()
    await ws.stop()
    return 1 if failed else 0


if __name__ == "__main__":
    code = asyncio.run(main())
    sys.exit(code)
