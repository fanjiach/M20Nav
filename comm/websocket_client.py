import asyncio
import base64
import dataclasses
import gzip
import hashlib
import json
import logging
import os
import random
import string
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Deque, Dict, Optional

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
    _HAS_CRYPTO = False


Handler = Callable[[Dict[str, Any]], Awaitable[None]]


@dataclass(frozen=True)
class WebSocketClientConfig:
    dog_id: str
    server_url: str
    token: str
    secret: str
    aes_key: Optional[bytes] = None
    tz_offset_hours: int = 8
    heartbeat_interval_s: float = 3.0
    heartbeat_ack_timeout_s: float = 1.0
    heartbeat_fail_threshold: int = 2
    connect_open_timeout_s: float = 8.0
    max_offline_queue: int = 1000
    dedupe_window_s: float = 3.0
    replay_window_s: float = 300.0
    replay_clock_skew_s: float = 5.0
    enable_sign_verify: bool = True
    enable_replay_protect: bool = True
    download_tmp_dir: Optional[str] = None
    download_speed_limit_bps: int = 1024 * 1024
    download_timeout_s: int = 30
    download_max_retries: int = 3


class WebSocketClient:
    def __init__(self, config: WebSocketClientConfig):
        self._cfg = config
        self._log = logging.getLogger("WebSocketClient")
        self._ws = None
        self._stop_evt = asyncio.Event()
        self._connected_evt = asyncio.Event()

        self._send_q: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self._offline_q: Deque[Dict[str, Any]] = deque()

        self._handlers: Dict[str, Handler] = {}
        self._default_handler: Optional[Handler] = None

        self._recent_seq: Deque[tuple[str, float]] = deque()
        self._recent_seq_set: set[str] = set()

        self._recent_nonce: Deque[tuple[str, float]] = deque()
        self._recent_nonce_set: set[str] = set()

        self._pending_heartbeat_seq: Optional[str] = None
        self._heartbeat_ack_evt = asyncio.Event()
        self._heartbeat_fail_count = 0

        self._tasks: list[asyncio.Task] = []
        self._reconnect_count = 0

    def on(self, msg_type: str, handler: Handler) -> None:
        self._handlers[msg_type] = handler

    def set_default_handler(self, handler: Handler) -> None:
        self._default_handler = handler

    def is_connected(self) -> bool:
        return self._connected_evt.is_set()

    async def start(self) -> None:
        self._stop_evt.clear()
        self._tasks = [asyncio.create_task(self._run())]
        await asyncio.sleep(0)

    async def stop(self) -> None:
        self._stop_evt.set()
        for t in list(self._tasks):
            t.cancel()
        await self._close_ws()

    async def wait_connected(self, timeout_s: Optional[float] = None) -> bool:
        try:
            await asyncio.wait_for(self._connected_evt.wait(), timeout=timeout_s)
            return True
        except asyncio.TimeoutError:
            return False

    async def send(self, msg: Dict[str, Any]) -> None:
        await self._send_q.put(msg)

    async def send_json(self, msg_type: str, **fields: Any) -> str:
        seq_id = self._generate_seq_id()
        msg: Dict[str, Any] = {
            "msg_type": msg_type,
            "dog_id": self._cfg.dog_id,
            "timestamp": self._now_iso(),
            "seq_id": seq_id,
            "nonce": self._generate_nonce(),
        }
        msg.update(fields)
        await self.send(msg)
        return seq_id

    async def send_task_ack(self, task_id: str, ack_result: str, error_code: int = 0) -> str:
        return await self.send_json(
            "task_ack",
            task_id=task_id,
            ack_result=ack_result,
            error_code=error_code,
        )

    async def send_remote_ack(self, control_type: str, ack_result: str, execute_time_ms: int) -> str:
        return await self.send_json(
            "remote_ack",
            control_type=control_type,
            ack_result=ack_result,
            execute_time=execute_time_ms,
        )

    async def send_alarm(
        self,
        alarm_level: int,
        alarm_msg: str,
        error_code: int,
        video_url: str = "",
        snapshot_url: str = "",
        confidence: Optional[float] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        payload: Dict[str, Any] = {
            "alarm_level": alarm_level,
            "alarm_msg": alarm_msg,
            "error_code": error_code,
            "video_url": video_url,
            "snapshot_url": snapshot_url,
        }
        if confidence is not None:
            payload["confidence"] = confidence
        if context is not None:
            payload["context"] = context
        return await self.send_json("alarm_upload", **payload)

    async def download_file(
        self,
        url: str,
        dest_path: str,
        expected_md5: Optional[str] = None,
    ) -> Any:
        from .file_downloader import download_file

        return await download_file(
            url=url,
            dest_path=dest_path,
            expected_md5=expected_md5,
            tmp_dir=self._cfg.download_tmp_dir,
            speed_limit_bytes_per_s=self._cfg.download_speed_limit_bps,
            timeout_s=self._cfg.download_timeout_s,
            max_retries=self._cfg.download_max_retries,
        )

    async def _run(self) -> None:
        while not self._stop_evt.is_set():
            try:
                await self._connect_once()
                self._reconnect_count = 0
                while self._connected_evt.is_set() and not self._stop_evt.is_set():
                    await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._log.warning("run loop error: %s", e)
            finally:
                await self._close_ws()
                self._connected_evt.clear()
            if self._stop_evt.is_set():
                break
            self._reconnect_count += 1
            wait_s = self._reconnect_wait_s(self._reconnect_count)
            await asyncio.sleep(wait_s)

    async def _connect_once(self) -> None:
        if not _HAS_WS:
            raise RuntimeError("websockets not available; install requirements.txt first")
        headers = [
            ("Authorization", f"Bearer {self._cfg.token}"),
            ("Dog-Version", self._get_local_version()),
        ]
        self._log.info("connecting: %s", self._cfg.server_url)
        self._ws = await websockets.connect(
            self._cfg.server_url,
            extra_headers=headers,
            open_timeout=self._cfg.connect_open_timeout_s,
            ping_interval=None,
            close_timeout=2,
            max_size=None,
        )
        self._connected_evt.set()
        self._heartbeat_fail_count = 0
        self._pending_heartbeat_seq = None
        self._heartbeat_ack_evt.clear()

        self._tasks = [
            asyncio.create_task(self._recv_loop()),
            asyncio.create_task(self._send_loop()),
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._flush_offline_loop()),
        ]

    async def _close_ws(self) -> None:
        ws = self._ws
        self._ws = None
        for t in list(self._tasks):
            if t.done():
                continue
            t.cancel()
        self._tasks = []
        if ws is None:
            return
        try:
            await ws.close()
        except Exception:
            pass

    async def _recv_loop(self) -> None:
        try:
            while not self._stop_evt.is_set():
                ws = self._ws
                if ws is None:
                    break
                raw = await ws.recv()
                msg = self._decode_incoming(raw)
                if msg is None:
                    continue
                if not self._dedupe_incoming(msg):
                    continue
                if self._cfg.enable_replay_protect and not self._check_replay(msg):
                    continue
                if self._cfg.enable_sign_verify and not self._verify_sign_if_present(msg):
                    continue
                if msg.get("encrypt") == "aes-128-cbc" and isinstance(msg.get("data"), str):
                    try:
                        msg = self._decrypt(msg)
                    except Exception:
                        continue
                await self._dispatch(msg)
        except asyncio.CancelledError:
            return
        except Exception as e:
            self._log.warning("recv loop ended: %s", e)
        finally:
            self._connected_evt.clear()

    async def _send_loop(self) -> None:
        try:
            while not self._stop_evt.is_set():
                msg = await self._send_q.get()
                await self._send_now(msg)
        except asyncio.CancelledError:
            return
        except Exception as e:
            self._log.warning("send loop ended: %s", e)
        finally:
            self._connected_evt.clear()

    async def _heartbeat_loop(self) -> None:
        try:
            while not self._stop_evt.is_set():
                start = time.monotonic()
                seq = self._generate_seq_id()
                self._pending_heartbeat_seq = seq
                self._heartbeat_ack_evt.clear()
                hb = {
                    "msg_type": "heartbeat",
                    "dog_id": self._cfg.dog_id,
                    "timestamp": self._now_iso(),
                    "seq_id": seq,
                    "nonce": self._generate_nonce(),
                    "status": 0,
                    "battery": 100,
                    "motion_state": 0,
                    "error_code": 0,
                }
                self._log.info("send: heartbeat seq_id=%s", seq)
                await self._send_now(hb, allow_offline=False)
                try:
                    await asyncio.wait_for(
                        self._heartbeat_ack_evt.wait(),
                        timeout=self._cfg.heartbeat_ack_timeout_s,
                    )
                    self._log.info("recv: heartbeat_ack seq_id=%s", seq)
                    self._heartbeat_fail_count = 0
                except asyncio.TimeoutError:
                    self._heartbeat_fail_count += 1
                    if self._heartbeat_fail_count >= self._cfg.heartbeat_fail_threshold:
                        self._log.warning("heartbeat ack timeout threshold reached")
                        self._connected_evt.clear()
                        break
                elapsed = time.monotonic() - start
                sleep_s = self._cfg.heartbeat_interval_s - elapsed
                if sleep_s > 0:
                    await asyncio.sleep(sleep_s)
        except asyncio.CancelledError:
            return
        except Exception as e:
            self._log.warning("heartbeat loop ended: %s", e)
        finally:
            self._connected_evt.clear()

    async def _flush_offline_loop(self) -> None:
        try:
            while not self._stop_evt.is_set():
                if not self._connected_evt.is_set():
                    await asyncio.sleep(0.2)
                    continue
                if not self._offline_q:
                    await asyncio.sleep(0.2)
                    continue
                msg = self._offline_q.popleft()
                await self._send_now(msg, allow_offline=False)
        except asyncio.CancelledError:
            return
        except Exception as e:
            self._log.warning("offline flush ended: %s", e)
        finally:
            self._connected_evt.clear()

    async def _send_now(self, msg: Dict[str, Any], allow_offline: bool = True) -> None:
        ws = self._ws
        if ws is None or not self._connected_evt.is_set():
            if allow_offline and self._should_cache_offline(msg):
                self._push_offline(msg)
            return

        out = self._prepare_outgoing(msg)
        try:
            await ws.send(out)
        except Exception:
            self._connected_evt.clear()
            if allow_offline and self._should_cache_offline(msg):
                self._push_offline(msg)
            raise

    def _prepare_outgoing(self, msg: Dict[str, Any]) -> str:
        base = dict(msg)
        self._check_required_fields(base)
        base.setdefault("nonce", self._generate_nonce())
        base = self._encrypt_if_needed(base)
        encoded = self._encode_maybe_compress(base)
        encoded["sign"] = self._sign(encoded)
        return json.dumps(encoded, ensure_ascii=False, separators=(",", ":"))

    def _encode_maybe_compress(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        raw = json.dumps(msg, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(raw) <= 1024:
            return msg
        compressed = gzip.compress(raw, compresslevel=6)
        wrapper = {
            "msg_type": msg["msg_type"],
            "dog_id": msg["dog_id"],
            "timestamp": msg["timestamp"],
            "seq_id": msg["seq_id"],
            "nonce": msg.get("nonce", self._generate_nonce()),
            "compress": "gzip",
            "data": base64.b64encode(compressed).decode("utf-8"),
        }
        return wrapper

    def _decode_incoming(self, raw: Any) -> Optional[Dict[str, Any]]:
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

    async def _dispatch(self, msg: Dict[str, Any]) -> None:
        mt = msg.get("msg_type")
        if mt == "heartbeat_ack":
            if msg.get("seq_id") == self._pending_heartbeat_seq:
                self._heartbeat_ack_evt.set()
            return

        handler = self._handlers.get(mt)
        if handler is not None:
            await handler(msg)
            return
        if self._default_handler is not None:
            await self._default_handler(msg)

    def _check_required_fields(self, msg: Dict[str, Any]) -> None:
        for f in ("msg_type", "dog_id", "timestamp", "seq_id"):
            if f not in msg:
                raise ValueError(f"missing field: {f}")

    def _encrypt_if_needed(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        mt = msg.get("msg_type")
        if mt != "remote_control":
            return msg
        if "control_param" not in msg:
            return msg
        if not _HAS_CRYPTO:
            raise RuntimeError("cryptography not available for AES encryption")
        key = self._get_aes_key()
        iv = os.urandom(16)
        plaintext = json.dumps(msg["control_param"], ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        padder = padding.PKCS7(128).padder()
        padded = padder.update(plaintext) + padder.finalize()
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        encryptor = cipher.encryptor()
        ct = encryptor.update(padded) + encryptor.finalize()
        out = dict(msg)
        out["encrypt"] = "aes-128-cbc"
        out["iv"] = base64.b64encode(iv).decode("utf-8")
        out["data"] = base64.b64encode(ct).decode("utf-8")
        out.pop("control_param", None)
        return out

    def _decrypt(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        if not _HAS_CRYPTO:
            raise RuntimeError("cryptography not available for AES decryption")
        key = self._get_aes_key()
        iv = base64.b64decode(msg.get("iv", ""))
        ct = base64.b64decode(msg["data"])
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        padded = decryptor.update(ct) + decryptor.finalize()
        unpadder = padding.PKCS7(128).unpadder()
        plaintext = unpadder.update(padded) + unpadder.finalize()
        payload = json.loads(plaintext.decode("utf-8"))
        out = dict(msg)
        if out.get("msg_type") == "remote_control":
            out["control_param"] = payload
            out.pop("data", None)
        return out

    def _get_aes_key(self) -> bytes:
        if self._cfg.aes_key is not None:
            key = self._cfg.aes_key
        else:
            key = self._cfg.secret.encode("utf-8")
        if len(key) < 16:
            key = (key * (16 // len(key) + 1))[:16]
        return key[:16]

    def _sign(self, msg: Dict[str, Any]) -> str:
        obj = dict(msg)
        obj.pop("sign", None)
        canonical = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        s = f"{obj.get('dog_id','')}{obj.get('timestamp','')}{canonical}{self._cfg.secret}"
        return hashlib.md5(s.encode("utf-8")).hexdigest()

    def _verify_sign_if_present(self, msg: Dict[str, Any]) -> bool:
        sign = msg.get("sign")
        if not isinstance(sign, str) or not sign:
            return True
        expect = self._sign(msg)
        if sign != expect:
            self._log.warning("invalid sign: seq_id=%s", msg.get("seq_id"))
            return False
        return True

    def _dedupe_incoming(self, msg: Dict[str, Any]) -> bool:
        seq = msg.get("seq_id")
        if not isinstance(seq, str) or not seq:
            return True
        now = time.monotonic()
        self._purge_recent(self._recent_seq, self._recent_seq_set, now, self._cfg.dedupe_window_s)
        if seq in self._recent_seq_set:
            return False
        self._recent_seq.append((seq, now))
        self._recent_seq_set.add(seq)
        return True

    def _check_replay(self, msg: Dict[str, Any]) -> bool:
        nonce = msg.get("nonce")
        ts = msg.get("timestamp")
        if not isinstance(nonce, str) or not nonce:
            return False
        if not isinstance(ts, str) or not ts:
            return False
        now = time.time()
        self._purge_recent(self._recent_nonce, self._recent_nonce_set, now, self._cfg.replay_window_s)
        if nonce in self._recent_nonce_set:
            return False
        try:
            msg_ts = self._parse_iso(ts).timestamp()
        except Exception:
            return False
        if abs(now - msg_ts) > self._cfg.replay_clock_skew_s:
            return False
        self._recent_nonce.append((nonce, now))
        self._recent_nonce_set.add(nonce)
        return True

    @staticmethod
    def _purge_recent(q: Deque[tuple[str, float]], s: set[str], now: float, window: float) -> None:
        while q:
            k, t = q[0]
            if now - t <= window:
                break
            q.popleft()
            s.discard(k)

    def _should_cache_offline(self, msg: Dict[str, Any]) -> bool:
        mt = msg.get("msg_type")
        if mt == "heartbeat":
            return False
        return True

    def _push_offline(self, msg: Dict[str, Any]) -> None:
        if len(self._offline_q) >= self._cfg.max_offline_queue:
            self._offline_q.popleft()
        self._offline_q.append(dict(msg))

    @staticmethod
    def _reconnect_wait_s(n: int) -> float:
        if n <= 3:
            return 1.0
        if n <= 6:
            return 3.0
        if n <= 9:
            return 5.0
        return 10.0

    def _generate_seq_id(self) -> str:
        dt = self._now_dt()
        ts = dt.strftime("%Y%m%d%H%M%S")
        r = "".join(random.choices(string.digits, k=6))
        return f"SEQ{ts}{r}"

    @staticmethod
    def _generate_nonce() -> str:
        return "".join(random.choices(string.digits, k=6))

    def _now_dt(self) -> datetime:
        tz = timezone(timedelta(hours=self._cfg.tz_offset_hours))
        return datetime.now(tz=tz)

    def _now_iso(self) -> str:
        return self._now_dt().isoformat()

    @staticmethod
    def _parse_iso(s: str) -> datetime:
        try:
            return datetime.fromisoformat(s)
        except Exception:
            if s.endswith("Z"):
                return datetime.fromisoformat(s[:-1] + "+00:00")
            raise

    @staticmethod
    def _get_local_version() -> str:
        return os.environ.get("DOG_CLIENT_VERSION", "dev")

