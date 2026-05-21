import asyncio
import base64
import gzip
import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

try:
    import websockets
except Exception:
    websockets = None


def now_iso() -> str:
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz=tz).isoformat()


def decode(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    obj = json.loads(raw)
    if obj.get("compress") == "gzip":
        data = base64.b64decode(obj["data"])
        obj = json.loads(gzip.decompress(data).decode("utf-8"))
    return obj


async def handler(ws, path=None):
    try:
        p = path if path is not None else getattr(ws, "path", "")
    except Exception:
        p = ""
    logging.info("client connected path=%s", p)

    async def push_task():
        while True:
            await asyncio.sleep(10)
            msg = {
                "msg_type": "task_push",
                "task_id": f"T{int(time.time())}",
                "execute_time": now_iso(),
                "route_json": {"route_id": "R001", "waypoints": []},
                "priority": 3,
                "map_md5": "demo",
                "dog_id": "X30_001",
                "timestamp": now_iso(),
                "seq_id": f"SEQ{int(time.time())}000000",
                "nonce": f"{random.randint(0, 999999):06d}",
            }
            await ws.send(json.dumps(msg, ensure_ascii=False, separators=(",", ":")))

    push_task_t = asyncio.create_task(push_task())

    try:
        async for raw in ws:
            msg = decode(raw)
            mt = msg.get("msg_type")
            if mt == "heartbeat":
                logging.info("recv: heartbeat seq_id=%s", msg.get("seq_id"))
                ack = {
                    "msg_type": "heartbeat_ack",
                    "dog_id": msg.get("dog_id", ""),
                    "timestamp": now_iso(),
                    "server_time": now_iso(),
                    "seq_id": msg.get("seq_id", ""),
                    "nonce": f"{random.randint(0, 999999):06d}",
                }
                await ws.send(json.dumps(ack, ensure_ascii=False, separators=(",", ":")))
            else:
                logging.info("recv: %s", mt)
    except Exception as e:
        logging.exception("handler error: %s", e)
    finally:
        push_task_t.cancel()
        try:
            await push_task_t
        except Exception:
            pass
        logging.info("client disconnected")


async def main() -> None:
    if websockets is None:
        raise RuntimeError("websockets not available; install requirements.txt first")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    async with websockets.serve(handler, "127.0.0.1", 8765, ping_interval=None):
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())

