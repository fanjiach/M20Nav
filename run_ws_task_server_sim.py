import asyncio
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

try:
    import websockets
except Exception:
    websockets = None


def now_iso() -> str:
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz=tz).isoformat()


def nonce6() -> str:
    return f"{random.randint(0, 999999):06d}"


def seq_id() -> str:
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"SEQ{ts}{random.randint(0, 999999):06d}"


def make_task_push(
    dog_id: str,
    task_id: str,
    execute_time: str,
    priority: int,
    route_json: Dict[str, Any],
    zip_url: str = "",
    map_md5: str = "",
) -> Dict[str, Any]:
    return {
        "msg_type": "task_push",
        "dog_id": dog_id,
        "timestamp": now_iso(),
        "seq_id": seq_id(),
        "nonce": nonce6(),
        "task_id": task_id,
        "execute_time": execute_time,
        "priority": priority,
        "route_json": route_json,
        "zip_url": zip_url,
        "map_md5": map_md5,
    }


class SectionState:
    def __init__(self) -> None:
        self._occupied_by: Dict[str, str] = {}

    def check(self, section_id: str) -> Tuple[str, str]:
        dog = self._occupied_by.get(section_id, "")
        if dog:
            return "occupied", dog
        return "free", ""

    def occupy(self, section_id: str, dog_id: str) -> None:
        if section_id:
            self._occupied_by[section_id] = dog_id

    def release(self, section_id: str, dog_id: str) -> None:
        if self._occupied_by.get(section_id) == dog_id:
            self._occupied_by.pop(section_id, None)


async def send_json(ws, msg: Dict[str, Any]) -> None:
    await ws.send(json.dumps(msg, ensure_ascii=False, separators=(",", ":")))


def decode(raw: Any) -> Optional[Dict[str, Any]]:
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
    return obj


async def scenario_publish_tasks(ws, dog_id: str) -> None:
    await asyncio.sleep(2)
    future_time = (datetime.now(timezone(timedelta(hours=8))) + timedelta(seconds=30)).isoformat()
    scheduled = make_task_push(
        dog_id=dog_id,
        task_id=f"T_SCHEDULE_{int(time.time())}",
        execute_time=future_time,
        priority=4,
        route_json={
            "route_id": "R_SCHEDULE",
            "point_ids": ["P001", "P002"],
            "patrol_times": 2,
            "repeat_daily": False,
            "return_to": "HOME",
        },
    )
    await send_json(ws, scheduled)
    logging.info("push scheduled task: %s execute_time=%s priority=%s", scheduled["task_id"], scheduled["execute_time"], scheduled["priority"])

    await asyncio.sleep(8)
    temp = make_task_push(
        dog_id=dog_id,
        task_id=f"T_TEMP_{int(time.time())}",
        execute_time="immediate",
        priority=3,
        route_json={
            "route_id": "R_TEMP",
            "point_ids": ["P101", "P102", "P103"],
            "patrol_times": 1,
            "repeat_daily": False,
            "return_to": "HOME",
        },
    )
    await send_json(ws, temp)
    logging.info("push temp task: %s execute_time=%s priority=%s", temp["task_id"], temp["execute_time"], temp["priority"])


async def handler(ws, path=None):
    dog_id = "X30_001"
    try:
        p = path if path is not None else getattr(ws, "path", "")
        if isinstance(p, str) and p.startswith("/robot/"):
            dog_id = p.split("/robot/", 1)[1] or dog_id
    except Exception:
        pass

    logging.info("client connected dog_id=%s path=%s", dog_id, path if path is not None else getattr(ws, "path", ""))
    sections = SectionState()
    scenario_task = asyncio.create_task(scenario_publish_tasks(ws, dog_id))

    try:
        async for raw in ws:
            msg = decode(raw)
            if msg is None:
                continue
            mt = msg.get("msg_type")
            if mt == "heartbeat":
                ack = {
                    "msg_type": "heartbeat_ack",
                    "dog_id": str(msg.get("dog_id") or dog_id),
                    "timestamp": now_iso(),
                    "server_time": now_iso(),
                    "seq_id": str(msg.get("seq_id") or ""),
                    "nonce": nonce6(),
                }
                await send_json(ws, ack)
            elif mt == "section_check":
                sec_id = str(msg.get("section_id") or "")
                occupy_status = str(msg.get("occupy_status") or "")
                req_dog = str(msg.get("dog_id") or dog_id)
                if occupy_status == "occupied":
                    sections.occupy(sec_id, req_dog)
                elif occupy_status == "free":
                    sections.release(sec_id, req_dog)
                st, occupy_dog = sections.check(sec_id)
                ack = {
                    "msg_type": "section_ack",
                    "dog_id": req_dog,
                    "timestamp": now_iso(),
                    "seq_id": str(msg.get("seq_id") or ""),
                    "nonce": nonce6(),
                    "section_id": sec_id,
                    "ack_result": "success",
                    "occupy_status": st,
                    "occupy_dog_id": occupy_dog,
                }
                await send_json(ws, ack)
            elif mt in ("task_ack", "result_upload", "alarm_upload", "remote_ack", "version_ack", "time_sync"):
                logging.info("recv: %s %s", mt, json.dumps(msg, ensure_ascii=False))
            else:
                logging.info("recv: %s", mt)
    finally:
        scenario_task.cancel()
        try:
            await scenario_task
        except Exception:
            pass
        logging.info("client disconnected dog_id=%s", dog_id)


async def main() -> None:
    if websockets is None:
        raise RuntimeError("websockets not available; install requirements.txt first")
    host = os.environ.get("WS_SIM_HOST", "127.0.0.1")
    port = int(os.environ.get("WS_SIM_PORT", "8765"))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    async with websockets.serve(handler, host, port, ping_interval=None):
        logging.info("task sim server listening on %s:%s", host, port)
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
