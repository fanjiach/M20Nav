import asyncio
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class SectionAck:
    section_id: str
    ack_result: str
    occupy_dog_id: str = ""
    occupy_status: str = ""


class SectionLocker:
    def __init__(self, client: Any):
        self._client = client
        self._pending: Dict[str, asyncio.Future] = {}

    async def on_section_ack(self, msg: Dict[str, Any]) -> None:
        section_id = str(msg.get("section_id") or "")
        fut = self._pending.pop(section_id, None)
        if fut is None or fut.done():
            return
        fut.set_result(
            SectionAck(
                section_id=section_id,
                ack_result=str(msg.get("ack_result") or ""),
                occupy_dog_id=str(msg.get("occupy_dog_id") or ""),
                occupy_status=str(msg.get("occupy_status") or ""),
            )
        )

    async def acquire(self, section_id: str, timeout_s: int = 30) -> bool:
        deadline = time.monotonic() + float(timeout_s)
        while time.monotonic() < deadline:
            seq_id = await self._client.send_json(
                "section_check",
                section_id=section_id,
                dog_id=self._client._cfg.dog_id,
                occupy_status="waiting",
                timeout=timeout_s,
            )
            fut = asyncio.get_event_loop().create_future()
            self._pending[section_id] = fut
            try:
                ack: SectionAck = await asyncio.wait_for(fut, timeout=1.0)
            except asyncio.TimeoutError:
                self._pending.pop(section_id, None)
                continue
            if ack.ack_result == "success" and (ack.occupy_status == "free" or not ack.occupy_status):
                await self._client.send_json(
                    "section_check",
                    section_id=section_id,
                    dog_id=self._client._cfg.dog_id,
                    occupy_status="occupied",
                    timeout=timeout_s,
                )
                return True
            await asyncio.sleep(1.0)
        return False

    async def release(self, section_id: str) -> None:
        await self._client.send_json(
            "section_check",
            section_id=section_id,
            dog_id=self._client._cfg.dog_id,
            occupy_status="free",
            timeout=1,
        )

