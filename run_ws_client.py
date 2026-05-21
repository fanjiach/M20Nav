import asyncio
import logging
import os
import pathlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from comm import WebSocketClient, WebSocketClientConfig


def _parse_iso(s: str) -> datetime:
    try:
        return datetime.fromisoformat(s)
    except Exception:
        if s.endswith("Z"):
            return datetime.fromisoformat(s[:-1] + "+00:00")
        raise


def _now_iso() -> str:
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz=tz).isoformat()


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    dog_id = os.environ.get("DOG_ID", "X30_001")
    server_url = os.environ.get("WS_URL", "ws://127.0.0.1:8765/robot/X30_001")
    token = os.environ.get("WS_TOKEN", "demo-token")
    secret = os.environ.get("WS_SECRET", "demo-secret")
    download_dir = pathlib.Path(os.environ.get("DOWNLOAD_DIR", "./downloads"))
    download_dir.mkdir(parents=True, exist_ok=True)

    client = WebSocketClient(
        WebSocketClientConfig(
            dog_id=dog_id,
            server_url=server_url,
            token=token,
            secret=secret,
            download_tmp_dir=str(download_dir / "tmp"),
        )
    )

    async def on_task_push(msg: Dict[str, Any]) -> None:
        task_id = str(msg.get("task_id") or "")
        try:
            if not task_id:
                raise ValueError("missing task_id")
            execute_time = msg.get("execute_time")
            if not isinstance(execute_time, str) or not execute_time:
                raise ValueError("missing execute_time")
            try:
                et = _parse_iso(execute_time)
                now = _parse_iso(_now_iso())
                if et <= now + timedelta(seconds=10):
                    logging.warning("task execute_time too close: %s", execute_time)
            except Exception:
                logging.warning("invalid execute_time: %s", execute_time)
            zip_url = msg.get("zip_url")
            map_md5 = msg.get("map_md5")
            if isinstance(zip_url, str) and zip_url:
                dest = download_dir / "task" / task_id / "task.zip"
                dest.parent.mkdir(parents=True, exist_ok=True)
                await client.download_file(
                    url=zip_url,
                    dest_path=str(dest),
                    expected_md5=str(map_md5) if isinstance(map_md5, str) and map_md5 else None,
                )
                logging.info("task file ready: %s", str(dest))
            await client.send_task_ack(task_id=task_id, ack_result="success", error_code=0)
        except Exception as e:
            logging.exception("task_push failed: %s", e)
            await client.send_task_ack(task_id=task_id, ack_result="fail", error_code=3005)
            await client.send_alarm(alarm_level=2, alarm_msg=f"task_push处理失败: {e}", error_code=3005)

    async def on_remote_control(msg: Dict[str, Any]) -> None:
        control_type = str(msg.get("control_type") or "")
        try:
            await client.send_remote_ack(control_type=control_type, ack_result="success", execute_time_ms=10)
        except Exception as e:
            logging.exception("remote_control failed: %s", e)
            await client.send_remote_ack(control_type=control_type, ack_result="fail", execute_time_ms=0)
            await client.send_alarm(alarm_level=2, alarm_msg=f"remote_control处理失败: {e}", error_code=2005)

    async def on_version_check(msg: Dict[str, Any]) -> None:
        local_program_md5 = os.environ.get("PROGRAM_MD5", "")
        local_map_md5 = os.environ.get("MAP_MD5", "")
        local_config_md5 = os.environ.get("CONFIG_MD5", "")

        remote_program_md5 = str(msg.get("program_md5") or "")
        remote_map_md5 = str(msg.get("map_md5") or "")
        remote_config_md5 = str(msg.get("config_md5") or "")
        version_url = msg.get("version_url")

        match = True
        if local_program_md5 and remote_program_md5 and local_program_md5.lower() != remote_program_md5.lower():
            match = False
        if local_map_md5 and remote_map_md5 and local_map_md5.lower() != remote_map_md5.lower():
            match = False
        if local_config_md5 and remote_config_md5 and local_config_md5.lower() != remote_config_md5.lower():
            match = False

        try:
            if not match and isinstance(version_url, str) and version_url:
                dest = download_dir / "version" / "update.pkg"
                dest.parent.mkdir(parents=True, exist_ok=True)
                await client.download_file(url=version_url, dest_path=str(dest), expected_md5=None)
                logging.info("version package downloaded: %s", str(dest))
            await client.send_json(
                "version_ack",
                check_result="match" if match else "mismatch",
                version_url=str(version_url) if isinstance(version_url, str) else "",
                update_size=0,
            )
        except Exception as e:
            logging.exception("version_check failed: %s", e)
            await client.send_alarm(alarm_level=2, alarm_msg=f"version_check处理失败: {e}", error_code=5001)

    async def on_time_sync(msg: Dict[str, Any]) -> None:
        server_time = msg.get("server_time") or msg.get("timestamp")
        if not isinstance(server_time, str) or not server_time:
            return
        try:
            st = _parse_iso(server_time)
            lt = _parse_iso(_now_iso())
            offset = (st - lt).total_seconds()
            await client.send_json(
                "time_sync",
                server_time=server_time,
                local_time=lt.isoformat(),
                offset=offset,
                sync_result="ok",
            )
        except Exception as e:
            logging.exception("time_sync failed: %s", e)

    async def on_default(msg: Dict[str, Any]) -> None:
        logging.info("recv: %s", msg.get("msg_type"))

    client.on("task_push", on_task_push)
    client.on("remote_control", on_remote_control)
    client.on("version_check", on_version_check)
    client.on("time_sync", on_time_sync)
    client.set_default_handler(on_default)

    await client.start()
    await client.wait_connected(timeout_s=10)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())

