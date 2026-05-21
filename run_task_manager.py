import asyncio
import logging
import os
from typing import Any, Dict, Tuple

from comm import WebSocketClient, WebSocketClientConfig
from task_manager import TaskManager, TaskManagerConfig, Waypoint


async def dispatch_nav(wp: Waypoint) -> Tuple[bool, str]:
    await asyncio.sleep(max(int(wp.stay_time_s), 0))
    return True, ""


def get_dog_status() -> Dict[str, Any]:
    return {"online": True, "battery": int(os.environ.get("BATTERY", "100"))}


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dog_id = os.environ.get("DOG_ID", "X30_001")
    server_url = os.environ.get("WS_URL", "ws://127.0.0.1:8765/robot/X30_001")
    token = os.environ.get("WS_TOKEN", "demo-token")
    secret = os.environ.get("WS_SECRET", "demo-secret")

    client = WebSocketClient(
        WebSocketClientConfig(
            dog_id=dog_id,
            server_url=server_url,
            token=token,
            secret=secret,
            download_tmp_dir=os.environ.get("DOWNLOAD_TMP", "./downloads/tmp"),
        )
    )
    await client.start()
    await client.wait_connected(timeout_s=10)

    tm = TaskManager(
        TaskManagerConfig(
            dog_id=dog_id,
            db_path=os.environ.get("TASK_DB", "./data/task_db.sqlite"),
            map_install_root=os.environ.get("MAP_INSTALL_ROOT", "/opt/robot/maps"),
            map_symlink_path=os.environ.get("MAP_SYMLINK_PATH", "/opt/robot/current_map"),
            map_path_json_target=os.environ.get("MAP_PATH_JSON_TARGET", "/opt/robot/current_map_path.json"),
        ),
        client=client,
        dispatch_nav=dispatch_nav,
        get_dog_status=get_dog_status,
    )
    await tm.start()
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
