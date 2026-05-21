import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from .models import Task, TaskContext, Waypoint
from .map_graph import build_point_index, build_waypoints_from_point_ids, pick_home_point_id
from .map_package import extract_task_zip, install_map_folder, load_map_path_json
from .route_parser import parse_route
from .section_locker import SectionLocker
from .store import TaskStore


GetDogStatus = Callable[[], Dict[str, Any]]
DispatchNav = Callable[[Waypoint], Awaitable[Tuple[bool, str]]]


@dataclass(frozen=True)
class TaskManagerConfig:
    dog_id: str
    db_path: str = "./data/task_db.sqlite"
    download_root: str = "./downloads"
    map_install_root: str = "/opt/robot/maps"
    map_symlink_path: str = "/opt/robot/current_map"
    map_path_json_target: str = "/opt/robot/current_map_path.json"
    default_section_timeout_s: int = 30
    waypoint_retry: int = 3


class TaskManager:
    def __init__(
        self,
        cfg: TaskManagerConfig,
        client: Any,
        dispatch_nav: Optional[DispatchNav] = None,
        get_dog_status: Optional[GetDogStatus] = None,
    ):
        self._cfg = cfg
        self._client = client
        self._store = TaskStore(cfg.db_path)
        self._locker = SectionLocker(client)
        self._log = logging.getLogger("TaskManager")

        self._dispatch_nav = dispatch_nav or self._dispatch_nav_stub
        self._get_dog_status = get_dog_status or self._get_dog_status_stub
        self._current_map_path: Optional[str] = None

        self._stop_evt = asyncio.Event()
        self._runner_task: Optional[asyncio.Task] = None
        self._current_task_id: Optional[str] = None
        self._pause_flags: Dict[str, bool] = {}
        self._cancel_flags: Dict[str, bool] = {}
        self._recent_seq: Dict[str, float] = {}
        self._recent_seq_window_s = 3600.0

        client.on("section_ack", self._locker.on_section_ack)
        client.on("task_push", self.on_task_push)
        client.on("task_pause", self.on_task_pause)
        client.on("task_cancel", self.on_task_cancel)

    async def start(self) -> None:
        self._stop_evt.clear()
        if self._runner_task is None or self._runner_task.done():
            self._runner_task = asyncio.create_task(self._run_loop())
        await asyncio.sleep(0)

    async def stop(self) -> None:
        self._stop_evt.set()
        if self._runner_task is not None:
            self._runner_task.cancel()
        self._runner_task = None

    async def on_task_push(self, msg: Dict[str, Any]) -> None:
        task_id = str(msg.get("task_id") or "")
        try:
            seq_id = str(msg.get("seq_id") or "")
            if seq_id and self._is_duplicate_seq(seq_id):
                self._log.info("duplicate seq_id dropped: %s", seq_id)
                if task_id:
                    await self._client.send_task_ack(task_id=task_id, ack_result="success", error_code=0)
                return
            task = self._parse_task(msg)
            if task.dog_id and task.dog_id != self._cfg.dog_id:
                raise RuntimeError("dog_id mismatch")
            self._store.upsert_task(task)
            self._pause_flags.pop(task.task_id, None)
            self._cancel_flags.pop(task.task_id, None)
            await self._client.send_task_ack(task_id=task.task_id, ack_result="success", error_code=0)
        except Exception as e:
            self._log.exception("task_push parse failed: %s", e)
            if task_id:
                await self._client.send_task_ack(task_id=task_id, ack_result="fail", error_code=3005)
            await self._client.send_alarm(alarm_level=2, alarm_msg=f"task_push处理失败: {e}", error_code=3005)

    def _is_duplicate_seq(self, seq_id: str) -> bool:
        now = time.monotonic()
        dead = [k for k, t in self._recent_seq.items() if now - t > self._recent_seq_window_s]
        for k in dead:
            self._recent_seq.pop(k, None)
        if seq_id in self._recent_seq:
            return True
        self._recent_seq[seq_id] = now
        return False

    async def on_task_pause(self, msg: Dict[str, Any]) -> None:
        task_id = str(msg.get("task_id") or "")
        if not task_id:
            return
        self._pause_flags[task_id] = True
        self._store.update_status(task_id, "paused")
        await self._client.send_task_ack(task_id=task_id, ack_result="success", error_code=0)

    async def on_task_cancel(self, msg: Dict[str, Any]) -> None:
        task_id = str(msg.get("task_id") or "")
        if not task_id:
            return
        self._cancel_flags[task_id] = True
        self._store.update_status(task_id, "cancelled")
        await self._client.send_task_ack(task_id=task_id, ack_result="success", error_code=0)

    def _parse_task(self, msg: Dict[str, Any]) -> Task:
        task_id = str(msg.get("task_id") or "")
        if not task_id:
            raise ValueError("missing task_id")
        dog_id = str(msg.get("dog_id") or self._cfg.dog_id)
        task_type = str(msg.get("task_type") or "patrol")
        execute_time = str(msg.get("execute_time") or "immediate")
        priority = int(msg.get("priority") or 3)
        route_json = msg.get("route_json")
        if not isinstance(route_json, dict):
            route_json = {}
        zip_url = str(msg.get("zip_url") or "")
        map_md5 = str(msg.get("map_md5") or route_json.get("map_md5") or "")
        seq_id = str(msg.get("seq_id") or "")
        create_time = str(msg.get("timestamp") or msg.get("create_time") or "")
        status = str(msg.get("status") or "pending")
        return Task(
            task_id=task_id,
            dog_id=dog_id,
            task_type=task_type,
            execute_time=execute_time,
            priority=priority,
            route_json=route_json,
            zip_url=zip_url,
            map_md5=map_md5,
            seq_id=seq_id,
            status=status,
            create_time=create_time,
        )

    async def _run_loop(self) -> None:
        while not self._stop_evt.is_set():
            try:
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._log.warning("tick error: %s", e)
            await asyncio.sleep(0.5)

    async def _tick(self) -> None:
        if self._current_task_id:
            return
        tasks = self._store.list_tasks(["pending"])
        if not tasks:
            return
        tasks.sort(key=lambda t: (t.priority, t.execute_time))
        for task in tasks:
            if task.dog_id and task.dog_id != self._cfg.dog_id:
                continue
            if not self._is_time_to_run(task.execute_time):
                continue
            self._current_task_id = task.task_id
            asyncio.create_task(self._execute_task(task))
            return

    def _is_time_to_run(self, execute_time: str) -> bool:
        if execute_time == "immediate":
            return True
        try:
            dt = datetime.fromisoformat(execute_time)
        except Exception:
            try:
                dt = datetime.strptime(execute_time, "%Y-%m-%d %H:%M:%S")
            except Exception:
                return True
        return datetime.now(dt.tzinfo) >= dt

    async def _execute_task(self, task: Task) -> None:
        ctx = self._store.load_context(task.task_id) or TaskContext(task_id=task.task_id)
        if not ctx.execute_start_time:
            ctx.execute_start_time = time.strftime("%Y-%m-%d %H:%M:%S")
        self._store.update_status(task.task_id, "executing")

        try:
            ok, err, start_pid = await self._precheck(task)
            if not ok:
                ctx.last_error_code = 3005
                ctx.last_error_msg = err
                self._store.save_context(ctx)
                self._store.update_status(task.task_id, "failed")
                await self._client.send_alarm(alarm_level=2, alarm_msg=f"任务前置校验失败: {err}", error_code=3005)
                await self._send_result(task, ctx, execute_result="failed")
                return
            if not ctx.start_point_id and start_pid:
                ctx.start_point_id = start_pid
                self._store.save_context(ctx)

            map_path = await self._prepare_files(task)
            wps, loops, return_pid = self._build_execution_plan(task, map_path)
            if not wps:
                raise RuntimeError("empty route")
            if not return_pid and ctx.start_point_id:
                return_pid = ctx.start_point_id

            completed_total = 0
            total_points = len(wps) * loops
            for loop_idx in range(loops):
                for wp in wps:
                    if self._cancel_flags.get(task.task_id):
                        self._store.update_status(task.task_id, "cancelled")
                        await self._send_result(task, ctx, execute_result="cancelled")
                        return
                    if self._pause_flags.get(task.task_id):
                        self._store.save_context(ctx)
                        self._store.update_status(task.task_id, "paused")
                        await self._send_result(task, ctx, execute_result="paused")
                        return
                    ctx.current_point_id = wp.point_id
                    if wp.section_occupy and wp.section_id:
                        ok_lock = await self._locker.acquire(wp.section_id, timeout_s=self._cfg.default_section_timeout_s)
                        if not ok_lock:
                            ctx.failed_points.append(wp.point_id)
                            ctx.last_error_code = 3001
                            ctx.last_error_msg = "section occupy timeout"
                            self._store.save_context(ctx)
                            await self._client.send_alarm(alarm_level=2, alarm_msg="路段占用超时", error_code=3001, context={"point_id": wp.point_id, "section_id": wp.section_id})
                            await self._send_progress(task, ctx, completed_total, total_points, execute_result="executing")
                            continue
                        if wp.section_id not in ctx.occupy_sections:
                            ctx.occupy_sections.append(wp.section_id)
                            self._store.save_context(ctx)
                    try:
                        nav_ok = False
                        nav_err = ""
                        for _ in range(self._cfg.waypoint_retry):
                            nav_ok, nav_err = await self._dispatch_nav(wp)
                            if nav_ok:
                                break
                        if not nav_ok:
                            ctx.failed_points.append(wp.point_id)
                            ctx.last_error_code = 3008
                            ctx.last_error_msg = nav_err
                            await self._client.send_alarm(alarm_level=2, alarm_msg=f"路径点执行失败: {nav_err}", error_code=3008, context={"point_id": wp.point_id})
                        else:
                            ctx.completed_points.append(wp.point_id)
                            completed_total += 1
                        ctx.execute_duration_s = int(time.time() - time.mktime(time.strptime(ctx.execute_start_time, "%Y-%m-%d %H:%M:%S")))
                        ctx.complete_rate = float(completed_total) / float(total_points) if total_points else 0.0
                        self._store.save_context(ctx)
                        await self._send_progress(task, ctx, completed_total, total_points, execute_result="executing")
                    finally:
                        if wp.section_occupy and wp.section_id:
                            await self._locker.release(wp.section_id)
                            if wp.section_id in ctx.occupy_sections:
                                ctx.occupy_sections.remove(wp.section_id)
                                self._store.save_context(ctx)

            if return_pid:
                return_wp = Waypoint(point_id=return_pid, coordinate=(0.0, 0.0, 0.0), stay_time_s=0, detect_type="navigation", section_id="", section_occupy=False)
                await self._dispatch_nav(return_wp)

            repeat_daily = self._route_bool(task.route_json, "repeat_daily", "daily", "everyday")
            if ctx.complete_rate >= 1.0:
                if repeat_daily:
                    next_time = self._next_day_same_time(task.execute_time)
                    task.execute_time = next_time
                    task.status = "pending"
                    ctx = TaskContext(task_id=task.task_id)
                    self._store.upsert_task(task)
                    self._store.save_context(ctx)
                    await self._send_result(task, ctx, execute_result="completed")
                    return
                self._store.update_status(task.task_id, "completed")
                await self._send_result(task, ctx, execute_result="completed")
            else:
                self._store.update_status(task.task_id, "failed")
                await self._send_result(task, ctx, execute_result="failed")
        except Exception as e:
            ctx.last_error_code = 3005
            ctx.last_error_msg = str(e)
            self._store.save_context(ctx)
            self._store.update_status(task.task_id, "failed")
            await self._client.send_alarm(alarm_level=2, alarm_msg=f"任务执行异常: {e}", error_code=3005)
            await self._send_result(task, ctx, execute_result="failed")
        finally:
            self._current_task_id = None

    async def _precheck(self, task: Task) -> Tuple[bool, str, str]:
        if task.dog_id and task.dog_id != self._cfg.dog_id:
            return False, "dog_id mismatch", ""
        st = self._get_dog_status()
        if int(st.get("battery", 100)) < 20:
            return False, "battery low", ""
        if st.get("online") is False:
            return False, "dog offline", ""
        start_pid = str(st.get("current_point_id") or st.get("point_id") or "")
        return True, "", start_pid

    async def _prepare_files(self, task: Task) -> Optional[str]:
        if not task.zip_url:
            return self._current_map_path
        root = os.path.join(self._cfg.download_root, "task", task.task_id)
        os.makedirs(root, exist_ok=True)
        dest_zip = os.path.join(root, "task.zip")
        await self._client.download_file(task.zip_url, dest_zip, expected_md5=None)

        extracted = os.path.join(root, "pkg")
        info = extract_task_zip(dest_zip, extracted)
        install_name = f"{task.task_id}" if task.task_id else f"map_{int(time.time())}"
        target_map_dir = os.path.join(self._cfg.map_install_root, install_name, "map")
        install_map_folder(info.map_dir, target_map_dir, symlink_path=self._cfg.map_symlink_path)

        try:
            os.makedirs(os.path.dirname(self._cfg.map_path_json_target) or ".", exist_ok=True)
            with open(info.map_path_json, "rb") as src, open(self._cfg.map_path_json_target, "wb") as dst:
                dst.write(src.read())
        except Exception:
            pass
        self._current_map_path = self._cfg.map_path_json_target
        return self._current_map_path

    def _build_execution_plan(self, task: Task, map_path_file: Optional[str]) -> Tuple[List[Waypoint], int, str]:
        route = task.route_json or {}
        if isinstance(route.get("waypoints"), list) or isinstance(route.get("points"), list):
            wps = parse_route(route)
            loops = int(route.get("patrol_times") or route.get("loops") or 1)
            return_pid = str(route.get("return_point_id") or route.get("return_to") or "")
            return wps, max(1, loops), return_pid

        point_ids = route.get("point_ids") or route.get("point_id_list") or route.get("point_list") or []
        if isinstance(point_ids, str):
            point_ids = [point_ids]
        if not isinstance(point_ids, list):
            point_ids = []
        point_ids = [str(x) for x in point_ids if x is not None and str(x)]

        loops = int(route.get("patrol_times") or route.get("loops") or route.get("n") or 1)
        return_to = route.get("return_point_id") or route.get("return_to") or ""
        return_to_s = str(return_to or "")
        return_pid = return_to_s

        if not point_ids:
            return [], max(1, loops), return_pid

        if not map_path_file:
            raise RuntimeError("map_path_json missing for point_ids route")

        mp = load_map_path_json(map_path_file)
        idx = build_point_index(mp)
        home_pid = pick_home_point_id(mp)
        if return_to_s.lower() in ("home", "home_point", "home_point_id"):
            return_pid = home_pid
        elif return_to_s.lower() in ("start", "start_point", "start_point_id"):
            return_pid = ""
        elif not return_pid:
            return_pid = home_pid

        default_stay = int(route.get("default_stay_time") or 0)
        wps = build_waypoints_from_point_ids(point_ids, idx, default_stay_s=default_stay, strict=True)
        return wps, max(1, loops), return_pid

    @staticmethod
    def _route_bool(route: Dict[str, Any], *keys: str) -> bool:
        for k in keys:
            v = route.get(k)
            if isinstance(v, bool):
                return v
            if isinstance(v, str) and v.lower() in ("1", "true", "yes", "y"):
                return True
        return False

    @staticmethod
    def _next_day_same_time(execute_time: str) -> str:
        if execute_time == "immediate":
            dt = datetime.now()
        else:
            try:
                dt = datetime.fromisoformat(execute_time)
            except Exception:
                try:
                    dt = datetime.strptime(execute_time, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    dt = datetime.now()
        nd = dt + timedelta(days=1)
        try:
            return nd.isoformat()
        except Exception:
            return nd.strftime("%Y-%m-%d %H:%M:%S")

    async def _send_progress(self, task: Task, ctx: TaskContext, completed_total: int, total_points: int, execute_result: str) -> None:
        await self._client.send_json(
            "result_upload",
            task_id=task.task_id,
            execute_result=execute_result,
            complete_rate=int((float(completed_total) / float(total_points) * 100) if total_points else 0),
            execute_duration=int(ctx.execute_duration_s),
            error_detail=ctx.last_error_msg,
            current_point_id=ctx.current_point_id,
        )

    async def _send_result(self, task: Task, ctx: TaskContext, execute_result: str) -> None:
        await self._client.send_json(
            "result_upload",
            task_id=task.task_id,
            execute_result=execute_result,
            complete_rate=int(ctx.complete_rate * 100),
            execute_duration=int(ctx.execute_duration_s),
            error_detail=ctx.last_error_msg,
            current_point_id=ctx.current_point_id,
        )

    async def _dispatch_nav_stub(self, wp: Waypoint) -> Tuple[bool, str]:
        await asyncio.sleep(max(wp.stay_time_s, 0))
        return True, ""

    def _get_dog_status_stub(self) -> Dict[str, Any]:
        return {"online": True, "battery": 100}
