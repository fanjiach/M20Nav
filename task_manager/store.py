import json
import os
import sqlite3
import time
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Tuple

from .models import Task, TaskContext


class TaskStore:
    def __init__(self, db_path: str):
        self._db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS task_info (
                    task_id TEXT PRIMARY KEY,
                    dog_id TEXT,
                    task_type TEXT,
                    execute_time TEXT,
                    priority INTEGER,
                    route_json TEXT,
                    zip_url TEXT,
                    map_md5 TEXT,
                    status TEXT,
                    create_time TEXT,
                    update_time TEXT,
                    seq_id TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS task_context (
                    task_id TEXT PRIMARY KEY,
                    start_point_id TEXT,
                    current_point_id TEXT,
                    completed_points TEXT,
                    failed_points TEXT,
                    execute_start_time TEXT,
                    execute_duration INTEGER,
                    complete_rate REAL,
                    last_error_code INTEGER,
                    last_error_msg TEXT,
                    occupy_sections TEXT
                )
                """
            )
            cols = {r[1] for r in conn.execute("PRAGMA table_info(task_context)").fetchall()}
            if "start_point_id" not in cols:
                conn.execute("ALTER TABLE task_context ADD COLUMN start_point_id TEXT")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_task_status ON task_info(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_task_priority ON task_info(priority)")
            conn.commit()
        finally:
            conn.close()

    def upsert_task(self, task: Task) -> None:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO task_info(task_id,dog_id,task_type,execute_time,priority,route_json,zip_url,map_md5,status,create_time,update_time,seq_id)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(task_id) DO UPDATE SET
                    dog_id=excluded.dog_id,
                    task_type=excluded.task_type,
                    execute_time=excluded.execute_time,
                    priority=excluded.priority,
                    route_json=excluded.route_json,
                    zip_url=excluded.zip_url,
                    map_md5=excluded.map_md5,
                    status=excluded.status,
                    update_time=excluded.update_time,
                    seq_id=excluded.seq_id
                """,
                (
                    task.task_id,
                    task.dog_id,
                    task.task_type,
                    task.execute_time,
                    task.priority,
                    json.dumps(task.route_json, ensure_ascii=False, separators=(",", ":")),
                    task.zip_url,
                    task.map_md5,
                    task.status,
                    task.create_time or now,
                    now,
                    task.seq_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def update_status(self, task_id: str, status: str) -> None:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        conn = self._connect()
        try:
            conn.execute("UPDATE task_info SET status=?, update_time=? WHERE task_id=?", (status, now, task_id))
            conn.commit()
        finally:
            conn.close()

    def get_task(self, task_id: str) -> Optional[Task]:
        conn = self._connect()
        try:
            row = conn.execute("SELECT * FROM task_info WHERE task_id=?", (task_id,)).fetchone()
            if row is None:
                return None
            return Task(
                task_id=row["task_id"],
                dog_id=row["dog_id"] or "",
                task_type=row["task_type"] or "",
                execute_time=row["execute_time"] or "",
                priority=int(row["priority"] or 3),
                route_json=json.loads(row["route_json"] or "{}"),
                zip_url=row["zip_url"] or "",
                map_md5=row["map_md5"] or "",
                seq_id=row["seq_id"] or "",
                status=row["status"] or "pending",
                create_time=row["create_time"] or "",
            )
        finally:
            conn.close()

    def list_tasks(self, statuses: List[str]) -> List[Task]:
        if not statuses:
            return []
        conn = self._connect()
        try:
            qs = ",".join(["?"] * len(statuses))
            rows = conn.execute(f"SELECT * FROM task_info WHERE status IN ({qs}) ORDER BY priority ASC, execute_time ASC", statuses).fetchall()
            out: List[Task] = []
            for row in rows:
                out.append(
                    Task(
                        task_id=row["task_id"],
                        dog_id=row["dog_id"] or "",
                        task_type=row["task_type"] or "",
                        execute_time=row["execute_time"] or "",
                        priority=int(row["priority"] or 3),
                        route_json=json.loads(row["route_json"] or "{}"),
                        zip_url=row["zip_url"] or "",
                        map_md5=row["map_md5"] or "",
                        seq_id=row["seq_id"] or "",
                        status=row["status"] or "pending",
                        create_time=row["create_time"] or "",
                    )
                )
            return out
        finally:
            conn.close()

    def save_context(self, ctx: TaskContext) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO task_context(task_id,start_point_id,current_point_id,completed_points,failed_points,execute_start_time,execute_duration,complete_rate,last_error_code,last_error_msg,occupy_sections)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(task_id) DO UPDATE SET
                    start_point_id=excluded.start_point_id,
                    current_point_id=excluded.current_point_id,
                    completed_points=excluded.completed_points,
                    failed_points=excluded.failed_points,
                    execute_start_time=excluded.execute_start_time,
                    execute_duration=excluded.execute_duration,
                    complete_rate=excluded.complete_rate,
                    last_error_code=excluded.last_error_code,
                    last_error_msg=excluded.last_error_msg,
                    occupy_sections=excluded.occupy_sections
                """,
                (
                    ctx.task_id,
                    ctx.start_point_id,
                    ctx.current_point_id,
                    json.dumps(ctx.completed_points, ensure_ascii=False, separators=(",", ":")),
                    json.dumps(ctx.failed_points, ensure_ascii=False, separators=(",", ":")),
                    ctx.execute_start_time,
                    int(ctx.execute_duration_s),
                    float(ctx.complete_rate),
                    int(ctx.last_error_code),
                    ctx.last_error_msg,
                    json.dumps(ctx.occupy_sections, ensure_ascii=False, separators=(",", ":")),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def load_context(self, task_id: str) -> Optional[TaskContext]:
        conn = self._connect()
        try:
            row = conn.execute("SELECT * FROM task_context WHERE task_id=?", (task_id,)).fetchone()
            if row is None:
                return None
            return TaskContext(
                task_id=row["task_id"],
                start_point_id=(row["start_point_id"] or "") if "start_point_id" in row.keys() else "",
                current_point_id=row["current_point_id"] or "",
                completed_points=json.loads(row["completed_points"] or "[]"),
                failed_points=json.loads(row["failed_points"] or "[]"),
                execute_start_time=row["execute_start_time"] or "",
                execute_duration_s=int(row["execute_duration"] or 0),
                complete_rate=float(row["complete_rate"] or 0.0),
                last_error_code=int(row["last_error_code"] or 0),
                last_error_msg=row["last_error_msg"] or "",
                occupy_sections=json.loads(row["occupy_sections"] or "[]"),
            )
        finally:
            conn.close()
