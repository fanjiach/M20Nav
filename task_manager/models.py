import dataclasses
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class Waypoint:
    point_id: str
    coordinate: Tuple[float, float, float]
    stay_time_s: int
    detect_type: str
    section_id: str
    section_occupy: bool = False


@dataclass
class Task:
    task_id: str
    dog_id: str
    task_type: str
    execute_time: str
    priority: int
    route_json: Dict[str, Any]
    zip_url: str = ""
    map_md5: str = ""
    seq_id: str = ""
    status: str = "pending"
    create_time: str = ""


@dataclass
class TaskContext:
    task_id: str
    start_point_id: str = ""
    current_point_id: str = ""
    completed_points: List[str] = dataclasses.field(default_factory=list)
    failed_points: List[str] = dataclasses.field(default_factory=list)
    execute_start_time: str = ""
    execute_duration_s: int = 0
    complete_rate: float = 0.0
    last_error_code: int = 0
    last_error_msg: str = ""
    occupy_sections: List[str] = dataclasses.field(default_factory=list)
