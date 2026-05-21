from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .models import Waypoint


def _to_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


@dataclass(frozen=True)
class NormalizedMapPath:
    map_id: str
    map_version: str
    home_point_id: str
    points: Dict[str, Waypoint]
    segments: List[Dict[str, Any]]


def normalize_map_path_json(map_path: Dict[str, Any]) -> NormalizedMapPath:
    if not isinstance(map_path, dict):
        raise RuntimeError("map_path_json must be object")

    map_id = str(map_path.get("map_id") or map_path.get("id") or "")
    map_version = str(map_path.get("map_version") or map_path.get("version") or "")

    home_point_id = pick_home_point_id(map_path)

    points_raw = map_path.get("points")
    points: Dict[str, Waypoint] = {}
    if isinstance(points_raw, dict):
        for pid, p in points_raw.items():
            if not isinstance(p, dict):
                continue
            pid_s = str(pid)
            points[pid_s] = _waypoint_from_point_dict(pid_s, p)
    elif isinstance(points_raw, list):
        for p in points_raw:
            if not isinstance(p, dict):
                continue
            pid_s = str(p.get("point_id") or p.get("id") or p.get("wp_id") or "")
            if not pid_s:
                continue
            points[pid_s] = _waypoint_from_point_dict(pid_s, p)
    else:
        wps = map_path.get("waypoints")
        if isinstance(wps, list):
            for p in wps:
                if not isinstance(p, dict):
                    continue
                pid_s = str(p.get("point_id") or p.get("wp_id") or "")
                if not pid_s:
                    continue
                points[pid_s] = _waypoint_from_point_dict(pid_s, p)

    if not points:
        raise RuntimeError("map_path_json.points required")

    segments_raw = map_path.get("segments")
    segments: List[Dict[str, Any]] = []
    if isinstance(segments_raw, list):
        for s in segments_raw:
            if isinstance(s, dict):
                segments.append(s)

    _validate_segments(points, segments)
    return NormalizedMapPath(map_id=map_id, map_version=map_version, home_point_id=home_point_id, points=points, segments=segments)


def _validate_segments(points: Dict[str, Waypoint], segments: List[Dict[str, Any]]) -> None:
    if not segments:
        return
    for s in segments:
        src = str(s.get("from") or s.get("from_point_id") or s.get("src") or "")
        dst = str(s.get("to") or s.get("to_point_id") or s.get("dst") or "")
        if not src or not dst:
            raise RuntimeError("map_path_json.segments requires from/to")
        if src not in points or dst not in points:
            raise RuntimeError(f"map_path_json.segments invalid endpoint: {src}->{dst}")


def build_point_index(map_path: Dict[str, Any]) -> Dict[str, Waypoint]:
    return normalize_map_path_json(map_path).points


def _waypoint_from_point_dict(pid: str, p: Dict[str, Any]) -> Waypoint:
    if "coordinate" in p and isinstance(p["coordinate"], (list, tuple)) and len(p["coordinate"]) >= 3:
        c = p["coordinate"]
        coord: Tuple[float, float, float] = (_to_float(c[0]), _to_float(c[1]), _to_float(c[2]))
    else:
        coord = (_to_float(p.get("x")), _to_float(p.get("y")), _to_float(p.get("z")))

    stay = int(p.get("stay_time") or p.get("stay_time_s") or 0)
    detect = str(p.get("detect_type") or "navigation")
    section_id = str(p.get("section_id") or "")
    occupy = bool(p.get("section_occupy") or False)
    return Waypoint(point_id=pid, coordinate=coord, stay_time_s=stay, detect_type=detect, section_id=section_id, section_occupy=occupy)


def build_waypoints_from_point_ids(
    point_ids: List[str],
    point_index: Dict[str, Waypoint],
    default_stay_s: int = 0,
    strict: bool = True,
) -> List[Waypoint]:
    out: List[Waypoint] = []
    for pid in point_ids:
        wp = point_index.get(pid)
        if wp is None:
            if strict:
                raise RuntimeError(f"point_id not found in map_path_json: {pid}")
            out.append(
                Waypoint(
                    point_id=pid,
                    coordinate=(0.0, 0.0, 0.0),
                    stay_time_s=default_stay_s,
                    detect_type="navigation",
                    section_id="",
                    section_occupy=False,
                )
            )
            continue
        if wp.stay_time_s == 0 and default_stay_s:
            out.append(
                Waypoint(
                    point_id=wp.point_id,
                    coordinate=wp.coordinate,
                    stay_time_s=default_stay_s,
                    detect_type=wp.detect_type,
                    section_id=wp.section_id,
                    section_occupy=wp.section_occupy,
                )
            )
        else:
            out.append(wp)
    return out


def pick_home_point_id(map_path: Dict[str, Any]) -> str:
    for k in ("home_point_id", "start_point_id", "origin_point_id"):
        v = map_path.get(k)
        if isinstance(v, str) and v:
            return v
    return ""
