from typing import Any, Dict, List, Tuple

from .models import Waypoint


def _to_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def parse_route(route_json: Dict[str, Any]) -> List[Waypoint]:
    pts = route_json.get("waypoints")
    if not isinstance(pts, list):
        pts = route_json.get("points")
    if not isinstance(pts, list):
        return []
    out: List[Waypoint] = []
    for p in pts:
        if not isinstance(p, dict):
            continue
        point_id = str(p.get("wp_id") or p.get("point_id") or "")
        if not point_id:
            continue
        if "coordinate" in p and isinstance(p["coordinate"], (list, tuple)) and len(p["coordinate"]) >= 3:
            c = p["coordinate"]
            coord: Tuple[float, float, float] = (_to_float(c[0]), _to_float(c[1]), _to_float(c[2]))
        else:
            coord = (_to_float(p.get("x")), _to_float(p.get("y")), _to_float(p.get("z")))
        stay = int(p.get("stay_time") or p.get("stay_time_s") or 0)
        detect = str(p.get("detect_type") or "navigation")
        section_id = str(p.get("section_id") or "")
        occupy = bool(p.get("section_occupy") or False)
        out.append(
            Waypoint(
                point_id=point_id,
                coordinate=coord,
                stay_time_s=stay,
                detect_type=detect,
                section_id=section_id,
                section_occupy=occupy,
            )
        )
    return out

