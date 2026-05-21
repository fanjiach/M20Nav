import json
import os
import pathlib
import shutil
import zipfile
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass(frozen=True)
class MapPackageInfo:
    extracted_dir: str
    map_dir: str
    map_path_json: str


def _is_hidden(p: pathlib.Path) -> bool:
    name = p.name
    return name.startswith(".") or name.startswith("__MACOSX")


def _find_map_dir_and_json(extracted_dir: str) -> MapPackageInfo:
    root = pathlib.Path(extracted_dir)
    map_dir: Optional[pathlib.Path] = None
    map_json: Optional[pathlib.Path] = None

    for p in root.rglob("*"):
        if _is_hidden(p):
            continue
        if p.is_file() and p.suffix.lower() == ".json":
            if p.name.lower() in ("map_path.json", "map_route.json", "map_points.json"):
                map_json = p
                break
            if map_json is None:
                map_json = p

    for p in root.iterdir():
        if _is_hidden(p):
            continue
        if p.is_dir():
            map_dir = p
            break

    if map_dir is None:
        for p in root.rglob("*"):
            if _is_hidden(p):
                continue
            if p.is_dir():
                map_dir = p
                break

    if map_dir is None:
        raise RuntimeError("map folder not found in zip")
    if map_json is None:
        raise RuntimeError("map path json not found in zip")

    return MapPackageInfo(extracted_dir=str(root), map_dir=str(map_dir), map_path_json=str(map_json))


def extract_task_zip(zip_path: str, extract_dir: str) -> MapPackageInfo:
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    return _find_map_dir_and_json(extract_dir)


def install_map_folder(
    map_dir: str,
    target_dir: str,
    symlink_path: Optional[str] = None,
) -> str:
    src = pathlib.Path(map_dir)
    if not src.exists() or not src.is_dir():
        raise RuntimeError("map_dir invalid")
    dst = pathlib.Path(target_dir)
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        shutil.rmtree(str(dst))
    shutil.copytree(str(src), str(dst))

    if symlink_path:
        link = pathlib.Path(symlink_path)
        try:
            if link.is_symlink() or link.exists():
                try:
                    if link.is_dir() and not link.is_symlink():
                        shutil.rmtree(str(link))
                    else:
                        link.unlink()
                except Exception:
                    pass
            link.parent.mkdir(parents=True, exist_ok=True)
            os.symlink(str(dst), str(link))
        except Exception:
            pass
    return str(dst)


def load_map_path_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

