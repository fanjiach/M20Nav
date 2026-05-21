import asyncio
import hashlib
import os
import pathlib
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

try:
    import aiohttp

    _HAS_AIOHTTP = True
except Exception:
    aiohttp = None
    _HAS_AIOHTTP = False


@dataclass(frozen=True)
class FileVerifyResult:
    md5: str
    segment_md5_list: List[str]
    size_bytes: int


@dataclass(frozen=True)
class FileDownloadResult:
    url: str
    dest_path: str
    tmp_path: str
    size_bytes: int
    md5: str
    segment_md5_list: List[str]
    resumed: bool


def _ensure_dir(p: pathlib.Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _default_tmp_dir() -> pathlib.Path:
    if os.name == "nt":
        return pathlib.Path(os.environ.get("TEMP", ".")) / "robot_download"
    return pathlib.Path("/tmp/robot_download")


def verify_file_md5(path: str, segment_size_bytes: int = 1024 * 1024) -> FileVerifyResult:
    h = hashlib.md5()
    seg_h = hashlib.md5()
    seg_size = 0
    seg_list: List[str] = []
    size = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(64 * 1024)
            if not chunk:
                break
            size += len(chunk)
            h.update(chunk)
            off = 0
            while off < len(chunk):
                take = min(segment_size_bytes - seg_size, len(chunk) - off)
                seg_h.update(chunk[off : off + take])
                seg_size += take
                off += take
                if seg_size == segment_size_bytes:
                    seg_list.append(seg_h.hexdigest())
                    seg_h = hashlib.md5()
                    seg_size = 0
    if seg_size > 0:
        seg_list.append(seg_h.hexdigest())
    return FileVerifyResult(md5=h.hexdigest(), segment_md5_list=seg_list, size_bytes=size)


async def download_file(
    url: str,
    dest_path: str,
    expected_md5: Optional[str] = None,
    tmp_dir: Optional[str] = None,
    speed_limit_bytes_per_s: int = 1024 * 1024,
    timeout_s: int = 30,
    max_retries: int = 3,
    segment_size_bytes: int = 1024 * 1024,
) -> FileDownloadResult:
    if not _HAS_AIOHTTP:
        raise RuntimeError("aiohttp not available; install requirements.txt first")

    dest = pathlib.Path(dest_path)
    _ensure_dir(dest.parent)

    if dest.exists() and expected_md5:
        vr = verify_file_md5(str(dest), segment_size_bytes=segment_size_bytes)
        if vr.md5.lower() == expected_md5.lower():
            return FileDownloadResult(
                url=url,
                dest_path=str(dest),
                tmp_path=str(dest),
                size_bytes=vr.size_bytes,
                md5=vr.md5,
                segment_md5_list=vr.segment_md5_list,
                resumed=False,
            )

    td = pathlib.Path(tmp_dir) if tmp_dir else _default_tmp_dir()
    _ensure_dir(td)
    tmp = td / (dest.name + ".part")

    last_err: Optional[BaseException] = None
    for attempt in range(1, max_retries + 1):
        resumed = False
        resume_from = 0
        headers = {}
        mode = "wb"
        if tmp.exists():
            resume_from = tmp.stat().st_size
            if resume_from > 0:
                headers["Range"] = f"bytes={resume_from}-"
                mode = "ab"
                resumed = True
        try:
            timeout = aiohttp.ClientTimeout(total=timeout_s)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers) as resp:
                    if resp.status not in (200, 206):
                        raise RuntimeError(f"http status {resp.status}")
                    if resume_from > 0 and resp.status == 200:
                        tmp.unlink(missing_ok=True)  # type: ignore[arg-type]
                        resume_from = 0
                        mode = "wb"
                        resumed = False
                    bytes_window = 0
                    window_start = time.monotonic()
                    with open(tmp, mode) as f:
                        async for chunk in resp.content.iter_chunked(64 * 1024):
                            if not chunk:
                                continue
                            f.write(chunk)
                            bytes_window += len(chunk)
                            if speed_limit_bytes_per_s > 0 and bytes_window >= speed_limit_bytes_per_s:
                                now = time.monotonic()
                                elapsed = now - window_start
                                target = bytes_window / float(speed_limit_bytes_per_s)
                                if target > elapsed:
                                    await asyncio.sleep(target - elapsed)
                                window_start = time.monotonic()
                                bytes_window = 0

            vr = verify_file_md5(str(tmp), segment_size_bytes=segment_size_bytes)
            if expected_md5 and vr.md5.lower() != expected_md5.lower():
                tmp.unlink(missing_ok=True)  # type: ignore[arg-type]
                raise RuntimeError("md5 mismatch")
            os.replace(str(tmp), str(dest))
            return FileDownloadResult(
                url=url,
                dest_path=str(dest),
                tmp_path=str(tmp),
                size_bytes=vr.size_bytes,
                md5=vr.md5,
                segment_md5_list=vr.segment_md5_list,
                resumed=resumed,
            )
        except Exception as e:
            last_err = e
            try:
                await asyncio.sleep(min(3 * attempt, 10))
            except Exception:
                pass
            continue
    raise RuntimeError(f"download failed after {max_retries} retries: {last_err}")

