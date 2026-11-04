"""Local filesystem persistence: append-free raw copies and atomic derived outputs."""
from pathlib import Path, PureWindowsPath
import hashlib
import json
import os
import tempfile
from typing import Any


def ensure_data_dirs(root: Path) -> None:
    for section in ("raw", "processed", "rejected", "reports", "manifests", "generated"):
        (root / section).mkdir(parents=True, exist_ok=True)


def atomic_bytes(path: Path, content: bytes, *, overwrite: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        raise FileExistsError(str(path))
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".tmp-", delete=False) as tmp:
            temp_path = Path(tmp.name)
            tmp.write(content)
            tmp.flush()
            os.fsync(tmp.fileno())
        os.replace(temp_path, path)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def atomic_json(path: Path, data: Any, *, overwrite: bool = False) -> None:
    atomic_bytes(path, (json.dumps(data, indent=2, default=str, allow_nan=False) + "\n").encode(),
                 overwrite=overwrite)


def atomic_copy(source: Path, target: Path) -> str:
    target.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    temp_path = None
    try:
        with source.open("rb") as src, tempfile.NamedTemporaryFile(
            dir=target.parent, prefix=".tmp-", delete=False
        ) as out:
            temp_path = Path(out.name)
            for chunk in iter(lambda: src.read(1024 * 1024), b""):
                digest.update(chunk)
                out.write(chunk)
            out.flush()
            os.fsync(out.fileno())
        # Hard-link creation is atomic and refuses to replace an existing immutable raw file.
        os.link(temp_path, target)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
    return digest.hexdigest()


def safe_relative_path(path: str) -> Path:
    rel = Path(path)
    windows_path = PureWindowsPath(path)
    if ("\\" in path or rel.is_absolute() or rel.anchor or windows_path.is_absolute()
            or windows_path.drive or ".." in rel.parts or ".." in windows_path.parts):
        raise ValueError("Absolute and parent-relative paths are not allowed")
    return rel
