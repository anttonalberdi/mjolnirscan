from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import time
from typing import Iterable


@dataclass(frozen=True)
class DirResult:
    path: str
    size_bytes: int
    last_touched: float


@dataclass(frozen=True)
class ScanError:
    path: str
    message: str


@dataclass(frozen=True)
class ScanStats:
    dirs_scanned: int
    files_scanned: int
    skipped_symlinks: int
    skipped_other_fs: int
    errors: int


@dataclass(frozen=True)
class ScanReport:
    candidates: list[DirResult]
    errors: list[ScanError]
    stats: ScanStats


def scan_directories(
    root: str,
    min_size: int,
    cutoff: float,
    time_attr: str,
    *,
    follow_symlinks: bool = False,
    one_filesystem: bool = False,
    error_limit: int = 200,
) -> ScanReport:
    root = os.path.abspath(root)
    now = time.time()

    errors: list[ScanError] = []
    error_count = 0

    def record_error(path: str, exc: BaseException) -> None:
        nonlocal error_count
        error_count += 1
        if len(errors) < error_limit:
            errors.append(ScanError(path=path, message=str(exc)))

    root_dev = None
    if one_filesystem:
        try:
            root_dev = os.stat(root, follow_symlinks=follow_symlinks).st_dev
        except OSError as exc:
            record_error(root, exc)
            return ScanReport(
                candidates=[],
                errors=errors,
                stats=ScanStats(
                    dirs_scanned=0,
                    files_scanned=0,
                    skipped_symlinks=0,
                    skipped_other_fs=0,
                    errors=error_count,
                ),
            )

    stack: list[tuple[str, str | None, bool]] = [(root, None, False)]
    aggregate: dict[str, list[float | int]] = {}
    candidates: list[DirResult] = []

    dirs_scanned = 0
    files_scanned = 0
    skipped_symlinks = 0
    skipped_other_fs = 0

    visited_dirs: set[tuple[int, int]] = set()

    scandir = os.scandir
    stat = os.stat

    while stack:
        path, parent, visited = stack.pop()
        if not visited:
            dir_stat = None
            newest = now
            try:
                dir_stat = stat(path, follow_symlinks=follow_symlinks)
                newest = getattr(dir_stat, time_attr)
            except OSError as exc:
                record_error(path, exc)
                newest = now

            if follow_symlinks and dir_stat is not None:
                key = (dir_stat.st_dev, dir_stat.st_ino)
                if key in visited_dirs:
                    skipped_symlinks += 1
                    continue
                visited_dirs.add(key)

            stack.append((path, parent, True))
            size = 0

            aggregate[path] = [size, newest]
            dirs_scanned += 1

            try:
                with scandir(path) as it:
                    for entry in it:
                        try:
                            if entry.is_dir(follow_symlinks=follow_symlinks):
                                if entry.is_symlink() and not follow_symlinks:
                                    skipped_symlinks += 1
                                    continue
                                if one_filesystem:
                                    try:
                                        entry_stat = entry.stat(
                                            follow_symlinks=follow_symlinks
                                        )
                                    except OSError as exc:
                                        record_error(entry.path, exc)
                                        newest = max(newest, now)
                                        continue
                                    if entry_stat.st_dev != root_dev:
                                        skipped_other_fs += 1
                                        continue
                                stack.append((entry.path, path, False))
                            else:
                                if entry.is_symlink() and not follow_symlinks:
                                    skipped_symlinks += 1
                                    continue
                                try:
                                    entry_stat = entry.stat(
                                        follow_symlinks=follow_symlinks
                                    )
                                except OSError as exc:
                                    record_error(entry.path, exc)
                                    newest = max(newest, now)
                                    continue
                                files_scanned += 1
                                size += entry_stat.st_size
                                entry_time = getattr(entry_stat, time_attr)
                                if entry_time > newest:
                                    newest = entry_time
                        except OSError as exc:
                            record_error(entry.path, exc)
                            newest = max(newest, now)
            except OSError as exc:
                record_error(path, exc)
                newest = max(newest, now)

            aggregate[path] = [size, newest]
        else:
            size, newest = aggregate.pop(path, [0, now])
            if size >= min_size and newest <= cutoff:
                candidates.append(DirResult(path=path, size_bytes=int(size), last_touched=float(newest)))

            if parent is not None:
                parent_size, parent_newest = aggregate.get(parent, [0, now])
                parent_size += size
                if newest > parent_newest:
                    parent_newest = newest
                aggregate[parent] = [parent_size, parent_newest]

    stats = ScanStats(
        dirs_scanned=dirs_scanned,
        files_scanned=files_scanned,
        skipped_symlinks=skipped_symlinks,
        skipped_other_fs=skipped_other_fs,
        errors=error_count,
    )
    return ScanReport(candidates=candidates, errors=errors, stats=stats)


def select_top_level(candidates: Iterable[DirResult]) -> list[DirResult]:
    candidates_list = list(candidates)
    candidates_list.sort(key=lambda item: (len(Path(item.path).parts), item.path))

    selected: list[DirResult] = []
    selected_paths: list[Path] = []

    for item in candidates_list:
        path = Path(item.path)
        if any(path.is_relative_to(base) for base in selected_paths):
            continue
        selected.append(item)
        selected_paths.append(path)

    return selected
