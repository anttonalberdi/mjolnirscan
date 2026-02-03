from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
import errno
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
    root_summary: DirResult | None


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
    root_summary: DirResult | None = None

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
            if path == root:
                root_summary = DirResult(
                    path=path, size_bytes=int(size), last_touched=float(newest)
                )

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
    return ScanReport(
        candidates=candidates, errors=errors, stats=stats, root_summary=root_summary
    )


def scan_directories_parallel(
    root: str,
    min_size: int,
    cutoff: float,
    time_attr: str,
    workers: int,
    *,
    follow_symlinks: bool = False,
    one_filesystem: bool = False,
    error_limit: int = 200,
) -> ScanReport:
    _patch_multiprocessing_tempdir_cleanup()
    if workers <= 1:
        return scan_directories(
            root=root,
            min_size=min_size,
            cutoff=cutoff,
            time_attr=time_attr,
            follow_symlinks=follow_symlinks,
            one_filesystem=one_filesystem,
            error_limit=error_limit,
        )

    root = os.path.abspath(root)
    now = time.time()

    errors: list[ScanError] = []
    error_count = 0

    def record_error(path: str, exc: BaseException) -> None:
        nonlocal error_count
        error_count += 1
        if len(errors) < error_limit:
            errors.append(ScanError(path=path, message=str(exc)))

    root_stat = None
    if one_filesystem:
        try:
            root_stat = os.stat(root, follow_symlinks=follow_symlinks)
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
                root_summary=None,
            )
    else:
        try:
            root_stat = os.stat(root, follow_symlinks=follow_symlinks)
        except OSError as exc:
            record_error(root, exc)

    root_dev = root_stat.st_dev if (one_filesystem and root_stat is not None) else None
    root_time = getattr(root_stat, time_attr) if root_stat is not None else now
    root_size = 0
    root_newest = root_time

    dirs_scanned = 1
    files_scanned = 0
    skipped_symlinks = 0
    skipped_other_fs = 0

    child_dirs: list[str] = []

    try:
        with os.scandir(root) as it:
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
                                root_newest = max(root_newest, now)
                                continue
                            if entry_stat.st_dev != root_dev:
                                skipped_other_fs += 1
                                continue
                        child_dirs.append(entry.path)
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
                            root_newest = max(root_newest, now)
                            continue
                        files_scanned += 1
                        root_size += entry_stat.st_size
                        entry_time = getattr(entry_stat, time_attr)
                        if entry_time > root_newest:
                            root_newest = entry_time
                except OSError as exc:
                    record_error(entry.path, exc)
                    root_newest = max(root_newest, now)
    except OSError as exc:
        record_error(root, exc)
        root_newest = max(root_newest, now)

    candidates: list[DirResult] = []
    reports: list[ScanReport] = []

    if child_dirs:
        max_workers = min(workers, len(child_dirs))
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    scan_directories,
                    child,
                    min_size,
                    cutoff,
                    time_attr,
                    follow_symlinks=follow_symlinks,
                    one_filesystem=one_filesystem,
                    error_limit=error_limit,
                ): child
                for child in child_dirs
            }
            for future in as_completed(futures):
                child = futures[future]
                try:
                    report = future.result()
                except Exception as exc:
                    record_error(child, exc)
                    root_newest = max(root_newest, now)
                else:
                    reports.append(report)

    combined_errors = list(errors)
    total_errors = error_count

    for report in reports:
        candidates.extend(report.candidates)
        total_errors += report.stats.errors
        dirs_scanned += report.stats.dirs_scanned
        files_scanned += report.stats.files_scanned
        skipped_symlinks += report.stats.skipped_symlinks
        skipped_other_fs += report.stats.skipped_other_fs

        for err in report.errors:
            if len(combined_errors) >= error_limit:
                break
            combined_errors.append(err)

        if report.root_summary is not None:
            root_size += report.root_summary.size_bytes
            if report.root_summary.last_touched > root_newest:
                root_newest = report.root_summary.last_touched
        else:
            root_newest = max(root_newest, now)

    root_summary = DirResult(
        path=root, size_bytes=int(root_size), last_touched=float(root_newest)
    )
    if root_size >= min_size and root_newest <= cutoff:
        candidates.append(root_summary)

    stats = ScanStats(
        dirs_scanned=dirs_scanned,
        files_scanned=files_scanned,
        skipped_symlinks=skipped_symlinks,
        skipped_other_fs=skipped_other_fs,
        errors=total_errors,
    )

    return ScanReport(
        candidates=candidates,
        errors=combined_errors,
        stats=stats,
        root_summary=root_summary,
    )


def _patch_multiprocessing_tempdir_cleanup() -> None:
    try:
        import multiprocessing.util as mp_util
    except Exception:
        return

    if getattr(mp_util, "_mjolnirscan_patched", False):
        return

    orig = getattr(mp_util, "_remove_temp_dir", None)
    if orig is None:
        return

    def _patched_remove_temp_dir(*args, **kwargs):
        try:
            return orig(*args, **kwargs)
        except OSError as exc:
            if exc.errno == errno.ENOTEMPTY:
                return
            raise

    mp_util._remove_temp_dir = _patched_remove_temp_dir
    mp_util._mjolnirscan_patched = True


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
