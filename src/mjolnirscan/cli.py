from __future__ import annotations

import argparse
import datetime as dt
import html
import os
import re
import sys
import time

from . import __version__
from .scanner import DirResult, ScanReport, scan_directories, select_top_level


_DURATION_UNITS = {
    "s": 1,
    "m": 60,
    "h": 60 * 60,
    "d": 60 * 60 * 24,
    "w": 60 * 60 * 24 * 7,
    "y": 60 * 60 * 24 * 365,
}

_DECIMAL_UNITS = {
    "kb": 1000**1,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "pb": 1000**5,
}

_BINARY_UNITS = {
    "k": 1024**1,
    "m": 1024**2,
    "g": 1024**3,
    "t": 1024**4,
    "p": 1024**5,
    "kib": 1024**1,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
    "pib": 1024**5,
}


def parse_duration(value: str) -> int:
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([smhdwy]?)\s*", value.lower())
    if not match:
        raise argparse.ArgumentTypeError(
            "Invalid duration. Examples: 30d, 12h, 4w, 1y"
        )
    amount = float(match.group(1))
    unit = match.group(2) or "d"
    if amount < 0:
        raise argparse.ArgumentTypeError("Duration must be non-negative")
    return int(amount * _DURATION_UNITS[unit])


def parse_size(value: str) -> int:
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([a-z]*)\s*", value.lower())
    if not match:
        raise argparse.ArgumentTypeError(
            "Invalid size. Examples: 500g, 1.5tb, 200gb, 750m"
        )
    amount = float(match.group(1))
    unit = match.group(2)
    if amount < 0:
        raise argparse.ArgumentTypeError("Size must be non-negative")
    if unit in ("", "b", "bytes"):
        multiplier = 1
    elif unit in _DECIMAL_UNITS:
        multiplier = _DECIMAL_UNITS[unit]
    elif unit in _BINARY_UNITS:
        multiplier = _BINARY_UNITS[unit]
    else:
        raise argparse.ArgumentTypeError(
            "Unknown size unit. Use B, KB, MB, GB, TB, PB, or KiB, MiB, GiB"
        )
    return int(amount * multiplier)


def format_size(num_bytes: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f}{unit}" if unit != "B" else f"{int(size)}B"
        size /= 1024
    return f"{int(num_bytes)}B"


def format_elapsed(seconds: float) -> str:
    total = int(seconds)
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def format_timestamp(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d")


def render_text(
    results: list[DirResult],
    report: ScanReport,
    cutoff: float,
    min_size: int,
    elapsed: float,
) -> None:
    cutoff_date = format_timestamp(cutoff)
    print(
        f"Scanned {report.stats.dirs_scanned} directories, "
        f"{report.stats.files_scanned} files in {format_elapsed(elapsed)}"
    )
    print(
        f"Criteria: last touched before {cutoff_date} and size >= {format_size(min_size)}"
    )

    if not results:
        print("No directories matched the criteria.")
    else:
        print(f"Flagged {len(results)} top-level directories:")
        now = time.time()
        for item in results:
            age_days = (now - item.last_touched) / 86400
            print(
                f"- {item.path} | {format_size(item.size_bytes)} | "
                f"last touched {format_timestamp(item.last_touched)} "
                f"({age_days:.0f} days ago)"
            )

    if report.stats.errors:
        print(
            f"Warnings: {report.stats.errors} access errors. "
            f"Showing {len(report.errors)} sample(s)."
        )
        for err in report.errors:
            print(f"  - {err.path}: {err.message}")
    if report.stats.skipped_symlinks:
        print(f"Skipped symlinks: {report.stats.skipped_symlinks}")
    if report.stats.skipped_other_fs:
        print(f"Skipped other filesystems: {report.stats.skipped_other_fs}")


def write_html_report(
    output_path: str,
    results: list[DirResult],
    report: ScanReport,
    cutoff: float,
    min_size: int,
    elapsed: float,
) -> None:
    cutoff_date = format_timestamp(cutoff)
    generated = format_timestamp(time.time())

    rows = []
    for item in results:
        age_days = (time.time() - item.last_touched) / 86400
        rows.append(
            """
            <tr>
              <td>{path}</td>
              <td>{size}</td>
              <td>{last}</td>
              <td>{age:.0f}</td>
            </tr>
            """.format(
                path=html.escape(item.path),
                size=html.escape(format_size(item.size_bytes)),
                last=html.escape(format_timestamp(item.last_touched)),
                age=age_days,
            )
        )

    html_doc = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>mjolnirscan report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    h1 {{ margin-bottom: 8px; }}
    .meta {{ color: #555; margin-bottom: 16px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ccc; padding: 8px; text-align: left; }}
    th {{ background: #f6f6f6; }}
  </style>
</head>
<body>
  <h1>mjolnirscan report</h1>
  <div class="meta">Generated {generated}</div>
  <div class="meta">Criteria: last touched before {cutoff} and size &gt;= {min_size}</div>
  <div class="meta">Scanned {dirs} directories, {files} files in {elapsed}</div>
  <h2>Flagged directories ({count})</h2>
  <table>
    <thead>
      <tr>
        <th>Path</th>
        <th>Size</th>
        <th>Last touched</th>
        <th>Age (days)</th>
      </tr>
    </thead>
    <tbody>
      {rows}
    </tbody>
  </table>
</body>
</html>
""".format(
        generated=html.escape(generated),
        cutoff=html.escape(cutoff_date),
        min_size=html.escape(format_size(min_size)),
        dirs=report.stats.dirs_scanned,
        files=report.stats.files_scanned,
        elapsed=html.escape(format_elapsed(elapsed)),
        count=len(results),
        rows="\n".join(rows) or "<tr><td colspan=\"4\">No matches</td></tr>",
    )

    with open(output_path, "w", encoding="utf-8") as handle:
        handle.write(html_doc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Find large directories that have not been touched in a long time."
    )
    parser.add_argument(
        "path",
        nargs="?",
        default=".",
        help="Root directory to scan (default: current working directory)",
    )
    parser.add_argument(
        "--older-than",
        dest="older_than",
        default="180d",
        type=parse_duration,
        help="Age threshold, e.g. 90d, 12w, 1y (default: 180d)",
    )
    parser.add_argument(
        "--min-size",
        dest="min_size",
        default="10g",
        type=parse_size,
        help="Minimum directory size, e.g. 500g, 1.5tb (default: 10g)",
    )
    parser.add_argument(
        "--time-basis",
        choices=["mtime", "atime", "ctime"],
        default="mtime",
        help="Which timestamp to use for recency (default: mtime)",
    )
    parser.add_argument(
        "--one-filesystem",
        action="store_true",
        help="Do not cross filesystem boundaries",
    )
    parser.add_argument(
        "--follow-symlinks",
        action="store_true",
        help="Follow symlinks (cycle detection enabled)",
    )
    parser.add_argument(
        "--html",
        metavar="PATH",
        help="Write an HTML report to the given path",
    )
    parser.add_argument(
        "--sort",
        choices=["size", "age", "path"],
        default="size",
        help="Sort results by size, age, or path (default: size)",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"mjolnirscan {__version__}",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    root = os.path.abspath(args.path)
    if not os.path.isdir(root):
        print(f"Error: {root} is not a directory", file=sys.stderr)
        return 2

    time_attr = {
        "mtime": "st_mtime",
        "atime": "st_atime",
        "ctime": "st_ctime",
    }[args.time_basis]

    now = time.time()
    cutoff = now - args.older_than

    start = time.monotonic()
    report = scan_directories(
        root=root,
        min_size=args.min_size,
        cutoff=cutoff,
        time_attr=time_attr,
        follow_symlinks=args.follow_symlinks,
        one_filesystem=args.one_filesystem,
    )
    elapsed = time.monotonic() - start

    results = select_top_level(report.candidates)

    if args.sort == "size":
        results.sort(key=lambda item: item.size_bytes, reverse=True)
    elif args.sort == "age":
        results.sort(key=lambda item: item.last_touched)
    else:
        results.sort(key=lambda item: item.path)

    render_text(results, report, cutoff, args.min_size, elapsed)

    if args.html:
        try:
            write_html_report(args.html, results, report, cutoff, args.min_size, elapsed)
            print(f"HTML report written to {args.html}")
        except OSError as exc:
            print(f"Failed to write HTML report: {exc}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
