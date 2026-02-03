# mjolnirscan

mjolnirscan is a fast, single-pass directory scanner for Unix systems. It finds large directories that have not been touched for a long time, helping identify unused data that increases HPC storage costs. Results are de-duplicated so you only see the most appropriate directory level (no recursive spam).

## Install

From the repo root:

```bash
pip install .
```

For editable installs during development:

```bash
pip install -e .
```

## Conda environment

Create and activate a dedicated conda environment (uses `environment.yml`):

```bash
conda env create -f environment.yml
conda activate mjolnirscan
```

If you already have the environment and want to update it:

```bash
conda env update -f environment.yml --prune
```

## Usage

Scan the current directory with defaults (older than 180 days and at least 10 GiB):

```bash
mjolnirscan
```

Scan a specific path with custom thresholds:

```bash
mjolnirscan /data --older-than 365d --min-size 2tb
```

Generate an HTML report:

```bash
mjolnirscan /data --older-than 90d --min-size 500g --html report.html
```

Run in parallel across top-level directories:

```bash
mjolnirscan /data --workers 4
```

Increase verbosity (use `-vv` for more detail):

```bash
mjolnirscan /data -v
```

## Notes

- Recency defaults to `mtime` (last modification time). You can also use `atime` or `ctime` via `--time-basis`.
- Many HPC filesystems disable `atime`, so `mtime` is usually more reliable.
- The scanner uses a single pass with `os.scandir` for efficiency and avoids reporting subdirectories when a parent already matches the criteria.
- Parallel mode (`--workers`) splits work across top-level directories; `--follow-symlinks` disables parallel mode to avoid double counting across symlinked trees.
- Use `--one-filesystem` to stay on a single mount and `--follow-symlinks` if you want symlink traversal (cycle detection is enabled).
