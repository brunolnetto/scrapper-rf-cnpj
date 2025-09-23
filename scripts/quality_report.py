"""Generate a human-friendly quality report using radon output.

This script runs `radon raw -j src` and `radon cc -j src` (requires `radon` to be installed in the active environment),
parses the JSON output, and produces `reports/quality_report.md` with:
 - overall raw metrics summary (LOC/lloc/sloc/comments)
 - top files by LOC
 - top functions by cyclomatic complexity
 - suggestions & next steps

Usage:
    python scripts/quality_report.py --src src --out reports/quality_report.md --top-files 15 --top-funcs 30

If radon is not found, the script will print an instruction to install it.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Tuple


def run_radon_raw(src: str) -> Dict[str, Any]:
    cmd = ["radon", "raw", "-j", src]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"radon raw failed: {proc.stderr.strip()}")
    return json.loads(proc.stdout)


def run_radon_cc(src: str) -> Dict[str, Any]:
    # radon cc supports JSON output with -j
    cmd = ["radon", "cc", "-j", "-s", src]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"radon cc failed: {proc.stderr.strip()}")
    return json.loads(proc.stdout)


def summarize_raw(raw: Dict[str, Any]) -> Dict[str, int]:
    totals = {"loc": 0, "lloc": 0, "sloc": 0, "comments": 0, "blank": 0}
    for v in raw.values():
        totals["loc"] += int(v.get("loc", 0))
        totals["lloc"] += int(v.get("lloc", 0))
        totals["sloc"] += int(v.get("sloc", 0))
        totals["comments"] += int(v.get("comments", 0))
        totals["blank"] += int(v.get("blank", 0))
    return totals


def top_files_by_loc(raw: Dict[str, Any], top: int = 10) -> List[Tuple[str, int]]:
    items = [(p, int(metrics.get("loc", 0))) for p, metrics in raw.items()]
    items.sort(key=lambda x: x[1], reverse=True)
    return items[:top]


def flatten_cc(cc_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    funcs = []
    # radon cc JSON mapping: file -> list of blocks (functions, classes)
    for file_path, entries in cc_json.items():
        for ent in entries:
            # ent has: name, lineno, col, endline, complexity, rank
            ent_copy = dict(ent)
            ent_copy["file"] = file_path
            funcs.append(ent_copy)
    return funcs


def top_functions_by_complexity(cc_json: Dict[str, Any], top: int = 20) -> List[Dict[str, Any]]:
    funcs = flatten_cc(cc_json)
    funcs.sort(key=lambda e: (int(e.get("complexity", 0)), e.get("file")), reverse=True)
    return funcs[:top]


def format_markdown(
    raw: Dict[str, Any],
    cc_json: Dict[str, Any],
    out_path: Path,
    top_files: int = 15,
    top_funcs: int = 30,
    complexity_threshold: int = 12,
    loc_threshold: int = 400,
) -> None:
    totals = summarize_raw(raw)
    files = top_files_by_loc(raw, top_files)
    funcs = top_functions_by_complexity(cc_json, top_funcs)

    md_lines: List[str] = []
    md_lines.append(f"# Code Quality Report\n")
    md_lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()} UTC\n")
    md_lines.append("## Summary\n")
    md_lines.append(f"- Analyzed path: `src`\n")
    md_lines.append(f"- Files analyzed: {len(raw)}\n")
    md_lines.append(f"- Total LOC: {totals['loc']}  (SLOC: {totals['sloc']}, LLOC: {totals['lloc']})\n")
    md_lines.append(f"- Total comments: {totals['comments']}  | blanks: {totals['blank']}\n")
    md_lines.append("\n---\n")

    # Complexity summary by rank
    funcs_all = flatten_cc(cc_json)
    rank_counts: Dict[str, int] = {}
    for f in funcs_all:
        rank = f.get("rank") or "?"
        rank_counts[rank] = rank_counts.get(rank, 0) + 1

    md_lines.append("## Complexity summary\n")
    md_lines.append("| Rank | Count |")
    md_lines.append("|---|---:|")
    for rank in sorted(rank_counts.keys(), reverse=True):
        md_lines.append(f"| {rank} | {rank_counts[rank]} |")
    md_lines.append("\n---\n")

    # Files exceeding LOC threshold
    large_files = [(p, int(metrics.get("loc", 0))) for p, metrics in raw.items() if int(metrics.get("loc", 0)) >= loc_threshold]
    large_files.sort(key=lambda x: x[1], reverse=True)
    if large_files:
        md_lines.append(f"## Files with LOC >= {loc_threshold}\n")
        md_lines.append("| File | LOC | Suggestion |")
        md_lines.append("|---|---:|---|")
        for p, loc in large_files:
            md_lines.append(
                f"| `{p}` | {loc} | Consider splitting module, moving helpers to separate modules, and adding module-level tests. |"
            )
        md_lines.append("\n---\n")

    # Functions above complexity threshold
    complex_funcs = [f for f in funcs_all if int(f.get("complexity", 0)) >= complexity_threshold]
    complex_funcs.sort(key=lambda e: int(e.get("complexity", 0)), reverse=True)
    if complex_funcs:
        md_lines.append(f"## Functions with complexity >= {complexity_threshold}\n")
        md_lines.append("| # | File | Name | Line | Complexity | Rank | Suggestion |")
        md_lines.append("|---:|---|---|---:|---:|---:|")
        for i, f in enumerate(complex_funcs[: top_funcs], start=1):
            suggestion = (
                "Extract smaller functions, add focused unit tests, simplify branching (early returns), "
                "and add type hints. Consider splitting responsibilities into classes or modules."
            )
            md_lines.append(
                f"| {i} | `{f.get('file')}` | `{f.get('name')}` | {f.get('lineno')} | {f.get('complexity')} | {f.get('rank', '')} | {suggestion} |"
            )
        md_lines.append("\n---\n")

    md_lines.append("## Top files by LOC\n")
    md_lines.append("| # | File | LOC | LLOC | SLOC | Comments | Blank |")
    md_lines.append("|---:|---|---:|---:|---:|---:|---:|")
    for i, (p, loc) in enumerate(files, start=1):
        m = raw.get(p, {})
        md_lines.append(
            f"| {i} | `{p}` | {m.get('loc', 0)} | {m.get('lloc', 0)} | {m.get('sloc', 0)} | {m.get('comments', 0)} | {m.get('blank', 0)} |"
        )
    md_lines.append("\n---\n")

    md_lines.append("## Top functions by cyclomatic complexity\n")
    md_lines.append("| # | File | Name | Line | Complexity | Rank |")
    md_lines.append("|---:|---|---|---:|---:|---:|")
    for i, f in enumerate(funcs, start=1):
        md_lines.append(
            f"| {i} | `{f.get('file')}` | `{f.get('name')}` | {f.get('lineno')} | {f.get('complexity')} | {f.get('rank', '')} |"
        )

    md_lines.append("\n---\n")
    md_lines.append("## Notes & suggested next steps\n")
    md_lines.append("- Focus on files with large LOC and high-complexity functions (see tables above).")
    md_lines.append("- Consider extracting functions/classes, adding unit tests, and reducing nesting in the highest-ranked functions.")
    md_lines.append("- Re-run `radon cc -n B src` after refactors to verify improvements.\n")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(md_lines), encoding="utf-8")


def check_radon_installed() -> bool:
    return shutil.which("radon") is not None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", default="src", help="Path to source to analyze")
    parser.add_argument("--out", default="reports/quality_report.md", help="Output markdown path")
    parser.add_argument("--top-files", type=int, default=15, help="Number of top files by LOC to include")
    parser.add_argument("--top-funcs", type=int, default=30, help="Number of top complex functions to include")
    args = parser.parse_args()

    if not check_radon_installed():
        print("radon not found in PATH. Install with: pip install radon")
        raise SystemExit(2)

    src = args.src
    out_path = Path(args.out)

    print(f"Running radon raw on {src}...")
    raw = run_radon_raw(src)
    print(f"Running radon cc on {src}...")
    cc_json = run_radon_cc(src)

    print(f"Formatting markdown to {out_path}...")
    format_markdown(raw, cc_json, out_path, top_files=args.top_files, top_funcs=args.top_funcs)
    print(f"Report generated: {out_path}")


if __name__ == "__main__":
    main()
