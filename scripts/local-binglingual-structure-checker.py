#!/usr/bin/env python3
"""Check local English/Chinese Markdown document structures for Cloud docs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from resolve_cloud_source_files import extract_markdown_doc_links
from translation_structure_validator import (
    StructureValidationIssue,
    validate_markdown_heading_structures,
)


DEFAULT_EN_ROOT = Path("/Users/grcai/Documents/GitHub/docs-en-release-8.5")
DEFAULT_ZH_ROOT = Path("/Users/grcai/Documents/GitHub/docs")
DEFAULT_CLOUD_TOCS = [
    "TOC-tidb-cloud.md",
    "TOC-tidb-cloud-starter.md",
    "TOC-tidb-cloud-essential.md",
    "TOC-tidb-cloud-premium.md",
    "TOC-tidb-cloud-releases.md",
]


def normalize_doc_path(value: str) -> str:
    rel = (value or "").strip().replace("\\", "/")
    rel = rel.split("#", 1)[0].split("?", 1)[0].strip()
    rel = rel.lstrip("/")
    if rel.startswith("./"):
        rel = rel[2:]
    if rel.startswith("docs/"):
        rel = rel[5:]
    return rel


def collect_cloud_markdown_files(en_root: Path, toc_files: list[str]) -> list[str]:
    files = set()

    for toc_file in toc_files:
        toc_path = en_root / toc_file
        if not toc_path.exists():
            raise FileNotFoundError(f"Cloud TOC file not found: {toc_path}")

        for link in extract_markdown_doc_links(toc_path.read_text(encoding="utf-8")):
            rel = normalize_doc_path(link)
            if rel.endswith(".md"):
                files.add(rel)

    return sorted(files)


def read_text(path: Path) -> str | None:
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def check_file(en_root: Path, zh_root: Path, rel_path: str) -> list[StructureValidationIssue]:
    en_content = read_text(en_root / rel_path)
    zh_content = read_text(zh_root / rel_path)

    if en_content is None:
        return [StructureValidationIssue(rel_path, "source file missing")]
    if zh_content is None:
        return [StructureValidationIssue(rel_path, "target file missing")]

    return validate_markdown_heading_structures(
        [rel_path],
        source_content_loader=lambda _: en_content,
        target_content_loader=lambda _: zh_content,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare local English and Chinese Markdown heading and CustomContent structures "
            "for all Markdown files linked from the five TiDB Cloud TOCs."
        )
    )
    parser.add_argument("--en-root", default=str(DEFAULT_EN_ROOT), help="local English docs root")
    parser.add_argument("--zh-root", default=str(DEFAULT_ZH_ROOT), help="local Chinese docs root")
    parser.add_argument(
        "--toc",
        action="append",
        dest="toc_files",
        help="Cloud TOC file to scan; can be provided multiple times",
    )
    parser.add_argument("--json-out", help="optional path to write machine-readable issues")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="maximum issues to print; 0 means print all",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    en_root = Path(args.en_root).expanduser()
    zh_root = Path(args.zh_root).expanduser()
    toc_files = args.toc_files or DEFAULT_CLOUD_TOCS

    try:
        file_paths = collect_cloud_markdown_files(en_root, toc_files)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    issues = []
    for rel_path in file_paths:
        issues.extend(check_file(en_root, zh_root, rel_path))

    print(f"English root: {en_root}")
    print(f"Chinese root: {zh_root}")
    print(f"Cloud TOCs: {', '.join(toc_files)}")
    print(f"Markdown files in Cloud scope: {len(file_paths)}")
    print(f"Document structure issues: {len(issues)}")

    if issues:
        print()
        print("Structure issues:")
        printable = issues if args.limit <= 0 else issues[: args.limit]
        for issue in printable:
            print(f"- {issue.file_path}: {issue.reason}")
            if issue.first_difference:
                print(f"  first difference: {issue.first_difference}")
            if issue.source_compact:
                print(f"  English: {issue.source_compact}")
            if issue.target_compact:
                print(f"  Chinese: {issue.target_compact}")

        if args.limit > 0 and len(issues) > args.limit:
            print(f"... {len(issues) - args.limit} more issue(s) omitted by --limit")

    if args.json_out:
        out_path = Path(args.json_out).expanduser()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps([issue.to_dict() for issue in issues], ensure_ascii=False, indent=2)
            + "\n",
            encoding="utf-8",
        )
        print()
        print(f"JSON report written to: {out_path}")

    return 1 if issues else 0


if __name__ == "__main__":
    raise SystemExit(main())
