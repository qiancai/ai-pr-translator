#!/usr/bin/env python3
"""Check local English/Chinese Markdown document structures for Cloud docs."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sys

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from resolve_cloud_source_files import extract_markdown_doc_links
from translation_structure_validator import (
    HEADING_RE,
    StructureValidationIssue,
    extract_custom_content_tags,
    iter_markdown_content_lines,
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


def issue_category(issue: StructureValidationIssue) -> str:
    reason = issue.reason.lower()
    if "customcontent" in reason:
        return "CustomContent"
    if "heading" in reason:
        return "Heading"
    if "missing" in reason:
        return "Missing file"
    return "Structure"


_HEADER_FONT = Font(bold=True, color="FFFFFF", size=11)
_HEADER_FILL = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
_ISSUE_FILL = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
_MATCH_FILL = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
_MISSING_FILL = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")
_FILE_FILL = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")
_FILE_FONT = Font(bold=True, size=11, color="1F4E79")
_MATCH_FONT = Font(bold=True, color="006100")
_MISMATCH_FONT = Font(bold=True, color="9C0006")
_THIN_BORDER = Border(
    left=Side(style="thin"),
    right=Side(style="thin"),
    top=Side(style="thin"),
    bottom=Side(style="thin"),
)
_ISSUE_TYPE_ORDER = ["Heading", "CustomContent", "Missing file", "Structure"]


def default_excel_path() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path(__file__).resolve().parent / f"local_structure_check_{timestamp}.xlsx"


def _group_issues_by_type(issues: list[StructureValidationIssue]):
    groups = {}
    for issue in issues:
        groups.setdefault(issue_category(issue), []).append(issue)

    ordered = [
        (issue_type, groups.pop(issue_type))
        for issue_type in _ISSUE_TYPE_ORDER
        if issue_type in groups
    ]
    ordered.extend(sorted(groups.items()))
    return ordered


def _extract_heading_details(content: str | None):
    if content is None:
        return []

    headings = []
    for line_number, line in iter_markdown_content_lines(content):
        match = HEADING_RE.match(line)
        if match:
            headings.append({
                "line": line_number,
                "level": len(match.group(1)),
                "text": match.group(2).strip(),
            })
    return headings


def _format_heading_details(content: str | None) -> str:
    if content is None:
        return "(file missing or unavailable)"

    headings = _extract_heading_details(content)
    if not headings:
        return "(no headings)"
    return "\n".join(
        f"line {item['line']}: {'#' * item['level']} {item['text']}"
        for item in headings
    )


def _format_custom_content_details(content: str | None) -> str:
    if content is None:
        return "(file missing or unavailable)"
    tags = extract_custom_content_tags(content)
    if not tags:
        return "0 CustomContent tags"
    return "\n".join(f"line {tag.line_number}: {tag.text}" for tag in tags)


def _read_issue_content(root: Path | None, rel_path: str, cache: dict[tuple[Path, str], str | None]):
    if root is None:
        return None
    key = (root, rel_path)
    if key not in cache:
        cache[key] = read_text(root / rel_path)
    return cache[key]


def _style_header_row(worksheet, row_num: int, headers: list[str]) -> None:
    for col, header in enumerate(headers, 1):
        cell = worksheet.cell(row=row_num, column=col, value=header)
        cell.font = _HEADER_FONT
        cell.fill = _HEADER_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = _THIN_BORDER
    worksheet.row_dimensions[row_num].height = 32


def _write_file_summary_row(
    worksheet,
    row_num: int,
    issue: StructureValidationIssue,
    max_col: int,
) -> None:
    values = [
        issue.file_path,
        issue.reason,
        issue.first_difference,
        issue.source_compact,
        issue.target_compact,
    ]
    for col in range(1, max_col + 1):
        value = values[col - 1] if col <= len(values) else ""
        cell = worksheet.cell(row=row_num, column=col, value=value)
        cell.font = _FILE_FONT if col == 1 else Font(italic=True, size=10, color="404040")
        cell.fill = _FILE_FILL
        cell.border = _THIN_BORDER
        cell.alignment = Alignment(vertical="top", wrap_text=True)
    worksheet.row_dimensions[row_num].height = 42


def _cell_value(item, key: str):
    if not item:
        return ""
    return item[key]


def _heading_level(item) -> str:
    if not item:
        return "(missing)"
    return "#" * item["level"]


def _heading_text(item) -> str:
    if not item:
        return "(missing)"
    return item["text"]


def _tag_line(tag):
    return tag.line_number if tag else ""


def _tag_text(tag) -> str:
    return tag.text if tag else "(missing)"


def _tag_signature(tag):
    return (tag.kind, tag.text) if tag else None


def _write_heading_issue_table(worksheet, row_num: int, issue, en_content, zh_content) -> int:
    headers = [
        "#",
        "English Line",
        "English Level",
        "English Heading",
        "Chinese Line",
        "Chinese Level",
        "Chinese Heading",
        "Level Match",
    ]
    _write_file_summary_row(worksheet, row_num, issue, len(headers))
    row_num += 1
    _style_header_row(worksheet, row_num, headers)
    row_num += 1

    en_headings = _extract_heading_details(en_content)
    zh_headings = _extract_heading_details(zh_content)
    max_len = max(len(en_headings), len(zh_headings))
    for index in range(max_len):
        en_heading = en_headings[index] if index < len(en_headings) else None
        zh_heading = zh_headings[index] if index < len(zh_headings) else None
        levels_match = (
            en_heading is not None
            and zh_heading is not None
            and en_heading["level"] == zh_heading["level"]
        )
        values = [
            index + 1,
            _cell_value(en_heading, "line"),
            _heading_level(en_heading),
            _heading_text(en_heading),
            _cell_value(zh_heading, "line"),
            _heading_level(zh_heading),
            _heading_text(zh_heading),
        ]
        fill = _MATCH_FILL if levels_match else _ISSUE_FILL
        if en_heading is None or zh_heading is None:
            fill = _MISSING_FILL
        for col, value in enumerate(values, 1):
            cell = worksheet.cell(row=row_num, column=col, value=value)
            cell.fill = fill
            cell.border = _THIN_BORDER
            cell.alignment = Alignment(
                horizontal="center" if col in (1, 2, 3, 5, 6) else "left",
                vertical="top",
                wrap_text=True,
            )
        match_cell = worksheet.cell(row=row_num, column=len(headers))
        match_cell.value = "Match" if levels_match else "Mismatch"
        match_cell.font = _MATCH_FONT if levels_match else _MISMATCH_FONT
        match_cell.fill = fill
        match_cell.border = _THIN_BORDER
        match_cell.alignment = Alignment(horizontal="center", vertical="top")
        row_num += 1

    if max_len == 0:
        worksheet.cell(row=row_num, column=1, value="No headings found in either file.")
        row_num += 1

    return row_num + 1


def _write_custom_content_issue_table(worksheet, row_num: int, issue, en_content, zh_content) -> int:
    headers = [
        "#",
        "English Line",
        "English Tag",
        "Chinese Line",
        "Chinese Tag",
        "Tag Match",
    ]
    _write_file_summary_row(worksheet, row_num, issue, len(headers))
    row_num += 1
    _style_header_row(worksheet, row_num, headers)
    row_num += 1

    en_tags = extract_custom_content_tags(en_content)
    zh_tags = extract_custom_content_tags(zh_content)
    max_len = max(len(en_tags), len(zh_tags))
    for index in range(max_len):
        en_tag = en_tags[index] if index < len(en_tags) else None
        zh_tag = zh_tags[index] if index < len(zh_tags) else None
        tags_match = _tag_signature(en_tag) == _tag_signature(zh_tag)
        values = [
            index + 1,
            _tag_line(en_tag),
            _tag_text(en_tag),
            _tag_line(zh_tag),
            _tag_text(zh_tag),
        ]
        fill = _MATCH_FILL if tags_match else _ISSUE_FILL
        if en_tag is None or zh_tag is None:
            fill = _MISSING_FILL
        for col, value in enumerate(values, 1):
            cell = worksheet.cell(row=row_num, column=col, value=value)
            cell.fill = fill
            cell.border = _THIN_BORDER
            cell.alignment = Alignment(
                horizontal="center" if col in (1, 2, 4) else "left",
                vertical="top",
                wrap_text=True,
            )
        match_cell = worksheet.cell(row=row_num, column=len(headers))
        match_cell.value = "Match" if tags_match else "Mismatch"
        match_cell.font = _MATCH_FONT if tags_match else _MISMATCH_FONT
        match_cell.fill = fill
        match_cell.border = _THIN_BORDER
        match_cell.alignment = Alignment(horizontal="center", vertical="top")
        row_num += 1

    if max_len == 0:
        worksheet.cell(row=row_num, column=1, value="0 CustomContent tags in both files.")
        row_num += 1

    return row_num + 1


def _write_generic_issue_table(worksheet, row_num: int, issue, en_content, zh_content) -> int:
    headers = ["Field", "English", "Chinese"]
    _write_file_summary_row(worksheet, row_num, issue, len(headers))
    row_num += 1
    _style_header_row(worksheet, row_num, headers)
    row_num += 1

    rows = [
        ("Reason", issue.reason, issue.reason),
        ("First Difference", issue.first_difference, issue.first_difference),
        ("Structure", issue.source_compact, issue.target_compact),
        ("Headings", _format_heading_details(en_content), _format_heading_details(zh_content)),
        (
            "CustomContent",
            _format_custom_content_details(en_content),
            _format_custom_content_details(zh_content),
        ),
    ]
    for field, en_value, zh_value in rows:
        for col, value in enumerate([field, en_value, zh_value], 1):
            cell = worksheet.cell(row=row_num, column=col, value=value)
            cell.fill = _ISSUE_FILL
            cell.border = _THIN_BORDER
            cell.alignment = Alignment(vertical="top", wrap_text=True)
        max_lines = max(str(value).count("\n") + 1 for value in (field, en_value, zh_value) if value)
        worksheet.row_dimensions[row_num].height = min(220, max(36, max_lines * 15))
        row_num += 1

    return row_num + 1


def _write_issue_sheet(
    worksheet,
    issue_type: str,
    issues: list[StructureValidationIssue],
    en_root: Path | None,
    zh_root: Path | None,
    content_cache: dict[tuple[Path, str], str | None],
) -> None:
    row_num = 1
    for issue in issues:
        en_content = _read_issue_content(en_root, issue.file_path, content_cache)
        zh_content = _read_issue_content(zh_root, issue.file_path, content_cache)
        if issue_type == "Heading":
            row_num = _write_heading_issue_table(
                worksheet, row_num, issue, en_content, zh_content
            )
        elif issue_type == "CustomContent":
            row_num = _write_custom_content_issue_table(
                worksheet, row_num, issue, en_content, zh_content
            )
        else:
            row_num = _write_generic_issue_table(
                worksheet, row_num, issue, en_content, zh_content
            )

    widths_by_type = {
        "Heading": [5, 12, 14, 64, 12, 14, 64, 14],
        "CustomContent": [5, 12, 72, 12, 72, 14],
    }
    widths = widths_by_type.get(issue_type, [24, 72, 72])
    for col, width in enumerate(widths, 1):
        worksheet.column_dimensions[get_column_letter(col)].width = width


def write_excel_report(
    issues: list[StructureValidationIssue],
    output_path: Path,
    en_root: Path | None = None,
    zh_root: Path | None = None,
) -> None:
    """Write a local structure report that contains only problematic rows."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    workbook = Workbook()
    grouped_issues = _group_issues_by_type(issues)
    if not grouped_issues:
        worksheet = workbook.active
        worksheet.title = "No Issues"
        worksheet.cell(row=1, column=1, value="No structure issues found.")
        workbook.save(output_path)
        return

    content_cache = {}
    for index, (issue_type, issue_list) in enumerate(grouped_issues):
        worksheet = workbook.active if index == 0 else workbook.create_sheet()
        worksheet.title = issue_type
        _write_issue_sheet(worksheet, issue_type, issue_list, en_root, zh_root, content_cache)

    workbook.save(output_path)


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
        "--excel-out",
        help=(
            "optional path to write an Excel report; defaults to a timestamped "
            "local_structure_check_*.xlsx file next to this script"
        ),
    )
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

    excel_path = (
        Path(args.excel_out).expanduser()
        if args.excel_out
        else default_excel_path()
    )
    write_excel_report(issues, excel_path, en_root, zh_root)
    print()
    print(f"Excel report written to: {excel_path}")

    return 1 if issues else 0


if __name__ == "__main__":
    raise SystemExit(main())
