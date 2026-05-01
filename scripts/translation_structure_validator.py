"""Validate translated Markdown document structure against source HEAD."""

from collections.abc import Callable, Iterable
from dataclasses import dataclass
import re


FENCE_RE = re.compile(r"^ {0,3}(```+|~~~+)")
HEADING_RE = re.compile(r"^ {0,3}(#{1,6})[ \t]+(.+)")


@dataclass
class StructureValidationIssue:
    file_path: str
    reason: str
    source_compact: str = ""
    target_compact: str = ""
    first_difference: str = ""

    def to_dict(self):
        return {
            "file_path": self.file_path,
            "reason": self.reason,
            "source_compact": self.source_compact,
            "target_compact": self.target_compact,
            "first_difference": self.first_difference,
        }


def extract_heading_levels(content):
    """Extract Markdown heading levels while skipping fenced code blocks."""
    levels = []
    in_code_block = False
    code_block_marker = None

    for line in (content or "").splitlines():
        fence_match = FENCE_RE.match(line)
        if fence_match:
            marker = fence_match.group(1)
            if not in_code_block:
                in_code_block = True
                code_block_marker = marker
            elif (
                marker[0] == code_block_marker[0]
                and len(marker) >= len(code_block_marker)
                and not line[fence_match.end():].strip()
            ):
                in_code_block = False
                code_block_marker = None
            continue

        if in_code_block:
            continue

        heading_match = HEADING_RE.match(line)
        if heading_match:
            levels.append(len(heading_match.group(1)))

    return levels


def compact_heading_levels(levels):
    """Run-length encode heading levels for concise reports."""
    if not levels:
        return "(no headings)"

    runs = []
    index = 0
    while index < len(levels):
        level = levels[index]
        count = 1
        while index + count < len(levels) and levels[index + count] == level:
            count += 1
        runs.append(f"{'#' * level}x{count}")
        index += count
    return " ".join(runs)


def describe_first_difference(source_levels, target_levels):
    max_len = max(len(source_levels), len(target_levels))
    for index in range(max_len):
        source_level = source_levels[index] if index < len(source_levels) else None
        target_level = target_levels[index] if index < len(target_levels) else None
        if source_level == target_level:
            continue

        source_label = "#" * source_level if source_level else "(missing)"
        target_label = "#" * target_level if target_level else "(missing)"
        return f"heading {index + 1}: source {source_label}, target {target_label}"

    return ""


def compare_heading_structure(file_path, source_content, target_content):
    """Return a structure issue when source and target heading sequences differ."""
    source_levels = extract_heading_levels(source_content)
    target_levels = extract_heading_levels(target_content)

    if source_levels == target_levels:
        return None

    return StructureValidationIssue(
        file_path=file_path,
        reason="heading level sequence differs",
        source_compact=compact_heading_levels(source_levels),
        target_compact=compact_heading_levels(target_levels),
        first_difference=describe_first_difference(source_levels, target_levels),
    )


def validate_markdown_heading_structures(
    file_paths: Iterable[str],
    source_content_loader: Callable[[str], str | None],
    target_content_loader: Callable[[str], str | None],
):
    """Validate all Markdown files and return structure issues."""
    issues = []

    for file_path in sorted(file_paths):
        if not file_path.lower().endswith(".md"):
            continue

        try:
            source_content = source_content_loader(file_path)
        except Exception as exc:
            issues.append(
                StructureValidationIssue(
                    file_path=file_path,
                    reason=f"could not read source HEAD content: {exc}",
                )
            )
            continue

        try:
            target_content = target_content_loader(file_path)
        except Exception as exc:
            issues.append(
                StructureValidationIssue(
                    file_path=file_path,
                    reason=f"could not read translated target content: {exc}",
                )
            )
            continue

        if source_content is None:
            issues.append(
                StructureValidationIssue(
                    file_path=file_path,
                    reason="could not read source HEAD content",
                )
            )
            continue

        if target_content is None:
            issues.append(
                StructureValidationIssue(
                    file_path=file_path,
                    reason="could not read translated target content",
                )
            )
            continue

        issue = compare_heading_structure(file_path, source_content, target_content)
        if issue:
            issues.append(issue)

    return issues
