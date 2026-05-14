"""Validate translated Markdown document structure against source HEAD."""

from collections.abc import Callable, Iterable
from dataclasses import dataclass
import re
from typing import Optional


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


@dataclass(frozen=True)
class CustomContentTag:
    line_number: int
    text: str
    kind: str


CUSTOM_CONTENT_TAG_RE = re.compile(r"</?CustomContent\b[^<>]*>")


def iter_markdown_content_lines(content):
    """Yield non-fenced Markdown lines as ``(line_number, line)`` tuples."""
    in_code_block = False
    code_block_marker = None

    for line_number, line in enumerate((content or "").splitlines(), 1):
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

        if not in_code_block:
            yield line_number, line


def extract_heading_levels(content):
    """Extract Markdown heading levels while skipping fenced code blocks."""
    levels = []
    for _, line in iter_markdown_content_lines(content):
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


def _custom_content_tag_kind(tag_text):
    if tag_text.startswith("</"):
        return "closing"
    if tag_text.rstrip().endswith("/>"):
        return "self-closing"
    return "opening"


def extract_custom_content_tags(content):
    """Extract CustomContent tags outside fenced code blocks in document order."""
    tags = []
    for line_number, line in iter_markdown_content_lines(content):
        for match in CUSTOM_CONTENT_TAG_RE.finditer(line):
            tag_text = match.group(0)
            tags.append(
                CustomContentTag(
                    line_number=line_number,
                    text=tag_text,
                    kind=_custom_content_tag_kind(tag_text),
                )
            )
    return tags


def custom_content_tag_signature(tags):
    return [(tag.kind, tag.text) for tag in tags]


def compact_custom_content_tags(tags, limit=6):
    """Summarize CustomContent tags for reports."""
    if not tags:
        return "0 CustomContent tags"

    shown = [tag.text for tag in tags[:limit]]
    if len(tags) > limit:
        shown.append(f"... (+{len(tags) - limit} more)")
    return f"{len(tags)} CustomContent tags: " + " | ".join(shown)


def describe_first_custom_content_difference(source_tags, target_tags):
    max_len = max(len(source_tags), len(target_tags))
    for index in range(max_len):
        source_tag = source_tags[index] if index < len(source_tags) else None
        target_tag = target_tags[index] if index < len(target_tags) else None
        if source_tag and target_tag and (source_tag.kind, source_tag.text) == (target_tag.kind, target_tag.text):
            continue

        source_label = (
            f"{source_tag.text} at line {source_tag.line_number}"
            if source_tag
            else "(missing)"
        )
        target_label = (
            f"{target_tag.text} at line {target_tag.line_number}"
            if target_tag
            else "(missing)"
        )
        return f"CustomContent tag {index + 1}: source {source_label}, target {target_label}"

    return ""


def describe_custom_content_balance_issue(tags):
    stack = []
    for index, tag in enumerate(tags, 1):
        if tag.kind == "self-closing":
            continue
        if tag.kind == "opening":
            stack.append((index, tag))
            continue
        if not stack:
            return f"CustomContent tag {index} at line {tag.line_number}: closing tag without matching opening tag"
        stack.pop()

    if stack:
        index, tag = stack[-1]
        return f"CustomContent tag {index} at line {tag.line_number}: opening tag without matching closing tag"

    return ""


def compare_custom_content_structure(file_path, source_content, target_content):
    """Return a structure issue when CustomContent tags differ or are unbalanced."""
    source_tags = extract_custom_content_tags(source_content)
    target_tags = extract_custom_content_tags(target_content)

    source_balance_issue = describe_custom_content_balance_issue(source_tags)
    if source_balance_issue:
        return StructureValidationIssue(
            file_path=file_path,
            reason="source CustomContent tags are unbalanced",
            source_compact=compact_custom_content_tags(source_tags),
            target_compact=compact_custom_content_tags(target_tags),
            first_difference=source_balance_issue,
        )

    target_balance_issue = describe_custom_content_balance_issue(target_tags)
    if target_balance_issue:
        return StructureValidationIssue(
            file_path=file_path,
            reason="target CustomContent tags are unbalanced",
            source_compact=compact_custom_content_tags(source_tags),
            target_compact=compact_custom_content_tags(target_tags),
            first_difference=target_balance_issue,
        )

    if custom_content_tag_signature(source_tags) == custom_content_tag_signature(target_tags):
        return None

    return StructureValidationIssue(
        file_path=file_path,
        reason="CustomContent tag sequence differs",
        source_compact=compact_custom_content_tags(source_tags),
        target_compact=compact_custom_content_tags(target_tags),
        first_difference=describe_first_custom_content_difference(source_tags, target_tags),
    )


def validate_markdown_heading_structures(
    file_paths: Iterable[str],
    source_content_loader: Callable[[str], Optional[str]],
    target_content_loader: Callable[[str], Optional[str]],
):
    """Validate Markdown heading levels and CustomContent tags."""
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

        for issue in (
            compare_heading_structure(file_path, source_content, target_content),
            compare_custom_content_structure(file_path, source_content, target_content),
        ):
            if issue:
                issues.append(issue)

    return issues
