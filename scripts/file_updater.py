"""
File Updater Module
Handles processing and translation of updated files and sections
"""

import os
import re
import json
import ast
import difflib
import threading
from concurrent.futures import ThreadPoolExecutor
from github import Github
from openai import OpenAI
from log_sanitizer import sanitize_exception_message
from special_file_utils import source_scope_includes_folder
from svg_preprocessor import (
    strip_svgs,
    restore_svgs,
    strip_svgs_from_sections_and_diff,
    restore_svgs_in_dict,
)

# Thread-safe printing
print_lock = threading.Lock()

def thread_safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

def verbose_logging_enabled():
    return os.getenv("VERBOSE_WORKFLOW_LOGS", "true").lower() in ("1", "true", "yes", "on")

def verbose_thread_safe_print(*args, **kwargs):
    if verbose_logging_enabled():
        thread_safe_print(*args, **kwargs)

def read_text_lines_preserve_newlines(file_path):
    """Read text lines without normalizing existing line endings."""
    with open(file_path, 'r', encoding='utf-8', newline='') as f:
        return f.readlines()

def write_text_lines_preserve_newlines(file_path, lines):
    """Write text lines while preserving line endings and the original EOF newline state."""
    lines = normalize_trailing_blank_lines(
        lines,
        preserve_final_newline=file_ends_with_newline(file_path),
    )
    with open(file_path, 'w', encoding='utf-8', newline='') as f:
        f.writelines(lines)

def file_ends_with_newline(file_path):
    """Return whether the existing file ends with a newline byte."""
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return False

    with open(file_path, 'rb') as f:
        f.seek(-1, os.SEEK_END)
        return f.read(1) in (b'\n', b'\r')

def normalize_trailing_blank_lines(lines, preserve_final_newline=True):
    """Remove blank-only lines at EOF while preserving the original final newline state."""
    normalized = list(lines)
    preferred_newline = "\n"
    for line in reversed(normalized):
        if line.endswith("\r\n"):
            preferred_newline = "\r\n"
            break
        if line.endswith("\n"):
            preferred_newline = "\n"
            break

    while normalized and not normalized[-1].strip():
        normalized.pop()

    if normalized:
        if preserve_final_newline:
            if not normalized[-1].endswith(("\n", "\r")):
                normalized[-1] += preferred_newline
        else:
            normalized[-1] = normalized[-1].rstrip("\r\n")

    return normalized

def is_markdown_heading(line):
    """Return True only for real markdown headings at column 0."""
    if not line or not isinstance(line, str):
        return False
    if line != line.lstrip():
        return False
    return re.match(r'^#{1,10}\s+\S', line) is not None


EXPLICIT_HEADING_ANCHOR_RE = re.compile(r'\s+\{#([^}]+)\}\s*$')
NON_TOP_LEVEL_HEADING_RE = re.compile(r'^(#{2,10})\s+(.+?)\s*$')
DOC_VARIABLE_EXAMPLE = "{{{ .starter }}}"
ALIASES_LINE_RE = re.compile(r'^(?P<prefix>\+?)(?P<indent>\s*)aliases:(?P<spacing>\s*)(?P<value>.+?)\s*$')
TIDB_CLOUD_LINK_RE = re.compile(r'\[([^\]]+)\]\((/tidb-cloud/[^)]+)\)')
TIDB_CLOUD_ABSOLUTE_LINK_PREFIX = os.getenv(
    "TIDB_CLOUD_ABSOLUTE_LINK_PREFIX",
    "https://docs.pingcap.com/tidbcloud/",
)
TRANSLATION_CHUNK_SECTION_THRESHOLD = int(os.getenv("TRANSLATION_CHUNK_SECTION_THRESHOLD", "10"))
TRANSLATION_CHUNK_TOKEN_THRESHOLD = int(os.getenv("TRANSLATION_CHUNK_TOKEN_THRESHOLD", "12000"))
SYSTEM_TRANSLATION_CHUNK_SIZE = int(os.getenv("SYSTEM_TRANSLATION_CHUNK_SIZE", "20"))
REGULAR_TRANSLATION_CHUNK_SIZE = int(os.getenv("REGULAR_TRANSLATION_CHUNK_SIZE", "8"))
TRANSLATION_CHUNK_MAX_SECTIONS_ENV = os.getenv("TRANSLATION_CHUNK_MAX_SECTIONS")
TRANSLATION_CHUNK_MAX_SECTIONS = int(
    TRANSLATION_CHUNK_MAX_SECTIONS_ENV or str(REGULAR_TRANSLATION_CHUNK_SIZE)
)
TRANSLATION_CHUNK_CHAR_LIMIT = int(os.getenv("TRANSLATION_CHUNK_CHAR_LIMIT", "40000"))


class TranslationResult(dict):
    """Dictionary of translated sections with non-fatal chunk failures attached."""

    def __init__(self, initial=None, failures=None):
        super().__init__(initial or {})
        self.failures = list(failures or [])


def has_explicit_heading_anchor(heading_line):
    """Return True when a markdown heading already carries an explicit anchor."""
    if not heading_line:
        return False
    return EXPLICIT_HEADING_ANCHOR_RE.search(heading_line.strip()) is not None


def build_heading_anchor_slug(heading_text):
    """Build a stable slug for an English markdown heading."""
    if not heading_text:
        return ""

    text = EXPLICIT_HEADING_ANCHOR_RE.sub("", heading_text.strip())
    text = re.sub(r'`([^`]*)`', r'\1', text)
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
    # Ignore HTML tags themselves while preserving their visible text content.
    text = re.sub(r'</?[^>]+>', ' ', text)
    # Keep dotted version numbers compact in anchors, e.g. v4.0.10 -> v4010.
    text = re.sub(r'(?<=\d)\.(?=\d)', '', text)
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s-]', ' ', text)
    text = re.sub(r'\s+', '-', text.strip())
    text = re.sub(r'-+', '-', text)
    return text.strip('-')


def add_heading_anchor_if_needed(heading_line):
    """Append an explicit anchor to a non-top-level heading when safe to do so."""
    stripped = heading_line.rstrip()
    if has_explicit_heading_anchor(stripped):
        return heading_line

    match = NON_TOP_LEVEL_HEADING_RE.match(stripped)
    if not match:
        return heading_line

    heading_text = match.group(2)
    slug = build_heading_anchor_slug(heading_text)
    if not slug:
        return heading_line

    return f"{stripped} {{#{slug}}}"


def extract_heading_anchor_slug(heading_line):
    """Return the anchor slug implied by a heading line, if any."""
    if not heading_line:
        return ""

    stripped = heading_line.rstrip()
    explicit_match = EXPLICIT_HEADING_ANCHOR_RE.search(stripped)
    if explicit_match:
        return explicit_match.group(1).strip()

    match = NON_TOP_LEVEL_HEADING_RE.match(stripped)
    if not match:
        return ""

    return build_heading_anchor_slug(match.group(2))


def get_source_mode(source_context_or_pr_url):
    """Return the high-level source mode for the current workflow input."""
    if isinstance(source_context_or_pr_url, dict):
        return source_context_or_pr_url.get("mode", "")
    return "pr"


def should_apply_tidb_cloud_link_rewrite(source_language, target_language, source_mode=""):
    """Return True when /tidb-cloud/ markdown links should become absolute URLs."""
    if (source_language or "").lower() != "english" or (target_language or "").lower() != "chinese":
        return False

    normalized_mode = (source_mode or "").lower()
    if normalized_mode == "pr":
        return True
    if normalized_mode != "commit":
        return False

    return source_scope_includes_folder(
        "ai",
        source_folder=os.getenv("SOURCE_FOLDER", ""),
        source_files=os.getenv("SOURCE_FILES", ""),
    )


def get_tidb_cloud_absolute_link_prefix():
    """Return the configured absolute link prefix for /tidb-cloud/ rewrites."""
    return os.getenv("TIDB_CLOUD_ABSOLUTE_LINK_PREFIX", TIDB_CLOUD_ABSOLUTE_LINK_PREFIX)


def normalize_aliases_value_for_zh(value):
    """Add /zh to alias paths that do not already carry the zh prefix."""
    try:
        aliases = ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return value

    if not isinstance(aliases, list):
        return value

    normalized = []
    changed = False
    for alias in aliases:
        if not isinstance(alias, str):
            return value

        updated = alias
        if alias.startswith("/") and alias != "/zh" and not alias.startswith("/zh/"):
            updated = f"/zh{alias}"
        normalized.append(updated)
        if updated != alias:
            changed = True

    if not changed:
        return value

    return "[" + ",".join(repr(alias) for alias in normalized) + "]"


def preprocess_aliases_line_for_zh(line, diff_added_only=False):
    """Normalize aliases lines for English->Chinese translation prompts."""
    if diff_added_only and not line.startswith("+"):
        return line

    match = ALIASES_LINE_RE.match(line)
    if not match:
        return line

    normalized_value = normalize_aliases_value_for_zh(match.group("value"))
    if normalized_value == match.group("value"):
        return line

    return (
        f"{match.group('prefix')}{match.group('indent')}aliases:"
        f"{match.group('spacing')}{normalized_value}"
    )


def build_tidb_cloud_absolute_url(relative_url):
    """Convert /tidb-cloud/... markdown paths to docs.pingcap.com/tidbcloud absolute URLs."""
    anchor = ""
    path = relative_url
    if "#" in relative_url:
        path, anchor = relative_url.split("#", 1)

    filename = os.path.basename(path.rstrip("/"))
    if filename.endswith(".md"):
        filename = filename[:-3]

    if not filename:
        return relative_url

    prefix = get_tidb_cloud_absolute_link_prefix()
    absolute = f"{prefix.rstrip('/')}/{filename}"
    if anchor:
        absolute = f"{absolute}#{anchor}"
    return absolute


def preprocess_tidb_cloud_links_in_line(line, diff_added_only=False):
    """Rewrite /tidb-cloud/ markdown links to absolute URLs."""
    if diff_added_only and not line.startswith("+"):
        return line

    def repl(match):
        label = match.group(1)
        relative_url = match.group(2)
        absolute_url = build_tidb_cloud_absolute_url(relative_url)
        return f"[{label}]({absolute_url})"

    return TIDB_CLOUD_LINK_RE.sub(repl, line)


def preprocess_diff_for_heading_anchor_stability(pr_diff, source_language, target_language, source_mode=""):
    """Add prompt-only stability tweaks for commit-based English -> Chinese translation."""
    if not pr_diff:
        return pr_diff

    if (source_language or "").lower() != "english" or (target_language or "").lower() != "chinese":
        return pr_diff

    enable_commit_only_preprocessing = (source_mode or "").lower() == "commit"
    enable_tidb_cloud_link_rewrite = should_apply_tidb_cloud_link_rewrite(
        source_language,
        target_language,
        source_mode=source_mode,
    )

    if not enable_commit_only_preprocessing and not enable_tidb_cloud_link_rewrite:
        return pr_diff

    lines = pr_diff.splitlines()
    processed_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if enable_commit_only_preprocessing and line.startswith('-') and not line.startswith('---'):
            removed_heading = line[1:]
            removed_slug = extract_heading_anchor_slug(removed_heading)
            buffered = [line]
            j = i + 1
            consumed_replacement = False

            while j < len(lines) and lines[j].startswith('+') and not lines[j].startswith('+++'):
                added_line = lines[j]
                added_heading = added_line[1:]
                added_slug = extract_heading_anchor_slug(added_heading)

                if removed_slug and added_slug and removed_slug != added_slug:
                    added_line = f"+{add_heading_anchor_if_needed(added_heading)}"
                    consumed_replacement = True

                buffered.append(added_line)
                j += 1

            if consumed_replacement or len(buffered) > 1:
                processed_lines.extend(buffered)
                i = j
                continue

        if enable_commit_only_preprocessing:
            line = preprocess_aliases_line_for_zh(line, diff_added_only=True)
        if enable_tidb_cloud_link_rewrite:
            line = preprocess_tidb_cloud_links_in_line(line, diff_added_only=True)
        processed_lines.append(line)
        i += 1

    return '\n'.join(processed_lines)

def resolve_section_start_line(target_lines, target_line_num, target_hierarchy):
    """Resolve a robust 0-based section start line for replace/delete."""
    if target_line_num <= 0:
        return 0

    candidate = target_line_num - 1
    heading_text = (target_hierarchy or "").strip()

    # 1) Exact candidate
    if 0 <= candidate < len(target_lines) and is_markdown_heading(target_lines[candidate]):
        return candidate

    # 2) Off-by-one previous line
    if candidate - 1 >= 0 and is_markdown_heading(target_lines[candidate - 1]):
        if not heading_text or target_lines[candidate - 1].strip() == heading_text:
            thread_safe_print(f"   🔧 Adjusted start line from {target_line_num} to {target_line_num - 1} (off-by-one heading)")
            return candidate - 1

    # 3) Search exact heading text in file
    if heading_text:
        for idx, raw_line in enumerate(target_lines):
            if raw_line.strip() == heading_text and is_markdown_heading(raw_line):
                thread_safe_print(f"   🔧 Resolved start line by heading text at line {idx + 1}")
                return idx

    # 4) Last resort: nearest heading around candidate
    for delta in range(1, 6):
        for idx in (candidate - delta, candidate + delta):
            if 0 <= idx < len(target_lines) and is_markdown_heading(target_lines[idx]):
                thread_safe_print(f"   🔧 Resolved start line by nearby heading at line {idx + 1}")
                return idx

    return max(0, min(candidate, len(target_lines) - 1))

def count_changed_lines(old_text, new_text):
    """Count changed lines between two text blocks."""
    old_lines = old_text.splitlines()
    new_lines = new_text.splitlines()
    matcher = difflib.SequenceMatcher(None, old_lines, new_lines)
    changed = 0
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag != 'equal':
            changed += max(i2 - i1, j2 - j1)
    return changed

def count_changed_diff_lines(pr_diff):
    """Count changed non-header diff lines in unified diff text."""
    if not pr_diff:
        return 0
    count = 0
    for line in pr_diff.splitlines():
        if line.startswith('+++') or line.startswith('---') or line.startswith('@@') or line.startswith('File:'):
            continue
        if line.startswith('+') or line.startswith('-'):
            count += 1
    return count

def extract_literal_replacements_from_pr_diff(pr_diff):
    """Extract deterministic literal replacements from adjacent -/+ diff lines."""
    replacements = []
    if not pr_diff:
        return replacements

    lines = pr_diff.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith('-') and not line.startswith('---'):
            old_line = line[1:]
            j = i + 1
            while j < len(lines) and lines[j].startswith('+') and not lines[j].startswith('+++'):
                new_line = lines[j][1:]
                if old_line != new_line:
                    old_links = re.findall(r'\[([^\]]+)\]\(([^)]+)\)', old_line)
                    new_links = re.findall(r'\[([^\]]+)\]\(([^)]+)\)', new_line)

                    # Prefer markdown-link text updates when URL stays the same.
                    for old_text, old_url in old_links:
                        for new_text, new_url in new_links:
                            if old_url == new_url and old_text != new_text:
                                replacements.append((f'[{old_text}]({old_url})', f'[{new_text}]({new_url})'))
                                replacements.append((old_text, new_text))

                    # Fallback full-line replacement for exact match scenarios.
                    replacements.append((old_line, new_line))
                j += 1
            i = j
            continue
        i += 1

    # Keep order but deduplicate
    deduped = []
    seen = set()
    for old, new in replacements:
        if old and new and old != new and (old, new) not in seen:
            deduped.append((old, new))
            seen.add((old, new))
    return deduped

def apply_literal_replacements(text, replacements):
    """Apply deterministic literal replacements in order."""
    updated = text
    for old, new in replacements:
        if old in updated:
            updated = updated.replace(old, new)
    return updated

def enforce_minimal_target_updates(target_sections, updated_sections, pr_diff):
    """Guardrail: prevent large style rewrites for small source diffs."""
    if not updated_sections:
        return updated_sections

    diff_changed_lines = count_changed_diff_lines(pr_diff)
    # Small source diff should produce small target edits.
    # Keep a little buffer for language expansion.
    max_allowed = max(2, diff_changed_lines * 3)
    replacements = extract_literal_replacements_from_pr_diff(pr_diff)

    guarded = {}
    for key, updated_text in updated_sections.items():
        original_text = target_sections.get(key, "")
        if not original_text:
            guarded[key] = updated_text
            continue

        changed = count_changed_lines(original_text, updated_text)
        if changed <= max_allowed:
            guarded[key] = updated_text
            continue

        # AI changed too much; apply deterministic replacements instead.
        deterministic = apply_literal_replacements(original_text, replacements)
        if deterministic != original_text:
            thread_safe_print(f"   🛡️  Minimal-change guard applied for {key}: {changed} changed lines -> deterministic patch")
            guarded[key] = deterministic
        else:
            thread_safe_print(f"   🛡️  Minimal-change guard kept original for {key}: {changed} changed lines exceeds limit {max_allowed}")
            guarded[key] = original_text

    return guarded


_tiktoken_encoding = None
_tiktoken_unavailable = False


def estimate_translation_tokens(text):
    """Estimate token count for chunk-mode routing."""
    global _tiktoken_encoding, _tiktoken_unavailable
    if not text:
        return 0
    if _tiktoken_unavailable:
        return max(1, len(str(text)) // 4)
    if _tiktoken_encoding is None:
        try:
            import tiktoken
            _tiktoken_encoding = tiktoken.get_encoding("cl100k_base")
        except Exception:
            _tiktoken_unavailable = True
            return max(1, len(str(text)) // 4)
    return len(_tiktoken_encoding.encode(str(text)))


def get_target_file_prefix_for_debug(target_file_name, target_sections):
    """Build the temp_output file prefix used by prompt/result debug files."""
    if target_file_name:
        return target_file_name.replace('/', '_').replace('.md', '')

    if target_sections:
        first_key = next(iter(target_sections.keys()), "")
        if "_" in first_key:
            parts = first_key.split("_")
            if len(parts) > 1:
                return parts[0]

    return "unknown"


def get_temp_output_dir():
    """Return the scripts/temp_output directory and create it if needed."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    temp_dir = os.path.join(script_dir, "temp_output")
    os.makedirs(temp_dir, exist_ok=True)
    return temp_dir


def extract_first_heading_title(content):
    """Return the first markdown heading title from a section."""
    for line in str(content or "").splitlines():
        if is_markdown_heading(line):
            return re.sub(r'^#{1,10}\s+', '', line.strip()).strip()
    return ""


def estimate_chunk_section_chars(key, source_sections, target_sections=None):
    """Estimate how much prompt text a section contributes before diff/glossary."""
    target_sections = target_sections or {}
    return len(str(source_sections.get(key, ""))) + len(str(target_sections.get(key, "")))


def get_translation_chunk_max_sections(target_file_name=None):
    """Return per-file chunk size, preserving larger chunks for dense reference docs."""
    if TRANSLATION_CHUNK_MAX_SECTIONS_ENV:
        return TRANSLATION_CHUNK_MAX_SECTIONS

    basename = os.path.basename(target_file_name or "")
    if basename in {
        "system-variables.md",
        "configuration-file.md",
        "tidb-configuration-file.md",
        "tikv-configuration-file.md",
        "pd-configuration-file.md",
        "tiflash-configuration.md",
    }:
        return SYSTEM_TRANSLATION_CHUNK_SIZE

    return REGULAR_TRANSLATION_CHUNK_SIZE


def build_translation_chunks(source_sections, target_sections=None, max_sections=None):
    """Split ordered section keys by section count and approximate character budget."""
    chunks = []
    current_keys = []
    current_chars = 0
    max_sections = max_sections or TRANSLATION_CHUNK_MAX_SECTIONS

    for key in source_sections:
        section_chars = estimate_chunk_section_chars(key, source_sections, target_sections)

        if current_keys and (
            len(current_keys) >= max_sections
            or current_chars + section_chars > TRANSLATION_CHUNK_CHAR_LIMIT
        ):
            chunks.append(
                {
                    "type": "balanced",
                    "keys": current_keys,
                    "limit": max_sections,
                    "chars": current_chars,
                }
            )
            current_keys = []
            current_chars = 0

        current_keys.append(key)
        current_chars += section_chars

    if current_keys:
        chunks.append(
            {
                "type": "balanced",
                "keys": current_keys,
                "limit": max_sections,
                "chars": current_chars,
            }
        )

    return chunks


def summarize_chunk_sections(chunk_keys, source_sections, max_items=6):
    """Build a compact section list for failure reports."""
    names = []
    for key in chunk_keys:
        title = extract_first_heading_title(source_sections.get(key, ""))
        cleaned = title.replace("`", "").strip() if title else key
        names.append(cleaned or key)

    if len(names) <= max_items:
        return ", ".join(names)

    shown = ", ".join(names[:max_items])
    return f"{shown}, ... (+{len(names) - max_items} more)"


def should_use_translation_chunk_mode(source_sections):
    """Decide whether a section update should be translated in smaller chunks."""
    total_tokens = sum(
        estimate_translation_tokens(content)
        for content in source_sections.values()
        if content
    )
    section_count = len(source_sections)
    use_chunk_mode = (
        section_count > TRANSLATION_CHUNK_SECTION_THRESHOLD
        or total_tokens > TRANSLATION_CHUNK_TOKEN_THRESHOLD
    )

    return use_chunk_mode, total_tokens


def _extract_line_number_from_key(key):
    """Extract the source line number from a section key like 'modified_390'."""
    parts = key.rsplit("_", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return int(parts[1])
    return None


def _parse_diff_hunks(pr_diff):
    """Split a unified diff into its header and individual hunks."""
    if not pr_diff:
        return "", []
    lines = pr_diff.splitlines(True)
    header_lines = []
    hunks = []
    current_hunk = []
    for line in lines:
        if line.startswith("@@"):
            if current_hunk:
                hunks.append(current_hunk)
            current_hunk = [line]
        elif current_hunk:
            current_hunk.append(line)
        else:
            header_lines.append(line)
    if current_hunk:
        hunks.append(current_hunk)
    return "".join(header_lines), hunks


_HUNK_HEADER_RE = re.compile(r'@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@')


def _hunk_new_line_range(hunk_header):
    """Return (start, end) line range for the new-file side of a hunk."""
    m = _HUNK_HEADER_RE.match(hunk_header)
    if not m:
        return None, None
    start = int(m.group(3))
    count = int(m.group(4)) if m.group(4) is not None else 1
    return start, start + count - 1


def filter_diff_for_chunk_sections(pr_diff, chunk_keys, all_section_keys):
    """Return only the diff hunks that overlap with the given chunk's sections."""
    if not pr_diff or not chunk_keys:
        return pr_diff or ""

    chunk_line_numbers = sorted(
        n for n in (_extract_line_number_from_key(k) for k in chunk_keys) if n is not None
    )
    if not chunk_line_numbers:
        return pr_diff

    all_line_numbers = sorted(
        n for n in (_extract_line_number_from_key(k) for k in all_section_keys) if n is not None
    )

    chunk_ranges = []
    for ln in chunk_line_numbers:
        idx = all_line_numbers.index(ln)
        end = all_line_numbers[idx + 1] - 1 if idx + 1 < len(all_line_numbers) else 999999
        chunk_ranges.append((ln, end))

    header, hunks = _parse_diff_hunks(pr_diff)
    relevant = []
    for hunk in hunks:
        h_start, h_end = _hunk_new_line_range(hunk[0])
        if h_start is None:
            relevant.append(hunk)
            continue
        for r_start, r_end in chunk_ranges:
            if h_start <= r_end and h_end >= r_start:
                relevant.append(hunk)
                break

    if not relevant:
        return header.rstrip("\n")
    return header + "".join("".join(h) for h in relevant)


def _prepare_translation_prompt(
    pr_diff, source_sections, target_sections,
    source_language, target_language, source_mode,
    glossary_matcher=None, chunk_label=None, post_change_context_keys=None,  # post_change_context_keys kept for backward compat, unused
):
    """Build the AI translation prompt string.

    Returns (prompt, prompt_pr_diff) so callers can reuse the preprocessed diff
    for enforce_minimal_target_updates.
    """
    formatted_source_sections = json.dumps(source_sections, ensure_ascii=False, indent=2)
    formatted_target_sections = json.dumps(target_sections, ensure_ascii=False, indent=2)
    source_sections_heading = (
        f"1. Source sections in {source_language} (post-change, i.e. after applying the diff):"
    )

    thread_safe_print(f"   📊 Source sections: {len(source_sections)} sections")
    thread_safe_print(f"   📊 Target sections: {len(target_sections)} sections")

    total_source_chars = sum(len(str(c)) for c in source_sections.values())
    total_target_chars = sum(len(str(c)) for c in target_sections.values())
    thread_safe_print(f"   📏 Content size: Source={total_source_chars:,} chars, Target={total_target_chars:,} chars")

    thread_safe_print(f"   🤖 Getting AI translation for {len(source_sections)} sections...")

    prompt_pr_diff = preprocess_diff_for_heading_anchor_stability(
        pr_diff, source_language, target_language, source_mode=source_mode
    )

    glossary_prompt_section = ""
    glossary_instruction = ""
    if glossary_matcher:
        from glossary import filter_terms_for_content, format_terms_for_prompt
        matched_terms = filter_terms_for_content(
            glossary_matcher,
            prompt_pr_diff or '',
            source_language=source_language,
        )
        if matched_terms:
            glossary_text = format_terms_for_prompt(matched_terms)
            glossary_prompt_section = f"\n4. {glossary_text}\n"
            glossary_instruction = "\n6. When translating terms listed in the glossary above, use the provided translations for consistency."
            thread_safe_print(f"   📚 Matched {len(matched_terms)} glossary terms for prompt")

    prompt = f"""You are a professional technical writer in the Database domain. I will provide you with:

1. Latest source sections in {source_language}:
{formatted_source_sections}

2. GitHub PR changes (Diff):
{prompt_pr_diff}

3. Current target sections in {target_language}:
{formatted_target_sections}

4. Glossary for terms in {source_language} and {target_language}:
{glossary_prompt_section}

Task: Update the target sections in {target_language} according to the diff in {source_language}.

Instructions:
1. The source sections above show the final state after the diff was applied. Carefully analyze the PR diff to identify exactly which source lines and words changed in {source_language}. Make sure the returned translation covers ALL content in each source section — do not omit any paragraph or sentence.
2. According to the diff, identify the lines that should be updated accordingly in {target_language}. For lines that needs to be updated in {target_language}, apply the corresponding minimal edits according to the diff. For lines not changed or not included in the diff, make sure to keep the corresponding target lines byte-for-byte identical (same wording, punctuation, spacing, indentation, list markers, and line breaks), which means Do Not add, remove, or modify lines not included in the diff. Never rewrite style, improve wording, or rephrase unaffected content.
3. Translation rules:
   - Preserve doc variables/placeholders exactly as they appear, including triple braces, such as {DOC_VARIABLE_EXAMPLE}. This also applies when they appear inside HTML attributes or tab labels.
   - Keep UI button/label names wrapped in ** such as **My TiDB** in English.
   - Preserve explicit heading anchors such as {{#example-test}} exactly as they appear.
4. Keep the JSON structure unchanged, only modify section content where required by the diff.
5. Ensure updated target content is logically consistent with the source diff. If uncertain, prefer leaving a line unchanged rather than rewriting.{glossary_instruction}

Please return the complete updated JSON in the same format as target sections, without any additional explanatory text."""

    return prompt, prompt_pr_diff


def _execute_ai_translation(
    prompt, ai_client, target_sections, pr_diff,
    target_file_prefix, prompt_suffix,
    source_language, target_language,
):
    """Send prompt to AI, parse, enforce minimal updates, save, and return result dict."""
    formatted_source_preview = ""
    formatted_target_preview = ""
    try:
        src_json = json.loads(prompt.split("1. Source sections in")[1].split("\n2. GitHub PR changes")[0].strip().rsplit("\n", 1)[0])
        formatted_source_preview = json.dumps(src_json, ensure_ascii=False, indent=2)[:500]
    except Exception:
        formatted_source_preview = "(preview unavailable)"
    try:
        tgt_json = json.loads(prompt.split(f"3. Current target sections in")[1].split("\n4. Glossary")[0].strip().rsplit("\n", 1)[0])
        formatted_target_preview = json.dumps(tgt_json, ensure_ascii=False, indent=2)[:500]
    except Exception:
        formatted_target_preview = "(preview unavailable)"

    target_section_count = len(target_sections) if hasattr(target_sections, "__len__") else 0
    thread_safe_print(
        f"\n   📤 AI update request ({source_language} → {target_language}): "
        f"{target_section_count} target section(s), {len(prompt):,} prompt chars"
    )
    verbose_thread_safe_print(f"   " + "="*80)
    verbose_thread_safe_print(f"   Source Sections: {formatted_source_preview}...")
    verbose_thread_safe_print(f"   PR Diff (first 500 chars): {pr_diff[:500] if pr_diff else '(none)'}...")
    verbose_thread_safe_print(f"   Target Sections: {formatted_target_preview}...")
    verbose_thread_safe_print(f"   " + "="*80)

    try:
        from main import print_token_estimation
        print_token_estimation(prompt, f"Document translation ({source_language} → {target_language})")
    except ImportError:
        try:
            import tiktoken
            enc = tiktoken.get_encoding("cl100k_base")
            actual_tokens = len(enc.encode(prompt))
            char_count = len(prompt)
            thread_safe_print(f"   💰 Document translation ({source_language} → {target_language})")
            thread_safe_print(f"      📝 Input: {char_count:,} characters")
            thread_safe_print(f"      🔢 Actual tokens: {actual_tokens:,} (using tiktoken cl100k_base)")
        except Exception:
            estimated_tokens = len(prompt) // 4
            char_count = len(prompt)
            thread_safe_print(f"   💰 Document translation ({source_language} → {target_language})")
            thread_safe_print(f"      📝 Input: {char_count:,} characters")
            thread_safe_print(f"      🔢 Estimated tokens: ~{estimated_tokens:,} (fallback: 4 chars/token approximation)")

    temp_dir = get_temp_output_dir()
    try:
        ai_response = ai_client.chat_completion(
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
        )
        thread_safe_print(f"   📝 AI translation response received ({len(ai_response or ''):,} chars)")
        verbose_thread_safe_print(f"   📋 AI response (first 500 chars): {(ai_response or '')[:500]}...")

        result = parse_updated_sections(ai_response)
        result = enforce_minimal_target_updates(target_sections, result, pr_diff)
        thread_safe_print(f"   📊 Parsed {len(result)} sections from AI response")

        ai_results_file = os.path.join(
            temp_dir,
            f"{target_file_prefix}_updated_sections_from_ai{prompt_suffix}.json",
        )
        with open(ai_results_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        thread_safe_print(f"   💾 AI results saved to {ai_results_file}")
        return result

    except Exception as e:
        thread_safe_print(f"   ❌ AI translation failed: {sanitize_exception_message(e)}")
        return {}


def get_updated_sections_from_ai_chunked(
    pr_diff,
    target_sections,
    source_sections,
    ai_client,
    source_language,
    target_language,
    target_file_name=None,
    glossary_matcher=None,
    dry_run=False,
    source_mode="",
    chunk_routing_sections=None,
    post_change_context_keys=None,
):
    """Translate large section sets chunk by chunk and merge successful chunks.

    Phase 1 builds and saves all prompts (with per-chunk filtered diffs).
    Phase 2 sends each prompt to AI and collects results.
    """
    target_file_prefix = get_target_file_prefix_for_debug(target_file_name, target_sections)
    routing_sections = chunk_routing_sections or source_sections
    chunk_max_sections = get_translation_chunk_max_sections(target_file_name)
    chunks = build_translation_chunks(
        routing_sections,
        target_sections,
        max_sections=chunk_max_sections,
    )
    total_chunks = len(chunks)
    all_section_keys = list(source_sections.keys())
    post_change_context_key_set = set(post_change_context_keys or [])

    thread_safe_print(
        f"   🧩 Chunk mode enabled: {len(source_sections)} sections split into {total_chunks} chunks"
    )

    # ------------------------------------------------------------------
    # Phase 1: Build and save all prompts
    # ------------------------------------------------------------------
    prepared = []
    temp_dir = get_temp_output_dir()
    for chunk_index, chunk in enumerate(chunks, 1):
        chunk_keys = chunk["keys"]
        chunk_source = {key: source_sections[key] for key in chunk_keys}
        chunk_target = {
            key: target_sections[key]
            for key in chunk_keys
            if key in target_sections
        }
        chunk_diff = filter_diff_for_chunk_sections(pr_diff, chunk_keys, all_section_keys)
        section_summary = summarize_chunk_sections(chunk_keys, routing_sections)
        chunk_label = f"part-{chunk_index:03d}"

        thread_safe_print(
            f"   🧩 Building prompt for chunk {chunk_index}/{total_chunks}: "
            f"{len(chunk_keys)} {chunk['type']} section(s) ({section_summary})"
        )

        prompt, prompt_pr_diff = _prepare_translation_prompt(
            chunk_diff, chunk_source, chunk_target,
            source_language, target_language, source_mode,
            glossary_matcher=glossary_matcher,
            chunk_label=chunk_label,
            post_change_context_keys=[
                key for key in chunk_keys if key in post_change_context_key_set
            ],
        )

        prompt_file = os.path.join(
            temp_dir,
            f"{target_file_prefix}_prompt-for-ai-translation.{chunk_label}.txt",
        )
        with open(prompt_file, 'w', encoding='utf-8') as f:
            f.write(prompt)
        thread_safe_print(f"   💾 Prompt saved to {prompt_file} ({len(prompt):,} chars)")

        prepared.append({
            "index": chunk_index,
            "keys": chunk_keys,
            "type": chunk["type"],
            "prompt": prompt,
            "chunk_diff": chunk_diff,
            "chunk_target": chunk_target,
            "section_summary": section_summary,
            "chunk_label": chunk_label,
        })

    thread_safe_print(
        f"\n   📝 All {total_chunks} chunk prompts generated. "
        + ("Dry-run: skipping AI calls." if dry_run else "Starting AI translation...")
    )

    if dry_run:
        return TranslationResult()

    # ------------------------------------------------------------------
    # Phase 2: Send each prompt to AI and collect results
    # ------------------------------------------------------------------
    merged_result = TranslationResult()
    failures = []

    for cp in prepared:
        chunk_index = cp["index"]
        thread_safe_print(
            f"\n   🤖 Sending chunk {chunk_index}/{total_chunks} to AI "
            f"({cp['section_summary']})"
        )

        chunk_result = _execute_ai_translation(
            cp["prompt"],
            ai_client,
            cp["chunk_target"],
            cp["chunk_diff"],
            target_file_prefix,
            f".{cp['chunk_label']}",
            source_language,
            target_language,
        )

        if not chunk_result:
            failures.append(
                f"failed to translate chunk {chunk_index}/{total_chunks} "
                f"(sections: {cp['section_summary']})"
            )
            continue

        merged_result.update(chunk_result)
        missing_keys = [key for key in cp["keys"] if key not in chunk_result]
        if missing_keys:
            missing_summary = summarize_chunk_sections(missing_keys, routing_sections)
            failures.append(
                f"failed to translate chunk {chunk_index}/{total_chunks} "
                f"(missing sections: {missing_summary})"
            )

    merged_result.failures = failures

    ai_results_file = os.path.join(temp_dir, f"{target_file_prefix}_updated_sections_from_ai.json")
    with open(ai_results_file, 'w', encoding='utf-8') as f:
        json.dump(merged_result, f, ensure_ascii=False, indent=2)
    thread_safe_print(f"   💾 Merged AI chunk results saved to {ai_results_file}")

    if failures:
        thread_safe_print(f"   ⚠️  Chunk translation completed with {len(failures)} failure(s)")
        for failure in failures:
            thread_safe_print(f"      ❌ {failure}")

    return merged_result


def get_updated_sections_from_ai(pr_diff, target_sections, source_old_content_dict, ai_client, source_language, target_language, target_file_name=None, glossary_matcher=None, dry_run=False, source_mode="", _disable_chunking=False, _chunk_label=None, _chunk_routing_content_dict=None, _post_change_context_keys=None):
    """Use AI to update target sections based on source content (post-change), PR diff, and target sections."""
    if not source_old_content_dict or not target_sections:
        return {}
    
    # Filter out deleted sections and prepare source sections from old content
    source_sections = {}
    for key, old_content in source_old_content_dict.items():
        # Skip deleted sections
        if 'deleted' in key:
            continue
        
        # Handle null values by using empty string
        content = old_content if old_content is not None else ""
        source_sections[key] = content

    # Strip SVG tags from all content before sending to AI to save tokens
    # and avoid truncation.  The svg_map is used to restore originals after
    # the AI responds.
    source_sections, target_sections, pr_diff, svg_map = \
        strip_svgs_from_sections_and_diff(source_sections, target_sections, pr_diff)
    if svg_map:
        thread_safe_print(f"   🖼️  Replaced {len(svg_map)} SVG(s) with placeholders for AI translation")

    if _chunk_routing_content_dict:
        routing_sections = {
            key: _chunk_routing_content_dict.get(key, source_sections[key])
            for key in source_sections
        }
    else:
        routing_sections = source_sections

    def _restore_result(result):
        """Restore SVG placeholders in the AI result dict."""
        if not svg_map or not result:
            return result
        restored = restore_svgs_in_dict(result, svg_map)
        if hasattr(result, "failures"):
            restored_obj = TranslationResult(restored)
            restored_obj.failures = result.failures
            return restored_obj
        return restored

    if not _disable_chunking:
        use_chunk_mode, total_source_tokens = should_use_translation_chunk_mode(routing_sections)
        thread_safe_print(
            f"   🔢 Source new_content tokens for translation routing: ~{total_source_tokens:,}"
        )
        if use_chunk_mode:
            result = get_updated_sections_from_ai_chunked(
                pr_diff,
                target_sections,
                source_sections,
                ai_client,
                source_language,
                target_language,
                target_file_name,
                glossary_matcher=glossary_matcher,
                dry_run=dry_run,
                source_mode=source_mode,
                chunk_routing_sections=routing_sections,
                post_change_context_keys=_post_change_context_keys,
            )
            return _restore_result(result)

    prompt, prompt_pr_diff = _prepare_translation_prompt(
        pr_diff, source_sections, target_sections,
        source_language, target_language, source_mode,
        glossary_matcher=glossary_matcher,
        chunk_label=_chunk_label,
        post_change_context_keys=_post_change_context_keys,
    )

    # Save prompt to file for reference
    target_file_prefix = get_target_file_prefix_for_debug(target_file_name, target_sections)
    temp_dir = get_temp_output_dir()
    
    prompt_suffix = f".{_chunk_label}" if _chunk_label else ""
    prompt_file = os.path.join(temp_dir, f"{target_file_prefix}_prompt-for-ai-translation{prompt_suffix}.txt")
    with open(prompt_file, 'w', encoding='utf-8') as f:
        f.write(prompt)
    
    thread_safe_print(f"\n💾 Prompt saved to {prompt_file}")
    thread_safe_print(f"📝 Prompt length: {len(prompt)} characters")
    thread_safe_print(f"📊 Source sections: {len(source_sections)}")
    thread_safe_print(f"📊 Target sections: {len(target_sections)}")

    if dry_run:
        thread_safe_print(f"⏸️  Dry-run mode: prompt saved, skipping AI call")
        return {}

    thread_safe_print(f"🤖 Sending prompt to AI...")

    result = _execute_ai_translation(
        prompt, ai_client, target_sections, pr_diff,
        target_file_prefix, prompt_suffix,
        source_language, target_language,
    )
    return _restore_result(result)

def parse_updated_sections(ai_response):
    """Parse AI response and extract JSON (from get-updated-target-sections.py)"""
    # Ensure temp_output directory exists for debug files
    script_dir = os.path.dirname(os.path.abspath(__file__))
    temp_dir = os.path.join(script_dir, "temp_output")
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        print(f"\n   🔧 Parsing AI response...")
        print(f"   Raw response length: {len(ai_response)} characters")
        
        # Try to extract JSON from AI response
        cleaned_response = ai_response.strip()
        
        # Remove markdown code blocks if present
        if cleaned_response.startswith('```json'):
            cleaned_response = cleaned_response[7:]
            print(f"   📝 Removed '```json' prefix")
        elif cleaned_response.startswith('```'):
            cleaned_response = cleaned_response[3:]
            print(f"   📝 Removed '```' prefix")
        
        if cleaned_response.endswith('```'):
            cleaned_response = cleaned_response[:-3]
            print(f"   📝 Removed '```' suffix")
        
        cleaned_response = cleaned_response.strip()
        
        print(f"   📝 Cleaned response length: {len(cleaned_response)} characters")
        verbose_thread_safe_print(f"   📝 First 200 chars: {cleaned_response[:200]}...")
        verbose_thread_safe_print(f"   📝 Last 200 chars: ...{cleaned_response[-200:]}")
        
        # Try to find JSON content between curly braces
        start_idx = cleaned_response.find('{')
        end_idx = cleaned_response.rfind('}')
        
        if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
            json_content = cleaned_response[start_idx:end_idx+1]
            print(f"   📝 Extracted JSON content length: {len(json_content)} characters")
            
            try:
                # Parse JSON
                updated_sections = json.loads(json_content)
                print(f"   ✅ Successfully parsed JSON with {len(updated_sections)} sections")
                return updated_sections
            except json.JSONDecodeError as e:
                print(f"   ⚠️  JSON seems incomplete, trying to fix...")
                
                # Try to fix incomplete JSON by finding the last complete entry
                lines = json_content.split('\n')
                fixed_lines = []
                in_value = False
                quote_count = 0
                
                for line in lines:
                    if '"' in line:
                        quote_count += line.count('"')
                    
                    fixed_lines.append(line)
                    
                    # If we have an even number of quotes, we might have a complete entry
                    if quote_count % 2 == 0 and (line.strip().endswith(',') or line.strip().endswith('"')):
                        # Try to parse up to this point
                        potential_json = '\n'.join(fixed_lines)
                        if not potential_json.rstrip().endswith('}'):
                            # Remove trailing comma and add closing brace
                            if potential_json.rstrip().endswith(','):
                                potential_json = potential_json.rstrip()[:-1] + '\n}'
                            else:
                                potential_json += '\n}'
                        
                        try:
                            partial_sections = json.loads(potential_json)
                            print(f"   🔧 Fixed JSON with {len(partial_sections)} sections")
                            return partial_sections
                        except:
                            continue
                
                # If all else fails, return the original error
                raise e
        else:
            print(f"   ❌ Could not find valid JSON structure in response")
            return None
        
    except json.JSONDecodeError as e:
        print(f"   ❌ Error parsing AI response as JSON: {sanitize_exception_message(e)}")
        print(f"   📝 Error at position: {e.pos if hasattr(e, 'pos') else 'unknown'}")
        
        # Save debug info
        debug_file = os.path.join(temp_dir, f"ai_response_debug_{os.getpid()}.txt")
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write("Original AI Response:\n")
            f.write("="*80 + "\n")
            f.write(ai_response)
            f.write("\n" + "="*80 + "\n")
            f.write("Cleaned Response:\n")
            f.write("-"*80 + "\n")
            f.write(cleaned_response if 'cleaned_response' in locals() else "Not available")
        
        print(f"   📁 Debug info saved to: {debug_file}")
        return None
    except Exception as e:
        print(f"   ❌ Unexpected error parsing AI response: {sanitize_exception_message(e)}")
        return None


def replace_frontmatter_content(lines, new_content):
    """Replace content from beginning of file to first top-level header"""
    # Find the first top-level header
    first_header_idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith('# '):
            first_header_idx = i
            break
    
    if first_header_idx is None:
        # No top-level header found, replace entire content
        return new_content.split('\n')
    
    # Replace content from start to before first header
    new_lines = new_content.split('\n')
    return new_lines + lines[first_header_idx:]


def replace_toplevel_section_content(lines, target_line_num, new_content):
    """Replace content from top-level header to first next-level header"""
    start_idx = target_line_num - 1  # Convert to 0-based index
    
    # Find the end of top-level section (before first ## header)
    end_idx = len(lines)
    for i in range(start_idx + 1, len(lines)):
        line = lines[i].strip()
        if line.startswith('##'):  # Found first next-level header
            end_idx = i
            break
    
    # Replace the top-level section content (from start_idx to end_idx)
    new_lines = new_content.split('\n')
    return lines[:start_idx] + new_lines + lines[end_idx:]


def update_local_document(file_path, updated_sections, hierarchy_dict, target_local_path):
    """Update local document using hierarchy-based section identification (from update-target-doc-v2.py)"""
    local_path = os.path.join(target_local_path, file_path)
    
    if not os.path.exists(local_path):
        print(f"   ❌ Local file not found: {local_path}")
        return False
    
    try:
        # Read document content
        with open(local_path, 'r', encoding='utf-8') as f:
            document_content = f.read()
        
        lines = document_content.split('\n')
        
        replacements_made = []
        
        # Use a unified approach: build a complete replacement plan first, then execute it
        # This avoids line number shifts during the replacement process
        
        # Find section boundaries for ALL sections
        section_boundaries = find_section_boundaries(lines, hierarchy_dict)
        
        # Create a comprehensive replacement plan
        replacement_plan = []
        
        for line_num, new_content in updated_sections.items():
            if line_num == "0":
                # Special handling for frontmatter
                first_header_idx = None
                for i, line in enumerate(lines):
                    if line.strip().startswith('# '):
                        first_header_idx = i
                        break
                
                replacement_plan.append({
                    'type': 'frontmatter',
                    'start': 0,
                    'end': first_header_idx if first_header_idx else len(lines),
                    'new_content': new_content,
                    'line_num': line_num
                })
                
            elif line_num in hierarchy_dict:
                hierarchy = hierarchy_dict[line_num]
                if ' > ' not in hierarchy:  # Top-level section
                    # Special handling for top-level sections
                    start_idx = int(line_num) - 1
                    end_idx = len(lines)
                    for i in range(start_idx + 1, len(lines)):
                        line = lines[i].strip()
                        if line.startswith('##'):
                            end_idx = i
                            break
                    
                    replacement_plan.append({
                        'type': 'toplevel',
                        'start': start_idx,
                        'end': end_idx,
                        'new_content': new_content,
                        'line_num': line_num
                    })
                else:
                    # Regular section
                    if line_num in section_boundaries:
                        boundary = section_boundaries[line_num]
                        replacement_plan.append({
                            'type': 'regular',
                            'start': boundary['start'],
                            'end': boundary['end'],
                            'new_content': new_content,
                            'line_num': line_num,
                            'hierarchy': boundary['hierarchy']
                        })
                    else:
                        print(f"      ⚠️  Section at line {line_num} not found in hierarchy")
        
        # Sort replacement plan: process from bottom to top of the document to avoid line shifts
        # Sort by start line in reverse order (highest line number first)
        replacement_plan.sort(key=lambda x: -x['start'])
        
        # Execute replacements in the planned order (from bottom to top)
        print(f"      📋 Executing {len(replacement_plan)} replacements from bottom to top:")
        for i, replacement in enumerate(replacement_plan):
            print(f"      {i+1}. {replacement['type']} (line {replacement.get('line_num', '0')}, start: {replacement['start']})")
        
        for replacement in replacement_plan:
            start = replacement['start']
            end = replacement['end']
            new_content = replacement['new_content']
            new_lines = new_content.split('\n')
            
            # Replace the content
            lines = lines[:start] + new_lines + lines[end:]
            
            # Record the replacement
            original_line_count = end - start
            line_diff = len(new_lines) - original_line_count
            
            replacements_made.append({
                'type': replacement['type'],
                'line_num': replacement.get('line_num', 'N/A'),
                'hierarchy': replacement.get('hierarchy', 'N/A'),
                'start': start,
                'end': end,
                'original_lines': original_line_count,
                'new_lines': len(new_lines),
                'line_diff': line_diff
            })
            
            print(f"      ✅ Updated {replacement['type']} section: {replacement.get('line_num', 'frontmatter')}")
        
        # Save updated document
        with open(local_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        
        print(f"   ✅ Updated {len(replacements_made)} sections")
        for replacement in replacements_made:
            print(f"      📝 Line {replacement['line_num']}: {replacement['hierarchy']}")
        
        return True
        
    except Exception as e:
        thread_safe_print(f"   ❌ Error updating file: {sanitize_exception_message(e)}")
        return False

def find_section_boundaries(lines, hierarchy_dict):
    """Find the start and end line for each section based on hierarchy (from update-target-doc-v2.py)"""
    section_boundaries = {}
    
    # Sort sections by line number
    sorted_sections = sorted(hierarchy_dict.items(), key=lambda x: int(x[0]))
    
    for i, (line_num, hierarchy) in enumerate(sorted_sections):
        start_line = int(line_num) - 1  # Convert to 0-based index
        
        # Find end line (start of next section at same or higher level)
        end_line = len(lines)  # Default to end of document
        
        if start_line >= len(lines):
            continue
            
        # Get current section level
        raw_current_line = lines[start_line]
        current_line = raw_current_line.strip()
        if not is_markdown_heading(raw_current_line):
            continue
            
        current_level = len(current_line.split()[0])  # Count # characters
        
        # Look for next section at same or higher level.
        # Ignore markdown-like headers inside fenced code blocks.
        in_code_block = False
        code_block_delimiter = None
        for j in range(start_line + 1, len(lines)):
            raw_line = lines[j]
            line = raw_line.strip()

            if line.startswith('```') or line.startswith('~~~'):
                if not in_code_block:
                    in_code_block = True
                    code_block_delimiter = line[:3]
                elif line.startswith(code_block_delimiter):
                    in_code_block = False
                    code_block_delimiter = None
                continue

            if not in_code_block and is_markdown_heading(raw_line):
                line_level = len(line.split()[0]) if line.split() else 0
                if line_level <= current_level:
                    end_line = j
                    break
        
        section_boundaries[line_num] = {
            'start': start_line,
            'end': end_line,
            'hierarchy': hierarchy,
            'level': current_level
        }
    
    return section_boundaries

def insert_sections_into_document(file_path, translated_sections, target_insertion_points, target_local_path):
    """Insert translated sections into the target document at specified points"""
    
    if not translated_sections or not target_insertion_points:
        thread_safe_print(f"   ⚠️  No sections or insertion points provided")
        return False
    
    local_path = os.path.join(target_local_path, file_path)
    
    if not os.path.exists(local_path):
        thread_safe_print(f"   ❌ Local file not found: {local_path}")
        return False
    
    try:
        # Read document content
        with open(local_path, 'r', encoding='utf-8') as f:
            document_content = f.read()
        
        lines = document_content.split('\n')
        thread_safe_print(f"   📄 Document has {len(lines)} lines")
        
        # Sort insertion points by line number in descending order to avoid position shifts
        sorted_insertions = sorted(
            target_insertion_points.items(), 
            key=lambda x: x[1]['insertion_after_line'], 
            reverse=True
        )
        
        insertions_made = []
        
        for group_id, point_data in sorted_insertions:
            insertion_after_line = point_data['insertion_after_line']
            new_sections = point_data['new_sections']
            insertion_type = point_data['insertion_type']
            
            thread_safe_print(f"     📌 Inserting {len(new_sections)} sections after line {insertion_after_line}")
            
            # Convert 1-based line number to 0-based index for insertion point
            # insertion_after_line is 1-based, so insertion_index should be insertion_after_line - 1
            insertion_index = insertion_after_line - 1
            
            # Prepare new content to insert
            new_content_lines = []
            
            # Add an empty line before the new sections if not already present
            if insertion_index < len(lines) and lines[insertion_index].strip():
                new_content_lines.append("")
            
            # Add each translated section
            for section_line_num in new_sections:
                # Find the corresponding translated content
                section_hierarchy = None
                section_content = None
                
                # Search for the section in translated_sections by line number or hierarchy
                for hierarchy, content in translated_sections.items():
                    # Try to match by hierarchy or find the content
                    if str(section_line_num) in hierarchy or content:  # This is a simplified matching
                        section_hierarchy = hierarchy
                        section_content = content
                        break
                
                if section_content:
                    # Split content into lines and add to insertion
                    content_lines = section_content.split('\n')
                    new_content_lines.extend(content_lines)
                    
                    # Add spacing between sections
                    if section_line_num != new_sections[-1]:  # Not the last section
                        new_content_lines.append("")
                    
                    thread_safe_print(f"       ✅ Added section: {section_hierarchy}")
                else:
                    thread_safe_print(f"       ⚠️  Could not find translated content for section at line {section_line_num}")
            
            # Add an empty line after the new sections if not already present
            # Check if the new content already ends with an empty line
            if new_content_lines and not new_content_lines[-1].strip():
                # Content already ends with empty line, don't add another
                pass
            elif insertion_index + 1 < len(lines) and lines[insertion_index + 1].strip():
                # Next line has content and our content doesn't end with empty line, add one
                new_content_lines.append("")
            
            # Insert the new content (insert after insertion_index line, before the next line)
            # If insertion_after_line is 251, we want to insert at position 252 (0-based index 251)
            lines = lines[:insertion_index + 1] + new_content_lines + lines[insertion_index + 1:]
            
            insertions_made.append({
                'group_id': group_id,
                'insertion_after_line': insertion_after_line,
                'sections_count': len(new_sections),
                'lines_added': len(new_content_lines),
                'insertion_type': insertion_type
            })
        
        # Save updated document
        with open(local_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        
        thread_safe_print(f"   ✅ Successfully inserted {len(insertions_made)} section groups")
        for insertion in insertions_made:
            thread_safe_print(f"      📝 {insertion['group_id']}: {insertion['sections_count']} sections, {insertion['lines_added']} lines after line {insertion['insertion_after_line']}")
        
        return True
        
    except Exception as e:
        thread_safe_print(f"   ❌ Error inserting sections: {sanitize_exception_message(e)}")
        return False

def process_modified_sections(modified_sections, pr_diff, source_context_or_pr_url, github_client, ai_client, repo_config, max_non_system_sections=120, glossary_matcher=None, dry_run=False):
    """Process modified sections with full data structure support"""
    results = []
    
    for file_path, file_data in modified_sections.items():
        thread_safe_print(f"\n📄 Processing {file_path}")
        
        try:
            # Call process_single_file with the complete data structure
            success, message = process_single_file(
                file_path, 
                file_data,  # Pass the complete data structure (includes 'sections', 'original_hierarchy', etc.)
                pr_diff, 
                source_context_or_pr_url, 
                github_client, 
                ai_client, 
                repo_config, 
                max_non_system_sections,
                glossary_matcher=glossary_matcher,
                dry_run=dry_run
            )
            
            if success:
                thread_safe_print(f"   ✅ Successfully processed {file_path}")
                results.append((file_path, True, message))
            else:
                thread_safe_print(f"   ❌ Failed to process {file_path}: {message}")
                results.append((file_path, False, message))
                
        except Exception as e:
            sanitized = sanitize_exception_message(e)
            thread_safe_print(f"   ❌ Error processing {file_path}: {sanitized}")
            results.append((file_path, False, f"Error processing {file_path}: {sanitized}"))
    
    return results

def process_deleted_sections(deleted_sections, source_context_or_pr_url, github_client, ai_client, repo_config, max_non_system_sections=120):
    """Process deleted sections with full data structure support"""
    results = []
    
    for file_path, source_sections in deleted_sections.items():
        thread_safe_print(f"\n🗑️  Processing deleted sections in {file_path}")
        
        try:
            # Call process_single_file_deletion with the complete data structure
            success, message = process_single_file_deletion(
                file_path, 
                source_sections, 
                source_context_or_pr_url, 
                github_client, 
                ai_client, 
                repo_config, 
                max_non_system_sections
            )
            
            if success:
                thread_safe_print(f"   ✅ Successfully processed deletions in {file_path}")
                results.append((file_path, True, message))
            else:
                thread_safe_print(f"   ❌ Failed to process deletions in {file_path}: {message}")
                results.append((file_path, False, message))
                
        except Exception as e:
            sanitized = sanitize_exception_message(e)
            thread_safe_print(f"   ❌ Error processing deletions in {file_path}: {sanitized}")
            results.append((file_path, False, f"Error processing deletions in {file_path}: {sanitized}"))
    
    return results

def process_single_file_deletion(file_path, source_sections, source_context_or_pr_url, github_client, ai_client, repo_config, max_non_system_sections=120):
    """Process deletion of sections in a single file"""
    
    # Import needed functions
    from diff_analyzer import get_target_hierarchy_and_content
    from section_matcher import (
        find_direct_matches_for_special_files, 
        filter_non_system_sections, 
        get_corresponding_sections,
        is_system_variable_or_config,
        clean_title_for_matching,
        parse_ai_response,
        find_matching_line_numbers
    )
    
    # Get target file hierarchy and content
    target_hierarchy, target_lines = get_target_hierarchy_and_content(
        file_path,
        github_client,
        repo_config['target_repo'],
        repo_config.get('target_local_path'),
        repo_config.get('prefer_local_target_for_read', False),
        repo_config.get('target_ref'),
    )
    
    if not target_hierarchy:
        return False, f"Could not get target hierarchy for {file_path}"
    
    # Separate system variables from regular sections for hybrid mapping
    system_sections = {}
    regular_sections = {}
    
    for line_num, hierarchy in source_sections.items():
        # Extract title for checking
        if ' > ' in hierarchy:
            title = hierarchy.split(' > ')[-1]
        else:
            title = hierarchy
        
        cleaned_title = clean_title_for_matching(title)
        if is_system_variable_or_config(cleaned_title):
            system_sections[line_num] = hierarchy
        else:
            regular_sections[line_num] = hierarchy
    
    sections_to_delete = []
    
    # Process system variables with direct matching
    if system_sections:
        thread_safe_print(f"   🎯 Direct matching for {len(system_sections)} system sections...")
        matched_dict, failed_matches, skipped_sections = find_direct_matches_for_special_files(
            system_sections, target_hierarchy, target_lines
        )
        
        for target_line_num, hierarchy_string in matched_dict.items():
            sections_to_delete.append(int(target_line_num))
            thread_safe_print(f"      ✅ Marked system section for deletion: line {target_line_num}")
        
        if failed_matches:
            thread_safe_print(f"      ❌ Failed to match {len(failed_matches)} system sections")
            for failed_line in failed_matches:
                thread_safe_print(f"         - Line {failed_line}: {system_sections[failed_line]}")
    
    # Process regular sections with AI matching
    if regular_sections:
        thread_safe_print(f"   🤖 AI matching for {len(regular_sections)} regular sections...")
        
        # Filter target hierarchy for AI
        filtered_target_hierarchy = filter_non_system_sections(target_hierarchy)
        
        # Check if filtered hierarchy is reasonable for AI
        if len(filtered_target_hierarchy) > max_non_system_sections:
            thread_safe_print(f"      ❌ Target hierarchy too large for AI: {len(filtered_target_hierarchy)} > {max_non_system_sections}")
        else:
            # Get AI mapping (convert dict values to lists as expected by the function)
            source_list = list(regular_sections.values())
            target_list = list(filtered_target_hierarchy.values())
            
            ai_mapping = get_corresponding_sections(
                source_list, 
                target_list, 
                ai_client,
                repo_config['source_language'], 
                repo_config['target_language'],
                max_tokens=20000  # Use default value for now, can be made configurable later
            )
            
            if ai_mapping:
                # Parse AI response and find matching line numbers
                ai_sections = parse_ai_response(ai_mapping)
                ai_matched = find_matching_line_numbers(ai_sections, target_hierarchy)
                
                for source_line, target_line in ai_matched.items():
                    try:
                        sections_to_delete.append(int(target_line))
                        thread_safe_print(f"      ✅ Marked regular section for deletion: line {target_line}")
                    except ValueError as e:
                        thread_safe_print(
                            f"      ❌ Error converting target_line to int: {target_line}, error: {sanitize_exception_message(e)}"
                        )
                        # If target_line is not a number, try to find it in target_hierarchy
                        for line_num, hierarchy in target_hierarchy.items():
                            if target_line in hierarchy or hierarchy in target_line:
                                sections_to_delete.append(int(line_num))
                                thread_safe_print(f"      ✅ Found matching section at line {line_num}: {hierarchy}")
                                break
    
    # Delete the sections from local document
    if sections_to_delete:
        success = delete_sections_from_document(file_path, sections_to_delete, repo_config['target_local_path'])
        if success:
            return True, f"Successfully deleted {len(sections_to_delete)} sections from {file_path}"
        else:
            return False, f"Failed to delete sections from {file_path}"
    else:
        return False, f"No sections to delete in {file_path}"

def delete_sections_from_document(file_path, sections_to_delete, target_local_path):
    """Delete specified sections from the local document"""
    target_file_path = os.path.join(target_local_path, file_path)
    
    if not os.path.exists(target_file_path):
        thread_safe_print(f"   ❌ Target file not found: {target_file_path}")
        return False
    
    try:
        # Read current file content without normalizing unrelated line endings.
        lines = read_text_lines_preserve_newlines(target_file_path)
        content = ''.join(lines)
        
        # Import needed function
        from diff_analyzer import build_hierarchy_dict
        
        # Build hierarchy to understand section boundaries
        target_hierarchy = build_hierarchy_dict(content)
        
        # Sort sections to delete in reverse order to maintain line numbers
        sections_to_delete.sort(reverse=True)
        
        thread_safe_print(f"   🗑️  Deleting {len(sections_to_delete)} sections from {file_path}")
        
        for section_line in sections_to_delete:
            section_start = section_line - 1  # Convert to 0-based index
            
            if section_start < 0 or section_start >= len(lines):
                thread_safe_print(f"      ❌ Invalid section line: {section_line}")
                continue
            
            # Find section end
            section_end = len(lines) - 1  # Default to end of file
            
            # Look for next header at same or higher level
            raw_current_line = lines[section_start]
            current_line = raw_current_line.strip()
            if is_markdown_heading(raw_current_line):
                current_level = len(current_line.split('#')[1:])  # Count # characters
                
                for i in range(section_start + 1, len(lines)):
                    raw_line = lines[i]
                    line = raw_line.strip()
                    if is_markdown_heading(raw_line):
                        line_level = len(line.split('#')[1:])
                        if line_level <= current_level:
                            section_end = i - 1
                            break
            
            # Delete section (from section_start to section_end inclusive)
            thread_safe_print(f"      🗑️  Deleting lines {section_start + 1} to {section_end + 1}")
            del lines[section_start:section_end + 1]
        
        # Write updated content back to file while preserving untouched line endings.
        write_text_lines_preserve_newlines(target_file_path, lines)
        
        thread_safe_print(f"   ✅ Updated file: {target_file_path}")
        return True
        
    except Exception as e:
        thread_safe_print(
            f"   ❌ Error deleting sections from {target_file_path}: {sanitize_exception_message(e)}"
        )
        return False

def process_single_file(file_path, source_sections, pr_diff, source_context_or_pr_url, github_client, ai_client, repo_config, max_non_system_sections=120, glossary_matcher=None, dry_run=False):
    """Process a single file - thread-safe function for parallel processing"""
    thread_id = threading.current_thread().name
    source_mode = get_source_mode(source_context_or_pr_url)
    thread_safe_print(f"\n📄 [{thread_id}] Processing {file_path}")
    
    try:
        # Check if this is a TOC file with special operations
        if isinstance(source_sections, dict) and 'type' in source_sections and source_sections['type'] == 'toc':
            from toc_processor import process_toc_file
            return process_toc_file(file_path, source_sections, source_context_or_pr_url, github_client, ai_client, repo_config)
        
        # Check if this is enhanced sections
        if isinstance(source_sections, dict) and 'sections' in source_sections:
            if source_sections.get('type') == 'enhanced_sections':
                # Skip all the matching logic and directly extract data
                thread_safe_print(f"   [{thread_id}] 🚀 Using enhanced sections data, skipping matching logic")
                enhanced_sections = source_sections['sections']
                
                # Extract target sections and source old content from enhanced sections
                # Maintain the exact order from match_source_diff_to_target.json
                from collections import OrderedDict
                target_sections = OrderedDict()
                source_old_content_dict = OrderedDict()
                source_routing_content_dict = OrderedDict()
                
                # Process in the exact order they appear in enhanced_sections (which comes from match_source_diff_to_target.json)
                for key, section_info in enhanced_sections.items():
                    if isinstance(section_info, dict):
                        operation = section_info.get('source_operation', '')
                        
                        # Skip deleted sections - they shouldn't be in the enhanced_sections anyway
                        if operation == 'deleted':
                            continue
                        
                        # Always use source_new_content (post-change) so the AI
                        # sees the final source state alongside the diff and
                        # current target.  This avoids the AI missing newly added
                        # paragraphs that only appeared in the diff.
                        source_content = section_info.get('source_new_content', '')
                        if not source_content:
                            source_content = section_info.get('source_old_content', '')
                        routing_content = source_content
                        
                        # For target sections: use target_content for modified, empty string for added
                        if operation == 'added':
                            target_content = ""  # Added sections have no existing target content
                        else:  # modified
                            target_content = section_info.get('target_content', '')
                        
                        # Add to both dictionaries using the same key from match_source_diff_to_target.json
                        if source_content is not None:
                            source_old_content_dict[key] = source_content
                        source_routing_content_dict[key] = routing_content if routing_content is not None else source_content
                        target_sections[key] = target_content
                
                thread_safe_print(f"   [{thread_id}] 📊 Extracted: {len(target_sections)} target sections, {len(source_old_content_dict)} source old content entries")
                
                # Update sections with AI (get-updated-target-sections.py logic)
                thread_safe_print(f"   [{thread_id}] 🤖 Getting updated sections from AI...")
                updated_sections = get_updated_sections_from_ai(
                    pr_diff,
                    target_sections,
                    source_old_content_dict,
                    ai_client,
                    repo_config['source_language'],
                    repo_config['target_language'],
                    file_path,
                    glossary_matcher=glossary_matcher,
                    dry_run=dry_run,
                    source_mode=source_mode,
                    _chunk_routing_content_dict=source_routing_content_dict,
                )
                if dry_run:
                    thread_safe_print(f"   [{thread_id}] ⏸️  Dry-run: prompt saved for {file_path}")
                    return True, {}
                if not updated_sections:
                    thread_safe_print(f"   [{thread_id}] ⚠️  Could not get AI update")
                    chunk_failures = getattr(updated_sections, "failures", [])
                    if chunk_failures:
                        return False, "; ".join(chunk_failures)
                    return False, f"Could not get AI update for {file_path}"

                chunk_failures = getattr(updated_sections, "failures", [])
                if chunk_failures:
                    thread_safe_print(f"   [{thread_id}] ⚠️  Skipping file update due to chunk translation failures")
                    return False, "; ".join(chunk_failures)

                # Return the AI results for further processing
                thread_safe_print(f"   [{thread_id}] ✅ Successfully got AI translation results for {file_path}")
                return True, updated_sections  # Return the actual AI results
                    
            else:
                # New format: complete data structure
                actual_sections = source_sections['sections']
        
        # Regular file processing continues here for old format
        # Get target hierarchy and content (get-target-affected-hierarchy.py logic)
        from diff_analyzer import get_target_hierarchy_and_content
        target_hierarchy, target_lines = get_target_hierarchy_and_content(
            file_path,
            github_client,
            repo_config['target_repo'],
            repo_config.get('target_local_path'),
            repo_config.get('prefer_local_target_for_read', False),
            repo_config.get('target_ref'),
        )
        if not target_hierarchy:
            thread_safe_print(f"   [{thread_id}] ⚠️  Could not get target content")
            return False, f"Could not get target content for {file_path}"
        else:
            # Old format: direct dict
            actual_sections = source_sections
            
        # Only do mapping if we don't have enhanced sections
        if 'enhanced_sections' not in locals() or not enhanced_sections:
            # Separate different types of sections
            from section_matcher import is_system_variable_or_config
            system_var_sections = {}
            toplevel_sections = {}
            frontmatter_sections = {}
            regular_sections = {}
            
            for line_num, hierarchy in actual_sections.items():
                if line_num == "0" and hierarchy == "frontmatter":
                    # Special handling for frontmatter
                    frontmatter_sections[line_num] = hierarchy
                else:
                    # Extract the leaf title from hierarchy
                    leaf_title = hierarchy.split(' > ')[-1] if ' > ' in hierarchy else hierarchy
                    
                    if is_system_variable_or_config(leaf_title):
                        system_var_sections[line_num] = hierarchy
                    elif leaf_title.startswith('# '):
                        # Top-level titles need special handling
                        toplevel_sections[line_num] = hierarchy
                    else:
                        regular_sections[line_num] = hierarchy
        
        thread_safe_print(f"   [{thread_id}] 📊 Found {len(system_var_sections)} system variable/config, {len(toplevel_sections)} top-level, {len(frontmatter_sections)} frontmatter, and {len(regular_sections)} regular sections")
        
        target_affected = {}
        
        # Process frontmatter sections with special handling
        if frontmatter_sections:
            thread_safe_print(f"   [{thread_id}] 📄 Processing frontmatter section...")
            # For frontmatter, we simply map it to line 0 in target
            for line_num, hierarchy in frontmatter_sections.items():
                target_affected[line_num] = hierarchy
            thread_safe_print(f"   [{thread_id}] ✅ Mapped {len(frontmatter_sections)} frontmatter section")
        
        # Process top-level titles with special matching
        if toplevel_sections:
            thread_safe_print(f"   [{thread_id}] 🔝 Top-level title matching for {len(toplevel_sections)} sections...")
            from section_matcher import find_toplevel_title_matches
            toplevel_matched, toplevel_failed, toplevel_skipped = find_toplevel_title_matches(toplevel_sections, target_lines)
            
            if toplevel_matched:
                target_affected.update(toplevel_matched)
                thread_safe_print(f"   [{thread_id}] ✅ Top-level matched {len(toplevel_matched)} sections")
            
            if toplevel_failed:
                thread_safe_print(f"   [{thread_id}] ⚠️  {len(toplevel_failed)} top-level sections failed matching")
                for failed in toplevel_failed:
                    thread_safe_print(f"       ❌ {failed['hierarchy']}: {failed['reason']}")
        
        # Process system variables/config sections with direct matching
        if system_var_sections:
            thread_safe_print(f"   [{thread_id}] 🎯 Direct matching {len(system_var_sections)} system variable/config sections...")
            from section_matcher import find_direct_matches_for_special_files
            direct_matched, failed_matches, skipped_sections = find_direct_matches_for_special_files(system_var_sections, target_hierarchy, target_lines)
            
            if direct_matched:
                target_affected.update(direct_matched)
                thread_safe_print(f"   [{thread_id}] ✅ Direct matched {len(direct_matched)} system variable/config sections")
            
            if failed_matches:
                thread_safe_print(f"   [{thread_id}] ⚠️  {len(failed_matches)} system variable/config sections failed direct matching")
                for failed in failed_matches:
                    thread_safe_print(f"       ❌ {failed['hierarchy']}: {failed['reason']}")
        
        # Process regular sections with AI mapping using filtered target hierarchy
        if regular_sections:
            thread_safe_print(f"   [{thread_id}] 🤖 AI mapping {len(regular_sections)} regular sections...")
            
            # Filter target hierarchy to only include non-system sections for AI mapping
            from section_matcher import filter_non_system_sections
            filtered_target_hierarchy = filter_non_system_sections(target_hierarchy)
            
            # Check if filtered target hierarchy exceeds the maximum allowed for AI mapping
            MAX_NON_SYSTEM_SECTIONS_FOR_AI = 120
            if len(filtered_target_hierarchy) > MAX_NON_SYSTEM_SECTIONS_FOR_AI:
                thread_safe_print(f"   [{thread_id}] ❌ Too many non-system sections ({len(filtered_target_hierarchy)} > {MAX_NON_SYSTEM_SECTIONS_FOR_AI})")
                thread_safe_print(f"   [{thread_id}] ⚠️  Skipping AI mapping for regular sections to avoid complexity")
                
                # If no system sections were matched either, return error
                if not target_affected:
                    error_message = f"File {file_path} has too many non-system sections ({len(filtered_target_hierarchy)} > {MAX_NON_SYSTEM_SECTIONS_FOR_AI}) and no system variable sections were matched"
                    return False, error_message
                
                # Continue with only system variable matches if available
                thread_safe_print(f"   [{thread_id}] ✅ Proceeding with {len(target_affected)} system variable/config sections only")
            else:
                # Proceed with AI mapping using filtered hierarchy
                source_list = list(regular_sections.values())
                target_list = list(filtered_target_hierarchy.values())
                
                from section_matcher import get_corresponding_sections
                ai_response = get_corresponding_sections(source_list, target_list, ai_client, repo_config['source_language'], repo_config['target_language'], max_tokens=20000)
                if ai_response:
                    # Parse AI response and find matching line numbers in the original (unfiltered) hierarchy
                    from section_matcher import parse_ai_response, find_matching_line_numbers
                    ai_sections = parse_ai_response(ai_response)
                    ai_matched = find_matching_line_numbers(ai_sections, target_hierarchy)  # Use original hierarchy for line number lookup
                    
                    if ai_matched:
                        target_affected.update(ai_matched)
                        thread_safe_print(f"   [{thread_id}] ✅ AI mapped {len(ai_matched)} regular sections")
                    else:
                        thread_safe_print(f"   [{thread_id}] ⚠️  AI mapping failed for regular sections")
                else:
                    thread_safe_print(f"   [{thread_id}] ⚠️  Could not get AI response for regular sections")
        
        # Summary of mapping results
        thread_safe_print(f"   [{thread_id}] 📊 Total mapped: {len(target_affected)} out of {len(actual_sections)} sections")
        
        if not target_affected:
            thread_safe_print(f"   [{thread_id}] ⚠️  Could not map sections")
            return False, f"Could not map sections for {file_path}"
        
        thread_safe_print(f"   [{thread_id}] ✅ Mapped {len(target_affected)} sections")
        
        # Extract target sections (get-target-affected-sections.py logic)
        thread_safe_print(f"   [{thread_id}] 📝 Extracting target sections...")
        from diff_analyzer import extract_affected_sections
        target_sections = extract_affected_sections(target_affected, target_lines)
        
        # Extract source old content from the enhanced data structure
        thread_safe_print(f"   [{thread_id}] 📖 Extracting source old content...")
        source_old_content_dict = {}
        source_routing_content_dict = {}
        
        # Handle different data structures for source_sections
        if isinstance(source_sections, dict) and 'sections' in source_sections:
            # New format: complete data structure with enhanced matching info
            # Always prefer source_new_content (post-change) so the AI sees
            # the final source state alongside the diff.
            for key, section_info in source_sections.items():
                if isinstance(section_info, dict) and ('source_new_content' in section_info or 'source_old_content' in section_info):
                    source_content = section_info.get('source_new_content') or section_info.get('source_old_content', '')
                    source_old_content_dict[key] = source_content
                    source_routing_content_dict[key] = source_content
        else:
            # Fallback: if we don't have the enhanced structure, we need to get it differently
            thread_safe_print(f"   [{thread_id}] ⚠️  Source sections missing enhanced structure, using fallback")
            # For now, create empty dict to avoid errors - this should be addressed in the calling code
            source_old_content_dict = {}
        
        # Update sections with AI (get-updated-target-sections.py logic)
        thread_safe_print(f"   [{thread_id}] 🤖 Getting updated sections from AI...")
        updated_sections = get_updated_sections_from_ai(
            pr_diff, target_sections, source_old_content_dict, ai_client,
            repo_config['source_language'], repo_config['target_language'], file_path,
            glossary_matcher=glossary_matcher, dry_run=dry_run, source_mode=source_mode,
            _chunk_routing_content_dict=source_routing_content_dict or None,
        )
        if dry_run:
            thread_safe_print(f"   [{thread_id}] ⏸️  Dry-run: prompt saved for {file_path}")
            return True, {}
        if not updated_sections:
            thread_safe_print(f"   [{thread_id}] ⚠️  Could not get AI update")
            chunk_failures = getattr(updated_sections, "failures", [])
            if chunk_failures:
                return False, "; ".join(chunk_failures)
            return False, f"Could not get AI update for {file_path}"

        chunk_failures = getattr(updated_sections, "failures", [])
        if chunk_failures:
            thread_safe_print(f"   [{thread_id}] ⚠️  Skipping file update due to chunk translation failures")
            return False, "; ".join(chunk_failures)
        
        # Update local document (update-target-doc-v2.py logic)
        thread_safe_print(f"   [{thread_id}] 💾 Updating local document...")
        success = update_local_document(file_path, updated_sections, target_affected, repo_config['target_local_path'])
        
        if success:
            thread_safe_print(f"   [{thread_id}] 🎉 Successfully updated {file_path}")
            return True, f"Successfully updated {file_path}"
        else:
            thread_safe_print(f"   [{thread_id}] ❌ Failed to update {file_path}")
            return False, f"Failed to update {file_path}"
            
    except Exception as e:
        sanitized = sanitize_exception_message(e)
        thread_safe_print(f"   [{thread_id}] ❌ Error processing {file_path}: {sanitized}")
        return False, f"Error processing {file_path}: {sanitized}"

def process_added_sections(added_sections, pr_diff, source_context_or_pr_url, github_client, ai_client, repo_config, max_non_system_sections=120, glossary_matcher=None):
    """Process added sections by translating and inserting them"""
    if not added_sections:
        thread_safe_print("\n➕ No added sections to process")
        return

    source_mode = get_source_mode(source_context_or_pr_url)
    
    thread_safe_print(f"\n➕ Processing added sections from {len(added_sections)} files...")
    
    # Import needed functions
    from section_matcher import map_insertion_points_to_target
    from diff_analyzer import get_target_hierarchy_and_content
    
    for file_path, section_data in added_sections.items():
        thread_safe_print(f"\n➕ Processing added sections in {file_path}")
        
        source_sections = section_data['sections']
        insertion_points = section_data['insertion_points']
        
        # Get target file hierarchy and content
        target_hierarchy, target_lines = get_target_hierarchy_and_content(
            file_path,
            github_client,
            repo_config['target_repo'],
            repo_config.get('target_local_path'),
            repo_config.get('prefer_local_target_for_read', False),
            repo_config.get('target_ref'),
        )
        
        if not target_hierarchy:
            thread_safe_print(f"   ❌ Could not get target hierarchy for {file_path}")
            continue
        
        # Map insertion points to target language
        target_insertion_points = map_insertion_points_to_target(
            insertion_points, target_hierarchy, target_lines, file_path, source_context_or_pr_url, github_client, ai_client, repo_config, max_non_system_sections
        )
        
        if not target_insertion_points:
            thread_safe_print(f"   ❌ No insertion points mapped for {file_path}")
            continue
        
        # Use AI to translate/update new sections (similar to modified sections)
        # Since we're now using source_old_content, we need to extract it from the added sections
        source_old_content_dict = {}
        for key, content in source_sections.items():
            # For added sections, source_old_content is typically None or empty
            # We use the new content (from the source file) as the content to translate
            source_old_content_dict[key] = content if content is not None else ""
        
        # Get target sections (empty for new sections, but we need the structure)
        target_sections = {}  # New sections don't have existing target content
        
        # Use the same AI function to translate the new sections
        translated_sections = get_updated_sections_from_ai(
            pr_diff, 
            target_sections, 
            source_old_content_dict, 
            ai_client,
            repo_config['source_language'], 
            repo_config['target_language'],
            file_path,
            glossary_matcher=glossary_matcher,
            source_mode=source_mode
        )
        
        if translated_sections:
            # Insert translated sections into document
            insert_sections_into_document(file_path, translated_sections, target_insertion_points, repo_config['target_local_path'])
            thread_safe_print(f"   ✅ Successfully inserted {len(translated_sections)} sections in {file_path}")
        else:
            thread_safe_print(f"   ⚠️  No sections were translated for {file_path}")

def process_files_in_batches(source_changes, pr_diff, source_context_or_pr_url, github_client, ai_client, repo_config, operation_type="modified", batch_size=5, max_non_system_sections=120, glossary_matcher=None):
    """Process files in parallel batches"""
    # Handle different data formats
    if isinstance(source_changes, dict):
        files = []
        for path, data in source_changes.items():
            if isinstance(data, dict):
                if 'type' in data and data['type'] == 'toc':
                    # TOC file with special operations
                    files.append((path, data))
                elif 'sections' in data:
                    # New format: extract sections for processing
                    files.append((path, data['sections']))
                else:
                    # Old format: direct dict
                    files.append((path, data))
            else:
                # Old format: direct dict
                files.append((path, data))
    else:
        files = list(source_changes.items())
    
    total_files = len(files)
    
    if total_files == 0:
        return []
    
    thread_safe_print(f"\n🔄 Processing {total_files} files in batches of {batch_size}")
    
    results = []
    
    # Process files in batches
    for i in range(0, total_files, batch_size):
        batch = files[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total_files + batch_size - 1) // batch_size
        
        thread_safe_print(f"\n📦 Batch {batch_num}/{total_batches}: Processing {len(batch)} files")
        
        # Process current batch in parallel
        with ThreadPoolExecutor(max_workers=len(batch), thread_name_prefix=f"Batch{batch_num}") as executor:
            # Submit all files in current batch
            future_to_file = {}
            for file_path, source_sections in batch:
                future = executor.submit(
                    process_single_file, 
                    file_path, 
                    source_sections, 
                    pr_diff, 
                    source_context_or_pr_url, 
                    github_client, 
                    ai_client,
                    repo_config,
                    max_non_system_sections,
                    glossary_matcher=glossary_matcher
                )
                future_to_file[future] = file_path
            
            # Collect results as they complete
            from concurrent.futures import as_completed
            batch_results = []
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    success, message = future.result()
                    batch_results.append((file_path, success, message))
                except Exception as e:
                    batch_results.append(
                        (file_path, False, f"Exception in thread: {sanitize_exception_message(e)}")
                    )
            
            results.extend(batch_results)
        
        # Brief pause between batches to avoid overwhelming the APIs
        if i + batch_size < total_files:
            thread_safe_print(f"   ⏸️  Waiting 2 seconds before next batch...")
            import time
            time.sleep(2)
    
    return results

def _detect_and_fix_cross_parent_moves(sections_with_line):
    """Detect sections that moved to a different parent and split them into delete + insert.

    When a source file restructure moves a section from one parent heading to
    another (e.g. ``### Modify project roles`` under ``## Manage project
    access`` becomes ``### Remove instance access`` under a new ``## Manage
    instance access``), the matcher maps the change to the *old* target
    position.  An in-place replace would leave the translated content under the
    wrong parent.

    This function detects such cases and converts the single "modified/replace"
    entry into two entries — a *delete* at the old position and an *insert*
    next to the newly added parent — so the content lands in the right place.
    """
    from section_matcher import extract_first_heading_from_content, clean_title_for_matching

    def _source_line(key):
        parts = key.split('_')
        if len(parts) >= 2 and parts[-1].isdigit():
            return int(parts[-1])
        return 0

    def _heading_level(heading_line):
        if not heading_line:
            return 0
        stripped = heading_line.lstrip()
        level = 0
        for ch in stripped:
            if ch == '#':
                level += 1
            else:
                break
        return level

    added_parents = []
    for key, section_data, line_num in sections_with_line:
        if section_data.get('source_operation') != 'added':
            continue
        new_content = section_data.get('source_new_content', '')
        heading = extract_first_heading_from_content(new_content)
        if heading and _heading_level(heading) == 2:
            added_parents.append((_source_line(key), key, section_data, line_num))
    added_parents.sort(key=lambda x: x[0])

    if not added_parents:
        return sections_with_line

    result = []
    for key, section_data, line_num in sections_with_line:
        if section_data.get('source_operation') != 'modified':
            result.append((key, section_data, line_num))
            continue

        old_content = section_data.get('source_old_content', '') or ''
        new_content = section_data.get('source_new_content', '') or ''

        if old_content.strip():
            result.append((key, section_data, line_num))
            continue

        old_hierarchy = section_data.get('source_original_hierarchy', '')
        old_leaf = old_hierarchy.rsplit(' > ', 1)[-1] if old_hierarchy else ''
        old_title = clean_title_for_matching(old_leaf)

        new_heading = extract_first_heading_from_content(new_content)
        new_title = clean_title_for_matching(new_heading) if new_heading else ''

        if not old_title or not new_title or old_title == new_title:
            result.append((key, section_data, line_num))
            continue

        src_line = _source_line(key)
        parent_match = None
        for p_src_line, p_key, p_data, p_line_num in reversed(added_parents):
            if p_src_line < src_line:
                parent_match = (p_key, p_data, p_line_num)
                break

        if parent_match is None:
            result.append((key, section_data, line_num))
            continue

        p_key, p_data, p_target_line = parent_match

        thread_safe_print(
            f"   🔀 Cross-parent move detected: {key}"
            f"\n      Old position: {old_leaf} (target line {line_num})"
            f"\n      New parent: {p_key} (target line {p_target_line})"
            f"\n      → Splitting into DELETE at line {line_num} + INSERT before line {p_target_line}"
        )

        delete_data = dict(section_data)
        delete_data['source_operation'] = 'deleted'
        delete_data['target_new_content'] = None
        result.append((key + '_delete', delete_data, line_num))

        insert_data = dict(section_data)
        insert_data['insertion_type'] = 'before_reference'
        result.append((key + '_insert', insert_data, p_target_line))

    return result


def update_target_document_from_match_data(match_file_path, target_local_path, target_file_name=None):
    """
    Update target document using data from match_source_diff_to_target.json
    This integrates the logic from test_target_update.py
    
    Args:
        match_file_path: Path to the match_source_diff_to_target.json file
        target_local_path: Local path to the target repository 
        target_file_name: Optional target file name (if not provided, will be extracted from match_file_path)
    """
    import json
    import os
    from pathlib import Path
    
    # Load match data
    if not os.path.exists(match_file_path):
        thread_safe_print(f"❌ {match_file_path} file does not exist")
        return False
    
    with open(match_file_path, 'r', encoding='utf-8') as f:
        match_data = json.load(f)
    
    thread_safe_print(f"✅ Loaded {len(match_data)} section matching data from {match_file_path}")
    thread_safe_print(f"   Reading translation results directly from target_new_content field")
    
    if not match_data:
        thread_safe_print("❌ No matching data found")
        return False
    
    # Sort sections by target_line from large to small (modify from back to front)
    sections_with_line = []
    
    for key, section_data in match_data.items():
        operation = section_data.get('source_operation', '')
        target_new_content = section_data.get('target_new_content')
        
        # For deleted sections, target_new_content should be null
        if operation == 'deleted':
            if target_new_content is not None:
                thread_safe_print(f"   ⚠️  Deleted section {key} has non-null target_new_content, should be fixed")
            thread_safe_print(f"   🗑️  Including deleted section: {key}")
        elif not target_new_content:
            thread_safe_print(f"   ⚠️  Skipping section without target_new_content: {key}")
            continue
        
        target_line = section_data.get('target_line')
        if target_line and target_line != 'unknown':
            try:
                # Handle special case for bottom sections
                if target_line == "-1":
                    line_num = -1  # Special marker for bottom sections
                else:
                    line_num = int(target_line)
                sections_with_line.append((key, section_data, line_num))
            except ValueError:
                thread_safe_print(f"⚠️  Skipping invalid target_line: {target_line} for {key}")
    
    # ------------------------------------------------------------------
    # Detect cross-parent section moves: when a "modified" section's
    # content was completely replaced with content that belongs under a
    # newly added parent heading, split it into delete + insert so the
    # new content lands under the correct parent.
    # ------------------------------------------------------------------
    sections_with_line = _detect_and_fix_cross_parent_moves(sections_with_line)

    # Separate sections into different processing groups
    bottom_modified_sections = []  # Process first: modify existing content at document end
    regular_sections = []          # Process second: normal operations from back to front
    bottom_added_sections = []     # Process last: append new content to document end
    
    for key, section_data, line_num in sections_with_line:
        target_hierarchy = section_data.get('target_hierarchy', '')
        
        if target_hierarchy.startswith('bottom-modified-'):
            bottom_modified_sections.append((key, section_data, line_num))
        elif target_hierarchy.startswith('bottom-added-'):
            bottom_added_sections.append((key, section_data, line_num))
        else:
            regular_sections.append((key, section_data, line_num))
    
    # Sort each group appropriately
    def get_source_line_num(item):
        key, section_data, line_num = item
        if '_' in key and key.split('_')[1].isdigit():
            return int(key.split('_')[1])
        return 0
    
    # Bottom modified: sort by source line number (large to small)
    bottom_modified_sections.sort(key=lambda x: -get_source_line_num(x))
    
    # Regular sections: sort by target_line (large to small), then by source line number
    regular_sections.sort(key=lambda x: (-x[2], -get_source_line_num(x)))
    
    # Bottom added: sort by source line number (small to large) for proper document order
    bottom_added_sections.sort(key=lambda x: get_source_line_num(x))
    
    # Combine all sections in processing order
    all_sections = bottom_modified_sections + regular_sections + bottom_added_sections
    
    thread_safe_print(f"\n📊 Processing order: bottom-modified -> regular -> bottom-added")
    thread_safe_print(f"   📋 Bottom modified sections: {len(bottom_modified_sections)}")
    thread_safe_print(f"   📋 Regular sections: {len(regular_sections)}")  
    thread_safe_print(f"   📋 Bottom added sections: {len(bottom_added_sections)}")
    
    if not all_sections:
        thread_safe_print("❌ No valid sections found for update")
        return False
    
    if verbose_logging_enabled():
        thread_safe_print(f"\n📊 Detailed processing order:")
        for i, (key, section_data, line_num) in enumerate(all_sections, 1):
            operation = section_data.get('source_operation', '')
            hierarchy = section_data.get('target_hierarchy', '')
            insertion_type = section_data.get('insertion_type', '')

            # Extract source line number for display
            source_line_num = int(key.split('_')[1]) if '_' in key and key.split('_')[1].isdigit() else 'N/A'

            # Display target_line with special handling for bottom sections
            target_display = "END" if line_num == -1 else str(line_num)

            # Determine section group
            if hierarchy.startswith('bottom-modified-'):
                group = "BotMod"
            elif hierarchy.startswith('bottom-added-'):
                group = "BotAdd"
            else:
                group = "Regular"

            if operation == 'deleted':
                action = "delete"
            elif insertion_type == "before_reference":
                action = "insert"
            elif line_num == -1:
                action = "append"
            else:
                action = "replace"

            thread_safe_print(f"   {i:2}. [{group:7}] Target:{target_display:>3} Src:{source_line_num:3} | {key:15} ({operation:8}) | {action:7} | {hierarchy}")
    
    # Determine target file name
    if target_file_name is None:
        # Extract target file name from match file path
        # e.g., "tikv-configuration-file-match_source_diff_to_target.json" -> "tikv-configuration-file.md"
        match_filename = os.path.basename(match_file_path)
        if match_filename.endswith('-match_source_diff_to_target.json'):
            extracted_name = match_filename[:-len('-match_source_diff_to_target.json')] + '.md'
            target_file_name = extracted_name
            thread_safe_print(f"   📂 Extracted target file name from match file: {target_file_name}")
        else:
            # Fallback: try to determine from source hierarchy
            first_entry = next(iter(match_data.values()))
            source_hierarchy = first_entry.get('source_original_hierarchy', '')
            
            if 'TiFlash' in source_hierarchy or 'tiflash' in source_hierarchy.lower():
                target_file_name = "tiflash/tiflash-configuration.md"
            else:
                # Default to command-line flags for other cases
                target_file_name = "command-line-flags-for-tidb-configuration.md"
            thread_safe_print(f"   📂 Determined target file name from hierarchy: {target_file_name}")
    else:
        thread_safe_print(f"   📂 Using provided target file name: {target_file_name}")
    
    target_file_path = os.path.join(target_local_path, target_file_name)
    thread_safe_print(f"\n📄 Target file path: {target_file_path}")
    
    # Update target document
    thread_safe_print(f"\n🚀 Starting target document update, will modify {len(all_sections)} sections...")
    success = update_target_document_sections(all_sections, target_file_path)
    
    return success

def update_target_document_sections(all_sections, target_file_path):
    """
    Update target document sections - integrated from test_target_update.py
    """
    thread_safe_print(f"\n🚀 Starting target document update: {target_file_path}")
    
    # Read target document
    if not os.path.exists(target_file_path):
        thread_safe_print(f"❌ Target file does not exist: {target_file_path}")
        return False
    
    target_lines = read_text_lines_preserve_newlines(target_file_path)
    
    thread_safe_print(f"📄 Target document total lines: {len(target_lines)}")
    
    # Process modifications in order (bottom-modified -> regular -> bottom-added)
    for i, (key, section_data, target_line_num) in enumerate(all_sections, 1):
        operation = section_data.get('source_operation', '')
        insertion_type = section_data.get('insertion_type', '')
        target_hierarchy = section_data.get('target_hierarchy', '')
        target_new_content = section_data.get('target_new_content')
        target_end_marker = section_data.get('target_end_marker')
        
        thread_safe_print(f"\n📝 {i}/{len(all_sections)} Processing {key} (Line {target_line_num})")
        thread_safe_print(f"   Operation type: {operation}")
        thread_safe_print(f"   Target section: {target_hierarchy}")
        
        if operation == 'deleted':
            # Delete logic: remove the specified section
            if target_line_num == -1:
                thread_safe_print(f"   ❌ Invalid delete operation for bottom section")
                continue
                
            thread_safe_print(f"   🗑️  Delete mode: removing section starting at line {target_line_num}")
            
            # Find section end position
            start_line = resolve_section_start_line(target_lines, target_line_num, target_hierarchy)
            
            if start_line >= len(target_lines):
                thread_safe_print(f"   ❌ Line number out of range: {target_line_num} > {len(target_lines)}")
                continue
            
            # Find section end position
            end_line = find_section_end_for_update(
                target_lines,
                start_line,
                target_hierarchy,
                end_marker=target_end_marker,
            )
            
            thread_safe_print(f"   📍 Delete range: line {start_line + 1} to {end_line}")
            thread_safe_print(f"   📄 Delete content: {target_lines[start_line].strip()[:50]}...")
            
            # Delete content
            deleted_lines = target_lines[start_line:end_line]
            target_lines[start_line:end_line] = []
            
            thread_safe_print(f"   ✅ Deleted {len(deleted_lines)} lines of content")
            
        elif target_new_content is None:
            thread_safe_print(f"   ⚠️  Skipping: target_new_content is null")
            continue
            
        elif not target_new_content:
            thread_safe_print(f"   ⚠️  Skipping: target_new_content is empty")
            continue
            
        else:
            # Handle content format
            verbose_thread_safe_print(f"   📄 Content preview: {repr(target_new_content[:80])}...")
            
            if target_hierarchy.startswith('bottom-'):
                # Bottom section special handling
                if target_hierarchy.startswith('bottom-modified-'):
                    # Bottom modified: find and replace existing content at document end
                    thread_safe_print(f"   🔄 Bottom modified section: replacing existing content at document end")
                    
                    # Get the old content to search for
                    source_operation_data = section_data.get('source_operation_data', {})
                    old_content = source_operation_data.get('old_content', '').strip()
                    
                    if old_content:
                        # Search backwards from end to find the matching section
                        found_line = None
                        for idx in range(len(target_lines) - 1, -1, -1):
                            line_content = target_lines[idx].strip()
                            if line_content == old_content:
                                found_line = idx
                                thread_safe_print(f"   📍 Found target section at line {found_line + 1}: {line_content[:50]}...")
                                break
                        
                        if found_line is not None:
                            # Find section end
                            end_line = find_section_end_for_update(target_lines, found_line, target_hierarchy)
                            
                            # Ensure content format is correct
                            if not target_new_content.endswith('\n'):
                                target_new_content += '\n'
                            
                            # Split content by lines
                            new_lines = target_new_content.splitlines(keepends=True)
                            
                            # Replace content
                            target_lines[found_line:end_line] = new_lines
                            
                            thread_safe_print(f"   ✅ Replaced {end_line - found_line} lines with {len(new_lines)} lines")
                        else:
                            thread_safe_print(f"   ⚠️  Could not find target section, appending to end instead")
                            # Fallback: append to end
                            if not target_new_content.endswith('\n'):
                                target_new_content += '\n'
                            if target_lines and target_lines[-1].strip():
                                target_new_content = '\n' + target_new_content
                            new_lines = target_new_content.splitlines(keepends=True)
                            target_lines.extend(new_lines)
                            thread_safe_print(f"   ✅ Appended {len(new_lines)} lines to end of document")
                    else:
                        thread_safe_print(f"   ⚠️  No old_content found, appending to end instead")
                        # Fallback: append to end
                        if not target_new_content.endswith('\n'):
                            target_new_content += '\n'
                        if target_lines and target_lines[-1].strip():
                            target_new_content = '\n' + target_new_content
                        new_lines = target_new_content.splitlines(keepends=True)
                        target_lines.extend(new_lines)
                        thread_safe_print(f"   ✅ Appended {len(new_lines)} lines to end of document")
                        
                elif target_hierarchy.startswith('bottom-added-'):
                    # Bottom added: append new content to end of document
                    thread_safe_print(f"   🔚 Bottom added section: appending new content to end")
                    
                    # Ensure content format is correct
                    if not target_new_content.endswith('\n'):
                        target_new_content += '\n'
                    
                    # Add spacing before new section if needed
                    if target_lines and target_lines[-1].strip():
                        target_new_content = '\n' + target_new_content
                    
                    # Split content by lines
                    new_lines = target_new_content.splitlines(keepends=True)
                    
                    # Append to end of document
                    target_lines.extend(new_lines)
                    
                    thread_safe_print(f"   ✅ Appended {len(new_lines)} lines to end of document")
                else:
                    # Other bottom sections: append to end
                    thread_safe_print(f"   🔚 Other bottom section: appending to end of document")
                    
                    # Ensure content format is correct
                    if not target_new_content.endswith('\n'):
                        target_new_content += '\n'
                    
                    # Add spacing before new section if needed
                    if target_lines and target_lines[-1].strip():
                        target_new_content = '\n' + target_new_content
                    
                    # Split content by lines
                    new_lines = target_new_content.splitlines(keepends=True)
                    
                    # Append to end of document
                    target_lines.extend(new_lines)
                    
                    thread_safe_print(f"   ✅ Appended {len(new_lines)} lines to end of document")
                
            elif target_hierarchy == "intro_section":
                # Intro section: from first # heading to first ## heading
                thread_safe_print(f"   📄 Intro section mode: replacing from first # to first ##")
                
                # Find first # heading in current buffer
                first_heading_line = None
                for i, line in enumerate(target_lines):
                    if line.strip().startswith('# '):
                        first_heading_line = i
                        break
                if first_heading_line is None:
                    thread_safe_print(f"   ⚠️  No # heading found in target, skipping intro_section update")
                    continue
                
                # Find first ## heading in current buffer
                first_level2_line = None
                for i, line in enumerate(target_lines):
                    if is_markdown_heading(line) and line.strip().startswith('## '):
                        first_level2_line = i
                        break
                if first_level2_line is None:
                    first_level2_line = len(target_lines)
                
                thread_safe_print(f"   📍 Intro section range: line {first_heading_line + 1} to {first_level2_line}")
                
                # Split new content by lines, preserving original structure
                new_lines = target_new_content.splitlines(keepends=True)
                
                # Ensure content ends with proper newline
                if target_new_content.endswith('\n') and not new_lines[-1].endswith('\n\n'):
                    new_lines.append('\n')
                elif target_new_content and not target_new_content.endswith('\n'):
                    if new_lines and not new_lines[-1].endswith('\n'):
                        new_lines[-1] += '\n'
                
                # Replace from # heading to ## heading (leaves frontmatter untouched)
                target_lines[first_heading_line:first_level2_line] = new_lines
                
                thread_safe_print(f"   ✅ Replaced {first_level2_line - first_heading_line} lines of intro section with {len(new_lines)} lines")
                
            elif target_hierarchy == "frontmatter":
                # Frontmatter special handling: directly replace front lines
                thread_safe_print(f"   📄 Frontmatter mode: directly replacing document beginning")
                
                # Find the first top-level heading position
                first_header_line = 0
                for i, line in enumerate(target_lines):
                    if line.strip().startswith('# '):
                        first_header_line = i
                        break
                
                thread_safe_print(f"   📍 Frontmatter range: line 1 to {first_header_line}")
                
                # Split new content by lines, preserving original structure including trailing empty lines
                new_lines = target_new_content.splitlines(keepends=True)
                
                # If the original content ends with \n, it means there should be an empty line after the last content line
                # splitlines() doesn't create this empty line, so we need to add it manually
                if target_new_content.endswith('\n'):
                    new_lines.append('\n')
                elif target_new_content:
                    # If content doesn't end with newline, ensure the last line has one
                    if not new_lines[-1].endswith('\n'):
                        new_lines[-1] += '\n'
                
                # Replace frontmatter
                target_lines[0:first_header_line] = new_lines
                
                thread_safe_print(f"   ✅ Replaced {first_header_line} lines of frontmatter with {len(new_lines)} lines")
                
            elif insertion_type == "before_reference":
                # Insert logic: insert before specified line
                if target_line_num == -1:
                    thread_safe_print(f"   ❌ Invalid insert operation for bottom section")
                    continue
                    
                thread_safe_print(f"   📍 Insert mode: inserting before line {target_line_num}")
                
                # Ensure content format is correct
                if not target_new_content.endswith('\n'):
                    target_new_content += '\n'
                
                # Ensure spacing between sections
                if not target_new_content.endswith('\n\n'):
                    target_new_content += '\n'
                
                # Split content by lines
                new_lines = target_new_content.splitlines(keepends=True)
                
                # Insert at specified position
                insert_position = target_line_num - 1  # Convert to 0-based index
                if insert_position < 0:
                    insert_position = 0
                elif insert_position > len(target_lines):
                    insert_position = len(target_lines)
                
                # Execute insertion
                for j, line in enumerate(new_lines):
                    target_lines.insert(insert_position + j, line)
                
                thread_safe_print(f"   ✅ Inserted {len(new_lines)} lines of content")
                
            else:
                # Replace logic: find target section and replace
                if target_line_num == -1:
                    thread_safe_print(f"   ❌ Invalid replace operation for bottom section")
                    continue
                    
                thread_safe_print(f"   🔄 Replace mode: replacing section starting at line {target_line_num}")
                
                # Ensure content format is correct
                if not target_new_content.endswith('\n'):
                    target_new_content += '\n'
                
                # Ensure spacing between sections
                if not target_new_content.endswith('\n\n'):
                    target_new_content += '\n'
                
                # Find section end position
                start_line = resolve_section_start_line(target_lines, target_line_num, target_hierarchy)
                
                if start_line >= len(target_lines):
                    thread_safe_print(f"   ❌ Line number out of range: {target_line_num} > {len(target_lines)}")
                    continue
                
                # Find section end position
                end_line = find_section_end_for_update(
                    target_lines,
                    start_line,
                    target_hierarchy,
                    end_marker=target_end_marker,
                )
                
                thread_safe_print(f"   📍 Replace range: line {start_line + 1} to {end_line}")
                
                # Split new content by lines
                new_lines = target_new_content.splitlines(keepends=True)
                
                # Replace content
                target_lines[start_line:end_line] = new_lines
                
                thread_safe_print(f"   ✅ Replaced {end_line - start_line} lines with {len(new_lines)} lines")
    
    
    write_text_lines_preserve_newlines(target_file_path, target_lines)
    
    thread_safe_print(f"\n✅ Target document update completed!")
    thread_safe_print(f"📄 Updated file: {target_file_path}")
    
    return True

def find_section_end_for_update(lines, start_line, target_hierarchy, end_marker=None):
    """Find section end position - based on test_target_update.py logic"""
    current_line = lines[start_line].strip()

    if end_marker:
        for i in range(start_line + 1, len(lines)):
            if end_marker in lines[i]:
                thread_safe_print(f"     📍 End marker '{end_marker}' found at line {i + 1}")
                return i
    
    if target_hierarchy == "frontmatter":
        # Frontmatter special handling: from --- to second ---, then to first top-level heading
        if start_line == 0 and current_line.startswith('---'):
            # Find second ---
            for i in range(start_line + 1, len(lines)):
                if lines[i].strip() == '---':
                    # Found frontmatter end, but need to include up to next content start
                    # Look for first non-empty line or first heading
                    for j in range(i + 1, len(lines)):
                        line = lines[j].strip()
                        if line and line.startswith('# '):
                            thread_safe_print(f"     📍 Frontmatter ends at line {j} (before first top-level heading)")
                            return j
                        elif line and not line.startswith('#'):
                            # If there's other content, end there
                            thread_safe_print(f"     📍 Frontmatter ends at line {j} (before other content)")
                            return j
                    # If no other content found, end after second ---
                    thread_safe_print(f"     📍 Frontmatter ends at line {i+1} (after second ---)")
                    return i + 1
        # If not standard frontmatter format, find first top-level heading
        for i in range(start_line + 1, len(lines)):
            if is_markdown_heading(lines[i]) and lines[i].startswith('# '):
                thread_safe_print(f"     📍 Frontmatter ends at line {i} (before first top-level heading)")
                return i
        # If no top-level heading found, process entire file
        return len(lines)
    
    if is_markdown_heading(lines[start_line]):
        # Use file_updater.py method to calculate heading level
        current_level = len(current_line.split()[0]) if current_line.split() else 0
        thread_safe_print(f"     🔍 Current heading level: {current_level} (heading: {current_line[:50]}...)")
        
        # Special handling for top-level headings: only process until first second-level heading
        in_code_block = False
        code_block_delimiter = None
        if current_level == 1:
            for i in range(start_line + 1, len(lines)):
                raw_line = lines[i]
                line = raw_line.strip()

                if line.startswith('```') or line.startswith('~~~'):
                    if not in_code_block:
                        in_code_block = True
                        code_block_delimiter = line[:3]
                    elif line.startswith(code_block_delimiter):
                        in_code_block = False
                        code_block_delimiter = None
                    continue

                if not in_code_block and is_markdown_heading(raw_line) and line.startswith('##'):  # Find first second-level heading
                    thread_safe_print(f"     📍 Top-level heading ends at line {i} (before first second-level heading)")
                    return i
            # If no second-level heading found, look for next top-level heading
            for i in range(start_line + 1, len(lines)):
                raw_line = lines[i]
                line = raw_line.strip()

                if line.startswith('```') or line.startswith('~~~'):
                    if not in_code_block:
                        in_code_block = True
                        code_block_delimiter = line[:3]
                    elif line.startswith(code_block_delimiter):
                        in_code_block = False
                        code_block_delimiter = None
                    continue

                if not in_code_block and is_markdown_heading(raw_line) and line.startswith('#') and not line.startswith('##'):
                    thread_safe_print(f"     📍 Top-level heading ends at line {i} (before next top-level heading)")
                    return i
        else:
            # For other level headings, stop at ANY header to get only direct content
            # This prevents including sub-sections in the update range
            for i in range(start_line + 1, len(lines)):
                raw_line = lines[i]
                line = raw_line.strip()

                if line.startswith('```') or line.startswith('~~~'):
                    if not in_code_block:
                        in_code_block = True
                        code_block_delimiter = line[:3]
                    elif line.startswith(code_block_delimiter):
                        in_code_block = False
                        code_block_delimiter = None
                    continue

                if not in_code_block and is_markdown_heading(raw_line):
                    # Stop at ANY header to get only direct content
                    thread_safe_print(f"     📍 Found header at line {i}: {line[:30]}... (stopping for direct content only)")
                    return i
        
        # If not found, return file end
        thread_safe_print(f"     📍 No end position found, using file end")
        return len(lines)
    
    # Non-heading line, only replace current line
    return start_line + 1
