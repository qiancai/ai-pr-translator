"""Structural reconciliation for restructured documents.

When a modified document is *restructured* (sections moved around, or existing
content newly wrapped in ``<CustomContent>`` tags), the incremental
section-update path mishandles it: moved sections get duplicated or dropped and
CustomContent tags become unbalanced.  Full re-translation fixes the structure
but throws away the existing translation of every unchanged paragraph.

This module rebuilds the target file in the new HEAD structure while *reusing*
the existing translation for any block whose source (English) content is
unchanged or merely moved.  The AI is invoked only for genuinely modified or
newly added blocks, so translation churn stays proportional to the real change.

The *reuse* unit and the *translation* unit are deliberately decoupled:

* Reuse is decided per **block** (blank-line separated, code fences atomic), so a
  small edit (one changed paragraph, or an added ``<CustomContent>`` tag line)
  leaves adjacent paragraphs, code blocks, and headings reused byte-for-byte.
  Unchanged code blocks are therefore never sent to the AI, which avoids the AI
  mangling them.
* Translation is done per **run** of consecutive changed blocks, and the model
  is given the whole **enclosing section** (heading-bounded) as read-only
  context.  This restores section-level context for accurate terminology and
  tone, while only the changed run is translated and spliced back; everything
  else stays reused.

Reuse keying
------------
Blocks are matched between the base and head source by normalized content
(per-line trailing whitespace and trailing blank lines ignored), so a moved or
merely re-wrapped block still matches.  The existing target translation is taken
to be structurally parallel to the base source. When the whole document has the
same block sequence, a matched base block maps to its target translation by
index. If document-level block counts differ, sections are aligned by headings;
unchanged/moved sections are reused whole, and block-level reuse is attempted
inside each changed section whose local source/target block counts still match.
"""

import re
from collections import defaultdict, deque
from difflib import SequenceMatcher

from ai_client import thread_safe_print
from file_adder import translate_file_batch
from translation_structure_validator import (
    HEADING_RE,
    compare_custom_content_structure,
    compare_heading_structure,
    extract_heading_levels,
    iter_markdown_content_lines,
)


_CUSTOM_CONTENT_LINE_RE = re.compile(r"^</?CustomContent\b[^<>]*>$")
_FENCE_RE = re.compile(r"^(```|~~~)")
_SPAN_TAG_RE = re.compile(
    r"<(?P<closing>/)?span\b(?P<attrs>[^>]*)>",
    re.IGNORECASE | re.DOTALL,
)
_CLASS_ATTR_RE = re.compile(
    r"\bclass\s*=\s*(['\"])(?P<classes>.*?)\1",
    re.IGNORECASE | re.DOTALL,
)
_VERSION_RE = re.compile(r"(?<![\w.])v?\d+(?:\.\d+){1,3}(?![\w.])", re.IGNORECASE)
_VERSION_MARK_PLACEHOLDER = "__VERSION_MARK_CONTENT__"


def _find_version_mark_span(content):
    """Return the sole balanced ``version-mark`` span in content.

    A small stack is used instead of an ``.*?</span>`` regex so nested spans do
    not truncate the outer version marker and leave unmatched closing tags when
    its inner text is replaced.
    """
    stack = []
    matches = []
    for tag in _SPAN_TAG_RE.finditer(content):
        if not tag.group("closing"):
            class_match = _CLASS_ATTR_RE.search(tag.group("attrs"))
            is_version_mark = bool(
                class_match
                and "version-mark" in class_match.group("classes").split()
            )
            stack.append((tag, is_version_mark))
            continue

        if not stack:
            return None
        opening, is_version_mark = stack.pop()
        if is_version_mark:
            matches.append(
                {
                    "inner": content[opening.end() : tag.start()],
                    "inner_start": opening.end(),
                    "inner_end": tag.start(),
                }
            )

    if stack:
        return None
    return matches[0] if len(matches) == 1 else None


def _replace_span_inner(content, span_match, replacement):
    return (
        content[: span_match["inner_start"]]
        + replacement
        + content[span_match["inner_end"] :]
    )


def _source_version_mark_only_change(head_content, base_content):
    """Describe a source change confined to one version-mark span."""
    if not head_content or not base_content:
        return None

    head_span = _find_version_mark_span(head_content)
    base_span = _find_version_mark_span(base_content)
    if not head_span or not base_span:
        return None
    if head_span["inner"] == base_span["inner"]:
        return None

    head_skeleton = _replace_span_inner(
        head_content, head_span, _VERSION_MARK_PLACEHOLDER
    )
    base_skeleton = _replace_span_inner(
        base_content, base_span, _VERSION_MARK_PLACEHOLDER
    )
    if _normalize_for_match(head_skeleton) != _normalize_for_match(base_skeleton):
        return None

    return {
        "head_inner": head_span["inner"],
        "base_inner": base_span["inner"],
    }


def _version_mark_only_change(head_content, base_content, target_content):
    """Describe a span-only change that can reuse the complete target section.

    The source comparison covers the complete supplied content, so callers can
    safely retain every byte of the existing target outside the span.
    """
    source_change = _source_version_mark_only_change(head_content, base_content)
    target_span = (
        _find_version_mark_span(target_content) if target_content else None
    )
    if not source_change or not target_span:
        return None
    return {
        **source_change,
        "target_inner": target_span["inner"],
        "target_span": target_span,
    }


def _deterministic_version_mark_inner(change):
    """Return a target span with only its version token changed, when safe."""
    base_versions = list(_VERSION_RE.finditer(change["base_inner"]))
    head_versions = list(_VERSION_RE.finditer(change["head_inner"]))
    if len(base_versions) != 1 or len(head_versions) != 1:
        return None

    base_match = base_versions[0]
    head_match = head_versions[0]
    base_skeleton = (
        change["base_inner"][: base_match.start()]
        + _VERSION_MARK_PLACEHOLDER
        + change["base_inner"][base_match.end() :]
    )
    head_skeleton = (
        change["head_inner"][: head_match.start()]
        + _VERSION_MARK_PLACEHOLDER
        + change["head_inner"][head_match.end() :]
    )
    if base_skeleton != head_skeleton:
        return None

    old_version = base_match.group(0)
    new_version = head_match.group(0)
    target_matches = [
        match
        for match in _VERSION_RE.finditer(change["target_inner"])
        if match.group(0) == old_version
    ]
    if len(target_matches) != 1:
        return None
    target_match = target_matches[0]
    return (
        change["target_inner"][: target_match.start()]
        + new_version
        + change["target_inner"][target_match.end() :]
    )


def reconcile_version_mark_only_change(
    head_content,
    base_content,
    target_content,
    ai_client,
    source_language,
    target_language,
    glossary_matcher=None,
    source_mode="",
):
    """Minimally apply a source change confined to a version-mark span.

    For a pure version-token bump, update the token deterministically without
    calling AI. For other span wording changes, translate only the span's inner
    text. In both cases every target byte outside that inner text is retained.
    Return ``None`` when the source change is not span-only or cannot be applied
    safely, allowing the caller to use its normal translation path.
    """
    change = _version_mark_only_change(head_content, base_content, target_content)
    if not change:
        return None

    translated_inner = _deterministic_version_mark_inner(change)
    if translated_inner is None:
        try:
            translated_inner = translate_file_batch(
                change["head_inner"],
                ai_client,
                source_language,
                target_language,
                glossary_matcher=glossary_matcher,
                source_mode=source_mode,
                context_reference=head_content,
                prior_translation_reference=change["target_inner"],
            ).strip()
        except Exception as exc:
            thread_safe_print(
                f"   ⚠️  Reconciler: version-mark span translation failed "
                f"({exc.__class__.__name__}); falling back to normal section translation"
            )
            return None
        # The model receives inner text, not markup. If it nevertheless emits a
        # span or changes a single-line marker into multiple lines, let the
        # caller use the normal guarded path instead of creating invalid HTML.
        if (
            not translated_inner
            or "<span" in translated_inner.lower()
            or "</span>" in translated_inner.lower()
            or ("\n" not in change["head_inner"] and "\n" in translated_inner)
        ):
            return None

    updated = _replace_span_inner(
        target_content,
        change["target_span"],
        translated_inner,
    )
    structure_issue = compare_heading_structure(
        "version-mark target section", target_content, updated
    ) or compare_custom_content_structure(
        "version-mark target section", target_content, updated
    )
    if structure_issue:
        return None
    return updated


def split_into_blocks(content):
    """Split markdown content into ordered, code-fence-aware blocks.

    ``''.join(split_into_blocks(content)) == content`` always holds.  A block is
    a run of consecutive non-blank lines (with fenced code blocks kept atomic)
    together with the blank line(s) that trail it.
    """
    if not content:
        return []

    lines = content.splitlines(keepends=True)
    n = len(lines)

    # Mark which lines are "content" lines. Lines inside a fenced code block are
    # always content (so blank lines inside a fence do not split the block).
    in_fence = False
    fence_delimiter = None
    is_content = [False] * n
    for index, raw_line in enumerate(lines):
        stripped = raw_line.strip()
        if not in_fence and (stripped.startswith("```") or stripped.startswith("~~~")):
            in_fence = True
            fence_delimiter = stripped[:3]
            is_content[index] = True
            continue
        if in_fence:
            is_content[index] = True
            if stripped.startswith(fence_delimiter):
                in_fence = False
                fence_delimiter = None
            continue
        is_content[index] = stripped != ""

    # A block starts at a content line whose predecessor is not a content line.
    run_starts = [
        index
        for index in range(n)
        if is_content[index] and (index == 0 or not is_content[index - 1])
    ]
    if not run_starts:
        return [content]

    blocks = []
    if run_starts[0] > 0:
        blocks.append("".join(lines[: run_starts[0]]))
    for position, start in enumerate(run_starts):
        end = run_starts[position + 1] if position + 1 < len(run_starts) else n
        blocks.append("".join(lines[start:end]))
    return blocks


def _normalize_for_match(block):
    """Normalize a block for content equality.

    Strips per-line trailing whitespace and trailing blank lines so that
    whitespace-only and trailing-newline differences do not force a needless
    re-translation, while genuine content changes are still detected.
    """
    normalized = block.replace("\r\n", "\n").replace("\r", "\n")
    stripped_lines = [line.rstrip() for line in normalized.split("\n")]
    while stripped_lines and stripped_lines[-1] == "":
        stripped_lines.pop()
    return "\n".join(stripped_lines)


def _trailing_newlines(block):
    """Return the trailing newline run of a block (separator to the next)."""
    stripped = block.rstrip("\r\n")
    return block[len(stripped):]


def _strip_trailing_newlines(block):
    """Return the block body without its trailing newline run."""
    return block.rstrip("\r\n")


def _block_is_heading(block):
    """Return True when a block's first non-blank line is a markdown heading."""
    for line in block.splitlines():
        if line.strip():
            # Keep section splitting identical to structure validation:
            # CommonMark permits up to three leading spaces and ATX headings
            # have at most six levels.
            return HEADING_RE.match(line) is not None
    return False


def _build_section_context(blocks):
    """Map each block to its enclosing heading-bounded section.

    Returns ``(section_ids, section_source)`` where ``section_ids[i]`` is the
    section id of block ``i`` and ``section_source[sid]`` is the full source text
    of that section.  Blocks before the first heading share section id 0.
    """
    section_ids = []
    current = 0
    for block in blocks:
        if _block_is_heading(block):
            current += 1
        section_ids.append(current)

    section_source = {}
    for block, sid in zip(blocks, section_ids):
        section_source[sid] = section_source.get(sid, "") + block
    return section_ids, section_source


def _build_prior_translation_lookup(base_blocks, target_blocks):
    """Map each base section to its existing target-language translation.

    Returns ``(by_heading, preamble_text)`` where ``by_heading`` maps a section's
    normalized heading text to that section's existing target translation, and
    ``preamble_text`` is the translation of the pre-heading preamble (or None).

    Relies on the base/target block parity already enforced by the caller, so
    ``target_blocks[i]`` is the translation of ``base_blocks[i]``.
    """
    base_section_ids, _ = _build_section_context(base_blocks)

    section_target_parts = {}
    section_heading = {}
    for index, (base_block, sid) in enumerate(zip(base_blocks, base_section_ids)):
        section_target_parts.setdefault(sid, []).append(target_blocks[index])
        if sid not in section_heading:
            section_heading[sid] = (
                _normalize_for_match(base_block) if _block_is_heading(base_block) else None
            )

    by_heading = {}
    preamble_text = None
    for sid, parts in section_target_parts.items():
        text = "".join(parts)
        heading = section_heading.get(sid)
        if heading is None:
            preamble_text = text
        else:
            by_heading.setdefault(heading, text)  # first occurrence wins on dupes
    return by_heading, preamble_text


def _is_non_translatable_block(block):
    """Return True for blocks that carry no natural-language text to translate.

    Fenced code blocks and CustomContent-tag-only blocks are emitted verbatim
    rather than sent to the AI: code must never be altered, and tag lines are
    immutable markup.  This protects newly added code/markup blocks (matched
    blocks are already reused without translation).
    """
    non_blank = [line.strip() for line in block.splitlines() if line.strip()]
    if not non_blank:
        return True
    if _FENCE_RE.match(non_blank[0]):
        return True
    return all(_CUSTOM_CONTENT_LINE_RE.match(line) for line in non_blank)


def _split_into_heading_sections(content):
    """Split content into flat heading-led sections while preserving bytes."""
    sections = []
    current = []
    for block in split_into_blocks(content):
        if _block_is_heading(block) and current:
            sections.append(current)
            current = []
        current.append(block)
    if current:
        sections.append(current)
    return sections


def _section_heading_key(section):
    """Return a source-language heading key, or None for a preamble section."""
    for block in section:
        if _block_is_heading(block):
            return _normalize_for_match(block)
    return None


def _section_heading_match_key(section):
    """Return a heading key stable across version-token changes only.

    Text such as ``New in`` and ``Deprecated in`` remains part of the key, so
    semantically different markers on otherwise identical headings do not
    collapse into one duplicate-heading group.
    """
    for block in section:
        if not _block_is_heading(block):
            continue
        span = _find_version_mark_span(block)
        if span:
            normalized_inner = _VERSION_RE.sub(
                _VERSION_MARK_PLACEHOLDER, span["inner"]
            )
            block = _replace_span_inner(block, span, normalized_inner)
        return _normalize_for_match(block)
    return None


def _section_heading_span_key(section):
    """Return a heading key ignoring all version-mark inner text."""
    for block in section:
        if not _block_is_heading(block):
            continue
        span = _find_version_mark_span(block)
        if span:
            block = _replace_span_inner(block, span, _VERSION_MARK_PLACEHOLDER)
        return _normalize_for_match(block)
    return None


def _has_version_mark_only_section_change(base_source, head_source):
    """Return True if any matched section changed only its version-mark span."""
    base_sections = _split_into_heading_sections(base_source)
    head_sections = _split_into_heading_sections(head_source)
    matches = _match_head_sections_to_base(base_sections, head_sections)
    for head_index, base_index in matches.items():
        if _source_version_mark_only_change(
            "".join(head_sections[head_index]),
            "".join(base_sections[base_index]),
        ):
            return True
    return False


def _repair_and_validate_translated_structure(source_content, translated_content):
    """Repair heading-level drift and reject non-repairable section output.

    Models occasionally preserve a heading but change ``##`` to ``###``. When
    the heading count is unchanged, restore the source level deterministically
    while retaining the translated title. Missing/extra headings or changed
    CustomContent structure cannot be repaired safely and return ``None``.
    """
    source_levels = extract_heading_levels(source_content)
    translated_headings = []
    for line_number, line in iter_markdown_content_lines(translated_content):
        match = HEADING_RE.match(line)
        if match:
            translated_headings.append((line_number, match))

    if len(source_levels) != len(translated_headings):
        return None

    repaired = translated_content
    translated_levels = [len(match.group(1)) for _, match in translated_headings]
    if source_levels != translated_levels:
        lines = translated_content.splitlines(keepends=True)
        for expected_level, (line_number, match) in zip(
            source_levels, translated_headings
        ):
            line_index = line_number - 1
            line = lines[line_index]
            lines[line_index] = (
                line[: match.start(1)]
                + ("#" * expected_level)
                + line[match.end(1) :]
            )
        repaired = "".join(lines)

    issue = compare_heading_structure(
        "section", source_content, repaired
    ) or compare_custom_content_structure("section", source_content, repaired)
    if issue:
        return None
    return repaired


def _translate_section_blocks(
    head_blocks,
    base_blocks,
    target_blocks,
    ai_client,
    source_language,
    target_language,
    glossary_matcher,
    source_mode,
):
    """Translate one changed section while reusing its unchanged blocks.

    ``base_blocks`` and ``target_blocks`` refer to the corresponding existing
    section.  When their local block counts match, unchanged paragraphs are
    reused byte-for-byte.  A local mismatch only falls back to translating this
    one section, never the complete document.
    """
    section_source = "".join(head_blocks)
    base_source = "".join(base_blocks)
    prior_translation = "".join(target_blocks) if target_blocks else None

    updated = reconcile_version_mark_only_change(
        section_source,
        base_source,
        prior_translation,
        ai_client,
        source_language,
        target_language,
        glossary_matcher=glossary_matcher,
        source_mode=source_mode,
    )
    if updated is not None:
        thread_safe_print(
            "   ♻️  Reconciler: minimally updated version-mark span and reused "
            "the rest of its section"
        )
        return _strip_trailing_newlines(updated) + _trailing_newlines(section_source)

    if not base_blocks or not target_blocks or len(base_blocks) != len(target_blocks):
        # Without local source/target block parity there is no deterministic,
        # language-independent way to splice individual translated paragraphs:
        # a translator may legitimately merge or split them. Limit the fallback
        # to this section and provide its existing translation for minimal-edit
        # wording, but do not duplicate section_source as read-only context.
        translated = translate_file_batch(
            section_source,
            ai_client,
            source_language,
            target_language,
            glossary_matcher=glossary_matcher,
            source_mode=source_mode,
            prior_translation_reference=prior_translation,
        )
        repaired = _repair_and_validate_translated_structure(
            section_source, translated
        )
        if repaired is None:
            heading = _section_heading_key(head_blocks) or "(preamble)"
            thread_safe_print(
                f"   ❌ Reconciler: AI output changed non-repairable structure "
                f"for section {heading}"
            )
        elif repaired != translated:
            heading = _section_heading_key(head_blocks) or "(preamble)"
            thread_safe_print(
                f"   🛠️  Reconciler: restored source heading level(s) for "
                f"section {heading}"
            )
        if repaired is None:
            return None
        # Models commonly omit trailing newlines. They are the separator to the
        # next heading, so restore the source section's exact newline run before
        # sections are joined; otherwise ``body### Next`` hides that heading.
        return _strip_trailing_newlines(repaired) + _trailing_newlines(
            section_source
        )

    base_index = defaultdict(deque)
    for base_position, block in enumerate(base_blocks):
        base_index[_normalize_for_match(block)].append(base_position)

    decisions = []
    for head_block in head_blocks:
        matches = base_index.get(_normalize_for_match(head_block))
        if matches:
            base_position = matches.popleft()
            reused_body = _strip_trailing_newlines(target_blocks[base_position])
            decisions.append(("reuse", reused_body + _trailing_newlines(head_block)))
        elif _is_non_translatable_block(head_block):
            decisions.append(("verbatim", head_block))
        else:
            decisions.append(("translate", head_block))

    output = []
    index = 0
    while index < len(decisions):
        kind, payload = decisions[index]
        if kind in ("reuse", "verbatim"):
            output.append(payload)
            index += 1
            continue

        end = index
        while end < len(decisions) and decisions[end][0] == "translate":
            end += 1
        run_source = "".join(head_blocks[index:end])
        translated = translate_file_batch(
            run_source,
            ai_client,
            source_language,
            target_language,
            glossary_matcher=glossary_matcher,
            source_mode=source_mode,
            context_reference=section_source,
            prior_translation_reference=prior_translation,
        )
        translated = _repair_and_validate_translated_structure(
            run_source, translated
        )
        if translated is None:
            heading = _section_heading_key(head_blocks) or "(preamble)"
            thread_safe_print(
                f"   ❌ Reconciler: AI output changed non-repairable structure "
                f"for translated run in section {heading}"
            )
            return None
        output.append(
            _strip_trailing_newlines(translated) + _trailing_newlines(run_source)
        )
        index = end

    return "".join(output)


def _section_similarity_text(section):
    """Return normalized section body text used to disambiguate duplicate headings."""
    if not section:
        return ""
    start = 1 if _block_is_heading(section[0]) else 0
    return _normalize_for_match("".join(section[start:]))


def _match_head_sections_to_base(base_sections, head_sections):
    """Match HEAD sections to BASE, handling duplicate heading titles safely.

    Exact heading keys are matched first. Duplicate exact headings reserve full
    section matches and then use source-body similarity. Relaxed version-marker
    matching is attempted only for a remaining unambiguous 1:1 pair, preventing
    semantically different or duplicated markers from being cross-paired.
    """
    assignments = {}
    unmatched_head = set(range(len(head_sections)))
    unmatched_base = set(range(len(base_sections)))

    def grouped(indices, sections, key_function):
        result = defaultdict(list)
        for index in sorted(indices):
            result[key_function(sections[index])].append(index)
        return result

    # Phase 1: retain the original exact-heading behavior. Within an exact
    # duplicate group, reserve unchanged sections before similarity matching.
    base_groups = grouped(unmatched_base, base_sections, _section_heading_key)
    head_groups = grouped(unmatched_head, head_sections, _section_heading_key)
    for heading_key, head_indices in head_groups.items():
        base_indices = base_groups.get(heading_key, [])
        if not base_indices:
            continue

        group_head = set(head_indices)
        group_base = set(base_indices)
        for head_index in head_indices:
            head_text = _normalize_for_match("".join(head_sections[head_index]))
            exact_base = next(
                (
                    base_index
                    for base_index in base_indices
                    if base_index in group_base
                    and _normalize_for_match("".join(base_sections[base_index]))
                    == head_text
                ),
                None,
            )
            if exact_base is not None:
                assignments[head_index] = exact_base
                group_head.remove(head_index)
                group_base.remove(exact_base)

        scored_pairs = []
        for head_index in group_head:
            head_body = _section_similarity_text(head_sections[head_index])
            for base_index in group_base:
                base_body = _section_similarity_text(base_sections[base_index])
                score = SequenceMatcher(
                    None, head_body, base_body, autojunk=False
                ).ratio()
                scored_pairs.append((-score, head_index, base_index))

        for _, head_index, base_index in sorted(scored_pairs):
            if head_index not in group_head or base_index not in group_base:
                continue
            assignments[head_index] = base_index
            group_head.remove(head_index)
            group_base.remove(base_index)

        unmatched_head.difference_update(set(head_indices) - group_head)
        unmatched_base.difference_update(set(base_indices) - group_base)

    def match_unambiguous_relaxed(key_function, require_span_only=False):
        base_relaxed = grouped(unmatched_base, base_sections, key_function)
        head_relaxed = grouped(unmatched_head, head_sections, key_function)
        for heading_key, head_indices in head_relaxed.items():
            base_indices = base_relaxed.get(heading_key, [])
            if heading_key is None or len(head_indices) != 1 or len(base_indices) != 1:
                continue
            head_index = head_indices[0]
            base_index = base_indices[0]
            if require_span_only and not _source_version_mark_only_change(
                "".join(head_sections[head_index]),
                "".join(base_sections[base_index]),
            ):
                continue
            assignments[head_index] = base_index
            unmatched_head.remove(head_index)
            unmatched_base.remove(base_index)

    # Phase 2 keeps marker semantics and ignores only version tokens. Phase 3
    # supports non-version wording changes only when the complete section proves
    # that the span inner text is the sole change and the pairing is unique.
    match_unambiguous_relaxed(_section_heading_match_key)
    match_unambiguous_relaxed(
        _section_heading_span_key,
        require_span_only=True,
    )

    return assignments


def _reconcile_by_heading_sections(
    source_file_path,
    head_source,
    base_source,
    existing_target,
    ai_client,
    repo_config,
    glossary_matcher,
    source_mode,
):
    """Reconcile a structurally parallel translation with block-count drift.

    Base and target sections are paired by heading position (their translated
    titles naturally differ). HEAD sections are then matched to base sections
    by exact source headings first. Version-marker matching is relaxed only for
    unambiguous remaining pairs, allowing moved or version-bumped sections to
    retain existing target text without risking duplicate-heading cross-pairs.
    """
    heading_issue = compare_heading_structure(
        source_file_path, base_source, existing_target
    )
    if heading_issue:
        thread_safe_print(
            f"   ⚠️  Reconciler: base/target heading structure mismatch "
            f"({heading_issue.reason}); cannot align sections safely"
        )
        return None

    base_sections = _split_into_heading_sections(base_source)
    target_sections = _split_into_heading_sections(existing_target)
    head_sections = _split_into_heading_sections(head_source)
    if len(base_sections) != len(target_sections):
        thread_safe_print(
            f"   ⚠️  Reconciler: base/target section count mismatch "
            f"({len(base_sections)} vs {len(target_sections)}); cannot align safely"
        )
        return None

    section_matches = _match_head_sections_to_base(base_sections, head_sections)

    source_language = repo_config.get("source_language", "English")
    target_language = repo_config.get("target_language", "Chinese")
    output_sections = []
    reused_sections = 0
    changed_sections = 0
    new_sections = 0

    for head_index, head_section in enumerate(head_sections):
        base_index = section_matches.get(head_index)
        if base_index is not None:
            base_section = base_sections[base_index]
            target_section = target_sections[base_index]
            if _normalize_for_match("".join(head_section)) == _normalize_for_match(
                "".join(base_section)
            ):
                output_sections.append("".join(target_section))
                reused_sections += 1
            else:
                translated_section = _translate_section_blocks(
                    head_section,
                    base_section,
                    target_section,
                    ai_client,
                    source_language,
                    target_language,
                    glossary_matcher,
                    source_mode,
                )
                if translated_section is None:
                    return None
                output_sections.append(translated_section)
                changed_sections += 1
        else:
            translated_section = _translate_section_blocks(
                head_section,
                [],
                [],
                ai_client,
                source_language,
                target_language,
                glossary_matcher,
                source_mode,
            )
            if translated_section is None:
                return None
            output_sections.append(translated_section)
            new_sections += 1

    reconciled = "".join(output_sections)
    thread_safe_print(
        f"   ♻️  Reconciler: reused {reused_sections} unchanged/moved section(s), "
        f"updated {changed_sections} changed section(s), translated "
        f"{new_sections} new section(s)"
    )
    return reconciled


def reconcile_restructured_file(
    source_file_path,
    head_source,
    base_source,
    existing_target,
    ai_client,
    repo_config,
    glossary_matcher=None,
    source_mode="",
):
    """Rebuild the target translation for a restructured file with reuse.

    Returns the reconciled target content as a string, or ``None`` when
    reconciliation preconditions are not met or the output fails a structure
    self-check against ``head_source``. The caller decides whether a missing
    target can use full translation or an existing target must be preserved.
    """
    if not head_source or not base_source or not existing_target:
        return None

    base_blocks = split_into_blocks(base_source)
    target_blocks = split_into_blocks(existing_target)
    head_blocks = split_into_blocks(head_source)

    # Prefer exact block parity because it supports byte-for-byte reuse at the
    # finest granularity.  Translation files often have harmless paragraph or
    # blank-line drift, though; in that case align heading sections instead of
    # falling back to a destructive full-file translation.
    block_mismatch = len(base_blocks) != len(target_blocks)
    version_mark_only_change = (
        not block_mismatch
        and _has_version_mark_only_section_change(base_source, head_source)
    )
    if block_mismatch or version_mark_only_change:
        if block_mismatch:
            thread_safe_print(
                f"   ⚠️  Reconciler: base/target block count mismatch "
                f"({len(base_blocks)} vs {len(target_blocks)}); aligning by heading section"
            )
        else:
            thread_safe_print(
                "   ♻️  Reconciler: version-mark-only heading change; aligning "
                "by section for a minimal update"
            )
        reconciled = _reconcile_by_heading_sections(
            source_file_path,
            head_source,
            base_source,
            existing_target,
            ai_client,
            repo_config,
            glossary_matcher,
            source_mode,
        )
        if reconciled is None:
            return None

        issue = compare_custom_content_structure(
            source_file_path, head_source, reconciled
        ) or compare_heading_structure(source_file_path, head_source, reconciled)
        if issue:
            detail = f": {issue.first_difference}" if issue.first_difference else ""
            thread_safe_print(
                f"   ⚠️  Reconciler: section-aligned output failed structure "
                f"self-check ({issue.reason}{detail})"
            )
            return None
        return reconciled

    source_language = repo_config.get("source_language", "English")
    target_language = repo_config.get("target_language", "Chinese")

    section_ids, section_source = _build_section_context(head_blocks)

    # Heading key per head section, so each section can be matched to its base
    # counterpart's existing translation for minimal-edit reuse of wording.
    head_section_heading = {}
    for block, sid in zip(head_blocks, section_ids):
        if sid not in head_section_heading:
            head_section_heading[sid] = (
                _normalize_for_match(block) if _block_is_heading(block) else None
            )
    prior_by_heading, prior_preamble = _build_prior_translation_lookup(
        base_blocks, target_blocks
    )

    def prior_translation_for(sid):
        heading = head_section_heading.get(sid)
        if heading is None:
            return prior_preamble
        return prior_by_heading.get(heading)

    # Index base blocks by normalized source content; a deque per key handles
    # duplicate content (consume one occurrence per match, in order).
    base_index = defaultdict(deque)
    for base_position, block in enumerate(base_blocks):
        base_index[_normalize_for_match(block)].append(base_position)

    # First pass: decide per block whether to reuse, keep verbatim, or translate.
    # ('reuse', text) | ('verbatim', text) | ('translate', source_block)
    decisions = []
    for head_block in head_blocks:
        matches = base_index.get(_normalize_for_match(head_block))
        if matches:
            base_position = matches.popleft()
            reused_body = _strip_trailing_newlines(target_blocks[base_position])
            decisions.append(("reuse", reused_body + _trailing_newlines(head_block)))
        elif _is_non_translatable_block(head_block):
            # New code / markup block: keep it exactly as in HEAD source.
            decisions.append(("verbatim", head_block))
        else:
            decisions.append(("translate", head_block))

    # Second pass: emit blocks in order, translating each run of consecutive
    # changed blocks (within one section) as a unit, with the enclosing section
    # as read-only context.
    reused_count = sum(1 for kind, _ in decisions if kind == "reuse")
    verbatim_count = sum(1 for kind, _ in decisions if kind == "verbatim")
    translated_count = 0
    run_count = 0
    output_blocks = []

    index = 0
    total = len(head_blocks)
    while index < total:
        kind, payload = decisions[index]
        if kind in ("reuse", "verbatim"):
            output_blocks.append(payload)
            index += 1
            continue

        # Gather a run of consecutive translatable blocks in the same section.
        section_id = section_ids[index]
        end = index
        while (
            end < total
            and decisions[end][0] == "translate"
            and section_ids[end] == section_id
        ):
            end += 1

        run_source = "".join(head_blocks[index:end])
        translated = translate_file_batch(
            run_source,
            ai_client,
            source_language,
            target_language,
            glossary_matcher=glossary_matcher,
            source_mode=source_mode,
            context_reference=section_source.get(section_id),
            prior_translation_reference=prior_translation_for(section_id),
        )
        output_blocks.append(
            _strip_trailing_newlines(translated) + _trailing_newlines(run_source)
        )
        translated_count += end - index
        run_count += 1
        index = end

    reconciled = "".join(output_blocks)

    thread_safe_print(
        f"   ♻️  Reconciler: reused {reused_count} block(s), "
        f"translated {translated_count} block(s) in {run_count} run(s) with section context, "
        f"kept {verbatim_count} code/markup block(s) verbatim"
    )

    # Structure self-check against HEAD. The caller applies the safe policy for
    # a declined reconciliation.
    issue = compare_custom_content_structure(
        source_file_path, head_source, reconciled
    ) or compare_heading_structure(source_file_path, head_source, reconciled)
    if issue:
        thread_safe_print(
            f"   ⚠️  Reconciler: output failed structure self-check "
            f"({issue.reason}); reconciliation declined"
        )
        return None

    return reconciled
