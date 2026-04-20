"""
SVG Preprocessor for AI Translation Pipeline

Replaces inline SVG tags with lightweight <MDSvgIcon name="icon-XXXXX" /> placeholders
before sending content to AI, then restores the original SVGs after translation.

This avoids wasting tokens on large SVG blobs that do not need translation and
prevents truncation / JSON parse errors in AI responses.
"""

import re

SVG_PATTERN = re.compile(r'<svg\b[^>]*(?:/>|>.*?</svg>)', re.DOTALL)

PLACEHOLDER_TEMPLATE = '<MDSvgIcon name="icon-{id:05d}" />'

PLACEHOLDER_RE = re.compile(r'<MDSvgIcon\s+name="icon-(\d{5})"\s*/>')


def strip_svgs(text):
    """Replace every SVG tag in *text* with a numbered placeholder.

    Returns (cleaned_text, svg_map) where *svg_map* maps placeholder strings
    to the original SVG markup they replaced.  Identical SVG strings share the
    same placeholder.
    """
    if not text:
        return text, {}

    seen = {}
    svg_map = {}
    counter = [0]

    def _replacer(match):
        svg = match.group(0)
        if svg in seen:
            return seen[svg]
        counter[0] += 1
        placeholder = PLACEHOLDER_TEMPLATE.format(id=counter[0])
        seen[svg] = placeholder
        svg_map[placeholder] = svg
        return placeholder

    cleaned = SVG_PATTERN.sub(_replacer, text)
    return cleaned, svg_map


def restore_svgs(text, svg_map):
    """Replace every placeholder in *text* with its original SVG from *svg_map*."""
    if not text or not svg_map:
        return text

    def _replacer(match):
        full = match.group(0)
        return svg_map.get(full, full)

    return PLACEHOLDER_RE.sub(_replacer, text)


def strip_svgs_from_dict(d):
    """Strip SVGs from all string values in a dict.

    Returns (cleaned_dict, svg_map).  The svg_map is a merged mapping from
    placeholders to original SVGs across all values and is safe to pass to
    ``restore_svgs_in_dict``.
    """
    if not d:
        return d, {}

    merged_svg_map = {}
    reverse_lookup = {}
    counter = [0]
    cleaned = {}

    for key, value in d.items():
        if not isinstance(value, str):
            cleaned[key] = value
            continue

        parts = []
        last_end = 0
        for m in SVG_PATTERN.finditer(value):
            svg = m.group(0)
            parts.append(value[last_end:m.start()])
            if svg in reverse_lookup:
                placeholder = reverse_lookup[svg]
            else:
                counter[0] += 1
                placeholder = PLACEHOLDER_TEMPLATE.format(id=counter[0])
                reverse_lookup[svg] = placeholder
                merged_svg_map[placeholder] = svg
            parts.append(placeholder)
            last_end = m.end()
        parts.append(value[last_end:])
        cleaned[key] = "".join(parts)

    return cleaned, merged_svg_map


def restore_svgs_in_dict(d, svg_map):
    """Restore SVG placeholders in all string values of *d*."""
    if not d or not svg_map:
        return d

    restored = {}
    for key, value in d.items():
        if isinstance(value, str):
            restored[key] = restore_svgs(value, svg_map)
        else:
            restored[key] = value
    return restored


def merge_svg_maps(*maps):
    """Merge multiple svg_maps into one, raising on conflicting placeholders."""
    merged = {}
    for m in maps:
        for placeholder, svg in m.items():
            if placeholder in merged:
                if merged[placeholder] != svg:
                    raise ValueError(
                        f"Conflicting SVG for placeholder {placeholder}"
                    )
            else:
                merged[placeholder] = svg
    return merged


def strip_svgs_from_sections_and_diff(source_sections, target_sections, pr_diff):
    """One-call convenience: strip SVGs from source dict, target dict, and diff string.

    Returns (clean_source, clean_target, clean_diff, svg_map).
    The shared *svg_map* uses a global counter so placeholders are unique across
    all three inputs.
    """
    clean_source, map1 = strip_svgs_from_dict(source_sections or {})
    offset = max((int(PLACEHOLDER_RE.search(p).group(1)) for p in map1), default=0)

    clean_target = {}
    map2 = {}
    if target_sections:
        reverse_lookup = {v: k for k, v in map1.items()}
        counter = [offset]
        for key, value in target_sections.items():
            if not isinstance(value, str):
                clean_target[key] = value
                continue
            parts = []
            last_end = 0
            for m in SVG_PATTERN.finditer(value):
                svg = m.group(0)
                parts.append(value[last_end:m.start()])
                if svg in reverse_lookup:
                    placeholder = reverse_lookup[svg]
                else:
                    counter[0] += 1
                    placeholder = PLACEHOLDER_TEMPLATE.format(id=counter[0])
                    reverse_lookup[svg] = placeholder
                    map2[placeholder] = svg
                parts.append(placeholder)
                last_end = m.end()
            parts.append(value[last_end:])
            clean_target[key] = "".join(parts)

    offset2 = max(
        (int(PLACEHOLDER_RE.search(p).group(1)) for p in {**map1, **map2}),
        default=0,
    )

    clean_diff = pr_diff
    map3 = {}
    if pr_diff and isinstance(pr_diff, str):
        reverse_lookup_all = {v: k for k, v in {**map1, **map2}.items()}
        counter = [offset2]
        parts = []
        last_end = 0
        for m in SVG_PATTERN.finditer(pr_diff):
            svg = m.group(0)
            parts.append(pr_diff[last_end:m.start()])
            if svg in reverse_lookup_all:
                placeholder = reverse_lookup_all[svg]
            else:
                counter[0] += 1
                placeholder = PLACEHOLDER_TEMPLATE.format(id=counter[0])
                reverse_lookup_all[svg] = placeholder
                map3[placeholder] = svg
            parts.append(placeholder)
            last_end = m.end()
        parts.append(pr_diff[last_end:])
        clean_diff = "".join(parts)

    svg_map = merge_svg_maps(map1, map2, map3)
    return clean_source, clean_target, clean_diff, svg_map
