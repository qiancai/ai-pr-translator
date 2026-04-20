import sys
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from svg_preprocessor import (
    strip_svgs,
    restore_svgs,
    strip_svgs_from_dict,
    restore_svgs_in_dict,
    strip_svgs_from_sections_and_diff,
    merge_svg_maps,
)


SMALL_SVG = '<svg width="1em" height="1em" viewBox="0 0 16 16" fill="none"></svg>'
LARGE_SVG = (
    '<svg width="1.1em" height="1.1em" viewBox="0 0 24 24" fill="none" '
    'xmlns="http://www.w3.org/2000/svg" stroke-width="0.7" '
    'class="tiui-icon DProject">'
    '<path d="M11.8845 4.76892L11.2136 5.10433"/>'
    "</svg>"
)
SELF_CLOSING_SVG = '<svg width="1em" height="1em" viewBox="0 0 16 16" />'


class TestStripSvgs(unittest.TestCase):

    def test_no_svgs(self):
        text = "Hello world with **markdown** and `code`."
        cleaned, svg_map = strip_svgs(text)
        self.assertEqual(cleaned, text)
        self.assertEqual(svg_map, {})

    def test_single_svg(self):
        text = f"Click {SMALL_SVG} to delete."
        cleaned, svg_map = strip_svgs(text)
        self.assertNotIn("<svg", cleaned)
        self.assertIn("MDSvgIcon", cleaned)
        self.assertEqual(len(svg_map), 1)

    def test_multiple_distinct_svgs(self):
        text = f"Icon A: {SMALL_SVG} and icon B: {LARGE_SVG}."
        cleaned, svg_map = strip_svgs(text)
        self.assertEqual(len(svg_map), 2)
        self.assertNotIn("<svg", cleaned)

    def test_duplicate_svgs_share_placeholder(self):
        text = f"First {SMALL_SVG} then {SMALL_SVG} again."
        cleaned, svg_map = strip_svgs(text)
        self.assertEqual(len(svg_map), 1)
        placeholder = list(svg_map.keys())[0]
        self.assertEqual(cleaned.count(placeholder), 2)

    def test_self_closing_svg(self):
        text = f"Here: {SELF_CLOSING_SVG} done."
        cleaned, svg_map = strip_svgs(text)
        self.assertNotIn("<svg", cleaned)
        self.assertEqual(len(svg_map), 1)

    def test_empty_and_none(self):
        self.assertEqual(strip_svgs(""), ("", {}))
        self.assertEqual(strip_svgs(None), (None, {}))

    def test_multiline_svg(self):
        svg = '<svg width="1em"\nheight="1em"\nviewBox="0 0 16 16">\n<path d="M0 0"/>\n</svg>'
        text = f"Before\n{svg}\nAfter"
        cleaned, svg_map = strip_svgs(text)
        self.assertNotIn("<svg", cleaned)
        self.assertIn("Before", cleaned)
        self.assertIn("After", cleaned)
        self.assertEqual(len(svg_map), 1)


class TestRestoreSvgs(unittest.TestCase):

    def test_roundtrip(self):
        original = f"Click {SMALL_SVG} to delete {LARGE_SVG}."
        cleaned, svg_map = strip_svgs(original)
        restored = restore_svgs(cleaned, svg_map)
        self.assertEqual(restored, original)

    def test_no_placeholders(self):
        text = "No placeholders here."
        self.assertEqual(restore_svgs(text, {}), text)

    def test_empty_and_none(self):
        self.assertEqual(restore_svgs("", {}), "")
        self.assertIsNone(restore_svgs(None, {}))


class TestDictOperations(unittest.TestCase):

    def test_strip_and_restore_dict(self):
        d = {
            "section_1": f"Text with {SMALL_SVG} icon.",
            "section_2": f"Two icons: {SMALL_SVG} and {LARGE_SVG}.",
            "section_3": "No SVG here.",
        }
        cleaned, svg_map = strip_svgs_from_dict(d)
        self.assertNotIn("<svg", cleaned["section_1"])
        self.assertNotIn("<svg", cleaned["section_2"])
        self.assertEqual(cleaned["section_3"], d["section_3"])

        restored = restore_svgs_in_dict(cleaned, svg_map)
        self.assertEqual(restored, d)

    def test_non_string_values_preserved(self):
        d = {"a": 42, "b": None, "c": f"svg: {SMALL_SVG}"}
        cleaned, svg_map = strip_svgs_from_dict(d)
        self.assertEqual(cleaned["a"], 42)
        self.assertIsNone(cleaned["b"])
        restored = restore_svgs_in_dict(cleaned, svg_map)
        self.assertEqual(restored["c"], d["c"])

    def test_empty_dict(self):
        cleaned, svg_map = strip_svgs_from_dict({})
        self.assertEqual(cleaned, {})
        self.assertEqual(svg_map, {})


class TestStripSectionsAndDiff(unittest.TestCase):

    def test_shared_svg_across_source_target_diff(self):
        source = {"s1": f"Click {SMALL_SVG} to edit."}
        target = {"s1": f"点击 {SMALL_SVG} 进行编辑。"}
        diff = f"+Click {SMALL_SVG} to edit."

        cs, ct, cd, svg_map = strip_svgs_from_sections_and_diff(source, target, diff)

        self.assertEqual(len(svg_map), 1)
        placeholder = list(svg_map.keys())[0]
        self.assertIn(placeholder, cs["s1"])
        self.assertIn(placeholder, ct["s1"])
        self.assertIn(placeholder, cd)

    def test_unique_svgs_across_inputs(self):
        source = {"s1": f"Icon: {SMALL_SVG}"}
        target = {"s1": f"图标: {LARGE_SVG}"}
        diff = ""

        cs, ct, cd, svg_map = strip_svgs_from_sections_and_diff(source, target, diff)
        self.assertEqual(len(svg_map), 2)

    def test_roundtrip_sections_and_diff(self):
        source = {"s1": f"A {SMALL_SVG} B", "s2": f"C {LARGE_SVG} D"}
        target = {"s1": f"甲 {SMALL_SVG} 乙", "s2": f"丙 {LARGE_SVG} 丁"}
        diff = f"+A {SMALL_SVG} B\n-old line\n+C {LARGE_SVG} D"

        cs, ct, cd, svg_map = strip_svgs_from_sections_and_diff(source, target, diff)

        rs = restore_svgs_in_dict(cs, svg_map)
        rt = restore_svgs_in_dict(ct, svg_map)
        rd = restore_svgs(cd, svg_map)

        self.assertEqual(rs, source)
        self.assertEqual(rt, target)
        self.assertEqual(rd, diff)

    def test_none_inputs(self):
        cs, ct, cd, svg_map = strip_svgs_from_sections_and_diff(None, None, None)
        self.assertEqual(cs, {})
        self.assertEqual(ct, {})
        self.assertIsNone(cd)
        self.assertEqual(svg_map, {})


class TestMergeSvgMaps(unittest.TestCase):

    def test_merge_disjoint(self):
        m1 = {'<MDSvgIcon name="icon-00001" />': "<svg>a</svg>"}
        m2 = {'<MDSvgIcon name="icon-00002" />': "<svg>b</svg>"}
        merged = merge_svg_maps(m1, m2)
        self.assertEqual(len(merged), 2)

    def test_merge_same_key_same_value(self):
        m1 = {'<MDSvgIcon name="icon-00001" />': "<svg>a</svg>"}
        m2 = {'<MDSvgIcon name="icon-00001" />': "<svg>a</svg>"}
        merged = merge_svg_maps(m1, m2)
        self.assertEqual(len(merged), 1)

    def test_merge_conflict_raises(self):
        m1 = {'<MDSvgIcon name="icon-00001" />': "<svg>a</svg>"}
        m2 = {'<MDSvgIcon name="icon-00001" />': "<svg>different</svg>"}
        with self.assertRaises(ValueError):
            merge_svg_maps(m1, m2)


if __name__ == "__main__":
    unittest.main()
