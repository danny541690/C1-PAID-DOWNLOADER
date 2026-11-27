import unittest

from scripts.r11_backer.integration_pilot import digest, encode
from scripts.r11_backer.v2_coverage import bounded_shards, query_key, subtract_coverage
from scripts.r11_backer.v2_registry import NativeContract, validate_original, validate_pages


class V2ContractsTests(unittest.TestCase):
    def setUp(self):
        self.rule = NativeContract("Fixture", "daily", "entity_range", ("date", "code", "kind"),
                                   ("date", "code", "kind"), identity_field="code")
        self.params = {"dataset": "Fixture", "data_id": "001", "start_date": "1999-12-30",
                       "end_date": "2000-01-03"}
        self.rows = [{"date": "1999-12-30", "code": "001", "kind": key} for key in ("B", "A")]
        self.completeness = {"method": "expected_keys", "keys": [list(item.values()) for item in self.rows]}

    def test_multirow_native_receipt_and_lossless_order(self):
        raw = encode({"status": 200, "data": self.rows})
        proof = validate_original(self.rule, self.params, raw, completeness=self.completeness)
        self.assertEqual(proof["rows"], 2)
        self.assertEqual(proof["raw_sha"], digest(raw))
        self.assertEqual(proof["first_sha"], digest(encode(self.rows[0])))
        self.assertEqual(proof["last_sha"], digest(encode(self.rows[-1])))
        with self.assertRaisesRegex(ValueError, "INCOMPLETE_EXPECTED_KEYS"):
            validate_original(self.rule, self.params, encode({"status": 200, "data": self.rows[:1]}),
                              completeness=self.completeness)

    def test_closed_pagination_with_full_set_proof(self):
        from dataclasses import replace
        rule = replace(self.rule, page_field="next_page")
        first = encode({"status": 200, "data": self.rows[:1], "next_page": "two"})
        last = encode({"status": 200, "data": self.rows[1:], "next_page": None})
        pages = ((self.params, first), (self.params | {"page": "two"}, last))
        self.assertEqual(validate_pages(rule, pages, completeness=self.completeness)["rows"], 2)
        with self.assertRaisesRegex(ValueError, "INCOMPLETE_PAGINATION"):
            validate_pages(rule, pages[:1], completeness=self.completeness)

    def test_hashes_partitions_and_venue_aliases(self):
        subsets = subtract_coverage("1999-12-01", "2000-01-04", [
            ("1999-12-04", "1999-12-10"), ("1999-12-06", "2000-01-01"),
            ("2000-01-03", "2000-03-10"),
        ])
        self.assertEqual(subsets, [["1999-12-01", "1999-12-03"], ["2000-01-02", "2000-01-02"]])
        self.assertEqual(query_key(self.params, "entity_range", market="TWSE"),
                         query_key(self.params, "symbol_range", market="TPEx"))
        self.assertNotEqual(query_key(self.params, "entity_range", market="TWSE", market_specific=True),
                            query_key(self.params, "entity_range", market="TPEx", market_specific=True))
        items = [{"value": "x" * 1_000_000}] * 5
        self.assertEqual([len(b) for b in bounded_shards(items)], [4, 1])
