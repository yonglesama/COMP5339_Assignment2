"""Unit tests for member B's cleaning stage.  Run from the project root:

    python -m unittest discover -s tests -v          (no extra packages needed)
    python -m pytest tests                            (if pytest is installed)
"""
import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("cleaning_run", ROOT / "src" / "cleaning" / "run.py")
run = importlib.util.module_from_spec(_spec)
sys.modules["cleaning_run"] = run          # needed by @dataclass when loading by path
_spec.loader.exec_module(run)

RAW_CSV = Path(os.environ.get("EV_RAW_CSV", ROOT / run.RAW_NSW_CSV))
SA4_SHP = Path(os.environ.get("SA4_SHP", ROOT / run.RAW_SA4_SHP))


def make_raw(*rows):
    """rows: dicts with any of the raw column names; missing ones become ''."""
    data = [{c: r.get(c, "") for c in run.RAW_COLUMNS} for r in rows]
    return pd.DataFrame(data, dtype=str)


def rec(**kw):
    base = dict(Station_name="", Station_address="1 Test St, Sydney, 2000", Operator="Evie", Number_of_plugs="2",
                Charger_Type="DC", Charger_rating="50 kW", Latitude="-33.8", Longitude="151.2",
                LGANAME="Test Council", PCODE="2000", Source="Existing Fast Chargers")
    base.update(kw)
    return base


# 0.0001 deg of latitude is about 11.1 m
def lat(offset_m: float) -> str:
    return f"{-33.8 - offset_m / 111_195:.7f}"


class TextHelpers(unittest.TestCase):
    def test_clean_text(self):
        self.assertEqual(run.clean_text("  a\n  b\u00a0c "), "a b c")
        self.assertIsNone(run.clean_text("   "))
        self.assertIsNone(run.clean_text(None))

    def test_normalise_address(self):
        n = run.normalise_address
        self.assertEqual(n("8 Holtermann St\nCrows Nest NSW 2065\nAustralia"), "8 Holtermann St, Crows Nest NSW 2065")
        self.assertEqual(n("10 Victoria St, Wollongong , NSW 2500"), "10 Victoria St, Wollongong, NSW 2500")
        self.assertEqual(n(", Muswellbrook, 2333"), "Muswellbrook, 2333")
        self.assertEqual(n("Berowra Railway Station, Pacific Highway, Berowra, New South Wales 2081, Australia"),
                         "Berowra Railway Station, Pacific Highway, Berowra, NSW 2081")
        self.assertEqual(n("129 Pacific Hwy Ourimbah NSW 2258 Australia"), "129 Pacific Hwy Ourimbah NSW 2258")

    def test_extract_postcode(self):
        e = run.extract_postcode
        self.assertEqual(e("2260 Camden Valley Way, Sydney, 2170"), "2170")      # street number is not the postcode
        self.assertEqual(e("Stoney Creek Road Car Park, 497 Forest Rd, Bexley, 2207, NSW"), "2207")
        self.assertEqual(e("10 Victoria St, Wollongong, NSW 2500"), "2500")
        self.assertIsNone(e("1A Keith St Dulwich Hill NSW"))
        self.assertIsNone(e(None))

    def test_normalise_operator(self):
        self.assertEqual(run.normalise_operator("Tesla Motors "), ("Tesla", False))
        self.assertEqual(run.normalise_operator("Non-Networked"), ("Non-networked", False))
        self.assertEqual(run.normalise_operator("Viva Energy A"), ("Viva Energy Australia", True))
        self.assertEqual(run.normalise_operator("University of", "University of Wollongong, Northfields Avenue"),
                         ("University of Wollongong", True))
        self.assertEqual(run.normalise_operator("Exploren"), ("Exploren", False))

    def test_parse_rating(self):
        p = run.parse_rating
        self.assertEqual(p("22 kW")["kw_max"], 22.0)
        self.assertEqual(p("22")["text"], "22 kW")
        self.assertIsNone(p("AC"))                                   # placeholder, not a rating
        mixed = p("2x350kW & 2x175kW")
        self.assertEqual((mixed["kw_max"], mixed["mixed"], mixed["plugs_in_text"]), (350.0, True, 4))
        self.assertEqual(mixed["text"], "2x350 kW & 2x175 kW")
        self.assertIsNone(p("22 kW")["plugs_in_text"])                # no multiplier -> no plug count claimed


class Rules(unittest.TestCase):
    def clean(self, *rows):
        return run.run_cleaning(make_raw(*rows))

    def test_upcoming_and_bad_coordinates_are_excluded_with_reason(self):
        res = self.clean(rec(), rec(Charger_Type="Upcoming", Latitude=lat(500)), rec(Latitude="0", Longitude="151.2"),
                         rec(Latitude="33.8"))
        self.assertEqual(len(res.clean), 1)
        self.assertEqual(sorted(res.excluded["reason"]),
                         ["coordinates_swapped_or_wrong_hemisphere", "coordinates_zero", "upcoming_not_installed"])

    def test_cross_programme_duplicate_is_merged_and_traceable(self):
        res = self.clean(
            rec(Operator="Evie", Charger_rating="50 kW", Source="Existing Fast Chargers"),
            rec(Operator="Evie Networks", Charger_rating="50", Latitude=lat(17), Source="Kerbside Charging R1"),
            rec(Operator="Evie Networks ", Charger_rating="50", Latitude=lat(17), Source=""),
        )
        self.assertEqual(len(res.clean), 1)
        row = res.clean.iloc[0]
        self.assertEqual(row.operator_name, "Evie")
        self.assertEqual(row.source_rows, "2;3;4")
        self.assertEqual(row.merged_record_count, 3)
        self.assertEqual(len(res.excluded), 2)
        self.assertTrue((res.excluded["reason"] == "duplicate_merged").all())

    def test_same_programme_close_records_are_not_merged(self):
        res = self.clean(rec(Operator="PLUS ES", Number_of_plugs="1", Charger_Type="AC", Charger_rating="22 kW",
                             Source="Kerbside Charging R1"),
                         rec(Operator="PLUS ES", Number_of_plugs="1", Charger_Type="AC", Charger_rating="22 kW",
                             Latitude=lat(20), Source="Kerbside Charging R1"))
        self.assertEqual(len(res.clean), 2)

    def test_beyond_threshold_is_kept_but_reviewed(self):
        res = self.clean(rec(Source="Existing Fast Chargers"),
                         rec(Station_address="9 Other St, Sydney, 2000", Latitude=lat(60), Source="Kerbside Charging R1"))
        self.assertEqual(len(res.clean), 2)
        self.assertEqual(list(res.review["reason"]), ["beyond_merge_distance"])

    def test_same_address_is_merged_up_to_100m_but_different_address_is_not(self):
        same = self.clean(rec(Station_address="14 Macauley St, Denman NSW 2328", Source="Existing Fast Chargers"),
                          rec(Station_address="14 Macauley St, Denman, 2328", Latitude=lat(60), Source="Kerbside Charging R1"))
        self.assertEqual(len(same.clean), 1)
        other = self.clean(rec(Station_address="14 Macauley St, Denman NSW 2328", Source="Existing Fast Chargers"),
                           rec(Station_address="99 Other St, Denman NSW 2328", Latitude=lat(60), Source="Kerbside Charging R1"))
        self.assertEqual(len(other.clean), 2)

    def test_chain_must_not_merge_different_ac_ratings(self):
        common = dict(Charger_Type="AC", Operator="Exploren", Number_of_plugs="2")
        res = self.clean(rec(Charger_rating="22 kW", Source="Existing Destination Chargers", **common),
                         rec(Charger_rating="AC", Source="Destination Charging R1", **common),
                         rec(Charger_rating="7", Source="", **common))
        self.assertEqual(len(res.clean), 2)                          # 22 kW and 7 kW stay apart

    def test_placeholder_operator_matches_named_operator(self):
        res = self.clean(rec(Operator="Non-networked", Charger_Type="AC", Charger_rating="AC", Source="Existing Destination Chargers"),
                         rec(Operator="Noodoe", Charger_Type="AC", Charger_rating="22 kW", Source="Destination Charging R2"))
        self.assertEqual(len(res.clean), 1)
        self.assertEqual(res.clean.iloc[0].operator_name, "Noodoe")
        self.assertEqual(res.clean.iloc[0].rating_kw, 22.0)          # gap filled from the duplicate

    def test_postcode_address_wins_and_prefix_is_stripped(self):
        res = self.clean(rec(Station_address="1 - 7 Ross St, Wilcannia NSW 2836, Australia", PCODE="2350"),
                         rec(Station_address="10 Victoria St, Wollongong , NSW 2500", PCODE="NSW 2500", Latitude=lat(900)),
                         rec(Station_address="Somewhere St, Sydney", PCODE="", Latitude=lat(1800)))
        pcs = dict(zip(res.clean["address"], res.clean["postcode"]))
        self.assertEqual(pcs["1 - 7 Ross St, Wilcannia NSW 2836"], "2836")
        self.assertEqual(pcs["10 Victoria St, Wollongong, NSW 2500"], "2500")
        self.assertTrue(pd.isna(pcs["Somewhere St, Sydney"]))

    def test_station_name_remains_missing_when_blank_or_operator_only(self):
        res = self.clean(rec(Station_name="", Station_address="72-78 Brook St, Muswellbrook, 2333"),
                         rec(Station_name="Evie", Latitude=lat(900)),
                         rec(Station_name="Bunnings Rhodes", Latitude=lat(1800)))
        self.assertEqual(int(res.clean["station_name"].isna().sum()), 2)
        self.assertEqual(res.clean["station_name"].dropna().tolist(), ["Bunnings Rhodes"])

    def test_ids_are_stable_unique_and_stations_group_ac_and_dc(self):
        rows = [rec(Operator="Engie", Charger_rating="150 kW", Source="Existing Fast Chargers"),
                rec(Operator="Chargefox", Charger_Type="AC", Charger_rating="22 kW", Source="Existing Destination Chargers"),
                rec(Latitude=lat(2000), Source="Existing Fast Chargers")]
        a, b = self.clean(*rows).clean, self.clean(*rows).clean
        self.assertEqual(list(a.charger_id), list(b.charger_id))
        self.assertTrue(a.charger_id.is_unique)
        self.assertEqual(a.station_id.nunique(), 2)                  # AC + DC at one spot = one station

    def test_clean_export_uses_standard_names_and_keeps_blanks(self):
        res = self.clean(rec(Station_name="", Operator="Tesla Motors ", Charger_rating="50", OBJECTID="7"),
                         rec(Station_name="Bunnings", Operator="Evie", Charger_rating="AC", Charger_Type="AC",
                             Latitude=lat(900)))
        exp = res.clean_export
        self.assertEqual(list(exp.columns), run.BASE_CLEAN_COLUMNS)
        first, second = exp.iloc[0], exp.iloc[1]                     # file order of the raw CSV
        self.assertEqual((first.operator_name, first.rating_text, first.source_objectid), ("Tesla", "50 kW", "7"))
        self.assertTrue(pd.isna(first.station_name))                 # blank stays blank
        self.assertEqual(second.station_name, "Bunnings")
        self.assertTrue(pd.isna(second.rating_text))                 # placeholder 'AC' -> null
        self.assertEqual(list(res.audit.charger_id), list(exp.charger_id))

    def test_reconciliation_raw_equals_kept_plus_excluded(self):
        res = self.clean(rec(), rec(Latitude=lat(10), Source="Kerbside Charging R1", Operator="Evie Networks"),
                         rec(Charger_Type="Upcoming", Latitude=lat(900)))
        self.assertEqual(len(res.clean) + len(res.excluded), 3)


@unittest.skipUnless(RAW_CSV.exists() and SA4_SHP.exists(), "raw EV CSV or SA4 shapefile not found")
class RealData(unittest.TestCase):
    """Expected numbers for ev_20251216.csv (sha256 43970e77...).  Update them if a rule or threshold changes."""

    @classmethod
    def setUpClass(cls):
        cls.raw = run.load_raw(RAW_CSV)
        base = run.run_cleaning(cls.raw)
        cls.res = run.add_sa4_region(base, SA4_SHP)

    def test_shape_and_reconciliation(self):
        self.assertEqual(len(self.raw), 1958)
        self.assertEqual(len(self.res.clean) + len(self.res.excluded), 1958)
        self.assertEqual(int((self.res.excluded.reason == "upcoming_not_installed").sum()), 98)

    def test_expected_counts(self):
        self.assertEqual(len(self.res.clean), 1803)
        self.assertEqual(int((self.res.clean.charger_type == "DC").sum()), 419)
        self.assertEqual(len(self.res.dc_candidates), 419)
        self.assertEqual(int(self.res.clean_export.sa4_region.isna().sum()), 0)
        self.assertEqual(int(self.res.dc_candidates.sa4_region.isna().sum()), 0)

    def test_clean_export_file(self):
        exp = self.res.clean_export
        self.assertEqual(list(exp.columns), run.CLEAN_COLUMNS)
        self.assertEqual(len(exp), 1803)
        self.assertTrue(exp.charger_id.is_unique)
        self.assertEqual(set(exp.charger_id), set(self.res.clean.charger_id))

    def test_parquet_and_csv_pairs_have_the_same_content(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run.write_outputs(root, self.res)
            clean_parquet = pd.read_parquet(root / run.OUT_CLEAN)
            clean_csv = pd.read_csv(
                root / run.OUT_CLEAN_CSV,
                dtype={"source_objectid": "string", "postcode": "string"},
            )
            dc_parquet = pd.read_parquet(root / run.OUT_DC)
            dc_csv = pd.read_csv(root / run.OUT_DC_CSV, dtype={"postcode": "string"})

        def comparable(frame):
            return frame.astype("string").fillna("<missing>")

        self.assertEqual(list(clean_parquet.columns), run.CLEAN_COLUMNS)
        self.assertEqual(list(dc_parquet.columns), run.DC_COLUMNS)
        self.assertEqual(clean_parquet.shape, (1803, 15))
        self.assertEqual(dc_parquet.shape, (419, 14))
        pd.testing.assert_frame_equal(comparable(clean_parquet), comparable(clean_csv))
        pd.testing.assert_frame_equal(comparable(dc_parquet), comparable(dc_csv))
        pd.testing.assert_frame_equal(clean_parquet, self.res.clean_export)
        pd.testing.assert_frame_equal(dc_parquet, self.res.dc_candidates)

    def test_integrity(self):
        c = self.res.clean
        self.assertTrue(c.charger_id.is_unique)
        self.assertFalse(c[["operator_name", "address", "latitude", "longitude"]].isna().any().any())
        self.assertEqual(int(c.station_name.isna().sum()), 1304)
        self.assertEqual(int(self.res.dc_candidates.station_name.isna().sum()), 419)
        self.assertTrue(c.latitude.between(-37.6, -28.0).all() and c.longitude.between(140.9, 153.7).all())
        self.assertTrue(set(c.charger_type) <= {"AC", "DC"})
        self.assertTrue(c.postcode.str.fullmatch(r"\d{4}").all())
        self.assertEqual(int(self.res.dc_candidates.rating_kw.isna().sum()), 0)   # every DC record has a rating
        self.assertLessEqual(len(set(c.operator_name)), 39)


if __name__ == "__main__":
    unittest.main()
