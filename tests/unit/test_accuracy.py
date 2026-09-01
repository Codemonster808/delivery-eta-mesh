import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from accuracy import LAT_MAX, LAT_MIN, LON_MAX, LON_MIN, _mae_by_group, zone_for  # noqa: E402


def test_zone_for_is_deterministic_and_grid_bucketed():
    sw_corner = zone_for(LAT_MIN + 0.001, LON_MIN + 0.001)
    ne_corner = zone_for(LAT_MAX - 0.001, LON_MAX - 0.001)
    assert sw_corner == "zone_0_0"
    assert ne_corner == "zone_2_2"
    # same point always buckets to the same zone
    assert zone_for(-23.55, -46.65) == zone_for(-23.55, -46.65)


def test_zone_for_clamps_out_of_range_coordinates():
    # a coordinate outside the synthetic delivery area (e.g. bad GPS data)
    # still resolves to a valid zone instead of raising or going negative
    assert zone_for(LAT_MIN - 5, LON_MIN - 5) == "zone_0_0"
    assert zone_for(LAT_MAX + 5, LON_MAX + 5) == "zone_2_2"


def test_mae_by_group_computes_per_zone_hour_mae():
    ground_truth = {
        "o1": (20.0, 8),  # zone_a, hour 8
        "o2": (30.0, 8),  # zone_a, hour 8
        "o3": (10.0, 20),  # zone_b, hour 20
    }
    zones = {"o1": "zone_a", "o2": "zone_a", "o3": "zone_b"}
    predicted = {"o1": 22.0, "o2": 33.0, "o3": 15.0}

    rows = _mae_by_group(ground_truth, zones, predicted)

    by_key = {(r["zone"], r["hour"]): r for r in rows}
    assert by_key[("zone_a", 8)]["n_orders"] == 2
    assert by_key[("zone_a", 8)]["mae_minutes"] == 2.5  # mean of |22-20|=2 and |33-30|=3
    assert by_key[("zone_b", 20)]["n_orders"] == 1
    assert by_key[("zone_b", 20)]["mae_minutes"] == 5.0


def test_mae_by_group_skips_unmatched_orders():
    # order present in ground truth but never scored (not in `predicted`)
    # and an order with no GPS ping (not in `zones`) must both be excluded.
    ground_truth = {"scored": (10.0, 9), "unscored": (10.0, 9), "no_gps": (10.0, 9)}
    zones = {"scored": "zone_a", "no_gps_missing_on_purpose": "zone_a"}
    predicted = {"scored": 12.0, "unscored": 12.0}

    rows = _mae_by_group(ground_truth, zones, predicted)

    assert len(rows) == 1
    assert rows[0]["zone"] == "zone_a"
    assert rows[0]["n_orders"] == 1
