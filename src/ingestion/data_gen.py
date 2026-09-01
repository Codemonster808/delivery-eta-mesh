#!/usr/bin/env python3
"""
Generates a synthetic dispatch day: orders + courier GPS pings, with a
skewed restaurant distribution (5% of restaurants get 60% of orders — the
scenario src/transformation/replay.py's Spark job has to handle without
falling over on one overloaded partition) and a fraction of GPS pings
deliberately arriving 5-15 minutes "late" (out of event-time order).
"""

import argparse
import json
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.synth import seeded_rng, skewed_choice  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--restaurants", type=int, default=100)
    parser.add_argument("--orders-per-hour", type=int, default=200)
    parser.add_argument("--out", required=True)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rng = seeded_rng(args.seed)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    restaurant_ids = [f"rest_{i:04d}" for i in range(args.restaurants)]

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    n_events = n_late = 0
    restaurant_order_counts = {r: 0 for r in restaurant_ids}

    with out_path.open("w") as f:
        for hour in range(args.hours):
            for _ in range(args.orders_per_hour):
                restaurant_id = skewed_choice(
                    rng, restaurant_ids, hot_fraction=0.05, hot_weight=0.6
                )
                restaurant_order_counts[restaurant_id] += 1
                order_id = str(uuid.uuid4())
                order_ts = start + timedelta(hours=hour, minutes=rng.randint(0, 59))
                distance_km = round(rng.uniform(1.0, 12.0), 2)

                # Ground truth actual delivery time, for measuring the
                # worker's ETA MAE against — deliberately uses a different
                # (noisier, traffic-varying) speed than the worker's fixed
                # 22 km/h heuristic, so MAE is a real, non-zero number
                # reflecting the heuristic's real-world error, not a tautology.
                actual_speed_kmh = max(8.0, rng.gauss(20.0, 5.0))
                actual_prep_min = max(5.0, rng.gauss(12.0, 3.0))
                actual_delivery_minutes = round(
                    actual_prep_min + (distance_km / actual_speed_kmh) * 60, 1
                )

                # order_placed event
                f.write(
                    json.dumps(
                        {
                            "event_type": "order_placed",
                            "order_id": order_id,
                            "restaurant_id": restaurant_id,
                            "distance_km": distance_km,
                            "actual_delivery_minutes": actual_delivery_minutes,
                            "ts": order_ts.isoformat(),
                            "event_ts": order_ts.isoformat(),
                        }
                    )
                    + "\n"
                )
                n_events += 1

                # courier GPS ping — 12% arrive late (event_ts stays truthful,
                # but the ping isn't "seen" by the pipeline until later)
                gps_event_ts = order_ts + timedelta(minutes=rng.randint(5, 25))
                is_late = rng.random() < 0.12
                arrival_delay = timedelta(minutes=rng.randint(5, 15)) if is_late else timedelta(0)
                f.write(
                    json.dumps(
                        {
                            "event_type": "courier_gps",
                            "order_id": order_id,
                            "restaurant_id": restaurant_id,
                            "lat": round(rng.uniform(-23.7, -23.4), 4),
                            "lon": round(rng.uniform(-46.8, -46.5), 4),
                            "event_ts": gps_event_ts.isoformat(),
                            "ts": (
                                gps_event_ts + arrival_delay
                            ).isoformat(),  # pipeline "sees" it at ts
                            "is_late": is_late,
                        }
                    )
                    + "\n"
                )
                n_events += 1
                n_late += int(is_late)

    top5pct = sorted(restaurant_order_counts.values(), reverse=True)[
        : max(1, args.restaurants // 20)
    ]
    total_orders = sum(restaurant_order_counts.values())
    top_share = sum(top5pct) / total_orders if total_orders else 0

    print(f"wrote {n_events} events ({args.hours}h, {args.restaurants} restaurants) to {out_path}")
    print(f"  top 5% of restaurants received {top_share:.1%} of orders (target ~60%)")
    print(f"  {n_late} GPS pings marked late ({n_late / (n_events / 2):.1%} of orders)")


if __name__ == "__main__":
    main()
