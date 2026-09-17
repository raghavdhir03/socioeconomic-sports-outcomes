"""Summarize an ingestion run's progress from its JSONL log.

Usage:
    python src/ingestion/status.py
    python src/ingestion/status.py --log-path .cache/maxpreps-contests/ingestion.jsonl
"""

import argparse
import json
from collections import Counter
from pathlib import Path


def load_latest(log_path):
    """The last-logged record for each unit_id, in the order first seen."""
    latest = {}
    with open(log_path) as log_file:
        for line in log_file:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            latest[record["unit_id"]] = record
    return latest


def summarize(latest):
    """Compute the stats `main()` prints, kept separate so it's testable
    without touching a real file."""
    statuses = Counter(record["status"] for record in latest.values())

    all_seasons = {record["SEASON"] for record in latest.values()}
    by_state = {}
    for record in latest.values():
        seasons_done = by_state.setdefault(record["STATE"], set())
        if record["status"] in ("succeeded", "skipped"):
            seasons_done.add(record["SEASON"])
    complete_states = sorted(
        state for state, seasons_done in by_state.items() if all_seasons <= seasons_done
    )

    running = sorted(uid for uid, r in latest.items() if r["status"] == "running")
    failures = [
        (uid, r.get("error_message", ""))
        for uid, r in latest.items()
        if r["status"] == "failed"
    ]

    return {
        "total_units": len(latest),
        "status_counts": dict(statuses),
        "states_total": len(by_state),
        "states_complete": complete_states,
        "running": running,
        "failures": failures,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--log-path",
        default=".cache/maxpreps-contests/ingestion.jsonl",
        help="Path to the ingestion.jsonl log (default: %(default)s)",
    )
    args = parser.parse_args()

    if not Path(args.log_path).exists():
        print(f"No log found at {args.log_path}")
        return

    stats = summarize(load_latest(args.log_path))

    print(f"{stats['total_units']} units seen — {stats['status_counts']}")
    print(
        f"{len(stats['states_complete'])} / {stats['states_total']} states fully complete: "
        f"{stats['states_complete']}"
    )

    if stats["running"]:
        print(f"\nCurrently running: {', '.join(stats['running'])}")

    if stats["failures"]:
        print(f"\n{len(stats['failures'])} failure(s):")
        for unit_id, message in stats["failures"]:
            print(f"  {unit_id} - {message[:120]}")
    else:
        print("\nNo failures.")


if __name__ == "__main__":
    main()
