"""Continuously enqueue demo tasks. Usage: python demo/producer.py [--rate 2] [--duration 60]"""

from __future__ import annotations

import argparse
import time

from demo.tasks import enqueue_random


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=2.0, help="tasks per second")
    parser.add_argument("--duration", type=float, default=0, help="seconds to run (0 = forever)")
    args = parser.parse_args()
    deadline = time.monotonic() + args.duration if args.duration else None
    interval = 1.0 / max(args.rate, 0.01)
    sent = 0
    try:
        while deadline is None or time.monotonic() < deadline:
            enqueue_random()
            sent += 1
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    print(f"enqueued {sent} tasks")


if __name__ == "__main__":
    main()
