"""CLI entry point for the isolated Phase D runner."""
from __future__ import annotations

import argparse

from experiments.phase_d.runner import run_phase_d


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("development", "full"), required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    return run_phase_d(args.mode, args.output)


if __name__ == "__main__":
    raise SystemExit(main())
