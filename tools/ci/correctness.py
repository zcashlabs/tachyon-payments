#!/usr/bin/env python3
"""The warm correctness run: every workspace test under `--profile release-fast`,
in under 300 s, with each test under 30 s (spec section 4.4).

The clock starts before compilation, so a warm run includes Cargo's freshness
check and any incremental rebuild; report a cold first build separately. The
run is, in order:

1. the CI tool tests (`python3 -m unittest discover -s tools/ci/tests`);
2. `cargo test --no-run --locked --workspace --profile release-fast`, then every
   test executable under tools/ci/test_budget.py (per-test deadline, no ignored
   tests, parallel across executables);
3. the workspace doctests under the same per-test deadline.

Slow cases belong behind the `qualification-tests` feature, which this run does
not enable. A JSON summary is written to .local-pir/correctness/latest.json.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, environment, run  # noqa: E402
import test_budget  # noqa: E402

RUN_SECONDS = 300.0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--budget-seconds', type=test_budget.case_seconds, default=RUN_SECONDS,
                        help='whole-run budget (default: 300)')
    parser.add_argument('--case-seconds', type=test_budget.case_seconds,
                        default=test_budget.DEFAULT_CASE_SECONDS, help='per-test budget (default: 30)')
    parser.add_argument('--jobs', type=int, default=min(4, os.cpu_count() or 1),
                        help='test executables run at once (default: min(4, CPUs))')
    parser.add_argument('--offline', action='store_true', help='pass --offline to cargo')
    args = parser.parse_args()

    started = time.monotonic()
    deadline = started + args.budget_seconds
    test_budget.install_cancellation()
    # A hard stop for the steps that have no deadline of their own (compilation).
    def overrun():
        print(f'error: the correctness run reached its {args.budget_seconds:g} s budget; stopping',
              flush=True)
        os.kill(os.getpid(), signal.SIGTERM)
    timer = threading.Timer(args.budget_seconds, overrun)
    timer.daemon = True
    timer.start()
    env = environment()
    cargo_args = ['--locked', '--workspace', '--profile', 'release-fast',
                  *(['--offline'] if args.offline else [])]
    stages = {}
    try:
        stages['ci-tool-tests'] = run([sys.executable, '-m', 'unittest', 'discover', '-s', 'tools/ci/tests'],
                                      env=env)
        mark = time.monotonic()
        programs = test_budget.build(cargo_args, env)
        stages['compile'] = time.monotonic() - mark
        mark = time.monotonic()
        outcomes = test_budget.run_programs(programs, seconds=args.case_seconds, jobs=args.jobs,
                                            env=env, deadline=deadline)
        stages['tests'] = time.monotonic() - mark
        mark = time.monotonic()
        outcomes.append(test_budget.run_doctests(cargo_args, seconds=args.case_seconds, env=env,
                                                 deadline=deadline))
        stages['doctests'] = time.monotonic() - mark
    finally:
        timer.cancel()
    elapsed = time.monotonic() - started
    summary = test_budget.summarize(outcomes, args.case_seconds)
    over = elapsed >= args.budget_seconds
    if over:
        summary['problems'].append(f'the correctness run took {elapsed:.1f} s; the budget is '
                                   f'{args.budget_seconds:g} s')
    summary.update(passed=not summary['problems'], seconds=round(elapsed, 3),
                   budget_seconds=args.budget_seconds,
                   stages={name: round(seconds, 3) for name, seconds in stages.items()},
                   executables=len(programs))
    output = ROOT / '.local-pir/correctness'
    output.mkdir(parents=True, exist_ok=True)
    (output / 'latest.json').write_text(json.dumps(summary, indent=2) + '\n')
    test_budget.print_summary(summary)
    print('stages: ' + ', '.join(f'{name} {seconds:.1f} s' for name, seconds in stages.items()))
    print(f'correctness run: {elapsed:.1f} s of {args.budget_seconds:g} s, '
          f'{"passed" if summary["passed"] else "FAILED"}')
    if not summary['passed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
