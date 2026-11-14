#!/usr/bin/env python3
"""Enforce the per-test budget (each CPU test under 30 s, spec section 4.4) on stable Rust.

libtest's per-test timing (`--report-time`, `--format=json`) is nightly-only, and
`cargo test` gives no per-test deadline. This tool times each test from the
output stable libtest already prints, as wallet-pir's tools/ci/test_budget.py
does:

1. `cargo test --no-run --message-format=json-render-diagnostics <args>` builds
   every test executable and reports its path, package and target.
2. For each executable, `<exe> --list` names every test it holds and
   `<exe> --list --ignored` names the ignored ones (the repository forbids them).
3. The executable runs with `--test-threads=1 --format=pretty`. With a single
   test thread libtest writes `test <name> ... ` when a test starts and the
   result (`ok`, `FAILED`, `ignored`) when it ends. The output is read as raw
   bytes, partial lines included, so the start of a test is seen at once and a
   stalled test is killed (its whole process group) when it reaches the budget.
   Time spent outside any test (process start, fixtures between tests, exit) is
   held to the same deadline.

A run fails if any test fails, is ignored, reaches the budget, is missing from
the output, or if the executable exits non-zero. Executables are run in parallel
with one another (`--jobs`); the tests inside one executable run one at a time,
so a fixture cached in a process is shared by its tests as it is under
`cargo test`, and a test's time is its own. Targets with `harness = false` have
no libtest protocol and are timed as one case.

Doctests are run by `cargo test --doc ... -- --test-threads=1 --format=pretty`,
which prints the same protocol, and are timed by the same reader (see
`run_doctests`).
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import selectors
import shlex
import signal
import subprocess
import sys
import threading
import time
import tomllib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, environment  # noqa: E402

DEFAULT_CASE_SECONDS = 30.0
START = re.compile(r'(?:^|\n)test (.+?) \.\.\. ')
RESULT = re.compile(r'(?:^|\n)(ok|FAILED|ignored)\b[^\n]*\n')
_print_lock = threading.Lock()
_processes: set[subprocess.Popen] = set()
_processes_lock = threading.Lock()


def case_seconds(value) -> float:
    seconds = float(value)
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError('the case budget must be a positive number of seconds')
    return seconds


@dataclass
class Program:
    """One test executable and the package it belongs to."""
    executable: str
    package: str
    version: str
    manifest_dir: str
    target: str
    harness: bool = True


@dataclass
class Outcome:
    """The cases one executable (or the doctest run) produced."""
    label: str
    cases: list[dict] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)
    output: str = ''


def kill_group(process: subprocess.Popen) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def stop_all() -> None:
    """Kill every running test process group (used on cancellation)."""
    with _processes_lock:
        for process in list(_processes):
            kill_group(process)


def listed(command: list[str], *, cwd: str, env: dict, ignored: bool = False) -> list[str]:
    arguments = [*command, '--list', '--format=terse'] + (['--ignored'] if ignored else [])
    output = subprocess.check_output(arguments, cwd=cwd, env=env, text=True,
                                     stderr=subprocess.STDOUT, timeout=60)
    return [line[:-len(': test')] for line in output.splitlines() if line.endswith(': test')]


def stream(command: list[str], *, cwd: str, env: dict, seconds: float, label: str,
           expected: set[str] | None, deadline: float | None, executable: str,
           idle_seconds: float | None) -> Outcome:
    """Run one libtest process with one test thread, timing each test from its
    `test <name> ... ` line to its result. Time outside any test is held to
    `idle_seconds` (None: only the overall deadline). `expected` (a set of
    names) is checked when given; doctest runs pass None."""
    outcome = Outcome(label)
    killed = False
    process = subprocess.Popen(command, cwd=cwd, env=env, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, start_new_session=True)
    with _processes_lock:
        _processes.add(process)
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    pending, output = '', []
    active = None  # (name, started)
    completed: set[str] = set()
    last_event = time.monotonic()
    try:
        while selector.get_map():
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                outcome.problems.append(f'{label}: stopped at the overall deadline'
                                        + (f' while running {active[0]}' if active else ''))
                kill_group(process)
                killed = True
                break
            limit = seconds if active else idle_seconds
            if limit is not None and now - (active[1] if active else last_event) >= limit:
                if active:
                    outcome.cases.append(dict(test=active[0], executable=executable,
                                              seconds=round(now - active[1], 3), status='timeout'))
                    outcome.problems.append(f'{label}: {active[0]} reached the {seconds:g} s budget; killed')
                else:
                    outcome.problems.append(f'{label}: {limit:g} s passed outside any test; killed')
                with _print_lock:
                    print(outcome.problems[-1], flush=True)
                kill_group(process)
                killed = True
                break
            for key, _ in selector.select(timeout=0.05):
                data = os.read(key.fd, 65536)
                if not data:
                    selector.unregister(key.fileobj)
                    continue
                text = data.decode(errors='replace')
                output.append(text)
                pending += text
                while True:
                    if active is None:
                        if expected is not None and completed == expected:
                            pending = ''
                            break
                        match = START.search(pending)
                        if match is None:
                            # Keep only a possibly incomplete trailing line.
                            pending = pending[pending.rfind('\n'):] if '\n' in pending else pending
                            break
                        name = match[1].removesuffix(' - should panic')
                        if expected is not None and (name not in expected or name in completed):
                            outcome.problems.append(f'{label}: unexpected or repeated test {name!r}')
                        active = (name, time.monotonic())
                        pending = pending[match.end():]
                    match = RESULT.search(pending)
                    if match is None:
                        break
                    now = time.monotonic()
                    name, began = active
                    elapsed = now - began
                    status = match[1]
                    if status == 'ok' and elapsed >= seconds:
                        status = 'over-budget'
                    outcome.cases.append(dict(test=name, executable=executable,
                                              seconds=round(elapsed, 3), status=status))
                    if status != 'ok':
                        outcome.problems.append(f'{label}: {name} {status} after {elapsed:.2f} s')
                    completed.add(name)
                    pending = pending[match.end():]
                    active = None
                    last_event = now
        code = process.wait()
        if not killed:
            if code != 0:
                outcome.problems.append(f'{label}: exited with status {code}')
            missing = sorted(expected - completed) if expected is not None else []
            if missing:
                outcome.problems.append(f'{label}: tests listed but not run: {missing}')
            if active is not None:
                outcome.problems.append(f'{label}: {active[0]} started but reported no result')
    finally:
        kill_group(process)
        process.wait()
        process.stdout.close()
        selector.close()
        with _processes_lock:
            _processes.discard(process)
    outcome.output = ''.join(output)
    return outcome


def test_environment(program: Program, env: dict) -> dict:
    """The run-time variables `cargo test` gives a test process."""
    deps = str(Path(program.executable).parent)
    library_path = 'DYLD_FALLBACK_LIBRARY_PATH' if sys.platform == 'darwin' else 'LD_LIBRARY_PATH'
    return dict(env, CARGO_MANIFEST_DIR=program.manifest_dir, CARGO_PKG_NAME=program.package,
                CARGO_PKG_VERSION=program.version, CARGO_CRATE_NAME=program.target.replace('-', '_'),
                **{library_path: os.pathsep.join(filter(None, [deps, str(Path(deps).parent),
                                                                env.get(library_path, '')]))})


def run_program(program: Program, *, seconds: float, env: dict, deadline: float | None) -> Outcome:
    label = f'{program.package}::{program.target} ({Path(program.executable).name})'
    env = test_environment(program, env)
    if not program.harness:
        # No libtest protocol to read: the whole executable is one case.
        outcome = Outcome(label)
        started = time.monotonic()
        limit = seconds if deadline is None else max(0.0, min(seconds, deadline - started))
        process = subprocess.Popen([program.executable], cwd=program.manifest_dir, env=env,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        with _processes_lock:
            _processes.add(process)
        try:
            stdout, _ = process.communicate(timeout=limit)
            status = 'ok' if process.returncode == 0 else 'FAILED'
        except subprocess.TimeoutExpired:
            kill_group(process)
            stdout, _ = process.communicate()
            status = 'timeout'
        finally:
            with _processes_lock:
                _processes.discard(process)
        elapsed = time.monotonic() - started
        outcome.output = stdout.decode(errors='replace')
        outcome.cases.append(dict(test=f'{program.target} (harness = false)',
                                  executable=Path(program.executable).name,
                                  seconds=round(elapsed, 3), status=status))
        if status != 'ok':
            outcome.problems.append(f'{label}: {status} after {elapsed:.2f} s')
        return outcome
    command = [program.executable]
    try:
        names = listed(command, cwd=program.manifest_dir, env=env)
        ignored = listed(command, cwd=program.manifest_dir, env=env, ignored=True)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        outcome = Outcome(label, problems=[f'{label}: could not list tests: {error}'])
        outcome.output = getattr(error, 'output', '') or ''
        return outcome
    if not names:
        return Outcome(label)
    outcome = stream([*command, '--test-threads=1', '--color=never', '--format=pretty'],
                     cwd=program.manifest_dir, env=env, seconds=seconds, label=label,
                     expected=set(names), deadline=deadline, executable=Path(program.executable).name,
                     idle_seconds=seconds)
    for name in ignored:
        outcome.problems.append(f'{label}: {name} is #[ignore]d; ignored tests are not allowed '
                                '(gate slow cases behind the qualification-tests feature)')
    return outcome


def manifest(manifest_dir: str) -> tuple[str, str, set[str]]:
    """(package name, version, targets declared with `harness = false`)."""
    data = tomllib.loads((Path(manifest_dir) / 'Cargo.toml').read_text())
    names = set()
    package = data['package']['name']
    version = data['package'].get('version', '')
    version = version if isinstance(version, str) else ''
    lib = data.get('lib', {})
    if lib.get('harness') is False:
        names.add(lib.get('name', package.replace('-', '_')))
    for kind in ('test', 'bench', 'bin', 'example'):
        for target in data.get(kind, []):
            if target.get('harness') is False:
                names.add(target.get('name'))
    return package, version, names


def build(cargo_args: list[str], env: dict) -> list[Program]:
    """Compile the test executables `cargo test <cargo_args>` would run and return them."""
    command = ['cargo', 'test', '--no-run', '--message-format=json-render-diagnostics', *cargo_args]
    print('+ ' + shlex.join(command), flush=True)
    process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE, text=True,
                               start_new_session=True)
    with _processes_lock:
        _processes.add(process)
    programs, seen = [], set()
    for line in process.stdout:
        if not line.startswith('{'):
            sys.stdout.write(line)
            continue
        message = json.loads(line)
        if message.get('reason') != 'compiler-artifact' or not message.get('executable'):
            continue
        if not message.get('profile', {}).get('test'):
            continue
        executable = message['executable']
        if executable in seen:
            continue
        seen.add(executable)
        manifest_dir = str(Path(message['manifest_path']).parent)
        target = message['target']['name']
        package, version, no_harness = manifest(manifest_dir)
        if not version:
            # A workspace-inherited version: the package id ends in `#[name@]version`.
            version = message['package_id'].rsplit('#', 1)[-1].rpartition('@')[2]
        programs.append(Program(executable=executable, package=package, version=version,
                                manifest_dir=manifest_dir, target=target,
                                harness=target not in no_harness))
    process.stdout.close()
    code = process.wait()
    with _processes_lock:
        _processes.discard(process)
    if code != 0:
        raise SystemExit('test build failed: ' + shlex.join(command))
    return programs


def run_programs(programs: list[Program], *, seconds: float, jobs: int, env: dict,
                 deadline: float | None = None) -> list[Outcome]:
    def one(program):
        outcome = run_program(program, seconds=seconds, env=env, deadline=deadline)
        with _print_lock:
            slowest = max((case['seconds'] for case in outcome.cases), default=0.0)
            print(f'--- {outcome.label}: {len(outcome.cases)} test(s), slowest {slowest:.2f} s, '
                  f'{"FAILED" if outcome.problems else "ok"}', flush=True)
            if outcome.problems:
                sys.stdout.write(outcome.output)
                if outcome.output and not outcome.output.endswith('\n'):
                    sys.stdout.write('\n')
        return outcome
    with ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
        return list(pool.map(one, programs))


def run_doctests(cargo_args: list[str], *, seconds: float, env: dict,
                 deadline: float | None = None) -> Outcome:
    """Run doctests one at a time under the same per-case budget."""
    command = ['cargo', 'test', '--doc', *cargo_args, '--',
               '--test-threads=1', '--color=never', '--format=pretty']
    print('+ ' + shlex.join(command), flush=True)
    # Cargo compiles each package's library before rustdoc runs its tests, so
    # time outside a case is bounded by the overall deadline only; each doctest
    # (rustdoc compiles and runs it inside the case) keeps the per-case budget.
    outcome = stream(command, cwd=str(ROOT), env=env, seconds=seconds, label='doctests',
                     expected=None, deadline=deadline, executable='rustdoc', idle_seconds=None)
    with _print_lock:
        print(f'--- doctests: {len(outcome.cases)} test(s), '
              f'{"FAILED" if outcome.problems else "ok"}', flush=True)
        if outcome.problems:
            sys.stdout.write(outcome.output)
    return outcome


def summarize(outcomes: list[Outcome], seconds: float) -> dict:
    cases = [case for outcome in outcomes for case in outcome.cases]
    problems = [problem for outcome in outcomes for problem in outcome.problems]
    return dict(passed=not problems, case_seconds=seconds, cases=len(cases), problems=problems,
                slowest=sorted(cases, key=lambda case: case['seconds'], reverse=True)[:10])


def print_summary(summary: dict) -> None:
    print(f'test budget: {summary["cases"]} test(s) under a {summary["case_seconds"]:g} s per-test budget')
    for case in summary['slowest']:
        print(f'  {case["seconds"]:8.2f} s  {case["status"]:<11} {case["executable"]}  {case["test"]}')
    for problem in summary['problems']:
        print(f'error: {problem}')


def install_cancellation() -> None:
    def cancel(number, _frame):
        stop_all()
        raise SystemExit(128 + number)
    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, cancel)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
        usage='%(prog)s [--case-seconds N] [--jobs N] [--doc] -- <cargo test arguments>')
    parser.add_argument('--case-seconds', type=case_seconds, default=DEFAULT_CASE_SECONDS)
    parser.add_argument('--jobs', type=int, default=min(4, os.cpu_count() or 1))
    parser.add_argument('--doc', action='store_true', help='also run doctests under the budget')
    parser.add_argument('--report', type=Path, help='write the JSON summary to this file')
    parser.add_argument('cargo_args', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    cargo_args = args.cargo_args[1:] if args.cargo_args[:1] == ['--'] else args.cargo_args
    install_cancellation()
    env = environment()
    outcomes = run_programs(build(cargo_args, env), seconds=args.case_seconds, jobs=args.jobs, env=env)
    if args.doc:
        outcomes.append(run_doctests(cargo_args, seconds=args.case_seconds, env=env))
    summary = summarize(outcomes, args.case_seconds)
    if args.report:
        args.report.write_text(json.dumps(summary, indent=2) + '\n')
    print_summary(summary)
    if not summary['passed']:
        raise SystemExit('test budget failed')


if __name__ == '__main__':
    main()
