#!/usr/bin/env python3
"""Fast feedback for the inner loop: `make check-fast` and `make check-package`.

check-fast runs, in order:

1. `cargo fmt --all --check`;
2. the dependency boundary check (tools/ci/boundary.py);
3. the CI tool tests, when tools/ci/ or the Makefile changed (or BASE is unset);
4. `cargo clippy --locked --all-targets -p ... -- -D warnings` and
   `cargo test --locked --profile release-fast -p ...` for the affected packages.

Affected packages are those owning a path changed since BASE (committed,
staged, unstaged and untracked), plus every workspace package that depends on
one of them, by any edge kind. Without BASE, or when a path that affects every
build changes (the root manifest, the lock file, the toolchain, .cargo/) or a
path no package owns and no rule classifies, the whole workspace is checked.
Documentation, evidence and the spec select no Rust packages.

check-package runs fmt, the boundary check, clippy and release-fast tests for
one package, with optional features and a test-name filter.
"""
from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, cargo_metadata, environment, relative_directory, run, workspace_packages  # noqa: E402

# Root inputs that change every package's build.
GLOBAL = ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', '.cargo/')
CI_TOOLING = ('tools/ci/', 'Makefile')
NO_RUST = ('docs/', 'evidence/', '.gitignore')
DRY_RUN = False


def step(command: list[str], env: dict) -> None:
    """Run one gate command, or only print it under --dry-run."""
    if DRY_RUN:
        print('would run: ' + shlex.join(command))
    else:
        run(command, env=env)


def changed_paths(base: str) -> list[str]:
    """Paths that differ between `base` and the working tree, plus untracked files."""
    def names(*args):
        output = subprocess.check_output(['git', *args], cwd=ROOT)
        return [name.decode() for name in output.split(b'\0') if name]
    try:
        subprocess.check_output(['git', 'rev-parse', '--verify', '--quiet', base + '^{commit}'], cwd=ROOT)
    except subprocess.CalledProcessError:
        raise SystemExit(f'BASE={base} is not a commit in this repository')
    tracked = names('diff', '--name-only', '--no-renames', '-z', base, '--')
    untracked = names('ls-files', '--others', '--exclude-standard', '-z')
    return sorted(set(tracked) | set(untracked))


def classify(paths: list[str], packages: dict[str, str]) -> tuple[set[str] | None, bool, list[str]]:
    """(owned packages or None for the whole workspace, CI tooling changed, reasons).

    `packages` maps a package name to its directory relative to the root."""
    owned: set[str] = set()
    tooling = False
    reasons = []
    by_depth = sorted(packages.items(), key=lambda item: len(item[1]), reverse=True)
    whole = False
    for path in paths:
        owner = next((name for name, directory in by_depth if path.startswith(directory + '/')), None)
        if owner is not None:
            owned.add(owner)
        elif path in GLOBAL or path.startswith('.cargo/'):
            whole = True
            reasons.append(f'{path} affects every package')
        elif path.startswith(CI_TOOLING):
            tooling = True
        elif path.startswith(NO_RUST) or path.endswith('.md'):
            continue
        else:
            whole = True
            reasons.append(f'{path} is not owned by a workspace package')
    return (None if whole else owned), tooling, reasons


def dependents(selected: set[str], metadata: dict) -> set[str]:
    """`selected` plus every workspace package that depends on one of them."""
    members = {package['name']: package for package in workspace_packages(metadata)}
    reverse: dict[str, set[str]] = {name: set() for name in members}
    for name, package in members.items():
        for dependency in package['dependencies']:
            if dependency['name'] in reverse:
                reverse[dependency['name']].add(name)
    result, pending = set(), list(selected)
    while pending:
        name = pending.pop()
        if name not in result:
            result.add(name)
            pending.extend(reverse.get(name, ()))
    return result


def boundary(offline: list[str], env: dict) -> None:
    step([sys.executable, 'tools/ci/boundary.py', *offline], env)


def ci_tool_tests(env: dict) -> None:
    step([sys.executable, '-m', 'unittest', 'discover', '-s', 'tools/ci/tests'], env)


def check_fast(base: str | None, offline: list[str]) -> None:
    env = environment()
    metadata = cargo_metadata('--no-deps', env=env)
    packages = {package['name']: relative_directory(package['manifest_path'], Path(metadata['workspace_root']))
                for package in workspace_packages(metadata)}
    if base:
        paths = changed_paths(base)
        owned, tooling, reasons = classify(paths, packages)
        print(f'check-fast: {len(paths)} path(s) changed since {base}')
        for reason in reasons:
            print(f'  {reason}')
        selected = None if owned is None else dependents(owned, metadata)
    else:
        print('check-fast: BASE is unset; checking the whole workspace')
        selected, tooling = None, True

    step(['cargo', 'fmt', '--all', '--check'], env)
    boundary(offline, env)
    if tooling:
        ci_tool_tests(env)
    if selected is not None and not selected:
        print('check-fast: no Rust package is affected; clippy and tests skipped')
        return
    scope = ['--workspace'] if selected is None else [arg for name in sorted(selected) for arg in ('-p', name)]
    print('check-fast: Rust scope ' + ('whole workspace' if selected is None else ', '.join(sorted(selected))))
    step(['cargo', 'clippy', '--locked', *offline, '--all-targets', *scope, '--', '-D', 'warnings'], env)
    step(['cargo', 'test', '--locked', *offline, '--profile', 'release-fast', *scope], env)


def check_package(package: str, test: str | None, features: str | None, offline: list[str]) -> None:
    env = environment()
    metadata = cargo_metadata('--no-deps', env=env)
    names = sorted(package['name'] for package in workspace_packages(metadata))
    if package not in names:
        raise SystemExit(f'PACKAGE={package} is not a workspace package; choose one of: {", ".join(names)}')
    selection = ['-p', package, *(['--features', features] if features else [])]
    step(['cargo', 'fmt', '--check', '-p', package], env)
    boundary(offline, env)
    step(['cargo', 'clippy', '--locked', *offline, '--all-targets', *selection, '--', '-D', 'warnings'], env)
    step(['cargo', 'test', '--locked', *offline, '--profile', 'release-fast', *selection,
          *([test] if test else [])], env)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--base', help='compare against this commit (check-fast)')
    parser.add_argument('--package', help='check one package (check-package)')
    parser.add_argument('--test', help='test-name filter, with --package')
    parser.add_argument('--features', help='comma-separated features, with --package')
    parser.add_argument('--offline', action='store_true', help='pass --offline to cargo')
    parser.add_argument('--dry-run', action='store_true',
                        help='print the selection and the commands without running them')
    args = parser.parse_args()
    global DRY_RUN
    DRY_RUN = args.dry_run
    offline = ['--offline'] if args.offline else []
    if args.package:
        if args.base:
            parser.error('--base applies to check-fast, not --package')
        check_package(args.package, args.test, args.features, offline)
    else:
        if args.test or args.features:
            parser.error('--test and --features need --package')
        check_fast(args.base, offline)


if __name__ == '__main__':
    main()
