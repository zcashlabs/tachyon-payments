"""Shared helpers for the CI gates. Python 3 standard library only."""
from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def environment() -> dict[str, str]:
    """The process environment, with rustup's default bin directory on PATH when
    cargo is not already reachable (non-login shells often lack it)."""
    env = dict(os.environ)
    if shutil.which('cargo', path=env.get('PATH')) is None:
        cargo_bin = Path(env.get('CARGO_HOME', Path.home() / '.cargo')) / 'bin'
        if (cargo_bin / 'cargo').exists():
            env['PATH'] = os.pathsep.join([str(cargo_bin), env.get('PATH', '')])
    return env


def run(command: list[str], *, env: dict[str, str] | None = None, cwd: Path = ROOT) -> float:
    """Run one command in the foreground, echoing it; raise on failure. Returns
    its wall-clock seconds."""
    print('+ ' + shlex.join(command), flush=True)
    started = time.monotonic()
    result = subprocess.run(command, cwd=cwd, env=env or environment())
    elapsed = time.monotonic() - started
    if result.returncode != 0:
        raise SystemExit(f'command failed (exit {result.returncode}) after {elapsed:.1f}s: '
                         + shlex.join(command))
    return elapsed


def cargo_metadata(*args: str, env: dict[str, str] | None = None) -> dict:
    """`cargo metadata --format-version 1` with extra arguments. Cargo's warnings
    go to stderr and are passed through; stdout is the JSON document."""
    command = ['cargo', 'metadata', '--format-version', '1', *args]
    result = subprocess.run(command, cwd=ROOT, env=env or environment(),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        sys.stderr.write(result.stderr.decode(errors='replace'))
        raise SystemExit('cargo metadata failed: ' + shlex.join(command))
    return json.loads(result.stdout)


def workspace_packages(metadata: dict) -> list[dict]:
    """The workspace member packages of a metadata document."""
    members = set(metadata['workspace_members'])
    return [package for package in metadata['packages'] if package['id'] in members]


def relative_directory(manifest_path: str, root: Path = ROOT) -> str:
    """A package's directory relative to the workspace root, POSIX style."""
    return Path(manifest_path).parent.resolve().relative_to(root.resolve()).as_posix()
