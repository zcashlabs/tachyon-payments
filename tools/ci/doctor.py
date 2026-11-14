#!/usr/bin/env python3
"""Check local prerequisites for building and testing this workspace.

Checks:

- the toolchain pinned in rust-toolchain.toml (1.97.1) is installed, and is the
  one cargo selects in this checkout (rustup is told not to install anything);
- cargo, rustfmt and clippy run under it;
- git and make are present, and python3 is 3.11 or newer (tomllib);
- the pinned git and crates.io dependencies are fetchable: `cargo fetch --locked`
  (this downloads into Cargo's cache; it changes nothing in the checkout);
- with --network (`make doctor NETWORK=1`): the Tachyon alpha testnet RPC answers
  `getblockcount`, a read-only call, with a positive height.

Prints one line per check and exits non-zero if any fails.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tomllib
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, environment  # noqa: E402

TESTNET_RPC = 'http://tachyscan.cnode.phd:18232'
MIN_PYTHON = (3, 11)


def probe(command: list[str], env: dict, timeout: float = 60) -> tuple[bool, str]:
    """(succeeded, first line of output or the reason it failed)."""
    try:
        result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return False, f'timed out after {timeout:g} s'
    except OSError as error:
        return False, error.strerror or str(error)
    lines = [line.strip() for line in (result.stdout + result.stderr).splitlines() if line.strip()]
    if result.returncode == 0:
        return True, lines[0] if lines else 'ok'
    errors = [line for line in lines if line.startswith('error')] or lines
    return False, f'exit {result.returncode}: {errors[-1] if errors else "no output"}'


def rpc_block_count(url: str, timeout: float = 15) -> int:
    body = json.dumps({'jsonrpc': '1.0', 'id': 'doctor', 'method': 'getblockcount', 'params': []}).encode()
    request = urllib.request.Request(url, data=body, headers={'Content-Type': 'application/json',
                                                              'User-Agent': 'tachyon-payments-doctor'})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        reply = json.loads(response.read())
    if reply.get('error'):
        raise ValueError(f'RPC error: {reply["error"]}')
    height = reply.get('result')
    if not isinstance(height, int) or height <= 0:
        raise ValueError(f'unexpected getblockcount result: {height!r}')
    return height


def inspect(network: bool, rpc_url: str) -> list[tuple[str, bool, str]]:
    env = environment()
    env['RUSTUP_AUTO_INSTALL'] = '0'
    env['GIT_TERMINAL_PROMPT'] = '0'
    channel = tomllib.loads((ROOT / 'rust-toolchain.toml').read_text())['toolchain']['channel']
    remedy = f'rustup toolchain install {channel} --component rustfmt --component clippy'
    rows = []

    def add(name, ok, detail, fix=None):
        rows.append((name, ok, detail if ok or not fix else f'{detail}; fix: {fix}'))

    if shutil.which('rustup', path=env.get('PATH')) is None:
        add('rustup', False, 'not found on PATH', 'install rustup from https://rustup.rs')
    else:
        ok, detail = probe(['rustup', 'run', channel, 'rustc', '--version'], env)
        add(f'toolchain {channel}', ok and detail.startswith(f'rustc {channel} '), detail, remedy)
        ok, detail = probe(['rustc', '--version'], env)
        add('toolchain selected in this checkout', ok and detail.startswith(f'rustc {channel} '), detail,
            'remove a RUSTUP_TOOLCHAIN override or a rustup directory override for this checkout')
        for name, command in (('cargo', ['cargo', '--version']), ('rustfmt', ['rustfmt', '--version']),
                              ('clippy', ['cargo', 'clippy', '--version'])):
            ok, detail = probe(['rustup', 'run', channel, *command], env)
            add(name, ok, detail, remedy)
    for tool in ('git', 'make'):
        path = shutil.which(tool, path=env.get('PATH'))
        add(tool, path is not None, path or 'not found on PATH', f'install {tool}')
    version = sys.version_info
    add('python3', version[:2] >= MIN_PYTHON, f'{sys.executable} {version.major}.{version.minor}.{version.micro}',
        f'install Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]} or newer')
    ok, detail = probe(['cargo', 'fetch', '--locked'], env, timeout=600)
    add('dependencies fetchable (cargo fetch --locked)', ok,
        'every locked dependency is fetched' if ok else detail,
        'check git access to github.com/valargroup/wallet-pir and valargroup/ipir-sp, '
        'and that Cargo.lock is current')
    if network:
        try:
            height = rpc_block_count(rpc_url)
            add(f'testnet RPC getblockcount ({rpc_url})', True, f'height {height}')
        except (OSError, ValueError, urllib.error.URLError) as error:
            add(f'testnet RPC getblockcount ({rpc_url})', False, str(error),
                'check network access to the Tachyon alpha testnet node')
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--network', action='store_true', help='also probe the testnet RPC (read-only)')
    parser.add_argument('--rpc-url', default=TESTNET_RPC)
    args = parser.parse_args()
    rows = inspect(args.network, args.rpc_url)
    width = max(len(name) for name, _, _ in rows)
    for name, ok, detail in rows:
        print(f'{"ok  " if ok else "FAIL"}  {name:<{width}}  {detail}')
    failed = [name for name, ok, _ in rows if not ok]
    if failed:
        raise SystemExit(f'doctor: {len(failed)} check(s) failed: {", ".join(failed)}')
    print(f'doctor: all {len(rows)} checks passed')


if __name__ == '__main__':
    main()
