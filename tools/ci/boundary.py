#!/usr/bin/env python3
"""Enforce the dependency rules of spec section 4.2 on the resolved Cargo graph.

Rules, with package sets taken from tools/ci/boundary.json:

(a) Serving code never depends on chain or ingestion code. No package in
    `serving` may reach a package in `chain_or_ingestion` through its normal or
    build dependencies, transitively, nor through a direct dev-dependency.
(b) Product crates never depend on each other. No package of one product
    family may reach a package of another family, by the same edges.
    first-contact is served from the memo crates and is part of the memo family.
(c) Hosts and tools may depend on anything; rules (a) and (b) still apply to the
    serving query host, which is listed in `serving`.

The graph comes from `cargo metadata --format-version 1 --all-features`, so
optional dependencies behind product features (`memo`, `first-contact`,
`tachygram`, `registry`) and target-specific dependencies are all included.
Packages listed in boundary.json that are not yet in the workspace are skipped
with a note. A workspace package missing from boundary.json, or found at a
different directory than listed, is an error: every new crate must be
classified when it is added.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, cargo_metadata, relative_directory, workspace_packages  # noqa: E402

CONFIG = ROOT / 'tools/ci/boundary.json'
CONFIG_KEYS = {'comment', 'packages', 'external', 'serving', 'chain_or_ingestion', 'families'}
# Edge kinds that put a dependency's code into a package's own build. Cargo
# reports a normal edge as kind null.
LINKED_KINDS = {None, 'build'}


def load_config(path: Path = CONFIG) -> dict:
    return json.loads(path.read_text())


def config_errors(config: dict) -> list[str]:
    """Problems with boundary.json itself: unknown keys, names that are not
    classified, and sets that overlap where they must not."""
    errors = []
    unknown = set(config) - CONFIG_KEYS
    if unknown:
        errors.append(f'boundary.json: unknown keys {sorted(unknown)}')
    packages = config.get('packages', {})
    external = config.get('external', {})
    serving = config.get('serving', [])
    chain = config.get('chain_or_ingestion', [])
    families = config.get('families', {})
    for name in set(packages) & set(external):
        errors.append(f'boundary.json: {name} is listed as both a workspace and an external package')
    for name in serving:
        if name not in packages:
            errors.append(f'boundary.json: serving package {name} is not listed under packages')
    for name in chain:
        if name not in packages and name not in external:
            errors.append(f'boundary.json: chain_or_ingestion package {name} is not listed '
                          'under packages or external')
    for name in sorted(set(serving) & set(chain)):
        errors.append(f'boundary.json: {name} is both serving and chain_or_ingestion')
    owner: dict[str, str] = {}
    for family, members in families.items():
        for name in members:
            if name not in packages:
                errors.append(f'boundary.json: family {family} member {name} is not listed under packages')
            if name in owner:
                errors.append(f'boundary.json: {name} is in families {owner[name]} and {family}')
            owner[name] = family
    return errors


class Graph:
    """The resolved dependency graph, keyed by package id."""

    def __init__(self, metadata: dict):
        self.names = {package['id']: package['name'] for package in metadata['packages']}
        self.edges: dict[str, list[tuple[str, set]]] = {}
        for node in metadata['resolve']['nodes']:
            self.edges[node['id']] = [
                (dep['pkg'], {kind['kind'] for kind in dep.get('dep_kinds', [])})
                for dep in node.get('deps', [])
            ]

    def linked_paths(self, start: str) -> dict[str, list[str]]:
        """Every package reachable from `start` through normal and build edges,
        with one shortest path of names leading to it."""
        paths = {start: [self.names[start]]}
        queue = deque([start])
        while queue:
            current = queue.popleft()
            for target, kinds in self.edges.get(current, []):
                if target not in paths and kinds & LINKED_KINDS:
                    paths[target] = paths[current] + [self.names[target]]
                    queue.append(target)
        return paths

    def direct_dev(self, start: str) -> list[str]:
        """Packages `start` names as dev-dependencies."""
        return [target for target, kinds in self.edges.get(start, []) if 'dev' in kinds]


def check(metadata: dict, config: dict) -> tuple[list[str], list[str]]:
    """Return (errors, notes) for one metadata document."""
    errors = config_errors(config)
    notes = []
    if errors:
        return errors, notes
    root = Path(metadata['workspace_root'])
    members = {package['name']: package for package in workspace_packages(metadata)}
    listed = config['packages']

    for name, package in sorted(members.items()):
        if name not in listed:
            errors.append(f'{name}: workspace package is not classified in tools/ci/boundary.json')
            continue
        directory = relative_directory(package['manifest_path'], root)
        if directory != listed[name]:
            errors.append(f'{name}: found at {directory}, but boundary.json places it at {listed[name]}')
    for name, directory in sorted(listed.items()):
        if name not in members:
            notes.append(f'{name}: planned at {directory}, not in the workspace yet; skipped')

    graph = Graph(metadata)
    ids = {package['name']: package['id'] for package in members.values()}
    forbidden_for_serving = set(config['chain_or_ingestion'])
    family_of = {name: family for family, names in config['families'].items() for name in names}

    def reached(start_id: str):
        """(name, path, via) for every package `start_id` may not escape to:
        transitive normal/build edges, then direct dev edges."""
        for target, path in graph.linked_paths(start_id).items():
            if target != start_id:
                yield graph.names[target], path, 'normal/build'
        for target in graph.direct_dev(start_id):
            yield graph.names[target], [graph.names[start_id], graph.names[target]], 'dev'

    for name in config['serving']:
        if name not in ids:
            continue
        for target, path, via in reached(ids[name]):
            if target in forbidden_for_serving:
                errors.append(f'{name}: serving package reaches chain/ingestion package {target} '
                              f'({via}: {" -> ".join(path)})')

    for name, family in sorted(family_of.items()):
        if name not in ids:
            continue
        for target, path, via in reached(ids[name]):
            other = family_of.get(target)
            if other is not None and other != family:
                errors.append(f'{name}: {family} product package reaches {other} product package '
                              f'{target} ({via}: {" -> ".join(path)})')

    return sorted(set(errors)), notes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--metadata', type=Path,
                        help='read this cargo metadata JSON instead of running cargo')
    parser.add_argument('--config', type=Path, default=CONFIG)
    parser.add_argument('--offline', action='store_true', help='pass --offline to cargo')
    args = parser.parse_args()
    if args.metadata:
        metadata = json.loads(args.metadata.read_text())
    else:
        metadata = cargo_metadata('--all-features', '--locked', *(['--offline'] if args.offline else []))
    errors, notes = check(metadata, load_config(args.config))
    for note in notes:
        print(f'note: {note}')
    for error in errors:
        print(f'error: {error}')
    checked = len(workspace_packages(metadata))
    if errors:
        raise SystemExit(f'boundary check failed: {len(errors)} violation(s) across {checked} workspace packages')
    print(f'boundary check passed: {checked} workspace packages classified, '
          f'{len(notes)} planned package(s) not yet present')


if __name__ == '__main__':
    main()
