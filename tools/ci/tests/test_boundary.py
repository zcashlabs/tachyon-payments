"""Unit tests for tools/ci/boundary.py over synthetic `cargo metadata` documents."""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

CI = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CI))
import boundary  # noqa: E402

WS = '/ws'


def package_id(name):
    return f'path+file://{WS}/{name}#0.1.0'


class Metadata:
    """Builds a metadata document with the fields boundary.py reads."""

    def __init__(self):
        self.packages = {}
        self.members = []
        self.deps = {}

    def member(self, name, directory):
        self.packages[name] = {'id': package_id(name), 'name': name,
                               'manifest_path': f'{WS}/{directory}/Cargo.toml'}
        self.members.append(name)
        self.deps.setdefault(name, [])
        return self

    def external(self, name):
        self.packages[name] = {'id': f'registry+https://example/{name}#1.0.0', 'name': name,
                               'manifest_path': f'/registry/{name}/Cargo.toml'}
        self.deps.setdefault(name, [])
        return self

    def edge(self, source, target, *kinds):
        self.deps[source].append((target, list(kinds) or [None]))
        return self

    def build(self):
        nodes = [{'id': self.packages[name]['id'],
                  'deps': [{'name': target.replace('-', '_'), 'pkg': self.packages[target]['id'],
                            'dep_kinds': [{'kind': kind, 'target': None} for kind in kinds]}
                           for target, kinds in edges]}
                 for name, edges in self.deps.items()]
        return {'packages': list(self.packages.values()),
                'workspace_members': [self.packages[name]['id'] for name in self.members],
                'resolve': {'nodes': nodes, 'root': None},
                'workspace_root': WS, 'version': 1}


def current_layout():
    """The two packages that exist today, with no edges."""
    return Metadata().member('tachyon-chain', 'src/shared/tachyon-chain') \
                     .member('tachyon-fixtures', 'tools/fixtures')


def full_layout():
    """Every planned package at its spec 4.2 path, wired the way the spec intends."""
    config = boundary.load_config()
    metadata = Metadata()
    for name, directory in config['packages'].items():
        metadata.member(name, directory)
    metadata.external('pir-node-rpc').external('pir-pipeline').external('serde')
    metadata.edge('tachyon-chain', 'pir-node-rpc')
    metadata.edge('tachyon-pir-table', 'tachyon-pir-protocol')
    metadata.edge('tachyon-pir-protocol', 'serde')
    for family in ('memo', 'tachygram', 'registry'):
        metadata.edge(f'{family}-store', 'tachyon-pir-protocol')
        metadata.edge(f'{family}-publisher', f'{family}-store')
        metadata.edge(f'{family}-publisher', 'tachyon-pir-table')
        metadata.edge(f'{family}-publisher', 'tachyon-chain')
        metadata.edge(f'{family}-publisher', 'tachyon-fixtures', 'dev')
        metadata.edge('tachyon-pir-publisher', f'{family}-publisher')
    metadata.edge('tachyon-pir-query-server', 'tachyon-pir-table')
    metadata.edge('tachyon-pir-query-server', 'pir-pipeline')
    for host in ('tachyon-pir-publisher', 'tachyon-pir-query-server', 'tachyon-chain', 'tachyon-fixtures'):
        metadata.edge('tachyon-pir-e2e-tests', host)
    metadata.edge('tachyon-pir-cli', 'tachyon-pir-protocol')
    metadata.edge('tachyon-fixtures', 'tachyon-chain')
    return metadata


def check(metadata, config=None):
    return boundary.check(metadata.build(), config or boundary.load_config())


class BoundaryTest(unittest.TestCase):
    def test_current_workspace_shape_passes_and_notes_planned_packages(self):
        errors, notes = check(current_layout())
        self.assertEqual(errors, [])
        planned = {note.split(':')[0] for note in notes}
        self.assertIn('tachyon-pir-query-server', planned)
        self.assertNotIn('tachyon-chain', planned)
        self.assertEqual(len(notes), len(boundary.load_config()['packages']) - 2)

    def test_complete_intended_layout_passes(self):
        self.assertEqual(check(full_layout()), ([], []))

    def test_serving_reaches_chain_transitively(self):
        metadata = full_layout().external('helper')
        metadata.edge('tachyon-pir-table', 'helper').edge('helper', 'tachyon-chain')
        errors, _ = check(metadata)
        self.assertIn('tachyon-pir-table: serving package reaches chain/ingestion package '
                      'tachyon-chain (normal/build: tachyon-pir-table -> helper -> tachyon-chain)', errors)
        # The query host reaches it through the table, and the chain crate's own
        # transport comes with it.
        offenders = {(error.split(':')[0], error.split(' package ')[-1].split(' ')[0]) for error in errors}
        self.assertEqual(offenders, {(serving, target)
                                     for serving in ('tachyon-pir-table', 'tachyon-pir-query-server')
                                     for target in ('tachyon-chain', 'pir-node-rpc')})

    def test_serving_reaches_node_rpc_through_build_dependency(self):
        metadata = full_layout().edge('tachyon-pir-protocol', 'pir-node-rpc', 'build')
        errors, _ = check(metadata)
        # The protocol crate and both serving packages above it are reported.
        self.assertEqual(len(errors), 3)
        self.assertTrue(all('pir-node-rpc' in error for error in errors))

    def test_query_host_may_not_reach_a_product_store(self):
        errors, _ = check(full_layout().edge('tachyon-pir-query-server', 'memo-store'))
        self.assertEqual(errors, ['tachyon-pir-query-server: serving package reaches chain/ingestion '
                                  'package memo-store (normal/build: tachyon-pir-query-server -> memo-store)'])

    def test_serving_direct_dev_dependency_on_fixtures_is_refused(self):
        errors, _ = check(full_layout().edge('tachyon-pir-table', 'tachyon-fixtures', 'dev'))
        self.assertEqual(errors, ['tachyon-pir-table: serving package reaches chain/ingestion package '
                                  'tachyon-fixtures (dev: tachyon-pir-table -> tachyon-fixtures)'])

    def test_dev_dependencies_are_checked_directly_not_transitively(self):
        # A serving test may use a tool that itself reads the chain; only the
        # direct dev edge is checked, as the rule states.
        errors, _ = check(full_layout().edge('tachyon-pir-table', 'tachyon-pir-cli', 'dev')
                          .edge('tachyon-pir-cli', 'tachyon-chain'))
        self.assertEqual(errors, [])

    def test_products_never_depend_on_each_other(self):
        errors, _ = check(full_layout().edge('memo-publisher', 'tachygram-store'))
        self.assertEqual(errors, ['memo-publisher: memo product package reaches tachygram product package '
                                  'tachygram-store (normal/build: memo-publisher -> tachygram-store)'])

    def test_product_dependency_through_a_shared_crate_is_found(self):
        metadata = full_layout().external('shim')
        metadata.edge('registry-store', 'shim').edge('shim', 'memo-store')
        errors, _ = check(metadata)
        self.assertIn('registry-store: registry product package reaches memo product package memo-store '
                      '(normal/build: registry-store -> shim -> memo-store)', errors)
        # registry-publisher reaches it through registry-store too.
        self.assertTrue(any(error.startswith('registry-publisher:') for error in errors))

    def test_product_dev_dependency_across_families_is_refused(self):
        errors, _ = check(full_layout().edge('tachygram-store', 'registry-store', 'dev'))
        self.assertEqual(errors, ['tachygram-store: tachygram product package reaches registry product '
                                  'package registry-store (dev: tachygram-store -> registry-store)'])

    def test_hosts_and_tools_may_depend_on_anything(self):
        metadata = full_layout()
        for target in ('tachyon-pir-query-server', 'memo-store', 'tachygram-store', 'registry-publisher'):
            metadata.edge('tachyon-pir-cli', target)
            metadata.edge('tachyon-fixtures', target, 'dev')
        self.assertEqual(check(metadata), ([], []))

    def test_unclassified_workspace_package_is_an_error(self):
        errors, _ = check(current_layout().member('tachyon-oss', 'src/tachyon-oss'))
        self.assertEqual(errors, ['tachyon-oss: workspace package is not classified in tools/ci/boundary.json'])

    def test_package_at_wrong_directory_is_an_error(self):
        errors, _ = check(Metadata().member('tachyon-chain', 'src/tachyon-chain'))
        self.assertEqual(errors, ['tachyon-chain: found at src/tachyon-chain, but boundary.json places it '
                                  'at src/shared/tachyon-chain'])

    def test_configuration_errors_are_reported_before_graph_checks(self):
        config = boundary.load_config()
        config['serving'] = config['serving'] + ['tachyon-chain', 'nonexistent']
        config['families'] = dict(config['families'], extra=['memo-store'])
        errors, _ = check(current_layout(), config)
        self.assertIn('boundary.json: serving package nonexistent is not listed under packages', errors)
        self.assertIn('boundary.json: tachyon-chain is both serving and chain_or_ingestion', errors)
        self.assertIn('boundary.json: memo-store is in families memo and extra', errors)

    def test_checked_in_configuration_is_consistent(self):
        self.assertEqual(boundary.config_errors(boundary.load_config()), [])

    def test_command_line_exit_status(self):
        with tempfile.TemporaryDirectory() as directory:
            good = Path(directory) / 'good.json'
            bad = Path(directory) / 'bad.json'
            good.write_text(json.dumps(full_layout().build()))
            bad.write_text(json.dumps(full_layout().edge('tachyon-pir-protocol', 'tachyon-chain').build()))
            script = str(CI / 'boundary.py')
            passed = subprocess.run([sys.executable, script, '--metadata', str(good)],
                                    capture_output=True, text=True)
            self.assertEqual(passed.returncode, 0, passed.stdout + passed.stderr)
            self.assertIn('boundary check passed', passed.stdout)
            failed = subprocess.run([sys.executable, script, '--metadata', str(bad)],
                                    capture_output=True, text=True)
            self.assertEqual(failed.returncode, 1)
            self.assertIn('error: tachyon-pir-protocol: serving package reaches', failed.stdout)


if __name__ == '__main__':
    unittest.main()
