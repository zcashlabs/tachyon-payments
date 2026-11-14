"""Tests for the affected-package selection in tools/ci/fast.py."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fast  # noqa: E402

PACKAGES = {'tachyon-chain': 'src/shared/tachyon-chain', 'tachyon-fixtures': 'tools/fixtures',
            'tachyon-pir-table': 'src/shared/tachyon-pir-table',
            'tachyon-pir-table-extra': 'src/shared/tachyon-pir-table-extra'}


def metadata(edges):
    return {'workspace_members': [f'id-{name}' for name in PACKAGES],
            'packages': [{'id': f'id-{name}', 'name': name,
                          'dependencies': [{'name': target} for target in edges.get(name, [])]}
                         for name in PACKAGES]}


class ClassifyTest(unittest.TestCase):
    def test_package_paths_select_their_owner(self):
        owned, tooling, _ = fast.classify(['src/shared/tachyon-chain/src/lib.rs',
                                           'src/shared/tachyon-pir-table-extra/Cargo.toml'], PACKAGES)
        self.assertEqual(owned, {'tachyon-chain', 'tachyon-pir-table-extra'})
        self.assertFalse(tooling)

    def test_documentation_selects_nothing(self):
        self.assertEqual(fast.classify(['docs/memo/README.md', 'evidence/README.md', 'AGENTS.md',
                                        'docs/spec/spec-tachyon-payment-pir.md'], PACKAGES),
                         (set(), False, []))

    def test_root_build_inputs_select_the_workspace(self):
        for path in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', '.cargo/config.toml'):
            owned, _, reasons = fast.classify([path], PACKAGES)
            self.assertIsNone(owned, path)
            self.assertEqual(reasons, [f'{path} affects every package'])

    def test_unknown_paths_select_the_workspace(self):
        owned, _, reasons = fast.classify(['src/tachyon-pir-publisher/src/main.rs'], PACKAGES)
        self.assertIsNone(owned)
        self.assertEqual(reasons, ['src/tachyon-pir-publisher/src/main.rs is not owned by a workspace package'])

    def test_ci_tooling_runs_tool_tests(self):
        self.assertEqual(fast.classify(['tools/ci/boundary.json', 'Makefile'], PACKAGES), (set(), True, []))

    def test_dependents_are_added_transitively(self):
        graph = metadata({'tachyon-fixtures': ['tachyon-chain'], 'tachyon-pir-table-extra': ['tachyon-fixtures']})
        self.assertEqual(fast.dependents({'tachyon-chain'}, graph),
                         {'tachyon-chain', 'tachyon-fixtures', 'tachyon-pir-table-extra'})
        self.assertEqual(fast.dependents({'tachyon-pir-table'}, graph), {'tachyon-pir-table'})


if __name__ == '__main__':
    unittest.main()
