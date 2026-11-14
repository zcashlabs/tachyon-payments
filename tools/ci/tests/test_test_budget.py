"""Tests for tools/ci/test_budget.py: a scripted libtest double for the edge
cases, and one real stable-Rust crate to prove the protocol it reads."""
import json
import os
import shutil
import sys
import tempfile
import textwrap
import time
import tomllib
import unittest
from pathlib import Path

CI = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CI))
import test_budget  # noqa: E402
from common import ROOT, environment  # noqa: E402

# A stand-in for a libtest executable. FAKE_PLAN is a JSON list of
# [name, seconds, status]; FAKE_LIST optionally lists more names than it runs;
# FAKE_STALL sleeps before the first test.
FAKE = r'''
import json, os, sys, time
plan = json.loads(os.environ['FAKE_PLAN'])
names = json.loads(os.environ.get('FAKE_LIST', 'null')) or [name for name, _, _ in plan]
if '--list' in sys.argv:
    ignored = '--ignored' in sys.argv
    for name, _, status in plan:
        if not ignored or status == 'ignored':
            print(f'{name}: test')
    for name in [] if ignored else names[len(plan):]:
        print(f'{name}: test')
    sys.exit(0)
assert '--test-threads=1' in sys.argv and '--format=pretty' in sys.argv
print(f'\nrunning {len(names)} tests', flush=True)
time.sleep(float(os.environ.get('FAKE_STALL', '0')))
failed = False
for name, seconds, status in plan:
    sys.stdout.write(f'test {name} ... ')
    sys.stdout.flush()
    time.sleep(seconds)
    print(status, flush=True)
    failed |= status == 'FAILED'
print(f'\ntest result: {"FAILED" if failed else "ok"}. {len(plan)} passed', flush=True)
sys.exit(101 if failed else 0)
'''


class FakeExecutableTest(unittest.TestCase):
    def setUp(self):
        self.directory = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.directory)
        self.executable = self.directory / 'fake-tests'
        self.executable.write_text(f'#!{sys.executable}\n{FAKE}')
        self.executable.chmod(0o755)

    def run_fake(self, plan, seconds=5.0, **extra):
        program = test_budget.Program(executable=str(self.executable), package='fake', version='0.1.0',
                                      manifest_dir=str(self.directory), target='fake')
        env = dict(os.environ, FAKE_PLAN=json.dumps(plan),
                   **{key: json.dumps(value) if not isinstance(value, str) else value
                      for key, value in extra.items()})
        return test_budget.run_program(program, seconds=seconds, env=env, deadline=None)

    def test_passing_tests_are_timed_individually(self):
        outcome = self.run_fake([['a::fast', 0.0, 'ok'], ['b::slower', 0.4, 'ok']])
        self.assertEqual(outcome.problems, [])
        times = {case['test']: case['seconds'] for case in outcome.cases}
        self.assertLess(times['a::fast'], 0.3)
        self.assertGreaterEqual(times['b::slower'], 0.4)
        self.assertLess(times['b::slower'], 1.5)

    def test_a_test_over_budget_is_killed_at_the_budget(self):
        started = time.monotonic()
        outcome = self.run_fake([['quick', 0.0, 'ok'], ['stuck', 30.0, 'ok'], ['after', 0.0, 'ok']],
                                seconds=0.5)
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(outcome.problems, ['fake::fake (fake-tests): stuck reached the 0.5 s budget; killed'])
        self.assertEqual([case['status'] for case in outcome.cases], ['ok', 'timeout'])

    def test_a_test_that_finishes_just_over_budget_fails(self):
        outcome = self.run_fake([['slow', 0.6, 'ok']], seconds=0.55)
        self.assertEqual(len(outcome.problems), 1)
        self.assertIn('slow', outcome.problems[0])

    def test_failed_test_and_exit_status_are_reported(self):
        outcome = self.run_fake([['good', 0.0, 'ok'], ['bad', 0.0, 'FAILED']])
        self.assertIn('fake::fake (fake-tests): bad FAILED after 0.00 s', outcome.problems)
        self.assertIn('fake::fake (fake-tests): exited with status 101', outcome.problems)

    def test_ignored_tests_are_refused(self):
        outcome = self.run_fake([['kept', 0.0, 'ok'], ['skipped', 0.0, 'ignored']])
        self.assertTrue(any('skipped is #[ignore]d' in problem for problem in outcome.problems))

    def test_listed_tests_that_never_run_are_refused(self):
        outcome = self.run_fake([['ran', 0.0, 'ok']], FAKE_LIST=['ran', 'vanished'])
        self.assertEqual(outcome.problems, ["fake::fake (fake-tests): tests listed but not run: ['vanished']"])

    def test_time_outside_any_test_is_bounded(self):
        started = time.monotonic()
        outcome = self.run_fake([['late', 0.0, 'ok']], seconds=0.5, FAKE_STALL='30')
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(outcome.problems, ['fake::fake (fake-tests): 0.5 s passed outside any test; killed'])

    def test_overall_deadline_stops_the_run(self):
        program = test_budget.Program(executable=str(self.executable), package='fake', version='0.1.0',
                                      manifest_dir=str(self.directory), target='fake')
        env = dict(os.environ, FAKE_PLAN=json.dumps([['long', 10.0, 'ok']]))
        outcome = test_budget.run_program(program, seconds=30, env=env, deadline=time.monotonic() + 0.5)
        self.assertEqual(outcome.problems, ['fake::fake (fake-tests): stopped at the overall deadline '
                                            'while running long'])


class StableLibtestTest(unittest.TestCase):
    """Builds a tiny crate with the pinned toolchain and times its real tests."""

    def test_real_libtest_protocol_on_the_pinned_toolchain(self):
        directory = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, directory)
        (directory / 'src').mkdir()
        (directory / 'Cargo.toml').write_text(textwrap.dedent('''\
            [package]
            name = "budget-probe"
            version = "0.1.0"
            edition = "2021"

            [workspace]
            '''))
        (directory / 'src/lib.rs').write_text(textwrap.dedent('''\
            /// ```
            /// assert_eq!(budget_probe::double(2), 4);
            /// ```
            pub fn double(x: u32) -> u32 {
                x * 2
            }

            #[cfg(test)]
            mod tests {
                #[test]
                fn a_fast() {
                    assert_eq!(1 + 1, 2);
                }

                #[test]
                #[should_panic(expected = "boom")]
                fn b_should_panic() {
                    panic!("boom");
                }

                #[test]
                fn c_slow() {
                    std::thread::sleep(std::time::Duration::from_millis(1500));
                }
            }
            '''))
        channel = tomllib.loads((ROOT / 'rust-toolchain.toml').read_text())['toolchain']['channel']
        env = dict(environment(), RUSTUP_TOOLCHAIN=channel, CARGO_TARGET_DIR=str(directory / 'target'))
        programs = test_budget.build(['--offline', '--manifest-path', str(directory / 'Cargo.toml')], env)
        self.assertEqual([(p.package, p.target, p.harness) for p in programs], [('budget-probe', 'budget_probe', True)])

        within = test_budget.run_programs(programs, seconds=30, jobs=1, env=env)[0]
        self.assertEqual(within.problems, [])
        times = {case['test']: case['seconds'] for case in within.cases}
        self.assertEqual(set(times), {'tests::a_fast', 'tests::b_should_panic', 'tests::c_slow'})
        self.assertGreaterEqual(times['tests::c_slow'], 1.5)
        self.assertLess(times['tests::a_fast'], 0.5)

        over = test_budget.run_programs(programs, seconds=1.0, jobs=1, env=env)[0]
        self.assertEqual(over.problems, ['budget-probe::budget_probe (%s): tests::c_slow reached the 1 s '
                                         'budget; killed' % Path(programs[0].executable).name])

        doctests = test_budget.run_doctests(['--offline', '--manifest-path', str(directory / 'Cargo.toml')],
                                            seconds=30, env=env)
        self.assertEqual(doctests.problems, [])
        self.assertEqual([(case['test'], case['status']) for case in doctests.cases],
                         [('src/lib.rs - double (line 1)', 'ok')])


if __name__ == '__main__':
    unittest.main()
