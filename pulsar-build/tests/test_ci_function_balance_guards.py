#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""Verify whole-class Function/Go balancing using recording dispatcher stubs."""

from contextlib import contextmanager, redirect_stdout
import io
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "pulsar-build"))
import verify_ci_test_partitions as guard


class FunctionBalanceGuardTest(unittest.TestCase):
    @contextmanager
    def checkout(self):
        with tempfile.TemporaryDirectory(prefix="pulsar-function-balance-") as directory:
            temporary = Path(directory)
            resources = temporary / "tests/integration/src/test/resources"
            resources.mkdir(parents=True)
            for filename in ("pulsar-function.xml", "pulsar-function-balanced.xml",
                             "pulsar-function-metadata-state.xml", "pulsar-process-go.xml"):
                shutil.copyfile(ROOT / "tests/integration/src/test/resources" / filename, resources / filename)
            for filename in ("pulsar-build/run_integration_group_gradle.sh", ".github/workflows/pulsar-ci.yaml"):
                target = temporary / filename
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(ROOT / filename, target)
            previous = guard.ROOT, guard.RESOURCES
            guard.ROOT, guard.RESOURCES = temporary, resources
            try:
                yield temporary, resources
            finally:
                guard.ROOT, guard.RESOURCES = previous

    def verify(self):
        with redirect_stdout(io.StringIO()):
            return guard.verify_function_balance()

    def replace(self, path, old, new):
        source = path.read_text()
        self.assertIn(old, source)
        path.write_text(source.replace(old, new, 1))

    def test_reviewed_balance(self):
        with self.checkout():
            records = self.verify()
        self.assertEqual(["pull_request", "workflow_dispatch"], [record["event"] for record in records])
        for record in records:
            go = record["baseline"]["PULSAR_CONNECTORS_PROCESS_GO"]
            candidate = record["candidate"]["PULSAR_CONNECTORS_PROCESS_GO_BALANCED"]
            self.assertEqual(go, candidate[:-1])
            self.assertFalse(any(argument.startswith("-PtestGroups=") for argument in candidate[-1]))

    def test_whole_class_membership_mutations_fail(self):
        metadata = "org.apache.pulsar.tests.integration.functions.PulsarMetadataStateStoreTest"
        bookkeeper = "org.apache.pulsar.tests.integration.functions.PulsarBKStateStoreTest"
        for kind in ("omit", "duplicate", "method", "wrong moved class"):
            with self.subTest(kind=kind), self.checkout() as (_, resources):
                path = resources / "pulsar-function-metadata-state.xml"
                source = path.read_text()
                original = f'<class name="{metadata}" />'
                replacements = {
                    "omit": "",
                    "duplicate": original + original,
                    "method": f'<class name="{metadata}"><methods><include name="testJavaWordCountFunction" /></methods></class>',
                    "wrong moved class": f'<class name="{bookkeeper}" />',
                }
                path.write_text(source.replace(original, replacements[kind], 1))
                if kind == "wrong moved class":
                    self.replace(resources / "pulsar-function-balanced.xml", bookkeeper, metadata)
                with self.assertRaises(SystemExit):
                    self.verify()

    def test_suite_options_mutations_fail(self):
        mutations = (
            ('preserve-order="true"', 'preserve-order="false"'),
            ('annotations="JDK"', 'annotations="Other"'),
            ('<classes>', '<groups><run><include name="function" /></run></groups><classes>'),
            ('<classes>', '<parameter name="changed" value="true" /><classes>'),
            ('<test name=', '<listeners><listener class-name="example.Listener" /></listeners><test name='),
        )
        for filename in ("pulsar-function-balanced.xml", "pulsar-function-metadata-state.xml"):
            for old, new in mutations:
                with self.subTest(filename=filename, mutation=old), self.checkout() as (_, resources):
                    self.replace(resources / filename, old, new)
                    with self.assertRaises(SystemExit):
                        self.verify()

    def test_remaining_function_class_order_is_preserved(self):
        with self.checkout() as (_, resources):
            path = resources / "pulsar-function-balanced.xml"
            source = path.read_text()
            first = '            <class name="org.apache.pulsar.tests.integration.io.PulsarGenericObjectSinkTest"/>'
            second = '            <class name="org.apache.pulsar.tests.integration.io.sources.GenericRecordSourceTest" />'
            self.assertIn(first + "\n" + second, source)
            path.write_text(source.replace(first + "\n" + second, second + "\n" + first, 1))
            with self.assertRaises(SystemExit):
                self.verify()

    def test_dispatcher_mutations_fail(self):
        metadata = '  gradle_integration_test -PintegrationTestSuiteFile=pulsar-function-metadata-state.xml "$@"'
        go = '  test_group_pulsar_connectors_process_go "$@"'
        balanced = '  gradle_integration_test -PintegrationTestSuiteFile=pulsar-function-balanced.xml "$@"'
        mutations = (
            (metadata, metadata.replace(' "$@"', ' -PtestGroups=function "$@"')),
            (balanced, balanced.replace(' "$@"', ' -PtestGroups=function "$@"')),
            (go, ""),
            (go, go + "\n" + go),
            (go, go.replace(' "$@"', ' -PtestGroups=sink "$@"')),
            (metadata, metadata + "\n" + metadata),
            (metadata, metadata.replace(' "$@"', ' -PtestRetryCount=0 "$@"')),
            (metadata, metadata.replace(' "$@"', ' -PtestJavaVersion=17 "$@"')),
            ('  "$SCRIPT_DIR/pulsar_ci_tool.sh" move_test_reports', ''),
        )
        for old, new in mutations:
            with self.subTest(mutation=new), self.checkout() as (root, _):
                self.replace(root / "pulsar-build/run_integration_group_gradle.sh", old, new)
                with self.assertRaises(SystemExit):
                    self.verify()

    def test_go_must_precede_metadata(self):
        with self.checkout() as (root, _):
            path = root / "pulsar-build/run_integration_group_gradle.sh"
            source = path.read_text()
            go = '  test_group_pulsar_connectors_process_go "$@"'
            metadata = '  gradle_integration_test -PintegrationTestSuiteFile=pulsar-function-metadata-state.xml "$@"'
            self.assertEqual(1, source.count(go))
            self.assertEqual(1, source.count(metadata))
            source = source.replace(go, "__GO_PLACEHOLDER__", 1).replace(metadata, go, 1)
            path.write_text(source.replace("__GO_PLACEHOLDER__", metadata, 1))
            with self.assertRaises(SystemExit):
                self.verify()

    def test_actual_workflow_uses_balanced_entries(self):
        mutations = (
            ("group: FUNCTION_BALANCED", "group: FUNCTION"),
            ("group: PULSAR_CONNECTORS_PROCESS_GO_BALANCED", "group: PULSAR_CONNECTORS_PROCESS_GO"),
            ("group: PULSAR_CONNECTORS_PROCESS_GO_BALANCED", "group: FUNCTION_BALANCED"),
            ('./pulsar-build/run_integration_group_gradle.sh ${{ matrix.group }}',
             './pulsar-build/run_integration_group_gradle.sh FUNCTION'),
        )
        for old, new in mutations:
            with self.subTest(mutation=new), self.checkout() as (root, _):
                path = root / ".github/workflows/pulsar-ci.yaml"
                source = path.read_text()
                # Only mutate the System step; Integration has the same dispatcher line.
                before, system = source.split("  system-tests:", 1)
                self.assertIn(old, system)
                path.write_text(before + "  system-tests:" + system.replace(old, new, 1))
                with self.assertRaises(SystemExit):
                    self.verify()

    def test_quoted_matrix_literals_are_equivalent(self):
        with self.checkout() as (root, _):
            path = root / ".github/workflows/pulsar-ci.yaml"
            self.replace(path, "group: FUNCTION_BALANCED", "group: 'FUNCTION_BALANCED'")
            self.replace(path, "group: PULSAR_CONNECTORS_PROCESS_GO_BALANCED",
                         'group: "PULSAR_CONNECTORS_PROCESS_GO_BALANCED"')
            self.verify()

    def test_added_quoted_legacy_or_duplicate_group_fails(self):
        for group in ("'FUNCTION'", '"PULSAR_CONNECTORS_PROCESS_GO"', "'FUNCTION_BALANCED'"):
            with self.subTest(group=group), self.checkout() as (root, _):
                path = root / ".github/workflows/pulsar-ci.yaml"
                old = "            group: FUNCTION_BALANCED"
                extra = old + "\n\n          - name: Unexpected duplicate\n            group: " + group
                self.replace(path, old, extra)
                with self.assertRaises(SystemExit):
                    self.verify()


if __name__ == "__main__":
    unittest.main()
