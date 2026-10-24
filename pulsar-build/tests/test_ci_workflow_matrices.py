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

"""Mutate real workflow rows and exercise the production partition-guard entries."""

from contextlib import redirect_stdout
import io
from pathlib import Path
import re
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "pulsar-build"))
import verify_ci_broker_shards as broker
import verify_ci_test_partitions as partitions
from verify_ci_workflow_matrices import read_matrix


class WorkflowMatrixTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = (ROOT / ".github/workflows/pulsar-ci.yaml").read_text()

    def check(self, source, verifier):
        with tempfile.TemporaryDirectory(prefix="pulsar-matrix-guard-") as directory:
            path = Path(directory) / "pulsar-ci.yaml"
            path.write_text(source)
            with redirect_stdout(io.StringIO()):
                return verifier(path)

    def row(self, group, runtime=None):
        rows = re.findall(r"(?m)^          - name:[^\n]*\n(?:            [^\n]*\n)*", self.workflow)
        matches = [row for row in rows if f"            group: {group}\n" in row
                   and (runtime is None or f"            runtime_jdk: {runtime}\n" in row)]
        self.assertEqual(len(matches), 1, (group, runtime))
        return matches[0]

    def test_current_workflow_passes_both_production_entries(self):
        self.check(self.workflow, broker.verify)
        self.check(self.workflow, partitions.main)

    def test_missing_duplicate_and_wrong_groups_fail_production_entries(self):
        selections = (
            ("BROKER_GROUP_1_SHARD_2", None, broker.verify, "unit-tests-broker"),
            ("CLI_ADMIN", None, partitions.main, "integration-tests"),
            ("MESSAGING_SMOKE", None, partitions.main, "integration-tests"),
            ("PULSAR_CONNECTORS_PROCESS_JAVA_SHARD_2", None, partitions.main, "system-tests"),
            ("PULSAR_CONNECTORS_THREAD_SHARD_2", None, partitions.main, "system-tests"),
            ("SHADE_RUN", 17, partitions.main, "integration-tests"),
        )
        for group, runtime, verifier, job in selections:
            row = self.row(group, runtime)
            mutations = {
                "missing": "",
                "duplicate": row + row,
                "wrong group": row.replace(f"group: {group}\n", "group: MISSING_TEST_SELECTION\n"),
            }
            for label, replacement in mutations.items():
                with self.subTest(group=group, mutation=label):
                    changed = self.workflow.replace(row, replacement, 1)
                    with self.assertRaisesRegex(SystemExit, job):
                        self.check(changed, verifier)

    def test_same_shade_count_cannot_hide_a_missing_runtime(self):
        changed = self.workflow.replace(self.row("SHADE_RUN", 17), self.row("SHADE_RUN", 21), 1)
        with self.assertRaisesRegex(SystemExit, "integration-tests"):
            self.check(changed, partitions.main)

    def test_matrix_syntax_changes_fail_closed(self):
        mutations = (
            ("      matrix:\n", "      matrix: ${{ fromJSON(needs.plan.outputs.matrix) }}\n"),
            ("      matrix:\n", "      matrix: &shared_matrix\n"),
            ("      matrix:\n", "      matrix: ${{ inputs.matrix }}\n      matrix:\n"),
            ("        include:\n", "        platform: [ubuntu-24.04]\n        include:\n"),
            ("        include:\n", "        exclude: []\n        include:\n"),
            ("            group: BROKER_GROUP_1_SHARD_1\n",
             "            group: BROKER_GROUP_1_SHARD_1\n            group: BROKER_GROUP_1_SHARD_2\n"),
            ("            group: BROKER_GROUP_1_SHARD_1\n", "            group: ${{ inputs.group }}\n"),
        )
        # The first matrix is unit-tests-broker, matching the actual workflow.
        for original, replacement in mutations:
            with self.subTest(replacement=replacement):
                changed = self.workflow.replace(original, replacement, 1)
                self.assertNotEqual(changed, self.workflow)
                with self.assertRaises(SystemExit):
                    self.check(changed, broker.verify)

    def test_literal_quotes_and_comments_preserve_selection(self):
        changed = self.workflow.replace("group: BROKER_GROUP_1_SHARD_1\n",
                                        "group: 'BROKER_GROUP_1_SHARD_1' # retained\n", 1)
        self.check(changed, broker.verify)

    def test_absent_job_and_empty_matrix_fail(self):
        with self.assertRaises(SystemExit):
            self.check(self.workflow, lambda path: read_matrix(path, "absent-job"))
        changed = self.workflow
        for row in read_matrix(ROOT / ".github/workflows/pulsar-ci.yaml", "unit-tests-broker"):
            changed = changed.replace(self.row(row["group"]), "", 1)
        with self.assertRaises(SystemExit):
            self.check(changed, broker.verify)


if __name__ == "__main__":
    unittest.main()
