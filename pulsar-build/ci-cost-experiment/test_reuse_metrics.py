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

import unittest

from reuse_metrics import compilation_tasks, parse_tasks, require_executed_task


class TaskOutcomeTests(unittest.TestCase):
    def test_repeated_bare_headers_count_as_one_execution(self):
        tasks = parse_tasks("\n".join(["> Task :broker:compileJava"] * 3))
        self.assertEqual(tasks, [{"path": ":broker:compileJava", "outcome": "EXECUTED"}])

    def test_explicit_outcome_survives_later_bare_headers(self):
        for outcome in ("FROM-CACHE", "UP-TO-DATE", "SKIPPED", "NO-SOURCE"):
            with self.subTest(outcome=outcome):
                tasks = parse_tasks(
                    f"> Task :broker:compileJava\n"
                    f"> Task :broker:compileJava {outcome}\n"
                    f"> Task :broker:compileJava\n"
                )
                self.assertEqual(tasks[0]["outcome"], outcome)

    def test_failure_wins_over_all_later_headers(self):
        tasks = parse_tasks(
            "> Task :broker:compileJava FROM-CACHE\n"
            "> Task :broker:compileJava FAILED\n"
            "> Task :broker:compileJava\n"
            "> Task :broker:compileJava UP-TO-DATE\n"
        )
        self.assertEqual(tasks[0]["outcome"], "FAILED")

    def test_actual_info_log_repeated_smoke_headers_are_accepted(self):
        tasks = parse_tasks(
            "> Task :pulsar-broker:test\n"
            "Task ':pulsar-broker:test' is not up-to-date because:\n"
            "Starting process 'Gradle Test Executor 1'.\n"
            "> Task :pulsar-broker:test\n"
            "Gradle suite > Gradle test > EntryBatchIndexesAcksTest STANDARD_OUT\n"
            "    ------- SUCCESS -- shouldResetStateBeforeReusing -------\n"
            "Gradle Test Executor 1 finished executing tests.\n"
            "> Task :pulsar-broker:test\n"
        )
        self.assertEqual(require_executed_task(tasks, ":pulsar-broker:test")["outcome"], "EXECUTED")

    def test_reused_smoke_result_is_rejected(self):
        for outcome in ("FROM-CACHE", "UP-TO-DATE", "SKIPPED", "FAILED"):
            with self.subTest(outcome=outcome):
                tasks = parse_tasks(f"> Task :pulsar-broker:test {outcome}\n> Task :pulsar-broker:test")
                with self.assertRaises(ValueError):
                    require_executed_task(tasks, ":pulsar-broker:test")

    def test_missing_task_is_rejected(self):
        with self.assertRaises(ValueError):
            require_executed_task([], ":pulsar-broker:test")

    def test_compilation_filter_retains_each_real_task_once(self):
        tasks = parse_tasks(
            "> Task :build-logic:conventions:compileKotlin FROM-CACHE\n"
            "> Task :broker:compileJava\n"
            "> Task :broker:compileTestJava UP-TO-DATE\n"
            "> Task :broker:compileTestJava\n"
            "> Task :broker:testClasses\n"
        )
        self.assertEqual(len(compilation_tasks(tasks)), 3)


if __name__ == "__main__":
    unittest.main()
