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

"""Extract task outcomes without counting repeated Gradle --info headings."""

import re


TASK_HEADER = re.compile(
    r"> Task (:\S+?)(?: (UP-TO-DATE|FROM-CACHE|SKIPPED|NO-SOURCE|FAILED))?"
)
COMPILATION_TASK = re.compile(r":compile(?:Test)?(?:Java|Kotlin)$")


def parse_tasks(log):
    """Count paths once; explicit outcomes override bare headings; failure wins."""
    outcomes = {}
    for line in log.splitlines():
        match = TASK_HEADER.fullmatch(line.strip())
        if match is None:
            continue
        path, explicit_outcome = match.groups()
        previous = outcomes.get(path)
        if previous == "FAILED":
            continue
        if explicit_outcome is not None or previous is None:
            outcomes[path] = explicit_outcome or "EXECUTED"
    return [{"path": path, "outcome": outcome} for path, outcome in outcomes.items()]


def compilation_tasks(tasks):
    return [task for task in tasks if COMPILATION_TASK.search(task["path"])]


def require_executed_task(tasks, path):
    matches = [task for task in tasks if task["path"] == path]
    if len(matches) != 1 or matches[0]["outcome"] != "EXECUTED":
        raise ValueError(f"Expected {path} to execute exactly once; found {matches}")
    return matches[0]
