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

"""Read the literal CI include matrices and require every selected group exactly once.

This is deliberately a parser for the workflow's small matrix syntax, not a YAML
implementation. Dynamic matrices, aliases, additional axes and excludes fail
closed: review the selection contract before adopting another matrix shape.
"""

from collections import Counter
import json
from pathlib import Path
import re


def _fail(message):
    raise SystemExit("CI workflow matrix: " + message)


def _meaningful(lines):
    return [line for line in lines if line.strip() and not line.lstrip().startswith("#")]


def _block(lines, key, indent):
    prefix = " " * indent + re.escape(key)
    matches = [index for index, line in enumerate(lines) if re.fullmatch(prefix + r":.*", line)]
    if len(matches) != 1:
        _fail(f"expected one literal {key!r} block at indentation {indent}")
    if not re.fullmatch(prefix + r":\s*(?:#.*)?", lines[matches[0]]):
        _fail(f"expected a literal {key!r} block at indentation {indent}")
    start = matches[0] + 1
    end = start
    while end < len(lines):
        line = lines[end]
        if line.strip() and not line.lstrip().startswith("#"):
            if len(line) - len(line.lstrip(" ")) <= indent:
                break
        end += 1
    return lines[start:end]


def _scalar(value):
    if value.startswith('"'):
        match = re.fullmatch(r'("(?:\\.|[^"\\])*")(?:\s+#.*)?', value)
        if match:
            try:
                return json.loads(match[1])
            except json.JSONDecodeError:
                pass
    elif value.startswith("'"):
        match = re.fullmatch(r"'((?:[^']|'')*)'(?:\s+#.*)?", value)
        if match:
            return match[1].replace("''", "'")
    else:
        value = re.split(r"\s+#", value, maxsplit=1)[0].rstrip()
        if re.fullmatch(r"[A-Za-z0-9_ ./+\-]+", value):
            return value
    _fail(f"unsupported or nonliteral matrix value: {value!r}")


def read_matrix(workflow, job):
    jobs = _block(Path(workflow).read_text().splitlines(), "jobs", 0)
    selected = _block(jobs, job, 2)
    strategy = _block(selected, "strategy", 4)
    matrix = _meaningful(_block(strategy, "matrix", 6))
    if not matrix or not re.fullmatch(r"        include:\s*(?:#.*)?", matrix[0]):
        _fail(f"{job}: expected a literal matrix containing only include rows")
    rows = []
    row = None
    for line in matrix[1:]:
        first = re.fullmatch(r"          - ([a-z_]+):\s+(.+)", line)
        following = re.fullmatch(r"            ([a-z_]+):\s+(.+)", line)
        if first:
            row = {}
            rows.append(row)
            key, value = first.groups()
        elif following and row is not None:
            key, value = following.groups()
        else:
            _fail(f"{job}: unsupported matrix syntax: {line.strip()!r}")
        if key in row:
            _fail(f"{job}: duplicate matrix row key {key!r}")
        row[key] = _scalar(value)
    if not rows:
        _fail(f"{job}: empty matrix")
    for row in rows:
        if not row.get("name") or not re.fullmatch(r"[A-Z][A-Z0-9_]*", row.get("group", "")):
            _fail(f"{job}: every row needs a literal name and group")
    return rows


def verify_matrix(workflow, job, expected):
    # The Shade group intentionally occurs once for each runtime JDK. Counting
    # (group, runtime_jdk) also detects deleting one row and duplicating another.
    expected = Counter((item, None) if isinstance(item, str) else item for item in expected)
    rows = read_matrix(workflow, job)
    actual = Counter((row["group"], row.get("runtime_jdk")) for row in rows)
    if actual != expected:
        _fail(f"{job}: missing={dict(expected - actual)}, extra={dict(actual - expected)}")
    return rows


if __name__ == "__main__":
    import verify_ci_broker_shards
    import verify_ci_test_partitions

    verify_ci_broker_shards.verify()
    verify_ci_test_partitions.verify_workflow_matrices()
    print("Broker, Integration and System workflow matrices contain every expected group/JDK row exactly once")
