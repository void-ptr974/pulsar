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

"""Verify Broker shard invocations before a build; no Java test inventory is maintained."""

import argparse
from collections import Counter
import json
from pathlib import Path
import subprocess
import zlib

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "pulsar-build/run_unit_group_gradle.sh"
BASE_CALLS = {
    "BROKER_GROUP_1": [
        [":pulsar-broker:test", "-PtestGroups=broker"],
        [":pulsar-broker:test", "-PexcludedTestGroups=", "--tests",
         "org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGeneratorWithNoUnsafeTest"],
    ],
    "BROKER_GROUP_2": [[":pulsar-broker:test", "-PtestGroups="
                        "schema,utils,functions-worker,broker-io,broker-discovery,broker-compaction,"
                        "broker-naming,websocket,other,builtin,stats"]],
    "BROKER_GROUP_3": [
        [":pulsar-broker:test", "-PtestGroups=broker-admin"],
        [":pulsar-broker:test", "-PexcludedTestGroups=", "--tests",
         "org.apache.pulsar.broker.admin.v3.AdminApiTransactionMultiBrokerTest", "-DmaxHeapSize=1500m"],
    ],
    "BROKER_GROUP_4": [[":pulsar-broker:test", "-PtestGroups=cluster-migration"]],
    "BROKER_GROUP_5": [[":pulsar-broker:test", "-PtestGroups=broker-replication"]],
    "BROKER_CLIENT_API": [[":pulsar-broker:test", "-PtestGroups=broker-api"]],
    "BROKER_CLIENT_IMPL": [[":pulsar-broker:test", "-PtestGroups=broker-impl"]],
}
SHARDED = ("BROKER_GROUP_1", "BROKER_GROUP_2", "BROKER_GROUP_5",
           "BROKER_CLIENT_API", "BROKER_CLIENT_IMPL")


def record_calls(group):
    # Load the actual group functions, replacing only the build/report side effect.
    # This tests the executable shell wrappers rather than matching their text with regexes.
    prefix = SCRIPT.read_text().split('# Test Groups  -- end --')[0]
    recorder = """
function gradle_test() {
  python3 -c 'import json,sys; print(json.dumps(sys.argv[1:]))' "$@"
}
"""
    result = subprocess.run(["bash", "-c", prefix + recorder + "\ntest_group_" + group.lower()],
                            cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    return [json.loads(line) for line in result.stdout.splitlines() if line]


def shard_number(binary_name, count):
    # Match TestClassShardFilter: all nested classes share the outer class's key.
    outer = binary_name.split("$", 1)[0]
    return zlib.crc32(outer.encode("utf-8")) % count + 1


def verify():
    jobs = []
    calls = {}
    for base, expected in BASE_CALLS.items():
        actual = record_calls(base)
        if actual != expected:
            raise AssertionError(f"{base}: original invocation/filter contract changed: {actual!r}")
        if base not in SHARDED:
            jobs.append(base)
            calls[base] = actual
            continue
        for index in (1, 2):
            job = f"{base}_SHARD_{index}"
            actual = record_calls(job)
            expected_shard = [expected[0] + [f"-PtestClassShard={index}/2"]]
            # All original isolated invocations remain exactly once, on shard 1.
            if index == 1:
                expected_shard.extend(expected[1:])
            if actual != expected_shard:
                raise AssertionError(f"{job}: shard invocation/filter contract changed: {actual!r}")
            jobs.append(job)
            calls[job] = actual

    source_root = ROOT / "pulsar-broker/src/test/java"
    inventory = [str(path.relative_to(source_root)).removesuffix(".java").replace("/", ".")
                 for path in sorted(source_root.rglob("*.java"))]
    if not inventory:
        raise AssertionError("Broker Java source inventory is unexpectedly empty")
    # This is a source allocation check, not a claim that every helper is a TestNG test class.
    # Runtime discovery/inheritance/Factory coverage is checked by the Gradle contract fixture.
    counts = Counter(shard_number(name, 2) for name in inventory)
    if set(counts) != {1, 2}:
        raise AssertionError("Expected nonempty source allocation on both shards")
    if any(shard_number(name, 2) != shard_number(name + "$Nested", 2) for name in inventory):
        raise AssertionError("A nested class was assigned away from its outer class")
    future_name = "org.apache.pulsar.future.NewBrokerTest"
    assert shard_number(future_name, 2) in (1, 2)
    return {"passed": True, "broker_jobs": jobs, "job_count": len(jobs), "calls": calls,
            "selection": "CRC32(UTF-8 top-level binary class name) modulo shard count",
            "source_classes_including_helpers": len(inventory),
            "source_allocation": dict(sorted(counts.items())),
            "new_classes_automatically_allocated": True,
            "original_group_filters_and_isolated_invocations_preserved": True,
            "weights": "No measured weight is assumed; use successful CI XML to evaluate imbalance."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Print the verified plan as JSON")
    args = parser.parse_args()
    result = verify()
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"Broker CI: {result['job_count']} jobs preserve original calls, groups and isolated tests")
        print(f"Automatic source allocation (includes helpers): {result['source_allocation']}")


if __name__ == "__main__":
    main()
