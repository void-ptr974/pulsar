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

"""Check CI class/method partitions and the commands emitted by their dispatcher.

This is a source-only check: Gradle and report collection are replaced by recording
stubs. It checks selection equivalence, not runtime Factory/provider/retry results;
those still need comparison using the successful baseline and candidate reports.
"""

from collections import Counter
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import xml.etree.ElementTree as ET

import verify_ci_java_function_methods


ROOT = Path(__file__).resolve().parents[1]
RESOURCES = ROOT / "tests/integration/src/test/resources"
PARTITIONS = (
    ("Process", "pulsar-process", "PULSAR_CONNECTORS_PROCESS", (
        ("java", "PULSAR_CONNECTORS_PROCESS_JAVA"),
        ("python", "PULSAR_CONNECTORS_PROCESS_PYTHON"),
        ("go", "PULSAR_CONNECTORS_PROCESS_GO"),
    )),
    ("Messaging", "pulsar-messaging", "MESSAGING", (
        ("smoke", "MESSAGING_SMOKE"),
        ("topic", "MESSAGING_TOPIC"),
        ("other", "MESSAGING_OTHER"),
    )),
    ("CLI", "pulsar-cli", "CLI", (
        ("admin", "CLI_ADMIN"),
        ("tools", "CLI_TOOLS"),
    )),
)


def read_suite(filename):
    root = ET.parse(RESOURCES / filename).getroot()
    tests = root.findall("test")
    if len(tests) != 1 or tests[0].find("classes") is None:
        raise SystemExit(f"{filename}: expected one test with explicit classes")
    classes = tests[0].find("classes")
    names = Counter()
    for test_class in classes:
        if test_class.tag != "class" or set(test_class.attrib) != {"name"} or len(test_class):
            raise SystemExit(f"{filename}: expected whole classes without method filters")
        names[test_class.attrib["name"]] += 1
    if not names:
        raise SystemExit(f"{filename}: empty test partition")
    # Compare suite/test options, listeners, groups and parameters after removing
    # only the display names and class lists that deliberately differ by shard.
    root.attrib.pop("name", None)
    tests[0].attrib.pop("name", None)
    tests[0].remove(classes)

    def signature(element):
        return (element.tag, sorted(element.attrib.items()), (element.text or "").strip(),
                tuple(signature(child) for child in element))

    return names, signature(root)


def verify_partition(label, suite, shards):
    original_filename = f"{suite}.xml"
    original, options = read_suite(original_filename)
    selections = {original_filename: original}
    partitioned = Counter()
    for suffix, _ in shards:
        filename = f"{suite}-{suffix}.xml"
        names, shard_options = read_suite(filename)
        if shard_options != options:
            raise SystemExit(f"{filename}: suite options differ from {original_filename}")
        selections[filename] = names
        partitioned.update(names)
    if partitioned != original:
        raise SystemExit(f"{label} partition mismatch: missing={original - partitioned}, "
                         f"extra={partitioned - original}")
    print(f"{label} CI partitions cover all {sum(original.values())} original classes exactly once")
    return selections


def verify_dispatcher(suite, original_group, shards, selections):
    original_groups = (original_group,) if isinstance(original_group, str) else tuple(original_group)
    records = []
    # Execute the real dispatcher, including gradle_integration_test, in an isolated
    # checkout skeleton. No Docker, Gradle daemon, compilation or test execution.
    with tempfile.TemporaryDirectory(prefix="pulsar-ci-partitions-") as directory:
        temporary_root = Path(directory)
        build = temporary_root / "pulsar-build"
        build.mkdir()
        dispatcher = build / "run_integration_group_gradle.sh"
        shutil.copyfile(ROOT / "pulsar-build/run_integration_group_gradle.sh", dispatcher)
        stub = ("#!/usr/bin/env python3\n"
                "import json, os, sys\n"
                "from pathlib import Path\n"
                "with open(os.environ['PULSAR_CI_PARTITION_CALLS'], 'a') as output:\n"
                "    output.write(json.dumps([Path(sys.argv[0]).name, sys.argv[1:]]) + '\\n')\n")
        for executable in (temporary_root / "gradlew", build / "pulsar_ci_tool.sh"):
            executable.write_text(stub)
            executable.chmod(0o755)
        calls_file = temporary_root / "calls.jsonl"
        forwarded = ("-PtestJavaVersion=21", "-PtestRetryCount=3",
                     "-PexcludedTestGroups=quarantine,flaky")

        def run_group(group, event):
            if calls_file.exists():
                calls_file.unlink()
            environment = dict(os.environ, GITHUB_ACTIONS="true", GITHUB_EVENT_NAME=event,
                               PULSAR_CI_PARTITION_CALLS=str(calls_file))
            result = subprocess.run(["bash", str(dispatcher), group, *forwarded],
                                    cwd=temporary_root, env=environment,
                                    capture_output=True, text=True)
            if result.returncode:
                raise SystemExit(f"{group}: dispatcher failed: {result.stderr}")
            if not calls_file.exists():
                raise SystemExit(f"{group}: dispatcher emitted no Gradle commands")
            calls = [json.loads(line) for line in calls_file.read_text().splitlines()]
            commands = []
            for index in range(0, len(calls), 2):
                if (calls[index][0] != "gradlew" or index + 1 >= len(calls)
                        or calls[index + 1] != ["pulsar_ci_tool.sh", ["move_test_reports"]]):
                    raise SystemExit(f"{group}: expected report collection after every Gradle command")
                arguments = calls[index][1]
                prefix = ["--no-configuration-cache", ":tests:integration:integrationTest"]
                failfast = f"-PtestFailFast={'true' if event == 'pull_request' else 'false'}"
                if (arguments[:2] != prefix or arguments.count(failfast) != 1
                        or any(arguments.count(argument) != 1 for argument in forwarded)):
                    raise SystemExit(f"{group}: Gradle task or CI flags changed: {arguments}")
                commands.append(arguments)
            return commands

        def expand(commands):
            selected = Counter()
            for arguments in commands:
                suite_arguments = [argument for argument in arguments
                                   if argument.startswith("-PintegrationTestSuiteFile=")]
                if len(suite_arguments) != 1:
                    raise SystemExit(f"Expected one suite selection: {arguments}")
                filename = suite_arguments[0].split("=", 1)[1]
                remaining = tuple(argument for argument in arguments if argument != suite_arguments[0])
                if filename in selections:
                    for name, count in selections[filename].items():
                        selector = name if isinstance(name, tuple) else ("class", name)
                        selected[(*selector, remaining)] += count
                else:
                    # Preserve supplemental package suites and even empty suites
                    # by exact filename/arguments, rather than guessing their tests.
                    selected[("suite", filename, remaining)] += 1
            return selected

        for event in ("pull_request", "workflow_dispatch"):
            baseline_commands = {group: run_group(group, event) for group in original_groups}
            baseline = Counter()
            for commands in baseline_commands.values():
                baseline.update(expand(commands))
            candidate_commands = {group: run_group(group, event) for _, group in shards}
            candidate = Counter()
            for commands in candidate_commands.values():
                candidate.update(expand(commands))
            if candidate != baseline:
                raise SystemExit(f"{suite} dispatcher mismatch ({event}): "
                                 f"missing={baseline - candidate}, extra={candidate - baseline}")
            records.append({"event": event, "baseline": baseline_commands, "candidate": candidate_commands})
    print(f"{' + '.join(original_groups)} dispatcher preserves classes, supplemental suites, groups and CI flags")
    return records


def verify_function_balance():
    metadata_class = "org.apache.pulsar.tests.integration.functions.PulsarMetadataStateStoreTest"
    balanced_suite = "pulsar-function-balanced.xml"
    metadata_suite = "pulsar-function-metadata-state.xml"
    selections = verify_partition("Function", "pulsar-function", (
        ("balanced", "FUNCTION_BALANCED"),
        ("metadata-state", "PULSAR_CONNECTORS_PROCESS_GO_BALANCED"),
    ))
    if selections[metadata_suite] != Counter({metadata_class: 1}):
        raise SystemExit("Function balance must move the complete Metadata state class exactly once")

    def ordered_classes(filename):
        return [element.attrib["name"]
                for element in ET.parse(RESOURCES / filename).getroot().find("test/classes")]

    expected_order = [name for name in ordered_classes("pulsar-function.xml") if name != metadata_class]
    if ordered_classes(balanced_suite) != expected_order:
        raise SystemExit("Balanced Function must preserve the remaining classes in their original order")
    selections["pulsar-process-go.xml"] = read_suite("pulsar-process-go.xml")[0]
    records = verify_dispatcher("Function + Process Go", ("FUNCTION", "PULSAR_CONNECTORS_PROCESS_GO"), (
        ("balanced", "FUNCTION_BALANCED"),
        ("go-balanced", "PULSAR_CONNECTORS_PROCESS_GO_BALANCED"),
    ), selections)

    def with_suite(arguments, filename):
        return [f"-PintegrationTestSuiteFile={filename}"
                if argument.startswith("-PintegrationTestSuiteFile=") else argument
                for argument in arguments]

    for record in records:
        function_calls = record["baseline"]["FUNCTION"]
        if len(function_calls) != 1 or any(argument.startswith("-PtestGroups=")
                                           for argument in function_calls[0]):
            raise SystemExit("Original Function must remain one invocation without a group filter")
        expected_function = [with_suite(function_calls[0], balanced_suite)]
        expected_go = record["baseline"]["PULSAR_CONNECTORS_PROCESS_GO"] + [
            with_suite(function_calls[0], metadata_suite)]
        if record["candidate"]["FUNCTION_BALANCED"] != expected_function:
            raise SystemExit("Balanced Function must retain the original unfiltered invocation and flags")
        if record["candidate"]["PULSAR_CONNECTORS_PROCESS_GO_BALANCED"] != expected_go:
            raise SystemExit("Balanced Go must run the unchanged Go calls before one unfiltered Metadata call")

    # Validate the actual System matrix rather than only testing unused helper entrypoints.
    workflow = (ROOT / ".github/workflows/pulsar-ci.yaml").read_text()
    match = re.search(r"^  system-tests:\n(.*?)(?=^  [a-zA-Z0-9_-]+:|\Z)", workflow, re.MULTILINE | re.DOTALL)
    if match is None:
        raise SystemExit("Missing System test job")
    system = match.group(1)
    matrix = re.search(r"^      matrix:\n(.*?)(?=^    steps:)", system, re.MULTILINE | re.DOTALL)
    if matrix is None:
        raise SystemExit("Missing explicit System matrix")
    groups = Counter()
    for value in re.findall(r"^\s+group:\s*([^\n]+)$", matrix.group(1), re.MULTILINE):
        literal = re.fullmatch(r'''(?:"([A-Z0-9_]+)"|'([A-Z0-9_]+)'|([A-Z0-9_]+))(?:\s+#.*)?''', value.strip())
        if literal is None:
            raise SystemExit(f"System matrix must use explicit dispatcher group names: {value}")
        groups[next(group for group in literal.groups() if group is not None)] += 1
    expected_groups = Counter((
        "TIERED_FILESYSTEM", "TIERED_JCLOUD", "FUNCTION_BALANCED", "SCHEMA", "PLUGIN",
        "PULSAR_CONNECTORS_THREAD_SHARD_1", "PULSAR_CONNECTORS_THREAD_SHARD_2",
        "PULSAR_CONNECTORS_PROCESS_JAVA_SHARD_1", "PULSAR_CONNECTORS_PROCESS_JAVA_SHARD_2",
        "PULSAR_CONNECTORS_PROCESS_PYTHON", "PULSAR_CONNECTORS_PROCESS_GO_BALANCED",
    ))
    if groups != expected_groups:
        raise SystemExit("System matrix must preserve all eleven groups with only the two balanced replacements")
    if len(re.findall(r"^ {10}\./pulsar-build/run_integration_group_gradle\.sh "
                      r"\$\{\{ matrix\.group \}\}\s*$", system, re.MULTILINE)) != 1:
        raise SystemExit("System job must dispatch its selected matrix group")
    print("Function and Go balance preserves whole classes, unfiltered state tests and invocation ordering")
    return records


def main():
    for label, suite, original_group, shards in PARTITIONS:
        selections = verify_partition(label, suite, shards)
        verify_dispatcher(suite, original_group, shards, selections)
    selections = verify_ci_java_function_methods.verify()
    verify_dispatcher("pulsar-process-java", "PULSAR_CONNECTORS_PROCESS_JAVA", (
        ("shard-1", "PULSAR_CONNECTORS_PROCESS_JAVA_SHARD_1"),
        ("shard-2", "PULSAR_CONNECTORS_PROCESS_JAVA_SHARD_2"),
    ), selections)
    selections = verify_ci_java_function_methods.verify(
        verify_ci_java_function_methods.THREAD_TARGET, "pulsar-thread.xml",
        ("pulsar-thread-shard-1.xml", "pulsar-thread-shard-2.xml"), "Thread Java")
    verify_dispatcher("pulsar-thread", "PULSAR_CONNECTORS_THREAD", (
        ("shard-1", "PULSAR_CONNECTORS_THREAD_SHARD_1"),
        ("shard-2", "PULSAR_CONNECTORS_THREAD_SHARD_2"),
    ), selections)
    verify_function_balance()


if __name__ == "__main__":
    main()
