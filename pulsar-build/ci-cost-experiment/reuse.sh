#!/usr/bin/env bash
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

set -euo pipefail

usage() {
    cat <<'USAGE'
Usage:
  reuse.sh producer OUTPUT_DIR
  reuse.sh consumer baseline|history|cache INPUT_DIR OUTPUT_DIR

Run from a fresh repository checkout. Requires bash, Python 3, tar, a supported
JDK and Gradle's normal dependency access. Set CI_EXPERIMENT_TASK_CACHE to an
empty absolute directory dedicated to this job. Existing dependency caches may
be reused; task-cache.init.gradle isolates the experiment's task cache.

The producer compiles :pulsar-broker:testClasses and writes reuse-history.tar
and reuse-task-cache.tar. artifacts.py packages compiled outputs separately.
For every consumer, extract the same compiled-output artifact before calling
this script. Only the history/cache variants additionally restore the named
state archive from INPUT_DIR.

Consumers compile the same target, run one existing smoke-test invocation,
then append a temporary source comment and require compileJava to execute.
The source is restored on exit. Each Gradle phase writes a log and JSON timing
and compilation-outcome summary into OUTPUT_DIR.
USAGE
}

if [[ ${1:-} == '--help' || ${1:-} == '-h' ]]; then
    usage
    exit 0
fi

if [[ ${1:-} == 'producer' && $# == 2 ]]; then
    role=producer
    variant=producer
    input_dir=''
    output_arg=$2
elif [[ ${1:-} == 'consumer' && $# == 4 ]]; then
    role=consumer
    variant=$2
    input_dir=$(cd "$3" && pwd)
    output_arg=$4
    case "$variant" in
        baseline|history|cache) ;;
        *) usage >&2; exit 2 ;;
    esac
else
    usage >&2
    exit 2
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/../.." && pwd)
mkdir -p "$output_arg"
output_dir=$(cd "$output_arg" && pwd)
cd "$repo_root"

: "${CI_EXPERIMENT_TASK_CACHE:?Set an empty absolute directory for the experiment task cache}"
case "$CI_EXPERIMENT_TASK_CACHE" in
    /*) ;;
    *) echo 'CI_EXPERIMENT_TASK_CACHE must be absolute' >&2; exit 2 ;;
esac
mkdir -p "$CI_EXPERIMENT_TASK_CACHE"
if [[ -n $(find "$CI_EXPERIMENT_TASK_CACHE" -mindepth 1 -maxdepth 1 -print -quit) ]]; then
    echo "Experiment task cache must initially be empty: $CI_EXPERIMENT_TASK_CACHE" >&2
    exit 2
fi
for state_dir in .gradle build-logic/.gradle; do
    if [[ -d "$state_dir" && -n $(find "$state_dir" -mindepth 1 -maxdepth 1 -print -quit) ]]; then
        echo "Start from a fresh project task-history directory: $state_dir" >&2
        exit 2
    fi
done

source_path=''
source_backup=''
cleanup() {
    if [[ -n "$source_backup" && -f "$source_backup" ]]; then
        cp "$source_backup" "$source_path"
    fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

run_gradle() {
    local phase=$1
    shift
    local started status finished
    local -a measurement_command=(env)
    if [[ $(uname -s) == Linux && -x /usr/bin/time ]]; then
        measurement_command=(/usr/bin/time -v -o "$output_dir/$phase.resources.txt")
    fi
    local -a pipeline_status
    started=$(python3 -c 'import time; print(time.time())')
    set +e
    "${measurement_command[@]}" ./gradlew --init-script "$script_dir/task-cache.init.gradle" \
        --no-configuration-cache --no-daemon --console=plain --info --profile "$@" \
        2>&1 | tee "$output_dir/$phase.log"
    pipeline_status=("${PIPESTATUS[@]}")
    status=${pipeline_status[0]}
    if [[ $status == 0 && ${pipeline_status[1]} != 0 ]]; then
        status=${pipeline_status[1]}
    fi
    set -e
    finished=$(python3 -c 'import time; print(time.time())')
    python3 - "$output_dir/$phase.log" "$output_dir/$phase.json" "$phase" \
        "$variant" "$started" "$finished" "$status" "$script_dir" <<'PY'
import collections
import json
from pathlib import Path
import shutil
import sys

log_path, json_path, phase, variant, started, finished, status, script_dir = sys.argv[1:]
sys.path.insert(0, script_dir)
import reuse_metrics

tasks = reuse_metrics.parse_tasks(Path(log_path).read_text(errors="replace"))
compilation = reuse_metrics.compilation_tasks(tasks)
report = {
    "phase": phase,
    "variant": variant,
    "elapsed_seconds": float(finished) - float(started),
    "exit_code": int(status),
    "tasks": tasks,
    "compilation_tasks": compilation,
    "compilation_outcomes": dict(collections.Counter(task["outcome"] for task in compilation)),
}
resources_path = Path(log_path).with_suffix(".resources.txt")
if resources_path.exists():
    resources = {}
    for line in resources_path.read_text().splitlines():
        key, separator, value = line.strip().partition(": ")
        if separator:
            resources[key] = value
    report["process_resources"] = resources
    report["process_resources_note"] = (
        "GNU time launcher statistics may omit detached Gradle daemons and test workers; "
        "CPU and RSS are not whole-build measurements"
    )
Path(json_path).write_text(json.dumps(report, indent=2) + "\n")
profile_directory = Path("build/reports/profile")
if profile_directory.is_dir():
    shutil.copytree(profile_directory, Path(json_path).parent / (phase + "-profile"), dirs_exist_ok=True)
print(json.dumps({key: value for key, value in report.items() if key not in ("tasks", "compilation_tasks")}))
PY
    return "$status"
}

if [[ "$role" == 'producer' ]]; then
    run_gradle compile :pulsar-broker:testClasses
    ./gradlew --stop
    history_dirs=()
    for state_dir in .gradle build-logic/.gradle; do
        if [[ -d "$state_dir" ]]; then
            history_dirs+=("$state_dir")
        fi
    done
    tar --exclude='*.lock' --exclude='*/configuration-cache*' \
        -cf "$output_dir/reuse-history.tar" "${history_dirs[@]}"
    tar --exclude='*.lock' -cf "$output_dir/reuse-task-cache.tar" \
        -C "$CI_EXPERIMENT_TASK_CACHE" .
    python3 - "$output_dir" <<'PY'
import json
from pathlib import Path
import sys

directory = Path(sys.argv[1])
sizes = {name: (directory / name).stat().st_size for name in ("reuse-history.tar", "reuse-task-cache.tar")}
(directory / "state-archive-sizes.json").write_text(json.dumps(sizes, indent=2) + "\n")
print(json.dumps(sizes))
PY
    exit 0
fi

case "$variant" in
    history) tar -xf "$input_dir/reuse-history.tar" ;;
    cache) tar -xf "$input_dir/reuse-task-cache.tar" -C "$CI_EXPERIMENT_TASK_CACHE" ;;
esac

run_gradle compile :pulsar-broker:testClasses
run_gradle smoke :pulsar-broker:test \
    --tests org.apache.pulsar.broker.service.EntryBatchIndexesAcksTest \
    -PtestRetryCount=0 -PtestFailFast=false
python3 - "$output_dir" "$script_dir" <<'PY'
import json
from pathlib import Path
import shutil
import sys
import xml.etree.ElementTree as ET

directory = Path(sys.argv[1])
sys.path.insert(0, sys.argv[2])
import reuse_metrics

saved_reports = directory / "smoke-test-results"
saved_reports.mkdir(exist_ok=True)
for path in Path("pulsar-broker/build/test-results/test").glob("TEST-*.xml"):
    shutil.copy2(path, saved_reports / path.name)
report = json.loads((directory / "smoke.json").read_text())
try:
    reuse_metrics.require_executed_task(report["tasks"], ":pulsar-broker:test")
except ValueError as error:
    raise SystemExit(str(error)) from error
cases = []
for path in saved_reports.glob("TEST-*.xml"):
    for case in ET.parse(path).iter("testcase"):
        if case.get("classname") == "org.apache.pulsar.broker.service.EntryBatchIndexesAcksTest":
            cases.append(case)
if len(cases) != 1 or any(case.find(tag) is not None for case in cases for tag in ("failure", "error", "skipped")):
    raise SystemExit("Expected exactly one successful EntryBatchIndexesAcksTest invocation")
result = {"executed": 1, "failed": 0, "skipped": 0}
(directory / "smoke-verification.json").write_text(json.dumps(result, indent=2) + "\n")
PY

source_path='pulsar-broker/src/main/java/org/apache/pulsar/broker/service/EntryBatchIndexesAcks.java'
source_backup="$output_dir/mutation-source-original.java"
cp "$source_path" "$source_backup"
printf '\n// CI cost experiment: changed source must invalidate compilation.\n' >> "$source_path"
run_gradle mutation :pulsar-broker:compileJava
python3 - "$output_dir" "$script_dir" <<'PY'
import json
from pathlib import Path
import sys

directory = Path(sys.argv[1])
sys.path.insert(0, sys.argv[2])
import reuse_metrics

report = json.loads((directory / "mutation.json").read_text())
try:
    task = reuse_metrics.require_executed_task(report["compilation_tasks"], ":pulsar-broker:compileJava")
except ValueError as error:
    raise SystemExit(str(error)) from error
(directory / "mutation-verification.json").write_text(json.dumps(task, indent=2) + "\n")
PY
