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

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <output.tar>" >&2
  exit 1
fi

# Preserve the complete declared producer outputs, including compiler state and test
# fixtures, while excluding unrelated distributions and Gradle user/project caches.
python3 - <<'PYTHON' | tar --create --file="$1" --null --files-from=-
import json
import sys
from pathlib import Path

root = Path.cwd().resolve()
manifest = root / "build/ci-unit-test-artifacts.json"
data = json.loads(manifest.read_text())
if (data.get("schemaVersion") != 1
        or data.get("prepareTask") != "prepareCiUnitTestArtifacts"
        or data.get("protobufOverride") is not False
        or not data.get("testModules")):
    raise SystemExit("Unexpected Unit artifact manifest")


def checked_path(entry):
    path = Path(entry)
    if path.is_absolute() or ".." in path.parts or "build" not in path.parts:
        raise SystemExit(f"Unsafe Unit artifact path: {entry}")
    if ".gradle" in path.parts or ".git" in path.parts:
        raise SystemExit(f"Cache/source-control path in Unit artifact: {entry}")
    if not (root / path).resolve().is_relative_to(root):
        raise SystemExit(f"Unit artifact path leaves checkout: {entry}")
    return path


paths = set()
for entry in data["outputs"]:
    path = checked_path(entry)
    if (root / path).exists():
        paths.add(path)
for entry in data["requiredOutputs"]:
    path = checked_path(entry)
    if not (root / path).exists():
        raise SystemExit(f"Required Unit output is missing: {entry}")
    if path not in paths and not any(parent in paths for parent in path.parents):
        raise SystemExit(f"Required Unit output is absent from producer outputs: {entry}")
for entry in data["runtimeOutputs"]:
    path = checked_path(entry)
    if (root / path).exists() and path not in paths and not any(parent in paths for parent in path.parents):
        raise SystemExit(f"Unit runtime output is absent from producer outputs: {entry}")
# Explicit Test dependencies can add resources below processTestResources' output.
# Keep each file once, even when one task output contains another task's output.
selected = sorted(path for path in paths if not any(parent in paths for parent in path.parents))
if not selected:
    raise SystemExit("Unit artifact has no produced outputs")
for path in selected:
    sys.stdout.buffer.write(str(path).encode() + b"\0")
sys.stdout.buffer.write(b"build/ci-unit-test-artifacts.json\0")
PYTHON
