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

# The manifest is generated from the prepareCiTestArtifacts producer graph. Keep all
# declared outputs, including compiler state, while excluding unrelated build outputs.
python3 - <<'PYTHON' | tar --create --file="$1" --null --files-from=-
import json
import sys
from pathlib import Path

root = Path.cwd().resolve()
manifest = root / "tests/integration/build/ci-test-artifacts.json"
data = json.loads(manifest.read_text())
if data.get("schemaVersion") != 1 or data.get("prepareTask") != ":tests:integration:prepareCiTestArtifacts":
    raise SystemExit("Unexpected integration artifact manifest")
paths = set()
for entry in data["outputs"]:
    path = Path(entry)
    if path.is_absolute() or ".." in path.parts or "build" not in path.parts:
        raise SystemExit(f"Unsafe integration artifact path: {entry}")
    if ".gradle" in path.parts or ".git" in path.parts:
        raise SystemExit(f"Cache/source-control path in integration artifact: {entry}")
    candidate = root / path
    if not candidate.resolve().is_relative_to(root):
        raise SystemExit(f"Integration artifact path leaves checkout: {entry}")
    if candidate.exists():
        paths.add(path)
for entry in data["requiredOutputs"]:
    path = Path(entry)
    if path.is_absolute() or ".." in path.parts or not (root / path).exists():
        raise SystemExit(f"Required integration output is missing or unsafe: {entry}")
    if path not in paths and not any(parent in paths for parent in path.parents):
        raise SystemExit(f"Required integration output is absent from producer outputs: {entry}")
for entry in data["runtimeOutputs"]:
    path = Path(entry)
    if path.is_absolute() or ".." in path.parts or "build" not in path.parts:
        raise SystemExit(f"Unsafe integration runtime path: {entry}")
    if not (root / path).resolve().is_relative_to(root):
        raise SystemExit(f"Integration runtime path leaves checkout: {entry}")
    if (root / path).exists() and path not in paths and not any(parent in paths for parent in path.parents):
        raise SystemExit(f"Integration runtime output is absent from producer outputs: {entry}")
# Some task outputs are nested (processTestResources includes copyCertificateAuthority).
# Archive each file once rather than recursively adding the nested output twice.
selected = sorted(path for path in paths if not any(parent in paths for parent in path.parents))
if not selected:
    raise SystemExit("Integration artifact has no produced outputs")
for path in selected:
    sys.stdout.buffer.write(str(path).encode() + b"\0")
sys.stdout.buffer.write(b"tests/integration/build/ci-test-artifacts.json\0")
PYTHON
