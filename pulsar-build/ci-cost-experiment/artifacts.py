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

"""Package existing build outputs for a controlled CI transport experiment."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import tarfile
import time


OUTPUT_DIRECTORIES = {"classes", "generated", "resources", "libs", "distributions"}


def is_output(path):
    """Match the output trees transported by pulsar-ci.yaml, including their roots."""
    parts = path.parts
    for index, part in enumerate(parts[:-1]):
        if part == "build":
            suffix = parts[index + 1:]
            if suffix[0] in OUTPUT_DIRECTORIES or suffix[:2] == ("tmp", "jar"):
                return True
    return False


def is_distribution(path):
    parts = path.parts
    return any(parts[index:index + 2] == ("build", "distributions")
               for index in range(len(parts) - 1))


def collect_outputs(workspace, output_dir):
    selected = []
    for directory, directories, files in os.walk(workspace, followlinks=False):
        current = Path(directory)
        directories[:] = sorted(name for name in directories
                                if name != ".git" and current / name != output_dir)
        for name in directories + sorted(files):
            path = current / name
            relative = path.relative_to(workspace)
            if is_output(relative):
                selected.append(relative)
    return sorted(selected)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def inventory(workspace, paths):
    entries = []
    hashes = {}
    for relative in paths:
        path = workspace / relative
        attributes = path.lstat()
        entry = {"path": relative.as_posix(), "mode": stat.S_IMODE(attributes.st_mode)}
        if stat.S_ISREG(attributes.st_mode):
            identity = (attributes.st_dev, attributes.st_ino)
            if identity not in hashes:
                hashes[identity] = sha256(path)
            entry.update(kind="file", size_bytes=attributes.st_size, sha256=hashes[identity])
        elif stat.S_ISDIR(attributes.st_mode):
            entry.update(kind="directory", size_bytes=0)
        elif stat.S_ISLNK(attributes.st_mode):
            entry.update(kind="symlink", size_bytes=0, target=os.readlink(path))
        else:
            raise ValueError("Unsupported build output type: " + str(relative))
        entries.append(entry)
    return entries


def write_archive(workspace, destination, paths):
    started = time.monotonic()
    # Each entry is added once. tarfile preserves symbolic and hard links without
    # recursively revisiting directories that already occur in the input list.
    with tarfile.open(destination, "w", dereference=False) as archive:
        for relative in paths:
            archive.add(workspace / relative, arcname=relative.as_posix(), recursive=False)
    elapsed = time.monotonic() - started
    return {"path": destination.name, "size_bytes": destination.stat().st_size,
            "sha256": sha256(destination), "entries": len(paths),
            "pack_seconds": elapsed}


def pack(workspace, output_dir):
    workspace = Path(workspace).resolve()
    output_dir = Path(output_dir).resolve()
    if not workspace.is_dir():
        raise ValueError("Workspace must be a directory")
    if workspace == output_dir:
        raise ValueError("Output directory must differ from the workspace")
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = collect_outputs(workspace, output_dir)
    if not paths:
        raise ValueError("No build outputs found in " + str(workspace))
    entries = inventory(workspace, paths)
    core_paths = [path for path in paths if not is_distribution(path)]
    manifest = {
        "schema_version": 1,
        "description": "Core excludes only build/distributions output trees",
        "archives": {
            "baseline": write_archive(workspace, output_dir / "baseline.tar", paths),
            "core": write_archive(workspace, output_dir / "core.tar", core_paths),
        },
        "outputs": entries,
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    pack_parser = commands.add_parser("pack", help="Write baseline.tar, core.tar and manifest.json")
    pack_parser.add_argument("--workspace", type=Path, default=Path.cwd())
    pack_parser.add_argument("--output-dir", type=Path, required=True)
    arguments = parser.parse_args()
    try:
        manifest = pack(arguments.workspace, arguments.output_dir)
    except (OSError, ValueError, tarfile.TarError) as error:
        parser.exit(1, "Artifact packaging failed: " + str(error) + "\n")
    print(json.dumps(manifest["archives"], indent=2))


if __name__ == "__main__":
    main()
