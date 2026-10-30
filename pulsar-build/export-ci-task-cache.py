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

"""Export only task-cache entries referenced by this CI build's Gradle info logs."""

import argparse
import json
import os
from pathlib import Path
import re
import tarfile
import time


CACHE_KEY = re.compile(r"\b(?:Build cache key for .+? is|with cache key) ([0-9a-f]{32})\b")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_tar", type=Path)
    parser.add_argument("logs", type=Path, nargs="+")
    args = parser.parse_args()
    started = time.monotonic()
    cache_directory = Path(os.environ["PULSAR_CI_TASK_CACHE"])
    if not cache_directory.is_absolute():
        raise ValueError("PULSAR_CI_TASK_CACHE must be an absolute path")
    if not cache_directory.is_dir():
        raise ValueError("The selected Gradle task-cache directory does not exist")

    keys = set()
    for log in args.logs:
        for line in log.read_text(errors="replace").splitlines():
            keys.update(CACHE_KEY.findall(line))
    if not keys:
        raise ValueError("No Gradle cache keys were found; capture producer commands with --info")

    args.output_tar.parent.mkdir(parents=True, exist_ok=True)
    exported_entries = 0
    exported_entry_bytes = 0
    missing_entries = []
    with tarfile.open(args.output_tar, "w") as archive:
        for key in sorted(keys):
            entry = cache_directory / key
            # Entry names are validated cache keys, never paths from a log.
            # Do not include cache locks, cleanup state, directories or symlinks.
            if not entry.is_file() or entry.is_symlink():
                missing_entries.append(key)
                continue
            archive.add(entry, arcname=key, recursive=False)
            exported_entries += 1
            exported_entry_bytes += entry.stat().st_size
    if not exported_entries:
        raise ValueError("None of the referenced cache entries exists in the selected directory")
    print(json.dumps({
        "cache_directory": str(cache_directory),
        "output_tar": str(args.output_tar),
        "logs": [str(log) for log in args.logs],
        "referenced_keys": len(keys),
        "exported_entries": exported_entries,
        "exported_entry_bytes": exported_entry_bytes,
        "missing_entries": missing_entries,
        "archive_bytes": args.output_tar.stat().st_size,
        "elapsed_seconds": time.monotonic() - started,
    }))


if __name__ == "__main__":
    main()
