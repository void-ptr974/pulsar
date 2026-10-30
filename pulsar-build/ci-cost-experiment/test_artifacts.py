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

import hashlib
import json
import os
from pathlib import Path
import tarfile
import tempfile
import unittest

from artifacts import pack


class ArtifactPackagingTest(unittest.TestCase):
    def test_round_trip_keeps_core_outputs_and_separates_only_distributions(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workspace = root / "workspace"
            files = {
                "module/build/classes/java/main/A.class": b"compiled",
                "module/build/generated/source/A.java": b"generated",
                "module/build/resources/main/data.txt": b"resource",
                "module/build/libs/shaded.jar": b"library",
                "module/build/tmp/jar/MANIFEST.MF": b"manifest",
                "distribution/server/build/distributions/server.tar.gz": b"distribution",
            }
            for relative, content in files.items():
                path = workspace / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            ignored = workspace / "module/build/reports/result.txt"
            ignored.parent.mkdir(parents=True)
            ignored.write_text("not transported", encoding="utf-8")
            output = workspace / "build/resources/artifact-experiment"
            manifest = pack(workspace, output)
            for variant in ("baseline", "core"):
                info = manifest["archives"][variant]
                archive_path = output / info["path"]
                self.assertEqual(hashlib.sha256(archive_path.read_bytes()).hexdigest(), info["sha256"])
                self.assertEqual(archive_path.stat().st_size, info["size_bytes"])
                with tarfile.open(archive_path) as archive:
                    names = archive.getnames()
                    self.assertEqual(len(names), len(set(names)))
                    self.assertNotIn("module/build/reports/result.txt", names)
                    self.assertFalse(any("artifact-experiment" in name for name in names))
                    for relative, content in files.items():
                        if variant == "core" and "/build/distributions/" in relative:
                            self.assertNotIn(relative, names)
                        else:
                            self.assertEqual(archive.extractfile(relative).read(), content)
            self.assertEqual(json.loads((output / "manifest.json").read_text()), manifest)

    def test_preserves_links_and_executable_mode_without_following_external_symlinks(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workspace = root / "workspace"
            outputs = workspace / "module/build/resources/main"
            outputs.mkdir(parents=True)
            original = outputs / "a-script"
            original.write_text("executable", encoding="utf-8")
            original.chmod(0o755)
            os.link(original, outputs / "b-hardlink")
            (outputs / "c-symlink").symlink_to("a-script")
            outside = root / "outside"
            outside.mkdir()
            (outside / "secret.txt").write_text("do not traverse", encoding="utf-8")
            (outputs / "outside-link").symlink_to(outside, target_is_directory=True)
            manifest = pack(workspace, root / "archives")
            for variant in ("baseline", "core"):
                with tarfile.open(root / "archives" / manifest["archives"][variant]["path"]) as archive:
                    prefix = "module/build/resources/main/"
                    self.assertEqual(archive.getmember(prefix + "a-script").mode, 0o755)
                    hardlink = archive.getmember(prefix + "b-hardlink")
                    self.assertTrue(hardlink.islnk())
                    self.assertEqual(hardlink.linkname, prefix + "a-script")
                    self.assertTrue(archive.getmember(prefix + "c-symlink").issym())
                    self.assertTrue(archive.getmember(prefix + "outside-link").issym())
                    self.assertFalse(any(name.endswith("secret.txt") for name in archive.getnames()))

    def test_rejects_empty_workspace(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workspace = root / "workspace"
            workspace.mkdir()
            with self.assertRaisesRegex(ValueError, "No build outputs"):
                pack(workspace, root / "archives")


if __name__ == "__main__":
    unittest.main()
