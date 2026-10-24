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

"""Exercise fail-closed method selection without starting Gradle or containers."""

from contextlib import contextmanager, redirect_stdout
import importlib.util
import io
from pathlib import Path
import shutil
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / "pulsar-build/verify_ci_java_function_methods.py"
spec = importlib.util.spec_from_file_location("java_function_shards", SPEC)
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


class MethodShardGuardTest(unittest.TestCase):
    @contextmanager
    def checkout(self):
        with tempfile.TemporaryDirectory(prefix="pulsar-method-guard-") as directory:
            temporary = Path(directory)
            resources = temporary / "tests/integration/src/test/resources"
            resources.mkdir(parents=True)
            for relative in guard.CHAIN:
                target = temporary / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(ROOT / relative, target)
            for name in (guard.ORIGINAL, *guard.SHARDS, "pulsar-thread.xml",
                         "pulsar-thread-shard-1.xml", "pulsar-thread-shard-2.xml"):
                shutil.copyfile(guard.RESOURCES / name, resources / name)
            original_root, original_resources = guard.ROOT, guard.RESOURCES
            guard.ROOT, guard.RESOURCES = temporary, resources
            try:
                yield temporary, resources
            finally:
                guard.ROOT, guard.RESOURCES = original_root, original_resources

    def verify(self):
        with redirect_stdout(io.StringIO()):
            guard.verify()
            guard.verify(guard.THREAD_TARGET, "pulsar-thread.xml",
                         ("pulsar-thread-shard-1.xml", "pulsar-thread-shard-2.xml"), "Thread Java")

    def test_reviewed_partition(self):
        with self.checkout():
            self.verify()

    def test_body_or_comment_change_does_not_freeze_test_logic(self):
        with self.checkout() as (root, _):
            path = root / (guard.PREFIX + "functions/java/PulsarFunctionsJavaTest.java")
            path.write_text(path.read_text().replace("testFunctionLocalRun(Runtime.JAVA);",
                                                     "/* body is not metadata */ testFunctionLocalRun(Runtime.JAVA);"))
            self.verify()

    def test_new_inherited_test_fails(self):
        with self.checkout() as (root, _):
            path = root / (guard.PREFIX + "functions/java/PulsarFunctionsJavaTest.java")
            source = path.read_text()
            addition = '@Test(groups = {"function"}) public void futureMethod() {}'
            path.write_text(source[:source.rfind("}")] + addition + "\n}\n")
            with self.assertRaises(SystemExit):
                self.verify()

    def test_metadata_mutations_fail(self):
        owner = guard.PREFIX + "functions/java/PulsarFunctionsJavaTest.java"
        base = guard.PREFIX + "functions/PulsarFunctionsTestBase.java"
        leaf = guard.PREFIX + "functions/java/PulsarFunctionsJavaProcessTest.java"
        mutations = (
            ("new inherited test", owner, "public void testJavaFunctionLocalRun()",
             "public void newlyAddedTest()"),
            ("group", owner, 'groups = {"java_function", "function"}', 'groups = {"java_function"}'),
            ("dependency", owner, '@Test(groups = {"java_function", "function"})',
             '@Test(groups = {"java_function", "function"}, dependsOnMethods = "testInitFunction")'),
            ("provider rows", base, "{ FunctionRuntimeType.THREAD }", "{ FunctionRuntimeType.PROCESS }"),
            ("resolved superclass", owner,
             "import org.apache.pulsar.tests.integration.functions.PulsarFunctionsTest;",
             "import example.other.PulsarFunctionsTest;"),
            ("runtime constructor", leaf, "FunctionRuntimeType.PROCESS", "FunctionRuntimeType.THREAD"),
            ("class annotation", owner, "public abstract class PulsarFunctionsJavaTest",
             "@Test public abstract class PulsarFunctionsJavaTest"),
            ("factory", leaf, "public PulsarFunctionsJavaProcessTest()",
             "@org.testng.annotations.Factory public PulsarFunctionsJavaProcessTest()"),
            ("provider", leaf, "public PulsarFunctionsJavaProcessTest()",
             "@org.testng.annotations.DataProvider public PulsarFunctionsJavaProcessTest()"),
        )
        for label, relative, old, new in mutations:
            with self.subTest(label=label), self.checkout() as (root, _):
                path = root / relative
                source = path.read_text()
                self.assertIn(old, source)
                path.write_text(source.replace(old, new, 1))
                with self.assertRaises(SystemExit):
                    self.verify()

    def test_unannotated_override_and_overload_fail(self):
        for declaration in ("public void testInitFunction() {}",
                            "public void testInitFunction(int ignored) {}",
                            "private void testInitFunction(String ignored) {}"):
            with self.subTest(declaration=declaration), self.checkout() as (root, _):
                path = root / (guard.PREFIX + "functions/PulsarFunctionsTest.java")
                source = path.read_text()
                path.write_text(source[:source.rfind("}")] + declaration + "\n}\n")
                with self.assertRaises(SystemExit):
                    self.verify()

    def test_suite_mutations_fail(self):
        mutations = (
            ("unknown", '<include name="testInitFunction" />', '<include name="misspelledMethod" />'),
            ("missing", '<include name="testInitFunction" />', ''),
            ("duplicate", '<include name="testInitFunction" />',
             '<include name="testInitFunction" /><include name="testInitFunction" />'),
            ("option drift", 'preserve-order="true"', 'preserve-order="false"'),
            ("unexpected filter", '<methods>', '<methods><exclude name="testInitFunction" />'),
        )
        for label, old, new in mutations:
            for filename in (guard.SHARDS[0], "pulsar-thread-shard-1.xml"):
                with self.subTest(label=label, suite=filename), self.checkout() as (_, resources):
                    path = resources / filename
                    source = path.read_text()
                    self.assertIn(old, source)
                    path.write_text(source.replace(old, new, 1))
                    with self.assertRaises(SystemExit):
                        self.verify()


if __name__ == "__main__":
    unittest.main()
