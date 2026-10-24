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

"""Exercise real Gradle task graphs, including dependency additions and broken producers."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / "build-logic/conventions/src/main/java/CiTestArtifactDependencies.java"
VERIFIER = ROOT / "pulsar-build/verify_ci_artifact_dependencies.gradle"


class ArtifactDependencyGraphTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix="pulsar-ci-artifact-dependencies-")
        cls.fixture = Path(cls.temporary.name)
        for name in ("broker", "integration", "unit", "fixtures", "buildSrc/src/main/java"):
            (cls.fixture / name).mkdir(parents=True)
        shutil.copy2(HELPER, cls.fixture / "buildSrc/src/main/java/CiTestArtifactDependencies.java")
        (cls.fixture / "settings.gradle").write_text(
            "rootProject.name='ci-artifact-dependencies'\n"
            "include 'broker', 'integration', 'unit', 'fixtures'\n"
        )
        (cls.fixture / "build.gradle").write_text("""
allprojects { apply plugin: 'java' }
project(':fixtures') {
    ['explicitFixture', 'implicitFixture', 'unrelatedFixture'].each { name ->
        tasks.register(name) {
            outputs.file(layout.buildDirectory.file("resources/${name}.txt"))
            doLast {
                def output = outputs.files.singleFile
                output.parentFile.mkdirs()
                output.text = name
            }
        }
    }
}
['broker', 'integration', 'unit'].each { name ->
    project(":${name}") { current ->
        def target = name == 'integration' ? tasks.register('integrationTest', Test) {
            testClassesDirs = sourceSets.test.output.classesDirs
            classpath = sourceSets.test.runtimeClasspath
        } : tasks.named('test', Test)
        target.configure {
            dependsOn(':fixtures:explicitFixture')
            classpath += files(project(':fixtures').tasks.named('implicitFixture'))
        }
        tasks.register(name == 'unit' ? 'prepareCiUnitTestArtifacts' : 'prepareCiTestArtifacts') {
            CiTestArtifactDependencies.configure(it, target)
        }
    }
}
""")
        cls.tasks = [
            ":broker:prepareCiTestArtifacts",
            ":integration:prepareCiTestArtifacts",
            ":unit:prepareCiUnitTestArtifacts",
        ]
        cls.gradle = os.environ.get("PULSAR_CI_GRADLE", str(ROOT / "gradlew"))

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def run_gradle(self, *arguments, mutation=None, guarded=True, expect_success=True):
        command = [self.gradle, "--project-dir", str(self.fixture), "--offline", "--console=plain"]
        if mutation is not None:
            mutation_file = self.fixture / "mutation.gradle"
            mutation_file.write_text("gradle.projectsEvaluated {\n"
                                     "  if (gradle.rootProject.name != 'ci-artifact-dependencies') return\n"
                                     + mutation + "\n}\n")
            command.extend(["--init-script", str(mutation_file)])
        if guarded:
            command.extend(["--init-script", str(VERIFIER), "--no-configuration-cache"])
        command.extend(arguments)
        result = subprocess.run(command, cwd=ROOT, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=180)
        if expect_success:
            self.assertEqual(result.returncode, 0, result.stdout)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
        return result.stdout

    def test_all_producers_follow_explicit_and_classpath_dependencies(self):
        output = self.run_gradle(*self.tasks)
        self.assertIn("Verified actual Test dependencies for 3", output)
        report = json.loads((self.fixture / "build/ci-artifact-dependencies.json").read_text())
        self.assertEqual(len(report["verified"]), 3)
        self.assertEqual({entry["testTask"] for entry in report["verified"]},
                         {":broker:test", ":integration:integrationTest", ":unit:test"})
        for entry in report["verified"]:
            self.assertIn(":fixtures:explicitFixture", entry["dependencies"])
            self.assertIn(":fixtures:implicitFixture", entry["dependencies"])
            self.assertNotIn(entry["testTask"], entry["dependencies"])
        for name in ("explicitFixture", "implicitFixture"):
            self.assertEqual((self.fixture / f"fixtures/build/resources/{name}.txt").read_text(), name)

    def test_new_test_dependency_is_prepared_without_a_new_allowlist(self):
        mutation = """
    def p = gradle.rootProject.project(':broker')
    def added = p.tasks.register('newTestFixture') {
        outputs.file(p.layout.buildDirectory.file('new-fixture.txt'))
        doLast {
            outputs.files.singleFile.parentFile.mkdirs()
            outputs.files.singleFile.text = 'added-after-prepare-registration'
        }
    }
    p.tasks.named('test') { dependsOn(added) }
"""
        self.run_gradle(self.tasks[0], mutation=mutation)
        self.assertEqual((self.fixture / "broker/build/new-fixture.txt").read_text(),
                         "added-after-prepare-registration")
        report = json.loads((self.fixture / "build/ci-artifact-dependencies.json").read_text())
        self.assertIn(":broker:newTestFixture", report["verified"][0]["dependencies"])

    def test_excluding_a_required_producer_fails(self):
        output = self.run_gradle(self.tasks[0], "-x", ":fixtures:explicitFixture", expect_success=False)
        self.assertIn("dependencies differ from :broker:test", output)
        self.assertIn("missing=[:fixtures:explicitFixture]", output)

    def test_excluding_a_transitive_producer_fails(self):
        mutation = """
    def p = gradle.rootProject.project(':fixtures')
    def nested = p.tasks.register('nestedFixture')
    p.tasks.named('explicitFixture') { dependsOn(nested) }
"""
        output = self.run_gradle(self.tasks[0], "-x", ":fixtures:nestedFixture",
                                 mutation=mutation, expect_success=False)
        self.assertIn("missing=[:fixtures:nestedFixture]", output)

    def test_disconnecting_prepare_from_test_fails(self):
        mutation = "gradle.rootProject.project(':broker').tasks.named('prepareCiTestArtifacts') { setDependsOn([]) }"
        output = self.run_gradle(self.tasks[0], mutation=mutation, expect_success=False)
        self.assertIn("producer has no captured Test dependencies", output)

    def test_unexpected_prepare_dependency_fails(self):
        mutation = """
    gradle.rootProject.project(':broker').tasks.named('prepareCiTestArtifacts') {
        dependsOn(':fixtures:unrelatedFixture')
    }
"""
        output = self.run_gradle(self.tasks[0], mutation=mutation, expect_success=False)
        self.assertIn("unexpected=[:fixtures:unrelatedFixture]", output)

    def test_direct_test_dependency_fails_before_execution(self):
        mutation = """
    def p = gradle.rootProject.project(':fixtures')
    def forbidden = p.tasks.register('forbiddenTest', Test)
    gradle.rootProject.project(':broker').tasks.named('test') { dependsOn(forbidden) }
"""
        output = self.run_gradle(self.tasks[0], mutation=mutation, expect_success=False)
        self.assertIn("declared dependencies include Test task :fixtures:forbiddenTest", output)

    def test_transitive_test_dependency_fails_before_execution(self):
        mutation = """
    def p = gradle.rootProject.project(':fixtures')
    def forbidden = p.tasks.register('forbiddenTest', Test)
    p.tasks.named('explicitFixture') { dependsOn(forbidden) }
"""
        output = self.run_gradle(self.tasks[0], mutation=mutation, expect_success=False)
        self.assertIn("CI artifact producer must not execute tests", output)
        self.assertIn(":fixtures:forbiddenTest", output)

    def test_mapping_the_test_provider_cannot_schedule_tests(self):
        mutation = """
    def p = gradle.rootProject.project(':broker')
    p.tasks.named('prepareCiTestArtifacts') {
        dependsOn(p.tasks.named('test').map { it.taskDependencies.getDependencies(it) })
    }
"""
        output = self.run_gradle(self.tasks[0], mutation=mutation, expect_success=False)
        self.assertIn("CI artifact producer must not execute tests", output)
        self.assertIn(":broker:test", output)

    def test_verification_requires_an_uncached_graph(self):
        output = self.run_gradle(self.tasks[0], "--configuration-cache", expect_success=False)
        self.assertIn("verification requires --no-configuration-cache", output)

    def test_prepare_tasks_support_configuration_cache_store_and_reuse(self):
        first = self.run_gradle(*self.tasks, "--configuration-cache", guarded=False)
        second = self.run_gradle(*self.tasks, "--configuration-cache", guarded=False)
        self.assertIn("Configuration cache entry stored", first)
        self.assertIn("Configuration cache entry reused", second)


if __name__ == "__main__":
    unittest.main(verbosity=2)
