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

"""Exercise the production class-file shard filter with Gradle and real TestNG discovery.

Run from any directory with JDK 21/25 and the repository wrapper available:
  python3 pulsar-build/tests/test_ci_test_class_shards.py [--offline] [--output-dir DIR]

The fixture compares both XML invocation counts and explicit parameter/Factory identities.
No Pulsar broker, container, or full build is started.
"""

import argparse
from collections import Counter
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / "build-logic/conventions/src/main/java/TestClassShardFilter.java"

SOURCES = {
    "Events.java": r"""
package fixture;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
public final class Events {
    public static void record(String value) throws Exception {
        Files.writeString(Path.of(System.getProperty("eventsFile")), value + "\n", StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
""",
    "AbstractParentTest.java": r"""
package fixture;
import org.testng.annotations.Test;
public abstract class AbstractParentTest {
    @Test(groups = "broker")
    public void inherited() throws Exception { Events.record(getClass().getName() + ":inherited"); }
}
""",
    "AlphaTest.java": r"""
package fixture;
import org.testng.annotations.*;
public class AlphaTest extends AbstractParentTest {
    @DataProvider public Object[][] values() { return new Object[][] {{"one"}, {"two"}}; }
    @Test(groups = "broker", dataProvider = "values", invocationCount = 2)
    public void parameters(String value) throws Exception { Events.record("AlphaTest:parameters:" + value); }
    @Test(groups = {"broker", "quarantine"}) public void quarantine() { throw new AssertionError(); }
    @Test(groups = {"broker", "flaky"}) public void flaky() { throw new AssertionError(); }
    @Test(groups = "other") public void unrelatedGroup() { throw new AssertionError(); }
    @Test(groups = "broker", enabled = false) public void disabled() { throw new AssertionError(); }
}
""",
    "BetaTest.java": r"""
package fixture;
import org.testng.annotations.Test;
public class BetaTest {
    @Test(groups = "broker") public void original() throws Exception { Events.record("BetaTest:original"); }
}
""",
    "NewlyAddedTest.java": r"""
package fixture;
import org.testng.annotations.Test;
public class NewlyAddedTest {
    @Test(groups = "broker") public void newClass() throws Exception { Events.record("NewlyAddedTest:newClass"); }
    public static class NestedTest {
        @Test(groups = "broker") public void nested() throws Exception { Events.record("NewlyAddedTest:NestedTest"); }
    }
}
""",
    "FactoryTest.java": r"""
package fixture;
import org.testng.annotations.*;
public class FactoryTest {
    private final int instance;
    @DataProvider public static Object[][] instances() { return new Object[][] {{1}, {2}}; }
    @Factory(dataProvider = "instances") public FactoryTest(int instance) { this.instance = instance; }
    @Test(groups = "broker")
    public void factoryInstance() throws Exception { Events.record("FactoryTest:instance:" + instance); }
}
""",
}


def groovy_string(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def xml_records(directory):
    records = Counter()
    for path in (directory / "build/test-results/test").glob("TEST-*.xml"):
        for case in ET.parse(path).getroot().findall("testcase"):
            status = "skipped" if case.find("skipped") is not None else (
                "failed" if case.find("failure") is not None else "passed")
            records[(case.get("classname"), case.get("name"), status)] += 1
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--output-dir", type=Path, help="Keep the generated project, reports and logs")
    args = parser.parse_args()
    directory = (args.output_dir or Path(tempfile.mkdtemp(prefix="pulsar-ci-class-shards-"))).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    build_src = directory / "buildSrc"
    (build_src / "src/main/java").mkdir(parents=True, exist_ok=True)
    shutil.copy2(SPEC, build_src / "src/main/java/TestClassShardFilter.java")
    (build_src / "build.gradle").write_text("plugins { id 'java-gradle-plugin' }\n")
    (directory / "settings.gradle").write_text(
        "rootProject.name = 'ci-class-shard-contract'\n"
        "dependencyResolutionManagement { versionCatalogs { libs { from(files("
        + groovy_string(ROOT / "gradle/libs.versions.toml") + ")) } } }\n")
    # Reuse the production convention block, including the task cache input.
    convention = (ROOT / "build-logic/conventions/src/main/kotlin/pulsar.java-conventions.gradle.kts").read_text()
    match = re.search(r'    providers.gradleProperty\("testClassShard"\).*?\n    }', convention, re.S)
    if not match:
        raise AssertionError("Production convention no longer configures testClassShard")
    (directory / "build.gradle.kts").write_text('''
plugins { java }
repositories { mavenCentral() }
dependencies {
    testImplementation(libs.testng)
    testImplementation(libs.slf4j.api)
}
tasks.withType<Test>().configureEach {
    useTestNG { includeGroups("broker"); excludeGroups("quarantine", "flaky") }
    maxParallelForks = 1
    systemProperty("eventsFile", providers.gradleProperty("eventsFile").get())
''' + match.group() + "\n}\n")
    source_dir = directory / "src/test/java/fixture"
    source_dir.mkdir(parents=True, exist_ok=True)
    for filename, source in SOURCES.items():
        (source_dir / filename).write_text(source)

    command = [str(ROOT / "gradlew"), "-p", str(directory), "test", "--no-daemon", "--console=plain",
               "--configuration-cache", "--rerun-tasks", "--max-workers=2"]
    if args.offline:
        command.append("--offline")
    outcomes = {}

    def run(label, properties):
        events_file = directory / (label + ".events")
        events_file.unlink(missing_ok=True)
        result = subprocess.run(command + ["-PeventsFile=" + str(events_file)] + properties,
                                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        (directory / (label + ".log")).write_text(result.stdout)
        if result.returncode:
            raise AssertionError(label + " failed; see " + str(directory / (label + ".log")))
        events = Counter(events_file.read_text().splitlines()) if events_file.exists() else Counter()
        outcomes[label] = (events, xml_records(directory))

    run("baseline", [])
    run("shard1", ["-PtestClassShard=1/2"])
    run("shard2", ["-PtestClassShard=2/2"])
    # Same properties and event path must deserialize the actual Spec from configuration cache.
    run("shard2", ["-PtestClassShard=2/2"])
    if "Reusing configuration cache." not in (directory / "shard2.log").read_text():
        raise AssertionError("Shard Spec did not survive configuration-cache reuse")

    baseline_events, baseline_xml = outcomes["baseline"]
    assert sum(baseline_events.values()) == 10, baseline_events
    assert outcomes["shard1"][0] and outcomes["shard2"][0], "Both shards must execute tests"
    assert baseline_events == outcomes["shard1"][0] + outcomes["shard2"][0], outcomes
    assert baseline_xml == outcomes["shard1"][1] + outcomes["shard2"][1], outcomes
    assert not (outcomes["shard1"][0] & outcomes["shard2"][0]), outcomes
    assert all(key[2] == "passed" for key in baseline_xml)
    assert sum(baseline_xml.values()) == 10, baseline_xml
    factory = Counter({"FactoryTest:instance:1": 1, "FactoryTest:instance:2": 1})
    assert baseline_events & factory == factory
    assert baseline_events["fixture.AlphaTest:inherited"] == 1
    assert baseline_events["AlphaTest:parameters:one"] == 2
    assert baseline_events["AlphaTest:parameters:two"] == 2
    assert baseline_events["NewlyAddedTest:NestedTest"] == 1
    for label in ("shard1", "shard2"):
        assert bool(outcomes[label][0]["NewlyAddedTest:newClass"]) == bool(
            outcomes[label][0]["NewlyAddedTest:NestedTest"])
    summary = {"passed": True, "baseline_invocations": 10,
               "shard_invocations": {label: sum(outcomes[label][0].values()) for label in ("shard1", "shard2")},
               "exact_xml_partition": True, "exact_parameter_and_factory_identity_partition": True,
               "inheritance_preserved": True, "nested_class_kept_with_outer": True,
               "group_exclusions_preserved": True, "new_class_automatically_included": True,
               "configuration_cache_reused": True, "fixture_directory": str(directory)}
    (directory / "validation.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
