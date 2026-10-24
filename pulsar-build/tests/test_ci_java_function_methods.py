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

"""Verify method XML selection with real TestNG and Pulsar retry/configuration code.

First build :buildtools:classes, then run with JDK21/25:
  python3 pulsar-build/tests/test_ci_java_function_methods.py [--offline] [--output-dir DIR]

No Pulsar broker or Docker container is started. The fixture selects inherited
methods through parsed suite XML, with Factory/provider identities, group filters
and actual AnnotationListener, RetryAnalyzer, FailFastNotifier and TestRetrySupport.
"""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SOURCE = r"""
import java.util.*;
import java.lang.reflect.Method;
import org.testng.*;
import org.testng.annotations.*;
import org.testng.xml.*;

public class TestNGMethodShardProbe {
  static final List<String> events = new ArrayList<>();
  static boolean forceRetry;
  public static class Grand extends org.apache.pulsar.tests.TestRetrySupport {
    final String id;
    boolean started;
    int plainAttempts;
    Grand(String id) { this.id = id; }
    protected void setup() {
      incrementSetupNumber(); started = true; events.add(id + ":setup");
    }
    protected void cleanup() {
      markCurrentSetupNumberCleaned(); started = false; events.add(id + ":cleanup");
    }
    @BeforeClass(alwaysRun=true) public void beforeClass() { setup(); }
    @AfterClass(alwaysRun=true) public void afterClass() { cleanup(); }
    @BeforeMethod(alwaysRun=true) public void beforeMethod(Method m) {
      if (!started) throw new AssertionError("missing fixture");
      events.add(id + ":beforeMethod:" + m.getName());
    }
    @AfterMethod(alwaysRun=true) public void afterMethod(ITestResult r) {
      events.add(id + ":afterMethod:" + r.getMethod().getMethodName());
    }
  }
  public static class Parent extends Grand {
    Parent(String id) { super(id); }
    @DataProvider(name="rows") public Object[][] rows() { return new Object[][]{{1},{2}}; }
    @Test(groups={"java_function","function"})
    public void inheritedPlain() {
      if (forceRetry && plainAttempts++ == 0) throw new AssertionError("injected");
      events.add("PASS:" + id + ":inheritedPlain");
    }
    @Test(groups={"java_function","function"}, dataProvider="rows")
    public void inheritedData(int row) { events.add("PASS:" + id + ":inheritedData:" + row); }
    @Test(groups={"function","flaky"})
    public void excludedFlaky() { throw new AssertionError("excluded group executed"); }
    public void publicHelper() { throw new AssertionError("unannotated helper executed"); }
  }
  public static class Child extends Parent {
    @Factory(dataProvider="instances") public Child(String id) { super(id); }
    @DataProvider(name="instances") public static Object[][] instances() { return new Object[][]{{"A"},{"B"}}; }
  }
  public static class PlainChild extends Parent { public PlainChild() { super("S"); } }
  public static class Once implements IRetryAnalyzer {
    public boolean retry(ITestResult r) { return ((Child) r.getInstance()).plainAttempts == 1; }
  }
  record Result(List<String> passed, long setups, long cleanups, boolean failed) {}
  static Result run(String label, List<String> includes, boolean retry) { return run(label,includes,retry,Child.class); }
  static Result run(String label, List<String> includes, boolean retry, Class<?> selectedClass) {
    events.clear(); forceRetry=retry;
    XmlSuite s=new XmlSuite(); s.setName(label); s.setParallel(XmlSuite.ParallelMode.NONE);
    XmlTest t=new XmlTest(s); t.setName(label); t.setPreserveOrder(true);
    t.setIncludedGroups(List.of("function"));t.setExcludedGroups(List.of("flaky","quarantine"));
    XmlClass c=new XmlClass(selectedClass);
    if (includes!=null) { List<XmlInclude> inc=new ArrayList<>();for (String name:includes) inc.add(new XmlInclude(name));c.setIncludedMethods(inc); }
    t.setXmlClasses(List.of(c));
    java.nio.file.Path suitePath=java.nio.file.Path.of(System.getProperty("fixtureSuitesDirectory"),label+".xml");
    try { java.nio.file.Files.writeString(suitePath,s.toXml()); } catch(java.io.IOException e) { throw new RuntimeException(e); }
    TestNG ng=new TestNG(false);
    ng.addListener(new org.apache.pulsar.tests.AnnotationListener());
    ng.addListener(new org.apache.pulsar.tests.FailFastNotifier());
    ng.setVerbose(0);ng.setUseDefaultListeners(false);ng.setTestSuites(List.of(suitePath.toString()));ng.run();
    List<String> passed=events.stream().filter(e->e.startsWith("PASS:")).sorted().toList();
    long setups=events.stream().filter(e->e.endsWith(":setup")).count();
    long cleanups=events.stream().filter(e->e.endsWith(":cleanup")).count();
    System.out.println(label + ": passed=" + passed + " setups=" + setups + " cleanups=" + cleanups + " failure=" + ng.hasFailure());
    if (!Objects.equals(setups,cleanups))throw new AssertionError("fixture leak");
    return new Result(passed,setups,cleanups,ng.hasFailure());
  }
  static void check(boolean retry) {
    Result all=run("baseline-"+retry,null,retry);
    Result a=run("plain-"+retry,List.of("inheritedPlain"),retry);
    Result b=run("data-"+retry,List.of("inheritedData"),retry);
    List<String> union=new ArrayList<>(a.passed());union.addAll(b.passed());Collections.sort(union);
    if (!all.passed().equals(union)||!Collections.disjoint(a.passed(),b.passed())||all.failed()||a.failed()||b.failed())throw new AssertionError("coverage/failure mismatch");
    if(all.passed().size()!=6||a.passed().size()!=2||b.passed().size()!=4)throw new AssertionError("factory/provider row lost");
  }
  public static void main(String[] args) {
    check(false);check(true);
    Result typo=run("typo-no-test",List.of("misspelledMethod"),false);
    System.out.println("TYPO_FACTORY_RAN_ALL=" + (typo.passed().size()==6&&!typo.failed()));
    Result plainAll=run("plain-child-baseline",null,false,PlainChild.class);
    Result plainOne=run("plain-child-include",List.of("inheritedPlain"),false,PlainChild.class);
    if(plainAll.passed().size()!=3||plainOne.passed().size()!=1||plainAll.failed()||plainOne.failed()) throw new AssertionError("plain class include mismatch");
    Result plainTypo=run("plain-child-typo",List.of("misspelledMethod"),false,PlainChild.class);
    System.out.println("TYPO_PLAIN_SILENT_EMPTY=" + (plainTypo.passed().isEmpty()&&!plainTypo.failed()));
    System.out.println("CONTRACT_PASS");
  }
}
"""


def groovy_string(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    directory = (args.output_dir or Path(tempfile.mkdtemp(prefix="pulsar-method-contract-"))).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    classes = ROOT / "buildtools/build/classes/java/main"
    if not (classes / "org/apache/pulsar/tests/TestRetrySupport.class").is_file():
        raise SystemExit("Build :buildtools:classes before running this fixture")
    (directory / "settings.gradle").write_text(
        "rootProject.name = 'ci-java-method-contract'\n"
        "dependencyResolutionManagement { versionCatalogs { libs { from(files("
        + groovy_string(ROOT / "gradle/libs.versions.toml") + ")) } } }\n")
    (directory / "build.gradle.kts").write_text(
        "plugins { java }\nrepositories { mavenCentral() }\ndependencies {\n"
        "implementation(libs.testng)\nimplementation(libs.slog)\n"
        "implementation(libs.commons.lang3)\nimplementation(libs.slf4j.api)\n"
        "runtimeOnly(libs.log4j.core)\nruntimeOnly(libs.log4j.slf4j2.impl)\n"
        "implementation(files(" + json.dumps(str(classes)) + "))\n}\n"
        "tasks.register<JavaExec>(\"probe\") {\n"
        "classpath = sourceSets.main.get().runtimeClasspath\n"
        "mainClass.set(\"TestNGMethodShardProbe\")\n"
        "systemProperty(\"testRetryCount\", \"1\")\n"
        "systemProperty(\"testFailFast\", \"false\")\n"
        "systemProperty(\"fixtureSuitesDirectory\", " + json.dumps(str(directory)) + ")\n}\n")
    sources = directory / "src/main/java"
    sources.mkdir(parents=True, exist_ok=True)
    (sources / "TestNGMethodShardProbe.java").write_text(SOURCE)
    command = [str(ROOT / "gradlew"), "-p", str(directory), "probe", "--no-daemon", "--console=plain",
               "--no-configuration-cache", "--max-workers=2"]
    if args.offline:
        command.append("--offline")
    result = subprocess.run(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    (directory / "probe.log").write_text(result.stdout)
    if result.returncode or "CONTRACT_PASS" not in result.stdout:
        raise AssertionError("Method shard fixture failed: " + str(directory / "probe.log"))
    summary = {"passed": True, "parsed_xml": True, "inherited_methods": True,
               "factory_and_provider_identities": True, "always_run_configurations": True,
               "actual_pulsar_retry_rebuild": True, "unknown_include_fallback_observed": True,
               "fixture_directory": str(directory)}
    (directory / "validation.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
