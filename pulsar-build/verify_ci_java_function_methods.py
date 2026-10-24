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

"""Fail closed when the reviewed Java function method partition needs reassessment.

This deliberately checks one small, known inheritance chain, not arbitrary Java.
Annotation/declaration fingerprints exclude test bodies, but include Factory and
DataProvider bodies. Review metadata changes and rerun the TestNG fixture before
updating the fingerprints or assigning new methods. A successful TestNG invocation
alone is insufficient: an unknown XML include can fall back to running all tests.
"""

from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
RESOURCES = ROOT / "tests/integration/src/test/resources"
PREFIX = "tests/integration/src/test/java/org/apache/pulsar/tests/integration/"
TARGET = "org.apache.pulsar.tests.integration.functions.java.PulsarFunctionsJavaProcessTest"
THREAD_TARGET = "org.apache.pulsar.tests.integration.functions.java.PulsarFunctionsJavaThreadTest"
ORIGINAL = "pulsar-process-java.xml"
SHARDS = ("pulsar-process-java-shard-1.xml", "pulsar-process-java-shard-2.xml")
CHAIN = {
    PREFIX + "functions/java/PulsarFunctionsJavaThreadTest.java": "6cb60c49cd365b5c9e537a61d52fee839ca86639f8ebbb670c030607df6302c7",
    PREFIX + "functions/java/PulsarFunctionsJavaProcessTest.java": "699a3548deac44cfb5d04f20312ad648625ec55a02bd27ed09c2d23b340aefbb",
    PREFIX + "functions/java/PulsarFunctionsJavaTest.java": "7f1d855c6232e732bbd9fe41c0e70adf933c8ccffa307a38c33b34f2df42428c",
    PREFIX + "functions/PulsarFunctionsTest.java": "f00aeed5ef392ec97dff2703cc7b137e115780b4dc2d3701516bb62717e0ea64",
    PREFIX + "functions/PulsarFunctionsTestBase.java": "60290fdff1f1621b629b7b5748687ed85b17f228f103e0ad041b467b6bcd0ee6",
    PREFIX + "suites/PulsarTestSuite.java": "33dd8835bac5a8f961cc5af7ae726b9dfb2f05755cd9348ea4af54737223d231",
    PREFIX + "topologies/PulsarClusterTestBase.java": "53c24540db0bd21512b16179a12d720f8f8bc3992405d800a3d57915bc47240e",
    PREFIX + "topologies/PulsarTestBase.java": "34b12c4f035826882380b4c6a6f0375ee3ec8d259d7f082ed4fb3f5f40d36c24",
    "buildtools/src/main/java/org/apache/pulsar/tests/TestRetrySupport.java": "9d2d86402cc27195ed350ed237ab9b50ee62cc3c3b55d46abd181e95edaeb662",
}
TOKEN = re.compile(r'""".*?"""|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|'
                   r'/\*.*?\*/|//[^\n]*|[A-Za-z_$][\w$]*|[^\s]', re.DOTALL)


def tokens(source):
    return [match.group() for match in TOKEN.finditer(source)
            if not match.group().startswith(("//", "/*"))]


def balanced_end(parts, start, opening="(", closing=")"):
    depth = 0
    for index in range(start, len(parts)):
        if parts[index] == opening:
            depth += 1
        elif parts[index] == closing:
            depth -= 1
            if depth == 0:
                return index + 1
    raise SystemExit("Java function metadata: unbalanced declaration; review the shard guard")


def metadata(source):
    parts = tokens(source)
    class_index = next(index for index, part in enumerate(parts)
                       if part == "class" and (index == 0 or parts[index - 1] != "."))
    class_end = parts.index("{", class_index)
    imports = []
    all_imports = []
    for index, part in enumerate(parts):
        if part == "import":
            end = parts.index(";", index)
            name = "".join(parts[index + 1:end])
            all_imports.append(name)
            if name.startswith("org.testng."):
                imports.append(name)
    parent = None
    if "extends" in parts[class_index:class_end]:
        start = parts.index("extends", class_index, class_end) + 1
        end = start + 1
        while end + 1 < class_end and parts[end] == ".":
            end += 2
        parent = "".join(parts[start:end])
        if "." not in parent:
            parent = next((name for name in all_imports if name.endswith("." + parent)),
                          "".join(parts[1:parts.index(";")]) + "." + parent)
    annotations = []
    for index, part in enumerate(parts):
        if part != "@":
            continue
        end = index + 1
        while end < len(parts) and (re.fullmatch(r"[A-Za-z_$][\w$]*", parts[end])
                                    or parts[end] == "."):
            # A bare annotation ends before modifiers, which are separate tokens.
            if end > index + 1 and parts[end - 1] != "." and parts[end] != ".":
                break
            end += 1
        name = "".join(parts[index + 1:end])
        simple = name.rsplit(".", 1)[-1]
        if not (name.startswith("org.testng.annotations.")
                or "org.testng.annotations." + simple in imports
                or "org.testng.annotations.*" in imports):
            continue
        arguments = []
        if end < len(parts) and parts[end] == "(":
            after = balanced_end(parts, end)
            arguments = parts[end:after]
            end = after
        declaration_end = end
        while declaration_end < len(parts) and parts[declaration_end] not in ("{", ";"):
            declaration_end += 1
        declaration = parts[end:declaration_end]
        record = {"annotation": name, "arguments": arguments, "declaration": declaration}
        if simple in ("Factory", "DataProvider") and parts[declaration_end] == "{":
            body_end = balanced_end(parts, declaration_end, "{", "}")
            record["body"] = parts[declaration_end:body_end]
        annotations.append(record)
    record = {"class": parts[class_index:class_end], "parent": parent,
              "imports": imports, "annotations": annotations}
    if parts[class_index + 1] in ("PulsarFunctionsJavaProcessTest", "PulsarFunctionsJavaThreadTest"):
        # The concrete runtime constructor is part of the reviewed selection.
        record["leaf"] = parts
    return record, parts


def reviewed_methods():
    methods = set()
    all_parts = []
    for relative, expected in CHAIN.items():
        record, parts = metadata((ROOT / relative).read_text())
        digest = hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()
        if digest != expected:
            raise SystemExit(f"{relative}: Java function TestNG metadata changed; review the inherited "
                             "methods/configuration/providers and rebalance both method shards")
        all_parts.append(parts)
        for annotation in record["annotations"]:
            if annotation["annotation"].rsplit(".", 1)[-1] != "Test":
                continue
            declaration = annotation["declaration"]
            if (len(declaration) < 5 or declaration[:2] != ["public", "void"]
                    or declaration[3:5] != ["(", ")"]):
                raise SystemExit("Java function: expected reviewed public zero-parameter @Test methods")
            if declaration[2] in methods:
                raise SystemExit("Java function: overloaded or duplicate inherited @Test method")
            methods.add(declaration[2])
    if len(methods) != 24:
        raise SystemExit("Java function: expected 24 reviewed methods; rebalance the method shards")
    # An unannotated override/overload can change inherited TestNG discovery too.
    for method in methods:
        declarations = 0
        for parts in all_parts:
            for index, part in enumerate(parts[:-1]):
                if part != method or parts[index + 1] != "(":
                    continue
                start = index - 1
                while start >= 0 and parts[start] not in ("{", "}", ";"):
                    start -= 1
                header = parts[start + 1:index]
                if any(modifier in header for modifier in ("public", "protected", "private")) and "=" not in header:
                    declarations += 1
        # Existing protected helpers intentionally share these two wrapper names.
        expected = 2 if method in ("testGenericObjectFunction", "testInitFunction") else 1
        if declarations != expected:
            raise SystemExit(f"Java function: override/overload of {method}; review the method shards")
    return methods


def suite(filename, methods, partitioned, target):
    root = ET.parse(RESOURCES / filename).getroot()
    tests = root.findall("test")
    if len(tests) != 1 or tests[0].find("classes") is None:
        raise SystemExit(f"{filename}: expected one test with explicit classes")
    classes = tests[0].find("classes")
    selected = Counter()
    seen = set()
    for entry in classes:
        if entry.tag != "class" or set(entry.attrib) != {"name"}:
            raise SystemExit(f"{filename}: unexpected class selection")
        name = entry.attrib["name"]
        if name in seen:
            raise SystemExit(f"{filename}: duplicate class selection")
        seen.add(name)
        if name != target:
            if len(entry):
                raise SystemExit(f"{filename}: only Java function may use method filters")
            selected[("class", name)] += 1
            continue
        if not partitioned:
            if len(entry):
                raise SystemExit(f"{filename}: original Java selection must remain whole-class")
            included = methods
        else:
            if len(entry) != 1 or entry[0].tag != "methods" or entry[0].attrib:
                raise SystemExit(f"{filename}: expected explicit method includes")
            included = []
            for include in entry[0]:
                if (include.tag != "include" or set(include.attrib) != {"name"}
                        or len(include) or include.attrib["name"] not in methods):
                    raise SystemExit(f"{filename}: unknown or nonliteral Java function method")
                included.append(include.attrib["name"])
            if not included or len(included) != len(set(included)):
                raise SystemExit(f"{filename}: empty or duplicate Java function method includes")
        selected.update(("method", name, method) for method in included)
    if not any(key[:2] == ("method", target) for key in selected):
        raise SystemExit(f"{filename}: missing Java function test class")
    root.attrib.pop("name", None)
    tests[0].attrib.pop("name", None)
    tests[0].remove(classes)

    def signature(element):
        return (element.tag, sorted(element.attrib.items()), (element.text or "").strip(),
                tuple(signature(child) for child in element))

    return selected, signature(root)


def verify(target=TARGET, original_filename=ORIGINAL, shard_filenames=SHARDS, label="Process Java"):
    methods = reviewed_methods()
    original, options = suite(original_filename, methods, False, target)
    selections = {original_filename: original}
    combined = Counter()
    for filename in shard_filenames:
        selected, shard_options = suite(filename, methods, True, target)
        if shard_options != options:
            raise SystemExit(f"{filename}: options differ from the unsharded Java suite")
        selections[filename] = selected
        combined.update(selected)
    if combined != original:
        raise SystemExit(f"{label} method partition mismatch: missing={original - combined}, "
                         f"extra={combined - original}")
    print(f"{label} shards preserve all 24 inherited methods and other original classes exactly once")
    return selections


if __name__ == "__main__":
    verify()
