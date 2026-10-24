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

"""Fail before building if the Process CI suites omit or duplicate a class."""

from collections import Counter
from pathlib import Path
import xml.etree.ElementTree as ET


RESOURCES = Path(__file__).resolve().parents[1] / "tests/integration/src/test/resources"


def read_suite(filename):
    root = ET.parse(RESOURCES / filename).getroot()
    tests = root.findall("test")
    if len(tests) != 1 or tests[0].find("classes") is None:
        raise SystemExit(f"{filename}: expected one test with explicit classes")
    classes = tests[0].find("classes")
    names = Counter()
    for test_class in classes:
        if test_class.tag != "class" or set(test_class.attrib) != {"name"} or len(test_class):
            raise SystemExit(f"{filename}: expected whole classes without method filters")
        names[test_class.attrib["name"]] += 1
    if not names:
        raise SystemExit(f"{filename}: empty test partition")
    # Compare suite/test options, listeners, groups and parameters after removing
    # only the display names and class lists that deliberately differ by shard.
    root.attrib.pop("name", None)
    tests[0].attrib.pop("name", None)
    tests[0].remove(classes)

    def signature(element):
        return (element.tag, sorted(element.attrib.items()), (element.text or "").strip(),
                tuple(signature(child) for child in element))

    return names, signature(root)


def main():
    original, options = read_suite("pulsar-process.xml")
    partitioned = Counter()
    for language in ("java", "python", "go"):
        filename = f"pulsar-process-{language}.xml"
        names, shard_options = read_suite(filename)
        if shard_options != options:
            raise SystemExit(f"{filename}: suite options differ from pulsar-process.xml")
        partitioned.update(names)
    if partitioned != original:
        raise SystemExit(f"Process partition mismatch: missing={original - partitioned}, "
                         f"extra={partitioned - original}")
    print(f"Process CI partitions cover all {sum(original.values())} original classes exactly once")


if __name__ == "__main__":
    main()
