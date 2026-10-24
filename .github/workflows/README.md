<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

## GitHub Workflows

This directory contains all Pulsar CI checks.

### Required Workflows

When adding new CI workflows, please update the [.asf.yaml](../../.asf.yaml) if the workflow is required to pass before
a PR can be merged. Instructions on how to update the file are below.

This project uses the [.asf.yaml](../../.asf.yaml) to configure which workflows are required to pass before a PR can
be merged. In the `.asf.yaml`, the required contexts are defined in the `github.protected_branches.*.required_status_checks.contexts.[]`
where * is any key in the `protected_branches` map.

You can view the currently required status checks by running the following command:

```shell
curl -s -H 'Accept: application/vnd.github.v3+json' https://api.github.com/repos/apache/pulsar/branches/master | \
jq .protection
```

These contexts get their names in one of two ways depending on how the workflow file is written in this directory. The
following command will print out the names of each file and the associated with the check. If the `name` field is `null`,
the context will be named by the `id`.

```shell
for f in .github/workflows/*.yaml; \
do FILE=$f yq eval -o j '.jobs | to_entries | {"file": env(FILE),"id":.[].key, "name":.[].value.name}' $f; \
done
```

Duplicate names are allowed, and all checks with the same name will be treated the same (required or not required).

### CI build and test dependencies

`pulsar-ci.yaml` prepares Broker test dependencies before building the complete distribution. Broker Unit jobs
consume `broker-test-build-outputs`. Other, Proxy, IO, Client and Metadata Unit jobs use a separate artifact
producer in parallel with the complete build. Protobuf v3, image builds and publication checks keep the complete
build dependency. Binary license and publication validation remain required through the final CI check.

```mermaid
flowchart LR
    P[Preconditions] --> B[Prepare Broker test artifacts]
    B --> U[Broker Unit matrix: 12 jobs]
    B --> A[Complete build and source checks]
    B --> L[Prepare Integration runtime artifacts]
    B --> N[Prepare default Unit test artifacts]
    N --> O[Other Unit matrix: 5 jobs]
    A --> T[Protobuf v3 Unit job]
    A --> V[Binary license and publication checks]
    A --> D[Docker test images]
    D --> I[Integration/System matrices]
    L --> I
    A --> M[MacOS build]
    U --> G[All Unit tests completed]
    O --> G
    T --> G
    G --> Q[CodeQL: existing conditions]
    G --> F[Final required CI check]
    V --> F
    I --> F
    M --> F
    Q --> F
```

The default Unit producer follows the original `test` task dependencies, including explicit NAR, shadow JAR
and fixture producers, without executing any `Test` task. It archives only declared producer outputs and checks
that each selected module's test runtime outputs are covered. Its five consumers restore
`unit-test-build-outputs`. Protobuf v3 still rebuilds separately from `gradle-build-outputs` with its existing
version override; it never consumes the default-version artifact. The one-off collector also requires
`--no-configuration-cache`:

```shell
./gradlew prepareCiUnitTestArtifacts \
  -x :pulsar-broker:prepareCiUnitTestArtifacts \
  -x :tests:pulsar-client-admin-shade-test:prepareCiUnitTestArtifacts \
  -x :tests:pulsar-client-all-shade-test:prepareCiUnitTestArtifacts \
  -x :tests:pulsar-client-shade-test:prepareCiUnitTestArtifacts \
  -x :tests:pulsar-client-native-image:prepareCiUnitTestArtifacts \
  --init-script pulsar-build/ci_unit_artifacts.gradle --no-configuration-cache
./pulsar-build/archive_ci_unit_outputs.sh /tmp/unit-test-build-outputs.tar
```

The Integration artifact producer reuses the Broker artifact while the complete build continues. It prepares
`:tests:integration:integrationTest` dependencies without running tests, then archives their declared outputs.
Ordinary Integration groups and all System groups restore `integration-test-build-outputs`; Shade and Native
Image groups retain `gradle-build-outputs`. Docker images continue to come from the complete build. This reduces
runner-side transfers without dropping generated resources, shared test JARs or certificate-authority fixtures.
The one-off manifest collector uses the realized task graph and must run with `--no-configuration-cache`:

```shell
./gradlew :tests:integration:prepareCiTestArtifacts \
  --init-script pulsar-build/ci_integration_artifacts.gradle --no-configuration-cache
./pulsar-build/archive_ci_integration_outputs.sh /tmp/integration-test-build-outputs.tar
```

Unit jobs initialize JDK and Gradle directly in the workflow before invoking the test and diagnostics composite.
This keeps Gradle's post-action inputs outside an additional nested composite context. Confirm cache read-only
behavior and the absence of post-action warnings when validating changes to this setup.

Five Broker groups use two class shards each. `-PtestClassShard=1/2` and `2/2` assign top-level class names by
CRC32, keeping nested classes together and leaving the runtime classpath intact. TestNG group filters, inherited
methods, factories and data providers still apply. The original unsharded dispatcher names remain available for
local reproduction. Isolated tests are invoked once outside the shard filter. Run
`python3 pulsar-build/verify_ci_broker_shards.py` and `python3 pulsar-build/verify_ci_test_partitions.py` to check the
dispatcher and suite partitions without Docker. The Gradle/TestNG fixture can also be run with
`python3 pulsar-build/tests/test_ci_test_class_shards.py`.

The build output archives exclude `.gradle` directories. Gradle caches are managed separately by `setup-gradle`;
a cold or incompatible cache can still cause compilation after an archive is restored. Compare actual compilation
steps, artifact transfer time, queue time and accumulated runner minutes when assessing the benefit. CRC32 gives
a stable initial assignment, but class counts do not guarantee equal test duration. Use successful test XML reports
retained in personal CI to measure the next balancing change against the same-revision baseline. Overall CI time
is measured through the final required check, including CodeQL and publication validation.

When working on workflow changes, one way to find out the names of the status checks is to retrieve the names
from the PR build run. The "check-runs" can be found by commit id. Here's an example:

```shell
curl -s "https://api.github.com/repos/apache/pulsar/commits/$(git rev-parse HEAD)/check-runs" | \
  jq -r '.check_runs | .[] | .name' |sort
```
