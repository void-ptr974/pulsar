/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;
import org.gradle.api.GradleException;
import org.gradle.api.Task;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

/** Prepares the actual Test dependencies without retaining the Test provider's producer dependency. */
public final class CiTestArtifactDependencies {
    private CiTestArtifactDependencies() {
    }

    public static void configure(Task prepare, TaskProvider<? extends Test> targetProvider) {
        prepare.getExtensions().getExtraProperties().set("ciArtifactTargetTestName", targetProvider.getName());
        // TaskProvider.map would also schedule the Test task. Resolve a plain provider while
        // Gradle constructs the graph, then let the CI verifier compare the captured paths with
        // the finalized graph. Re-resolving an unscheduled Test in whenReady is illegal in Gradle.
        prepare.dependsOn(prepare.getProject().getProviders().provider(() -> {
            Test target = targetProvider.get();
            Set<Task> dependencies = new LinkedHashSet<>(target.getTaskDependencies().getDependencies(target));
            Set<String> paths = new TreeSet<>();
            for (Task dependency : dependencies) {
                if (dependency instanceof Test) {
                    throw new GradleException("Cannot prepare " + target.getPath()
                            + ": its declared dependencies include Test task " + dependency.getPath());
                }
                paths.add(dependency.getPath());
            }
            prepare.getExtensions().getExtraProperties().set("ciArtifactTargetTestPath", target.getPath());
            prepare.getExtensions().getExtraProperties().set("ciArtifactExpectedDependencies", paths);
            return dependencies;
        }));
    }
}
