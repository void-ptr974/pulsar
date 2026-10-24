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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;
import org.gradle.api.file.FileTreeElement;
import org.gradle.api.specs.Spec;

/** Excludes candidate class files assigned to another CI shard, without changing the test classpath. */
public final class TestClassShardFilter implements Spec<FileTreeElement>, Serializable {
    private static final long serialVersionUID = 1L;

    private final int shardIndex;
    private final int shardCount;

    public TestClassShardFilter(String shard) {
        if (!shard.matches("[1-9][0-9]*/[1-9][0-9]*")) {
            throw new IllegalArgumentException("testClassShard must be a one-based index/count, for example 1/2");
        }
        String[] parts = shard.split("/");
        shardIndex = Integer.parseInt(parts[0]) - 1;
        shardCount = Integer.parseInt(parts[1]);
        if (shardIndex >= shardCount) {
            throw new IllegalArgumentException("testClassShard index must be between 1 and its count: " + shard);
        }
    }

    @Override
    public boolean isSatisfiedBy(FileTreeElement element) {
        if (element.isDirectory()) {
            return false;
        }
        String path = element.getRelativePath().getPathString();
        if (!path.endsWith(".class")) {
            return false;
        }
        // Keep all nested/anonymous classes with their top-level class. The complete test class,
        // including inherited methods, data-provider rows and Factory instances, remains intact.
        String className = path.substring(0, path.length() - ".class".length()).split("\\$", 2)[0]
                .replace('/', '.');
        CRC32 crc = new CRC32();
        crc.update(className.getBytes(StandardCharsets.UTF_8));
        return crc.getValue() % shardCount != shardIndex;
    }
}
