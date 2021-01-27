/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.multipart;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Maintain bytes uploaded for each block sync task.
 */
public class UploadHandlesCollector {

  private final Map<UUID, Optional<ByteBuffer>> uploadHandles;

  public UploadHandlesCollector(List<UUID> tasksToFinish) {
    this.uploadHandles = Maps.newHashMap();
    tasksToFinish.forEach(id -> uploadHandles.put(id, Optional.empty()));
  }

  public void addHandle(UUID id, ByteBuffer handle) {
    Optional<ByteBuffer> bytes = uploadHandles.get(id);
    if (bytes != null) {
      uploadHandles.replace(id, Optional.of(handle));
    }
  }

  public boolean allPresent() {
    return uploadHandles
        .values()
        .stream()
        .allMatch(Optional::isPresent);
  }

  public List<ByteBuffer> getCollectedHandles() {
    return uploadHandles
        .values()
        .stream()
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }
}
