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

import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.CurrentTasks;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.CurrentTasksFactory;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTask;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask.MultipartInitMetadataSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Track finished/failed sync task in INIT phase.
 */
public class InitTracker {

  private final CurrentTasksFactory currentTasksFactory;
  private CurrentTasks<MultipartInitMetadataSyncTask> currentInitTasks;
  private Function<ByteBuffer, List<BlockSyncTask>> createPutParts;
  private BiFunction<ByteBuffer, List<ByteBuffer>,
      MetadataSyncTask.MultipartCompleteMetadataSyncTask> createComplete;

  public InitTracker(MultipartInitMetadataSyncTask init,
      Function<ByteBuffer, List<BlockSyncTask>> createPutParts,
      BiFunction<ByteBuffer, List<ByteBuffer>,
          MetadataSyncTask.MultipartCompleteMetadataSyncTask> createComplete,
      CurrentTasksFactory currentTasksFactory) {
    this.currentInitTasks = currentTasksFactory.create(init);
    this.createPutParts = createPutParts;
    this.createComplete = createComplete;
    this.currentTasksFactory = currentTasksFactory;
  }

  public Optional<PartsTracker> markFinished(UUID syncTaskId,
      SyncTaskExecutionResult result) {
    currentInitTasks.markFinished(syncTaskId);
    if (currentInitTasks.isNotFinished()) {
      return Optional.empty();
    } else {
      ByteBuffer multipartUploadId = result.getResult();
      return Optional.of(
          new PartsTracker(
              createPutParts.apply(multipartUploadId),
              uploadIds -> createComplete.apply(multipartUploadId, uploadIds),
              currentTasksFactory
          )
      );
    }
  }

  public boolean markFailed(UUID syncTaskId, SyncTaskExecutionResult result) {
    return currentInitTasks.markFailure(syncTaskId);
  }

  public Optional<MetadataSyncTask> getTasks() {
    List<MultipartInitMetadataSyncTask> tasksToDo =
        this.currentInitTasks.getTasksToDo();
    if (tasksToDo.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(tasksToDo.get(0));
    }
  }

  public boolean isTaskUnderTrack(UUID syncTaskId) {
    return currentInitTasks.isTaskUnderTrack(syncTaskId);
  }
}
