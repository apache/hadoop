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
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Tack finished/failed sync task in PUT phase.
 */
public class PartsTracker {

  private final UploadHandlesCollector uploadHandlesCollector;
  private final CurrentTasksFactory currentTasksFactory;
  private final Function<List<ByteBuffer>,
      MetadataSyncTask.MultipartCompleteMetadataSyncTask> createComplete;
  private CurrentTasks<BlockSyncTask> currentPutPartTasks;

  public PartsTracker(List<BlockSyncTask> putPartSyncTasks,
      Function<List<ByteBuffer>,
          MetadataSyncTask.MultipartCompleteMetadataSyncTask> createComplete,
      CurrentTasksFactory currentTasksFactory) {
    this.createComplete = createComplete;
    this.currentPutPartTasks = currentTasksFactory.create(putPartSyncTasks);
    this.uploadHandlesCollector = new UploadHandlesCollector(
        putPartSyncTasks
            .stream()
            .map(BlockSyncTask::getSyncTaskId)
            .collect(Collectors.toList())
    );
    this.currentTasksFactory = currentTasksFactory;
  }

  public Optional<CompleteTracker> markFinished(UUID syncTaskId,
      SyncTaskExecutionResult result) {
    this.currentPutPartTasks.markFinished(syncTaskId);
    this.uploadHandlesCollector.addHandle(syncTaskId, result.getResult());
    if (this.uploadHandlesCollector.allPresent()) {
      return Optional.of(
          new CompleteTracker(
              this.createComplete.apply(
                  this.uploadHandlesCollector.getCollectedHandles()),
              currentTasksFactory
          )
      );
    } else {
      return Optional.empty();
    }
  }

  public boolean markFailed(UUID syncTaskId, SyncTaskExecutionResult result) {
    return this.currentPutPartTasks.markFailure(syncTaskId);
  }

  public List<BlockSyncTask> getPuts() {
    return this.currentPutPartTasks.getTasksToDo();
  }

  public boolean isTaskUnderTrack(UUID syncTaskId) {
    return currentPutPartTasks.isTaskUnderTrack(syncTaskId);
  }
}
