/*
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
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask.MultipartCompleteMetadataSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class CompleteTracker {

  private CurrentTasks<MultipartCompleteMetadataSyncTask> currentCompleteTasks;

  public CompleteTracker(MultipartCompleteMetadataSyncTask completeMetadataSyncTask,
      CurrentTasksFactory currentTasksFactory) {
    this.currentCompleteTasks = currentTasksFactory.create(completeMetadataSyncTask);
  }

  public void markFinished(UUID syncTaskId, Runnable trackerFinalizer) {
    Optional<MultipartCompleteMetadataSyncTask> multipartCompleteMetadataSyncTask =
        this.currentCompleteTasks.markFinished(syncTaskId);

    if (this.currentCompleteTasks.isFinished()) {
      trackerFinalizer.run();
    }
  }

  public boolean markFailed(UUID syncTaskId, SyncTaskExecutionResult result) {
    return this.currentCompleteTasks.markFailure(syncTaskId);
  }

  public Optional<MetadataSyncTask> getComplete() {
    List<MultipartCompleteMetadataSyncTask> tasksToDo =
        this.currentCompleteTasks.getTasksToDo();
    if (tasksToDo.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(tasksToDo.get(0));
    }
  }
}
