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
package org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker;

import org.apache.hadoop.hdfs.server.protocol.SyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask.CreateFileSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Track sync phase for a sync mount (backup/writeBack mount).
 */
public interface SyncMountSnapshotUpdateTracker {
  boolean isFinished();

  Optional<SyncTask> markFinished(UUID syncTaskId,
      SyncTaskExecutionResult result);

  List<CreateFileSyncTask> getFinishedFileSync();

  SchedulableSyncPhase getNextSchedulablePhase();

  boolean markFailed(UUID syncTaskId, SyncTaskExecutionResult result);

  boolean blockingCancel();

  boolean isTaskUnderTrack(UUID syncTaskId);
}
