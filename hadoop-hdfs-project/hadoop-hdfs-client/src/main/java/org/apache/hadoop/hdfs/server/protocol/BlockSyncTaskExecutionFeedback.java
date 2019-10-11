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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.hdfs.protocol.BlockSyncTaskOperation;

import java.util.UUID;

public class BlockSyncTaskExecutionFeedback {

  private UUID syncTaskId;
  private SyncTaskExecutionOutcome outcome;
  private SyncTaskExecutionResult result;
  private String syncMountId;
  private BlockSyncTaskOperation operation;

  public BlockSyncTaskExecutionFeedback(UUID syncTaskId,
      SyncTaskExecutionOutcome outcome,
      SyncTaskExecutionResult result,
      String syncMountId,
      BlockSyncTaskOperation operation) {
    this.syncTaskId = syncTaskId;
    this.outcome = outcome;
    this.result = result;
    this.syncMountId = syncMountId;
    this.operation = operation;
  }

  public static BlockSyncTaskExecutionFeedback finishedSuccessfully(UUID syncTaskId,
      String syncMountId,
      SyncTaskExecutionResult result,
      BlockSyncTaskOperation operation) {
    return new BlockSyncTaskExecutionFeedback(syncTaskId, SyncTaskExecutionOutcome.FINISHED_SUCCESSFULLY,
        result, syncMountId, operation);
  }

  public static BlockSyncTaskExecutionFeedback failedWithException(UUID syncTaskId,
     String syncMountId,
     Exception e,
     BlockSyncTaskOperation operation) {
    return new BlockSyncTaskExecutionFeedback(syncTaskId, SyncTaskExecutionOutcome.EXCEPTION,
        null, syncMountId, operation);
  }

  public UUID getSyncTaskId() {
    return syncTaskId;
  }

  public SyncTaskExecutionOutcome getOutcome() {
    return outcome;
  }

  public SyncTaskExecutionResult getResult() {
    return result;
  }

  public String getSyncMountId() {
    return syncMountId;
  }

  public BlockSyncTaskOperation getOperation() {
    return operation;
  }

}
