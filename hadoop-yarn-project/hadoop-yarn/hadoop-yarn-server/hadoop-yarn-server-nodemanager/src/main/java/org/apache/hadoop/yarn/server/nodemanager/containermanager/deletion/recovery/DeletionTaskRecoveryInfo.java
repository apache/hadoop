/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.recovery;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.DeletionTask;

import java.util.List;

/**
 * Encapsulates the recovery info needed to recover a DeletionTask from the NM
 * state store.
 */
public class DeletionTaskRecoveryInfo {

  private DeletionTask task;
  private List<Integer> successorTaskIds;
  private long deletionTimestamp;

  /**
   * Information needed for recovering the DeletionTask.
   *
   * @param task the DeletionTask
   * @param successorTaskIds the dependent DeletionTasks.
   * @param deletionTimestamp the scheduled times of deletion.
   */
  public DeletionTaskRecoveryInfo(DeletionTask task,
      List<Integer> successorTaskIds, long deletionTimestamp) {
    this.task = task;
    this.successorTaskIds = successorTaskIds;
    this.deletionTimestamp = deletionTimestamp;
  }

  /**
   * Return the recovered DeletionTask.
   *
   * @return the recovered DeletionTask.
   */
  public DeletionTask getTask() {
    return task;
  }

  /**
   * Return all of the dependent DeletionTasks.
   *
   * @return the dependent DeletionTasks.
   */
  public List<Integer> getSuccessorTaskIds() {
    return successorTaskIds;
  }

  /**
   * Return the deletion timestamp.
   *
   * @return the deletion timestamp.
   */
  public long getDeletionTimestamp() {
    return deletionTimestamp;
  }
}