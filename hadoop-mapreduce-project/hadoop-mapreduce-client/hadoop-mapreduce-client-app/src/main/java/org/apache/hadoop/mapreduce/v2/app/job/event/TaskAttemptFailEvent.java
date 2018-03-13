/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.v2.app.job.event;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

public class TaskAttemptFailEvent extends TaskAttemptEvent {
  private boolean fastFail;

  /**
   * Create a new TaskAttemptFailEvent, with task fastFail disabled.
   *
   * @param id the id of the task attempt
   */
  public TaskAttemptFailEvent(TaskAttemptId id) {
    this(id, false);
  }

  /**
   * Create a new TaskAttemptFailEvent.
   *
   * @param id the id of the task attempt
   * @param fastFail should the task fastFail or not.
   */
  public TaskAttemptFailEvent(TaskAttemptId id, boolean fastFail) {
    super(id, TaskAttemptEventType.TA_FAILMSG);
    this.fastFail = fastFail;
  }

  /**
   * Check if task should fast fail or retry
   * @return boolean value where true indicates the task should not retry
   */
  public boolean isFastFail() {
    return fastFail;
  }
}