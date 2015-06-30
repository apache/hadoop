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

/**
 * TaskAttemptTooManyFetchFailureEvent is used for TA_TOO_MANY_FETCH_FAILURE.
 */
public class TaskAttemptTooManyFetchFailureEvent extends TaskAttemptEvent {
  private TaskAttemptId reduceID;
  private String  reduceHostname;

  /**
   * Create a new TaskAttemptTooManyFetchFailureEvent.
   * @param attemptId the id of the mapper task attempt
   * @param reduceId the id of the reporting reduce task attempt.
   * @param reduceHost the hostname of the reporting reduce task attempt.
   */
  public TaskAttemptTooManyFetchFailureEvent(TaskAttemptId attemptId,
      TaskAttemptId reduceId, String reduceHost) {
      super(attemptId, TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURE);
    this.reduceID = reduceId;
    this.reduceHostname = reduceHost;
  }

  public TaskAttemptId getReduceId() {
    return reduceID;
  }

  public String getReduceHost() {
    return reduceHostname;
  }  
}
