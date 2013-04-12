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

public class TaskAttemptContainerLaunchedEvent extends TaskAttemptEvent {
  private int shufflePort;

  /**
   * Create a new TaskAttemptEvent.
   * @param id the id of the task attempt
   * @param shufflePort the port that shuffle is listening on.
   */
  public TaskAttemptContainerLaunchedEvent(TaskAttemptId id, int shufflePort) {
    super(id, TaskAttemptEventType.TA_CONTAINER_LAUNCHED);
    this.shufflePort = shufflePort;
  }

  
  /**
   * Get the port that the shuffle handler is listening on. This is only
   * valid if the type of the event is TA_CONTAINER_LAUNCHED
   * @return the port the shuffle handler is listening on.
   */
  public int getShufflePort() {
    return shufflePort;
  }
}
