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

package org.apache.hadoop.mapreduce.v2.app.speculate;

import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.yarn.event.EventHandler;

/**
 * Speculator component. Task Attempts' status updates are sent to this
 * component. Concrete implementation runs the speculative algorithm and
 * sends the TaskEventType.T_ADD_ATTEMPT.
 *
 * An implementation also has to arrange for the jobs to be scanned from
 * time to time, to launch the speculations.
 */
public interface Speculator
              extends EventHandler<SpeculatorEvent> {

  enum EventType {
    ATTEMPT_STATUS_UPDATE,
    ATTEMPT_START,
    TASK_CONTAINER_NEED_UPDATE,
    JOB_CREATE
  }

  // This will be implemented if we go to a model where the events are
  //  processed within the TaskAttempts' state transitions' code.
  public void handleAttempt(TaskAttemptStatus status);
}
