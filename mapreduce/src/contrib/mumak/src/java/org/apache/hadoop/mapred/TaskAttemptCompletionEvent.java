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
package org.apache.hadoop.mapred;

/**
 * This class is used by SimulatorTaskTrackers for signaling themselves when
 * a task attempt finishes. The rationale for having this redundant event sent 
 * is that (1) this way it is possible to monitor all task completion events
 * centrally from the Engine. (2) TTs used to call heartbeat() of the job 
 * tracker right after the task completed (so called "crazy heartbeats") not 
 * waiting for the heartbeat interval. If we wanted to simulate that we need 
 * to decouple task completion monitoring from periodic heartbeats. 
 */
public class TaskAttemptCompletionEvent extends SimulatorEvent {

  /** The final status of the completed task. */  
  private final TaskStatus status;

  /**
   * Constructs a task completion event from a task status.
   * @param listener the SimulatorTaskTracker the task is running on
   * @param status the final status of the completed task. Precondition: 
   *                status.getRunState() must be either State.SUCCEEDED or 
   *                State.FAILED.
   */
  public TaskAttemptCompletionEvent(SimulatorEventListener listener,
                                    TaskStatus status) {
    super(listener, status.getFinishTime());
    this.status = status;
  }
  
  /** Returns the final status of the task. */
  public TaskStatus getStatus() {
    return status;
  }
  
  @Override
  protected String realToString() {
    return super.realToString() + ", taskID=" + status.getTaskID();
  }
}
