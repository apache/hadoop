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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class is used for notifying a SimulatorTaskTracker running a reduce task
 * that all map tasks of the job are done. A SimulatorJobTracker notifies a
 * SimulatorTaskTracker by sending this TaskTrackerAction in response to a
 * heartbeat(). Represents a directive to start running the user code of the
 * reduce task.
 * 
 * We introduced this extra 'push' mechanism so that we don't have to implement
 * the corresponding, more complicated 'pull' part of the InterTrackerProtocol.
 * We do not use proper simulation Events for signaling, and hack heartbeat()
 * instead, since the job tracker does not emit Events and does not know the
 * recipient task tracker _Java_ object.
 */
class AllMapsCompletedTaskAction extends TaskTrackerAction {
  /** Task attempt id of the reduce task that can proceed. */
  private final org.apache.hadoop.mapreduce.TaskAttemptID taskId;

  /**
   * Constructs an AllMapsCompletedTaskAction object for a given
   * {@link org.apache.hadoop.mapreduce.TaskAttemptID}.
   * 
   * @param taskId
   *          {@link org.apache.hadoop.mapreduce.TaskAttemptID} of the reduce
   *          task that can proceed
   */
  public AllMapsCompletedTaskAction(
      org.apache.hadoop.mapreduce.TaskAttemptID taskId) {
    super(ActionType.LAUNCH_TASK);
    this.taskId = taskId;
  }

  /**
   * Get the task attempt id of the reduce task.
   * 
   * @return the {@link org.apache.hadoop.mapreduce.TaskAttemptID} of the
   *         task-attempt.
   */
  public org.apache.hadoop.mapreduce.TaskAttemptID getTaskID() {
    return taskId;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    taskId.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    taskId.readFields(in);
  }

  @Override
  public String toString() {
    return "AllMapsCompletedTaskAction[taskID=" + taskId + "]";
  }
}
