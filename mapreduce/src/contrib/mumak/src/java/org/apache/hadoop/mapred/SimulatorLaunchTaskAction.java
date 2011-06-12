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

import org.apache.hadoop.tools.rumen.TaskAttemptInfo;

/**
 * This class is used to augment {@link LaunchTaskAction} with run time statistics 
 * and the final task state (successfull xor failed).
 */
class SimulatorLaunchTaskAction extends LaunchTaskAction {
  /**
   * Run time resource usage of the task.
   */
  private TaskAttemptInfo taskAttemptInfo;

  /**
   * Constructs a SimulatorLaunchTaskAction object for a {@link Task}.
   * @param task Task task to be launched
   * @param taskAttemptInfo resource usage model for task execution
   */            
  public SimulatorLaunchTaskAction(Task task,
                                   TaskAttemptInfo taskAttemptInfo) {
    super(task);
    this.taskAttemptInfo = taskAttemptInfo;
  }
  
  /** Get the resource usage model for the task. */
  public TaskAttemptInfo getTaskAttemptInfo() {
    return taskAttemptInfo;
  }
  
  @Override
  public String toString() {
    return this.getClass().getName() + "[taskID=" + 
           this.getTask().getTaskID() + "]";
  }
}
