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
 * Represents a directive from the {@link org.apache.hadoop.mapred.JobTracker} 
 * to the {@link org.apache.hadoop.mapred.TaskTracker} to launch a new task.
 * 
 */
class LaunchTaskAction extends TaskTrackerAction {
  private TTTask task;

  public LaunchTaskAction() {
    super(ActionType.LAUNCH_TASK);
  }
  
  public LaunchTaskAction(Task task) {
    super(ActionType.LAUNCH_TASK);
    if (task.isMapTask()) {
      this.task = new TTMapTask((MapTask)task);
    } else {
      if (task.isUberTask()) {
        this.task = new TTUberTask((UberTask)task);
      } else {
        this.task = new TTReduceTask((ReduceTask)task);
      }
    }
  }
  
  public TTTask getTask() {
    return task;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    boolean isMapTask = in.readBoolean();
    if (isMapTask) {
      this.task = new TTMapTask(new MapTask());
    } else {
      boolean isUberTask = in.readBoolean();
      if (isUberTask) {
        task = new TTUberTask(new UberTask());
      } else {
        task = new TTReduceTask(new ReduceTask());
      }
    }
    task.getTask().readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Task task = this.task.getTask();
    out.writeBoolean(task.isMapTask());
    if (!task.isMapTask()) {
      // which flavor of ReduceTask, uber or regular?
      out.writeBoolean(task.isUberTask());
    }
    task.write(out);
  }
  
  
}
