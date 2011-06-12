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

import java.io.File;
import java.util.List;

/**
 * This TaskTrackerInstrumentation subclass forwards all the events it
 * receives to a list of instrumentation objects, and can thus be used to
 * attack multiple instrumentation objects to a TaskTracker.
 */
class CompositeTaskTrackerInstrumentation extends TaskTrackerInstrumentation {
  
  private List<TaskTrackerInstrumentation> instrumentations;

  public CompositeTaskTrackerInstrumentation(TaskTracker tt,
      List<TaskTrackerInstrumentation> instrumentations) {
    super(tt);
    this.instrumentations = instrumentations;
  }

  // Package-private getter methods for tests
  List<TaskTrackerInstrumentation> getInstrumentations() {
    return instrumentations;
  }
  
  @Override
  public void completeTask(TaskAttemptID t) {
    for (TaskTrackerInstrumentation tti: instrumentations) {
      tti.completeTask(t);
    }
  }
  
  @Override
  public void timedoutTask(TaskAttemptID t) {
    for (TaskTrackerInstrumentation tti: instrumentations) {
      tti.timedoutTask(t);
    }
  }
  
  @Override
  public void taskFailedPing(TaskAttemptID t) {
    for (TaskTrackerInstrumentation tti: instrumentations) {
      tti.taskFailedPing(t);
    }
  }

  @Override
  public void reportTaskLaunch(TaskAttemptID t, File stdout, File stderr) {
    for (TaskTrackerInstrumentation tti: instrumentations) {
      tti.reportTaskLaunch(t, stdout, stderr);
    }
  }
  
  @Override
  public void reportTaskEnd(TaskAttemptID t) {
    for (TaskTrackerInstrumentation tti: instrumentations) {
      tti.reportTaskEnd(t);
    }
  }
   
  @Override
  public void statusUpdate(Task task, TaskStatus taskStatus) {
    for (TaskTrackerInstrumentation tti: instrumentations) {
      tti.statusUpdate(task, taskStatus);
    }
  }
}
