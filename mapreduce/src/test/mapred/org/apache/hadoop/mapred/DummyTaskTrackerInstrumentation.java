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

/**
 * Mock instrumentation class used in TaskTrackerInstrumentation tests.
 * This class just records whether each instrumentation method was called.
 */
public class DummyTaskTrackerInstrumentation
  extends TaskTrackerInstrumentation
{
  boolean completeTaskCalled = false;
  boolean timedoutTaskCalled = false;
  boolean taskFailedPingCalled = false;
  boolean reportTaskLaunchCalled = false;
  boolean reportTaskEndCalled = false;
  boolean statusUpdateCalled = false;

  public DummyTaskTrackerInstrumentation(TaskTracker tt) {
    super(tt);
  }

  @Override
  public void completeTask(TaskAttemptID t) {
    completeTaskCalled = true;
  }

  @Override
  public void timedoutTask(TaskAttemptID t) {
    timedoutTaskCalled = true;
  }

  @Override
  public void taskFailedPing(TaskAttemptID t) {
    taskFailedPingCalled = true;
  }

  @Override
  public void reportTaskLaunch(TaskAttemptID t, File stdout, File stderr) {
    reportTaskLaunchCalled = true;
  }

  @Override
  public void reportTaskEnd(TaskAttemptID t) {
    reportTaskEndCalled = true;
  }

  @Override
  public void statusUpdate(Task t, TaskStatus s) {
    statusUpdateCalled = true;
  }
}
