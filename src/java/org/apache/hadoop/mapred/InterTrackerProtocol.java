/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.*;

/** 
 * Protocol that a TaskTracker and the central JobTracker use to communicate.
 * The JobTracker is the Server, which implements this protocol.
 */ 
interface InterTrackerProtocol {
  public final static int TRACKERS_OK = 0;
  public final static int UNKNOWN_TASKTRACKER = 1;

  /** 
   * Called regularly by the task tracker to update the status of its tasks
   * within the job tracker.  JobTracker responds with a code that tells the 
   * TaskTracker whether all is well.
   *
   * TaskTracker must also indicate whether this is the first interaction
   * (since state refresh)
   */
  int emitHeartbeat(TaskTrackerStatus status, boolean initialContact);

  /** Called to get new tasks from from the job tracker for this tracker.*/
  Task pollForNewTask(String trackerName);

  /** Called to find which tasks that have been run by this tracker should now
   * be closed because their job is complete.  This is used to, e.g., 
   * notify a map task that its output is no longer needed and may 
   * be removed. */
  String pollForTaskWithClosedJob(String trackerName);

  /** Called by a reduce task to find which map tasks are completed.
   *
   * @param taskId the reduce task id
   * @param mapTasksNeeded an array of UTF8 naming map task ids whose output is needed.
   * @return an array of MapOutputLocation
   */
  MapOutputLocation[] locateMapOutputs(String taskId, String[][] mapTasksNeeded);

  /**
   * The task tracker calls this once, to discern where it can find
   * files referred to by the JobTracker
   */
  public String getFilesystemName() throws IOException;
}


