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

import java.io.*;

import org.apache.hadoop.ipc.VersionedProtocol;

/** 
 * Protocol that a TaskTracker and the central JobTracker use to communicate.
 * The JobTracker is the Server, which implements this protocol.
 */ 
interface InterTrackerProtocol extends VersionedProtocol {
  /**
   * version 3 introduced to replace 
   * emitHearbeat/pollForNewTask/pollForTaskWithClosedJob with
   * {@link #heartbeat(TaskTrackerStatus, boolean, boolean, short)}
   */
  public static final long versionID = 3L;
  
  public final static int TRACKERS_OK = 0;
  public final static int UNKNOWN_TASKTRACKER = 1;

  /**
   * Called regularly by the {@link TaskTracker} to update the status of its 
   * tasks within the job tracker. {@link JobTracker} responds with a 
   * {@link HeartbeatResponse} that directs the 
   * {@link TaskTracker} to undertake a series of 'actions' 
   * (see {@link org.apache.hadoop.mapred.TaskTrackerAction.ActionType}).  
   * 
   * {@link TaskTracker} must also indicate whether this is the first 
   * interaction (since state refresh) and acknowledge the last response
   * it recieved from the {@link JobTracker} 
   * 
   * @param status the status update
   * @param initialContact <code>true</code> if this is first interaction since
   *                       'refresh', <code>false</code> otherwise.
   * @param acceptNewTasks <code>true</code> if the {@link TaskTracker} is
   *                       ready to accept new tasks to run.                 
   * @param responseId the last responseId successfully acted upon by the
   *                   {@link TaskTracker}.
   * @return a {@link org.apache.hadoop.mapred.HeartbeatResponse} with 
   *         fresh instructions.
   */
  HeartbeatResponse heartbeat(TaskTrackerStatus status, 
          boolean initialContact, boolean acceptNewTasks, short responseId)
  throws IOException;

  /** Called by a reduce task to find which map tasks are completed.
   *
   * @param jobId the job id
   * @param mapTasksNeeded an array of the mapIds that we need
   * @param partition the reduce's id
   * @return an array of MapOutputLocation
   */
  MapOutputLocation[] locateMapOutputs(String jobId, 
                                       int[] mapTasksNeeded,
                                       int partition
                                       ) throws IOException;

  /**
   * The task tracker calls this once, to discern where it can find
   * files referred to by the JobTracker
   */
  public String getFilesystemName() throws IOException;
  
  /**
   * Report a problem to the job tracker.
   * @param taskTracker the name of the task tracker
   * @param errorClass the kind of error (eg. the class that was thrown)
   * @param errorMessage the human readable error message
   * @throws IOException if there was a problem in communication or on the
   *                     remote side
   */
  public void reportTaskTrackerError(String taskTracker,
                                     String errorClass,
                                     String errorMessage) throws IOException;
}


