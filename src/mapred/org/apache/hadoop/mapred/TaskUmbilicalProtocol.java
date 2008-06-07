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

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

/** Protocol that task child process uses to contact its parent process.  The
 * parent is a daemon which which polls the central master for a new map or
 * reduce task and runs it as a child process.  All communication between child
 * and parent is via this protocol. */ 
interface TaskUmbilicalProtocol extends VersionedProtocol {

  /** 
   * Changed the version to 2, since we have a new method getMapOutputs 
   * Changed version to 3 to have progress() return a boolean
   * Changed the version to 4, since we have replaced 
   *         TaskUmbilicalProtocol.progress(String, float, String, 
   *         org.apache.hadoop.mapred.TaskStatus.Phase, Counters) 
   *         with statusUpdate(String, TaskStatus)
   * 
   * Version 5 changed counters representation for HADOOP-2248
   * Version 6 changes the TaskStatus representation for HADOOP-2208
   * Version 7 changes the done api (via HADOOP-3140). It now expects whether
   *           or not the task's output needs to be promoted.
   * Version 8 changes {job|tip|task}id's to use their corresponding 
   * objects rather than strings.
   * Version 9 changes the counter representation for HADOOP-1915
   * */

  public static final long versionID = 9L;
  
  /** Called when a child task process starts, to get its task.*/
  Task getTask(TaskAttemptID taskid) throws IOException;

  /**
   * Report child's progress to parent.
   * 
   * @param taskId task-id of the child
   * @param taskStatus status of the child
   * @throws IOException
   * @throws InterruptedException
   * @return True if the task is known
   */
  boolean statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus) 
  throws IOException, InterruptedException;
  
  /** Report error messages back to parent.  Calls should be sparing, since all
   *  such messages are held in the job tracker.
   *  @param taskid the id of the task involved
   *  @param trace the text to report
   */
  void reportDiagnosticInfo(TaskAttemptID taskid, String trace) throws IOException;

  /** Periodically called by child to check if parent is still alive. 
   * @return True if the task is known
   */
  boolean ping(TaskAttemptID taskid) throws IOException;

  /** Report that the task is successfully completed.  Failure is assumed if
   * the task process exits without calling this.
   * @param taskid task's id
   * @param shouldBePromoted whether to promote the task's output or not 
   */
  void done(TaskAttemptID taskid, boolean shouldBePromoted) throws IOException;

  /** Report that a reduce-task couldn't shuffle map-outputs.*/
  void shuffleError(TaskAttemptID taskId, String message) throws IOException;
  
  /** Report that the task encounted a local filesystem error.*/
  void fsError(TaskAttemptID taskId, String message) throws IOException;

  /** Called by a reduce task to get the map output locations for finished maps.
   *
   * @param taskId the reduce task id
   * @param fromIndex the index starting from which the locations should be 
   * fetched
   * @param maxLocs the max number of locations to fetch
   * @return an array of TaskCompletionEvent
   */
  TaskCompletionEvent[] getMapCompletionEvents(JobID jobId, 
                                               int fromIndex, int maxLocs) throws IOException;

}
