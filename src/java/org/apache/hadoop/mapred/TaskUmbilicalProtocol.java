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

  public static final long versionID = 1L;
  
  /** Called when a child task process starts, to get its task.*/
  Task getTask(String taskid) throws IOException;

  /** Report child's progress to parent.
   * @param taskid the id of the task
   * @param progress value between zero and one
   * @param state description of task's current state
   * @param phase current phase of the task.
   */
  void progress(String taskid, float progress, String state, 
                TaskStatus.Phase phase)
    throws IOException;

  /** Report error messages back to parent.  Calls should be sparing, since all
   *  such messages are held in the job tracker.
   *  @param taskid the id of the task involved
   *  @param trace the text to report
   */
  void reportDiagnosticInfo(String taskid, String trace) throws IOException;

  /** Periodically called by child to check if parent is still alive. 
   * @return True if the task is known
   */
  boolean ping(String taskid) throws IOException;

  /** Report that the task is successfully completed.  Failure is assumed if
   * the task process exits without calling this. */
  void done(String taskid) throws IOException;

  /** Report that the task encounted a local filesystem error.*/
  void fsError(String message) throws IOException;

}
