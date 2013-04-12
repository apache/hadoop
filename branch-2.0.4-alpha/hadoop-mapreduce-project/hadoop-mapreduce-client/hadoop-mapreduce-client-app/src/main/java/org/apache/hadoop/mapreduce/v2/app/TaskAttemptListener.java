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

package org.apache.hadoop.mapreduce.v2.app;

import java.net.InetSocketAddress;

import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.WrappedJvmID;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

/**
 * This class listens for changes to the state of a Task.
 */
public interface TaskAttemptListener {

  InetSocketAddress getAddress();

  /**
   * Register a JVM with the listener.  This should be called as soon as a 
   * JVM ID is assigned to a task attempt, before it has been launched.
   * @param task the task itself for this JVM.
   * @param jvmID The ID of the JVM .
   */
  void registerPendingTask(Task task, WrappedJvmID jvmID);
  
  /**
   * Register task attempt. This should be called when the JVM has been
   * launched.
   * 
   * @param attemptID
   *          the id of the attempt for this JVM.
   * @param jvmID the ID of the JVM.
   */
  void registerLaunchedTask(TaskAttemptId attemptID, WrappedJvmID jvmID);

  /**
   * Unregister the JVM and the attempt associated with it.  This should be 
   * called when the attempt/JVM has finished executing and is being cleaned up.
   * @param attemptID the ID of the attempt.
   * @param jvmID the ID of the JVM for that attempt.
   */
  void unregister(TaskAttemptId attemptID, WrappedJvmID jvmID);

}
