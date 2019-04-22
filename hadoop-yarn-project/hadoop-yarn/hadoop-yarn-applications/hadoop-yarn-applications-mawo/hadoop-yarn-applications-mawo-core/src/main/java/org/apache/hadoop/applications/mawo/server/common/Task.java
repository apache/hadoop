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

package org.apache.hadoop.applications.mawo.server.common;

import java.util.Map;

import org.apache.hadoop.io.Writable;

/**
 * Define Task Interface.
 */
public interface Task extends Writable {

  /**
   * Get TaskId of a Task.
   * @return value of TaskId
   */
  TaskId getTaskId();

  /**
   * Get Environment of Task.
   * @return map of environment
   */
  Map<String, String> getEnvironment();

  /**
   * Get Task cmd.
   * @return value of Task cmd such "sleep 1"
   */
  String getTaskCmd();

  /**
   * Get Task type such as Simple, Composite.
   * @return value of TaskType
   */
  TaskType getTaskType();

  /**
   * Set TaskId.
   * @param taskId : Task identifier
   */
  void setTaskId(TaskId taskId);

  /**
   * Set Task environment such as {"HOME":"/user/A"}.
   * @param environment : Map of environment variables
   */
  void setEnvironment(Map<String, String> environment);

  /**
   * Set Task command.
   * @param taskCMD : Task command to be executed
   */
  void setTaskCmd(String taskCMD);

  /**
   * Get Task Timeout in seconds.
   * @return value of TaskTimeout
   */
  long getTimeout();

  /**
   * Set Task Timeout.
   * @param timeout : value of Task Timeout
   */
  void setTimeout(long timeout);
}
