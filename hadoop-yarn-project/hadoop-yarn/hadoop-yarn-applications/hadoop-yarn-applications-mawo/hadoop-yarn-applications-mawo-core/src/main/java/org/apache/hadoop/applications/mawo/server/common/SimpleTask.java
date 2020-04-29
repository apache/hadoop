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

/**
 * Define Simple Task.
 * Each Task has only one command
 */
public class SimpleTask extends AbstractTask {
  /**
   * Simple Task default initializer.
   */
  public SimpleTask() {
    super();
    this.setTaskType(TaskType.SIMPLE);
  }

  /**
   * Set up Simple Task with Task object.
   * @param task : Task object
   */
  public SimpleTask(final Task task) {
    this(task.getTaskId(), task.getEnvironment(), task.getTaskCmd(),
        task.getTimeout());
  }

  /**
   * Create Simple Task with Task details.
   * @param taskId : task identifier
   * @param environment : task environment
   * @param taskCMD : task command
   * @param timeout : task timeout
   */
  public SimpleTask(final TaskId taskId, final Map<String, String> environment,
      final String taskCMD, final long timeout) {
    super(taskId, environment, taskCMD, timeout);
    this.setTaskType(TaskType.SIMPLE);
  }
}
