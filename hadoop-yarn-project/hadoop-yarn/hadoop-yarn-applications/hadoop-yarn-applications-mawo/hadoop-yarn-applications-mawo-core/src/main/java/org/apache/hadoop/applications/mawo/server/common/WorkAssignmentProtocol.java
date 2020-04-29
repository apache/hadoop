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

import org.apache.hadoop.applications.mawo.server.worker.WorkerId;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolInfo;

/**
 * Define work assignment protocol.
 */
@ProtocolInfo(protocolName = "WorkAssignmentProtocol", protocolVersion = 1)
public interface WorkAssignmentProtocol {

  /**
   * Get next workerId to which new task will be assigned.
   * @return return workerId text
   */
  Text getNewWorkerId();

  /**
   * Register Worker.
   * When worker will be launched first, it needs to be registered with Master.
   * @param workerId : Worker Id
   * @return Task instance
   */
  Task registerWorker(WorkerId workerId);

  /**
   * De Register worker.
   * When worker is de-registered, no new task will be assigned to this worker.
   * @param workerId : Worker identifier
   */
  void deRegisterWorker(WorkerId workerId);

  /**
   * Worker sends heartbeat to Master.
   * @param workerId : Worker Id
   * @param taskStatusList : TaskStatus list of all tasks assigned to worker.
   * @return Task instance
   */
  Task sendHeartbeat(WorkerId workerId, TaskStatus[] taskStatusList);

  /**
   * Add Task to the list.
   * @param task : Task object
   */
  void addTask(Task task);
}
