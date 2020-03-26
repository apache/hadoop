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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;


/**
 * A SchedulableEntity is a process to be scheduled.
 * for example, an application / application attempt
 */
public interface SchedulableEntity {
  
  /**
   * Id - each entity must have a unique id
   */
  public String getId();
  
  /**
   * Compare the passed SchedulableEntity to this one for input order.
   * Input order is implementation defined and should reflect the 
   * correct ordering for first-in first-out processing
   */
  public int compareInputOrderTo(SchedulableEntity other);
  
  /**
   * View of Resources wanted and consumed by the entity
   */
  public ResourceUsage getSchedulingResourceUsage();
  
  /**
   * Get the priority of the application
   */
  public Priority getPriority();

  /**
   * Whether application was running before RM restart.
   */
  public boolean isRecovering();

  /**
   * Get partition corresponding to this entity.
   * @return partition
   */
  String getPartition();

  /**
   * Start time of the job.
   * @return start time
   */
  long getStartTime();
}
