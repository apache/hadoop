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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * Represents an Application from the viewpoint of the scheduler.
 * Each running Application in the RM corresponds to one instance
 * of this class.
 */
@Private
@Unstable
public abstract class SchedulerApplication {

  /**
   * Get {@link ApplicationAttemptId} of the application master.
   * @return <code>ApplicationAttemptId</code> of the application master
   */
  public abstract ApplicationAttemptId getApplicationAttemptId();
  
  /**
   * Get the live containers of the application.
   * @return live containers of the application
   */
  public abstract Collection<RMContainer> getLiveContainers();
  
  /**
   * Get the reserved containers of the application.
   * @return the reserved containers of the application
   */
  public abstract Collection<RMContainer> getReservedContainers();
  
  /**
   * Is this application pending?
   * @return true if it is else false.
   */
  public abstract boolean isPending();

}
