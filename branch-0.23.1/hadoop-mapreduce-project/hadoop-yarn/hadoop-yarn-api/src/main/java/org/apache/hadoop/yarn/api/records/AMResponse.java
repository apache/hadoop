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

package org.apache.hadoop.yarn.api.records;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;

/**
 * <p>The response sent by the <code>ResourceManager</code> the  
 * <code>ApplicationMaster</code> during resource negotiation.</p>
 *
 * <p>The response includes:
 *   <ul>
 *     <li>Response ID to track duplicate responses.</li>
 *     <li>
 *       A reboot flag to let the <code>ApplicationMaster</code> know that its 
 *       horribly out of sync and needs to reboot.</li>
 *     <li>A list of newly allocated {@link Container}.</li>
 *     <li>A list of completed {@link Container}.</li>
 *     <li>
 *       The available headroom for resources in the cluster for the
 *       application. 
 *     </li>
 *   </ul>
 * </p>
 * 
 * @see AMRMProtocol#allocate(AllocateRequest)
 */
@Public
@Unstable
public interface AMResponse {
  /**
   * Should the <code>ApplicationMaster</code> reboot for being horribly 
   * out-of-sync with the <code>ResourceManager</code> as deigned by 
   * {@link #getResponseId()}?
   * 
   * @return <code>true</code> if the <code>ApplicationMaster</code> should
   *         reboot, <code>false</code> otherwise
   */
  @Public
  @Stable
  public boolean getReboot();
  
  @Private
  @Unstable
  public void setReboot(boolean reboot);

  /**
   * Get the <em>last response id</em>.
   * @return <em>last response id</em>
   */
  @Public
  @Stable
  public int getResponseId();
  
  @Private
  @Unstable
  public void setResponseId(int responseId);

  /**
   * Get the list of <em>newly allocated</em> <code>Container</code> by the 
   * <code>ResourceManager</code>.
   * @return list of <em>newly allocated</em> <code>Container</code>
   */
  @Public
  @Stable
  public List<Container> getAllocatedContainers();

  /**
   * Set the list of <em>newly allocated</em> <code>Container</code> by the 
   * <code>ResourceManager</code>.
   * @param containers list of <em>newly allocated</em> <code>Container</code>
   */
  @Public
  @Stable
  public void setAllocatedContainers(List<Container> containers);

  /**
   * Get the <em>available headroom</em> for resources in the cluster for the 
   * application.
   * @return limit of available headroom for resources in the cluster for the 
   * application
   */
  @Public
  @Stable
  public Resource getAvailableResources();

  @Private
  @Unstable
  public void setAvailableResources(Resource limit);
  
  /**
   * Get the list of <em>completed containers' statuses</em>.
   * @return the list of <em>completed containers' statuses</em>
   */
  @Public
  @Stable
  public List<ContainerStatus> getCompletedContainersStatuses();

  @Private
  @Unstable
  public void setCompletedContainersStatuses(List<ContainerStatus> containers);
}