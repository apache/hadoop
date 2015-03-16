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

package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ContainerResourceIncreaseRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The core request sent by the <code>ApplicationMaster</code> to the 
 * <code>ResourceManager</code> to obtain resources in the cluster.</p> 
 *
 * <p>The request includes:
 * <ul>
 *   <li>A response id to track duplicate responses.</li>
 *   <li>Progress information.</li>
 *   <li>
 *     A list of {@link ResourceRequest} to inform the
 *     <code>ResourceManager</code> about the application's
 *     resource requirements.
 *   </li>
 *   <li>
 *     A list of unused {@link Container} which are being returned.
 *   </li>
 * </ul>
 * 
 * @see ApplicationMasterProtocol#allocate(AllocateRequest)
 */
@Public
@Stable
public abstract class AllocateRequest {

  @Public
  @Stable
  public static AllocateRequest newInstance(int responseID, float appProgress,
      List<ResourceRequest> resourceAsk,
      List<ContainerId> containersToBeReleased,
      ResourceBlacklistRequest resourceBlacklistRequest) {
    return newInstance(responseID, appProgress, resourceAsk,
        containersToBeReleased, resourceBlacklistRequest, null);
  }
  
  @Public
  @Stable
  public static AllocateRequest newInstance(int responseID, float appProgress,
      List<ResourceRequest> resourceAsk,
      List<ContainerId> containersToBeReleased,
      ResourceBlacklistRequest resourceBlacklistRequest,
      List<ContainerResourceIncreaseRequest> increaseRequests) {
    AllocateRequest allocateRequest = Records.newRecord(AllocateRequest.class);
    allocateRequest.setResponseId(responseID);
    allocateRequest.setProgress(appProgress);
    allocateRequest.setAskList(resourceAsk);
    allocateRequest.setReleaseList(containersToBeReleased);
    allocateRequest.setResourceBlacklistRequest(resourceBlacklistRequest);
    allocateRequest.setIncreaseRequests(increaseRequests);
    return allocateRequest;
  }
  
  /**
   * Get the <em>response id</em> used to track duplicate responses.
   * @return <em>response id</em>
   */
  @Public
  @Stable
  public abstract int getResponseId();

  /**
   * Set the <em>response id</em> used to track duplicate responses.
   * @param id <em>response id</em>
   */
  @Public
  @Stable
  public abstract void setResponseId(int id);

  /**
   * Get the <em>current progress</em> of application. 
   * @return <em>current progress</em> of application
   */
  @Public
  @Stable
  public abstract float getProgress();
  
  /**
   * Set the <em>current progress</em> of application
   * @param progress <em>current progress</em> of application
   */
  @Public
  @Stable
  public abstract void setProgress(float progress);

  /**
   * Get the list of <code>ResourceRequest</code> to update the 
   * <code>ResourceManager</code> about the application's resource requirements.
   * @return the list of <code>ResourceRequest</code>
   * @see ResourceRequest
   */
  @Public
  @Stable
  public abstract List<ResourceRequest> getAskList();
  
  /**
   * Set list of <code>ResourceRequest</code> to update the
   * <code>ResourceManager</code> about the application's resource requirements.
   * @param resourceRequests list of <code>ResourceRequest</code> to update the 
   *                        <code>ResourceManager</code> about the application's 
   *                        resource requirements
   * @see ResourceRequest
   */
  @Public
  @Stable
  public abstract void setAskList(List<ResourceRequest> resourceRequests);

  /**
   * Get the list of <code>ContainerId</code> of containers being 
   * released by the <code>ApplicationMaster</code>.
   * @return list of <code>ContainerId</code> of containers being 
   *         released by the <code>ApplicationMaster</code> 
   */
  @Public
  @Stable
  public abstract List<ContainerId> getReleaseList();

  /**
   * Set the list of <code>ContainerId</code> of containers being
   * released by the <code>ApplicationMaster</code>
   * @param releaseContainers list of <code>ContainerId</code> of 
   *                          containers being released by the 
   *                          <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public abstract void setReleaseList(List<ContainerId> releaseContainers);
  
  /**
   * Get the <code>ResourceBlacklistRequest</code> being sent by the 
   * <code>ApplicationMaster</code>.
   * @return the <code>ResourceBlacklistRequest</code> being sent by the 
   *         <code>ApplicationMaster</code>
   * @see ResourceBlacklistRequest
   */
  @Public
  @Stable
  public abstract ResourceBlacklistRequest getResourceBlacklistRequest();
  
  /**
   * Set the <code>ResourceBlacklistRequest</code> to inform the 
   * <code>ResourceManager</code> about the blacklist additions and removals
   * per the <code>ApplicationMaster</code>.
   * 
   * @param resourceBlacklistRequest the <code>ResourceBlacklistRequest</code>  
   *                         to inform the <code>ResourceManager</code> about  
   *                         the blacklist additions and removals
   *                         per the <code>ApplicationMaster</code>
   * @see ResourceBlacklistRequest
   */
  @Public
  @Stable
  public abstract void setResourceBlacklistRequest(
      ResourceBlacklistRequest resourceBlacklistRequest);
  
  /**
   * Get the <code>ContainerResourceIncreaseRequest</code> being sent by the
   * <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public abstract List<ContainerResourceIncreaseRequest> getIncreaseRequests();
  
  /**
   * Set the <code>ContainerResourceIncreaseRequest</code> to inform the
   * <code>ResourceManager</code> about some container's resources need to be
   * increased
   */
  @Public
  @Stable
  public abstract void setIncreaseRequests(
      List<ContainerResourceIncreaseRequest> increaseRequests);
}
