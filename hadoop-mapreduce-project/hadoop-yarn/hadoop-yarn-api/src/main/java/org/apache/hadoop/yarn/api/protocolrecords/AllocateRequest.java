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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

/**
 * <p>The core request sent by the <code>ApplicationMaster</code> to the 
 * <code>ResourceManager</code> to obtain resources in the cluster via
 * {@link AMRMProtocol#allocate(AllocateRequest)}.</p> 
 *
 * <p>The request includes:
 *   <ul>
 *     <li>
 *         {@link ApplicationAttemptId} being managed by the 
 *         <code>ApplicationMaster</code>
 *     </li>
 *     <li>A response id to track duplicate responses.</li>
 *     <li>Progress information.</li>
 *     <li>
 *       A list of {@link ResourceRequest} to inform the 
 *       <code>ResourceManager</code> about the application's 
 *       resource requirements.
 *     </li>
 *     <li>
 *       A list of unused {@link Container} which are being returned. 
 *     </li>
 *     <li></li>
 *   </ul>
 * </p>
 * 
 */
@Public
@Stable
public interface AllocateRequest {

  /**
   * Get the {@link ApplicationAttemptId} being managed by the 
   * <code>ApplicationMaster</code>.
   * @return <code>ApplicationAttemptId</code> being managed by the 
   *         <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  ApplicationAttemptId getApplicationAttemptId();
  
  @Private
  @Unstable
  void setApplicationAttemptId(ApplicationAttemptId applicationAttemptId);

  /**
   * Get the response id.
   * @return the response id
   */
  @Public
  @Stable
  int getResponseId();
  
  @Private
  @Unstable
  void setResponseId(int id);

  /**
   * Get the current progress of application. 
   * @return the current progress of application
   */
  @Public
  @Stable
  float getProgress();
  
  @Private
  @Unstable
  void setProgress(float progress);

  /**
   * Get the list of <code>ResourceRequest</code> to upate the 
   * <code>ResourceManager</code> about the application's resource requirements.
   * @return the list of <code>ResourceRequest</code>
   */
  @Public
  @Stable
  List<ResourceRequest> getAskList();
  
  @Private
  @Unstable
  ResourceRequest getAsk(int index);
  
  @Private
  @Unstable
  int getAskCount();
  
  /**
   * Get the list of <code>ContainerId</code> of unused containers being 
   * released by the <code>ApplicationMaster</code>.
   * @return list of <code>ContainerId</code> of unused containers being 
   *         released by the <code>ApplicationMaster</code> 
   */
  @Public
  @Stable
  List<ContainerId> getReleaseList();
  
  @Private
  @Unstable
  ContainerId getRelease(int index);
  
  @Private
  @Unstable
  int getReleaseCount();

  
  @Private
  @Unstable
  void addAllAsks(List<ResourceRequest> resourceRequest);
  
  @Private
  @Unstable
  void addAsk(ResourceRequest request);
  
  @Private
  @Unstable
  void removeAsk(int index);
  
  @Private
  @Unstable
  void clearAsks();
  
  
  @Private
  @Unstable
  void addAllReleases(List<ContainerId> releaseContainers);
  
  @Private
  @Unstable
  void addRelease(ContainerId container);
  
  @Private
  @Unstable
  void removeRelease(int index);
  
  @Private
  @Unstable
  void clearReleases();
}
