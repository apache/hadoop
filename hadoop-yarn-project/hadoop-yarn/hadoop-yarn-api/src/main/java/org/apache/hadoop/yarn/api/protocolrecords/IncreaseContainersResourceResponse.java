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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * The response sent by the <code>NodeManager</code> to the
 * <code>ApplicationMaster</code> when asked to increase container resource.
 * </p>
 *
 * @see ContainerManagementProtocol#increaseContainersResource(IncreaseContainersResourceRequest)
 */
@Public
@Unstable
public abstract class IncreaseContainersResourceResponse {

  @Private
  @Unstable
  public static IncreaseContainersResourceResponse newInstance(
      List<ContainerId> successfullyIncreasedContainers,
      Map<ContainerId, SerializedException> failedRequests) {
    IncreaseContainersResourceResponse response =
        Records.newRecord(IncreaseContainersResourceResponse.class);
    response.setSuccessfullyIncreasedContainers(
        successfullyIncreasedContainers);
    response.setFailedRequests(failedRequests);
    return response;
  }

  /**
   * Get the list of containerIds of containers whose resource
   * have been successfully increased.
   *
   * @return the list of containerIds of containers whose resource have
   * been successfully increased.
   */
  @Public
  @Unstable
  public abstract List<ContainerId> getSuccessfullyIncreasedContainers();

  /**
   * Set the list of containerIds of containers whose resource have
   * been successfully increased.
   */
  @Private
  @Unstable
  public abstract void setSuccessfullyIncreasedContainers(
      List<ContainerId> succeedIncreasedContainers);

  /**
   * Get the containerId-to-exception map in which the exception indicates
   * error from each container for failed requests.
   * @return map of containerId-to-exception
   */
  @Public
  @Unstable
  public abstract Map<ContainerId, SerializedException> getFailedRequests();

  /**
   * Set the containerId-to-exception map in which the exception indicates
   * error from each container for failed requests.
   */
  @Private
  @Unstable
  public abstract void setFailedRequests(
      Map<ContainerId, SerializedException> failedRequests);
}
