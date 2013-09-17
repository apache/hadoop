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
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * The request which contains a list of {@link StartContainerRequest} sent by
 * the <code>ApplicationMaster</code> to the <code>NodeManager</code> to
 * <em>start</em> containers.
 * </p>
 * 
 * <p>
 * In each {@link StartContainerRequest}, the <code>ApplicationMaster</code> has
 * to provide details such as allocated resource capability, security tokens (if
 * enabled), command to be executed to start the container, environment for the
 * process, necessary binaries/jar/shared-objects etc. via the
 * {@link ContainerLaunchContext}.
 * </p>
 * 
 * @see ContainerManagementProtocol#startContainers(StartContainersRequest)
 */
@Public
@Stable
public abstract class StartContainersRequest {

  @Public
  @Stable
  public static StartContainersRequest newInstance(
      List<StartContainerRequest> requests) {
    StartContainersRequest request =
        Records.newRecord(StartContainersRequest.class);
    request.setStartContainerRequests(requests);
    return request;
  }

  /**
   * Get a list of {@link StartContainerRequest} to start containers.
   * @return a list of {@link StartContainerRequest} to start containers.
   */
  @Public
  @Stable
  public abstract List<StartContainerRequest> getStartContainerRequests();

  /**
   * Set a list of {@link StartContainerRequest} to start containers.
   * @param request a list of {@link StartContainerRequest} to start containers
   */
  @Public
  @Stable
  public abstract void setStartContainerRequests(
      List<StartContainerRequest> request);
}
