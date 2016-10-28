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
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The request sent by one <code>NodeManager</code> to another
 * <code>NodeManager</code> in order to get the launch context of the container with id
 * <em>containerId</em>.</p>
 *
 * <p>This request is used only for container relocation, where the launch context of
 * the origin container is necessary for launching the relocated container on the target node.</p>
 *
 * @see ContainerManagementProtocol#getContainerLaunchContext(GetContainerLaunchContextRequest)
 */
public abstract class GetContainerLaunchContextRequest {
  
  @Public
  @Stable
  public static GetContainerLaunchContextRequest newInstance(ContainerId containerId) {
    GetContainerLaunchContextRequest request =
        Records.newRecord(GetContainerLaunchContextRequest.class);
    request.setContainerId(containerId);
    return request;
  }
  
  /**
   * Gets the container id for which the launch context is requested.
   * @return the container id for which the launch context is requested
   */
  @Public
  @Stable
  public abstract ContainerId getContainerId();
  
  /**
   * Sets the container id for which the launch context is requested.
   * @param containerId the container id for which the launch context is requested
   */
  @Public
  @Stable
  public abstract void setContainerId(ContainerId containerId);
}