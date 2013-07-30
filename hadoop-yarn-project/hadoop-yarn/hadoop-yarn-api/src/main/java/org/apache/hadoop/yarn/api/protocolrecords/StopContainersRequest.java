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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The request sent by the <code>ApplicationMaster</code> to the
 * <code>NodeManager</code> to <em>stop</em> containers.</p>
 * 
 * @see ContainerManagementProtocol#stopContainers(StopContainersRequest)
 */
@Public
@Stable
public abstract class StopContainersRequest {

  @Public
  @Stable
  public static StopContainersRequest newInstance(List<ContainerId> containerIds) {
    StopContainersRequest request =
        Records.newRecord(StopContainersRequest.class);
    request.setContainerIds(containerIds);
    return request;
  }

  /**
   * Get the <code>ContainerId</code>s of the containers to be stopped.
   * @return <code>ContainerId</code>s of containers to be stopped
   */
  @Public
  @Stable
  public abstract List<ContainerId> getContainerIds();
  
  /**
   * Set the <code>ContainerId</code>s of the containers to be stopped.
   * @param containerIds <code>ContainerId</code>s of the containers to be stopped
   */
  @Public
  @Stable
  public abstract void setContainerIds(List<ContainerId> containerIds);
}
