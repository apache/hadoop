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
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The response for a container launch context request sent between
 * <code>NodeManager</code>s. It contains the requested container launch context.</p>
 *
 * <p>This request is used only for container relocation, where a launch context
 * of the origin container is necessary for launching the relocated container on
 * the target node.</p>
 */
public abstract class GetContainerLaunchContextResponse {
  
  @Public
  @Stable
  public static GetContainerLaunchContextResponse newInstance(ContainerLaunchContext
      containerLaunchContext) {
    GetContainerLaunchContextResponse request =
        Records.newRecord(GetContainerLaunchContextResponse.class);
    request.setContainerLaunchContext(containerLaunchContext);
    return request;
  }
  
  /**
   * Gets the launch context of the requested container.
   * @return the launch context of the requested container
   */
  @Public
  @Stable
  public abstract ContainerLaunchContext getContainerLaunchContext();
  
  /**
   * Sets the launch context of the requested container.
   * @param containerLaunchContext the launch context of the requested container
   */
  @Public
  @Stable
  public abstract void setContainerLaunchContext(ContainerLaunchContext containerLaunchContext);
}