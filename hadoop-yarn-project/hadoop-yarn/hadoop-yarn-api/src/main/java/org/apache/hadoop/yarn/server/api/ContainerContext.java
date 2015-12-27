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

package org.apache.hadoop.yarn.server.api;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * Base context class for {@link AuxiliaryService} initializing and stopping a
 * container.
 */
@Public
@Evolving
public class ContainerContext {
  private final String user;
  private final ContainerId containerId;
  private final Resource resource;
  private final ContainerType containerType;
  private final ExecutionType executionType;

  @Private
  @Unstable
  public ContainerContext(String user, ContainerId containerId,
      Resource resource) {
    this(user, containerId, resource, ContainerType.TASK);
  }

  @Private
  @Unstable
  public ContainerContext(String user, ContainerId containerId,
      Resource resource, ContainerType containerType) {
    this(user, containerId, resource, containerType,
        ExecutionType.GUARANTEED);
  }

  @Private
  @Unstable
  public ContainerContext(String user, ContainerId containerId,
      Resource resource, ContainerType containerType,
      ExecutionType executionType) {
    this.user = user;
    this.containerId = containerId;
    this.resource = resource;
    this.containerType = containerType;
    this.executionType = executionType;
  }

  /**
   * Get user of the container being initialized or stopped.
   *
   * @return the user
   */
  public String getUser() {
    return user;
  }

  /**
   * Get {@link ContainerId} of the container being initialized or stopped.
   *
   * @return the container ID
   */
  public ContainerId getContainerId() {
    return containerId;
  }

  /**
   * Get {@link Resource} the resource capability allocated to the container
   * being initialized or stopped.
   *
   * @return the resource capability.
   */
  public Resource getResource() {
    return resource;
  }

  /**
   * Get {@link ContainerType} the type of the container
   * being initialized or stopped.
   *
   * @return the type of the container
   */
  public ContainerType getContainerType() {
    return containerType;
  }

  /**
   * Get {@link ExecutionType} the execution type of the container
   * being initialized or stopped.
   *
   * @return the execution type of the container
   */
  public ExecutionType getExecutionType() {
    return executionType;
  }
}
