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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * Context class for {@link ContainerLogAggregationPolicy}.
 */
@Public
@Unstable
public class ContainerLogContext {
  private final ContainerId containerId;
  private final ContainerType containerType;
  private int exitCode;

  @Public
  @Unstable
  public ContainerLogContext(ContainerId containerId,
      ContainerType containerType, int exitCode) {
    this.containerId = containerId;
    this.containerType = containerType;
    this.exitCode = exitCode;
  }

  /**
   * Get {@link ContainerId} of the container.
   *
   * @return the container ID
   */
  public ContainerId getContainerId() {
    return containerId;
  }

  /**
   * Get {@link ContainerType} the type of the container.
   *
   * @return the type of the container
   */
  public ContainerType getContainerType() {
    return containerType;
  }

  /**
   * Get the exit code of the container.
   *
   * @return the exit code
   */
  public int getExitCode() {
    return exitCode;
  }

}
