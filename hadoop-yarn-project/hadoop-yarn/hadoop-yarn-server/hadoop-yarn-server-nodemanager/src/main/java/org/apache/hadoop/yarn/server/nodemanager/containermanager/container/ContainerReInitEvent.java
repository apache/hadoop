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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceSet;

/**
 * ContainerEvent sent by ContainerManager to ContainerImpl to
 * re-initiate Container.
 */
public class ContainerReInitEvent extends ContainerEvent {

  private final ContainerLaunchContext reInitLaunchContext;
  private final ResourceSet resourceSet;
  private final boolean autoCommit;

  /**
   * Container Re-Init Event.
   * @param cID Container Id.
   * @param upgradeContext Upgrade Context.
   * @param resourceSet Resource Set.
   * @param autoCommit Auto Commit.
   */
  public ContainerReInitEvent(ContainerId cID,
      ContainerLaunchContext upgradeContext,
      ResourceSet resourceSet, boolean autoCommit){
    super(cID, ContainerEventType.REINITIALIZE_CONTAINER);
    this.reInitLaunchContext = upgradeContext;
    this.resourceSet = resourceSet;
    this.autoCommit = autoCommit;
  }

  /**
   * Get the Launch Context to be used for upgrade.
   * @return ContainerLaunchContext
   */
  public ContainerLaunchContext getReInitLaunchContext() {
    return reInitLaunchContext;
  }

  /**
   * Get the ResourceSet.
   * @return ResourceSet.
   */
  public ResourceSet getResourceSet() {
    return resourceSet;
  }

  /**
   * Should this re-Initialization be auto-committed.
   * @return AutoCommit.
   */
  public boolean isAutoCommit() {
    return autoCommit;
  }
}
