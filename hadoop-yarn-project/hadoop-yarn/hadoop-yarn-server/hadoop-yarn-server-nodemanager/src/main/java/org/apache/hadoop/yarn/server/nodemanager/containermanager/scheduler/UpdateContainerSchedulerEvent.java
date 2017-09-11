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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container
    .Container;
/**
 * Update Event consumed by the {@link ContainerScheduler}.
 */
public class UpdateContainerSchedulerEvent extends ContainerSchedulerEvent {

  private ContainerTokenIdentifier updatedToken;
  private boolean isResourceChange;
  private boolean isExecTypeUpdate;
  private boolean isIncrease;

  /**
   * Create instance of Event.
   *
   * @param originalContainer Original Container.
   * @param updatedToken Updated Container Token.
   * @param isResourceChange is this a Resource Change.
   * @param isExecTypeUpdate is this an ExecTypeUpdate.
   * @param isIncrease is this a Container Increase.
   */
  public UpdateContainerSchedulerEvent(Container originalContainer,
      ContainerTokenIdentifier updatedToken, boolean isResourceChange,
      boolean isExecTypeUpdate, boolean isIncrease) {
    super(originalContainer, ContainerSchedulerEventType.UPDATE_CONTAINER);
    this.updatedToken = updatedToken;
    this.isResourceChange = isResourceChange;
    this.isExecTypeUpdate = isExecTypeUpdate;
    this.isIncrease = isIncrease;
  }

  /**
   * Update Container Token.
   *
   * @return Container Token.
   */
  public ContainerTokenIdentifier getUpdatedToken() {
    return updatedToken;
  }

  /**
   * isResourceChange.
   * @return isResourceChange.
   */
  public boolean isResourceChange() {
    return isResourceChange;
  }

  /**
   * isExecTypeUpdate.
   * @return isExecTypeUpdate.
   */
  public boolean isExecTypeUpdate() {
    return isExecTypeUpdate;
  }

  /**
   * isIncrease.
   * @return isIncrease.
   */
  public boolean isIncrease() {
    return isIncrease;
  }
}
