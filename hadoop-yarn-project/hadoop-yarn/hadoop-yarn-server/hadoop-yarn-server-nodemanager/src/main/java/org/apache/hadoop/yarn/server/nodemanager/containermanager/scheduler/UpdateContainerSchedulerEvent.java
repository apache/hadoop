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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.UpdateContainerTokenEvent;

/**
 * Update Event consumed by the {@link ContainerScheduler}.
 */
public class UpdateContainerSchedulerEvent extends ContainerSchedulerEvent {

  private final UpdateContainerTokenEvent containerEvent;
  private final ContainerTokenIdentifier originalToken;

  /**
   * Create instance of Event.
   *
   * @param container Container.
   * @param origToken The Original Container Token.
   * @param event The Container Event.
   */
  public UpdateContainerSchedulerEvent(Container container,
      ContainerTokenIdentifier origToken, UpdateContainerTokenEvent event) {
    super(container, ContainerSchedulerEventType.UPDATE_CONTAINER);
    this.containerEvent = event;
    this.originalToken = origToken;
  }

  /**
   * Original Token before update.
   *
   * @return Container Token.
   */
  public ContainerTokenIdentifier getOriginalToken() {
    return this.originalToken;
  }

  /**
   * Update Container Token.
   *
   * @return Container Token.
   */
  public ContainerTokenIdentifier getUpdatedToken() {
    return containerEvent.getUpdatedToken();
  }

  /**
   * isResourceChange.
   * @return isResourceChange.
   */
  public boolean isResourceChange() {
    return containerEvent.isResourceChange();
  }

  /**
   * isExecTypeUpdate.
   * @return isExecTypeUpdate.
   */
  public boolean isExecTypeUpdate() {
    return containerEvent.isExecTypeUpdate();
  }

  /**
   * isIncrease.
   * @return isIncrease.
   */
  public boolean isIncrease() {
    return containerEvent.isIncrease();
  }
}
