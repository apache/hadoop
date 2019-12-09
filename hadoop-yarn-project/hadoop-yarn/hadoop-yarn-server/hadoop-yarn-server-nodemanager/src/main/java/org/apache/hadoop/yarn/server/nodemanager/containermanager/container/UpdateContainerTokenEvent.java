/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;

/**
 * Update Event consumed by the Container.
 */
public class UpdateContainerTokenEvent extends ContainerEvent {
  private final ContainerTokenIdentifier updatedToken;
  private final boolean isResourceChange;
  private final boolean isExecTypeUpdate;
  private final boolean isIncrease;

  /**
   * Create Update event.
   *
   * @param cID Container Id.
   * @param updatedToken Updated Container Token.
   * @param isResourceChange Is Resource change.
   * @param isExecTypeUpdate Is ExecutionType Update.
   * @param isIncrease Is container increase.
   */
  public UpdateContainerTokenEvent(ContainerId cID,
      ContainerTokenIdentifier updatedToken, boolean isResourceChange,
      boolean isExecTypeUpdate, boolean isIncrease) {
    super(cID, ContainerEventType.UPDATE_CONTAINER_TOKEN);
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
   * Is this update a ResourceChange.
   *
   * @return isResourceChange.
   */
  public boolean isResourceChange() {
    return isResourceChange;
  }

  /**
   * Is this update an ExecType Update.
   *
   * @return isExecTypeUpdate.
   */
  public boolean isExecTypeUpdate() {
    return isExecTypeUpdate;
  }

  /**
   * Is this a container Increase.
   *
   * @return isIncrease.
   */
  public boolean isIncrease() {
    return isIncrease;
  }
}
