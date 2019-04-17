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
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalizationStatus;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * The request sent by an application master to the node manager to get
 * {@link LocalizationStatus}es of containers.
 *
 * @see ContainerManagementProtocol#getLocalizationStatuses(
 *        GetLocalizationStatusesRequest)
 */
@Public
@Unstable
public abstract class GetLocalizationStatusesRequest {

  @Public
  @Unstable
  public static GetLocalizationStatusesRequest newInstance(
      List<ContainerId> containerIds) {
    GetLocalizationStatusesRequest request =
        Records.newRecord(GetLocalizationStatusesRequest.class);
    request.setContainerIds(containerIds);
    return request;
  }

  /**
   * Get the list of container IDs of the containers for which the localization
   * statuses are needed.
   *
   * @return the list of container IDs.
   */
  @Public
  @Unstable
  public abstract List<ContainerId> getContainerIds();

  /**
   * Sets the list of container IDs of containers for which the localization
   * statuses are needed.
   * @param containerIds the list of container IDs.
   */
  @Public
  @Unstable
  public abstract void setContainerIds(List<ContainerId> containerIds);
}
