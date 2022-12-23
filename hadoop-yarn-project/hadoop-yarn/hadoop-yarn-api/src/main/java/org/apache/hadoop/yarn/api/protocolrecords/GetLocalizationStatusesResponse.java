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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalizationStatus;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;
import java.util.Map;


/**
 * The response sent by the node manager to an application master when
 * localization statuses are requested.
 *
 * @see ContainerManagementProtocol#getLocalizationStatuses(
 *        GetLocalizationStatusesRequest)
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class GetLocalizationStatusesResponse {

  public static GetLocalizationStatusesResponse newInstance(
      Map<ContainerId, List<LocalizationStatus>> statuses,
      Map<ContainerId, SerializedException> failedRequests) {
    GetLocalizationStatusesResponse response =
        Records.newRecord(GetLocalizationStatusesResponse.class);
    response.setLocalizationStatuses(statuses);
    return response;
  }

  /**
   * Get all the container localization statuses.
   *
   * @return container localization statuses.
   */
  public abstract Map<ContainerId,
      List<LocalizationStatus>> getLocalizationStatuses();

  /**
   * Sets the container localization statuses.
   *
   * @param statuses container localization statuses.
   */
  @InterfaceAudience.Private
  public abstract void setLocalizationStatuses(
      Map<ContainerId, List<LocalizationStatus>> statuses);


  /**
   * Get the containerId-to-exception map in which the exception indicates error
   * from per container for failed requests.
   *
   * @return map of containerId-to-exception
   */
  @InterfaceAudience.Private
  public abstract Map<ContainerId, SerializedException> getFailedRequests();

  /**
   * Set the containerId-to-exception map in which the exception indicates error
   * from per container for failed request.
   */
  @InterfaceAudience.Private
  public abstract void setFailedRequests(
      Map<ContainerId, SerializedException> failedContainers);
}
