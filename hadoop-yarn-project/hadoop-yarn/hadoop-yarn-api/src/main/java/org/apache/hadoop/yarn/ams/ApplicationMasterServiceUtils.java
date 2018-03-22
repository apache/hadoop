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

package org.apache.hadoop.yarn.ams;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.RejectedSchedulingRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods to be used by {@link ApplicationMasterServiceProcessor}.
 */
public final class ApplicationMasterServiceUtils {

  private ApplicationMasterServiceUtils() { }

  /**
   * Add update container errors to {@link AllocateResponse}.
   * @param allocateResponse Allocate Response.
   * @param updateContainerErrors Errors.
   */
  public static void addToUpdateContainerErrors(
      AllocateResponse allocateResponse,
      List<UpdateContainerError> updateContainerErrors) {
    if (!updateContainerErrors.isEmpty()) {
      if (allocateResponse.getUpdateErrors() != null
          && !allocateResponse.getUpdateErrors().isEmpty()) {
        updateContainerErrors.addAll(allocateResponse.getUpdateErrors());
      }
      allocateResponse.setUpdateErrors(updateContainerErrors);
    }
  }

  /**
   * Add updated containers to {@link AllocateResponse}.
   * @param allocateResponse Allocate Response.
   * @param updateType Update Type.
   * @param updatedContainers Updated Containers.
   */
  public static void addToUpdatedContainers(AllocateResponse allocateResponse,
      ContainerUpdateType updateType, List<Container> updatedContainers) {
    if (updatedContainers != null && updatedContainers.size() > 0) {
      ArrayList<UpdatedContainer> containersToSet = new ArrayList<>();
      if (allocateResponse.getUpdatedContainers() != null &&
          !allocateResponse.getUpdatedContainers().isEmpty()) {
        containersToSet.addAll(allocateResponse.getUpdatedContainers());
      }
      for (Container updatedContainer : updatedContainers) {
        containersToSet.add(
            UpdatedContainer.newInstance(updateType, updatedContainer));
      }
      allocateResponse.setUpdatedContainers(containersToSet);
    }
  }

  /**
   * Add allocated containers to {@link AllocateResponse}.
   * @param allocateResponse Allocate Response.
   * @param allocatedContainers Allocated Containers.
   */
  public static void addToAllocatedContainers(AllocateResponse allocateResponse,
      List<Container> allocatedContainers) {
    if (allocateResponse.getAllocatedContainers() != null
        && !allocateResponse.getAllocatedContainers().isEmpty()) {
      allocatedContainers.addAll(allocateResponse.getAllocatedContainers());
    }
    allocateResponse.setAllocatedContainers(allocatedContainers);
  }

  /**
   * Add rejected Scheduling Requests to {@link AllocateResponse}.
   * @param allocateResponse Allocate Response.
   * @param rejectedRequests Rejected SchedulingRequests.
   */
  public static void addToRejectedSchedulingRequests(
      AllocateResponse allocateResponse,
      List<RejectedSchedulingRequest> rejectedRequests) {
    if (allocateResponse.getRejectedSchedulingRequests() != null
        && !allocateResponse.getRejectedSchedulingRequests().isEmpty()) {
      rejectedRequests.addAll(allocateResponse.getRejectedSchedulingRequests());
    }
    allocateResponse.setRejectedSchedulingRequests(rejectedRequests);
  }
}
