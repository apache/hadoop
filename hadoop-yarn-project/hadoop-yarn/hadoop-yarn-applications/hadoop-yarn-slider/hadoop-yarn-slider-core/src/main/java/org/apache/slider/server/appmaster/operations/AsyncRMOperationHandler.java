/*
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

package org.apache.slider.server.appmaster.operations;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * Hands off RM operations to the Resource Manager.
 */
public class AsyncRMOperationHandler extends RMOperationHandler {
  protected static final Logger log =
    LoggerFactory.getLogger(AsyncRMOperationHandler.class);
  private final AMRMClientAsync client;
  private final Resource maxResources;

  public AsyncRMOperationHandler(AMRMClientAsync client, Resource maxResources) {
    this.client = client;
    this.maxResources = maxResources;
  }

  @Override
  public int cancelContainerRequests(Priority priority1,
      Priority priority2,
      int count) {
    // need to revoke a previously issued container request
    // so enum the sets and pick some
    int remaining = cancelSinglePriorityRequests(priority1, count);
    if (priority2 != null) {
      remaining = cancelSinglePriorityRequests(priority2, remaining);
    }

    return remaining;
  }

  /**
   * Cancel just one of the priority levels
   * @param priority priority to cancel
   * @param count count to cancel
   * @return number of requests cancelled
   */
  @SuppressWarnings("unchecked")
  protected int cancelSinglePriorityRequests(Priority priority,
      int count) {
    List<Collection<AMRMClient.ContainerRequest>> requestSets =
        client.getMatchingRequests(priority, "", maxResources);
    if (count <= 0) {
      return 0;
    }
    int remaining = count;
    for (Collection<AMRMClient.ContainerRequest> requestSet : requestSets) {
      if (remaining == 0) {
        break;
      }
      for (AMRMClient.ContainerRequest request : requestSet) {
        if (remaining == 0) {
          break;
        }
        // a single release
        cancelSingleRequest(request);
        remaining --;
      }
    }
    return remaining;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void cancelSingleRequest(AMRMClient.ContainerRequest request) {
    // a single release
    client.removeContainerRequest(request);
  }

  @Override
  public void releaseAssignedContainer(ContainerId containerId) {
    log.debug("Releasing container {}", containerId);

    client.releaseAssignedContainer(containerId);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void addContainerRequest(AMRMClient.ContainerRequest req) {
    client.addContainerRequest(req);
  }
}
