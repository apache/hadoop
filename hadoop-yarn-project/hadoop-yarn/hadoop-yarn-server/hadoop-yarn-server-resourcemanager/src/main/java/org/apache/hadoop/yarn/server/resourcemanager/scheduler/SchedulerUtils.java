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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Utilities shared by schedulers. 
 */
@Private
@Unstable
public class SchedulerUtils {
  
  private static final RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);

  public static final String RELEASED_CONTAINER = 
      "Container released by application";
  
  public static final String LOST_CONTAINER = 
      "Container released on a *lost* node";
  
  public static final String PREEMPTED_CONTAINER = 
      "Container preempted by scheduler";
  
  public static final String COMPLETED_APPLICATION = 
      "Container of a completed application";
  
  public static final String EXPIRED_CONTAINER =
      "Container expired since it was unused";
  
  public static final String UNRESERVED_CONTAINER =
      "Container reservation no longer required.";

  /**
   * Utility to create a {@link ContainerStatus} during exceptional
   * circumstances.
   *
   * @param containerId {@link ContainerId} of returned/released/lost container.
   * @param diagnostics diagnostic message
   * @return <code>ContainerStatus</code> for an returned/released/lost 
   *         container
   */
  public static ContainerStatus createAbnormalContainerStatus(
      ContainerId containerId, String diagnostics) {
    return createAbnormalContainerStatus(containerId, 
        ContainerExitStatus.ABORTED, diagnostics);
  }

  /**
   * Utility to create a {@link ContainerStatus} during exceptional
   * circumstances.
   *
   * @param containerId {@link ContainerId} of returned/released/lost container.
   * @param diagnostics diagnostic message
   * @return <code>ContainerStatus</code> for an returned/released/lost
   *         container
   */
  public static ContainerStatus createPreemptedContainerStatus(
      ContainerId containerId, String diagnostics) {
    return createAbnormalContainerStatus(containerId, 
        ContainerExitStatus.PREEMPTED, diagnostics);
  }

  /**
   * Utility to create a {@link ContainerStatus} during exceptional
   * circumstances.
   * 
   * @param containerId {@link ContainerId} of returned/released/lost container.
   * @param diagnostics diagnostic message
   * @return <code>ContainerStatus</code> for an returned/released/lost 
   *         container
   */
  private static ContainerStatus createAbnormalContainerStatus(
      ContainerId containerId, int exitStatus, String diagnostics) {
    ContainerStatus containerStatus = 
        recordFactory.newRecordInstance(ContainerStatus.class);
    containerStatus.setContainerId(containerId);
    containerStatus.setDiagnostics(diagnostics);
    containerStatus.setExitStatus(exitStatus);
    containerStatus.setState(ContainerState.COMPLETE);
    return containerStatus;
  }

  /**
   * Utility method to normalize a list of resource requests, by insuring that
   * the memory for each request is a multiple of minMemory and is not zero.
   */
  public static void normalizeRequests(
    List<ResourceRequest> asks,
    ResourceCalculator resourceCalculator,
    Resource clusterResource,
    Resource minimumResource,
    Resource maximumResource) {
    for (ResourceRequest ask : asks) {
      normalizeRequest(
        ask, resourceCalculator, clusterResource, minimumResource,
        maximumResource, minimumResource);
    }
  }

  /**
   * Utility method to normalize a resource request, by insuring that the
   * requested memory is a multiple of minMemory and is not zero.
   */
  public static void normalizeRequest(
    ResourceRequest ask,
    ResourceCalculator resourceCalculator,
    Resource clusterResource,
    Resource minimumResource,
    Resource maximumResource) {
    Resource normalized =
      Resources.normalize(
        resourceCalculator, ask.getCapability(), minimumResource,
        maximumResource, minimumResource);
    ask.setCapability(normalized);
  }
  
  /**
   * Utility method to normalize a list of resource requests, by insuring that
   * the memory for each request is a multiple of minMemory and is not zero.
   */
  public static void normalizeRequests(
      List<ResourceRequest> asks,
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource minimumResource,
      Resource maximumResource,
      Resource incrementResource) {
    for (ResourceRequest ask : asks) {
      normalizeRequest(
          ask, resourceCalculator, clusterResource, minimumResource,
          maximumResource, incrementResource);
    }
  }

  /**
   * Utility method to normalize a resource request, by insuring that the
   * requested memory is a multiple of minMemory and is not zero.
   */
  public static void normalizeRequest(
      ResourceRequest ask, 
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource minimumResource,
      Resource maximumResource,
      Resource incrementResource) {
    Resource normalized = 
        Resources.normalize(
            resourceCalculator, ask.getCapability(), minimumResource,
            maximumResource, incrementResource);
    ask.setCapability(normalized);
  }

  /**
   * Utility method to validate a resource request, by insuring that the
   * requested memory/vcore is non-negative and not greater than max
   * 
   * @throws <code>InvalidResourceRequestException</code> when there is invalid
   *         request
   */
  public static void validateResourceRequest(ResourceRequest resReq,
      Resource maximumResource) throws InvalidResourceRequestException {
    if (resReq.getCapability().getMemory() < 0 ||
        resReq.getCapability().getMemory() > maximumResource.getMemory()) {
      throw new InvalidResourceRequestException("Invalid resource request"
          + ", requested memory < 0"
          + ", or requested memory > max configured"
          + ", requestedMemory=" + resReq.getCapability().getMemory()
          + ", maxMemory=" + maximumResource.getMemory());
    }
    if (resReq.getCapability().getVirtualCores() < 0 ||
        resReq.getCapability().getVirtualCores() >
        maximumResource.getVirtualCores()) {
      throw new InvalidResourceRequestException("Invalid resource request"
          + ", requested virtual cores < 0"
          + ", or requested virtual cores > max configured"
          + ", requestedVirtualCores="
          + resReq.getCapability().getVirtualCores()
          + ", maxVirtualCores=" + maximumResource.getVirtualCores());
    }
  }
}
