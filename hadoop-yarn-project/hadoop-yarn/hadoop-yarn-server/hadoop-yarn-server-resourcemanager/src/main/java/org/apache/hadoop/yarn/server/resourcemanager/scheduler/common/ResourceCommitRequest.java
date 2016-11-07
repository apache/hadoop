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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Collections;
import java.util.List;

public class ResourceCommitRequest<A extends SchedulerApplicationAttempt,
    N extends SchedulerNode> {
  // New containers to be allocated
  private List<ContainerAllocationProposal<A, N>> containersToAllocate =
      Collections.emptyList();

  // New containers to be released
  private List<ContainerAllocationProposal<A, N>> containersToReserve =
      Collections.emptyList();

  // We don't need these containers anymore
  private List<SchedulerContainer<A, N>> toReleaseContainers =
      Collections.emptyList();

  private Resource totalAllocatedResource;
  private Resource totalReservedResource;
  private Resource totalReleasedResource;

  public ResourceCommitRequest(
      List<ContainerAllocationProposal<A, N>> containersToAllocate,
      List<ContainerAllocationProposal<A, N>> containersToReserve,
      List<SchedulerContainer<A, N>> toReleaseContainers) {
    if (null != containersToAllocate) {
      this.containersToAllocate = containersToAllocate;
    }
    if (null != containersToReserve) {
      this.containersToReserve = containersToReserve;
    }
    if (null != toReleaseContainers) {
      this.toReleaseContainers = toReleaseContainers;
    }

    totalAllocatedResource = Resources.createResource(0);
    totalReservedResource = Resources.createResource(0);

    /*
     * For total-release resource, it has two parts:
     * 1) Unconditional release: for example, an app reserved a container,
     *    but the app doesn't has any pending resource.
     * 2) Conditional release: for example, reservation continuous looking, or
     *    Lazy preemption -- which we need to kill some resource to allocate
     *    or reserve the new container.
     *
     * For the 2nd part, it is inside:
     * ContainerAllocationProposal#toRelease, which means we will kill/release
     * these containers to allocate/reserve the given container.
     *
     * So we need to account both of conditional/unconditional to-release
     * containers to the total release-able resource.
     */
    totalReleasedResource = Resources.createResource(0);

    for (ContainerAllocationProposal<A,N> c : this.containersToAllocate) {
      Resources.addTo(totalAllocatedResource,
          c.getAllocatedOrReservedResource());
      for (SchedulerContainer<A,N> r : c.getToRelease()) {
        Resources.addTo(totalReleasedResource,
            r.getRmContainer().getAllocatedOrReservedResource());
      }
    }

    for (ContainerAllocationProposal<A,N> c : this.containersToReserve) {
      Resources.addTo(totalReservedResource,
          c.getAllocatedOrReservedResource());
      for (SchedulerContainer<A,N> r : c.getToRelease()) {
        Resources.addTo(totalReleasedResource,
            r.getRmContainer().getAllocatedOrReservedResource());
      }
    }

    for (SchedulerContainer<A,N> r : this.toReleaseContainers) {
      Resources.addTo(totalReleasedResource,
          r.getRmContainer().getAllocatedOrReservedResource());
    }
  }

  public List<ContainerAllocationProposal<A, N>> getContainersToAllocate() {
    return containersToAllocate;
  }

  public List<ContainerAllocationProposal<A, N>> getContainersToReserve() {
    return containersToReserve;
  }

  public List<SchedulerContainer<A, N>> getContainersToRelease() {
    return toReleaseContainers;
  }

  public Resource getTotalAllocatedResource() {
    return totalAllocatedResource;
  }

  public Resource getTotalReservedResource() {
    return totalReservedResource;
  }

  public Resource getTotalReleasedResource() {
    return totalReleasedResource;
  }

  /*
   * Util functions to make your life easier
   */
  public boolean anythingAllocatedOrReserved() {
    return (!containersToAllocate.isEmpty()) || (!containersToReserve
        .isEmpty());
  }

  public ContainerAllocationProposal<A, N> getFirstAllocatedOrReservedContainer() {
    ContainerAllocationProposal<A, N> c = null;
    if (!containersToAllocate.isEmpty()) {
      c = containersToAllocate.get(0);
    }
    if (c == null && !containersToReserve.isEmpty()) {
      c = containersToReserve.get(0);
    }

    return c;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("New " + getClass().getName() + ":" + "\n");
    if (null != containersToAllocate && !containersToAllocate.isEmpty()) {
      sb.append("\t ALLOCATED=" + containersToAllocate.toString());
    }
    if (null != containersToReserve && !containersToReserve.isEmpty()) {
      sb.append("\t RESERVED=" + containersToReserve.toString());
    }
    if (null != toReleaseContainers && !toReleaseContainers.isEmpty()) {
      sb.append("\t RELEASED=" + toReleaseContainers.toString());
    }
    return sb.toString();
  }
}