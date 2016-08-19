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

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

/**
 * Composite key for outstanding scheduler requests for any schedulable entity.
 * Currently it includes {@link Priority}.
 */
public final class SchedulerRequestKey implements
    Comparable<SchedulerRequestKey> {

  private final Priority priority;
  private final long allocationRequestId;

  /**
   * Factory method to generate a SchedulerRequestKey from a ResourceRequest.
   * @param req ResourceRequest
   * @return SchedulerRequestKey
   */
  public static SchedulerRequestKey create(ResourceRequest req) {
    return new SchedulerRequestKey(req.getPriority(),
        req.getAllocationRequestId());
  }

  /**
   * Convenience method to extract the SchedulerRequestKey used to schedule the
   * Container.
   * @param container Container
   * @return SchedulerRequestKey
   */
  public static SchedulerRequestKey extractFrom(Container container) {
    return new SchedulerRequestKey(container.getPriority(),
        container.getAllocationRequestId());
  }

  private SchedulerRequestKey(Priority priority, long allocationRequestId) {
    this.priority = priority;
    this.allocationRequestId = allocationRequestId;
  }

  /**
   * Get the {@link Priority} of the request.
   *
   * @return the {@link Priority} of the request
   */
  public Priority getPriority() {
    return priority;
  }

  /**
   * Get the Id of the associated {@link ResourceRequest}.
   *
   * @return the Id of the associated {@link ResourceRequest}
   */
  public long getAllocationRequestId() {
    return allocationRequestId;
  }

  @Override
  public int compareTo(SchedulerRequestKey o) {
    if (o == null) {
      return (priority != null) ? -1 : 0;
    } else {
      if (priority == null) {
        return 1;
      }
    }
    int priorityCompare = o.getPriority().compareTo(priority);
    // we first sort by priority and then by allocationRequestId
    if (priorityCompare != 0) {
      return priorityCompare;
    }
    return Long.compare(allocationRequestId, o.getAllocationRequestId());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SchedulerRequestKey)) {
      return false;
    }

    SchedulerRequestKey that = (SchedulerRequestKey) o;

    if (getAllocationRequestId() != that.getAllocationRequestId()) {
      return false;
    }
    return getPriority() != null ?
        getPriority().equals(that.getPriority()) :
        that.getPriority() == null;
  }

  @Override
  public int hashCode() {
    int result = getPriority() != null ? getPriority().hashCode() : 0;
    result = 31 * result + (int) (getAllocationRequestId() ^ (
        getAllocationRequestId() >>> 32));
    return result;
  }
}
