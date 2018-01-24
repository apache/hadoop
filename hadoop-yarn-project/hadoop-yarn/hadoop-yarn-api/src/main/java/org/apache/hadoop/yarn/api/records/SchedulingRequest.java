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

package org.apache.hadoop.yarn.api.records;

import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@code SchedulingRequest} represents a request made by an application to the
 * {@code ResourceManager} to obtain an allocation. It is similar to the
 * {@link ResourceRequest}. However, it is more complete than the latter, as it
 * allows applications to specify allocation tags (e.g., to express that an
 * allocation belongs to {@code Spark} or is an {@code HBase-master}), as well
 * as involved {@link PlacementConstraint}s (e.g., anti-affinity between Spark
 * and HBase allocations).
 *
 * The size specification of the allocation is in {@code ResourceSizing}.
 */
@Public
@Unstable
public abstract class SchedulingRequest {

  @Public
  @Unstable
  public static SchedulingRequest newInstance(long allocationRequestId,
      Priority priority, ExecutionTypeRequest executionType,
      Set<String> allocationTags, ResourceSizing resourceSizing,
      PlacementConstraint placementConstraintExpression) {
    return SchedulingRequest.newBuilder()
        .allocationRequestId(allocationRequestId).priority(priority)
        .executionType(executionType).allocationTags(allocationTags)
        .resourceSizing(resourceSizing)
        .placementConstraintExpression(placementConstraintExpression).build();
  }

  @Public
  @Unstable
  public static SchedulingRequestBuilder newBuilder() {
    return new SchedulingRequestBuilder();
  }

  /**
   * Class to construct instances of {@link SchedulingRequest} with specific
   * options.
   */
  @Public
  @Unstable
  public static final class SchedulingRequestBuilder {
    private SchedulingRequest schedulingRequest =
            Records.newRecord(SchedulingRequest.class);

    private SchedulingRequestBuilder() {
      schedulingRequest.setAllocationRequestId(0);
      schedulingRequest.setPriority(Priority.newInstance(0));
      schedulingRequest.setExecutionType(ExecutionTypeRequest.newInstance());
    }

    /**
     * Set the <code>allocationRequestId</code> of the request.
     *
     * @see SchedulingRequest#setAllocationRequestId(long)
     * @param allocationRequestId <code>allocationRequestId</code> of the
     *          request
     * @return {@link SchedulingRequest.SchedulingRequestBuilder}
     */
    @Public
    @Unstable
    public SchedulingRequestBuilder allocationRequestId(
            long allocationRequestId) {
      schedulingRequest.setAllocationRequestId(allocationRequestId);
      return this;
    }

    /**
     * Set the <code>priority</code> of the request.
     *
     * @param priority <code>priority</code> of the request
     * @return {@link SchedulingRequest.SchedulingRequestBuilder}
     * @see SchedulingRequest#setPriority(Priority)
     */
    @Public
    @Unstable
    public SchedulingRequestBuilder priority(Priority priority) {
      schedulingRequest.setPriority(priority);
      return this;
    }

    /**
     * Set the <code>executionType</code> of the request.
     *
     * @see SchedulingRequest#setExecutionType(ExecutionTypeRequest)
     * @param executionType <code>executionType</code> of the request
     * @return {@link SchedulingRequest.SchedulingRequestBuilder}
     */
    @Public
    @Unstable
    public SchedulingRequestBuilder executionType(
        ExecutionTypeRequest executionType) {
      schedulingRequest.setExecutionType(executionType);
      return this;
    }

    /**
     * Set the <code>allocationTags</code> of the request.
     *
     * @see SchedulingRequest#setAllocationTags(Set)
     * @param allocationTags <code>allocationsTags</code> of the request
     * @return {@link SchedulingRequest.SchedulingRequestBuilder}
     */
    @Public
    @Unstable
    public SchedulingRequestBuilder allocationTags(Set<String> allocationTags) {
      schedulingRequest.setAllocationTags(allocationTags);
      return this;
    }

    /**
     * Set the <code>executionType</code> of the request.
     *
     * @see SchedulingRequest#setResourceSizing(ResourceSizing)
     * @param resourceSizing <code>resourceSizing</code> of the request
     * @return {@link SchedulingRequest.SchedulingRequestBuilder}
     */
    @Public
    @Unstable
    public SchedulingRequestBuilder resourceSizing(
        ResourceSizing resourceSizing) {
      schedulingRequest.setResourceSizing(resourceSizing);
      return this;
    }

    /**
     * Set the <code>placementConstraintExpression</code> of the request.
     *
     * @see SchedulingRequest#setPlacementConstraint(
     *      PlacementConstraint)
     * @param placementConstraintExpression <code>placementConstraints</code> of
     *          the request
     * @return {@link SchedulingRequest.SchedulingRequestBuilder}
     */
    @Public
    @Unstable
    public SchedulingRequestBuilder placementConstraintExpression(
        PlacementConstraint placementConstraintExpression) {
      schedulingRequest
          .setPlacementConstraint(placementConstraintExpression);
      return this;
    }

    /**
     * Return generated {@link SchedulingRequest} object.
     *
     * @return {@link SchedulingRequest}
     */
    @Public
    @Unstable
    public SchedulingRequest build() {
      return schedulingRequest;
    }
  }

  public abstract long getAllocationRequestId();

  public abstract void setAllocationRequestId(long allocationRequestId);

  public abstract Priority getPriority();

  public abstract void setPriority(Priority priority);

  public abstract ExecutionTypeRequest getExecutionType();

  public abstract void setExecutionType(ExecutionTypeRequest executionType);

  public abstract Set<String> getAllocationTags();

  public abstract void setAllocationTags(Set<String> allocationTags);

  public abstract ResourceSizing getResourceSizing();

  public abstract void setResourceSizing(ResourceSizing resourceSizing);

  public abstract PlacementConstraint getPlacementConstraint();

  public abstract void setPlacementConstraint(
      PlacementConstraint placementConstraint);
}
