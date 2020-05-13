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

package org.apache.hadoop.yarn.api.records.impl.pb;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.pb.PlacementConstraintFromProtoConverter;
import org.apache.hadoop.yarn.api.pb.PlacementConstraintToProtoConverter;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.proto.YarnProtos.ExecutionTypeRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PlacementConstraintProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceSizingProto;
import org.apache.hadoop.yarn.proto.YarnProtos.SchedulingRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.SchedulingRequestProtoOrBuilder;

/**
 * Proto implementation for {@link SchedulingRequest} interface.
 */
@Private
@Unstable
public class SchedulingRequestPBImpl extends SchedulingRequest {
  SchedulingRequestProto proto = SchedulingRequestProto.getDefaultInstance();
  SchedulingRequestProto.Builder builder = null;
  boolean viaProto = false;

  private Priority priority = null;
  private ExecutionTypeRequest executionType = null;
  private Set<String> allocationTags = null;
  private ResourceSizing resourceSizing = null;
  private PlacementConstraint placementConstraint = null;

  public SchedulingRequestPBImpl() {
    builder = SchedulingRequestProto.newBuilder();
  }

  public SchedulingRequestPBImpl(SchedulingRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public SchedulingRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.priority != null) {
      builder.setPriority(convertToProtoFormat(this.priority));
    }
    if (this.executionType != null) {
      builder.setExecutionType(convertToProtoFormat(this.executionType));
    }
    if (this.allocationTags != null) {
      builder.clearAllocationTags();
      builder.addAllAllocationTags(this.allocationTags);
    }
    if (this.resourceSizing != null) {
      builder.setResourceSizing(convertToProtoFormat(this.resourceSizing));
    }
    if (this.placementConstraint != null) {
      builder.setPlacementConstraint(
          convertToProtoFormat(this.placementConstraint));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SchedulingRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public long getAllocationRequestId() {
    SchedulingRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getAllocationRequestId());
  }

  @Override
  public void setAllocationRequestId(long allocationRequestId) {
    maybeInitBuilder();
    builder.setAllocationRequestId(allocationRequestId);
  }

  @Override
  public Priority getPriority() {
    SchedulingRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.priority != null) {
      return this.priority;
    }
    if (!p.hasPriority()) {
      return null;
    }
    this.priority = convertFromProtoFormat(p.getPriority());
    return this.priority;
  }

  @Override
  public void setPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null) {
      builder.clearPriority();
    }
    this.priority = priority;
  }

  @Override
  public ExecutionTypeRequest getExecutionType() {
    SchedulingRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.executionType != null) {
      return this.executionType;
    }
    if (!p.hasExecutionType()) {
      return null;
    }
    this.executionType = convertFromProtoFormat(p.getExecutionType());
    return this.executionType;
  }

  @Override
  public void setExecutionType(ExecutionTypeRequest executionType) {
    maybeInitBuilder();
    if (executionType == null) {
      builder.clearExecutionType();
    }
    this.executionType = executionType;
  }

  @Override
  public Set<String> getAllocationTags() {
    initAllocationTags();
    return this.allocationTags;
  }

  @Override
  public void setAllocationTags(Set<String> allocationTags) {
    maybeInitBuilder();
    builder.clearAllocationTags();
    this.allocationTags = allocationTags;
  }

  @Override
  public ResourceSizing getResourceSizing() {
    SchedulingRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.resourceSizing != null) {
      return this.resourceSizing;
    }
    if (!p.hasResourceSizing()) {
      return null;
    }
    this.resourceSizing = convertFromProtoFormat(p.getResourceSizing());
    return this.resourceSizing;
  }

  @Override
  public void setResourceSizing(ResourceSizing resourceSizing) {
    maybeInitBuilder();
    if (resourceSizing == null) {
      builder.clearResourceSizing();
    }
    this.resourceSizing = resourceSizing;
  }

  @Override
  public PlacementConstraint getPlacementConstraint() {
    SchedulingRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.placementConstraint != null) {
      return this.placementConstraint;
    }
    if (!p.hasPlacementConstraint()) {
      return null;
    }
    this.placementConstraint =
        convertFromProtoFormat(p.getPlacementConstraint());
    return this.placementConstraint;
  }

  @Override
  public void setPlacementConstraint(PlacementConstraint placementConstraint) {
    maybeInitBuilder();
    if (placementConstraint == null) {
      builder.clearPlacementConstraint();
    }
    this.placementConstraint = placementConstraint;
  }

  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority p) {
    return ((PriorityPBImpl) p).getProto();
  }

  private ExecutionTypeRequestPBImpl convertFromProtoFormat(
      ExecutionTypeRequestProto p) {
    return new ExecutionTypeRequestPBImpl(p);
  }

  private ExecutionTypeRequestProto convertToProtoFormat(
      ExecutionTypeRequest p) {
    return ((ExecutionTypeRequestPBImpl) p).getProto();
  }

  private ResourceSizingPBImpl convertFromProtoFormat(ResourceSizingProto p) {
    return new ResourceSizingPBImpl(p);
  }

  private ResourceSizingProto convertToProtoFormat(ResourceSizing p) {
    return ((ResourceSizingPBImpl) p).getProto();
  }

  private PlacementConstraint convertFromProtoFormat(
      PlacementConstraintProto c) {
    PlacementConstraintFromProtoConverter fromProtoConverter =
        new PlacementConstraintFromProtoConverter(c);
    return fromProtoConverter.convert();
  }

  private PlacementConstraintProto convertToProtoFormat(PlacementConstraint c) {
    PlacementConstraintToProtoConverter toProtoConverter =
        new PlacementConstraintToProtoConverter(c);
    return toProtoConverter.convert();
  }

  private void initAllocationTags() {
    if (this.allocationTags != null) {
      return;
    }
    SchedulingRequestProtoOrBuilder p = viaProto ? proto : builder;
    this.allocationTags = new HashSet<>();
    this.allocationTags.addAll(p.getAllocationTagsList());
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other instanceof SchedulingRequest) {
      if (this == other) {
        return true;
      }
      SchedulingRequest that = (SchedulingRequest) other;
      if (getAllocationRequestId() != that.getAllocationRequestId()) {
        return false;
      }
      if (!getAllocationTags().equals(that.getAllocationTags())) {
        return false;
      }
      if (!getPriority().equals(that.getPriority())) {
        return false;
      }
      if(!getExecutionType().equals(that.getExecutionType())) {
        return false;
      }
      if(!getResourceSizing().equals(that.getResourceSizing())) {
        return false;
      }
      return getPlacementConstraint().equals(that.getPlacementConstraint());
    }
    return false;
  }

  @Override
  public String toString() {
    return "SchedulingRequestPBImpl{" +
        "priority=" + getPriority() +
        ", allocationReqId=" + getAllocationRequestId() +
        ", executionType=" + getExecutionType() +
        ", allocationTags=" + getAllocationTags() +
        ", resourceSizing=" + getResourceSizing() +
        ", placementConstraint=" + getPlacementConstraint() +
        '}';
  }
}
