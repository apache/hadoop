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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ResourceAllocationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceAllocationRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceAllocationRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;

/**
 * {@code ResourceAllocationRequestPBImpl} which implements the
 * {@link ResourceAllocationRequest} class which represents an allocation
 * made for a reservation for the current state of the plan. This can be
 * changed for reasons such as re-planning, but will always be subject to the
 * constraints of the user contract as described by a
 * {@code ReservationDefinition}
 * {@link Resource}
 *
 * <p>
 * It includes:
 * <ul>
 *   <li>StartTime of the allocation.</li>
 *   <li>EndTime of the allocation.</li>
 *   <li>{@link Resource} reserved for the allocation.</li>
 * </ul>
 *
 * @see Resource
 */
@Private
@Unstable
public class ResourceAllocationRequestPBImpl extends
        ResourceAllocationRequest {
  private ResourceAllocationRequestProto proto =
          ResourceAllocationRequestProto.getDefaultInstance();
  private ResourceAllocationRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private Resource capability = null;

  public ResourceAllocationRequestPBImpl() {
    builder = ResourceAllocationRequestProto.newBuilder();
  }

  public ResourceAllocationRequestPBImpl(
          ResourceAllocationRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ResourceAllocationRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceAllocationRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public Resource getCapability() {
    ResourceAllocationRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.capability != null) {
      return this.capability;
    }
    if (!p.hasResource()) {
      return null;
    }
    this.capability = convertFromProtoFormat(p.getResource());
    return this.capability;
  }

  @Override
  public void setCapability(Resource newCapability) {
    maybeInitBuilder();
    if (newCapability == null) {
      builder.clearResource();
      return;
    }
    capability = newCapability;
  }

  @Override
  public long getStartTime() {
    ResourceAllocationRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasStartTime()) {
      return 0;
    }
    return (p.getStartTime());
  }

  @Override
  public void setStartTime(long startTime) {
    maybeInitBuilder();
    if (startTime <= 0) {
      builder.clearStartTime();
      return;
    }
    builder.setStartTime(startTime);
  }

  @Override
  public long getEndTime() {
    ResourceAllocationRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasEndTime()) {
      return 0;
    }
    return (p.getEndTime());
  }

  @Override
  public void setEndTime(long endTime) {
    maybeInitBuilder();
    if (endTime <= 0) {
      builder.clearEndTime();
      return;
    }
    builder.setEndTime(endTime);
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource p) {
    return ProtoUtils.convertToProtoFormat(p);
  }

  private void mergeLocalToBuilder() {
    if (this.capability != null) {
      builder.setResource(convertToProtoFormat(this.capability));
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

  @Override
  public String toString() {
    return "{Resource: " + getCapability() + ", # Start Time: "
        + getStartTime() + ", End Time: " + getEndTime() + "}";
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }
}