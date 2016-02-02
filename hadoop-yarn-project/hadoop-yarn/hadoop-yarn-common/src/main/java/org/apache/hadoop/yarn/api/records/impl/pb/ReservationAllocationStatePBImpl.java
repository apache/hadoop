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
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationDefinitionProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceAllocationRequestProto;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code ReservationAllocationStatePBImpl} implements the {@link
 * ReservationAllocationState} that represents the  reservation  that is
 * made by a user.
 *
 * <p>
 * It includes:
 * <ul>
 *   <li>Duration of the reservation.</li>
 *   <li>Acceptance time of the duration.</li>
 *   <li>
 *       List of {@link ResourceAllocationRequest}, which includes the time
 *       interval, and capability of the allocation.
 *       {@code ResourceAllocationRequest} represents an allocation
 *       made for a reservation for the current state of the plan. This can be
 *       changed for reasons such as re-planning, but will always be subject to
 *       the constraints of the user contract as described by
 *       {@link ReservationDefinition}
 *   </li>
 *   <li>{@link ReservationId} of the reservation.</li>
 *   <li>{@link ReservationDefinition} used to make the reservation.</li>
 * </ul>
 *
 * @see ResourceAllocationRequest
 * @see ReservationId
 * @see ReservationDefinition
 */
@Private
@Unstable
public class ReservationAllocationStatePBImpl extends
        ReservationAllocationState {
  private ReservationAllocationStateProto proto =
          ReservationAllocationStateProto.getDefaultInstance();;
  private ReservationAllocationStateProto.Builder builder = null;
  private boolean viaProto = false;

  private List<ResourceAllocationRequest> resourceAllocations = null;
  private ReservationId reservationId = null;
  private ReservationDefinition reservationDefinition = null;

  public ReservationAllocationStatePBImpl() {
    builder = ReservationAllocationStateProto.newBuilder();
  }

  public ReservationAllocationStatePBImpl(
          ReservationAllocationStateProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ReservationAllocationStateProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ReservationAllocationStateProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.resourceAllocations != null) {
      int size = resourceAllocations.size();
      builder.clearAllocationRequests();
      for (int i = 0; i < size; i++) {
        builder.addAllocationRequests(i, convertToProtoFormat(
                resourceAllocations.get(i)
        ));
      }
    }

    if (this.reservationId != null) {
      builder.setReservationId(convertToProtoFormat(this.reservationId));
    }

    if (this.reservationDefinition != null) {
      builder.setReservationDefinition(convertToProtoFormat(this
              .reservationDefinition));
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
  public long getAcceptanceTime() {
    ReservationAllocationStateProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAcceptanceTime()) {
      return 0;
    }
    return (p.getAcceptanceTime());
  }

  @Override
  public void setAcceptanceTime(long acceptanceTime) {
    maybeInitBuilder();
    if (acceptanceTime <= 0) {
      builder.clearAcceptanceTime();
      return;
    }
    builder.setAcceptanceTime(acceptanceTime);
  }

  @Override
  public String getUser() {
    ReservationAllocationStateProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasUser()) {
      return null;
    }
    return p.getUser();
  }

  @Override
  public void setUser(String user) {
    maybeInitBuilder();
    if (user == null) {
      builder.clearUser();
      return;
    }
    builder.setUser(user);
  }

  @Override
  public List<ResourceAllocationRequest>
      getResourceAllocationRequests() {
    initResourceAllocations();
    return this.resourceAllocations;
  }

  @Override
  public void setResourceAllocationRequests(
          List<ResourceAllocationRequest> newResourceAllocations) {
    maybeInitBuilder();
    if (newResourceAllocations == null) {
      builder.clearAllocationRequests();
    }
    this.resourceAllocations = newResourceAllocations;
  }

  @Override
  public ReservationId getReservationId() {
    ReservationAllocationStateProtoOrBuilder p = viaProto ? proto : builder;
    if (this.reservationId != null) {
      return this.reservationId;
    }
    this.reservationId = convertFromProtoFormat(p.getReservationId());
    return this.reservationId;
  }

  @Override
  public void setReservationId(ReservationId newReservationId) {
    maybeInitBuilder();
    if (newReservationId == null) {
      builder.clearReservationId();
    }
    reservationId = newReservationId;
  }

  @Override
  public ReservationDefinition getReservationDefinition() {
    ReservationAllocationStateProtoOrBuilder p = viaProto ? proto : builder;
    if (this.reservationDefinition != null) {
      return this.reservationDefinition;
    }
    this.reservationDefinition = convertFromProtoFormat(
            p.getReservationDefinition());
    return this.reservationDefinition;
  }

  @Override
  public void setReservationDefinition(ReservationDefinition
                                                 newReservationDefinition) {
    maybeInitBuilder();
    if (newReservationDefinition == null) {
      builder.clearReservationDefinition();
    }
    reservationDefinition = newReservationDefinition;
  }

  private ResourceAllocationRequestPBImpl convertFromProtoFormat(
          ResourceAllocationRequestProto p) {
    return new ResourceAllocationRequestPBImpl(p);
  }

  private ReservationIdPBImpl convertFromProtoFormat(ReservationIdProto p) {
    return new ReservationIdPBImpl(p);
  }

  private ReservationDefinitionPBImpl convertFromProtoFormat(
          ReservationDefinitionProto p) {
    return new ReservationDefinitionPBImpl(p);
  }

  private ResourceAllocationRequestProto convertToProtoFormat(
          ResourceAllocationRequest p) {
    return ((ResourceAllocationRequestPBImpl)p).getProto();
  }

  private ReservationIdProto convertToProtoFormat(ReservationId p) {
    return ((ReservationIdPBImpl)p).getProto();
  }

  private ReservationDefinitionProto convertToProtoFormat(
          ReservationDefinition p) {
    return ((ReservationDefinitionPBImpl)p).getProto();
  }

  private void initResourceAllocations() {
    if (this.resourceAllocations != null) {
      return;
    }
    ReservationAllocationStateProtoOrBuilder p = viaProto ? proto : builder;
    List<ResourceAllocationRequestProto> resourceAllocationProtos =
            p.getAllocationRequestsList();
    resourceAllocations = new ArrayList<>();

    for (ResourceAllocationRequestProto r : resourceAllocationProtos) {
      resourceAllocations.add(convertFromProtoFormat(r));
    }
  }

  @Override
  public String toString() {
    return "{Acceptance Time: "
        + getAcceptanceTime() + ", User: " + getUser()
        + ", Resource Allocations: " + getResourceAllocationRequests()
        + ", Reservation Id: " + getReservationId()
        + ", Reservation Definition: " + getReservationDefinition() + "}";
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