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

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.yarn.api.records.ReservationAllocationState;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ResourceAllocationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationDefinitionProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceAllocationRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Simple helper class for static methods used to transform across
 * common formats in tests
 */
public final class ReservationSystemUtil {

  private ReservationSystemUtil() {
    // not called
  }

  public static Resource toResource(ReservationRequest request) {
    Resource resource = Resources.multiply(request.getCapability(),
        (float) request.getNumContainers());
    return resource;
  }

  public static Map<ReservationInterval, Resource> toResources(
      Map<ReservationInterval, ReservationRequest> allocations) {
    Map<ReservationInterval, Resource> resources =
        new HashMap<ReservationInterval, Resource>();
    for (Map.Entry<ReservationInterval, ReservationRequest> entry :
        allocations.entrySet()) {
      resources.put(entry.getKey(),
          toResource(entry.getValue()));
    }
    return resources;
  }

  public static ReservationAllocationStateProto buildStateProto(
      ReservationAllocation allocation) {
    ReservationAllocationStateProto.Builder builder =
        ReservationAllocationStateProto.newBuilder();

    builder.setAcceptanceTime(allocation.getAcceptanceTime());
    builder.setContainsGangs(allocation.containsGangs());
    builder.setStartTime(allocation.getStartTime());
    builder.setEndTime(allocation.getEndTime());
    builder.setUser(allocation.getUser());
    ReservationDefinitionProto definitionProto = convertToProtoFormat(
        allocation.getReservationDefinition());
    builder.setReservationDefinition(definitionProto);

    for (Map.Entry<ReservationInterval, Resource> entry :
        allocation.getAllocationRequests().entrySet()) {
      ResourceAllocationRequestProto p =
          ResourceAllocationRequestProto.newBuilder()
          .setStartTime(entry.getKey().getStartTime())
          .setEndTime(entry.getKey().getEndTime())
          .setResource(convertToProtoFormat(entry.getValue()))
          .build();
      builder.addAllocationRequests(p);
    }

    ReservationAllocationStateProto allocationProto = builder.build();
    return allocationProto;
  }

  private static ReservationDefinitionProto convertToProtoFormat(
      ReservationDefinition reservationDefinition) {
    return ((ReservationDefinitionPBImpl)reservationDefinition).getProto();
  }

  public static ResourceProto convertToProtoFormat(Resource e) {
    return YarnProtos.ResourceProto.newBuilder()
        .setMemory(e.getMemorySize())
        .setVirtualCores(e.getVirtualCores())
        .build();
  }

  public static Map<ReservationInterval, Resource> toAllocations(
      List<ResourceAllocationRequestProto> allocationRequestsList) {
    Map<ReservationInterval, Resource> allocations = new HashMap<>();
    for (ResourceAllocationRequestProto proto : allocationRequestsList) {
      allocations.put(
          new ReservationInterval(proto.getStartTime(), proto.getEndTime()),
          convertFromProtoFormat(proto.getResource()));
    }
    return allocations;
  }

  private static ResourcePBImpl convertFromProtoFormat(ResourceProto resource) {
    return new ResourcePBImpl(resource);
  }

  public static ReservationDefinitionPBImpl convertFromProtoFormat(
      ReservationDefinitionProto r) {
    return new ReservationDefinitionPBImpl(r);
  }

  public static ReservationIdPBImpl convertFromProtoFormat(
      ReservationIdProto r) {
    return new ReservationIdPBImpl(r);
  }

  public static ReservationId toReservationId(
      ReservationIdProto reservationId) {
    return new ReservationIdPBImpl(reservationId);
  }

  public static InMemoryReservationAllocation toInMemoryAllocation(
          String planName, ReservationId reservationId,
          ReservationAllocationStateProto allocationState, Resource minAlloc,
          ResourceCalculator planResourceCalculator) {
    ReservationDefinition definition =
        convertFromProtoFormat(
            allocationState.getReservationDefinition());
    Map<ReservationInterval, Resource> allocations = toAllocations(
            allocationState.getAllocationRequestsList());
    InMemoryReservationAllocation allocation =
        new InMemoryReservationAllocation(reservationId, definition,
        allocationState.getUser(), planName, allocationState.getStartTime(),
        allocationState.getEndTime(), allocations, planResourceCalculator,
        minAlloc, allocationState.getContainsGangs());
    return allocation;
  }

  public static List<ReservationAllocationState>
        convertAllocationsToReservationInfo(Set<ReservationAllocation> res,
                        boolean includeResourceAllocations) {
    List<ReservationAllocationState> reservationInfo = new ArrayList<>();

    Map<ReservationInterval, Resource> requests;
    for (ReservationAllocation allocation : res) {
      List<ResourceAllocationRequest> allocations = new ArrayList<>();
      if (includeResourceAllocations) {
        requests = allocation.getAllocationRequests();

        for (Map.Entry<ReservationInterval, Resource> request :
                requests.entrySet()) {
          ReservationInterval interval = request.getKey();
          allocations.add(ResourceAllocationRequest.newInstance(
                  interval.getStartTime(), interval.getEndTime(),
                  request.getValue()));
        }
      }

      reservationInfo.add(ReservationAllocationState.newInstance(
              allocation.getAcceptanceTime(), allocation.getUser(),
              allocations, allocation.getReservationId(),
              allocation.getReservationDefinition()));
    }
    return reservationInfo;
  }
}
