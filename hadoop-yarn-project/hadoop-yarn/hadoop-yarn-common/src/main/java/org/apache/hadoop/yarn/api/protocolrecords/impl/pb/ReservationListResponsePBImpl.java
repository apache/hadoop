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

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.records.ReservationAllocationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationAllocationStatePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationListResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationListResponseProtoOrBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link ReservationListResponsePBImpl} is the implementation of the
 * {@link ReservationListResponse} which captures  the list of reservations
 * that the user has queried.
 */
public class ReservationListResponsePBImpl extends
    ReservationListResponse {

  private ReservationListResponseProto proto = ReservationListResponseProto
          .getDefaultInstance();
  private ReservationListResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private List<ReservationAllocationState> reservations;

  public ReservationListResponsePBImpl() {
    builder = ReservationListResponseProto.newBuilder();
  }

  public ReservationListResponsePBImpl(
          ReservationListResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ReservationListResponseProto getProto() {
    if (viaProto) {
      mergeLocalToProto();
    } else {
      proto = builder.build();
    }
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ReservationListResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public List<ReservationAllocationState> getReservationAllocationState() {
    initReservations();
    mergeLocalToProto();
    return this.reservations;
  }

  @Override
  public void setReservationAllocationState(List<ReservationAllocationState>
                                                      newReservations) {
    if (newReservations == null) {
      builder.clearReservations();
      return;
    }
    reservations = newReservations;
    mergeLocalToProto();
  }

  private void mergeLocalToBuilder() {
    if (this.reservations != null) {
      int size = reservations.size();
      builder.clearReservations();
      for (int i = 0; i < size; i++) {
        builder.addReservations(i, convertToProtoFormat(
                reservations.get(i)
        ));
      }
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

  private ReservationAllocationStatePBImpl convertFromProtoFormat(
          ReservationAllocationStateProto p) {
    return new ReservationAllocationStatePBImpl(p);
  }

  private ReservationAllocationStateProto convertToProtoFormat(
          ReservationAllocationState r) {
    return ((ReservationAllocationStatePBImpl)r).getProto();
  }

  private void initReservations() {
    if (this.reservations != null) {
      return;
    }
    ReservationListResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ReservationAllocationStateProto> reservationProtos =
            p.getReservationsList();
    reservations = new ArrayList<>();

    for (ReservationAllocationStateProto r : reservationProtos) {
      reservations.add(convertFromProtoFormat(r));
    }
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
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
