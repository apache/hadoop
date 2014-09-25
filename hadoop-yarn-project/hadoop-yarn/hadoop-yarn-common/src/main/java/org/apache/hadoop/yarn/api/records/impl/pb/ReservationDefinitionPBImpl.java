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

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationDefinitionProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationDefinitionProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationRequestsProto;

public class ReservationDefinitionPBImpl extends ReservationDefinition {

  ReservationDefinitionProto proto = ReservationDefinitionProto
      .getDefaultInstance();
  ReservationDefinitionProto.Builder builder = null;
  boolean viaProto = false;

  private ReservationRequests reservationReqs;

  public ReservationDefinitionPBImpl() {
    builder = ReservationDefinitionProto.newBuilder();
  }

  public ReservationDefinitionPBImpl(ReservationDefinitionProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ReservationDefinitionProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.reservationReqs != null) {
      builder
          .setReservationRequests(convertToProtoFormat(this.reservationReqs));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ReservationDefinitionProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public long getArrival() {
    ReservationDefinitionProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasArrival()) {
      return 0;
    }
    return (p.getArrival());
  }

  @Override
  public void setArrival(long earliestStartTime) {
    maybeInitBuilder();
    if (earliestStartTime <= 0) {
      builder.clearArrival();
      return;
    }
    builder.setArrival(earliestStartTime);
  }

  @Override
  public long getDeadline() {
    ReservationDefinitionProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDeadline()) {
      return 0;
    }
    return (p.getDeadline());
  }

  @Override
  public void setDeadline(long latestEndTime) {
    maybeInitBuilder();
    if (latestEndTime <= 0) {
      builder.clearDeadline();
      return;
    }
    builder.setDeadline(latestEndTime);
  }

  @Override
  public ReservationRequests getReservationRequests() {
    ReservationDefinitionProtoOrBuilder p = viaProto ? proto : builder;
    if (reservationReqs != null) {
      return reservationReqs;
    }
    if (!p.hasReservationRequests()) {
      return null;
    }
    reservationReqs = convertFromProtoFormat(p.getReservationRequests());
    return reservationReqs;
  }

  @Override
  public void setReservationRequests(ReservationRequests reservationRequests) {
    if (reservationRequests == null) {
      builder.clearReservationRequests();
      return;
    }
    this.reservationReqs = reservationRequests;
  }

  @Override
  public String getReservationName() {
    ReservationDefinitionProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasReservationName()) {
      return null;
    }
    return (p.getReservationName());
  }

  @Override
  public void setReservationName(String name) {
    maybeInitBuilder();
    if (name == null) {
      builder.clearReservationName();
      return;
    }
    builder.setReservationName(name);
  }

  private ReservationRequestsPBImpl convertFromProtoFormat(
      ReservationRequestsProto p) {
    return new ReservationRequestsPBImpl(p);
  }

  private ReservationRequestsProto convertToProtoFormat(ReservationRequests t) {
    return ((ReservationRequestsPBImpl) t).getProto();
  }

  @Override
  public String toString() {
    return "{Arrival: " + getArrival() + ", Deadline: " + getDeadline()
        + ", Reservation Name: " + getReservationName() + ", Resources: "
        + getReservationRequests() + "}";
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

}
