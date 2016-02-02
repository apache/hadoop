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
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.proto.YarnServiceProtos
        .ReservationListRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos
        .ReservationListRequestProtoOrBuilder;

/**
 * {@link ReservationListRequestPBImpl} implements the {@link
 * ReservationListRequest} abstract class which captures the set of requirements
 * the user has to list reservations.
 *
 * @see ReservationListRequest
 */
public class ReservationListRequestPBImpl extends
        ReservationListRequest {

  private ReservationListRequestProto proto = ReservationListRequestProto
          .getDefaultInstance();
  private ReservationListRequestProto.Builder builder = null;
  private boolean viaProto = false;

  public ReservationListRequestPBImpl() {
    builder = ReservationListRequestProto.newBuilder();
  }

  public ReservationListRequestPBImpl(
          ReservationListRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ReservationListRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public String getQueue() {
    ReservationListRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasQueue()) {
      return null;
    }
    return (p.getQueue());
  }

  @Override
  public void setQueue(String queue) {
    maybeInitBuilder();
    if (queue == null) {
      builder.clearQueue();
      return;
    }
    builder.setQueue(queue);
  }

  @Override
  public String getReservationId() {
    ReservationListRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasReservationId()) {
      return null;
    }
    return (p.getReservationId());
  }

  @Override
  public void setReservationId(String reservationId) {
    maybeInitBuilder();
    if (reservationId == null) {
      builder.clearReservationId();
      return;
    }
    builder.setReservationId(reservationId);
  }

  @Override
  public long getStartTime() {
    ReservationListRequestProtoOrBuilder p = viaProto ? proto : builder;
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
    ReservationListRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasEndTime()) {
      return Long.MAX_VALUE;
    }
    return (p.getEndTime());
  }

  @Override
  public void setEndTime(long endTime) {
    maybeInitBuilder();
    if (endTime < 0) {
      builder.setEndTime(Long.MAX_VALUE);
      return;
    }
    builder.setEndTime(endTime);
  }

  @Override
  public boolean getIncludeResourceAllocations() {
    ReservationListRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasIncludeResourceAllocations()) {
      return false;
    }
    return (p.getIncludeResourceAllocations());
  }

  @Override
  public void setIncludeResourceAllocations(boolean
                              includeReservationAllocations) {
    maybeInitBuilder();
    builder.setIncludeResourceAllocations(includeReservationAllocations);
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ReservationListRequestProto.newBuilder(proto);
    }
    viaProto = false;
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
