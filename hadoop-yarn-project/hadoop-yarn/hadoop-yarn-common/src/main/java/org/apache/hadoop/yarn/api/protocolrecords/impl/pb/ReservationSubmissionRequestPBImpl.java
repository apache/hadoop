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

import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationDefinitionProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationSubmissionRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationSubmissionRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

public class ReservationSubmissionRequestPBImpl extends
    ReservationSubmissionRequest {

  ReservationSubmissionRequestProto proto = ReservationSubmissionRequestProto
      .getDefaultInstance();
  ReservationSubmissionRequestProto.Builder builder = null;
  boolean viaProto = false;

  private ReservationDefinition reservationDefinition;

  public ReservationSubmissionRequestPBImpl() {
    builder = ReservationSubmissionRequestProto.newBuilder();
  }

  public ReservationSubmissionRequestPBImpl(
      ReservationSubmissionRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ReservationSubmissionRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.reservationDefinition != null) {
      builder
          .setReservationDefinition(convertToProtoFormat(reservationDefinition));
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
      builder = ReservationSubmissionRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ReservationDefinition getReservationDefinition() {
    ReservationSubmissionRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (reservationDefinition != null) {
      return reservationDefinition;
    }
    if (!p.hasReservationDefinition()) {
      return null;
    }
    reservationDefinition =
        convertFromProtoFormat(p.getReservationDefinition());
    return reservationDefinition;
  }

  @Override
  public void setReservationDefinition(
      ReservationDefinition reservationDefinition) {
    maybeInitBuilder();
    if (reservationDefinition == null) {
      builder.clearReservationDefinition();
    }
    this.reservationDefinition = reservationDefinition;
  }

  @Override
  public String getQueue() {
    ReservationSubmissionRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasQueue()) {
      return null;
    }
    return (p.getQueue());
  }

  @Override
  public void setQueue(String planName) {
    maybeInitBuilder();
    if (planName == null) {
      builder.clearQueue();
      return;
    }
    builder.setQueue(planName);
  }

  private ReservationDefinitionProto convertToProtoFormat(
      ReservationDefinition r) {
    return ((ReservationDefinitionPBImpl) r).getProto();
  }

  private ReservationDefinitionPBImpl convertFromProtoFormat(
      ReservationDefinitionProto r) {
    return new ReservationDefinitionPBImpl(r);
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

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

}
