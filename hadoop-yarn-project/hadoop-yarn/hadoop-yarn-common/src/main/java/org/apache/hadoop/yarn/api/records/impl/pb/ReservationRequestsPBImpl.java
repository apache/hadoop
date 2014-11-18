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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationRequestInterpreterProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationRequestsProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationRequestsProtoOrBuilder;

public class ReservationRequestsPBImpl extends ReservationRequests {

  ReservationRequestsProto proto = ReservationRequestsProto
      .getDefaultInstance();
  ReservationRequestsProto.Builder builder = null;
  boolean viaProto = false;

  public List<ReservationRequest> reservationRequests;

  public ReservationRequestsPBImpl() {
    builder = ReservationRequestsProto.newBuilder();
  }

  public ReservationRequestsPBImpl(ReservationRequestsProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ReservationRequestsProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.reservationRequests != null) {
      addReservationResourcesToProto();
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
      builder = ReservationRequestsProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public List<ReservationRequest> getReservationResources() {
    initReservationRequestsList();
    return reservationRequests;
  }

  @Override
  public void setReservationResources(List<ReservationRequest> resources) {
    if (resources == null) {
      builder.clearReservationResources();
      return;
    }
    this.reservationRequests = resources;
  }

  @Override
  public ReservationRequestInterpreter getInterpreter() {
    ReservationRequestsProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasInterpreter()) {
      return null;
    }
    return (convertFromProtoFormat(p.getInterpreter()));
  }

  @Override
  public void setInterpreter(ReservationRequestInterpreter interpreter) {
    maybeInitBuilder();
    if (interpreter == null) {
      builder.clearInterpreter();
      return;
    }
    builder.setInterpreter(convertToProtoFormat(interpreter));
  }

  private void initReservationRequestsList() {
    if (this.reservationRequests != null) {
      return;
    }
    ReservationRequestsProtoOrBuilder p = viaProto ? proto : builder;
    List<ReservationRequestProto> resourceProtos =
        p.getReservationResourcesList();
    reservationRequests = new ArrayList<ReservationRequest>();

    for (ReservationRequestProto r : resourceProtos) {
      reservationRequests.add(convertFromProtoFormat(r));
    }
  }

  private void addReservationResourcesToProto() {
    maybeInitBuilder();
    builder.clearReservationResources();
    if (reservationRequests == null)
      return;
    Iterable<ReservationRequestProto> iterable =
        new Iterable<ReservationRequestProto>() {
          @Override
          public Iterator<ReservationRequestProto> iterator() {
            return new Iterator<ReservationRequestProto>() {

              Iterator<ReservationRequest> iter = reservationRequests
                  .iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public ReservationRequestProto next() {
                return convertToProtoFormat(iter.next());
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

            };

          }

        };
    builder.addAllReservationResources(iterable);
  }

  private ReservationRequestProto convertToProtoFormat(ReservationRequest r) {
    return ((ReservationRequestPBImpl) r).getProto();
  }

  private ReservationRequestPBImpl convertFromProtoFormat(
      ReservationRequestProto r) {
    return new ReservationRequestPBImpl(r);
  }

  private ReservationRequestInterpreterProto convertToProtoFormat(
      ReservationRequestInterpreter r) {
    return ProtoUtils.convertToProtoFormat(r);
  }

  private ReservationRequestInterpreter convertFromProtoFormat(
      ReservationRequestInterpreterProto r) {
    return ProtoUtils.convertFromProtoFormat(r);
  }

  @Override
  public String toString() {
    return "{Reservation Resources: " + getReservationResources()
        + ", Reservation Type: " + getInterpreter() + "}";
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
