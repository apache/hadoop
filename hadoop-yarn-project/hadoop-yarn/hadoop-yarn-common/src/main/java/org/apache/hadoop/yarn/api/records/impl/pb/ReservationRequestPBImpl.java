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
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;

@Private
@Unstable
public class ReservationRequestPBImpl extends ReservationRequest {
  ReservationRequestProto proto = ReservationRequestProto.getDefaultInstance();
  ReservationRequestProto.Builder builder = null;
  boolean viaProto = false;

  private Resource capability = null;

  public ReservationRequestPBImpl() {
    builder = ReservationRequestProto.newBuilder();
  }

  public ReservationRequestPBImpl(ReservationRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ReservationRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.capability != null) {
      builder.setCapability(convertToProtoFormat(this.capability));
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
      builder = ReservationRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public Resource getCapability() {
    ReservationRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.capability != null) {
      return this.capability;
    }
    if (!p.hasCapability()) {
      return null;
    }
    this.capability = convertFromProtoFormat(p.getCapability());
    return this.capability;
  }

  @Override
  public void setCapability(Resource capability) {
    maybeInitBuilder();
    if (capability == null)
      builder.clearCapability();
    this.capability = capability;
  }

  @Override
  public int getNumContainers() {
    ReservationRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getNumContainers());
  }

  @Override
  public void setNumContainers(int numContainers) {
    maybeInitBuilder();
    builder.setNumContainers((numContainers));
  }

  @Override
  public int getConcurrency() {
    ReservationRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasConcurrency()) {
      return 1;
    }
    return (p.getConcurrency());
  }

  @Override
  public void setConcurrency(int numContainers) {
    maybeInitBuilder();
    builder.setConcurrency(numContainers);
  }

  @Override
  public long getDuration() {
    ReservationRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDuration()) {
      return 0;
    }
    return (p.getDuration());
  }

  @Override
  public void setDuration(long duration) {
    maybeInitBuilder();
    builder.setDuration(duration);
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ((ResourcePBImpl) t).getProto();
  }

  @Override
  public String toString() {
    return "{Capability: " + getCapability() + ", # Containers: "
        + getNumContainers() + ", Concurrency: " + getConcurrency()
        + ", Lease Duration: " + getDuration() + "}";
  }
}