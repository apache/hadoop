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


import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos.FinalApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.FinishApplicationMasterRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.FinishApplicationMasterRequestProtoOrBuilder;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

@Private
@Unstable
public class FinishApplicationMasterRequestPBImpl extends FinishApplicationMasterRequest {
  FinishApplicationMasterRequestProto proto = FinishApplicationMasterRequestProto.getDefaultInstance();
  FinishApplicationMasterRequestProto.Builder builder = null;
  boolean viaProto = false;

  public FinishApplicationMasterRequestPBImpl() {
    builder = FinishApplicationMasterRequestProto.newBuilder();
  }

  public FinishApplicationMasterRequestPBImpl(FinishApplicationMasterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public FinishApplicationMasterRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
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

  private void mergeLocalToBuilder() {
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
      builder = FinishApplicationMasterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getDiagnostics() {
    FinishApplicationMasterRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getDiagnostics();
  }

  @Override
  public void setDiagnostics(String diagnostics) {
    maybeInitBuilder();
    if (diagnostics == null) {
      builder.clearDiagnostics();
      return;
    }
    builder.setDiagnostics(diagnostics);
  }

  @Override
  public String getTrackingUrl() {
    FinishApplicationMasterRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getTrackingUrl();
  }

  @Override
  public void setTrackingUrl(String url) {
    maybeInitBuilder();
    if (url == null) {
      builder.clearTrackingUrl();
      return;
    }
    builder.setTrackingUrl(url);
  }

  @Override
  public FinalApplicationStatus getFinalApplicationStatus() {
    FinishApplicationMasterRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasFinalApplicationStatus()) {
      return null;
    }	
    return convertFromProtoFormat(p.getFinalApplicationStatus());
  }

  @Override
  public void setFinalApplicationStatus(FinalApplicationStatus finalState) {
    maybeInitBuilder();
    if (finalState == null) {
      builder.clearFinalApplicationStatus();
      return;
    }
    builder.setFinalApplicationStatus(convertToProtoFormat(finalState));
  }

  private FinalApplicationStatus convertFromProtoFormat(FinalApplicationStatusProto s) {
    return ProtoUtils.convertFromProtoFormat(s);
  }

  private FinalApplicationStatusProto convertToProtoFormat(FinalApplicationStatus s) {
    return ProtoUtils.convertToProtoFormat(s);
  }


}
