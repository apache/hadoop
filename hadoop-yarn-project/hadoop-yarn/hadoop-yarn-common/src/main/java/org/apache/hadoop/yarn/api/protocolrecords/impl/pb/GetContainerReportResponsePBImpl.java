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

import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerReportPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerReportProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerReportResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerReportResponseProtoOrBuilder;

import com.google.protobuf.TextFormat;

public class GetContainerReportResponsePBImpl extends
    GetContainerReportResponse {

  GetContainerReportResponseProto proto = GetContainerReportResponseProto
    .getDefaultInstance();
  GetContainerReportResponseProto.Builder builder = null;
  boolean viaProto = false;

  private ContainerReport containerReport = null;

  public GetContainerReportResponsePBImpl() {
    builder = GetContainerReportResponseProto.newBuilder();
  }

  public GetContainerReportResponsePBImpl(GetContainerReportResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetContainerReportResponseProto getProto() {
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
    if (this.containerReport != null) {
      builder.setContainerReport(convertToProtoFormat(this.containerReport));
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
      builder = GetContainerReportResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ContainerReport getContainerReport() {
    if (this.containerReport != null) {
      return this.containerReport;
    }
    GetContainerReportResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasContainerReport()) {
      return null;
    }
    this.containerReport = convertFromProtoFormat(p.getContainerReport());
    return this.containerReport;
  }

  @Override
  public void setContainerReport(ContainerReport containerReport) {
    maybeInitBuilder();
    if (containerReport == null) {
      builder.clearContainerReport();
    }
    this.containerReport = containerReport;
  }

  private ContainerReportPBImpl convertFromProtoFormat(ContainerReportProto p) {
    return new ContainerReportPBImpl(p);
  }

  private ContainerReportProto convertToProtoFormat(ContainerReport t) {
    return ((ContainerReportPBImpl) t).getProto();
  }

}
