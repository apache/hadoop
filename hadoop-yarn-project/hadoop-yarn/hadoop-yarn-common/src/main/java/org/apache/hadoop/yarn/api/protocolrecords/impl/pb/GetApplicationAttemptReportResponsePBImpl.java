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
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptReportPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptReportProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptReportResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptReportResponseProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class GetApplicationAttemptReportResponsePBImpl extends
    GetApplicationAttemptReportResponse {

  GetApplicationAttemptReportResponseProto proto =
      GetApplicationAttemptReportResponseProto.getDefaultInstance();
  GetApplicationAttemptReportResponseProto.Builder builder = null;
  boolean viaProto = false;

  private ApplicationAttemptReport applicationAttemptReport = null;

  public GetApplicationAttemptReportResponsePBImpl() {
    builder = GetApplicationAttemptReportResponseProto.newBuilder();
  }

  public GetApplicationAttemptReportResponsePBImpl(
      GetApplicationAttemptReportResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetApplicationAttemptReportResponseProto getProto() {
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
    if (other == null) {
      return false;
    }
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
    if (this.applicationAttemptReport != null) {
      builder
        .setApplicationAttemptReport(convertToProtoFormat(this.applicationAttemptReport));
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

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetApplicationAttemptReportResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ApplicationAttemptReport getApplicationAttemptReport() {
    if (this.applicationAttemptReport != null) {
      return this.applicationAttemptReport;
    }
    GetApplicationAttemptReportResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasApplicationAttemptReport()) {
      return null;
    }
    this.applicationAttemptReport =
        convertFromProtoFormat(p.getApplicationAttemptReport());
    return this.applicationAttemptReport;
  }

  @Override
  public void setApplicationAttemptReport(
      ApplicationAttemptReport ApplicationAttemptReport) {
    maybeInitBuilder();
    if (ApplicationAttemptReport == null) {
      builder.clearApplicationAttemptReport();
    }
    this.applicationAttemptReport = ApplicationAttemptReport;
  }

  private ApplicationAttemptReportPBImpl convertFromProtoFormat(
      ApplicationAttemptReportProto p) {
    return new ApplicationAttemptReportPBImpl(p);
  }

  private ApplicationAttemptReportProto convertToProtoFormat(
      ApplicationAttemptReport t) {
    return ((ApplicationAttemptReportPBImpl) t).getProto();
  }

}
