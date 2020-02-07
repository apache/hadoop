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
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationReportPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationReportProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationReportResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationReportResponseProtoOrBuilder;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

@Private
@Unstable
public class GetApplicationReportResponsePBImpl extends GetApplicationReportResponse {
  GetApplicationReportResponseProto proto = GetApplicationReportResponseProto.getDefaultInstance();
  GetApplicationReportResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationReport applicationReport = null;
  
  
  public GetApplicationReportResponsePBImpl() {
    builder = GetApplicationReportResponseProto.newBuilder();
  }

  public GetApplicationReportResponsePBImpl(GetApplicationReportResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetApplicationReportResponseProto getProto() {
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
    if (this.applicationReport != null) {
      builder.setApplicationReport(convertToProtoFormat(this.applicationReport));
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
      builder = GetApplicationReportResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public ApplicationReport getApplicationReport() {
    GetApplicationReportResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationReport != null) {
      return this.applicationReport;
    }
    if (!p.hasApplicationReport()) {
      return null;
    }
    this.applicationReport = convertFromProtoFormat(p.getApplicationReport());
    return this.applicationReport;
  }

  @Override
  public void setApplicationReport(ApplicationReport applicationMaster) {
    maybeInitBuilder();
    if (applicationMaster == null) 
      builder.clearApplicationReport();
    this.applicationReport = applicationMaster;
  }

  private ApplicationReportPBImpl convertFromProtoFormat(ApplicationReportProto p) {
    return new ApplicationReportPBImpl(p);
  }

  private ApplicationReportProto convertToProtoFormat(ApplicationReport t) {
    return ((ApplicationReportPBImpl)t).getProto();
  }



}  
