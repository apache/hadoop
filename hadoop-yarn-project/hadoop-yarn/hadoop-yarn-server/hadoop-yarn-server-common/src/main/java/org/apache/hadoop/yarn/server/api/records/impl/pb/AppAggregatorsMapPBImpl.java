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
package org.apache.hadoop.yarn.server.api.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.server.api.records.AppAggregatorsMap;

import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.AppAggregatorsMapProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.AppAggregatorsMapProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class AppAggregatorsMapPBImpl extends AppAggregatorsMap {

  AppAggregatorsMapProto proto = 
      AppAggregatorsMapProto.getDefaultInstance();
  
  AppAggregatorsMapProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationId appId = null;
  private String aggregatorAddr = null;
  
  public AppAggregatorsMapPBImpl() {
    builder = AppAggregatorsMapProto.newBuilder();
  }

  public AppAggregatorsMapPBImpl(AppAggregatorsMapProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public AppAggregatorsMapProto getProto() {
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
  
  @Override
  public ApplicationId getApplicationId() {
    AppAggregatorsMapProtoOrBuilder p = viaProto ? proto : builder;
    if (this.appId == null && p.hasAppId()) {
      this.appId = convertFromProtoFormat(p.getAppId());
    }
    return this.appId;
  }
  
  @Override
  public String getAggregatorAddr() {
    AppAggregatorsMapProtoOrBuilder p = viaProto ? proto : builder;
    if (this.aggregatorAddr == null 
        && p.hasAppAggregatorAddr()) {
      this.aggregatorAddr = p.getAppAggregatorAddr();
    }
    return this.aggregatorAddr;
  }

  @Override
  public void setApplicationId(ApplicationId appId) {
    maybeInitBuilder();
    if (appId == null) {
      builder.clearAppId();
    }
    this.appId = appId;
  }
  
  @Override
  public void setAggregatorAddr(String aggregatorAddr) {
    maybeInitBuilder();
    if (aggregatorAddr == null) {
      builder.clearAppAggregatorAddr();
    }
    this.aggregatorAddr = aggregatorAddr;
  }
  
  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }
  
  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }
  
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = AppAggregatorsMapProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }
  
  private void mergeLocalToBuilder() {
    if (this.appId != null) {
      builder.setAppId(convertToProtoFormat(this.appId));
    }
    if (this.aggregatorAddr != null) {
      builder.setAppAggregatorAddr(this.aggregatorAddr);
    }
  }

}
