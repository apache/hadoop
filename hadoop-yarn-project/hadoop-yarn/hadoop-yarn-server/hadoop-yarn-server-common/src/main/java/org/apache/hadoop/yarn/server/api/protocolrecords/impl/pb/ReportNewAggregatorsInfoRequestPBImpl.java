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
package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.AppAggregatorsMapProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ReportNewAggregatorsInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ReportNewAggregatorsInfoRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewAggregatorsInfoRequest;
import org.apache.hadoop.yarn.server.api.records.AppAggregatorsMap;
import org.apache.hadoop.yarn.server.api.records.impl.pb.AppAggregatorsMapPBImpl;

public class ReportNewAggregatorsInfoRequestPBImpl extends
    ReportNewAggregatorsInfoRequest {

  ReportNewAggregatorsInfoRequestProto proto = 
      ReportNewAggregatorsInfoRequestProto.getDefaultInstance();
  
  ReportNewAggregatorsInfoRequestProto.Builder builder = null;
  boolean viaProto = false;

  private List<AppAggregatorsMap> aggregatorsList = null;

  public ReportNewAggregatorsInfoRequestPBImpl() {
    builder = ReportNewAggregatorsInfoRequestProto.newBuilder();
  }

  public ReportNewAggregatorsInfoRequestPBImpl(
      ReportNewAggregatorsInfoRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ReportNewAggregatorsInfoRequestProto getProto() {
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

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (aggregatorsList != null) {
      addLocalAggregatorsToProto();
    }
  }
  
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ReportNewAggregatorsInfoRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addLocalAggregatorsToProto() {
    maybeInitBuilder();
    builder.clearAppAggregators();
    List<AppAggregatorsMapProto> protoList =
        new ArrayList<AppAggregatorsMapProto>();
    for (AppAggregatorsMap m : this.aggregatorsList) {
      protoList.add(convertToProtoFormat(m));
    }
    builder.addAllAppAggregators(protoList);
  }

  private void initLocalAggregatorsList() {
    ReportNewAggregatorsInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<AppAggregatorsMapProto> aggregatorsList =
        p.getAppAggregatorsList();
    this.aggregatorsList = new ArrayList<AppAggregatorsMap>();
    for (AppAggregatorsMapProto m : aggregatorsList) {
      this.aggregatorsList.add(convertFromProtoFormat(m));
    }
  }

  @Override
  public List<AppAggregatorsMap> getAppAggregatorsList() {  
    if (this.aggregatorsList == null) {
      initLocalAggregatorsList();
    }
    return this.aggregatorsList;
  }

  @Override
  public void setAppAggregatorsList(List<AppAggregatorsMap> appAggregatorsList) {
    maybeInitBuilder();
    if (appAggregatorsList == null) {
      builder.clearAppAggregators();
    }
    this.aggregatorsList = appAggregatorsList;
  }
  
  private AppAggregatorsMapPBImpl convertFromProtoFormat(
      AppAggregatorsMapProto p) {
    return new AppAggregatorsMapPBImpl(p);
  }

  private AppAggregatorsMapProto convertToProtoFormat(
      AppAggregatorsMap m) {
    return ((AppAggregatorsMapPBImpl) m).getProto();
  }

}
