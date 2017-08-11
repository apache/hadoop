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

import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.api.records.impl.pb.AppCollectorDataPBImpl;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.AppCollectorDataProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ReportNewCollectorInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ReportNewCollectorInfoRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoRequest;

public class ReportNewCollectorInfoRequestPBImpl extends
    ReportNewCollectorInfoRequest {

  private ReportNewCollectorInfoRequestProto proto =
      ReportNewCollectorInfoRequestProto.getDefaultInstance();

  private ReportNewCollectorInfoRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private List<AppCollectorData> collectorsList = null;

  public ReportNewCollectorInfoRequestPBImpl() {
    builder = ReportNewCollectorInfoRequestProto.newBuilder();
  }

  public ReportNewCollectorInfoRequestPBImpl(
      ReportNewCollectorInfoRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ReportNewCollectorInfoRequestProto getProto() {
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

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (collectorsList != null) {
      addLocalCollectorsToProto();
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ReportNewCollectorInfoRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addLocalCollectorsToProto() {
    maybeInitBuilder();
    builder.clearAppCollectors();
    List<AppCollectorDataProto> protoList =
        new ArrayList<AppCollectorDataProto>();
    for (AppCollectorData m : this.collectorsList) {
      protoList.add(convertToProtoFormat(m));
    }
    builder.addAllAppCollectors(protoList);
  }

  private void initLocalCollectorsList() {
    ReportNewCollectorInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<AppCollectorDataProto> list =
        p.getAppCollectorsList();
    this.collectorsList = new ArrayList<AppCollectorData>();
    for (AppCollectorDataProto m : list) {
      this.collectorsList.add(convertFromProtoFormat(m));
    }
  }

  @Override
  public List<AppCollectorData> getAppCollectorsList() {
    if (this.collectorsList == null) {
      initLocalCollectorsList();
    }
    return this.collectorsList;
  }

  @Override
  public void setAppCollectorsList(List<AppCollectorData> appCollectorsList) {
    maybeInitBuilder();
    if (appCollectorsList == null) {
      builder.clearAppCollectors();
    }
    this.collectorsList = appCollectorsList;
  }

  private AppCollectorDataPBImpl convertFromProtoFormat(
      AppCollectorDataProto p) {
    return new AppCollectorDataPBImpl(p);
  }

  private AppCollectorDataProto convertToProtoFormat(
      AppCollectorData m) {
    return ((AppCollectorDataPBImpl) m).getProto();
  }

}
