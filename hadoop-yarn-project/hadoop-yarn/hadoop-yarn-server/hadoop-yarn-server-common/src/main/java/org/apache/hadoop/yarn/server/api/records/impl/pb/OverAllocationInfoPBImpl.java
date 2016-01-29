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

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.OverAllocationInfoProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.OverAllocationInfoProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ResourceThresholdsProto;
import org.apache.hadoop.yarn.server.api.records.OverAllocationInfo;
import org.apache.hadoop.yarn.server.api.records.ResourceThresholds;

public class OverAllocationInfoPBImpl extends OverAllocationInfo {
  private OverAllocationInfoProto proto =
      OverAllocationInfoProto.getDefaultInstance();
  private OverAllocationInfoProto.Builder builder = null;
  private boolean viaProto = false;

  private ResourceThresholds overAllocationThresholds = null;

  public OverAllocationInfoPBImpl() {
    builder = OverAllocationInfoProto.newBuilder();
  }

  public OverAllocationInfoPBImpl(OverAllocationInfoProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized OverAllocationInfoProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void mergeLocalToBuilder() {
    if (overAllocationThresholds != null) {
      builder.setOverAllocationThresholds(
          convertToProtoFormat(overAllocationThresholds));
    }
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = OverAllocationInfoProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized ResourceThresholds getOverAllocationThresholds() {
    OverAllocationInfoProtoOrBuilder p = viaProto ? proto : builder;
    if (overAllocationThresholds != null) {
      return overAllocationThresholds;
    }
    if (!p.hasOverAllocationThresholds()) {
      return null;
    }
    overAllocationThresholds =
        convertFromProtoFormat(p.getOverAllocationThresholds());
    return overAllocationThresholds;
  }

  @Override
  public synchronized void setOverAllocationThreshold(
      ResourceThresholds resourceThresholds) {
    maybeInitBuilder();
    if (this.overAllocationThresholds != null) {
      builder.clearOverAllocationThresholds();
    }
    this.overAllocationThresholds = resourceThresholds;
  }

  private static ResourceThresholdsProto convertToProtoFormat(
      ResourceThresholds overAllocationThresholds) {
    return ((ResourceThresholdsPBImpl) overAllocationThresholds).getProto();
  }

  private static ResourceThresholds convertFromProtoFormat(
      ResourceThresholdsProto overAllocationThresholdsProto) {
    return new ResourceThresholdsPBImpl(overAllocationThresholdsProto);
  }
}
