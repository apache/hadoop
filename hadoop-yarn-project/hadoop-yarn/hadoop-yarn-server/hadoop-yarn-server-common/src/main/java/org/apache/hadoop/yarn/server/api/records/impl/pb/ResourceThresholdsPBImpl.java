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

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ResourceThresholdsProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ResourceThresholdsProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.ResourceThresholds;

public class ResourceThresholdsPBImpl extends ResourceThresholds{
  private ResourceThresholdsProto proto =
      ResourceThresholdsProto.getDefaultInstance();
  private ResourceThresholdsProto.Builder builder = null;
  private boolean viaProto = false;

  public ResourceThresholdsPBImpl() {
    builder = ResourceThresholdsProto.newBuilder();
  }

  public ResourceThresholdsPBImpl(ResourceThresholdsProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized ResourceThresholdsProto getProto() {
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
    /*
     * Right now, we have only memory and cpu thresholds that are floats.
     * This is a no-op until we add other non-static fields to the proto.
     */
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceThresholdsProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized float getMemoryThreshold() {
    ResourceThresholdsProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMemory();
  }

  @Override
  public synchronized float getCpuThreshold() {
    ResourceThresholdsProtoOrBuilder p = viaProto ? proto : builder;
    return p.getCpu();
  }

  @Override
  public synchronized void setMemoryThreshold(float memoryThreshold) {
    maybeInitBuilder();
    builder.setMemory(memoryThreshold);
  }

  @Override
  public synchronized void setCpuThreshold(float cpuThreshold) {
    maybeInitBuilder();
    builder.setCpu(cpuThreshold);
  }
}
