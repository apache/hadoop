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

import org.apache.hadoop.yarn.proto.YarnServerCommonProtos;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;

/**
 * Protocol Buffer implementation of OpportunisticContainersStatus.
 */
public class OpportunisticContainersStatusPBImpl
    extends OpportunisticContainersStatus {

  private YarnServerCommonProtos.OpportunisticContainersStatusProto proto =
      YarnServerCommonProtos.OpportunisticContainersStatusProto
          .getDefaultInstance();
  private YarnServerCommonProtos.OpportunisticContainersStatusProto.Builder
      builder = null;
  private boolean viaProto = false;

  public OpportunisticContainersStatusPBImpl() {
    builder =
        YarnServerCommonProtos.OpportunisticContainersStatusProto.newBuilder();
  }

  public OpportunisticContainersStatusPBImpl(YarnServerCommonProtos
      .OpportunisticContainersStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public YarnServerCommonProtos.OpportunisticContainersStatusProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = YarnServerCommonProtos.OpportunisticContainersStatusProto
          .newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int getRunningOpportContainers() {
    YarnServerCommonProtos.OpportunisticContainersStatusProtoOrBuilder p =
        viaProto ? proto : builder;
    return p.getRunningOpportContainers();
  }

  @Override
  public void setRunningOpportContainers(int runningOpportContainers) {
    maybeInitBuilder();
    builder.setRunningOpportContainers(runningOpportContainers);
  }

  @Override
  public long getOpportMemoryUsed() {
    YarnServerCommonProtos.OpportunisticContainersStatusProtoOrBuilder p =
        viaProto ? proto : builder;
    return p.getOpportMemoryUsed();
  }

  @Override
  public void setOpportMemoryUsed(long opportMemoryUsed) {
    maybeInitBuilder();
    builder.setOpportMemoryUsed(opportMemoryUsed);
  }

  @Override
  public int getOpportCoresUsed() {
    YarnServerCommonProtos.OpportunisticContainersStatusProtoOrBuilder p =
        viaProto ? proto : builder;
    return p.getOpportCoresUsed();
  }

  @Override
  public void setOpportCoresUsed(int opportCoresUsed) {
    maybeInitBuilder();
    builder.setOpportCoresUsed(opportCoresUsed);
  }

  @Override
  public int getQueuedOpportContainers() {
    YarnServerCommonProtos.OpportunisticContainersStatusProtoOrBuilder p =
        viaProto ? proto : builder;
    return p.getQueuedOpportContainers();
  }

  @Override
  public void setQueuedOpportContainers(int queuedOpportContainers) {
    maybeInitBuilder();
    builder.setQueuedOpportContainers(queuedOpportContainers);
  }

  @Override
  public int getWaitQueueLength() {
    YarnServerCommonProtos.OpportunisticContainersStatusProtoOrBuilder p =
        viaProto ? proto : builder;
    return p.getWaitQueueLength();
  }

  @Override
  public void setWaitQueueLength(int waitQueueLength) {
    maybeInitBuilder();
    builder.setWaitQueueLength(waitQueueLength);
  }

  @Override
  public int getEstimatedQueueWaitTime() {
    YarnServerCommonProtos.OpportunisticContainersStatusProtoOrBuilder p =
        viaProto ? proto : builder;
    return p.getEstimatedQueueWaitTime();
  }

  @Override
  public void setEstimatedQueueWaitTime(int queueWaitTime) {
    maybeInitBuilder();
    builder.setEstimatedQueueWaitTime(queueWaitTime);
  }

  @Override
  public int getOpportQueueCapacity() {
    YarnServerCommonProtos.OpportunisticContainersStatusProtoOrBuilder p =
        viaProto ? proto : builder;
    return p.getOpportQueueCapacity();
  }

  @Override
  public void setOpportQueueCapacity(int maxOpportQueueLength) {
    maybeInitBuilder();
    builder.setOpportQueueCapacity(maxOpportQueueLength);
  }
}
