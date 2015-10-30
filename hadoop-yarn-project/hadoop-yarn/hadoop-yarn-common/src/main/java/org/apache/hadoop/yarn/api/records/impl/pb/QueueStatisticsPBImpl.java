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

package org.apache.hadoop.yarn.api.records.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueStatisticsProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueStatisticsProtoOrBuilder;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class QueueStatisticsPBImpl extends QueueStatistics {

  QueueStatisticsProto proto = QueueStatisticsProto.getDefaultInstance();
  QueueStatisticsProto.Builder builder = null;
  boolean viaProto = false;

  public QueueStatisticsPBImpl() {
    builder = QueueStatisticsProto.newBuilder();
  }

  public QueueStatisticsPBImpl(QueueStatisticsProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public QueueStatisticsProto getProto() {
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

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = QueueStatisticsProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public long getNumAppsSubmitted() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumAppsSubmitted()) ? p.getNumAppsSubmitted() : -1;
  }

  @Override
  public void setNumAppsSubmitted(long numAppsSubmitted) {
    maybeInitBuilder();
    builder.setNumAppsSubmitted(numAppsSubmitted);
  }

  @Override
  public long getNumAppsRunning() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumAppsRunning()) ? p.getNumAppsRunning() : -1;
  }

  @Override
  public void setNumAppsRunning(long numAppsRunning) {
    maybeInitBuilder();
    builder.setNumAppsRunning(numAppsRunning);
  }

  @Override
  public long getNumAppsPending() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumAppsPending()) ? p.getNumAppsPending() : -1;
  }

  @Override
  public void setNumAppsPending(long numAppsPending) {
    maybeInitBuilder();
    builder.setNumAppsPending(numAppsPending);
  }

  @Override
  public long getNumAppsCompleted() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumAppsCompleted()) ? p.getNumAppsCompleted() : -1;
  }

  @Override
  public void setNumAppsCompleted(long numAppsCompleted) {
    maybeInitBuilder();
    builder.setNumAppsCompleted(numAppsCompleted);
  }

  @Override
  public long getNumAppsKilled() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumAppsKilled()) ? p.getNumAppsKilled() : -1;
  }

  @Override
  public void setNumAppsKilled(long numAppsKilled) {
    maybeInitBuilder();
    builder.setNumAppsKilled(numAppsKilled);
  }

  @Override
  public long getNumAppsFailed() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumAppsFailed()) ? p.getNumAppsFailed() : -1;
  }

  @Override
  public void setNumAppsFailed(long numAppsFailed) {
    maybeInitBuilder();
    builder.setNumAppsFailed(numAppsFailed);
  }

  @Override
  public long getNumActiveUsers() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumActiveUsers()) ? p.getNumActiveUsers() : -1;
  }

  @Override
  public void setNumActiveUsers(long numActiveUsers) {
    maybeInitBuilder();
    builder.setNumActiveUsers(numActiveUsers);
  }

  @Override
  public long getAvailableMemoryMB() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasAvailableMemoryMB()) ? p.getAvailableMemoryMB() : -1;
  }

  @Override
  public void setAvailableMemoryMB(long availableMemoryMB) {
    maybeInitBuilder();
    builder.setAvailableMemoryMB(availableMemoryMB);
  }

  @Override
  public long getAllocatedMemoryMB() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasAllocatedMemoryMB()) ? p.getAllocatedMemoryMB() : -1;
  }

  @Override
  public void setAllocatedMemoryMB(long allocatedMemoryMB) {
    maybeInitBuilder();
    builder.setAllocatedMemoryMB(allocatedMemoryMB);
  }

  @Override
  public long getPendingMemoryMB() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasPendingMemoryMB()) ? p.getPendingMemoryMB() : -1;
  }

  @Override
  public void setPendingMemoryMB(long pendingMemoryMB) {
    maybeInitBuilder();
    builder.setPendingMemoryMB(pendingMemoryMB);
  }

  @Override
  public long getReservedMemoryMB() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasReservedMemoryMB()) ? p.getReservedMemoryMB() : -1;
  }

  @Override
  public void setReservedMemoryMB(long reservedMemoryMB) {
    maybeInitBuilder();
    builder.setReservedMemoryMB(reservedMemoryMB);
  }

  @Override
  public long getAvailableVCores() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasAvailableVCores()) ? p.getAvailableVCores() : -1;
  }

  @Override
  public void setAvailableVCores(long availableVCores) {
    maybeInitBuilder();
    builder.setAvailableVCores(availableVCores);
  }

  @Override
  public long getAllocatedVCores() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasAllocatedVCores()) ? p.getAllocatedVCores() : -1;
  }

  @Override
  public void setAllocatedVCores(long allocatedVCores) {
    maybeInitBuilder();
    builder.setAllocatedVCores(allocatedVCores);
  }

  @Override
  public long getPendingVCores() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasPendingVCores()) ? p.getPendingVCores() : -1;
  }

  @Override
  public void setPendingVCores(long pendingVCores) {
    maybeInitBuilder();
    builder.setPendingVCores(pendingVCores);
  }

  @Override
  public long getReservedVCores() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasReservedVCores()) ? p.getReservedVCores() : -1;
  }

  @Override
  public void setReservedVCores(long reservedVCores) {
    maybeInitBuilder();
    builder.setReservedVCores(reservedVCores);
  }

  @Override
  public long getPendingContainers() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasPendingContainers()) ? p.getPendingContainers() : -1;
  }

  @Override
  public void setPendingContainers(long pendingContainers) {
    maybeInitBuilder();
    builder.setPendingContainers(pendingContainers);
  }

  @Override
  public long getAllocatedContainers() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasAllocatedContainers()) ? p.getAllocatedContainers() : -1;
  }

  @Override
  public void setAllocatedContainers(long allocatedContainers) {
    maybeInitBuilder();
    builder.setAllocatedContainers(allocatedContainers);
  }

  @Override
  public long getReservedContainers() {
    QueueStatisticsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasReservedContainers()) ? p.getReservedContainers() : -1;
  }

  @Override
  public void setReservedContainers(long reservedContainers) {
    maybeInitBuilder();
    builder.setReservedContainers(reservedContainers);
  }
}
