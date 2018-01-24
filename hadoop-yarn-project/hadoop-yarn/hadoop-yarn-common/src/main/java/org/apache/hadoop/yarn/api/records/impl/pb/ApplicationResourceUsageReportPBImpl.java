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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationResourceUsageReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationResourceUsageReportProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;

import com.google.protobuf.TextFormat;

import java.util.Map;

@Private
@Unstable
public class ApplicationResourceUsageReportPBImpl 
extends ApplicationResourceUsageReport {
  ApplicationResourceUsageReportProto proto = 
      ApplicationResourceUsageReportProto.getDefaultInstance();
  ApplicationResourceUsageReportProto.Builder builder = null;
  boolean viaProto = false;

  private Resource guaranteedResourceUsed;
  private Resource reservedResources;
  private Resource neededResources;
  private Resource opportunisticResourceUsed;

  private Map<String, Long> guaranteedResourceSecondsMap;
  private Map<String, Long> preemptedResourceSecondsMap;
  private Map<String, Long> opportunisticResourceSecondsMap;

  public ApplicationResourceUsageReportPBImpl() {
    builder = ApplicationResourceUsageReportProto.newBuilder();
  }

  public ApplicationResourceUsageReportPBImpl(
      ApplicationResourceUsageReportProto proto) {
    this.proto = proto;
    viaProto = true;
    getGuaranteedResourceSecondsMap();
    getPreemptedResourceSecondsMap();
    getOpportunisticResourceSecondsMap();
  }

  public synchronized ApplicationResourceUsageReportProto getProto() {
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
    if (this.guaranteedResourceUsed != null) {
      builder.setUsedResources(
          convertToProtoFormat(this.guaranteedResourceUsed));
    }
    if (this.opportunisticResourceUsed != null) {
      builder.setUsedOpportunisticResources(
          convertToProtoFormat(this.opportunisticResourceUsed));
    }
    if (this.reservedResources != null) {
      builder.setReservedResources(
          convertToProtoFormat(this.reservedResources));
    }
    if (this.neededResources != null) {
      builder.setNeededResources(convertToProtoFormat(this.neededResources));
    }
    builder.clearApplicationResourceUsageMap();
    builder.clearApplicationPreemptedResourceUsageMap();
    builder.clearApplicationOpportunisticResourceUsageMap();

    if (preemptedResourceSecondsMap != null && !preemptedResourceSecondsMap
        .isEmpty()) {
      builder.addAllApplicationPreemptedResourceUsageMap(ProtoUtils
          .convertMapToStringLongMapProtoList(preemptedResourceSecondsMap));
    }
    if (guaranteedResourceSecondsMap != null &&
        !guaranteedResourceSecondsMap.isEmpty()) {
      builder.addAllApplicationResourceUsageMap(
          ProtoUtils.convertMapToStringLongMapProtoList(
              guaranteedResourceSecondsMap));
    }
    if (opportunisticResourceSecondsMap != null &&
        !opportunisticResourceSecondsMap.isEmpty()) {
      builder.addAllApplicationOpportunisticResourceUsageMap(
          ProtoUtils.convertMapToStringLongMapProtoList(
              opportunisticResourceSecondsMap));
    }

    builder.setMemorySeconds(this.getGuaranteedMemorySeconds());
    builder.setVcoreSeconds(this.getGuaranteedVcoreSeconds());
    builder.setPreemptedMemorySeconds(this.getPreemptedMemorySeconds());
    builder.setPreemptedVcoreSeconds(this.getPreemptedVcoreSeconds());
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ApplicationResourceUsageReportProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized int getNumUsedContainers() {
    ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getNumUsedContainers());
  }

  @Override
  public synchronized void setNumUsedContainers(int num_containers) {
    maybeInitBuilder();
    builder.setNumUsedContainers((num_containers));
  }

  @Override
  public synchronized int getNumReservedContainers() {
    ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getNumReservedContainers());
  }

  @Override
  public synchronized void setNumReservedContainers(
      int num_reserved_containers) {
    maybeInitBuilder();
    builder.setNumReservedContainers((num_reserved_containers));
  }

  @Override
  @Deprecated
  public synchronized Resource getUsedResources() {
    return getGuaranteedResourcesUsed();
  }

  @Override
  public synchronized Resource getGuaranteedResourcesUsed() {
    ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.guaranteedResourceUsed != null) {
      return this.guaranteedResourceUsed;
    }
    if (!p.hasUsedResources()) {
      return null;
    }
    this.guaranteedResourceUsed = convertFromProtoFormat(p.getUsedResources());
    return this.guaranteedResourceUsed;
  }

  @Override
  public synchronized void setGuaranteedResourcesUsed(Resource resources) {
    maybeInitBuilder();
    if (resources == null)
      builder.clearUsedResources();
    this.guaranteedResourceUsed = resources;
  }

  @Override
  public synchronized Resource getOpportunisticResourcesUsed() {
    ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.opportunisticResourceUsed != null) {
      return this.opportunisticResourceUsed;
    }
    if (!p.hasUsedOpportunisticResources()) {
      return null;
    }
    this.opportunisticResourceUsed =
        convertFromProtoFormat(p.getUsedOpportunisticResources());
    return this.opportunisticResourceUsed;
  }

  @Override
  public synchronized void setOpportunisticResourcesUsed(Resource resources) {
    maybeInitBuilder();
    if (resources == null) {
      builder.clearUsedOpportunisticResources();
    }
    this.opportunisticResourceUsed = resources;
  }

  @Override
  public synchronized Resource getReservedResources() {
    ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.reservedResources != null) {
        return this.reservedResources;
    }
    if (!p.hasReservedResources()) {
      return null;
    }
    this.reservedResources = convertFromProtoFormat(p.getReservedResources());
    return this.reservedResources;
  }

  @Override
  public synchronized void setReservedResources(Resource reserved_resources) {
    maybeInitBuilder();
    if (reserved_resources == null)
      builder.clearReservedResources();
    this.reservedResources = reserved_resources;
  }

  @Override
  public synchronized Resource getNeededResources() {
    ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.neededResources != null) {
        return this.neededResources;
    }
    if (!p.hasNeededResources()) {
      return null;
    }
    this.neededResources = convertFromProtoFormat(p.getNeededResources());
    return this.neededResources;
  }

  @Override
  public synchronized void setNeededResources(Resource reserved_resources) {
    maybeInitBuilder();
    if (reserved_resources == null)
      builder.clearNeededResources();
    this.neededResources = reserved_resources;
  }

  @Override
  public synchronized void setGuaranteedMemorySeconds(long memorySeconds) {
    getGuaranteedResourceSecondsMap()
        .put(ResourceInformation.MEMORY_MB.getName(), memorySeconds);
  }

  @Override
  public synchronized long getGuaranteedMemorySeconds() {
    Map<String, Long> tmp = getGuaranteedResourceSecondsMap();
    if (tmp.containsKey(ResourceInformation.MEMORY_MB.getName())) {
      return tmp.get(ResourceInformation.MEMORY_MB.getName());
    }
    return 0;
  }

  @Override
  @Deprecated
  public synchronized long getMemorySeconds() {
    return getGuaranteedMemorySeconds();
  }

  @Override
  public synchronized void setGuaranteedVcoreSeconds(long vcoreSeconds) {
    getGuaranteedResourceSecondsMap()
        .put(ResourceInformation.VCORES.getName(), vcoreSeconds);
  }

  @Override
  public synchronized long getGuaranteedVcoreSeconds() {
    Map<String, Long> tmp = getGuaranteedResourceSecondsMap();
    if (tmp.containsKey(ResourceInformation.VCORES.getName())) {
      return tmp.get(ResourceInformation.VCORES.getName());
    }
    return 0;
  }

  @Override
  @Deprecated
  public synchronized long getVcoreSeconds() {
    return getGuaranteedVcoreSeconds();
  }

  @Override
  public synchronized long getOpportunisticMemorySeconds() {
    Map<String, Long> tmp = getOpportunisticResourceSecondsMap();
    if (tmp.containsKey(ResourceInformation.MEMORY_MB.getName())) {
      return tmp.get(ResourceInformation.MEMORY_MB.getName());
    }
    return 0;
  }

  @Override
  public synchronized long getOpportunisticVcoreSeconds() {
    Map<String, Long> tmp = getOpportunisticResourceSecondsMap();
    if (tmp.containsKey(ResourceInformation.VCORES.getName())) {
      return tmp.get(ResourceInformation.VCORES.getName());
    }
    return 0;
  }

  @Override
  public synchronized void setPreemptedMemorySeconds(
      long preemptedMemorySeconds) {
    getPreemptedResourceSecondsMap()
        .put(ResourceInformation.MEMORY_MB.getName(), preemptedMemorySeconds);
  }

  @Override
  public synchronized long getPreemptedMemorySeconds() {
    Map<String, Long> tmp = getPreemptedResourceSecondsMap();
    if (tmp.containsKey(ResourceInformation.MEMORY_MB.getName())) {
      return tmp.get(ResourceInformation.MEMORY_MB.getName());
    }
    return 0;
  }

  @Override
  public synchronized void setPreemptedVcoreSeconds(
      long vcoreSeconds) {
    getPreemptedResourceSecondsMap()
        .put(ResourceInformation.VCORES.getName(), vcoreSeconds);
  }

  @Override
  public synchronized long getPreemptedVcoreSeconds() {
    Map<String, Long> tmp = getPreemptedResourceSecondsMap();
    if (tmp.containsKey(ResourceInformation.VCORES.getName())) {
      return tmp.get(ResourceInformation.VCORES.getName());
    }
    return 0;
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ProtoUtils.convertToProtoFormat(t);
  }

  @Override
  public synchronized float getQueueUsagePercentage() {
    ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getQueueUsagePercentage());
  }

  @Override
  public synchronized void setQueueUsagePercentage(float queueUsagePerc) {
    maybeInitBuilder();
    builder.setQueueUsagePercentage((queueUsagePerc));
  }

  @Override
  public synchronized float getClusterUsagePercentage() {
    ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getClusterUsagePercentage());
  }

  @Override
  public synchronized void setClusterUsagePercentage(float clusterUsagePerc) {
    maybeInitBuilder();
    builder.setClusterUsagePercentage((clusterUsagePerc));
  }

  @Override
  public synchronized void setGuaranteedResourceSecondsMap(
      Map<String, Long> resourceSecondsMap) {
    this.guaranteedResourceSecondsMap = resourceSecondsMap;
    if (resourceSecondsMap == null) {
      return;
    }
    if (!resourceSecondsMap
        .containsKey(ResourceInformation.MEMORY_MB.getName())) {
      this.setGuaranteedMemorySeconds(0L);
    }
    if (!resourceSecondsMap.containsKey(ResourceInformation.VCORES.getName())) {
      this.setGuaranteedVcoreSeconds(0L);
    }
  }

  @Override
  @Deprecated
  public synchronized Map<String, Long> getResourceSecondsMap() {
    return getGuaranteedResourceSecondsMap();
  }

  @Override
  public synchronized Map<String, Long> getGuaranteedResourceSecondsMap() {
    if (this.guaranteedResourceSecondsMap != null) {
      return this.guaranteedResourceSecondsMap;
    }
    ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
    this.guaranteedResourceSecondsMap = ProtoUtils
        .convertStringLongMapProtoListToMap(
            p.getApplicationResourceUsageMapList());
    if (!this.guaranteedResourceSecondsMap
        .containsKey(ResourceInformation.MEMORY_MB.getName())) {
      this.setGuaranteedMemorySeconds(p.getMemorySeconds());
    }
    if (!this.guaranteedResourceSecondsMap
        .containsKey(ResourceInformation.VCORES.getName())) {
      this.setGuaranteedVcoreSeconds(p.getVcoreSeconds());
    }
    this.setGuaranteedMemorySeconds(p.getMemorySeconds());
    this.setGuaranteedVcoreSeconds(p.getVcoreSeconds());
    return this.guaranteedResourceSecondsMap;
  }

  @Override
  public synchronized void setPreemptedResourceSecondsMap(
      Map<String, Long> preemptedResourceSecondsMap) {
    this.preemptedResourceSecondsMap = preemptedResourceSecondsMap;
    if (preemptedResourceSecondsMap == null) {
      return;
    }
    if (!preemptedResourceSecondsMap
        .containsKey(ResourceInformation.MEMORY_MB.getName())) {
      this.setPreemptedMemorySeconds(0L);
    }
    if (!preemptedResourceSecondsMap
        .containsKey(ResourceInformation.VCORES.getName())) {
      this.setPreemptedVcoreSeconds(0L);
    }
  }

  @Override
  public synchronized Map<String, Long> getPreemptedResourceSecondsMap() {
    if (this.preemptedResourceSecondsMap != null) {
      return this.preemptedResourceSecondsMap;
    }
    ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
    this.preemptedResourceSecondsMap = ProtoUtils
        .convertStringLongMapProtoListToMap(
            p.getApplicationPreemptedResourceUsageMapList());
    if (!this.preemptedResourceSecondsMap
        .containsKey(ResourceInformation.MEMORY_MB.getName())) {
      this.setPreemptedMemorySeconds(p.getPreemptedMemorySeconds());
    }
    if (!this.preemptedResourceSecondsMap
        .containsKey(ResourceInformation.VCORES.getName())) {
      this.setPreemptedVcoreSeconds(p.getPreemptedVcoreSeconds());
    }
    this.setPreemptedMemorySeconds(p.getPreemptedMemorySeconds());
    this.setPreemptedVcoreSeconds(p.getPreemptedVcoreSeconds());
    return this.preemptedResourceSecondsMap;
  }

  @Override
  public synchronized Map<String, Long> getOpportunisticResourceSecondsMap() {
    if (this.opportunisticResourceSecondsMap != null) {
      return this.opportunisticResourceSecondsMap;
    }
    ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
    this.opportunisticResourceSecondsMap = ProtoUtils
        .convertStringLongMapProtoListToMap(
            p.getApplicationOpportunisticResourceUsageMapList());
    if (!opportunisticResourceSecondsMap.containsKey(
        ResourceInformation.MEMORY_MB.getName())) {
      this.opportunisticResourceSecondsMap.put(
          ResourceInformation.MEMORY_MB.getName(), 0L);
    }
    if (!opportunisticResourceSecondsMap.containsKey(
        ResourceInformation.VCORES.getName())) {
      this.opportunisticResourceSecondsMap.put(
          ResourceInformation.VCORES.getName(), 0L);
    }
    return this.opportunisticResourceSecondsMap;
  }

  @Override
  public synchronized void setOpportunisticResourceSecondsMap(
      Map<String, Long> opportunisticResourceSecondsMap) {
    this.opportunisticResourceSecondsMap = opportunisticResourceSecondsMap;
    if (opportunisticResourceSecondsMap == null) {
      return;
    }
    if (!opportunisticResourceSecondsMap
        .containsKey(ResourceInformation.MEMORY_MB.getName())) {
      this.opportunisticResourceSecondsMap.put(
          ResourceInformation.MEMORY_MB.getName(), 0L);
    }
    if (!opportunisticResourceSecondsMap
        .containsKey(ResourceInformation.VCORES.getName())) {
      this.opportunisticResourceSecondsMap.put(
          ResourceInformation.VCORES.getName(), 0L);
    }
  }
}
