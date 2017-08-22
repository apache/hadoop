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

  Resource usedResources;
  Resource reservedResources;
  Resource neededResources;

  private Map<String, Long> resourceSecondsMap;
  private Map<String, Long> preemptedResourceSecondsMap;

  public ApplicationResourceUsageReportPBImpl() {
    builder = ApplicationResourceUsageReportProto.newBuilder();
  }

  public ApplicationResourceUsageReportPBImpl(
      ApplicationResourceUsageReportProto proto) {
    this.proto = proto;
    viaProto = true;
    getResourceSecondsMap();
    getPreemptedResourceSecondsMap();
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
    if (this.usedResources != null) {
      builder.setUsedResources(convertToProtoFormat(this.usedResources));
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

    if (preemptedResourceSecondsMap != null && !preemptedResourceSecondsMap
        .isEmpty()) {
      builder.addAllApplicationPreemptedResourceUsageMap(ProtoUtils
          .convertMapToStringLongMapProtoList(preemptedResourceSecondsMap));
    }
    if (resourceSecondsMap != null && !resourceSecondsMap.isEmpty()) {
      builder.addAllApplicationResourceUsageMap(
          ProtoUtils.convertMapToStringLongMapProtoList(resourceSecondsMap));
    }

    builder.setMemorySeconds(this.getMemorySeconds());
    builder.setVcoreSeconds(this.getVcoreSeconds());
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
  public synchronized Resource getUsedResources() {
    ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.usedResources != null) {
      return this.usedResources;
    }
    if (!p.hasUsedResources()) {
      return null;
    }
    this.usedResources = convertFromProtoFormat(p.getUsedResources());
    return this.usedResources;
  }

  @Override
  public synchronized void setUsedResources(Resource resources) {
    maybeInitBuilder();
    if (resources == null)
      builder.clearUsedResources();
    this.usedResources = resources;
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
  public synchronized void setMemorySeconds(long memory_seconds) {
    getResourceSecondsMap()
        .put(ResourceInformation.MEMORY_MB.getName(), memory_seconds);
  }

  @Override
  public synchronized long getMemorySeconds() {
    Map<String, Long> tmp = getResourceSecondsMap();
    if (tmp.containsKey(ResourceInformation.MEMORY_MB.getName())) {
      return tmp.get(ResourceInformation.MEMORY_MB.getName());
    }
    return 0;
  }

  @Override
  public synchronized void setVcoreSeconds(long vcore_seconds) {
    getResourceSecondsMap()
        .put(ResourceInformation.VCORES.getName(), vcore_seconds);
  }

  @Override
  public synchronized long getVcoreSeconds() {
    Map<String, Long> tmp = getResourceSecondsMap();
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
  public synchronized void setResourceSecondsMap(
      Map<String, Long> resourceSecondsMap) {
    this.resourceSecondsMap = resourceSecondsMap;
    if (resourceSecondsMap == null) {
      return;
    }
    if (!resourceSecondsMap
        .containsKey(ResourceInformation.MEMORY_MB.getName())) {
      this.setMemorySeconds(0L);
    }
    if (!resourceSecondsMap.containsKey(ResourceInformation.VCORES.getName())) {
      this.setVcoreSeconds(0L);
    }
  }

  @Override
  public synchronized Map<String, Long> getResourceSecondsMap() {
    if (this.resourceSecondsMap != null) {
      return this.resourceSecondsMap;
    }
    ApplicationResourceUsageReportProtoOrBuilder p = viaProto ? proto : builder;
    this.resourceSecondsMap = ProtoUtils
        .convertStringLongMapProtoListToMap(
            p.getApplicationResourceUsageMapList());
    if (!this.resourceSecondsMap
        .containsKey(ResourceInformation.MEMORY_MB.getName())) {
      this.setMemorySeconds(p.getMemorySeconds());
    }
    if (!this.resourceSecondsMap
        .containsKey(ResourceInformation.VCORES.getName())) {
      this.setVcoreSeconds(p.getVcoreSeconds());
    }
    this.setMemorySeconds(p.getMemorySeconds());
    this.setVcoreSeconds(p.getVcoreSeconds());
    return this.resourceSecondsMap;
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
}
