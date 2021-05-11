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
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceUtilizationProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceUtilizationProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;

import java.util.Map;

@Private
@Unstable
public class ResourceUtilizationPBImpl extends ResourceUtilization {
  private ResourceUtilizationProto proto = ResourceUtilizationProto
      .getDefaultInstance();
  private ResourceUtilizationProto.Builder builder = null;
  private boolean viaProto = false;

  public ResourceUtilizationPBImpl() {
    builder = ResourceUtilizationProto.newBuilder();
  }

  public ResourceUtilizationPBImpl(ResourceUtilizationProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ResourceUtilizationProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceUtilizationProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int getPhysicalMemory() {
    ResourceUtilizationProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getPmem());
  }

  @Override
  public void setPhysicalMemory(int pmem) {
    maybeInitBuilder();
    builder.setPmem(pmem);
  }

  @Override
  public int getVirtualMemory() {
    ResourceUtilizationProtoOrBuilder p = viaProto ? proto : builder;
    return p.getVmem();
  }

  @Override
  public void setVirtualMemory(int vmem) {
    maybeInitBuilder();
    builder.setVmem(vmem);
  }

  @Override
  public float getCPU() {
    ResourceUtilizationProtoOrBuilder p = viaProto ? proto : builder;
    return p.getCpu();
  }

  @Override
  public void setCPU(float cpu) {
    maybeInitBuilder();
    builder.setCpu(cpu);
  }

  @Override
  public float getCustomResource(String resourceName) {
    return getCustomResources().get(resourceName);
  }

  @Override
  public Map<String, Float> getCustomResources() {
    ResourceUtilizationProtoOrBuilder p = viaProto ? proto : builder;
    return ProtoUtils.
        convertStringFloatMapProtoListToMap(p.
            getCustomResourcesList());
  }

  @Override
  public void setCustomResources(Map<String, Float> customResources) {
    if (customResources != null) {
      maybeInitBuilder();
      builder.addAllCustomResources(ProtoUtils.
          convertMapToStringFloatMapProtoList(customResources));
    }
  }

  @Override
  public int compareTo(ResourceUtilization other) {
    int diff = this.getPhysicalMemory() - other.getPhysicalMemory();
    if (diff == 0) {
      diff = this.getVirtualMemory() - other.getVirtualMemory();
      if (diff == 0) {
        diff = Float.compare(this.getCPU(), other.getCPU());
        if (diff == 0) {
          diff = this.getCustomResources().size() -
              other.getCustomResources().size();
          // todo how to compare custom resource in same size
        }
      }
    }
    return diff;
  }
}
