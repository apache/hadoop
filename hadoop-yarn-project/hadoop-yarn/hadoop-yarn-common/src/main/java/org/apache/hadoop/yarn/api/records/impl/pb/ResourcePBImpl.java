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
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ValueRanges;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ValueRangesProto;

@Private
@Unstable
public class ResourcePBImpl extends Resource {
  ResourceProto proto = ResourceProto.getDefaultInstance();
  ResourceProto.Builder builder = null;
  boolean viaProto = false;
  ValueRanges ports = null;
  
  public ResourcePBImpl() {
    builder = ResourceProto.newBuilder();
  }

  public ResourcePBImpl(ResourceProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ResourceProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.ports != null) {
      builder.setPorts(convertToProtoFormat(this.ports));
    }
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto){
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }


  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public int getMemory() {
    ResourceProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getMemory());
  }

  @Override
  public void setMemory(int memory) {
    maybeInitBuilder();
    builder.setMemory((memory));
  }

  @Override
  public int getVirtualCores() {
    ResourceProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getVirtualCores());
  }

  @Override
  public void setVirtualCores(int vCores) {
    maybeInitBuilder();
    builder.setVirtualCores((vCores));
  }

  @Override
  public int getGPUs() {
    ResourceProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getGPUs());
  }

  @Override
  public void setGPUs(int GPUs) {
    maybeInitBuilder();
    builder.setGPUs((GPUs));
  }

  @Override
  public long getGPUAttribute() {
    ResourceProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getGPUAttribute());
  }

  @Override
  public void setGPUAttribute(long GPUAttribute) {
    maybeInitBuilder();
    builder.setGPUAttribute((GPUAttribute));
  }

  @Override
  public void setPorts(ValueRanges ports) {
    maybeInitBuilder();
    if (ports == null) {
      builder.clearPorts();
    }
    this.ports = ports;
  }

  @Override
  public ValueRanges getPorts() {
    ResourceProtoOrBuilder p = viaProto ? proto : builder;
    if (this.ports != null) {
      return this.ports;
    }
    if (!p.hasPorts()) {
      return null;
    }
    this.ports = convertFromProtoFormat(p.getPorts());
    return this.ports;
  }

  @Override
  public int compareTo(Resource other) {
    int diff = this.getMemory() - other.getMemory();
    if (diff == 0) {
      diff = this.getVirtualCores() - other.getVirtualCores();
      if (diff == 0) {
        diff = this.getGPUs() - other.getGPUs();
      }
    }
    return diff;
  }

  private static ValueRanges convertFromProtoFormat( ValueRangesProto proto) {
    return new ValueRangesPBImpl(proto);
  }

  private ValueRangesProto convertToProtoFormat(ValueRanges m) {
    return ((ValueRangesPBImpl)m).getProto();
  }


}  
