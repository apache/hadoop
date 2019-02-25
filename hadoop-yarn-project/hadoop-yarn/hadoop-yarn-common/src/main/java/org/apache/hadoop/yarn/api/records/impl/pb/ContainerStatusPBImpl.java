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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerSubState;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ExecutionTypeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStatusProtoOrBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Private
@Unstable
public class ContainerStatusPBImpl extends ContainerStatus {
  ContainerStatusProto proto = ContainerStatusProto.getDefaultInstance();
  ContainerStatusProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerId containerId = null;
  private static final String HOST = "HOST";
  private static final String IPS = "IPS";
  private static final String PORTS = "PORTS";
  private Map<String, String> containerAttributes = new HashMap<>();


  public ContainerStatusPBImpl() {
    builder = ContainerStatusProto.newBuilder();
  }

  public ContainerStatusPBImpl(ContainerStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public synchronized ContainerStatusProto getProto() {
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
    StringBuilder sb = new StringBuilder();
    sb.append("ContainerStatus: [")
        .append("ContainerId: ").append(getContainerId()).append(", ")
        .append("ExecutionType: ").append(getExecutionType()).append(", ")
        .append("State: ").append(getState()).append(", ")
        .append("Capability: ").append(getCapability()).append(", ")
        .append("Diagnostics: ").append(getDiagnostics()).append(", ")
        .append("ExitStatus: ").append(getExitStatus()).append(", ")
        .append("IP: ").append(getIPs()).append(", ")
        .append("Host: ").append(getHost()).append(", ")
        .append("ExposedPorts: ").append(getExposedPorts()).append(", ")
        .append("ContainerSubState: ").append(getContainerSubState())
        .append("]");
    return sb.toString();
  }

  private void mergeLocalToBuilder() {
    if (containerId != null) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
    }
    if (containerAttributes != null && !containerAttributes.isEmpty()) {
      addContainerAttributesToProto();
    }
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ContainerStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addContainerAttributesToProto() {
    maybeInitBuilder();
    builder.clearContainerAttributes();
    if (containerAttributes == null) {
      return;
    }
    Iterable<YarnProtos.StringStringMapProto> iterable =
        new Iterable<YarnProtos.StringStringMapProto>() {

          @Override
          public Iterator<YarnProtos.StringStringMapProto> iterator() {
            return new Iterator<YarnProtos.StringStringMapProto>() {

              private Iterator<String> keyIter =
                  containerAttributes.keySet().iterator();

              @Override public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override public YarnProtos.StringStringMapProto next() {
                String key = keyIter.next();
                String value = containerAttributes.get(key);

                if (value == null) {
                  value = "";
                }

                return YarnProtos.StringStringMapProto.newBuilder().setKey(key)
                    .setValue((value)).build();
              }

              @Override public boolean hasNext() {
                return keyIter.hasNext();
              }
            };
          }
        };
    builder.addAllContainerAttributes(iterable);
  }

  private void initContainerAttributes() {
    ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    List<YarnProtos.StringStringMapProto> list = p.getContainerAttributesList();
    for (YarnProtos.StringStringMapProto c : list) {
      if (!containerAttributes.containsKey(c.getKey())) {
        this.containerAttributes.put(c.getKey(), c.getValue());
      }
    }
  }

  @Override
  public synchronized ExecutionType getExecutionType() {
    ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasExecutionType()) {
      return null;
    }
    return convertFromProtoFormat(p.getExecutionType());
  }

  @Override
  public synchronized void setExecutionType(ExecutionType executionType) {
    maybeInitBuilder();
    if (executionType == null) {
      builder.clearExecutionType();
      return;
    }
    builder.setExecutionType(convertToProtoFormat(executionType));
  }
  
  @Override
  public synchronized ContainerState getState() {
    ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasState()) {
      return null;
    }
    return convertFromProtoFormat(p.getState());
  }

  @Override
  public synchronized void setState(ContainerState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearState();
      return;
    }
    builder.setState(convertToProtoFormat(state));
  }

  @Override
  public synchronized ContainerSubState getContainerSubState() {
    ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasContainerSubState()) {
      return null;
    }
    return ProtoUtils.convertFromProtoFormat(p.getContainerSubState());
  }

  @Override
  public synchronized void setContainerSubState(ContainerSubState subState) {
    maybeInitBuilder();
    if (subState == null) {
      builder.clearContainerSubState();
      return;
    }
    builder.setContainerSubState(ProtoUtils.convertToProtoFormat(subState));
  }

  @Override
  public synchronized ContainerId getContainerId() {
    ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerId != null) {
      return this.containerId;
    }
    if (!p.hasContainerId()) {
      return null;
    }
    this.containerId =  convertFromProtoFormat(p.getContainerId());
    return this.containerId;
  }

  @Override
  public synchronized void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null) 
      builder.clearContainerId();
    this.containerId = containerId;
  }
  @Override
  public synchronized int getExitStatus() {
    ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    return p.getExitStatus();
  }

  @Override
  public synchronized void setExitStatus(int exitStatus) {
    maybeInitBuilder();
    builder.setExitStatus(exitStatus);
  }

  @Override
  public synchronized String getDiagnostics() {
    ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getDiagnostics());    
  }

  @Override
  public synchronized void setDiagnostics(String diagnostics) {
    maybeInitBuilder();
    builder.setDiagnostics(diagnostics);
  }

  @Override
  public synchronized Resource getCapability() {
    ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasCapability()) {
      return null;
    }
    return convertFromProtoFormat(p.getCapability());
  }

  @Override
  public synchronized void setCapability(Resource capability) {
    maybeInitBuilder();
    if (capability == null) {
      builder.clearCapability();
      return;
    }
    builder.setCapability(convertToProtoFormat(capability));
  }

  @Override
  public synchronized List<String> getIPs() {
    if (!containerAttributes.containsKey(IPS)) {
      initContainerAttributes();
    }
    String ips = containerAttributes.get((IPS));
    return ips == null ? null :  Arrays.asList(ips.split(","));
  }

  @Override
  public synchronized void setIPs(List<String> ips) {
    maybeInitBuilder();
    if (ips == null) {
      containerAttributes.remove(IPS);
      addContainerAttributesToProto();
      return;
    }
    containerAttributes.put(IPS, StringUtils.join(",", ips));
  }

  @Override
  public synchronized String getExposedPorts() {
    if (!containerAttributes.containsKey(PORTS)) {
      initContainerAttributes();
    }
    String ports = containerAttributes.get((PORTS));
    return ports == null ? "" : ports;
  }

  @Override
  public synchronized void setExposedPorts(String ports) {
    maybeInitBuilder();
    if (ports == null) {
      containerAttributes.remove(PORTS);
      return;
    }
    containerAttributes.put(PORTS, ports);
  }

  @Override
  public synchronized String getHost() {
    if (containerAttributes.get(HOST) == null) {
      initContainerAttributes();
    }
    return containerAttributes.get(HOST);
  }

  @Override
  public synchronized void setHost(String host) {
    maybeInitBuilder();
    if (host == null) {
      containerAttributes.remove(HOST);
      return;
    }
    containerAttributes.put(HOST, host);
  }

  private ContainerStateProto convertToProtoFormat(ContainerState e) {
    return ProtoUtils.convertToProtoFormat(e);
  }

  private ContainerState convertFromProtoFormat(ContainerStateProto e) {
    return ProtoUtils.convertFromProtoFormat(e);
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }

  private ExecutionType convertFromProtoFormat(ExecutionTypeProto e) {
    return ProtoUtils.convertFromProtoFormat(e);
  }

  private ExecutionTypeProto convertToProtoFormat(ExecutionType e) {
    return ProtoUtils.convertToProtoFormat(e);
  }

  private ResourceProto convertToProtoFormat(Resource e) {
    return ProtoUtils.convertToProtoFormat(e);
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }
}
