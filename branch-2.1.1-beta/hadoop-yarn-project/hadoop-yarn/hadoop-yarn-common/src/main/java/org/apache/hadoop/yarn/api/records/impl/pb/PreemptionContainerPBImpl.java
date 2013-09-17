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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PreemptionContainerProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PreemptionContainerProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class PreemptionContainerPBImpl extends PreemptionContainer {

  PreemptionContainerProto proto =
    PreemptionContainerProto.getDefaultInstance();
  PreemptionContainerProto.Builder builder = null;

  boolean viaProto = false;
  private ContainerId id;

  public PreemptionContainerPBImpl() {
    builder = PreemptionContainerProto.newBuilder();
  }

  public PreemptionContainerPBImpl(PreemptionContainerProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized PreemptionContainerProto getProto() {
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

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (id != null) {
      builder.setId(convertToProtoFormat(id));
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = PreemptionContainerProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized ContainerId getId() {
    PreemptionContainerProtoOrBuilder p = viaProto ? proto : builder;
    if (id != null) {
      return id;
    }
    if (!p.hasId()) {
      return null;
    }
    id = convertFromProtoFormat(p.getId());
    return id;
  }

  @Override
  public synchronized void setId(final ContainerId id) {
    maybeInitBuilder();
    if (null == id) {
      builder.clearId();
    }
    this.id = id;
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }

}
