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

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;

/**
 * Implementation of <code>UpdatedContainer</code>.
 */
public class UpdatedContainerPBImpl extends UpdatedContainer {
  private YarnServiceProtos.UpdatedContainerProto proto =
      YarnServiceProtos.UpdatedContainerProto.getDefaultInstance();
  private YarnServiceProtos.UpdatedContainerProto.Builder builder = null;
  private boolean viaProto = false;

  private Container container = null;

  public UpdatedContainerPBImpl() {
    builder = YarnServiceProtos.UpdatedContainerProto.newBuilder();
  }

  public UpdatedContainerPBImpl(YarnServiceProtos.UpdatedContainerProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.container != null) {
      builder.setContainer(ProtoUtils.convertToProtoFormat(this.container));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = YarnServiceProtos.UpdatedContainerProto.newBuilder(proto);
    }
    viaProto = false;
  }

  public YarnServiceProtos.UpdatedContainerProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public ContainerUpdateType getUpdateType() {
    YarnServiceProtos.UpdatedContainerProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasUpdateType()) {
      return null;
    }
    return ProtoUtils.convertFromProtoFormat(p.getUpdateType());
  }

  @Override
  public void setUpdateType(ContainerUpdateType updateType) {
    maybeInitBuilder();
    if (updateType == null) {
      builder.clearUpdateType();
      return;
    }
    builder.setUpdateType(ProtoUtils.convertToProtoFormat(updateType));
  }

  @Override
  public Container getContainer() {
    YarnServiceProtos.UpdatedContainerProtoOrBuilder p =
        viaProto ? proto : builder;
    if (this.container != null) {
      return this.container;
    }
    if (!p.hasContainer()) {
      return null;
    }
    this.container = ProtoUtils.convertFromProtoFormat(p.getContainer());
    return this.container;
  }

  @Override
  public void setContainer(Container container) {
    maybeInitBuilder();
    if (container == null) {
      builder.clearContainer();
    }
    this.container = container;
  }
}
