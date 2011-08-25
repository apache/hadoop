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


import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerTokenProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerTokenProtoOrBuilder;


    
public class ContainerTokenPBImpl extends ProtoBase<ContainerTokenProto> implements ContainerToken {
  private ContainerTokenProto proto = ContainerTokenProto.getDefaultInstance();
  private ContainerTokenProto.Builder builder = null;
  private boolean viaProto = false;
  
  private ByteBuffer identifier;
  private ByteBuffer password;
  
  
  public ContainerTokenPBImpl() {
    builder = ContainerTokenProto.newBuilder();
  }

  public ContainerTokenPBImpl(ContainerTokenProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public synchronized ContainerTokenProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.identifier != null) {
      builder.setIdentifier(convertToProtoFormat(this.identifier));
    }
    if (this.password != null) {
      builder.setPassword(convertToProtoFormat(this.password));
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
      builder = ContainerTokenProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public synchronized ByteBuffer getIdentifier() {
    ContainerTokenProtoOrBuilder p = viaProto ? proto : builder;
    if (this.identifier != null) {
      return this.identifier;
    }
    if (!p.hasIdentifier()) {
      return null;
    }
    this.identifier = convertFromProtoFormat(p.getIdentifier());
    return this.identifier;
  }

  @Override
  public synchronized void setIdentifier(ByteBuffer identifier) {
    maybeInitBuilder();
    if (identifier == null) 
      builder.clearIdentifier();
    this.identifier = identifier;
  }
  @Override
  public synchronized ByteBuffer getPassword() {
    ContainerTokenProtoOrBuilder p = viaProto ? proto : builder;
    if (this.password != null) {
      return this.password;
    }
    if (!p.hasPassword()) {
      return null;
    }
    this.password =  convertFromProtoFormat(p.getPassword());
    return this.password;
  }

  @Override
  public synchronized void setPassword(ByteBuffer password) {
    maybeInitBuilder();
    if (password == null) 
      builder.clearPassword();
    this.password = password;
  }
  @Override
  public synchronized String getKind() {
    ContainerTokenProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasKind()) {
      return null;
    }
    return (p.getKind());
  }

  @Override
  public synchronized void setKind(String kind) {
    maybeInitBuilder();
    if (kind == null) {
      builder.clearKind();
      return;
    }
    builder.setKind((kind));
  }
  @Override
  public synchronized String getService() {
    ContainerTokenProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasService()) {
      return null;
    }
    return (p.getService());
  }

  @Override
  public synchronized void setService(String service) {
    maybeInitBuilder();
    if (service == null) {
      builder.clearService();
      return;
    }
    builder.setService((service));
  }

}  
