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

import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.proto.YarnProtos.CollectorInfoProto;
import org.apache.hadoop.yarn.proto.YarnProtos.CollectorInfoProtoOrBuilder;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * Protocol record implementation of {@link CollectorInfo}.
 */
public class CollectorInfoPBImpl extends CollectorInfo {

  private CollectorInfoProto proto = CollectorInfoProto.getDefaultInstance();

  private CollectorInfoProto.Builder builder = null;
  private boolean viaProto = false;

  private String collectorAddr = null;
  private Token collectorToken = null;


  public CollectorInfoPBImpl() {
    builder = CollectorInfoProto.newBuilder();
  }

  public CollectorInfoPBImpl(CollectorInfoProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public CollectorInfoProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = CollectorInfoProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public String getCollectorAddr() {
    CollectorInfoProtoOrBuilder p = viaProto ? proto : builder;
    if (this.collectorAddr == null && p.hasCollectorAddr()) {
      this.collectorAddr = p.getCollectorAddr();
    }
    return this.collectorAddr;
  }

  @Override
  public void setCollectorAddr(String addr) {
    maybeInitBuilder();
    if (collectorAddr == null) {
      builder.clearCollectorAddr();
    }
    this.collectorAddr = addr;
  }

  @Override
  public Token getCollectorToken() {
    CollectorInfoProtoOrBuilder p = viaProto ? proto : builder;
    if (this.collectorToken != null) {
      return this.collectorToken;
    }
    if (!p.hasCollectorToken()) {
      return null;
    }
    this.collectorToken = convertFromProtoFormat(p.getCollectorToken());
    return this.collectorToken;
  }

  @Override
  public void setCollectorToken(Token token) {
    maybeInitBuilder();
    if (token == null) {
      builder.clearCollectorToken();
    }
    this.collectorToken = token;
  }

  private TokenPBImpl convertFromProtoFormat(TokenProto p) {
    return new TokenPBImpl(p);
  }

  private TokenProto convertToProtoFormat(Token t) {
    return ((TokenPBImpl) t).getProto();
  }

  private void mergeLocalToBuilder() {
    if (this.collectorAddr != null) {
      builder.setCollectorAddr(this.collectorAddr);
    }
    if (this.collectorToken != null) {
      builder.setCollectorToken(convertToProtoFormat(this.collectorToken));
    }
  }
}