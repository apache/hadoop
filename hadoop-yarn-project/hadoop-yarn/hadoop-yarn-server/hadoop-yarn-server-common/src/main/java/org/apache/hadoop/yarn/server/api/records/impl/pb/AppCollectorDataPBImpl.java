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
package org.apache.hadoop.yarn.server.api.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.AppCollectorDataProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.AppCollectorDataProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class AppCollectorDataPBImpl extends AppCollectorData {

  private AppCollectorDataProto proto =
      AppCollectorDataProto.getDefaultInstance();

  private AppCollectorDataProto.Builder builder = null;
  private boolean viaProto = false;

  private ApplicationId appId = null;
  private String collectorAddr = null;
  private Long rmIdentifier = null;
  private Long version = null;
  private Token collectorToken = null;

  public AppCollectorDataPBImpl() {
    builder = AppCollectorDataProto.newBuilder();
  }

  public AppCollectorDataPBImpl(AppCollectorDataProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public AppCollectorDataProto getProto() {
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
  public ApplicationId getApplicationId() {
    AppCollectorDataProtoOrBuilder p = viaProto ? proto : builder;
    if (this.appId == null && p.hasAppId()) {
      this.appId = convertFromProtoFormat(p.getAppId());
    }
    return this.appId;
  }

  @Override
  public String getCollectorAddr() {
    AppCollectorDataProtoOrBuilder p = viaProto ? proto : builder;
    if (this.collectorAddr == null
        && p.hasAppCollectorAddr()) {
      this.collectorAddr = p.getAppCollectorAddr();
    }
    return this.collectorAddr;
  }

  @Override
  public void setApplicationId(ApplicationId id) {
    maybeInitBuilder();
    if (id == null) {
      builder.clearAppId();
    }
    this.appId = id;
  }

  @Override
  public void setCollectorAddr(String collectorAddr) {
    maybeInitBuilder();
    if (collectorAddr == null) {
      builder.clearAppCollectorAddr();
    }
    this.collectorAddr = collectorAddr;
  }

  @Override
  public long getRMIdentifier() {
    AppCollectorDataProtoOrBuilder p = viaProto ? proto : builder;
    if (this.rmIdentifier == null && p.hasRmIdentifier()) {
      this.rmIdentifier = p.getRmIdentifier();
    }
    if (this.rmIdentifier != null) {
      return this.rmIdentifier;
    } else {
      return AppCollectorData.DEFAULT_TIMESTAMP_VALUE;
    }
  }

  @Override
  public void setRMIdentifier(long rmId) {
    maybeInitBuilder();
    this.rmIdentifier = rmId;
    builder.setRmIdentifier(rmId);
  }

  @Override
  public long getVersion() {
    AppCollectorDataProtoOrBuilder p = viaProto ? proto : builder;
    if (this.version == null && p.hasRmIdentifier()) {
      this.version = p.getRmIdentifier();
    }
    if (this.version != null) {
      return this.version;
    } else {
      return AppCollectorData.DEFAULT_TIMESTAMP_VALUE;
    }
  }

  @Override
  public void setVersion(long version) {
    maybeInitBuilder();
    this.version = version;
    builder.setVersion(version);
  }

  @Override
  public Token getCollectorToken() {
    AppCollectorDataProtoOrBuilder p = viaProto ? proto : builder;
    if (this.collectorToken != null) {
      return this.collectorToken;
    }
    if (!p.hasAppCollectorToken()) {
      return null;
    }
    this.collectorToken = new TokenPBImpl(p.getAppCollectorToken());
    return this.collectorToken;
  }

  @Override
  public void setCollectorToken(Token token) {
    maybeInitBuilder();
    if (token == null) {
      builder.clearAppCollectorToken();
    }
    this.collectorToken = token;
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = AppCollectorDataProto.newBuilder(proto);
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

  private void mergeLocalToBuilder() {
    if (this.appId != null) {
      builder.setAppId(convertToProtoFormat(this.appId));
    }
    if (this.collectorAddr != null) {
      builder.setAppCollectorAddr(this.collectorAddr);
    }
    if (this.rmIdentifier != null) {
      builder.setRmIdentifier(this.rmIdentifier);
    }
    if (this.version != null) {
      builder.setVersion(this.version);
    }
    if (this.collectorToken != null) {
      builder.setAppCollectorToken(
          ((TokenPBImpl)this.collectorToken).getProto());
    }
  }
}
