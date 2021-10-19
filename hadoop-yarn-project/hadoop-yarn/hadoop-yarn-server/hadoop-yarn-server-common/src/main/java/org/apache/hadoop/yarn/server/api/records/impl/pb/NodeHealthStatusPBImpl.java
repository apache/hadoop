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

import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeHealthStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeHealthStatusProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

public class NodeHealthStatusPBImpl extends NodeHealthStatus {

  private NodeHealthStatusProto.Builder builder;
  private boolean viaProto = false;
  private NodeHealthStatusProto proto = NodeHealthStatusProto
      .getDefaultInstance();

  public NodeHealthStatusPBImpl() {
    this.builder = NodeHealthStatusProto.newBuilder();
  }

  public NodeHealthStatusPBImpl(NodeHealthStatusProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  public NodeHealthStatusProto getProto() {
    mergeLocalToProto();
    this.proto = this.viaProto ? this.proto : this.builder.build();
    this.viaProto = true;
    return this.proto;
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
    if (this.viaProto)
      maybeInitBuilder();
    this.proto = this.builder.build();

    this.viaProto = true;
  }

  private void maybeInitBuilder() {
    if (this.viaProto || this.builder == null) {
      this.builder = NodeHealthStatusProto.newBuilder(this.proto);
    }
    this.viaProto = false;
  }

  @Override
  public boolean getIsNodeHealthy() {
    NodeHealthStatusProtoOrBuilder p =
        this.viaProto ? this.proto : this.builder;
    return p.getIsNodeHealthy();
  }

  @Override
  public void setIsNodeHealthy(boolean isNodeHealthy) {
    maybeInitBuilder();
    this.builder.setIsNodeHealthy(isNodeHealthy);
  }

  @Override
  public String getHealthReport() {
    NodeHealthStatusProtoOrBuilder p =
        this.viaProto ? this.proto : this.builder;
    if (!p.hasHealthReport()) {
      return null;
    }
    return (p.getHealthReport());
  }

  @Override
  public void setHealthReport(String healthReport) {
    maybeInitBuilder();
    if (healthReport == null) {
      this.builder.clearHealthReport();
      return;
    }
    this.builder.setHealthReport((healthReport));
  }

  @Override
  public long getLastHealthReportTime() {
    NodeHealthStatusProtoOrBuilder p =
        this.viaProto ? this.proto : this.builder;
    return (p.getLastHealthReportTime());
  }

  @Override
  public void setLastHealthReportTime(long lastHealthReport) {
    maybeInitBuilder();
    this.builder.setLastHealthReportTime((lastHealthReport));
  }

}
