/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.SetMaintenanceModeRequest;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SetMaintenanceModeRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SetMaintenanceModeRequestProtoOrBuilder;

public class SetMaintenanceModeRequestPBImpl extends SetMaintenanceModeRequest {
  SetMaintenanceModeRequestProto proto =
      SetMaintenanceModeRequestProto.getDefaultInstance();
  SetMaintenanceModeRequestProto.Builder builder = null;
  boolean viaProto = false;

  private static final long DEFAULT_MAINTENANCE_TIME = 0L;
  private long minimumMaintenanceTime = DEFAULT_MAINTENANCE_TIME;

  public SetMaintenanceModeRequestPBImpl() {
    builder = SetMaintenanceModeRequestProto.newBuilder();
  }

  public SetMaintenanceModeRequestPBImpl(SetMaintenanceModeRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public SetMaintenanceModeRequestProto getProto() {
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

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (minimumMaintenanceTime >= DEFAULT_MAINTENANCE_TIME) {
      this.builder.setMinimumMaintenanceTime(this.minimumMaintenanceTime);
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SetMaintenanceModeRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public void setMaintenance(boolean maintenance) {
    maybeInitBuilder();
    builder.setMaintenance(maintenance);
  }

  @Override
  public boolean isMaintenance() {
    SetMaintenanceModeRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMaintenance()) {
      return false;
    }
    return p.getMaintenance();
  }

  @Override
  public long getMinimumMaintenanceTime() {
    if (this.minimumMaintenanceTime <= DEFAULT_MAINTENANCE_TIME) {
      SetMaintenanceModeRequestProtoOrBuilder p = viaProto ? proto : builder;
      this.minimumMaintenanceTime = p.hasMinimumMaintenanceTime() ?
          p.getMinimumMaintenanceTime() :
          DEFAULT_MAINTENANCE_TIME;
    }
    return this.minimumMaintenanceTime;
  }

  @Override
  public void setMinimumMaintenanceTime(long value) {
    maybeInitBuilder();
    if (value < DEFAULT_MAINTENANCE_TIME) {
      this.builder.clearMinimumMaintenanceTime();
    }
    this.minimumMaintenanceTime = value;
  }
}