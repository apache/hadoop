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
package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoResponse;
import org.apache.hadoop.yarn.proto.CsiAdaptorProtos;

/**
 * Get plugin info response protobuf impl.
 */
public class GetPluginInfoResponsePBImpl extends GetPluginInfoResponse {

  private CsiAdaptorProtos.GetPluginInfoResponse.Builder builder;

  public GetPluginInfoResponsePBImpl(
      CsiAdaptorProtos.GetPluginInfoResponse responseProto) {
    this.builder = responseProto.toBuilder();
  }

  public GetPluginInfoResponsePBImpl() {
    this.builder = CsiAdaptorProtos.GetPluginInfoResponse.newBuilder();
  }

  @Override
  public void setDriverName(String driverName) {
    Preconditions.checkNotNull(builder);
    builder.setName(driverName);
  }

  @Override
  public String getDriverName() {
    Preconditions.checkNotNull(builder);
    return builder.getName();
  }

  @Override
  public void setVersion(String version) {
    Preconditions.checkNotNull(builder);
    builder.setVendorVersion(version);
  }

  @Override
  public String getVersion() {
    Preconditions.checkNotNull(builder);
    return builder.getVendorVersion();
  }

  public CsiAdaptorProtos.GetPluginInfoResponse getProto() {
    Preconditions.checkNotNull(builder);
    return builder.build();
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
}
