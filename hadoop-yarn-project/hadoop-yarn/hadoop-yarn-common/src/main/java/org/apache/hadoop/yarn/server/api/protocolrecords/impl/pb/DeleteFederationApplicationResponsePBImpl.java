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
package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.DeleteFederationApplicationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.DeleteFederationApplicationResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeleteFederationApplicationResponse;

@Private
@Unstable
public class DeleteFederationApplicationResponsePBImpl extends DeleteFederationApplicationResponse {

  private DeleteFederationApplicationResponseProto proto =
      DeleteFederationApplicationResponseProto.getDefaultInstance();
  private DeleteFederationApplicationResponseProto.Builder builder = null;
  private boolean viaProto = false;

  public DeleteFederationApplicationResponsePBImpl() {
    builder = DeleteFederationApplicationResponseProto.newBuilder();
  }

  public DeleteFederationApplicationResponsePBImpl(DeleteFederationApplicationResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public DeleteFederationApplicationResponseProto getProto() {
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
    if (!(other instanceof DeleteFederationApplicationResponse)) {
      return false;
    }
    DeleteFederationApplicationResponsePBImpl otherImpl = this.getClass().cast(other);
    return new EqualsBuilder().append(this.getProto(), otherImpl.getProto()).isEquals();
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public String getMessage() {
    DeleteFederationApplicationResponseProtoOrBuilder p = viaProto ? proto : builder;
    boolean hasMessage = p.hasMessage();
    if (hasMessage) {
      return p.getMessage();
    }
    return null;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = DeleteFederationApplicationResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public void setMessage(String msg) {
    maybeInitBuilder();
    if (msg == null) {
      builder.clearMessage();
      return;
    }
    builder.setMessage(msg);
  }
}
