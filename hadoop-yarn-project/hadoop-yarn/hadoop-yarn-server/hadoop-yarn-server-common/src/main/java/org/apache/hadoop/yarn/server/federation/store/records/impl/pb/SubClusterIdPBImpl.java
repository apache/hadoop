/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

/**
 * Protocol buffer based implementation of {@link SubClusterId}.
 */
@Private
@Unstable
public class SubClusterIdPBImpl extends SubClusterId {

  private SubClusterIdProto proto = SubClusterIdProto.getDefaultInstance();
  private SubClusterIdProto.Builder builder = null;
  private boolean viaProto = false;

  public SubClusterIdPBImpl() {
    builder = SubClusterIdProto.newBuilder();
  }

  public SubClusterIdPBImpl(SubClusterIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public SubClusterIdProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SubClusterIdProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getId() {
    SubClusterIdProtoOrBuilder p = viaProto ? proto : builder;
    return p.getId();
  }

  @Override
  protected void setId(String subClusterId) {
    maybeInitBuilder();
    if (subClusterId == null) {
      builder.clearId();
      return;
    }
    builder.setId(subClusterId);
  }

}
