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

import org.apache.hadoop.yarn.api.protocolrecords.PreemptionResourceRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceRequestPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.PreemptionResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.PreemptionResourceRequestProtoOrBuilder;

public class PreemptionResourceRequestPBImpl implements PreemptionResourceRequest {

  PreemptionResourceRequestProto proto =
    PreemptionResourceRequestProto.getDefaultInstance();
  PreemptionResourceRequestProto.Builder builder = null;

  boolean viaProto = false;
  private ResourceRequest rr;

  public PreemptionResourceRequestPBImpl() {
    builder = PreemptionResourceRequestProto.newBuilder();
  }

  public PreemptionResourceRequestPBImpl(PreemptionResourceRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized PreemptionResourceRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (rr != null) {
      builder.setResource(convertToProtoFormat(rr));
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = PreemptionResourceRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized ResourceRequest getResourceRequest() {
    PreemptionResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (rr != null) {
      return rr;
    }
    if (!p.hasResource()) {
      return null;
    }
    rr = convertFromProtoFormat(p.getResource());
    return rr;
  }

  @Override
  public synchronized void setResourceRequest(final ResourceRequest rr) {
    maybeInitBuilder();
    if (null == rr) {
      builder.clearResource();
    }
    this.rr = rr;
  }

  private ResourceRequestPBImpl convertFromProtoFormat(ResourceRequestProto p) {
    return new ResourceRequestPBImpl(p);
  }

  private ResourceRequestProto convertToProtoFormat(ResourceRequest t) {
    return ((ResourceRequestPBImpl)t).getProto();
  }

}
