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

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderNotifyResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderNotifyResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyResponse;

public class SCMUploaderNotifyResponsePBImpl extends SCMUploaderNotifyResponse {
  SCMUploaderNotifyResponseProto proto =
      SCMUploaderNotifyResponseProto.getDefaultInstance();
  SCMUploaderNotifyResponseProto.Builder builder = null;
  boolean viaProto = false;

  public SCMUploaderNotifyResponsePBImpl() {
    builder = SCMUploaderNotifyResponseProto.newBuilder();
  }

  public SCMUploaderNotifyResponsePBImpl(SCMUploaderNotifyResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public SCMUploaderNotifyResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public boolean getAccepted() {
    SCMUploaderNotifyResponseProtoOrBuilder p = viaProto ? proto : builder;
    // Default to true, when in doubt just leave the file in the cache
    return (p.hasAccepted()) ? p.getAccepted() : true;
  }

  @Override
  public void setAccepted(boolean b) {
    maybeInitBuilder();
    builder.setAccepted(b);
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SCMUploaderNotifyResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
}
