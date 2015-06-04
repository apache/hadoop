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

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.UnRegisterNodeManagerResponseProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;

/**
 * PBImpl class for UnRegisterNodeManagerResponse.
 */
public class UnRegisterNodeManagerResponsePBImpl extends
    UnRegisterNodeManagerResponse {
  private UnRegisterNodeManagerResponseProto proto =
      UnRegisterNodeManagerResponseProto.getDefaultInstance();
  private UnRegisterNodeManagerResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private boolean rebuild = false;

  public UnRegisterNodeManagerResponsePBImpl() {
    builder = UnRegisterNodeManagerResponseProto.newBuilder();
  }

  public UnRegisterNodeManagerResponsePBImpl(
      UnRegisterNodeManagerResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public UnRegisterNodeManagerResponseProto getProto() {
    if (rebuild) {
      mergeLocalToProto();
    }
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    proto = builder.build();
    rebuild = false;
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = UnRegisterNodeManagerResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
}
