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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.impl.pb.QueueInfoPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueInfoProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoResponseProtoOrBuilder;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

@Private
@Unstable
public class GetQueueInfoResponsePBImpl extends GetQueueInfoResponse {

  QueueInfo queueInfo;
  
  GetQueueInfoResponseProto proto = 
    GetQueueInfoResponseProto.getDefaultInstance();
  GetQueueInfoResponseProto.Builder builder = null;
  boolean viaProto = false;

  public GetQueueInfoResponsePBImpl() {
    builder = GetQueueInfoResponseProto.newBuilder();
  }
  
  public GetQueueInfoResponsePBImpl(GetQueueInfoResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetQueueInfoResponseProto getProto() {
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

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public QueueInfo getQueueInfo() {
    if (this.queueInfo != null) {
      return this.queueInfo;
    }

    GetQueueInfoResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasQueueInfo()) {
      return null;
    }
    this.queueInfo = convertFromProtoFormat(p.getQueueInfo());
    return this.queueInfo;
  }

  @Override
  public void setQueueInfo(QueueInfo queueInfo) {
    maybeInitBuilder();
    if(queueInfo == null) {
      builder.clearQueueInfo();
    }
    this.queueInfo = queueInfo;
  }

  private void mergeLocalToBuilder() {
    if (this.queueInfo != null) {
      builder.setQueueInfo(convertToProtoFormat(this.queueInfo));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetQueueInfoResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private QueueInfo convertFromProtoFormat(QueueInfoProto queueInfo) {
    return new QueueInfoPBImpl(queueInfo);
  }

  private QueueInfoProto convertToProtoFormat(QueueInfo queueInfo) {
    return ((QueueInfoPBImpl)queueInfo).getProto();
  }

}
