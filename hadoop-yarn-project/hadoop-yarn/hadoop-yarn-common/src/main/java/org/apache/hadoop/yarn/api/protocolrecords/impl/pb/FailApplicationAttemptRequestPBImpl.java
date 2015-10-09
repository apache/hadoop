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
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.FailApplicationAttemptRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.FailApplicationAttemptRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class FailApplicationAttemptRequestPBImpl extends FailApplicationAttemptRequest {
  FailApplicationAttemptRequestProto proto = FailApplicationAttemptRequestProto.getDefaultInstance();
  FailApplicationAttemptRequestProto.Builder builder = null;
  boolean viaProto = false;

  private ApplicationAttemptId applicationAttemptId = null;


  public FailApplicationAttemptRequestPBImpl() {
    builder = FailApplicationAttemptRequestProto.newBuilder();
  }

  public FailApplicationAttemptRequestPBImpl(FailApplicationAttemptRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public FailApplicationAttemptRequestProto getProto() {
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

  private void mergeLocalToBuilder() {
    if (this.applicationAttemptId != null) {
      builder.setApplicationAttemptId(
          convertToProtoFormat(this.applicationAttemptId));
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
      builder = FailApplicationAttemptRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }


  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    FailApplicationAttemptRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationAttemptId != null) {
      return this.applicationAttemptId;
    }
    if (!p.hasApplicationAttemptId()) {
      return null;
    }
    this.applicationAttemptId = convertFromProtoFormat(
        p.getApplicationAttemptId());
    return this.applicationAttemptId;
  }

  @Override
  public void setApplicationAttemptId(
      ApplicationAttemptId applicationAttemptId) {
    maybeInitBuilder();
    if (applicationAttemptId == null)
      builder.clearApplicationAttemptId();
    this.applicationAttemptId = applicationAttemptId;
  }

  private ApplicationAttemptIdPBImpl convertFromProtoFormat(
      ApplicationAttemptIdProto p) {
    return new ApplicationAttemptIdPBImpl(p);
  }

  private ApplicationAttemptIdProto convertToProtoFormat(
      ApplicationAttemptId t) {
    return ((ApplicationAttemptIdPBImpl)t).getProto();
  }

}
