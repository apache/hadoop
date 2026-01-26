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
package org.apache.hadoop.yarn.security.client.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProto;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProtoOrBuilder;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;

@Private
@Unstable
public class YARNDelegationTokenIdentifierPBImpl extends YARNDelegationTokenIdentifier {

  private YARNDelegationTokenIdentifierProto proto =
      YARNDelegationTokenIdentifierProto.getDefaultInstance();
  private YARNDelegationTokenIdentifierProto.Builder builder = null;
  private boolean viaProto = false;

  public YARNDelegationTokenIdentifierPBImpl() {
    builder = YARNDelegationTokenIdentifierProto.newBuilder();
  }

  public YARNDelegationTokenIdentifierPBImpl(YARNDelegationTokenIdentifierProto identifierProto) {
    this.proto = identifierProto;
    viaProto = true;
  }

  public YARNDelegationTokenIdentifierProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    // mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      if (proto == null) {
        proto = YARNDelegationTokenIdentifierProto.getDefaultInstance();
      }
      builder = YARNDelegationTokenIdentifierProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public Text getOwner() {
    YARNDelegationTokenIdentifierProtoOrBuilder p = viaProto ? proto : builder;
    return new Text(p.getOwner());
  }

  @Override
  public void setOwner(Text owner) {
    super.setOwner(owner);
    maybeInitBuilder();
    if (owner == null) {
      builder.clearOwner();
      return;
    }
    builder.setOwner(owner.toString());
  }

  @Override
  public Text getRenewer() {
    YARNDelegationTokenIdentifierProtoOrBuilder p = viaProto ? proto : builder;
    return new Text(p.getRenewer());
  }

  @Override
  public void setRenewer(Text renewer) {
    super.setRenewer(renewer);
    maybeInitBuilder();
    if (renewer == null) {
      builder.clearRenewer();
      return;
    }
    builder.setOwner(renewer.toString());
  }

  @Override
  public Text getRealUser() {
    YARNDelegationTokenIdentifierProtoOrBuilder p = viaProto ? proto : builder;
    return new Text(p.getRealUser());
  }

  @Override
  public void setRealUser(Text realUser) {
    super.setRealUser(realUser);
    maybeInitBuilder();
    if (realUser == null) {
      builder.clearRealUser();
      return;
    }
    builder.setRealUser(realUser.toString());
  }

  @Override
  public void setIssueDate(long issueDate) {
    super.setIssueDate(issueDate);
    maybeInitBuilder();
    builder.setIssueDate(issueDate);
  }

  @Override
  public long getIssueDate() {
    YARNDelegationTokenIdentifierProtoOrBuilder p = viaProto ? proto : builder;
    return p.getIssueDate();
  }

  @Override
  public void setMaxDate(long maxDate) {
    super.setMaxDate(maxDate);
    maybeInitBuilder();
    builder.setMaxDate(maxDate);
  }

  @Override
  public long getMaxDate() {
    YARNDelegationTokenIdentifierProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMaxDate();
  }

  @Override
  public void setSequenceNumber(int seqNum) {
    super.setSequenceNumber(seqNum);
    maybeInitBuilder();
    builder.setSequenceNumber(seqNum);
  }

  @Override
  public int getSequenceNumber() {
    YARNDelegationTokenIdentifierProtoOrBuilder p = viaProto ? proto : builder;
    return p.getSequenceNumber();
  }

  @Override
  public void setMasterKeyId(int newId) {
    super.setMasterKeyId(newId);
    maybeInitBuilder();
    builder.setMasterKeyId(newId);
  }

  @Override
  public int getMasterKeyId() {
    YARNDelegationTokenIdentifierProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMasterKeyId();
  }

  @Override
  public Text getKind() {
    return null;
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

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }
}
