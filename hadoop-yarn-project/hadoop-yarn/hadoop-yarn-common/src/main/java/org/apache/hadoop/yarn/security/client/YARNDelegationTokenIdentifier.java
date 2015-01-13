/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.security.client;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProto;

@Private
public abstract class YARNDelegationTokenIdentifier extends
    AbstractDelegationTokenIdentifier {

  YARNDelegationTokenIdentifierProto.Builder builder =
      YARNDelegationTokenIdentifierProto.newBuilder();

  public YARNDelegationTokenIdentifier() {
  }

  public YARNDelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
    super(owner, renewer, realUser);
  }

  public YARNDelegationTokenIdentifier(
      YARNDelegationTokenIdentifierProto.Builder builder) {
    this.builder = builder;
  }

  @Override
  public synchronized void readFields(DataInput in) throws IOException {
    builder.mergeFrom((DataInputStream) in);
    if (builder.getOwner() != null) {
      setOwner(new Text(builder.getOwner()));
    }
    if (builder.getRenewer() != null) {
      setRenewer(new Text(builder.getRenewer()));
    }
    if (builder.getRealUser() != null) {
      setRealUser(new Text(builder.getRealUser()));
    }
    setIssueDate(builder.getIssueDate());
    setMaxDate(builder.getMaxDate());
    setSequenceNumber(builder.getSequenceNumber());
    setMasterKeyId(builder.getMasterKeyId());
  }

  private void setBuilderFields() {
    if (builder.getOwner() != null &&
        !builder.getOwner().equals(getOwner().toString())) {
      builder.setOwner(getOwner().toString());
    }
    if (builder.getRenewer() != null &&
        !builder.getRenewer().equals(getRenewer().toString())) { 
      builder.setRenewer(getRenewer().toString());
    }
    if (builder.getRealUser() != null &&
        !builder.getRealUser().equals(getRealUser().toString())) {
      builder.setRealUser(getRealUser().toString());
    }
    if (builder.getIssueDate() != getIssueDate()) {
      builder.setIssueDate(getIssueDate());
    }
    if (builder.getMaxDate() != getMaxDate()) {
      builder.setMaxDate(getMaxDate());
    }
    if (builder.getSequenceNumber() != getSequenceNumber()) {
      builder.setSequenceNumber(getSequenceNumber());
    }
    if (builder.getMasterKeyId() != getMasterKeyId()) {
      builder.setMasterKeyId(getMasterKeyId());
    }
  }

  @Override
  public synchronized void write(DataOutput out) throws IOException {
    setBuilderFields();
    builder.build().writeTo((DataOutputStream) out);
  }

  public YARNDelegationTokenIdentifierProto getProto() {
    setBuilderFields();
    return builder.build();
  }
}
