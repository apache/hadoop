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
package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.proto.YarnSecurityTestClientAMTokenProtos.RMDelegationTokenIdentifierForTestProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;

public class RMDelegationTokenIdentifierForTest extends
    RMDelegationTokenIdentifier {

  private RMDelegationTokenIdentifierForTestProto.Builder builder =
      RMDelegationTokenIdentifierForTestProto.newBuilder();
  
  public RMDelegationTokenIdentifierForTest() {
  }
  
  public RMDelegationTokenIdentifierForTest(RMDelegationTokenIdentifier token,
      String message) {
    if (token.getOwner() != null) {
      setOwner(new Text(token.getOwner()));
    }
    if (token.getRenewer() != null) {
      setRenewer(new Text(token.getRenewer()));
    }
    if (token.getRealUser() != null) {
      setRealUser(new Text(token.getRealUser()));
    }
    setIssueDate(token.getIssueDate());
    setMaxDate(token.getMaxDate());
    setSequenceNumber(token.getSequenceNumber());
    setMasterKeyId(token.getMasterKeyId());
    builder.setMessage(message);
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    builder.setOwner(getOwner().toString());
    builder.setRenewer(getRenewer().toString());
    builder.setRealUser(getRealUser().toString());
    builder.setIssueDate(getIssueDate());
    builder.setMaxDate(getMaxDate());
    builder.setSequenceNumber(getSequenceNumber());
    builder.setMasterKeyId(getMasterKeyId());
    builder.setMessage(getMessage());
    builder.build().writeTo((DataOutputStream) out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
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
  
  public String getMessage() {
    return builder.getMessage();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof RMDelegationTokenIdentifierForTest) {
      RMDelegationTokenIdentifierForTest that = (RMDelegationTokenIdentifierForTest) obj;
      return this.getSequenceNumber() == that.getSequenceNumber() 
          && this.getIssueDate() == that.getIssueDate() 
          && this.getMaxDate() == that.getMaxDate()
          && this.getMasterKeyId() == that.getMasterKeyId()
          && isEqual(this.getOwner(), that.getOwner()) 
          && isEqual(this.getRenewer(), that.getRenewer())
          && isEqual(this.getRealUser(), that.getRealUser())
          && isEqual(this.getMessage(), that.getMessage());
    }
    return false;
  }
}
