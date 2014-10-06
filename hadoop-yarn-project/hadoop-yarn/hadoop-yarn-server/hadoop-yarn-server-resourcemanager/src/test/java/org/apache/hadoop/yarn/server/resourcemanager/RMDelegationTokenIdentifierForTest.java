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
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.proto.YarnSecurityTestClientAMTokenProtos.RMDelegationTokenIdentifierForTestProto;

public class RMDelegationTokenIdentifierForTest extends
    RMDelegationTokenIdentifier {

  private RMDelegationTokenIdentifierForTestProto proto;
  private RMDelegationTokenIdentifierForTestProto.Builder builder;
  
  public RMDelegationTokenIdentifierForTest() {
  }
  
  public RMDelegationTokenIdentifierForTest(
      RMDelegationTokenIdentifier token, String message) {
    builder = RMDelegationTokenIdentifierForTestProto.newBuilder();
    if (token.getOwner() != null) {
      builder.setOwner(token.getOwner().toString());
    }
    if (token.getRenewer() != null) {
      builder.setRenewer(token.getRenewer().toString());
    }
    if (token.getRealUser() != null) {
      builder.setRealUser(token.getRealUser().toString());
    }
    builder.setIssueDate(token.getIssueDate());
    builder.setMaxDate(token.getMaxDate());
    builder.setSequenceNumber(token.getSequenceNumber());
    builder.setMasterKeyId(token.getMasterKeyId());
    builder.setMessage(message);
    proto = builder.build();
    builder = null;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.write(proto.toByteArray());
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    DataInputStream dis = (DataInputStream)in;
    byte[] buffer = IOUtils.toByteArray(dis);
    proto = RMDelegationTokenIdentifierForTestProto.parseFrom(buffer);
  }
  
  /**
   * Get the username encoded in the token identifier
   * 
   * @return the username or owner
   */
  @Override
  public UserGroupInformation getUser() {
    String owner = getOwner().toString();
    String realUser = getRealUser().toString();
    if ( (owner == null) || (owner.toString().isEmpty())) {
      return null;
    }
    final UserGroupInformation realUgi;
    final UserGroupInformation ugi;
    if ((realUser == null) || (realUser.toString().isEmpty())
        || realUser.equals(owner)) {
      ugi = realUgi = UserGroupInformation.createRemoteUser(owner.toString());
    } else {
      realUgi = UserGroupInformation.createRemoteUser(realUser.toString());
      ugi = UserGroupInformation.createProxyUser(owner.toString(), realUgi);
    }
    realUgi.setAuthenticationMethod(AuthenticationMethod.TOKEN);
    return ugi;
  }

  public Text getOwner() {
    String owner = proto.getOwner();
    if (owner == null) {
      return null;
    } else {
      return new Text(owner);
    }
  }

  public Text getRenewer() {
    String renewer = proto.getRenewer();
    if (renewer == null) {
      return null;
    } else {
      return new Text(renewer);
    }
  }
  
  public Text getRealUser() {
    String realUser = proto.getRealUser();
    if (realUser == null) {
      return null;
    } else {
      return new Text(realUser);
    }
  }
  
  public void setIssueDate(long issueDate) {
    RMDelegationTokenIdentifierForTestProto.Builder builder = 
        RMDelegationTokenIdentifierForTestProto.newBuilder(proto);
    builder.setIssueDate(issueDate);
    proto = builder.build();
  }
  
  public long getIssueDate() {
    return proto.getIssueDate();
  }
  
  public void setMaxDate(long maxDate) {
    RMDelegationTokenIdentifierForTestProto.Builder builder = 
        RMDelegationTokenIdentifierForTestProto.newBuilder(proto);
    builder.setMaxDate(maxDate);
    proto = builder.build();
  }
  
  public long getMaxDate() {
    return proto.getMaxDate();
  }

  public void setSequenceNumber(int seqNum) {
    RMDelegationTokenIdentifierForTestProto.Builder builder = 
        RMDelegationTokenIdentifierForTestProto.newBuilder(proto);
    builder.setSequenceNumber(seqNum);
    proto = builder.build();
  }
  
  public int getSequenceNumber() {
    return proto.getSequenceNumber();
  }

  public void setMasterKeyId(int newId) {
    RMDelegationTokenIdentifierForTestProto.Builder builder = 
        RMDelegationTokenIdentifierForTestProto.newBuilder(proto);
    builder.setMasterKeyId(newId);
    proto = builder.build();
  }

  public int getMasterKeyId() {
    return proto.getMasterKeyId();
  }
  
  public String getMessage() {
    return proto.getMessage();
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

  @Override
  public int hashCode() {
    return this.getSequenceNumber();
  }

}
