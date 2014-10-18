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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProto;

public abstract class YARNDelegationTokenIdentifier extends
    AbstractDelegationTokenIdentifier {
  
  YARNDelegationTokenIdentifierProto.Builder builder = 
      YARNDelegationTokenIdentifierProto.newBuilder();

  public YARNDelegationTokenIdentifier() {}

  public YARNDelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
    setOwner(owner);
    setRenewer(renewer);
    setRealUser(realUser);
  }
  
  /**
   * Get the username encoded in the token identifier
   * 
   * @return the username or owner
   */
  @Override
  public UserGroupInformation getUser() {
    String owner = getOwner() == null ? null : getOwner().toString();
    String realUser = getRealUser() == null ? null: getRealUser().toString();
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
    String owner = builder.getOwner();
    if (owner == null) {
      return null;
    } else {
      return new Text(owner);
    }
  }

  @Override
  public void setOwner(Text owner) {
    if (builder != null && owner != null) {
      builder.setOwner(owner.toString());
    }
  }

  public Text getRenewer() {
    String renewer = builder.getRenewer();
    if (renewer == null) {
      return null;
    } else {
      return new Text(renewer);
    }
  }

  @Override
  public void setRenewer(Text renewer) {
    if (builder != null && renewer != null) {
      HadoopKerberosName renewerKrbName = new HadoopKerberosName(renewer.toString());
      try {
        builder.setRenewer(renewerKrbName.getShortName());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public Text getRealUser() {
    String realUser = builder.getRealUser();
    if (realUser == null) {
      return null;
    } else {
      return new Text(realUser);
    }
  }

  @Override
  public void setRealUser(Text realUser) {
    if (builder != null && realUser != null) {
      builder.setRealUser(realUser.toString());
    }
  }

  public void setIssueDate(long issueDate) {
    builder.setIssueDate(issueDate);
  }
  
  public long getIssueDate() {
    return builder.getIssueDate();
  }
  
  
  public void setRenewDate(long renewDate) {
    builder.setRenewDate(renewDate);
  }
  
  public long getRenewDate() {
    return builder.getRenewDate();
  }
  
  public void setMaxDate(long maxDate) {
    builder.setMaxDate(maxDate);
  }
  
  public long getMaxDate() {
    return builder.getMaxDate();
  }

  public void setSequenceNumber(int seqNum) {
    builder.setSequenceNumber(seqNum);
  }
  
  public int getSequenceNumber() {
    return builder.getSequenceNumber();
  }

  public void setMasterKeyId(int newId) {
    builder.setMasterKeyId(newId);
  }

  public int getMasterKeyId() {
    return builder.getMasterKeyId();
  }
  
  protected static boolean isEqual(Object a, Object b) {
    return a == null ? b == null : a.equals(b);
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof YARNDelegationTokenIdentifier) {
      YARNDelegationTokenIdentifier that = (YARNDelegationTokenIdentifier) obj;
      return this.getSequenceNumber() == that.getSequenceNumber() 
          && this.getIssueDate() == that.getIssueDate() 
          && this.getMaxDate() == that.getMaxDate()
          && this.getMasterKeyId() == that.getMasterKeyId()
          && isEqual(this.getOwner(), that.getOwner()) 
          && isEqual(this.getRenewer(), that.getRenewer())
          && isEqual(this.getRealUser(), that.getRealUser());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.getSequenceNumber();
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    builder.mergeFrom((DataInputStream) in);
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    builder.build().writeTo((DataOutputStream)out);
  }
  
  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer
        .append("owner=" + getOwner() + ", renewer=" + getRenewer() + ", realUser="
            + getRealUser() + ", issueDate=" + getIssueDate() 
            + ", maxDate=" + getMaxDate() + ", sequenceNumber=" 
            + getSequenceNumber() + ", masterKeyId="
            + getMasterKeyId());
    return buffer.toString();
  }

}
