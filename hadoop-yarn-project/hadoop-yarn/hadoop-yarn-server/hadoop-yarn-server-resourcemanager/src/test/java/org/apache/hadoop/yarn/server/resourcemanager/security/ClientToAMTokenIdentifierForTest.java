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
package org.apache.hadoop.yarn.server.resourcemanager.security;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.proto.YarnSecurityTestClientAMTokenProtos.ClientToAMTokenIdentifierForTestProto;

import com.google.protobuf.TextFormat;

public class ClientToAMTokenIdentifierForTest extends ClientToAMTokenIdentifier {

  private ClientToAMTokenIdentifierForTestProto proto;

  public ClientToAMTokenIdentifierForTest() {
  }
  
  public ClientToAMTokenIdentifierForTest(
      ClientToAMTokenIdentifier tokenIdentifier, String message) {
    ClientToAMTokenIdentifierForTestProto.Builder builder = 
        ClientToAMTokenIdentifierForTestProto.newBuilder();
    builder.setAppAttemptId(tokenIdentifier.getProto().getAppAttemptId());
    builder.setClientName(tokenIdentifier.getProto().getClientName());
    builder.setMessage(message);
    proto = builder.build();
  }

  public ApplicationAttemptId getApplicationAttemptID() {
    if (!proto.hasAppAttemptId()) {
      return null;
    }
    return new ApplicationAttemptIdPBImpl(proto.getAppAttemptId());
  }

  public String getClientName() {
    return proto.getClientName();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.write(proto.toByteArray());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    DataInputStream dis = (DataInputStream)in;
    byte[] buffer = IOUtils.toByteArray(dis);
    proto = ClientToAMTokenIdentifierForTestProto.parseFrom(buffer);
  }

  @Override
  public UserGroupInformation getUser() {
    String clientName = getClientName();
    if (clientName == null) {
      return null;
    }
    return UserGroupInformation.createRemoteUser(clientName);
  }
  
  @Override
  public int hashCode() {
    return getNewProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getNewProto().equals(this.getClass().cast(other).getNewProto());
    }
    return false;
  }
  
  public ClientToAMTokenIdentifierForTestProto getNewProto() {
    return proto;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getNewProto());
  }

}
