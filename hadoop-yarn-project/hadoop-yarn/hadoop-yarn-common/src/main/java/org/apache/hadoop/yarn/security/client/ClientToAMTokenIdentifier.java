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

package org.apache.hadoop.yarn.security.client;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.ClientToAMTokenIdentifierProto;

import com.google.protobuf.TextFormat;


@Public
@Evolving
public class ClientToAMTokenIdentifier extends TokenIdentifier {

  public static final Text KIND_NAME = new Text("YARN_CLIENT_TOKEN");

  private ClientToAMTokenIdentifierProto proto;

  // TODO: Add more information in the tokenID such that it is not
  // transferrable, more secure etc.

  public ClientToAMTokenIdentifier() {
  }

  public ClientToAMTokenIdentifier(ApplicationAttemptId id, String client) {
    ClientToAMTokenIdentifierProto.Builder builder = 
        ClientToAMTokenIdentifierProto.newBuilder();
    if (id != null) {
      builder.setAppAttemptId(((ApplicationAttemptIdPBImpl)id).getProto());
    }
    if (client != null) {
      builder.setClientName(client);
    }
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

  public ClientToAMTokenIdentifierProto getProto() {
    return proto;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.write(proto.toByteArray());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    proto = ClientToAMTokenIdentifierProto.parseFrom((DataInputStream)in);
  }

  @Override
  public Text getKind() {
    return KIND_NAME;
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
}
