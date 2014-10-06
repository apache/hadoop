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

package org.apache.hadoop.yarn.security;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.AMRMTokenIdentifierProto;

import com.google.protobuf.TextFormat;

/**
 * AMRMTokenIdentifier is the TokenIdentifier to be used by
 * ApplicationMasters to authenticate to the ResourceManager.
 */
@Public
@Evolving
public class AMRMTokenIdentifier extends TokenIdentifier {

  public static final Text KIND_NAME = new Text("YARN_AM_RM_TOKEN");
  private AMRMTokenIdentifierProto proto;

  public AMRMTokenIdentifier() {
  }
  
  public AMRMTokenIdentifier(ApplicationAttemptId appAttemptId,
      int masterKeyId) {
    AMRMTokenIdentifierProto.Builder builder = 
        AMRMTokenIdentifierProto.newBuilder();
    if (appAttemptId != null) {
      builder.setAppAttemptId(
          ((ApplicationAttemptIdPBImpl)appAttemptId).getProto());
    }
    builder.setKeyId(masterKeyId);
    proto = builder.build();
  }

  @Private
  public ApplicationAttemptId getApplicationAttemptId() {
    if (!proto.hasAppAttemptId()) {
      return null;
    }
    return new ApplicationAttemptIdPBImpl(proto.getAppAttemptId());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.write(proto.toByteArray());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    proto = AMRMTokenIdentifierProto.parseFrom((DataInputStream)in);
  }

  @Override
  public Text getKind() {
    return KIND_NAME;
  }

  @Override
  public UserGroupInformation getUser() {
    String appAttemptId = null;
    if (proto.hasAppAttemptId()) {
      appAttemptId = 
          new ApplicationAttemptIdPBImpl(proto.getAppAttemptId()).toString();
    }
    return UserGroupInformation.createRemoteUser(appAttemptId);
  }

  public int getKeyId() {
    return proto.getKeyId();
  }
  
  public AMRMTokenIdentifierProto getProto() {
    return this.proto;
  }

  // TODO: Needed?
  @InterfaceAudience.Private
  public static class Renewer extends Token.TrivialRenewer {
    @Override
    protected Text getKind() {
      return KIND_NAME;
    }
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
