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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.NMTokenIdentifierProto;

import com.google.protobuf.TextFormat;

@Public
@Evolving
public class NMTokenIdentifier extends TokenIdentifier {

  private static Log LOG = LogFactory.getLog(NMTokenIdentifier.class);

  public static final Text KIND = new Text("NMToken");
  
  private NMTokenIdentifierProto proto;

  public NMTokenIdentifier(ApplicationAttemptId appAttemptId, 
      NodeId nodeId, String applicationSubmitter, int masterKeyId) {
    NMTokenIdentifierProto.Builder builder = NMTokenIdentifierProto.newBuilder();
    if (appAttemptId != null) {
      builder.setAppAttemptId(
          ((ApplicationAttemptIdPBImpl)appAttemptId).getProto());
    }
    if (nodeId != null) {
      builder.setNodeId(((NodeIdPBImpl)nodeId).getProto());
    }
    builder.setAppSubmitter(applicationSubmitter);
    builder.setKeyId(masterKeyId);
    proto = builder.build();
  }
  
  /**
   * Default constructor needed by RPC/Secret manager
   */
  public NMTokenIdentifier() {
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    if (!proto.hasAppAttemptId()) {
      return null;
    }
    return new ApplicationAttemptIdPBImpl(proto.getAppAttemptId());
  }
  
  public NodeId getNodeId() {
    if (!proto.hasNodeId()) {
      return null;
    }
    return new NodeIdPBImpl(proto.getNodeId());
  }
  
  public String getApplicationSubmitter() {
    return proto.getAppSubmitter();
  }
  
  public int getKeyId() {
    return proto.getKeyId();
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    LOG.debug("Writing NMTokenIdentifier to RPC layer: " + this);
    out.write(proto.toByteArray());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte[] data = IOUtils.readFullyToByteArray(in);
    try {
      proto = NMTokenIdentifierProto.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("Recovering old formatted token");
      readFieldsInOldFormat(
          new DataInputStream(new ByteArrayInputStream(data)));
    }
  }

  private void readFieldsInOldFormat(DataInputStream in) throws IOException {
    NMTokenIdentifierProto.Builder builder =
        NMTokenIdentifierProto.newBuilder();

    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(in.readLong(), in.readInt()),
            in.readInt());
    builder.setAppAttemptId(((ApplicationAttemptIdPBImpl)appAttemptId)
        .getProto());
    String[] hostAddr = in.readUTF().split(":");
    NodeId nodeId = NodeId.newInstance(hostAddr[0],
        Integer.parseInt(hostAddr[1]));
    builder.setNodeId(((NodeIdPBImpl)nodeId).getProto());
    builder.setAppSubmitter(in.readUTF());
    builder.setKeyId(in.readInt());
    proto = builder.build();
  }

  @Override
  public Text getKind() {
    return KIND;
  }

  @Override
  public UserGroupInformation getUser() {
    String appAttemptId = null;
    if (proto.hasAppAttemptId()) {
      appAttemptId = new ApplicationAttemptIdPBImpl(
          proto.getAppAttemptId()).toString();
    }
    return UserGroupInformation.createRemoteUser(appAttemptId);
  }
  
  public NMTokenIdentifierProto getProto() {
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
}
