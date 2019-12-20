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
package org.apache.hadoop.yarn.server;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnSecurityTestTokenProtos.NMTokenIdentifierNewProto;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;

import com.google.protobuf.TextFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NMTokenIdentifierNewForTest extends NMTokenIdentifier {

  private static Logger LOG = LoggerFactory.getLogger(NMTokenIdentifierNewForTest.class);

  public static final Text KIND = new Text("NMToken");
  
  private NMTokenIdentifierNewProto proto;
  private NMTokenIdentifierNewProto.Builder builder;
  
  public NMTokenIdentifierNewForTest(){
    builder = NMTokenIdentifierNewProto.newBuilder();
  }
  
  public NMTokenIdentifierNewForTest(NMTokenIdentifierNewProto proto) {
    this.proto = proto;
  }
  
  public NMTokenIdentifierNewForTest(NMTokenIdentifier tokenIdentifier, 
      String message) {
    builder = NMTokenIdentifierNewProto.newBuilder();
    builder.setAppAttemptId(tokenIdentifier.getProto().getAppAttemptId());
    builder.setNodeId(tokenIdentifier.getProto().getNodeId());
    builder.setAppSubmitter(tokenIdentifier.getApplicationSubmitter());
    builder.setKeyId(tokenIdentifier.getKeyId());
    builder.setMessage(message);
    proto = builder.build();
    builder = null;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    LOG.debug("Writing NMTokenIdentifierNewForTest to RPC layer: {}", this);
    out.write(proto.toByteArray());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    DataInputStream dis = (DataInputStream)in;
    byte[] buffer = IOUtils.toByteArray(dis);
    proto = NMTokenIdentifierNewProto.parseFrom(buffer);
  }

  @Override
  public Text getKind() {
    return KIND;
  }

  @Override
  public UserGroupInformation getUser() {
    return null;
  }
  
  public String getMessage() {
    return proto.getMessage();
  }
  
  public void setMessage(String message) {
    builder.setMessage(message);
  }
  
  public NMTokenIdentifierNewProto getNewProto() {
    return proto;
  }
  
  public void build() {
    proto = builder.build();
    builder = null;
  }
  
  public ApplicationAttemptId getApplicationAttemptId() {
    return new ApplicationAttemptIdPBImpl(proto.getAppAttemptId());
  }
  
  public NodeId getNodeId() {
    return new NodeIdPBImpl(proto.getNodeId());
  }
  
  public String getApplicationSubmitter() {
    return proto.getAppSubmitter();
  }
  
  public int getKeyId() {
    return proto.getKeyId();
  }
  
  @Override
  public int hashCode() {
    return this.proto.hashCode();
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
  
  @Override
  public String toString() {
    return TextFormat.shortDebugString(this.proto);
  }

}
