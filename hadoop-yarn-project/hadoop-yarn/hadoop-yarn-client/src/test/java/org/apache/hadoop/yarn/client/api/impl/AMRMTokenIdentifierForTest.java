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

package org.apache.hadoop.yarn.client.api.impl;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnSecurityTestAMRMTokenProtos.AMRMTokenIdentifierForTestProto;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;

import com.google.protobuf.TextFormat;

public class AMRMTokenIdentifierForTest extends AMRMTokenIdentifier {

  private static Log LOG = LogFactory.getLog(AMRMTokenIdentifierForTest.class);

  public static final Text KIND = new Text("YARN_AM_RM_TOKEN");
  
  private AMRMTokenIdentifierForTestProto proto;
  private AMRMTokenIdentifierForTestProto.Builder builder;
  
  public AMRMTokenIdentifierForTest(){
    builder = AMRMTokenIdentifierForTestProto.newBuilder();
  }
  
  public AMRMTokenIdentifierForTest(AMRMTokenIdentifierForTestProto proto) {
    this.proto = proto;
  }
  
  public AMRMTokenIdentifierForTest(AMRMTokenIdentifier tokenIdentifier, 
      String message) {
    builder = AMRMTokenIdentifierForTestProto.newBuilder();
    builder.setAppAttemptId(tokenIdentifier.getProto().getAppAttemptId());
    builder.setKeyId(tokenIdentifier.getKeyId());
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
    proto = AMRMTokenIdentifierForTestProto.parseFrom(buffer);
  }

  @Override
  public Text getKind() {
    return KIND;
  }
  
  public String getMessage() {
    return proto.getMessage();
  }
  
  public void setMessage(String message) {
    builder.setMessage(message);
  }
  
  public void build() {
    proto = builder.build();
    builder = null;
  }
  
  public ApplicationAttemptId getApplicationAttemptId() {
    return new ApplicationAttemptIdPBImpl(proto.getAppAttemptId());
  }
  
  public int getKeyId() {
    return proto.getKeyId();
  }
  
  public AMRMTokenIdentifierForTestProto getNewProto(){
    return this.proto;
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
