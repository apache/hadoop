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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.LogAggregationContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PriorityPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.proto.YarnProtos.LogAggregationContextProto;
import org.apache.hadoop.yarn.proto.YarnSecurityTestTokenProtos.ContainerTokenIdentifierForTestProto;

import com.google.protobuf.TextFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerTokenIdentifierForTest extends ContainerTokenIdentifier {

  private static Logger LOG = LoggerFactory.getLogger(ContainerTokenIdentifier.class);

  public static final Text KIND = new Text("ContainerToken");
  
  private ContainerTokenIdentifierForTestProto proto;
  
  public ContainerTokenIdentifierForTest(ContainerId containerID,
      String hostName, String appSubmitter, Resource r, long expiryTimeStamp,
      int masterKeyId, long rmIdentifier, Priority priority, long creationTime,
      LogAggregationContext logAggregationContext) {
    ContainerTokenIdentifierForTestProto.Builder builder =
        ContainerTokenIdentifierForTestProto.newBuilder();
    if (containerID != null) {
      builder.setContainerId(((ContainerIdPBImpl)containerID).getProto());
    }
    builder.setNmHostAddr(hostName);
    builder.setAppSubmitter(appSubmitter);
    if (r != null) {
      builder.setResource(ProtoUtils.convertToProtoFormat(r));
    }
    builder.setExpiryTimeStamp(expiryTimeStamp);
    builder.setMasterKeyId(masterKeyId);
    builder.setRmIdentifier(rmIdentifier);
    if (priority != null) {
      builder.setPriority(((PriorityPBImpl)priority).getProto());
    }
    builder.setCreationTime(creationTime);
    
    if (logAggregationContext != null) {
      builder.setLogAggregationContext(
          ((LogAggregationContextPBImpl)logAggregationContext).getProto());
    }
    proto = builder.build();
  }

  public ContainerTokenIdentifierForTest(ContainerTokenIdentifier identifier,
      String message) {
    ContainerTokenIdentifierForTestProto.Builder builder =
        ContainerTokenIdentifierForTestProto.newBuilder();
    ContainerIdPBImpl containerID = 
        (ContainerIdPBImpl)identifier.getContainerID();
    if (containerID != null) {
      builder.setContainerId(containerID.getProto());
    }
    builder.setNmHostAddr(identifier.getNmHostAddress());
    builder.setAppSubmitter(identifier.getApplicationSubmitter());
    
    ResourcePBImpl resource = (ResourcePBImpl)identifier.getResource();
    if (resource != null) {
      builder.setResource(ProtoUtils.convertToProtoFormat(resource));
    }
    
    builder.setExpiryTimeStamp(identifier.getExpiryTimeStamp());
    builder.setMasterKeyId(identifier.getMasterKeyId());
    builder.setRmIdentifier(identifier.getRMIdentifier());
    
    PriorityPBImpl priority = (PriorityPBImpl)identifier.getPriority();
    if (priority != null) {
      builder.setPriority(priority.getProto());
    }
    
    builder.setCreationTime(identifier.getCreationTime());
    builder.setMessage(message);
    
    LogAggregationContextPBImpl logAggregationContext = 
        (LogAggregationContextPBImpl)identifier.getLogAggregationContext();
    
    if (logAggregationContext != null) {
      builder.setLogAggregationContext(logAggregationContext.getProto());
    }
    
    proto = builder.build();
  }

  public ContainerId getContainerID() {
    return new ContainerIdPBImpl(proto.getContainerId());
  }

  public String getApplicationSubmitter() {
    return proto.getAppSubmitter();
  }

  public String getNmHostAddress() {
    return proto.getNmHostAddr();
  }

  public Resource getResource() {
    return new ResourcePBImpl(proto.getResource());
  }

  public long getExpiryTimeStamp() {
    return proto.getExpiryTimeStamp();
  }

  public int getMasterKeyId() {
    return proto.getMasterKeyId();
  }

  public Priority getPriority() {
    return new PriorityPBImpl(proto.getPriority());
  }

  public long getCreationTime() {
    return proto.getCreationTime();
  }
  /**
   * Get the RMIdentifier of RM in which containers are allocated
   * @return RMIdentifier
   */
  public long getRMIdentifier() {
    return proto.getRmIdentifier();
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    DataInputStream dis = (DataInputStream)in;
    byte[] buffer = IOUtils.toByteArray(dis);
    proto = ContainerTokenIdentifierForTestProto.parseFrom(buffer);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    LOG.debug("Writing ContainerTokenIdentifierForTest to RPC layer: " + this);
    out.write(proto.toByteArray());
  }
  
  ContainerTokenIdentifierForTestProto getNewProto() {
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
