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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.LogAggregationContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PriorityPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.ContainerTokenIdentifierProto;

import com.google.protobuf.TextFormat;


/**
 * TokenIdentifier for a container. Encodes {@link ContainerId},
 * {@link Resource} needed by the container and the target NMs host-address.
 * 
 */
@Public
@Evolving
public class ContainerTokenIdentifier extends TokenIdentifier {

  private static Log LOG = LogFactory.getLog(ContainerTokenIdentifier.class);

  public static final Text KIND = new Text("ContainerToken");

  private ContainerTokenIdentifierProto proto;

  public ContainerTokenIdentifier(ContainerId containerID,
      String hostName, String appSubmitter, Resource r, long expiryTimeStamp,
      int masterKeyId, long rmIdentifier, Priority priority, long creationTime) {
    this(containerID, hostName, appSubmitter, r, expiryTimeStamp, masterKeyId,
        rmIdentifier, priority, creationTime, null);
  }

  public ContainerTokenIdentifier(ContainerId containerID, String hostName,
      String appSubmitter, Resource r, long expiryTimeStamp, int masterKeyId,
      long rmIdentifier, Priority priority, long creationTime,
      LogAggregationContext logAggregationContext) {
    ContainerTokenIdentifierProto.Builder builder = 
        ContainerTokenIdentifierProto.newBuilder();
    if (containerID != null) {
      builder.setContainerId(((ContainerIdPBImpl)containerID).getProto());
    }
    builder.setNmHostAddr(hostName);
    builder.setAppSubmitter(appSubmitter);
    if (r != null) {
      builder.setResource(((ResourcePBImpl)r).getProto());
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

  /**
   * Default constructor needed by RPC layer/SecretManager.
   */
  public ContainerTokenIdentifier() {
  }

  public ContainerId getContainerID() {
    if (!proto.hasContainerId()) {
      return null;
    }
    return new ContainerIdPBImpl(proto.getContainerId());
  }

  public String getApplicationSubmitter() {
    return proto.getAppSubmitter();
  }

  public String getNmHostAddress() {
    return proto.getNmHostAddr();
  }

  public Resource getResource() {
    if (!proto.hasResource()) {
      return null;
    }
    return new ResourcePBImpl(proto.getResource());
  }

  public long getExpiryTimeStamp() {
    return proto.getExpiryTimeStamp();
  }

  public int getMasterKeyId() {
    return proto.getMasterKeyId();
  }

  public Priority getPriority() {
    if (!proto.hasPriority()) {
      return null;
    }
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
  
  public ContainerTokenIdentifierProto getProto() {
    return proto;
  }

  public LogAggregationContext getLogAggregationContext() {
    if (!proto.hasLogAggregationContext()) {
      return null;
    }
    return new LogAggregationContextPBImpl(proto.getLogAggregationContext());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    LOG.debug("Writing ContainerTokenIdentifier to RPC layer: " + this);
    out.write(proto.toByteArray());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    proto = ContainerTokenIdentifierProto.parseFrom((DataInputStream)in);
  }

  @Override
  public Text getKind() {
    return KIND;
  }

  @Override
  public UserGroupInformation getUser() {
    String containerId = null;
    if (proto.hasContainerId()) {
      containerId = new ContainerIdPBImpl(proto.getContainerId()).toString();
    }
    return UserGroupInformation.createRemoteUser(
        containerId);
  }

  // TODO: Needed?
  @InterfaceAudience.Private
  public static class Renewer extends Token.TrivialRenewer {
    @Override
    protected Text getKind() {
      return KIND;
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
