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
import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.LogAggregationContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PriorityPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerTypeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ExecutionTypeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LogAggregationContextProto;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.ContainerTokenIdentifierProto;
import org.apache.hadoop.yarn.server.api.ContainerType;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * TokenIdentifier for a container. Encodes {@link ContainerId},
 * {@link Resource} needed by the container and the target NMs host-address.
 * 
 */
@Public
@Evolving
public class ContainerTokenIdentifier extends TokenIdentifier {

  private final static Logger LOG =
      LoggerFactory.getLogger(ContainerTokenIdentifier.class);

  public static final Text KIND = new Text("ContainerToken");

  private ContainerTokenIdentifierProto proto;

  public ContainerTokenIdentifier(ContainerId containerID,
      String hostName, String appSubmitter, Resource r, long expiryTimeStamp,
      int masterKeyId, long rmIdentifier, Priority priority, long creationTime) {
    this(containerID, hostName, appSubmitter, r, expiryTimeStamp, masterKeyId,
        rmIdentifier, priority, creationTime, null,
        CommonNodeLabelsManager.NO_LABEL, ContainerType.TASK);
  }

  /**
   * Creates a instance.
   *
   * @param appSubmitter appSubmitter
   * @param containerID container ID
   * @param creationTime creation time
   * @param expiryTimeStamp expiry timestamp
   * @param hostName hostname
   * @param logAggregationContext log aggregation context
   * @param masterKeyId master key ID
   * @param priority priority
   * @param r resource needed by the container
   * @param rmIdentifier ResourceManager identifier
   * @deprecated Use one of the other constructors instead.
   */
  @Deprecated
  public ContainerTokenIdentifier(ContainerId containerID, String hostName,
      String appSubmitter, Resource r, long expiryTimeStamp, int masterKeyId,
      long rmIdentifier, Priority priority, long creationTime,
      LogAggregationContext logAggregationContext) {
    this(containerID, hostName, appSubmitter, r, expiryTimeStamp, masterKeyId,
        rmIdentifier, priority, creationTime, logAggregationContext,
        CommonNodeLabelsManager.NO_LABEL);
  }

  public ContainerTokenIdentifier(ContainerId containerID, String hostName,
      String appSubmitter, Resource r, long expiryTimeStamp, int masterKeyId,
      long rmIdentifier, Priority priority, long creationTime,
      LogAggregationContext logAggregationContext, String nodeLabelExpression) {
    this(containerID, hostName, appSubmitter, r, expiryTimeStamp, masterKeyId,
        rmIdentifier, priority, creationTime, logAggregationContext,
        nodeLabelExpression, ContainerType.TASK);
  }

  public ContainerTokenIdentifier(ContainerId containerID, String hostName,
      String appSubmitter, Resource r, long expiryTimeStamp, int masterKeyId,
      long rmIdentifier, Priority priority, long creationTime,
      LogAggregationContext logAggregationContext, String nodeLabelExpression,
      ContainerType containerType) {
    this(containerID, 0, hostName, appSubmitter, r, expiryTimeStamp,
        masterKeyId, rmIdentifier, priority, creationTime,
        logAggregationContext, nodeLabelExpression, containerType,
        ExecutionType.GUARANTEED, -1, null);
  }

  public ContainerTokenIdentifier(ContainerId containerID, int containerVersion,
      String hostName, String appSubmitter, Resource r, long expiryTimeStamp,
      int masterKeyId, long rmIdentifier, Priority priority, long creationTime,
      LogAggregationContext logAggregationContext, String nodeLabelExpression,
      ContainerType containerType, ExecutionType executionType) {

    this(containerID, containerVersion, hostName, appSubmitter, r,
        expiryTimeStamp, masterKeyId, rmIdentifier, priority, creationTime,
        logAggregationContext, nodeLabelExpression, containerType,
        executionType, -1, null);
  }

  /**
   * Convenience Constructor for existing clients.
   *
   * @param containerID containerID
   * @param containerVersion containerVersion
   * @param hostName hostName
   * @param appSubmitter appSubmitter
   * @param r resource
   * @param expiryTimeStamp expiryTimeStamp
   * @param masterKeyId masterKeyId
   * @param rmIdentifier rmIdentifier
   * @param priority priority
   * @param creationTime creationTime
   * @param logAggregationContext logAggregationContext
   * @param nodeLabelExpression nodeLabelExpression
   * @param containerType containerType
   * @param executionType executionType
   * @param allocationRequestId allocationRequestId
   */
  public ContainerTokenIdentifier(ContainerId containerID, int containerVersion,
      String hostName, String appSubmitter, Resource r, long expiryTimeStamp,
      int masterKeyId, long rmIdentifier, Priority priority, long creationTime,
      LogAggregationContext logAggregationContext, String nodeLabelExpression,
      ContainerType containerType, ExecutionType executionType,
      long allocationRequestId) {
    this(containerID, containerVersion, hostName, appSubmitter, r,
        expiryTimeStamp, masterKeyId, rmIdentifier, priority, creationTime,
        logAggregationContext, nodeLabelExpression, containerType,
        executionType, allocationRequestId, null);
  }

  /**
   * Create a Container Token Identifier.
   *
   * @param containerID containerID
   * @param containerVersion containerVersion
   * @param hostName hostName
   * @param appSubmitter appSubmitter
   * @param r resource
   * @param expiryTimeStamp expiryTimeStamp
   * @param masterKeyId masterKeyId
   * @param rmIdentifier rmIdentifier
   * @param priority priority
   * @param creationTime creationTime
   * @param logAggregationContext logAggregationContext
   * @param nodeLabelExpression nodeLabelExpression
   * @param containerType containerType
   * @param executionType executionType
   * @param allocationRequestId allocationRequestId
   * @param allocationTags Set of allocation Tags.
   */
  public ContainerTokenIdentifier(ContainerId containerID, int containerVersion,
      String hostName, String appSubmitter, Resource r, long expiryTimeStamp,
      int masterKeyId, long rmIdentifier, Priority priority, long creationTime,
      LogAggregationContext logAggregationContext, String nodeLabelExpression,
      ContainerType containerType, ExecutionType executionType,
      long allocationRequestId, Set<String> allocationTags) {
    ContainerTokenIdentifierProto.Builder builder =
        ContainerTokenIdentifierProto.newBuilder();
    if (containerID != null) {
      builder.setContainerId(((ContainerIdPBImpl)containerID).getProto());
    }
    builder.setVersion(containerVersion);
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
    
    if (nodeLabelExpression != null) {
      builder.setNodeLabelExpression(nodeLabelExpression);
    }
    builder.setContainerType(convertToProtoFormat(containerType));
    builder.setExecutionType(convertToProtoFormat(executionType));
    builder.setAllocationRequestId(allocationRequestId);
    if (allocationTags != null) {
      builder.addAllAllocationTags(allocationTags);
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
   * Get the RMIdentifier of RM in which containers are allocated.
   * @return RMIdentifier
   */
  public long getRMIdentifier() {
    return proto.getRmIdentifier();
  }

  /**
   * Get the ContainerType of container to allocate.
   * @return ContainerType
   */
  public ContainerType getContainerType(){
    if (!proto.hasContainerType()) {
      return ContainerType.TASK;
    }
    return convertFromProtoFormat(proto.getContainerType());
  }

  /**
   * Get the ExecutionType of container to allocate
   * @return ExecutionType
   */
  public ExecutionType getExecutionType(){
    if (!proto.hasExecutionType()) {
      return ExecutionType.GUARANTEED;
    }
    return convertFromProtoFormat(proto.getExecutionType());
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

  public long getAllocationRequestId() {
    return proto.getAllocationRequestId();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    LOG.debug("Writing ContainerTokenIdentifier to RPC layer: {}", this);
    out.write(proto.toByteArray());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte[] data = IOUtils.readFullyToByteArray(in);
    try {
      proto = ContainerTokenIdentifierProto.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("Recovering old formatted token");
      readFieldsInOldFormat(
          new DataInputStream(new ByteArrayInputStream(data)));
    }
  }

  private void readFieldsInOldFormat(DataInputStream in) throws IOException {
    ContainerTokenIdentifierProto.Builder builder =
        ContainerTokenIdentifierProto.newBuilder();
    builder.setNodeLabelExpression(CommonNodeLabelsManager.NO_LABEL);
    builder.setContainerType(ProtoUtils.convertToProtoFormat(
        ContainerType.TASK));
    builder.setExecutionType(ProtoUtils.convertToProtoFormat(
        ExecutionType.GUARANTEED));
    builder.setAllocationRequestId(-1);
    builder.setVersion(0);

    ApplicationId applicationId =
        ApplicationId.newInstance(in.readLong(), in.readInt());
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, in.readInt());
    ContainerId containerId =
        ContainerId.newContainerId(applicationAttemptId, in.readLong());
    builder.setContainerId(ProtoUtils.convertToProtoFormat(containerId));
    builder.setNmHostAddr(in.readUTF());
    builder.setAppSubmitter(in.readUTF());
    int memory = in.readInt();
    int vCores = in.readInt();
    Resource resource = Resource.newInstance(memory, vCores);
    builder.setResource(ProtoUtils.convertToProtoFormat(resource));
    builder.setExpiryTimeStamp(in.readLong());
    builder.setMasterKeyId(in.readInt());
    builder.setRmIdentifier(in.readLong());
    Priority priority = Priority.newInstance(in.readInt());
    builder.setPriority(((PriorityPBImpl)priority).getProto());
    builder.setCreationTime(in.readLong());

    int logAggregationSize = -1;
    try {
      logAggregationSize = in.readInt();
    } catch (EOFException eof) {
      // In the old format, there was no versioning or proper handling of new
      // fields.  Depending on how old, the log aggregation size and data, may
      // or may not exist.  To handle that, we try to read it and ignore the
      // EOFException that's thrown if it's not there.
    }
    if (logAggregationSize != -1) {
      byte[] bytes = new byte[logAggregationSize];
      in.readFully(bytes);
      builder.setLogAggregationContext(
          LogAggregationContextProto.parseFrom(bytes));
    }
    proto = builder.build();
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

  /**
   * Get the Container version
   * @return container version
   */
  public int getVersion() {
    if (proto.hasVersion()) {
      return proto.getVersion();
    } else {
      return 0;
    }
  }
  /**
   * Get the node-label-expression in the original ResourceRequest
   */
  public String getNodeLabelExpression() {
    if (proto.hasNodeLabelExpression()) {
      return proto.getNodeLabelExpression();
    }
    return CommonNodeLabelsManager.NO_LABEL;
  }

  public Set<String> getAllcationTags() {
    if (proto.getAllocationTagsList() != null) {
      return new HashSet<>(proto.getAllocationTagsList());
    }
    return Collections.emptySet();
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

  private ContainerTypeProto convertToProtoFormat(ContainerType containerType) {
    return ProtoUtils.convertToProtoFormat(containerType);
  }

  private ContainerType convertFromProtoFormat(
      ContainerTypeProto containerType) {
    return ProtoUtils.convertFromProtoFormat(containerType);
  }

  private ExecutionTypeProto convertToProtoFormat(ExecutionType executionType) {
    return ProtoUtils.convertToProtoFormat(executionType);
  }

  private ExecutionType convertFromProtoFormat(
      ExecutionTypeProto executionType) {
    return ProtoUtils.convertFromProtoFormat(executionType);
  }
}
