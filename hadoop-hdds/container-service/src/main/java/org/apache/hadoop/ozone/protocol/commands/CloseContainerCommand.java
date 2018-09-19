/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.protocol.commands;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.CloseContainerCommandProto;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineID;

/**
 * Asks datanode to close a container.
 */
public class CloseContainerCommand
    extends SCMCommand<CloseContainerCommandProto> {

  private HddsProtos.ReplicationType replicationType;
  private PipelineID pipelineID;

  public CloseContainerCommand(long containerID,
      HddsProtos.ReplicationType replicationType,
      PipelineID pipelineID) {
    super(containerID);
    this.replicationType = replicationType;
    this.pipelineID = pipelineID;
  }

  /**
   * Returns the type of this command.
   *
   * @return Type
   */
  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.closeContainerCommand;
  }

  /**
   * Gets the protobuf message of this object.
   *
   * @return A protobuf message.
   */
  @Override
  public byte[] getProtoBufMessage() {
    return getProto().toByteArray();
  }

  public CloseContainerCommandProto getProto() {
    return CloseContainerCommandProto.newBuilder()
        .setContainerID(getId())
        .setCmdId(getId())
        .setReplicationType(replicationType)
        .setPipelineID(pipelineID.getProtobuf())
        .build();
  }

  public static CloseContainerCommand getFromProtobuf(
      CloseContainerCommandProto closeContainerProto) {
    Preconditions.checkNotNull(closeContainerProto);
    return new CloseContainerCommand(closeContainerProto.getCmdId(),
        closeContainerProto.getReplicationType(),
        PipelineID.getFromProtobuf(closeContainerProto.getPipelineID()));
  }

  public long getContainerID() {
    return getId();
  }
}
