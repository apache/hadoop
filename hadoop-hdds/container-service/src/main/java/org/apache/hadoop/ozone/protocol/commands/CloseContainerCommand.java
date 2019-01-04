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
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.CloseContainerCommandProto;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;

/**
 * Asks datanode to close a container.
 */
public class CloseContainerCommand
    extends SCMCommand<CloseContainerCommandProto> {

  private final PipelineID pipelineID;
  private boolean force;

  public CloseContainerCommand(final long containerID,
      final PipelineID pipelineID) {
    this(containerID, pipelineID, false);
  }

  public CloseContainerCommand(final long containerID,
      final PipelineID pipelineID, boolean force) {
    super(containerID);
    this.pipelineID = pipelineID;
    this.force = force;
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

  @Override
  public CloseContainerCommandProto getProto() {
    return CloseContainerCommandProto.newBuilder()
        .setContainerID(getId())
        .setCmdId(getId())
        .setPipelineID(pipelineID.getProtobuf())
        .setForce(force)
        .build();
  }

  public static CloseContainerCommand getFromProtobuf(
      CloseContainerCommandProto closeContainerProto) {
    Preconditions.checkNotNull(closeContainerProto);
    return new CloseContainerCommand(closeContainerProto.getCmdId(),
        PipelineID.getFromProtobuf(closeContainerProto.getPipelineID()),
        closeContainerProto.getForce());
  }

  public long getContainerID() {
    return getId();
  }

  public PipelineID getPipelineID() {
    return pipelineID;
  }
}
