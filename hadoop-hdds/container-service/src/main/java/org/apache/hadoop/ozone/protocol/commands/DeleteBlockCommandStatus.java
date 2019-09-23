/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.protocol.commands;

import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto;

/**
 * Command status to report about block deletion.
 */
public class DeleteBlockCommandStatus extends CommandStatus {

  private ContainerBlocksDeletionACKProto blocksDeletionAck = null;

  public DeleteBlockCommandStatus(Type type, Long cmdId,
      StorageContainerDatanodeProtocolProtos.CommandStatus.Status status,
      String msg,
      ContainerBlocksDeletionACKProto blocksDeletionAck) {
    super(type, cmdId, status, msg);
    this.blocksDeletionAck = blocksDeletionAck;
  }

  public void setBlocksDeletionAck(
      ContainerBlocksDeletionACKProto deletionAck) {
    blocksDeletionAck = deletionAck;
  }

  @Override
  public CommandStatus getFromProtoBuf(
      StorageContainerDatanodeProtocolProtos.CommandStatus cmdStatusProto) {
    return DeleteBlockCommandStatusBuilder.newBuilder()
        .setBlockDeletionAck(cmdStatusProto.getBlockDeletionAck())
        .setCmdId(cmdStatusProto.getCmdId())
        .setStatus(cmdStatusProto.getStatus())
        .setType(cmdStatusProto.getType())
        .setMsg(cmdStatusProto.getMsg())
        .build();
  }

  @Override
  public StorageContainerDatanodeProtocolProtos.CommandStatus
      getProtoBufMessage() {
    StorageContainerDatanodeProtocolProtos.CommandStatus.Builder builder =
        StorageContainerDatanodeProtocolProtos.CommandStatus.newBuilder()
            .setCmdId(this.getCmdId())
            .setStatus(this.getStatus())
            .setType(this.getType());
    if (blocksDeletionAck != null) {
      builder.setBlockDeletionAck(blocksDeletionAck);
    }
    if (this.getMsg() != null) {
      builder.setMsg(this.getMsg());
    }
    return builder.build();
  }

  /**
   * Builder for DeleteBlockCommandStatus.
   */
  public static final class DeleteBlockCommandStatusBuilder
      extends CommandStatusBuilder {
    private ContainerBlocksDeletionACKProto blocksDeletionAck = null;

    public static DeleteBlockCommandStatusBuilder newBuilder() {
      return new DeleteBlockCommandStatusBuilder();
    }

    public DeleteBlockCommandStatusBuilder setBlockDeletionAck(
        ContainerBlocksDeletionACKProto deletionAck) {
      this.blocksDeletionAck = deletionAck;
      return this;
    }

    @Override
    public CommandStatus build() {
      return new DeleteBlockCommandStatus(getType(), getCmdId(), getStatus(),
          getMsg(), blocksDeletionAck);
    }

  }
}
