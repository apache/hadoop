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

import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;

/**
 * A class that is used to communicate status of datanode commands.
 */
public class CommandStatus {

  private SCMCommandProto.Type type;
  private Long cmdId;
  private Status status;
  private String msg;

  CommandStatus(Type type, Long cmdId, Status status, String msg) {
    this.type = type;
    this.cmdId = cmdId;
    this.status = status;
    this.msg = msg;
  }

  public Type getType() {
    return type;
  }

  public Long getCmdId() {
    return cmdId;
  }

  public Status getStatus() {
    return status;
  }

  public String getMsg() {
    return msg;
  }

  /**
   * To allow change of status once commandStatus is initialized.
   *
   * @param status
   */
  public void setStatus(Status status) {
    this.status = status;
  }

  public void setStatus(boolean cmdExecuted) {
    setStatus(cmdExecuted ? Status.EXECUTED : Status.FAILED);
  }

  /**
   * Returns a CommandStatus from the protocol buffers.
   *
   * @param cmdStatusProto - protoBuf Message
   * @return CommandStatus
   */
  public CommandStatus getFromProtoBuf(
      StorageContainerDatanodeProtocolProtos.CommandStatus cmdStatusProto) {
    return CommandStatusBuilder.newBuilder()
        .setCmdId(cmdStatusProto.getCmdId())
        .setStatus(cmdStatusProto.getStatus())
        .setType(cmdStatusProto.getType())
        .setMsg(cmdStatusProto.getMsg())
        .build();
  }
  /**
   * Returns a CommandStatus from the protocol buffers.
   *
   * @return StorageContainerDatanodeProtocolProtos.CommandStatus
   */
  public StorageContainerDatanodeProtocolProtos.CommandStatus
      getProtoBufMessage() {
    StorageContainerDatanodeProtocolProtos.CommandStatus.Builder builder =
        StorageContainerDatanodeProtocolProtos.CommandStatus.newBuilder()
            .setCmdId(this.getCmdId())
            .setStatus(this.getStatus())
            .setType(this.getType());
    if (this.getMsg() != null) {
      builder.setMsg(this.getMsg());
    }
    return builder.build();
  }

  /**
   * Builder class for CommandStatus.
   */
  public static class CommandStatusBuilder {

    private SCMCommandProto.Type type;
    private Long cmdId;
    private StorageContainerDatanodeProtocolProtos.CommandStatus.Status status;
    private String msg;

    CommandStatusBuilder() {
    }

    public static CommandStatusBuilder newBuilder() {
      return new CommandStatusBuilder();
    }

    public Type getType() {
      return type;
    }

    public Long getCmdId() {
      return cmdId;
    }

    public Status getStatus() {
      return status;
    }

    public String getMsg() {
      return msg;
    }

    public CommandStatusBuilder setType(Type commandType) {
      this.type = commandType;
      return this;
    }

    public CommandStatusBuilder setCmdId(Long commandId) {
      this.cmdId = commandId;
      return this;
    }

    public CommandStatusBuilder setStatus(Status commandStatus) {
      this.status = commandStatus;
      return this;
    }

    public CommandStatusBuilder setMsg(String message) {
      this.msg = message;
      return this;
    }

    public CommandStatus build() {
      return new CommandStatus(type, cmdId, status, msg);
    }
  }
}
