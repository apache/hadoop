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
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.CreatePipelineCommandProto;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Asks datanode to create a pipeline.
 */
public class CreatePipelineCommand
    extends SCMCommand<CreatePipelineCommandProto> {

  private final PipelineID pipelineID;
  private final ReplicationFactor factor;
  private final ReplicationType type;
  private final List<DatanodeDetails> nodelist;

  public CreatePipelineCommand(final PipelineID pipelineID,
      final ReplicationType type, final ReplicationFactor factor,
      final List<DatanodeDetails> datanodeList) {
    super();
    this.pipelineID = pipelineID;
    this.factor = factor;
    this.type = type;
    this.nodelist = datanodeList;
  }

  public CreatePipelineCommand(long cmdId, final PipelineID pipelineID,
      final ReplicationType type, final ReplicationFactor factor,
      final List<DatanodeDetails> datanodeList) {
    super(cmdId);
    this.pipelineID = pipelineID;
    this.factor = factor;
    this.type = type;
    this.nodelist = datanodeList;
  }

  /**
   * Returns the type of this command.
   *
   * @return Type
   */
  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.createPipelineCommand;
  }

  @Override
  public CreatePipelineCommandProto getProto() {
    return CreatePipelineCommandProto.newBuilder()
        .setCmdId(getId())
        .setPipelineID(pipelineID.getProtobuf())
        .setFactor(factor)
        .setType(type)
        .addAllDatanode(nodelist.stream()
            .map(DatanodeDetails::getProtoBufMessage)
            .collect(Collectors.toList()))
        .build();
  }

  public static CreatePipelineCommand getFromProtobuf(
      CreatePipelineCommandProto createPipelineProto) {
    Preconditions.checkNotNull(createPipelineProto);
    return new CreatePipelineCommand(createPipelineProto.getCmdId(),
        PipelineID.getFromProtobuf(createPipelineProto.getPipelineID()),
        createPipelineProto.getType(), createPipelineProto.getFactor(),
        createPipelineProto.getDatanodeList().stream()
            .map(DatanodeDetails::getFromProtoBuf)
            .collect(Collectors.toList()));
  }

  public PipelineID getPipelineID() {
    return pipelineID;
  }
}
