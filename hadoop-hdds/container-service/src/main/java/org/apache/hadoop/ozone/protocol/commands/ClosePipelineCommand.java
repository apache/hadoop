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
    .StorageContainerDatanodeProtocolProtos.ClosePipelineCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;

/**
 * Asks datanode to close a pipeline.
 */
public class ClosePipelineCommand
    extends SCMCommand<ClosePipelineCommandProto> {

  private final PipelineID pipelineID;

  public ClosePipelineCommand(final PipelineID pipelineID) {
    super();
    this.pipelineID = pipelineID;
  }

  public ClosePipelineCommand(long cmdId, final PipelineID pipelineID) {
    super(cmdId);
    this.pipelineID = pipelineID;
  }

  /**
   * Returns the type of this command.
   *
   * @return Type
   */
  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.closePipelineCommand;
  }

  @Override
  public ClosePipelineCommandProto getProto() {
    ClosePipelineCommandProto.Builder builder =
        ClosePipelineCommandProto.newBuilder();
    builder.setCmdId(getId());
    builder.setPipelineID(pipelineID.getProtobuf());
    return builder.build();
  }

  public static ClosePipelineCommand getFromProtobuf(
      ClosePipelineCommandProto createPipelineProto) {
    Preconditions.checkNotNull(createPipelineProto);
    return new ClosePipelineCommand(createPipelineProto.getCmdId(),
        PipelineID.getFromProtobuf(createPipelineProto.getPipelineID()));
  }

  public PipelineID getPipelineID() {
    return pipelineID;
  }
}
