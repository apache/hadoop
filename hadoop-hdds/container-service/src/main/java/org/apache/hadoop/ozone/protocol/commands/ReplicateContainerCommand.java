/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.protocol.commands;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ReplicateContainerCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ReplicateContainerCommandProto
    .Builder;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;

import com.google.common.base.Preconditions;

/**
 * SCM command to request replication of a container.
 */
public class ReplicateContainerCommand
    extends SCMCommand<ReplicateContainerCommandProto> {

  private final long containerID;
  private final List<DatanodeDetails> sourceDatanodes;

  public ReplicateContainerCommand(long containerID,
      List<DatanodeDetails> sourceDatanodes) {
    super();
    this.containerID = containerID;
    this.sourceDatanodes = sourceDatanodes;
  }

  // Should be called only for protobuf conversion
  public ReplicateContainerCommand(long containerID,
      List<DatanodeDetails> sourceDatanodes, long id) {
    super(id);
    this.containerID = containerID;
    this.sourceDatanodes = sourceDatanodes;
  }

  @Override
  public Type getType() {
    return SCMCommandProto.Type.replicateContainerCommand;
  }

  @Override
  public ReplicateContainerCommandProto getProto() {
    Builder builder = ReplicateContainerCommandProto.newBuilder()
        .setCmdId(getId())
        .setContainerID(containerID);
    for (DatanodeDetails dd : sourceDatanodes) {
      builder.addSources(dd.getProtoBufMessage());
    }
    return builder.build();
  }

  public static ReplicateContainerCommand getFromProtobuf(
      ReplicateContainerCommandProto protoMessage) {
    Preconditions.checkNotNull(protoMessage);

    List<DatanodeDetails> datanodeDetails =
        protoMessage.getSourcesList()
            .stream()
            .map(DatanodeDetails::getFromProtoBuf)
            .collect(Collectors.toList());

    return new ReplicateContainerCommand(protoMessage.getContainerID(),
        datanodeDetails, protoMessage.getCmdId());

  }

  public long getContainerID() {
    return containerID;
  }

  public List<DatanodeDetails> getSourceDatanodes() {
    return sourceDatanodes;
  }
}
