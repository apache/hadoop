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
package org.apache.hadoop.scm.container.common.helpers;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos.LifeCycleState;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos.ReplicationType;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos.ReplicationFactor;

import java.util.Map;
import java.util.TreeMap;

/**
 * PipelineChannel information for a {@link Pipeline}.
 */
public class PipelineChannel {
  @JsonIgnore
  private String leaderID;
  @JsonIgnore
  private Map<String, DatanodeID> datanodes;
  private LifeCycleState lifeCycleState;
  private ReplicationType type;
  private ReplicationFactor factor;
  private String name;

  public PipelineChannel(String leaderID, LifeCycleState lifeCycleState,
      ReplicationType replicationType, ReplicationFactor replicationFactor,
      String name) {
    this.leaderID = leaderID;
    this.lifeCycleState = lifeCycleState;
    this.type = replicationType;
    this.factor = replicationFactor;
    this.name = name;
    datanodes = new TreeMap<>();
  }

  public String getLeaderID() {
    return leaderID;
  }

  public Map<String, DatanodeID> getDatanodes() {
    return datanodes;
  }

  public LifeCycleState getLifeCycleState() {
    return lifeCycleState;
  }

  public ReplicationType getType() {
    return type;
  }

  public ReplicationFactor getFactor() {
    return factor;
  }

  public String getName() {
    return name;
  }

  public void addMember(DatanodeID dataNodeId) {
    datanodes.put(dataNodeId.getDatanodeUuid(), dataNodeId);
  }

  @JsonIgnore
  public HdslProtos.PipelineChannel getProtobufMessage() {
    HdslProtos.PipelineChannel.Builder builder =
        HdslProtos.PipelineChannel.newBuilder();
    for (DatanodeID datanode : datanodes.values()) {
      builder.addMembers(datanode.getProtoBufMessage());
    }
    builder.setLeaderID(leaderID);

    if (this.getLifeCycleState() != null) {
      builder.setState(this.getLifeCycleState());
    }
    if (this.getType() != null) {
      builder.setType(this.getType());
    }

    if (this.getFactor() != null) {
      builder.setFactor(this.getFactor());
    }
    return builder.build();
  }

  public static PipelineChannel getFromProtoBuf(
      HdslProtos.PipelineChannel transportProtos) {
    Preconditions.checkNotNull(transportProtos);
    PipelineChannel pipelineChannel =
        new PipelineChannel(transportProtos.getLeaderID(),
            transportProtos.getState(),
            transportProtos.getType(),
            transportProtos.getFactor(),
            transportProtos.getName());

    for (HdfsProtos.DatanodeIDProto dataID : transportProtos.getMembersList()) {
      pipelineChannel.addMember(DatanodeID.getFromProtoBuf(dataID));
    }
    return pipelineChannel;
  }
}
