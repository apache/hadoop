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

package org.apache.hadoop.scm.container.common.helpers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A pipeline represents the group of machines over which a container lives.
 */
public class Pipeline {
  private String containerName;
  private String leaderID;
  private Map<String, DatanodeID> datanodes;
  /**
   * Allows you to maintain private data on pipelines.
   * This is not serialized via protobuf, just allows us to maintain some
   * private data.
   */
  private byte[] data;

  /**
   * Constructs a new pipeline data structure.
   *
   * @param leaderID - First machine in this pipeline.
   */
  public Pipeline(String leaderID) {
    this.leaderID = leaderID;
    datanodes = new TreeMap<>();
    data = null;
  }

  /**
   * Gets pipeline object from protobuf.
   *
   * @param pipeline - ProtoBuf definition for the pipeline.
   * @return Pipeline Object
   */
  public static Pipeline getFromProtoBuf(ContainerProtos.Pipeline pipeline) {
    Preconditions.checkNotNull(pipeline);
    Pipeline newPipeline = new Pipeline(pipeline.getLeaderID());
    for (HdfsProtos.DatanodeIDProto dataID : pipeline.getMembersList()) {
      newPipeline.addMember(DatanodeID.getFromProtoBuf(dataID));
    }

    newPipeline.setContainerName(pipeline.getContainerName());
    return newPipeline;
  }


  /**
   * Adds a member to the pipeline.
   *
   * @param dataNodeId - Datanode to be added.
   */
  public void addMember(DatanodeID dataNodeId) {
    datanodes.put(dataNodeId.getDatanodeUuid(), dataNodeId);
  }

  /**
   * Returns the first machine in the set of datanodes.
   *
   * @return First Machine.
   */
  public DatanodeID getLeader() {
    return datanodes.get(leaderID);
  }

  /**
   * Returns all machines that make up this pipeline.
   *
   * @return List of Machines.
   */
  public List<DatanodeID> getMachines() {
    return new ArrayList<>(datanodes.values());
  }

  /**
   * Return a Protobuf Pipeline message from pipeline.
   *
   * @return Protobuf message
   */
  public ContainerProtos.Pipeline getProtobufMessage() {
    ContainerProtos.Pipeline.Builder builder =
        ContainerProtos.Pipeline.newBuilder();
    for (DatanodeID datanode : datanodes.values()) {
      builder.addMembers(datanode.getProtoBufMessage());
    }
    builder.setLeaderID(leaderID);
    builder.setContainerName(this.containerName);
    return builder.build();
  }

  /**
   * Returns containerName if available.
   *
   * @return String.
   */
  public String getContainerName() {
    return containerName;
  }

  /**
   * Sets the container Name.
   *
   * @param containerName - Name of the container.
   */
  public void setContainerName(String containerName) {
    this.containerName = containerName;
  }

  /**
   * Set private data on pipeline.
   * @param data -- private data.
   */
  public void setData(byte[] data) {
    if (data != null) {
      this.data = Arrays.copyOf(data, data.length);
    }
  }

  /**
   * Returns private data that is set on this pipeline.
   *
   * @return blob, the user can interpret it any way they like.
   */
  public byte[] getData() {
    if (this.data != null) {
      return Arrays.copyOf(this.data, this.data.length);
    } else {
      return null;
    }
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(getClass().getSimpleName())
        .append("[");
    datanodes.keySet().stream()
        .forEach(id -> b.append(id.endsWith(leaderID)? "*" + id : id));
    b.append("] container:").append(containerName);
    return b.toString();
  }
}
