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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A pipeline represents the group of machines over which a container lives.
 */
public class Pipeline {
  static final String PIPELINE_INFO = "PIPELINE_INFO_FILTER";
  private static final ObjectWriter WRITER;

  static {
    ObjectMapper mapper = new ObjectMapper();
    String[] ignorableFieldNames = {"data", "leaderID", "datanodes"};
    FilterProvider filters = new SimpleFilterProvider()
        .addFilter(PIPELINE_INFO, SimpleBeanPropertyFilter
            .serializeAllExcept(ignorableFieldNames));
    mapper.setVisibility(PropertyAccessor.FIELD,
        JsonAutoDetect.Visibility.ANY);
    mapper.addMixIn(Object.class, MixIn.class);

    WRITER = mapper.writer(filters);
  }

  private String containerName;
  private String leaderID;
  private Map<String, DatanodeID> datanodes;
  private OzoneProtos.LifeCycleState lifeCycleState;
  private OzoneProtos.ReplicationType type;
  private OzoneProtos.ReplicationFactor factor;
  private String pipelineName;
  /**
   * Allows you to maintain private data on pipelines. This is not serialized
   * via protobuf, just allows us to maintain some private data.
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
  public static Pipeline getFromProtoBuf(OzoneProtos.Pipeline pipeline) {
    Preconditions.checkNotNull(pipeline);
    Pipeline newPipeline = new Pipeline(pipeline.getLeaderID());
    for (HdfsProtos.DatanodeIDProto dataID : pipeline.getMembersList()) {
      newPipeline.addMember(DatanodeID.getFromProtoBuf(dataID));
    }

    newPipeline.setContainerName(pipeline.getContainerName());
    newPipeline.setLifeCycleState(pipeline.getState());
    newPipeline.setType(pipeline.getType());
    newPipeline.setFactor(pipeline.getFactor());
    if (pipeline.hasPipelineName()) {
      newPipeline.setPipelineName(pipeline.getPipelineName());
    }
    return newPipeline;
  }

  public OzoneProtos.ReplicationFactor getFactor() {
    return factor;
  }

  public void setFactor(OzoneProtos.ReplicationFactor factor) {
    this.factor = factor;
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
  @JsonIgnore
  public DatanodeID getLeader() {
    return datanodes.get(leaderID);
  }

  /**
   * Returns the leader host.
   *
   * @return First Machine.
   */
  public String getLeaderHost() {
    return datanodes.get(leaderID).getHostName();
  }

  /**
   * Returns all machines that make up this pipeline.
   *
   * @return List of Machines.
   */
  @JsonIgnore
  public List<DatanodeID> getMachines() {
    return new ArrayList<>(datanodes.values());
  }

  /**
   * Returns all machines that make up this pipeline.
   *
   * @return List of Machines.
   */
  public List<String> getDatanodeHosts() {
    List<String> dataHosts = new ArrayList<>();
    for (DatanodeID id : datanodes.values()) {
      dataHosts.add(id.getHostName());
    }
    return dataHosts;
  }

  /**
   * Return a Protobuf Pipeline message from pipeline.
   *
   * @return Protobuf message
   */
  @JsonIgnore
  public OzoneProtos.Pipeline getProtobufMessage() {
    OzoneProtos.Pipeline.Builder builder =
        OzoneProtos.Pipeline.newBuilder();
    for (DatanodeID datanode : datanodes.values()) {
      builder.addMembers(datanode.getProtoBufMessage());
    }
    builder.setLeaderID(leaderID);
    builder.setContainerName(this.containerName);

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

  /**
   * Set private data on pipeline.
   *
   * @param data -- private data.
   */
  public void setData(byte[] data) {
    if (data != null) {
      this.data = Arrays.copyOf(data, data.length);
    }
  }

  /**
   * Gets the State of the pipeline.
   *
   * @return - LifeCycleStates.
   */
  public OzoneProtos.LifeCycleState getLifeCycleState() {
    return lifeCycleState;
  }

  /**
   * Sets the lifecycleState.
   *
   * @param lifeCycleStates - Enum
   */
  public void setLifeCycleState(OzoneProtos.LifeCycleState lifeCycleStates) {
    this.lifeCycleState = lifeCycleStates;
  }

  /**
   * Gets the pipeline Name.
   *
   * @return - Name of the pipeline
   */
  public String getPipelineName() {
    return pipelineName;
  }

  /**
   * Sets the pipeline name.
   *
   * @param pipelineName - Sets the name.
   */
  public void setPipelineName(String pipelineName) {
    this.pipelineName = pipelineName;
  }

  /**
   * Returns the type.
   *
   * @return type - Standalone, Ratis, Chained.
   */
  public OzoneProtos.ReplicationType getType() {
    return type;
  }

  /**
   * Sets the type of this pipeline.
   *
   * @param type - Standalone, Ratis, Chained.
   */
  public void setType(OzoneProtos.ReplicationType type) {
    this.type = type;
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(getClass().getSimpleName())
        .append("[");
    datanodes.keySet().stream()
        .forEach(id -> b.append(id.endsWith(leaderID) ? "*" + id : id));
    b.append("] container:").append(containerName);
    b.append(" name:").append(getPipelineName());
    if (getType() != null) {
      b.append(" type:").append(getType().toString());
    }
    if (getFactor() != null) {
      b.append(" factor:").append(getFactor().toString());
    }
    if (getLifeCycleState() != null) {
      b.append(" State:").append(getLifeCycleState().toString());
    }
    return b.toString();
  }

  /**
   * Returns a JSON string of this object.
   *
   * @return String - json string
   * @throws IOException
   */
  public String toJsonString() throws IOException {
    return WRITER.writeValueAsString(this);
  }

  @JsonFilter(PIPELINE_INFO)
  class MixIn {
  }
}
