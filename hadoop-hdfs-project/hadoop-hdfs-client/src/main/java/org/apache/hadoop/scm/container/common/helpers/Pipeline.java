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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.hdfs.protocol.DatanodeID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A pipeline represents the group of machines over which a container lives.
 */
public class Pipeline {
  static final String PIPELINE_INFO = "PIPELINE_INFO_FILTER";
  private static final ObjectWriter WRITER;

  static {
    ObjectMapper mapper = new ObjectMapper();
    String[] ignorableFieldNames = {"data"};
    FilterProvider filters = new SimpleFilterProvider()
        .addFilter(PIPELINE_INFO, SimpleBeanPropertyFilter
            .serializeAllExcept(ignorableFieldNames));
    mapper.setVisibility(PropertyAccessor.FIELD,
        JsonAutoDetect.Visibility.ANY);
    mapper.addMixIn(Object.class, MixIn.class);

    WRITER = mapper.writer(filters);
  }

  private String containerName;
  private PipelineChannel pipelineChannel;
  /**
   * Allows you to maintain private data on pipelines. This is not serialized
   * via protobuf, just allows us to maintain some private data.
   */
  @JsonIgnore
  private byte[] data;
  /**
   * Constructs a new pipeline data structure.
   *
   * @param containerName - Container
   * @param pipelineChannel - transport information for this container
   */
  public Pipeline(String containerName, PipelineChannel pipelineChannel) {
    this.containerName = containerName;
    this.pipelineChannel = pipelineChannel;
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
    PipelineChannel pipelineChannel =
        PipelineChannel.getFromProtoBuf(pipeline.getPipelineChannel());
    return new Pipeline(pipeline.getContainerName(), pipelineChannel);
  }

  public OzoneProtos.ReplicationFactor getFactor() {
    return pipelineChannel.getFactor();
  }

  /**
   * Returns the first machine in the set of datanodes.
   *
   * @return First Machine.
   */
  @JsonIgnore
  public DatanodeID getLeader() {
    return pipelineChannel.getDatanodes().get(pipelineChannel.getLeaderID());
  }

  /**
   * Returns the leader host.
   *
   * @return First Machine.
   */
  public String getLeaderHost() {
    return pipelineChannel.getDatanodes()
        .get(pipelineChannel.getLeaderID()).getHostName();
  }

  /**
   * Returns all machines that make up this pipeline.
   *
   * @return List of Machines.
   */
  @JsonIgnore
  public List<DatanodeID> getMachines() {
    return new ArrayList<>(pipelineChannel.getDatanodes().values());
  }

  /**
   * Returns all machines that make up this pipeline.
   *
   * @return List of Machines.
   */
  public List<String> getDatanodeHosts() {
    List<String> dataHosts = new ArrayList<>();
    for (DatanodeID id : pipelineChannel.getDatanodes().values()) {
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
    builder.setContainerName(this.containerName);
    builder.setPipelineChannel(this.pipelineChannel.getProtobufMessage());
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

  @VisibleForTesting
  public PipelineChannel getPipelineChannel() {
    return pipelineChannel;
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
    return pipelineChannel.getLifeCycleState();
  }

  /**
   * Gets the pipeline Name.
   *
   * @return - Name of the pipeline
   */
  public String getPipelineName() {
    return pipelineChannel.getName();
  }

  /**
   * Returns the type.
   *
   * @return type - Standalone, Ratis, Chained.
   */
  public OzoneProtos.ReplicationType getType() {
    return pipelineChannel.getType();
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(getClass().getSimpleName())
        .append("[");
    pipelineChannel.getDatanodes().keySet().stream()
        .forEach(id -> b.
            append(id.endsWith(pipelineChannel.getLeaderID()) ? "*" + id : id));
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
