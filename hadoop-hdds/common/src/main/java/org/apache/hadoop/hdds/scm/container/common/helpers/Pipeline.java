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

package org.apache.hadoop.hdds.scm.container.common.helpers;

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
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.List;

/**
 * A pipeline represents the group of machines over which a container lives.
 */
public class Pipeline {
  static final String PIPELINE_INFO = "PIPELINE_INFO_FILTER";
  private static final ObjectWriter WRITER;

  static {
    ObjectMapper mapper = new ObjectMapper();
    String[] ignorableFieldNames = {"leaderID", "datanodes"};
    FilterProvider filters = new SimpleFilterProvider()
        .addFilter(PIPELINE_INFO, SimpleBeanPropertyFilter
            .serializeAllExcept(ignorableFieldNames));
    mapper.setVisibility(PropertyAccessor.FIELD,
        JsonAutoDetect.Visibility.ANY);
    mapper.addMixIn(Object.class, MixIn.class);

    WRITER = mapper.writer(filters);
  }

  @JsonIgnore
  private String leaderID;
  @JsonIgnore
  private Map<String, DatanodeDetails> datanodes;
  private HddsProtos.LifeCycleState lifeCycleState;
  private HddsProtos.ReplicationType type;
  private HddsProtos.ReplicationFactor factor;
  private PipelineID id;

  /**
   * Constructs a new pipeline data structure.
   *
   * @param leaderID       -  Leader datanode id
   * @param lifeCycleState  - Pipeline State
   * @param replicationType - Replication protocol
   * @param replicationFactor - replication count on datanodes
   * @param id  - pipeline ID
   */
  public Pipeline(String leaderID, HddsProtos.LifeCycleState lifeCycleState,
      HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor, PipelineID id) {
    this.leaderID = leaderID;
    this.lifeCycleState = lifeCycleState;
    this.type = replicationType;
    this.factor = replicationFactor;
    this.id = id;
    datanodes = new TreeMap<>();
  }

  /**
   * Gets pipeline object from protobuf.
   *
   * @param pipelineProto - ProtoBuf definition for the pipeline.
   * @return Pipeline Object
   */
  public static Pipeline getFromProtoBuf(
      HddsProtos.Pipeline pipelineProto) {
    Preconditions.checkNotNull(pipelineProto);
    Pipeline pipeline =
        new Pipeline(pipelineProto.getLeaderID(),
            pipelineProto.getState(),
            pipelineProto.getType(),
            pipelineProto.getFactor(),
            PipelineID.getFromProtobuf(pipelineProto.getId()));

    for (HddsProtos.DatanodeDetailsProto dataID :
        pipelineProto.getMembersList()) {
      pipeline.addMember(DatanodeDetails.getFromProtoBuf(dataID));
    }
    return pipeline;
  }

  /**
   * returns the replication count.
   * @return Replication Factor
   */
  public HddsProtos.ReplicationFactor getFactor() {
    return factor;
  }

  /**
   * Returns the first machine in the set of datanodes.
   *
   * @return First Machine.
   */
  @JsonIgnore
  public DatanodeDetails getLeader() {
    return getDatanodes().get(leaderID);
  }

  public void addMember(DatanodeDetails datanodeDetails) {
    datanodes.put(datanodeDetails.getUuid().toString(),
        datanodeDetails);
  }

  public Map<String, DatanodeDetails> getDatanodes() {
    return datanodes;
  }
  /**
   * Returns the leader host.
   *
   * @return First Machine.
   */
  public String getLeaderHost() {
    return getDatanodes()
        .get(leaderID).getHostName();
  }

  /**
   *
   * @return lead
   */
  public String getLeaderID() {
    return leaderID;
  }
  /**
   * Returns all machines that make up this pipeline.
   *
   * @return List of Machines.
   */
  @JsonIgnore
  public List<DatanodeDetails> getMachines() {
    return new ArrayList<>(getDatanodes().values());
  }

  /**
   * Returns all machines that make up this pipeline.
   *
   * @return List of Machines.
   */
  public List<String> getDatanodeHosts() {
    List<String> dataHosts = new ArrayList<>();
    for (DatanodeDetails id :getDatanodes().values()) {
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
  public HddsProtos.Pipeline getProtobufMessage() {
    HddsProtos.Pipeline.Builder builder =
        HddsProtos.Pipeline.newBuilder();
    for (DatanodeDetails datanode : datanodes.values()) {
      builder.addMembers(datanode.getProtoBufMessage());
    }
    builder.setLeaderID(leaderID);

    if (lifeCycleState != null) {
      builder.setState(lifeCycleState);
    }
    if (type != null) {
      builder.setType(type);
    }

    if (factor != null) {
      builder.setFactor(factor);
    }

    if (id != null) {
      builder.setId(id.getProtobuf());
    }
    return builder.build();
  }

  /**
   * Gets the State of the pipeline.
   *
   * @return - LifeCycleStates.
   */
  public HddsProtos.LifeCycleState getLifeCycleState() {
    return lifeCycleState;
  }

  /**
   * Update the State of the pipeline.
   */
  public void setLifeCycleState(HddsProtos.LifeCycleState nextState) {
     lifeCycleState = nextState;
  }

  /**
   * Gets the pipeline id.
   *
   * @return - Id of the pipeline
   */
  public PipelineID getId() {
    return id;
  }

  /**
   * Returns the type.
   *
   * @return type - Standalone, Ratis, Chained.
   */
  public HddsProtos.ReplicationType getType() {
    return type;
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(getClass().getSimpleName())
        .append("[");
    getDatanodes().keySet().stream()
        .forEach(id -> b.
            append(id.endsWith(getLeaderID()) ? "*" + id : id));
    b.append(" id:").append(id);
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
