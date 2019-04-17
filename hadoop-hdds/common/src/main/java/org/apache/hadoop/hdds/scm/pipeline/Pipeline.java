/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents a group of datanodes which store a container.
 */
public final class Pipeline {

  private final PipelineID id;
  private final ReplicationType type;
  private final ReplicationFactor factor;

  private PipelineState state;
  private Map<DatanodeDetails, Long> nodeStatus;

  /**
   * The immutable properties of pipeline object is used in
   * ContainerStateManager#getMatchingContainerByPipeline to take a lock on
   * the container allocations for a particular pipeline.
   */
  private Pipeline(PipelineID id, ReplicationType type,
      ReplicationFactor factor, PipelineState state,
      Map<DatanodeDetails, Long> nodeStatus) {
    this.id = id;
    this.type = type;
    this.factor = factor;
    this.state = state;
    this.nodeStatus = nodeStatus;
  }

  /**
   * Returns the ID of this pipeline.
   *
   * @return PipelineID
   */
  public PipelineID getId() {
    return id;
  }

  /**
   * Returns the type.
   *
   * @return type - Simple or Ratis.
   */
  public ReplicationType getType() {
    return type;
  }

  /**
   * Returns the factor.
   *
   * @return type - Simple or Ratis.
   */
  public ReplicationFactor getFactor() {
    return factor;
  }

  /**
   * Returns the State of the pipeline.
   *
   * @return - LifeCycleStates.
   */
  public PipelineState getPipelineState() {
    return state;
  }

  /**
   * Returns the list of nodes which form this pipeline.
   *
   * @return List of DatanodeDetails
   */
  public List<DatanodeDetails> getNodes() {
    return new ArrayList<>(nodeStatus.keySet());
  }

  public DatanodeDetails getFirstNode() throws IOException {
    if (nodeStatus.isEmpty()) {
      throw new IOException(String.format("Pipeline=%s is empty", id));
    }
    return nodeStatus.keySet().iterator().next();
  }

  public boolean isClosed() {
    return state == PipelineState.CLOSED;
  }

  public boolean isOpen() {
    return state == PipelineState.OPEN;
  }

  void reportDatanode(DatanodeDetails dn) throws IOException {
    if (nodeStatus.get(dn) == null) {
      throw new IOException(
          String.format("Datanode=%s not part of pipeline=%s", dn, id));
    }
    nodeStatus.put(dn, System.currentTimeMillis());
  }

  boolean isHealthy() {
    for (Long reportedTime : nodeStatus.values()) {
      if (reportedTime < 0) {
        return false;
      }
    }
    return true;
  }

  public boolean isEmpty() {
    return nodeStatus.isEmpty();
  }

  public HddsProtos.Pipeline getProtobufMessage()
      throws UnknownPipelineStateException {
    HddsProtos.Pipeline.Builder builder = HddsProtos.Pipeline.newBuilder()
        .setId(id.getProtobuf())
        .setType(type)
        .setFactor(factor)
        .setState(PipelineState.getProtobuf(state))
        .setLeaderID("")
        .addAllMembers(nodeStatus.keySet().stream()
            .map(DatanodeDetails::getProtoBufMessage)
            .collect(Collectors.toList()));
    return builder.build();
  }

  public static Pipeline getFromProtobuf(HddsProtos.Pipeline pipeline)
      throws UnknownPipelineStateException {
    Preconditions.checkNotNull(pipeline, "Pipeline is null");
    return new Builder().setId(PipelineID.getFromProtobuf(pipeline.getId()))
        .setFactor(pipeline.getFactor())
        .setType(pipeline.getType())
        .setState(PipelineState.fromProtobuf(pipeline.getState()))
        .setNodes(pipeline.getMembersList().stream()
            .map(DatanodeDetails::getFromProtoBuf).collect(Collectors.toList()))
        .build();
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Pipeline that = (Pipeline) o;

    return new EqualsBuilder()
        .append(id, that.id)
        .append(type, that.type)
        .append(factor, that.factor)
        .append(getNodes(), that.getNodes())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(id)
        .append(type)
        .append(factor)
        .append(nodeStatus)
        .toHashCode();
  }

  @Override
  public String toString() {
    final StringBuilder b =
        new StringBuilder(getClass().getSimpleName()).append("[");
    b.append(" Id: ").append(id.getId());
    b.append(", Nodes: ");
    nodeStatus.keySet().forEach(b::append);
    b.append(", Type:").append(getType());
    b.append(", Factor:").append(getFactor());
    b.append(", State:").append(getPipelineState());
    b.append("]");
    return b.toString();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(Pipeline pipeline) {
    return new Builder(pipeline);
  }

  /**
   * Builder class for Pipeline.
   */
  public static class Builder {
    private PipelineID id = null;
    private ReplicationType type = null;
    private ReplicationFactor factor = null;
    private PipelineState state = null;
    private Map<DatanodeDetails, Long> nodeStatus = null;

    public Builder() {}

    public Builder(Pipeline pipeline) {
      this.id = pipeline.id;
      this.type = pipeline.type;
      this.factor = pipeline.factor;
      this.state = pipeline.state;
      this.nodeStatus = pipeline.nodeStatus;
    }

    public Builder setId(PipelineID id1) {
      this.id = id1;
      return this;
    }

    public Builder setType(ReplicationType type1) {
      this.type = type1;
      return this;
    }

    public Builder setFactor(ReplicationFactor factor1) {
      this.factor = factor1;
      return this;
    }

    public Builder setState(PipelineState state1) {
      this.state = state1;
      return this;
    }

    public Builder setNodes(List<DatanodeDetails> nodes) {
      this.nodeStatus = new LinkedHashMap<>();
      nodes.forEach(node -> nodeStatus.put(node, -1L));
      return this;
    }

    public Pipeline build() {
      Preconditions.checkNotNull(id);
      Preconditions.checkNotNull(type);
      Preconditions.checkNotNull(factor);
      Preconditions.checkNotNull(state);
      Preconditions.checkNotNull(nodeStatus);
      return new Pipeline(id, type, factor, state, nodeStatus);
    }
  }

  /**
   * Possible Pipeline states in SCM.
   */
  public enum PipelineState {
    ALLOCATED, OPEN, CLOSED;

    public static PipelineState fromProtobuf(HddsProtos.PipelineState state)
        throws UnknownPipelineStateException {
      Preconditions.checkNotNull(state, "Pipeline state is null");
      switch (state) {
      case PIPELINE_ALLOCATED: return ALLOCATED;
      case PIPELINE_OPEN: return OPEN;
      case PIPELINE_CLOSED: return CLOSED;
      default:
        throw new UnknownPipelineStateException(
            "Pipeline state: " + state + " is not recognized.");
      }
    }

    public static HddsProtos.PipelineState getProtobuf(PipelineState state)
        throws UnknownPipelineStateException {
      Preconditions.checkNotNull(state, "Pipeline state is null");
      switch (state) {
      case ALLOCATED: return HddsProtos.PipelineState.PIPELINE_ALLOCATED;
      case OPEN: return HddsProtos.PipelineState.PIPELINE_OPEN;
      case CLOSED: return HddsProtos.PipelineState.PIPELINE_CLOSED;
      default:
        throw new UnknownPipelineStateException(
            "Pipeline state: " + state + " is not recognized.");
      }
    }
  }
}
