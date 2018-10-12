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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a group of datanodes which store a container.
 */
public final class Pipeline {

  private final PipelineID id;
  private final ReplicationType type;
  private final ReplicationFactor factor;

  private LifeCycleState state;
  private List<DatanodeDetails> nodes;

  private Pipeline(PipelineID id, ReplicationType type,
      ReplicationFactor factor, LifeCycleState state,
      List<DatanodeDetails> nodes) {
    this.id = id;
    this.type = type;
    this.factor = factor;
    this.state = state;
    this.nodes = nodes;
  }

  /**
   * Returns the ID of this pipeline.
   *
   * @return PipelineID
   */
  public PipelineID getID() {
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
  public LifeCycleState getLifeCycleState() {
    return state;
  }

  /**
   * Returns the list of nodes which form this pipeline.
   *
   * @return List of DatanodeDetails
   */
  public List<DatanodeDetails> getNodes() {
    return new ArrayList<>(nodes);
  }

  public HddsProtos.Pipeline getProtobufMessage() {
    HddsProtos.Pipeline.Builder builder = HddsProtos.Pipeline.newBuilder();
    builder.setId(id.getProtobuf());
    builder.setType(type);
    builder.setState(state);
    builder.addAllMembers(nodes.stream().map(
        DatanodeDetails::getProtoBufMessage).collect(Collectors.toList()));
    return builder.build();
  }

  public static Pipeline fromProtobuf(HddsProtos.Pipeline pipeline) {
    return new Pipeline(PipelineID.getFromProtobuf(pipeline.getId()),
        pipeline.getType(), pipeline.getFactor(), pipeline.getState(),
        pipeline.getMembersList().stream().map(DatanodeDetails::getFromProtoBuf)
            .collect(Collectors.toList()));
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
        .append(state, that.state)
        .append(nodes, that.nodes)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(id)
        .append(type)
        .append(factor)
        .append(state)
        .append(nodes)
        .toHashCode();
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
    private LifeCycleState state = null;
    private List<DatanodeDetails> nodes = null;

    public Builder() {}

    public Builder(Pipeline pipeline) {
      this.id = pipeline.getID();
      this.type = pipeline.getType();
      this.factor = pipeline.getFactor();
      this.state = pipeline.getLifeCycleState();
      this.nodes = pipeline.getNodes();
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

    public Builder setState(LifeCycleState state1) {
      this.state = state1;
      return this;
    }

    public Builder setNodes(List<DatanodeDetails> nodes1) {
      this.nodes = nodes1;
      return this;
    }

    public Pipeline build() {
      Preconditions.checkNotNull(id);
      Preconditions.checkNotNull(type);
      Preconditions.checkNotNull(factor);
      Preconditions.checkNotNull(state);
      Preconditions.checkNotNull(nodes);
      return new Pipeline(id, type, factor, state, nodes);
    }
  }
}
