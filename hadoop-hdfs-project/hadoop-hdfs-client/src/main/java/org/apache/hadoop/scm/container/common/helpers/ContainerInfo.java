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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.util.Time;

/** Class wraps ozone container info. */
public class ContainerInfo {
  private OzoneProtos.LifeCycleState state;
  private Pipeline pipeline;
  // The wall-clock ms since the epoch at which the current state enters.
  private long stateEnterTime;
  private OzoneProtos.Owner owner;
  private String containerName;

  ContainerInfo(
      final String containerName,
      OzoneProtos.LifeCycleState state,
      Pipeline pipeline,
      long stateEnterTime,
      OzoneProtos.Owner owner) {
    this.containerName = containerName;
    this.pipeline = pipeline;
    this.state = state;
    this.stateEnterTime = stateEnterTime;
    this.owner = owner;
  }

  public ContainerInfo(ContainerInfo container) {
    this.pipeline = container.getPipeline();
    this.state = container.getState();
    this.containerName = container.getContainerName();
    this.stateEnterTime = container.getStateEnterTime();
    this.owner = container.getOwner();
  }

  /**
   * Needed for serialization findbugs.
   */
  public ContainerInfo() {
  }

  public static ContainerInfo fromProtobuf(OzoneProtos.SCMContainerInfo info) {
    ContainerInfo.Builder builder = new ContainerInfo.Builder();
    builder.setPipeline(Pipeline.getFromProtoBuf(info.getPipeline()));
    builder.setState(info.getState());
    builder.setStateEnterTime(info.getStateEnterTime());
    builder.setOwner(info.getOwner());
    builder.setContainerName(info.getContainerName());
    return builder.build();
  }

  public String getContainerName() {
    return containerName;
  }

  public void setContainerName(String containerName) {
    this.containerName = containerName;
  }

  public OzoneProtos.LifeCycleState getState() {
    return state;
  }

  /**
   * Update the current container state and state enter time to now.
   *
   * @param state
   */
  public void setState(OzoneProtos.LifeCycleState state) {
    this.state = state;
    this.stateEnterTime = Time.monotonicNow();
  }

  public long getStateEnterTime() {
    return stateEnterTime;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public OzoneProtos.SCMContainerInfo getProtobuf() {
    OzoneProtos.SCMContainerInfo.Builder builder =
        OzoneProtos.SCMContainerInfo.newBuilder();
    builder.setPipeline(getPipeline().getProtobufMessage());
    builder.setState(state);
    builder.setStateEnterTime(stateEnterTime);

    if (getOwner() != null) {
      builder.setOwner(getOwner());
    }
    builder.setContainerName(getContainerName());
    return builder.build();
  }

  public OzoneProtos.Owner getOwner() {
    return owner;
  }

  public void setOwner(OzoneProtos.Owner owner) {
    this.owner = owner;
  }

  @Override
  public String toString() {
    return "ContainerInfo{"
        + "state=" + state
        + ", pipeline=" + pipeline
        + ", stateEnterTime=" + stateEnterTime
        + ", owner=" + owner
        + ", containerName='" + containerName
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ContainerInfo that = (ContainerInfo) o;

    return new EqualsBuilder()
        .append(state, that.state)
        .append(pipeline.getContainerName(), that.pipeline.getContainerName())

        // TODO : Fix this later. If we add these factors some tests fail.
        // So Commenting this to continue and will enforce this with
        // Changes in pipeline where we remove Container Name to
        // SCMContainerinfo from Pipline.
        // .append(pipeline.getFactor(), that.pipeline.getFactor())
        // .append(pipeline.getType(), that.pipeline.getType())
        .append(owner, that.owner)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(11, 811)
        .append(state)
        .append(pipeline.getContainerName())
        .append(pipeline.getFactor())
        .append(pipeline.getType())
        .append(owner)
        .toHashCode();
  }

  /** Builder class for ContainerInfo. */
  public static class Builder {
    private OzoneProtos.LifeCycleState state;
    private Pipeline pipeline;
    private long stateEnterTime;
    private OzoneProtos.Owner owner;
    private String containerName;

    public Builder setState(OzoneProtos.LifeCycleState lifeCycleState) {
      this.state = lifeCycleState;
      return this;
    }

    public Builder setPipeline(Pipeline pipeline) {
      this.pipeline = pipeline;
      return this;
    }

    public Builder setStateEnterTime(long stateEnterTime) {
      this.stateEnterTime = stateEnterTime;
      return this;
    }

    public Builder setOwner(OzoneProtos.Owner owner) {
      this.owner = owner;
      return this;
    }

    public Builder setContainerName(String containerName) {
      this.containerName = containerName;
      return this;
    }

    public ContainerInfo build() {
      return new
          ContainerInfo(containerName, state, pipeline, stateEnterTime, owner);
    }
  }
}
