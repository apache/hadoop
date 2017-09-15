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

import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.util.Time;

/**
 * Class wraps ozone container info.
 */
public class ContainerInfo {
  private OzoneProtos.LifeCycleState state;
  private Pipeline pipeline;
  // The wall-clock ms since the epoch at which the current state enters.
  private long stateEnterTime;

  ContainerInfo(OzoneProtos.LifeCycleState state, Pipeline pipeline,
      long stateEnterTime) {
    this.pipeline = pipeline;
    this.state = state;
    this.stateEnterTime = stateEnterTime;
  }

  public ContainerInfo(ContainerInfo container) {
    this.pipeline = container.getPipeline();
    this.state = container.getState();
    this.stateEnterTime = container.getStateEnterTime();
  }

  /**
   * Update the current container state and state enter time to now.
   * @param state
   */
  public void setState(OzoneProtos.LifeCycleState state) {
    this.state = state;
    this.stateEnterTime = Time.monotonicNow();
  }

  public OzoneProtos.LifeCycleState getState() {
    return state;
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
    return builder.build();
  }

  public static ContainerInfo fromProtobuf(
      OzoneProtos.SCMContainerInfo info) {
    ContainerInfo.Builder builder = new ContainerInfo.Builder();
    builder.setPipeline(Pipeline.getFromProtoBuf(info.getPipeline()));
    builder.setState(info.getState());
    builder.setStateEnterTime(info.getStateEnterTime());
    return builder.build();
  }

  /** Builder class for ContainerInfo. */
  public static class Builder {
    private OzoneProtos.LifeCycleState state;
    private Pipeline pipeline;
    private long stateEnterTime;

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

    public ContainerInfo build() {
      return new ContainerInfo(state, pipeline, stateEnterTime);
    }
  }
}
