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

import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

/**
 * Allocated block wraps the result returned from SCM#allocateBlock which
 * contains a Pipeline and the key.
 */
public final class AllocatedBlock {
  private Pipeline pipeline;
  private ContainerBlockID containerBlockID;

  /**
   * Builder for AllocatedBlock.
   */
  public static class Builder {
    private Pipeline pipeline;
    private ContainerBlockID containerBlockID;

    public Builder setPipeline(Pipeline p) {
      this.pipeline = p;
      return this;
    }

    public Builder setContainerBlockID(ContainerBlockID blockId) {
      this.containerBlockID = blockId;
      return this;
    }

    public AllocatedBlock build() {
      return new AllocatedBlock(pipeline, containerBlockID);
    }
  }

  private AllocatedBlock(Pipeline pipeline, ContainerBlockID containerBlockID) {
    this.pipeline = pipeline;
    this.containerBlockID = containerBlockID;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public ContainerBlockID getBlockID() {
    return containerBlockID;
  }
}
