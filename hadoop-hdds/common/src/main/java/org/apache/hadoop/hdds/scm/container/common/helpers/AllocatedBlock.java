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

import org.apache.hadoop.hdds.client.BlockID;

/**
 * Allocated block wraps the result returned from SCM#allocateBlock which
 * contains a Pipeline and the key.
 */
public final class AllocatedBlock {
  private Pipeline pipeline;
  private BlockID blockID;
  // Indicates whether the client should create container before writing block.
  private boolean shouldCreateContainer;

  /**
   * Builder for AllocatedBlock.
   */
  public static class Builder {
    private Pipeline pipeline;
    private BlockID blockID;
    private boolean shouldCreateContainer;

    public Builder setPipeline(Pipeline p) {
      this.pipeline = p;
      return this;
    }

    public Builder setBlockID(BlockID blockId) {
      this.blockID = blockId;
      return this;
    }

    public Builder setShouldCreateContainer(boolean shouldCreate) {
      this.shouldCreateContainer = shouldCreate;
      return this;
    }

    public AllocatedBlock build() {
      return new AllocatedBlock(pipeline, blockID, shouldCreateContainer);
    }
  }

  private AllocatedBlock(Pipeline pipeline, BlockID blockID,
      boolean shouldCreateContainer) {
    this.pipeline = pipeline;
    this.blockID = blockID;
    this.shouldCreateContainer = shouldCreateContainer;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public BlockID getBlockID() {
    return blockID;
  }

  public boolean getCreateContainer() {
    return shouldCreateContainer;
  }
}
