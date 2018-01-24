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

package org.apache.hadoop.hdfs.server.common.sps;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * This class represents status from a block movement task. This will have the
 * information of the task which was successful or failed due to errors.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockMovementAttemptFinished {
  private final Block block;
  private final DatanodeInfo src;
  private final DatanodeInfo target;
  private final BlockMovementStatus status;

  /**
   * Construct movement attempt finished info.
   *
   * @param block
   *          block
   * @param src
   *          src datanode
   * @param target
   *          target datanode
   * @param status
   *          movement status
   */
  public BlockMovementAttemptFinished(Block block, DatanodeInfo src,
      DatanodeInfo target, BlockMovementStatus status) {
    this.block = block;
    this.src = src;
    this.target = target;
    this.status = status;
  }

  /**
   * @return details of the block, which attempted to move from src to target
   *         node.
   */
  public Block getBlock() {
    return block;
  }

  /**
   * @return block movement status code.
   */
  public BlockMovementStatus getStatus() {
    return status;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("Block movement attempt finished(\n  ")
        .append(" block : ").append(block).append(" src node: ").append(src)
        .append(" target node: ").append(target).append(" movement status: ")
        .append(status).append(")").toString();
  }
}