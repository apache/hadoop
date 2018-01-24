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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.Block;

/**
 * Blocks movements status handler, which is used to collect details of the
 * completed block movements and later these attempted finished(with success or
 * failure) blocks can be accessed to notify respective listeners, if any.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlocksMovementsStatusHandler {
  private final List<Block> blockIdVsMovementStatus = new ArrayList<>();

  /**
   * Collect all the storage movement attempt finished blocks. Later this will
   * be send to namenode via heart beat.
   *
   * @param moveAttemptFinishedBlks
   *          set of storage movement attempt finished blocks
   */
  public void handle(
      List<BlockMovementAttemptFinished> moveAttemptFinishedBlks) {
    List<Block> blocks = new ArrayList<>();

    for (BlockMovementAttemptFinished item : moveAttemptFinishedBlks) {
      blocks.add(item.getBlock());
    }
    // Adding to the tracking report list. Later this can be accessed to know
    // the attempted block movements.
    synchronized (blockIdVsMovementStatus) {
      blockIdVsMovementStatus.addAll(blocks);
    }
  }

  /**
   * @return unmodifiable list of storage movement attempt finished blocks.
   */
  public List<Block> getMoveAttemptFinishedBlocks() {
    List<Block> moveAttemptFinishedBlks = new ArrayList<>();
    // 1. Adding all the completed block ids.
    synchronized (blockIdVsMovementStatus) {
      if (blockIdVsMovementStatus.size() > 0) {
        moveAttemptFinishedBlks = Collections
            .unmodifiableList(blockIdVsMovementStatus);
      }
    }
    return moveAttemptFinishedBlks;
  }

  /**
   * Remove the storage movement attempt finished blocks from the tracking list.
   *
   * @param moveAttemptFinishedBlks
   *          set of storage movement attempt finished blocks
   */
  public void remove(List<Block> moveAttemptFinishedBlks) {
    if (moveAttemptFinishedBlks != null) {
      blockIdVsMovementStatus.removeAll(moveAttemptFinishedBlks);
    }
  }

  /**
   * Clear the blockID vs movement status tracking map.
   */
  public void removeAll() {
    synchronized (blockIdVsMovementStatus) {
      blockIdVsMovementStatus.clear();
    }
  }
}
