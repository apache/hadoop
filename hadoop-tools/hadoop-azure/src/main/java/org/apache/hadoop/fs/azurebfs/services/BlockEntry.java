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

package org.apache.hadoop.fs.azurebfs.services;

/**
 * Represents an entry for a block, containing a block ID, its position in the stream, and its status.
 */
public class BlockEntry {
  private final String blockId;
  private AbfsBlockStatus status;
  private long position;

  /**
   * Constructs a new {@code BlockEntry}.
   *
   * @param blockId The unique identifier for the block.
   * @param position The position of the block in the stream.
   * @param status The current status of the block.
   */
  public BlockEntry(String blockId, long position, AbfsBlockStatus status) {
    this.blockId = blockId;
    this.position = position;
    this.status = status;
  }

  /**
   * Returns the block ID of this {@code BlockEntry}.
   *
   * @return The block ID.
   */
  public String getBlockId() {
    return blockId;
  }

  /**
   * Returns the position of the block in the stream.
   *
   * @return The block's position.
   */
  public long getPosition() {
    return position;
  }

  /**
   * Returns the current status of the block.
   *
   * @return The block's status.
   */
  public AbfsBlockStatus getStatus() {
    return status;
  }

  /**
   * Sets the status of the block.
   *
   * @param status The new status to be set.
   */
  public void setStatus(AbfsBlockStatus status) {
    this.status = status;
  }

  /**
   * Sets the position of the block in the stream.
   *
   * @param position The new position to be set.
   */
  public void setPosition(final long position) {
    this.position = position;
  }
}
