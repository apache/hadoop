/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.keyvalue.interfaces;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;

import java.io.IOException;
import java.util.List;

/**
 * BlockManager is for performing key related operations on the container.
 */
public interface BlockManager {

  /**
   * Puts or overwrites a block.
   *
   * @param container - Container for which block need to be added.
   * @param data     - Block Data.
   * @return length of the Block.
   * @throws IOException
   */
  long putBlock(Container container, BlockData data) throws IOException;

  /**
   * Gets an existing block.
   *
   * @param container - Container from which block need to be get.
   * @param blockID - BlockID of the Block.
   * @return Block Data.
   * @throws IOException
   */
  BlockData getBlock(Container container, BlockID blockID)
      throws IOException;

  /**
   * Deletes an existing block.
   *
   * @param container - Container from which block need to be deleted.
   * @param blockID - ID of the block.
   * @throws StorageContainerException
   */
  void deleteBlock(Container container, BlockID blockID) throws IOException;

  /**
   * List blocks in a container.
   *
   * @param container - Container from which blocks need to be listed.
   * @param startLocalID  - Block to start from, 0 to begin.
   * @param count    - Number of blocks to return.
   * @return List of Blocks that match the criteria.
   */
  List<BlockData> listBlock(Container container, long startLocalID, int count)
      throws IOException;

  /**
   * Returns the last committed block length for the block.
   * @param blockID blockId
   */
  long getCommittedBlockLength(Container container, BlockID blockID)
      throws IOException;

  /**
   * Shutdown ContainerManager.
   */
  void shutdown();
}
