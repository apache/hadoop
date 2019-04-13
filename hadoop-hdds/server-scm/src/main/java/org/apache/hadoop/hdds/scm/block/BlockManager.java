/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.block;

import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 *
 *  Block APIs.
 *  Container is transparent to these APIs.
 */
public interface BlockManager extends Closeable {
  /**
   * Allocates a new block for a given size.
   * @param size - Block Size
   * @param type Replication Type
   * @param factor - Replication Factor
   * @param excludeList List of datanodes/containers to exclude during block
   *                    allocation.
   * @return AllocatedBlock
   * @throws IOException
   */
  AllocatedBlock allocateBlock(long size, HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor, String owner,
      ExcludeList excludeList) throws IOException;

  /**
   * Deletes a list of blocks in an atomic operation. Internally, SCM
   * writes these blocks into a {@link DeletedBlockLog} and deletes them
   * from SCM DB. If this is successful, given blocks are entering pending
   * deletion state and becomes invisible from SCM namespace.
   *
   * @param blockIDs block IDs. This is often the list of blocks of
   *                 a particular object key.
   * @throws IOException if exception happens, non of the blocks is deleted.
   */
  void deleteBlocks(List<BlockID> blockIDs) throws IOException;

  /**
   * @return the block deletion transaction log maintained by SCM.
   */
  DeletedBlockLog getDeletedBlockLog();

  /**
   * Start block manager background services.
   * @throws IOException
   */
  void start() throws IOException;

  /**
   * Shutdown block manager background services.
   * @throws IOException
   */
  void stop() throws IOException;

  /**
   * @return the block deleting service executed in SCM.
   */
  SCMBlockDeletingService getSCMBlockDeletingService();

  /**
   * Set SafeMode status.
   *
   * @param safeModeStatus
   */
  void setSafeModeStatus(boolean safeModeStatus);
}
