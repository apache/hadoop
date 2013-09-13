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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.PrintWriter;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;

/**
 * Subclass of InvalidateBlocks used by the BlockManager to
 * track blocks on each storage that are scheduled to be invalidated.
 */
public class InvalidateStoredBlocks extends InvalidateBlocks {

  private final DatanodeManager datanodeManager;

  InvalidateStoredBlocks(DatanodeManager datanodeManager) {
    this.datanodeManager = datanodeManager;
  }

  /** Print the contents to out. */
  synchronized void dump(final PrintWriter out) {
    final int size = numStorages();
    out.println("Metasave: Blocks " + numBlocks() 
        + " waiting deletion from " + size + " datanodes.");
    if (size == 0) {
      return;
    }

    List<String> storageIds = getStorageIDs();
    for (String storageId: storageIds) {
      LightWeightHashSet<Block> blocks = getBlocks(storageId);
      if (blocks != null && !blocks.isEmpty()) {
        out.println(datanodeManager.getDatanode(storageId));
        out.println(blocks);
      }
    }
  }

  @Override
  synchronized List<Block> invalidateWork(
      final String storageId, final DatanodeDescriptor dn) {
    final List<Block> toInvalidate = pollNumBlocks(storageId,
        datanodeManager.blockInvalidateLimit);
    if (toInvalidate != null) {
      dn.addBlocksToBeInvalidated(toInvalidate);
    }
    return toInvalidate;
  }
}
