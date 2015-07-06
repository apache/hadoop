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

import com.google.common.base.Preconditions;

/**
 * Utility class with logic on managing storage locations shared between
 * complete and under-construction blocks under the contiguous format --
 * {@link BlockInfoContiguous} and
 * {@link BlockInfoUnderConstructionContiguous}.
 */
class ContiguousBlockStorageOp {
  /**
   * Ensure that there is enough  space to include num more triplets.
   * @return first free triplet index.
   */
  private static int ensureCapacity(BlockInfo b, int num) {
    Preconditions.checkArgument(b.triplets != null,
        "BlockInfo is not initialized");
    int last = b.numNodes();
    if (b.triplets.length >= (last+num)*3) {
      return last;
    }
    /* Not enough space left. Create a new array. Should normally
     * happen only when replication is manually increased by the user. */
    Object[] old = b.triplets;
    b.triplets = new Object[(last+num)*3];
    System.arraycopy(old, 0, b.triplets, 0, last * 3);
    return last;
  }

  static void addStorage(BlockInfo b, DatanodeStorageInfo storage) {
    // find the last null node
    int lastNode = ensureCapacity(b, 1);
    b.setStorageInfo(lastNode, storage);
    b.setNext(lastNode, null);
    b.setPrevious(lastNode, null);
  }

  static boolean removeStorage(BlockInfo b,
      DatanodeStorageInfo storage) {
    int dnIndex = b.findStorageInfo(storage);
    if (dnIndex < 0) { // the node is not found
      return false;
    }
    Preconditions.checkArgument(b.getPrevious(dnIndex) == null &&
            b.getNext(dnIndex) == null,
        "Block is still in the list and must be removed first.");
    // find the last not null node
    int lastNode = b.numNodes()-1;
    // replace current node triplet by the lastNode one
    b.setStorageInfo(dnIndex, b.getStorageInfo(lastNode));
    b.setNext(dnIndex, b.getNext(lastNode));
    b.setPrevious(dnIndex, b.getPrevious(lastNode));
    // set the last triplet to null
    b.setStorageInfo(lastNode, null);
    b.setNext(lastNode, null);
    b.setPrevious(lastNode, null);
    return true;
  }

  static int numNodes(BlockInfo b) {
    Preconditions.checkArgument(b.triplets != null,
        "BlockInfo is not initialized");
    Preconditions.checkArgument(b.triplets.length % 3 == 0,
        "Malformed BlockInfo");

    for (int idx = b.getCapacity()-1; idx >= 0; idx--) {
      if (b.getDatanode(idx) != null) {
        return idx + 1;
      }
    }
    return 0;
  }

  static void replaceBlock(BlockInfo b, BlockInfo newBlock) {
    for (int i = b.numNodes() - 1; i >= 0; i--) {
      final DatanodeStorageInfo storage = b.getStorageInfo(i);
      final boolean removed = storage.removeBlock(b);
      Preconditions.checkState(removed, "currentBlock not found.");

      final DatanodeStorageInfo.AddBlockResult result = storage.addBlock(
          newBlock);
      Preconditions.checkState(
          result == DatanodeStorageInfo.AddBlockResult.ADDED,
          "newBlock already exists.");
    }
  }

  static boolean hasEmptyStorage(BlockInfo b) {
    return b.getStorageInfo(0) == null;
  }
}
