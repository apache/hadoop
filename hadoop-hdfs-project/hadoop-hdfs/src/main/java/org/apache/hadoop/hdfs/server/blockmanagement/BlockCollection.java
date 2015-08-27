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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ContentSummary;

/** 
 * This interface is used by the block manager to expose a
 * few characteristics of a collection of Block/BlockUnderConstruction.
 */
@InterfaceAudience.Private
public interface BlockCollection {
  /**
   * Get the last block of the collection.
   */
  public BlockInfo getLastBlock();

  /** 
   * Get content summary.
   */
  public ContentSummary computeContentSummary(BlockStoragePolicySuite bsps);

  /**
   * @return the number of blocks or block groups
   */ 
  public int numBlocks();

  /**
   * Get the blocks (striped or contiguous).
   */
  public BlockInfo[] getBlocks();

  /**
   * Get preferred block size for the collection 
   * @return preferred block size in bytes
   */
  public long getPreferredBlockSize();

  /**
   * Get block replication for the collection.
   * @return block replication value. Return 0 if the file is erasure coded.
   */
  public short getPreferredBlockReplication();

  /**
   * @return the storage policy ID.
   */
  public byte getStoragePolicyID();

  /**
   * Get the name of the collection.
   */
  public String getName();

  /**
   * Set the block (contiguous or striped) at the given index.
   */
  public void setBlock(int index, BlockInfo blk);

  /**
   * Convert the last block of the collection to an under-construction block
   * and set the locations.
   */
  public void convertLastBlockToUC(BlockInfo lastBlock,
      DatanodeStorageInfo[] targets) throws IOException;

  /**
   * @return whether the block collection is under construction.
   */
  public boolean isUnderConstruction();

  /**
   * @return whether the block collection is in striping format
   */
  boolean isStriped();

  /**
   * @return the id for the block collection
   */
  long getId();
}
