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

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;

public interface BlockInfoUnderConstruction {
  /**
   * Create array of expected replica locations
   * (as has been assigned by chooseTargets()).
   */
  public DatanodeStorageInfo[] getExpectedStorageLocations();

  /** Get recover block */
  public Block getTruncateBlock();

  /** Convert to a Block object */
  public Block toBlock();

  /** Get block recovery ID */
  public long getBlockRecoveryId();

  /** Get the number of expected locations */
  public int getNumExpectedLocations();

  /** Set expected locations */
  public void setExpectedLocations(DatanodeStorageInfo[] targets);

  /**
   * Process the recorded replicas. When about to commit or finish the
   * pipeline recovery sort out bad replicas.
   * @param genStamp  The final generation stamp for the block.
   */
  public void setGenerationStampAndVerifyReplicas(long genStamp);

  /**
   * Initialize lease recovery for this block.
   * Find the first alive data-node starting from the previous primary and
   * make it primary.
   */
  public void initializeBlockRecovery(long recoveryId);
  
  /** Add the reported replica if it is not already in the replica list. */
  public void addReplicaIfNotPresent(DatanodeStorageInfo storage,
      Block reportedBlock, ReplicaState rState);

  /**
   * Commit block's length and generation stamp as reported by the client.
   * Set block state to {@link BlockUCState#COMMITTED}.
   * @param block - contains client reported block length and generation 
   * @throws IOException if block ids are inconsistent.
   */
  public void commitBlock(Block block) throws IOException;

  /**
   * Convert an under construction block to a complete block.
   * 
   * @return a complete block.
   * @throws IOException
   *           if the state of the block (the generation stamp and the length)
   *           has not been committed by the client or it does not have at least
   *           a minimal number of replicas reported from data-nodes.
   */
  public BlockInfo convertToCompleteBlock() throws IOException;
}
