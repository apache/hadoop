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
package org.apache.hadoop.hdfs.protocol.datatransfer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

/**
 * Transfer data to/from datanode using a streaming protocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface DataTransferProtocol {
  public static final Log LOG = LogFactory.getLog(DataTransferProtocol.class);
  
  /** Version for data transfers between clients and datanodes
   * This should change when serialization of DatanodeInfo, not just
   * when protocol changes. It is not very obvious. 
   */
  /*
   * Version 28:
   *    Declare methods in DataTransferProtocol interface.
   */
  public static final int DATA_TRANSFER_VERSION = 28;

  /** 
   * Read a block.
   * 
   * @param blk the block being read.
   * @param blockToken security token for accessing the block.
   * @param clientName client's name.
   * @param blockOffset offset of the block.
   * @param length maximum number of bytes for this read.
   * @param sendChecksum if false, the DN should skip reading and sending
   *        checksums
   * @param cachingStrategy  The caching strategy to use.
   */
  public void readBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final long blockOffset,
      final long length,
      final boolean sendChecksum,
      final CachingStrategy cachingStrategy) throws IOException;

  /**
   * Write a block to a datanode pipeline.
   * The receiver datanode of this call is the next datanode in the pipeline.
   * The other downstream datanodes are specified by the targets parameter.
   * Note that the receiver {@link DatanodeInfo} is not required in the
   * parameter list since the receiver datanode knows its info.  However, the
   * {@link StorageType} for storing the replica in the receiver datanode is a 
   * parameter since the receiver datanode may support multiple storage types.
   *
   * @param blk the block being written.
   * @param storageType for storing the replica in the receiver datanode.
   * @param blockToken security token for accessing the block.
   * @param clientName client's name.
   * @param targets other downstream datanodes in the pipeline.
   * @param targetStorageTypes target {@link StorageType}s corresponding
   *                           to the target datanodes.
   * @param source source datanode.
   * @param stage pipeline stage.
   * @param pipelineSize the size of the pipeline.
   * @param minBytesRcvd minimum number of bytes received.
   * @param maxBytesRcvd maximum number of bytes received.
   * @param latestGenerationStamp the latest generation stamp of the block.
   * @param pinning whether to pin the block, so Balancer won't move it.
   * @param targetPinnings whether to pin the block on target datanode
   */
  public void writeBlock(final ExtendedBlock blk,
      final StorageType storageType, 
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final DatanodeInfo[] targets,
      final StorageType[] targetStorageTypes, 
      final DatanodeInfo source,
      final BlockConstructionStage stage,
      final int pipelineSize,
      final long minBytesRcvd,
      final long maxBytesRcvd,
      final long latestGenerationStamp,
      final DataChecksum requestedChecksum,
      final CachingStrategy cachingStrategy,
      final boolean allowLazyPersist,
      final boolean pinning,
      final boolean[] targetPinnings) throws IOException;
  /**
   * Transfer a block to another datanode.
   * The block stage must be
   * either {@link BlockConstructionStage#TRANSFER_RBW}
   * or {@link BlockConstructionStage#TRANSFER_FINALIZED}.
   * 
   * @param blk the block being transferred.
   * @param blockToken security token for accessing the block.
   * @param clientName client's name.
   * @param targets target datanodes.
   */
  public void transferBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final DatanodeInfo[] targets,
      final StorageType[] targetStorageTypes) throws IOException;

  /**
   * Request short circuit access file descriptors from a DataNode.
   *
   * @param blk             The block to get file descriptors for.
   * @param blockToken      Security token for accessing the block.
   * @param slotId          The shared memory slot id to use, or null 
   *                          to use no slot id.
   * @param maxVersion      Maximum version of the block data the client 
   *                          can understand.
   * @param supportsReceiptVerification  True if the client supports
   *                          receipt verification.
   */
  public void requestShortCircuitFds(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      SlotId slotId, int maxVersion, boolean supportsReceiptVerification)
        throws IOException;

  /**
   * Release a pair of short-circuit FDs requested earlier.
   *
   * @param slotId          SlotID used by the earlier file descriptors.
   */
  public void releaseShortCircuitFds(final SlotId slotId) throws IOException;

  /**
   * Request a short circuit shared memory area from a DataNode.
   * 
   * @param clientName       The name of the client.
   */
  public void requestShortCircuitShm(String clientName) throws IOException;
  
  /**
   * Receive a block from a source datanode
   * and then notifies the namenode
   * to remove the copy from the original datanode.
   * Note that the source datanode and the original datanode can be different.
   * It is used for balancing purpose.
   * 
   * @param blk the block being replaced.
   * @param storageType the {@link StorageType} for storing the block.
   * @param blockToken security token for accessing the block.
   * @param delHint the hint for deleting the block in the original datanode.
   * @param source the source datanode for receiving the block.
   */
  public void replaceBlock(final ExtendedBlock blk,
      final StorageType storageType, 
      final Token<BlockTokenIdentifier> blockToken,
      final String delHint,
      final DatanodeInfo source) throws IOException;

  /**
   * Copy a block. 
   * It is used for balancing purpose.
   * 
   * @param blk the block being copied.
   * @param blockToken security token for accessing the block.
   */
  public void copyBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken) throws IOException;

  /**
   * Get block checksum (MD5 of CRC32).
   * 
   * @param blk a block.
   * @param blockToken security token for accessing the block.
   * @throws IOException
   */
  public void blockChecksum(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken) throws IOException;
}
