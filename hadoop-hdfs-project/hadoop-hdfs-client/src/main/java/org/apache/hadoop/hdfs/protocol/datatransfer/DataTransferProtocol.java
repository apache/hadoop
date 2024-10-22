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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockChecksumOptions;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.StripedBlockInfo;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transfer data to/from datanode using a streaming protocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface DataTransferProtocol {
  Logger LOG = LoggerFactory.getLogger(DataTransferProtocol.class);

  /** Version for data transfers between clients and datanodes
   * This should change when serialization of DatanodeInfo, not just
   * when protocol changes. It is not very obvious.
   */
  /*
   * Version 28:
   *    Declare methods in DataTransferProtocol interface.
   */
  int DATA_TRANSFER_VERSION = 28;

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
  void readBlock(final ExtendedBlock blk,
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
   * @param requestedChecksum the requested checksum mechanism
   * @param cachingStrategy the caching strategy
   * @param allowLazyPersist hint to the DataNode that the block can be
   *                         allocated on transient storage i.e. memory and
   *                         written to disk lazily
   * @param pinning whether to pin the block, so Balancer won't move it.
   * @param targetPinnings whether to pin the block on target datanode
   * @param storageID optional StorageIDs designating where to write the
   *                  block. An empty String or null indicates that this
   *                  has not been provided.
   * @param targetStorageIDs target StorageIDs corresponding to the target
   *                         datanodes.
   */
  void writeBlock(final ExtendedBlock blk,
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
      final boolean[] targetPinnings,
      final String storageID,
      final String[] targetStorageIDs) throws IOException;
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
   * @param targetStorageIDs StorageID designating where to write the
   *                     block.
   */
  void transferBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final DatanodeInfo[] targets,
      final StorageType[] targetStorageTypes,
      final String[] targetStorageIDs) throws IOException;

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
  void requestShortCircuitFds(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      SlotId slotId, int maxVersion, boolean supportsReceiptVerification)
        throws IOException;

  /**
   * Release a pair of short-circuit FDs requested earlier.
   *
   * @param slotId          SlotID used by the earlier file descriptors.
   */
  void releaseShortCircuitFds(final SlotId slotId) throws IOException;

  /**
   * Request a short circuit shared memory area from a DataNode.
   *
   * @param clientName       The name of the client.
   */
  void requestShortCircuitShm(String clientName) throws IOException;

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
   * @param storageId an optional storage ID to designate where the block is
   *                  replaced to.
   */
  void replaceBlock(final ExtendedBlock blk,
      final StorageType storageType,
      final Token<BlockTokenIdentifier> blockToken,
      final String delHint,
      final DatanodeInfo source,
      final String storageId) throws IOException;

  /**
   * Copy a block.
   * It is used for balancing purpose.
   *
   * @param blk the block being copied.
   * @param blockToken security token for accessing the block.
   */
  void copyBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken) throws IOException;

  /**
   * Get block checksum (MD5 of CRC32).
   *
   * @param blk a block.
   * @param blockToken security token for accessing the block.
   * @param blockChecksumOptions determines how the block-level checksum is
   *     computed from underlying block metadata.
   * @throws IOException
   */
  void blockChecksum(ExtendedBlock blk,
      Token<BlockTokenIdentifier> blockToken,
      BlockChecksumOptions blockChecksumOptions) throws IOException;

  /**
   * Get striped block group checksum (MD5 of CRC32).
   *
   * @param stripedBlockInfo a striped block info.
   * @param blockToken security token for accessing the block.
   * @param requestedNumBytes requested number of bytes in the block group
   *                          to compute the checksum.
   * @param blockChecksumOptions determines how the block-level checksum is
   *     computed from underlying block metadata.
   * @throws IOException
   */
  void blockGroupChecksum(StripedBlockInfo stripedBlockInfo,
          Token<BlockTokenIdentifier> blockToken,
          long requestedNumBytes,
          BlockChecksumOptions blockChecksumOptions) throws IOException;

  /**
   * Copy a block cross Namespace.
   * It is used for fastcopy.
   *
   * @param sourceBlk the block being copied.
   * @param sourceBlockToken security token for accessing sourceBlk.
   * @param targetBlk the block to be writted.
   * @param targetBlockToken security token for accessing targetBlk.
   * @param targetDatanode the target datnode which sourceBlk will copy to as targetBlk.
   */
  void copyBlockCrossNamespace(ExtendedBlock sourceBlk,
      Token<BlockTokenIdentifier> sourceBlockToken, ExtendedBlock targetBlk,
      Token<BlockTokenIdentifier> targetBlockToken, DatanodeInfo targetDatanode) throws IOException;
}
