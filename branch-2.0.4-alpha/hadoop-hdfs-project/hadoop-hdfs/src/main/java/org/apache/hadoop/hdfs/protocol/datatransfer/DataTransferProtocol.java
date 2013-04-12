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
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
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
   */
  public void readBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final long blockOffset,
      final long length,
      final boolean sendChecksum) throws IOException;

  /**
   * Write a block to a datanode pipeline.
   * 
   * @param blk the block being written.
   * @param blockToken security token for accessing the block.
   * @param clientName client's name.
   * @param targets target datanodes in the pipeline.
   * @param source source datanode.
   * @param stage pipeline stage.
   * @param pipelineSize the size of the pipeline.
   * @param minBytesRcvd minimum number of bytes received.
   * @param maxBytesRcvd maximum number of bytes received.
   * @param latestGenerationStamp the latest generation stamp of the block.
   */
  public void writeBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final DatanodeInfo[] targets,
      final DatanodeInfo source,
      final BlockConstructionStage stage,
      final int pipelineSize,
      final long minBytesRcvd,
      final long maxBytesRcvd,
      final long latestGenerationStamp,
      final DataChecksum requestedChecksum) throws IOException;

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
      final DatanodeInfo[] targets) throws IOException;

  /**
   * Receive a block from a source datanode
   * and then notifies the namenode
   * to remove the copy from the original datanode.
   * Note that the source datanode and the original datanode can be different.
   * It is used for balancing purpose.
   * 
   * @param blk the block being replaced.
   * @param blockToken security token for accessing the block.
   * @param delHint the hint for deleting the block in the original datanode.
   * @param source the source datanode for receiving the block.
   */
  public void replaceBlock(final ExtendedBlock blk,
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
