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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferEncryptor;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;


/** 
 * Utility class to create BlockReader implementations.
 */
@InterfaceAudience.Private
public class BlockReaderFactory {
  /**
   * Create a new BlockReader specifically to satisfy a read.
   * This method also sends the OP_READ_BLOCK request.
   * 
   * @param conf the DFSClient configuration
   * @param file  File location
   * @param block  The block object
   * @param blockToken  The block token for security
   * @param startOffset  The read offset, relative to block head
   * @param len  The number of bytes to read, or -1 to read as many as
   *             possible.
   * @param bufferSize  The IO buffer size (not the client buffer size)
   *                    Ignored except on the legacy BlockReader.
   * @param verifyChecksum  Whether to verify checksum
   * @param clientName  Client name.  Used for log messages.
   * @param peer  The peer
   * @param datanodeID  The datanode that the Peer is connected to
   * @return New BlockReader instance, or null on error.
   */
  @SuppressWarnings("deprecation")
  public static BlockReader newBlockReader(
                                     Configuration conf,
                                     String file,
                                     ExtendedBlock block, 
                                     Token<BlockTokenIdentifier> blockToken,
                                     long startOffset, long len,
                                     boolean verifyChecksum,
                                     String clientName,
                                     Peer peer,
                                     DatanodeID datanodeID)
                                     throws IOException {
    peer.setReadTimeout(conf.getInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY,
        HdfsServerConstants.READ_TIMEOUT));
    peer.setWriteTimeout(HdfsServerConstants.WRITE_TIMEOUT);
    if (conf.getBoolean(DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADER,
        DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADER_DEFAULT)) {
      return RemoteBlockReader.newBlockReader(file,
          block, blockToken, startOffset, len,
          conf.getInt(DFSConfigKeys.IO_FILE_BUFFER_SIZE_KEY,
              DFSConfigKeys.IO_FILE_BUFFER_SIZE_DEFAULT),
          verifyChecksum, clientName, peer, datanodeID);
    } else {
      return RemoteBlockReader2.newBlockReader(
          file, block, blockToken, startOffset, len,
          verifyChecksum, clientName, peer, datanodeID);
    }
  }
  
  /**
   * File name to print when accessing a block directly (from servlets)
   * @param s Address of the block location
   * @param poolId Block pool ID of the block
   * @param blockId Block ID of the block
   * @return string that has a file name for debug purposes
   */
  public static String getFileName(final InetSocketAddress s,
      final String poolId, final long blockId) {
    return s.toString() + ":" + poolId + ":" + blockId;
  }
}
