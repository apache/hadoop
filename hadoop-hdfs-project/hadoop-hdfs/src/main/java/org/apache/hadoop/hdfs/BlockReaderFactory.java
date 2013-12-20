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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
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
   * @param domainSocketFactory  The DomainSocketFactory to notify if the Peer
   *                             is a DomainPeer which turns out to be faulty.
   *                             If null, no factory will be notified in this
   *                             case.
   * @param allowShortCircuitLocalReads  True if short-circuit local reads
   *                                     should be allowed.
   * @return New BlockReader instance
   */
  public static BlockReader newBlockReader(DFSClient.Conf conf,
                                     String file,
                                     ExtendedBlock block, 
                                     Token<BlockTokenIdentifier> blockToken,
                                     long startOffset, long len,
                                     boolean verifyChecksum,
                                     String clientName,
                                     Peer peer,
                                     DatanodeID datanodeID,
                                     DomainSocketFactory domSockFactory,
                                     PeerCache peerCache,
                                     FileInputStreamCache fisCache,
                                     boolean allowShortCircuitLocalReads,
                                     CachingStrategy cachingStrategy)
  throws IOException {
    peer.setReadTimeout(conf.socketTimeout);
    peer.setWriteTimeout(HdfsServerConstants.WRITE_TIMEOUT);

    if (peer.getDomainSocket() != null) {
      if (allowShortCircuitLocalReads && !conf.useLegacyBlockReaderLocal) {
        // If this is a domain socket, and short-circuit local reads are 
        // enabled, try to set up a BlockReaderLocal.
        BlockReader reader = newShortCircuitBlockReader(conf, file,
            block, blockToken, startOffset, len, peer, datanodeID,
            domSockFactory, verifyChecksum, fisCache, cachingStrategy);
        if (reader != null) {
          // One we've constructed the short-circuit block reader, we don't
          // need the socket any more.  So let's return it to the cache.
          if (peerCache != null) {
            peerCache.put(datanodeID, peer);
          } else {
            IOUtils.cleanup(null, peer);
          }
          return reader;
        }
      }
      // If this is a domain socket and we couldn't (or didn't want to) set
      // up a BlockReaderLocal, check that we are allowed to pass data traffic
      // over the socket before proceeding.
      if (!conf.domainSocketDataTraffic) {
        throw new IOException("Because we can't do short-circuit access, " +
          "and data traffic over domain sockets is disabled, " +
          "we cannot use this socket to talk to " + datanodeID);
      }
    }

    if (conf.useLegacyBlockReader) {
      @SuppressWarnings("deprecation")
      RemoteBlockReader reader = RemoteBlockReader.newBlockReader(file,
          block, blockToken, startOffset, len, conf.ioBufferSize,
          verifyChecksum, clientName, peer, datanodeID, peerCache,
          cachingStrategy);
      return reader;
    } else {
      return RemoteBlockReader2.newBlockReader(
          file, block, blockToken, startOffset, len,
          verifyChecksum, clientName, peer, datanodeID, peerCache,
          cachingStrategy);
    }
  }

  /**
   * Create a new short-circuit BlockReader.
   * 
   * Here, we ask the DataNode to pass us file descriptors over our
   * DomainSocket.  If the DataNode declines to do so, we'll return null here;
   * otherwise, we'll return the BlockReaderLocal.  If the DataNode declines,
   * this function will inform the DomainSocketFactory that short-circuit local
   * reads are disabled for this DataNode, so that we don't ask again.
   * 
   * @param conf               the configuration.
   * @param file               the file name. Used in log messages.
   * @param block              The block object.
   * @param blockToken         The block token for security.
   * @param startOffset        The read offset, relative to block head.
   * @param len                The number of bytes to read, or -1 to read 
   *                           as many as possible.
   * @param peer               The peer to use.
   * @param datanodeID         The datanode that the Peer is connected to.
   * @param domSockFactory     The DomainSocketFactory to notify if the Peer
   *                           is a DomainPeer which turns out to be faulty.
   *                           If null, no factory will be notified in this
   *                           case.
   * @param verifyChecksum     True if we should verify the checksums.
   *                           Note: even if this is true, when
   *                           DFS_CLIENT_READ_CHECKSUM_SKIP_CHECKSUM_KEY is
   *                           set or the block is mlocked, we will skip
   *                           checksums.
   *
   * @return                   The BlockReaderLocal, or null if the
   *                           DataNode declined to provide short-circuit
   *                           access.
   * @throws IOException       If there was a communication error.
   */
  private static BlockReaderLocal newShortCircuitBlockReader(
      DFSClient.Conf conf, String file, ExtendedBlock block,
      Token<BlockTokenIdentifier> blockToken, long startOffset,
      long len, Peer peer, DatanodeID datanodeID,
      DomainSocketFactory domSockFactory, boolean verifyChecksum,
      FileInputStreamCache fisCache,
      CachingStrategy cachingStrategy) throws IOException {
    final DataOutputStream out =
        new DataOutputStream(new BufferedOutputStream(
          peer.getOutputStream()));
    new Sender(out).requestShortCircuitFds(block, blockToken, 1);
    DataInputStream in =
        new DataInputStream(peer.getInputStream());
    BlockOpResponseProto resp = BlockOpResponseProto.parseFrom(
        PBHelper.vintPrefixed(in));
    DomainSocket sock = peer.getDomainSocket();
    switch (resp.getStatus()) {
    case SUCCESS:
      BlockReaderLocal reader = null;
      byte buf[] = new byte[1];
      FileInputStream fis[] = new FileInputStream[2];
      sock.recvFileInputStreams(fis, buf, 0, buf.length);
      try {
        reader = new BlockReaderLocal.Builder(conf).
            setFilename(file).
            setBlock(block).
            setStartOffset(startOffset).
            setStreams(fis).
            setDatanodeID(datanodeID).
            setVerifyChecksum(verifyChecksum).
            setBlockMetadataHeader(
                BlockMetadataHeader.preadHeader(fis[1].getChannel())).
            setFileInputStreamCache(fisCache).
            setCachingStrategy(cachingStrategy).
            build();
      } finally {
        if (reader == null) {
          IOUtils.cleanup(DFSClient.LOG, fis[0], fis[1]);
        }
      }
      return reader;
    case ERROR_UNSUPPORTED:
      if (!resp.hasShortCircuitAccessVersion()) {
        DFSClient.LOG.warn("short-circuit read access is disabled for " +
            "DataNode " + datanodeID + ".  reason: " + resp.getMessage());
        domSockFactory.disableShortCircuitForPath(sock.getPath());
      } else {
        DFSClient.LOG.warn("short-circuit read access for the file " +
            file + " is disabled for DataNode " + datanodeID +
            ".  reason: " + resp.getMessage());
      }
      return null;
    case ERROR_ACCESS_TOKEN:
      String msg = "access control error while " +
          "attempting to set up short-circuit access to " +
          file + resp.getMessage();
      DFSClient.LOG.debug(msg);
      throw new InvalidBlockTokenException(msg);
    default:
      DFSClient.LOG.warn("error while attempting to set up short-circuit " +
          "access to " + file + ": " + resp.getMessage());
      domSockFactory.disableShortCircuitForPath(sock.getPath());
      return null;
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

  /**
   * Get {@link BlockReaderLocalLegacy} for short circuited local reads.
   * This block reader implements the path-based style of local reads
   * first introduced in HDFS-2246.
   */
  static BlockReader getLegacyBlockReaderLocal(DFSClient dfsClient,
      String src, ExtendedBlock blk,
      Token<BlockTokenIdentifier> accessToken, DatanodeInfo chosenNode,
      long offsetIntoBlock) throws InvalidToken, IOException {
    try {
      final long length = blk.getNumBytes() - offsetIntoBlock;
      return BlockReaderLocalLegacy.newBlockReader(dfsClient, src, blk,
          accessToken, chosenNode, offsetIntoBlock, length);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(InvalidToken.class,
          AccessControlException.class);
    }
  }
}
