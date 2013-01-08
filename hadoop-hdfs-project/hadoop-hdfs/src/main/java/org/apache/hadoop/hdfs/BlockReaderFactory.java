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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.security.token.Token;

import com.google.common.base.Preconditions;


/** 
 * Utility class to create BlockReader implementations.
 */
@InterfaceAudience.Private
public class BlockReaderFactory {
  /**
   * Parameters for creating a BlockReader.
   *
   * Before you add something to here: think about whether it's already included
   * in Conf (or should be).
   */
  @InterfaceAudience.Private
  public static class Params {
    private final Conf conf;
    /**
     * The peer that this BlockReader will be connected to.
     * You must set this.
     */
    private Peer peer = null;
    
    /**
     * The file name that this BlockReader pertains to.
     * This is optional and only used for display and logging purposes.
     */
    private String file = null;

    /**
     * The block that this BlockReader is reading.
     * You must set this.
     */
    private ExtendedBlock block = null;
    
    /**
     * The BlockTokenIdentifier to use, or null to use none.
     */
    private Token<BlockTokenIdentifier> blockToken = null;

    /**
     * The offset in the block to start reading at.
     */
    private long startOffset = 0;
    
    /**
     * The total number of bytes we might want to read, or -1 to assume no
     * limit.
     */
    private long len = -1;
    
    /**
     * The buffer size to use.
     *
     * If this is not set, we will use the default from the Conf.
     */
    private int bufferSize;
    
    /**
     * Whether or not we should verify the checksum.
     *
     * This is used instead of conf.verifyChecksum, because there are some
     * cases when we may want to explicitly turn off checksum verification,
     * such as when the caller has explicitly asked for a file to be opened
     * without checksum verification.
     */
    private boolean verifyChecksum = true;

    /**
     * Whether or not we should try to use short circuit local reads.
     */
    private boolean shortCircuitLocalReads = false;

    /**
     * The name of the client using this BlockReader, for logging and
     * debugging purposes.
     */
    private String clientName = "";
    
    /**
     * The DataNode on which this Block resides.
     * You must set this.
     */
    private DatanodeID datanodeID = null;

    public Params(Conf conf) {
      this.conf = conf;
      this.bufferSize = conf.ioBufferSize;
    }
    public Conf getConf() {
      return conf;
    }
    public Peer getPeer() {
      return peer;
    }
    public Params setPeer(Peer peer) {
      this.peer = peer;
      return this;
    }
    public String getFile() {
      return file;
    }
    public Params setFile(String file) {
      this.file = file;
      return this;
    }
    public ExtendedBlock getBlock() {
      return block;
    }
    public Params setBlock(ExtendedBlock block) {
      this.block = block;
      return this;
    }
    public Token<BlockTokenIdentifier> getBlockToken() {
      return blockToken;
    }
    public Params setBlockToken(Token<BlockTokenIdentifier> blockToken) {
      this.blockToken = blockToken;
      return this;
    }
    public long getStartOffset() {
      return startOffset;
    }
    public Params setStartOffset(long startOffset) {
      this.startOffset = startOffset;
      return this;
    }
    public long getLen() {
      return len;
    }
    public Params setLen(long len) {
      this.len = len;
      return this;
    }
    public int getBufferSize() {
      return bufferSize;
    }
    public Params setBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }
    public boolean getVerifyChecksum() {
      return verifyChecksum;
    }
    public Params setVerifyChecksum(boolean verifyChecksum) {
      this.verifyChecksum = verifyChecksum;
      return this;
    }
    public boolean getShortCircuitLocalReads() {
      return shortCircuitLocalReads;
    }
    public Params setShortCircuitLocalReads(boolean on) {
      this.shortCircuitLocalReads = on;
      return this;
    }
    public String getClientName() {
      return clientName;
    }
    public Params setClientName(String clientName) {
      this.clientName = clientName;
      return this;
    }
    public Params setDatanodeID(DatanodeID datanodeID) {
      this.datanodeID = datanodeID;
      return this;
    }
    public DatanodeID getDatanodeID() {
      return datanodeID;
    }
  }

  /**
   * Create a new BlockReader specifically to satisfy a read.
   * This method also sends the OP_READ_BLOCK request.
   * 
   * @param params            The parameters
   *
   * @return                  New BlockReader instance
   * @throws IOException      If there was an error creating the BlockReader
   */
  @SuppressWarnings("deprecation")
  public static BlockReader newBlockReader(Params params) throws IOException {
    Preconditions.checkNotNull(params.getPeer());
    Preconditions.checkNotNull(params.getBlock());
    Preconditions.checkNotNull(params.getDatanodeID());
    // First, let's set the read and write timeouts appropriately.
    // This will keep us from blocking forever if something goes wrong during
    // network communication.
    Peer peer = params.getPeer();
    peer.setReadTimeout(params.getConf().socketTimeout);
    peer.setWriteTimeout(HdfsServerConstants.WRITE_TIMEOUT);

    if (params.getConf().useLegacyBlockReader) {
      // The legacy BlockReader doesn't require that the Peers it uses
      // have associated ReadableByteChannels.  This makes it easier to use 
      // with some older Socket classes like, say, SocksSocketImpl.
      //
      // TODO: create a wrapper class that makes channel-less sockets look like
      // they have a channel, so that we can finally remove the legacy
      // RemoteBlockReader.  See HDFS-2534.
      return RemoteBlockReader.newBlockReader(params);
    } else {
      // The usual block reader.
      return RemoteBlockReader2.newBlockReader(params);
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