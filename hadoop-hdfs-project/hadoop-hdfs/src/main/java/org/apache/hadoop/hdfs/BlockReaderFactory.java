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
import org.apache.hadoop.hdfs.DFSClient.Conf;
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
   * Parameters for creating a BlockReader.
   *
   * Before you add something to here: think about whether it's already included
   * in Conf (or should be).
   */
  @InterfaceAudience.Private
  public static class Params {
    private final Conf conf;
    private Socket socket = null;
    private String file = null;
    private ExtendedBlock block = null;
    private Token<BlockTokenIdentifier> blockToken = null;
    private long startOffset = 0;
    private long len = -1;
    private int bufferSize;
    private boolean verifyChecksum = true;
    private boolean shortCircuitLocalReads = false;
    private String clientName = "";
    private DataEncryptionKey encryptionKey = null;
    private IOStreamPair ioStreamPair = null;

    public Params(Conf conf) {
      this.conf = conf;
      this.bufferSize = conf.ioBufferSize;
    }
    public Conf getConf() {
      return conf;
    }
    public Socket getSocket() {
      return socket;
    }
    public Params setSocket(Socket socket) {
      this.socket = socket;
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
    public Params setEncryptionKey(DataEncryptionKey encryptionKey) {
      this.encryptionKey = encryptionKey;
      return this;
    }
    public DataEncryptionKey getEncryptionKey() {
      return encryptionKey;
    }
    public IOStreamPair getIoStreamPair() {
      return ioStreamPair;
    }
    public Params setIoStreamPair(IOStreamPair ioStreamPair) {
      this.ioStreamPair = ioStreamPair;
      return this;
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
    if (params.getConf().useLegacyBlockReader) {
      if (params.getEncryptionKey() != null) {
        throw new RuntimeException("Encryption is not supported with the legacy block reader.");
      }
      return RemoteBlockReader.newBlockReader(params);
    } else {
      Socket sock = params.getSocket();
      if (params.getIoStreamPair() == null) {
        params.setIoStreamPair(new IOStreamPair(NetUtils.getInputStream(sock),
            NetUtils.getOutputStream(sock, HdfsServerConstants.WRITE_TIMEOUT)));
        if (params.getEncryptionKey() != null) {
          IOStreamPair encryptedStreams =
              DataTransferEncryptor.getEncryptedStreams(
                  params.getIoStreamPair().out, params.getIoStreamPair().in, 
                  params.getEncryptionKey());
          params.setIoStreamPair(encryptedStreams);
        }
      }
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
