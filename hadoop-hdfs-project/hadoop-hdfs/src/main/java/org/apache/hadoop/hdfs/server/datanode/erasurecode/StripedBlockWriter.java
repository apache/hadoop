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
package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSPacket;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.EnumSet;

/**
 * A striped block writer that writes reconstructed data to the remote target
 * datanode.
 */
@InterfaceAudience.Private
class StripedBlockWriter {
  private final StripedWriter stripedWriter;
  private final DataNode datanode;
  private final Configuration conf;

  private final ExtendedBlock block;
  private final DatanodeInfo target;
  private final StorageType storageType;

  private Socket targetSocket;
  private DataOutputStream targetOutputStream;
  private DataInputStream targetInputStream;
  private ByteBuffer targetBuffer;
  private long blockOffset4Target = 0;
  private long seqNo4Target = 0;

  StripedBlockWriter(StripedWriter stripedWriter, DataNode datanode,
                     Configuration conf, ExtendedBlock block,
                     DatanodeInfo target, StorageType storageType)
      throws IOException {
    this.stripedWriter = stripedWriter;
    this.datanode = datanode;
    this.conf = conf;

    this.block = block;
    this.target = target;
    this.storageType = storageType;

    this.targetBuffer = stripedWriter.allocateWriteBuffer();

    init();
  }

  ByteBuffer getTargetBuffer() {
    return targetBuffer;
  }

  /**
   * Initialize  output/input streams for transferring data to target
   * and send create block request.
   */
  private void init() throws IOException {
    Socket socket = null;
    DataOutputStream out = null;
    DataInputStream in = null;
    boolean success = false;
    try {
      InetSocketAddress targetAddr =
          stripedWriter.getSocketAddress4Transfer(target);
      socket = datanode.newSocket();
      NetUtils.connect(socket, targetAddr,
          datanode.getDnConf().getSocketTimeout());
      socket.setSoTimeout(datanode.getDnConf().getSocketTimeout());

      Token<BlockTokenIdentifier> blockToken =
          datanode.getBlockAccessToken(block,
              EnumSet.of(BlockTokenIdentifier.AccessMode.WRITE));

      long writeTimeout = datanode.getDnConf().getSocketWriteTimeout();
      OutputStream unbufOut = NetUtils.getOutputStream(socket, writeTimeout);
      InputStream unbufIn = NetUtils.getInputStream(socket);
      DataEncryptionKeyFactory keyFactory =
          datanode.getDataEncryptionKeyFactoryForBlock(block);
      IOStreamPair saslStreams = datanode.getSaslClient().socketSend(
          socket, unbufOut, unbufIn, keyFactory, blockToken, target);

      unbufOut = saslStreams.out;
      unbufIn = saslStreams.in;

      out = new DataOutputStream(new BufferedOutputStream(unbufOut,
          DFSUtilClient.getSmallBufferSize(conf)));
      in = new DataInputStream(unbufIn);

      DatanodeInfo source = new DatanodeInfo(datanode.getDatanodeId());
      new Sender(out).writeBlock(block, storageType,
          blockToken, "", new DatanodeInfo[]{target},
          new StorageType[]{storageType}, source,
          BlockConstructionStage.PIPELINE_SETUP_CREATE, 0, 0, 0, 0,
          stripedWriter.getChecksum(), stripedWriter.getCachingStrategy(),
          false, false, null);

      targetSocket = socket;
      targetOutputStream = out;
      targetInputStream = in;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeStream(socket);
      }
    }
  }

  /**
   * Send data to targets.
   */
  void transferData2Target(byte[] packetBuf) throws IOException {
    if (targetBuffer.remaining() == 0) {
      return;
    }

    stripedWriter.getChecksum().calculateChunkedSums(
        targetBuffer.array(), 0, targetBuffer.remaining(),
        stripedWriter.getChecksumBuf(), 0);

    int ckOff = 0;
    while (targetBuffer.remaining() > 0) {
      DFSPacket packet = new DFSPacket(packetBuf,
          stripedWriter.getMaxChunksPerPacket(),
          blockOffset4Target, seqNo4Target++,
          stripedWriter.getChecksumSize(), false);
      int maxBytesToPacket = stripedWriter.getMaxChunksPerPacket()
          * stripedWriter.getBytesPerChecksum();
      int toWrite = targetBuffer.remaining() > maxBytesToPacket ?
          maxBytesToPacket : targetBuffer.remaining();
      int ckLen = ((toWrite - 1) / stripedWriter.getBytesPerChecksum() + 1)
          * stripedWriter.getChecksumSize();
      packet.writeChecksum(stripedWriter.getChecksumBuf(), ckOff, ckLen);
      ckOff += ckLen;
      packet.writeData(targetBuffer, toWrite);

      // Send packet
      packet.writeTo(targetOutputStream);

      blockOffset4Target += toWrite;
    }
  }

  // send an empty packet to mark the end of the block
  void endTargetBlock(byte[] packetBuf) throws IOException {
    DFSPacket packet = new DFSPacket(packetBuf, 0,
        blockOffset4Target, seqNo4Target++,
        stripedWriter.getChecksumSize(), true);
    packet.writeTo(targetOutputStream);
    targetOutputStream.flush();
  }

  void close() {
    IOUtils.closeStream(targetOutputStream);
    IOUtils.closeStream(targetInputStream);
    IOUtils.closeStream(targetSocket);
  }
}
