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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface.MetaDataInputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;

/**
 * Thread for processing incoming/outgoing data stream.
 */
class DataXceiver implements Runnable, FSConstants {
  public static final Log LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;
  
  Socket s;
  final String remoteAddress; // address of remote side
  final String localAddress;  // local address of this daemon
  DataNode datanode;
  DataXceiverServer dataXceiverServer;
  
  public DataXceiver(Socket s, DataNode datanode, 
      DataXceiverServer dataXceiverServer) {
    
    this.s = s;
    this.datanode = datanode;
    this.dataXceiverServer = dataXceiverServer;
    dataXceiverServer.childSockets.put(s, s);
    remoteAddress = s.getRemoteSocketAddress().toString();
    localAddress = s.getLocalSocketAddress().toString();
    LOG.debug("Number of active connections is: " + datanode.getXceiverCount());
  }

  /**
   * Read/write data from/to the DataXceiveServer.
   */
  public void run() {
    DataInputStream in=null; 
    try {
      in = new DataInputStream(
          new BufferedInputStream(NetUtils.getInputStream(s), 
                                  SMALL_BUFFER_SIZE));
      short version = in.readShort();
      if ( version != DataTransferProtocol.DATA_TRANSFER_VERSION ) {
        throw new IOException( "Version Mismatch" );
      }
      boolean local = s.getInetAddress().equals(s.getLocalAddress());
      byte op = in.readByte();
      // Make sure the xciver count is not exceeded
      int curXceiverCount = datanode.getXceiverCount();
      if (curXceiverCount > dataXceiverServer.maxXceiverCount) {
        throw new IOException("xceiverCount " + curXceiverCount
                              + " exceeds the limit of concurrent xcievers "
                              + dataXceiverServer.maxXceiverCount);
      }
      long startTime = DataNode.now();
      switch ( op ) {
      case DataTransferProtocol.OP_READ_BLOCK:
        readBlock( in );
        datanode.myMetrics.addReadBlockOp(DataNode.now() - startTime);
        if (local)
          datanode.myMetrics.incrReadsFromLocalClient();
        else
          datanode.myMetrics.incrReadsFromRemoteClient();
        break;
      case DataTransferProtocol.OP_WRITE_BLOCK:
        writeBlock( in );
        datanode.myMetrics.addWriteBlockOp(DataNode.now() - startTime);
        if (local)
          datanode.myMetrics.incrWritesFromLocalClient();
        else
          datanode.myMetrics.incrWritesFromRemoteClient();
        break;
      case DataTransferProtocol.OP_REPLACE_BLOCK: // for balancing purpose; send to a destination
        replaceBlock(in);
        datanode.myMetrics.addReplaceBlockOp(DataNode.now() - startTime);
        break;
      case DataTransferProtocol.OP_COPY_BLOCK:
            // for balancing purpose; send to a proxy source
        copyBlock(in);
        datanode.myMetrics.addCopyBlockOp(DataNode.now() - startTime);
        break;
      case DataTransferProtocol.OP_BLOCK_CHECKSUM: //get the checksum of a block
        getBlockChecksum(in);
        datanode.myMetrics.addBlockChecksumOp(DataNode.now() - startTime);
        break;
      default:
        throw new IOException("Unknown opcode " + op + " in data stream");
      }
    } catch (Throwable t) {
      LOG.error(datanode.dnRegistration + ":DataXceiver",t);
    } finally {
      LOG.debug(datanode.dnRegistration + ":Number of active connections is: "
                               + datanode.getXceiverCount());
      IOUtils.closeStream(in);
      IOUtils.closeSocket(s);
      dataXceiverServer.childSockets.remove(s);
    }
  }

  /**
   * Read a block from the disk.
   * @param in The stream to read from
   * @throws IOException
   */
  private void readBlock(DataInputStream in) throws IOException {
    //
    // Read in the header
    //
    long blockId = in.readLong();          
    Block block = new Block( blockId, 0 , in.readLong());

    long startOffset = in.readLong();
    long length = in.readLong();
    String clientName = Text.readString(in);
    Token<BlockTokenIdentifier> accessToken = new Token<BlockTokenIdentifier>();
    accessToken.readFields(in);
    OutputStream baseStream = NetUtils.getOutputStream(s, 
        datanode.socketWriteTimeout);
    DataOutputStream out = new DataOutputStream(
                 new BufferedOutputStream(baseStream, SMALL_BUFFER_SIZE));
    
    if (datanode.isBlockTokenEnabled) {
      try {
        datanode.blockTokenSecretManager.checkAccess(accessToken, null, block,
            BlockTokenSecretManager.AccessMode.READ);
      } catch (InvalidToken e) {
        try {
          out.writeShort(DataTransferProtocol.OP_STATUS_ERROR_ACCESS_TOKEN);
          out.flush();
          throw new IOException("Access token verification failed, for client "
              + remoteAddress + " for OP_READ_BLOCK for block " + block);
        } finally {
          IOUtils.closeStream(out);
        }
      }
    }
    // send the block
    BlockSender blockSender = null;
    final String clientTraceFmt =
      clientName.length() > 0 && ClientTraceLog.isInfoEnabled()
        ? String.format(DN_CLIENTTRACE_FORMAT, localAddress, remoteAddress,
            "%d", "HDFS_READ", clientName, "%d", 
            datanode.dnRegistration.getStorageID(), block, "%d")
        : datanode.dnRegistration + " Served block " + block + " to " +
            s.getInetAddress();
    try {
      try {
        blockSender = new BlockSender(block, startOffset, length,
            true, true, false, datanode, clientTraceFmt);
      } catch(IOException e) {
        out.writeShort(DataTransferProtocol.OP_STATUS_ERROR);
        throw e;
      }

      out.writeShort(DataTransferProtocol.OP_STATUS_SUCCESS); // send op status
      long read = blockSender.sendBlock(out, baseStream, null); // send data

      if (blockSender.isBlockReadFully()) {
        // See if client verification succeeded. 
        // This is an optional response from client.
        try {
          if (in.readShort() == DataTransferProtocol.OP_STATUS_CHECKSUM_OK  && 
              datanode.blockScanner != null) {
            datanode.blockScanner.verifiedByClient(block);
          }
        } catch (IOException ignored) {}
      }
      
      datanode.myMetrics.incrBytesRead((int) read);
      datanode.myMetrics.incrBlocksRead();
    } catch ( SocketException ignored ) {
      // Its ok for remote side to close the connection anytime.
      datanode.myMetrics.incrBlocksRead();
    } catch ( IOException ioe ) {
      /* What exactly should we do here?
       * Earlier version shutdown() datanode if there is disk error.
       */
      LOG.warn(datanode.dnRegistration +  ":Got exception while serving " + 
          block + " to " +
                s.getInetAddress() + ":\n" + 
                StringUtils.stringifyException(ioe) );
      throw ioe;
    } finally {
      IOUtils.closeStream(out);
      IOUtils.closeStream(blockSender);
    }
  }

  /**
   * Write a block to disk.
   * 
   * @param in The stream to read from
   * @throws IOException
   */
  private void writeBlock(DataInputStream in) throws IOException {
    DatanodeInfo srcDataNode = null;
    LOG.debug("writeBlock receive buf size " + s.getReceiveBufferSize() +
              " tcp no delay " + s.getTcpNoDelay());
    //
    // Read in the header
    //
    Block block = new Block(in.readLong(), 
        dataXceiverServer.estimateBlockSize, in.readLong());
    LOG.info("Receiving block " + block + 
             " src: " + remoteAddress +
             " dest: " + localAddress);
    int pipelineSize = in.readInt(); // num of datanodes in entire pipeline
    boolean isRecovery = in.readBoolean(); // is this part of recovery?
    String client = Text.readString(in); // working on behalf of this client
    boolean hasSrcDataNode = in.readBoolean(); // is src node info present
    if (hasSrcDataNode) {
      srcDataNode = new DatanodeInfo();
      srcDataNode.readFields(in);
    }
    int numTargets = in.readInt();
    if (numTargets < 0) {
      throw new IOException("Mislabelled incoming datastream.");
    }
    DatanodeInfo targets[] = new DatanodeInfo[numTargets];
    for (int i = 0; i < targets.length; i++) {
      DatanodeInfo tmp = new DatanodeInfo();
      tmp.readFields(in);
      targets[i] = tmp;
    }
    Token<BlockTokenIdentifier> accessToken = new Token<BlockTokenIdentifier>();
    accessToken.readFields(in);
    DataOutputStream replyOut = null;   // stream to prev target
    replyOut = new DataOutputStream(
                   NetUtils.getOutputStream(s, datanode.socketWriteTimeout));
    if (datanode.isBlockTokenEnabled) {
      try {
        datanode.blockTokenSecretManager.checkAccess(accessToken, null, block, 
            BlockTokenSecretManager.AccessMode.WRITE);
      } catch (InvalidToken e) {
        try {
          if (client.length() != 0) {
            replyOut.writeShort((short)DataTransferProtocol.OP_STATUS_ERROR_ACCESS_TOKEN);
            Text.writeString(replyOut, datanode.dnRegistration.getName());
            replyOut.flush();
          }
          throw new IOException("Access token verification failed, for client "
              + remoteAddress + " for OP_WRITE_BLOCK for block " + block);
        } finally {
          IOUtils.closeStream(replyOut);
        }
      }
    }

    DataOutputStream mirrorOut = null;  // stream to next target
    DataInputStream mirrorIn = null;    // reply from next target
    Socket mirrorSock = null;           // socket to next target
    BlockReceiver blockReceiver = null; // responsible for data handling
    String mirrorNode = null;           // the name:port of next target
    String firstBadLink = "";           // first datanode that failed in connection setup
    short mirrorInStatus = (short)DataTransferProtocol.OP_STATUS_SUCCESS;
    try {
      // open a block receiver and check if the block does not exist
      blockReceiver = new BlockReceiver(block, in, 
          s.getRemoteSocketAddress().toString(),
          s.getLocalSocketAddress().toString(),
          isRecovery, client, srcDataNode, datanode);

      //
      // Open network conn to backup machine, if 
      // appropriate
      //
      if (targets.length > 0) {
        InetSocketAddress mirrorTarget = null;
        // Connect to backup machine
        mirrorNode = targets[0].getName();
        mirrorTarget = NetUtils.createSocketAddr(mirrorNode);
        mirrorSock = datanode.newSocket();
        try {
          int timeoutValue = numTargets * datanode.socketTimeout;
          int writeTimeout = datanode.socketWriteTimeout + 
                             (HdfsConstants.WRITE_TIMEOUT_EXTENSION * numTargets);
          NetUtils.connect(mirrorSock, mirrorTarget, timeoutValue);
          mirrorSock.setSoTimeout(timeoutValue);
          mirrorSock.setSendBufferSize(DEFAULT_DATA_SOCKET_SIZE);
          mirrorOut = new DataOutputStream(
             new BufferedOutputStream(
                         NetUtils.getOutputStream(mirrorSock, writeTimeout),
                         SMALL_BUFFER_SIZE));
          mirrorIn = new DataInputStream(NetUtils.getInputStream(mirrorSock));

          // Write header: Copied from DFSClient.java!
          mirrorOut.writeShort( DataTransferProtocol.DATA_TRANSFER_VERSION );
          mirrorOut.write( DataTransferProtocol.OP_WRITE_BLOCK );
          mirrorOut.writeLong( block.getBlockId() );
          mirrorOut.writeLong( block.getGenerationStamp() );
          mirrorOut.writeInt( pipelineSize );
          mirrorOut.writeBoolean( isRecovery );
          Text.writeString( mirrorOut, client );
          mirrorOut.writeBoolean(hasSrcDataNode);
          if (hasSrcDataNode) { // pass src node information
            srcDataNode.write(mirrorOut);
          }
          mirrorOut.writeInt( targets.length - 1 );
          for ( int i = 1; i < targets.length; i++ ) {
            targets[i].write( mirrorOut );
          }
          accessToken.write(mirrorOut);

          blockReceiver.writeChecksumHeader(mirrorOut);
          mirrorOut.flush();

          // read connect ack (only for clients, not for replication req)
          if (client.length() != 0) {
            mirrorInStatus = mirrorIn.readShort();
            firstBadLink = Text.readString(mirrorIn);
            if (LOG.isDebugEnabled() || mirrorInStatus != DataTransferProtocol.OP_STATUS_SUCCESS) {
              LOG.info("Datanode " + targets.length +
                       " got response for connect ack " +
                       " from downstream datanode with firstbadlink as " +
                       firstBadLink);
            }
          }

        } catch (IOException e) {
          if (client.length() != 0) {
            replyOut.writeShort((short)DataTransferProtocol.OP_STATUS_ERROR);
            Text.writeString(replyOut, mirrorNode);
            replyOut.flush();
          }
          IOUtils.closeStream(mirrorOut);
          mirrorOut = null;
          IOUtils.closeStream(mirrorIn);
          mirrorIn = null;
          IOUtils.closeSocket(mirrorSock);
          mirrorSock = null;
          if (client.length() > 0) {
            throw e;
          } else {
            LOG.info(datanode.dnRegistration + ":Exception transfering block " +
                     block + " to mirror " + mirrorNode +
                     ". continuing without the mirror.\n" +
                     StringUtils.stringifyException(e));
          }
        }
      }

      // send connect ack back to source (only for clients)
      if (client.length() != 0) {
        if (LOG.isDebugEnabled() || mirrorInStatus != DataTransferProtocol.OP_STATUS_SUCCESS) {
          LOG.info("Datanode " + targets.length +
                   " forwarding connect ack to upstream firstbadlink is " +
                   firstBadLink);
        }
        replyOut.writeShort(mirrorInStatus);
        Text.writeString(replyOut, firstBadLink);
        replyOut.flush();
      }

      // receive the block and mirror to the next target
      String mirrorAddr = (mirrorSock == null) ? null : mirrorNode;
      blockReceiver.receiveBlock(mirrorOut, mirrorIn, replyOut,
                                 mirrorAddr, null, targets.length);

      // if this write is for a replication request (and not
      // from a client), then confirm block. For client-writes,
      // the block is finalized in the PacketResponder.
      if (client.length() == 0) {
        datanode.notifyNamenodeReceivedBlock(block, DataNode.EMPTY_DEL_HINT);
        LOG.info("Received block " + block + 
                 " src: " + remoteAddress +
                 " dest: " + localAddress +
                 " of size " + block.getNumBytes());
      }

      if (datanode.blockScanner != null) {
        datanode.blockScanner.addBlock(block);
      }
      
    } catch (IOException ioe) {
      LOG.info("writeBlock " + block + " received exception " + ioe);
      throw ioe;
    } finally {
      // close all opened streams
      IOUtils.closeStream(mirrorOut);
      IOUtils.closeStream(mirrorIn);
      IOUtils.closeStream(replyOut);
      IOUtils.closeSocket(mirrorSock);
      IOUtils.closeStream(blockReceiver);
    }
  }

  /**
   * Get block checksum (MD5 of CRC32).
   * @param in
   */
  void getBlockChecksum(DataInputStream in) throws IOException {
    final Block block = new Block(in.readLong(), 0 , in.readLong());
    Token<BlockTokenIdentifier> accessToken = new Token<BlockTokenIdentifier>();
    accessToken.readFields(in);
    DataOutputStream out = new DataOutputStream(NetUtils.getOutputStream(s,
        datanode.socketWriteTimeout));
    if (datanode.isBlockTokenEnabled) {
      try {
        datanode.blockTokenSecretManager.checkAccess(accessToken, null, block, 
            BlockTokenSecretManager.AccessMode.READ);
      } catch (InvalidToken e) {
        try {
          out.writeShort(DataTransferProtocol.OP_STATUS_ERROR_ACCESS_TOKEN);
          out.flush();
          throw new IOException(
              "Access token verification failed, for client " + remoteAddress
                  + " for OP_BLOCK_CHECKSUM for block " + block);
        } finally {
          IOUtils.closeStream(out);
        }
      }
    }

    final MetaDataInputStream metadataIn = datanode.data.getMetaDataInputStream(block);
    final DataInputStream checksumIn = new DataInputStream(new BufferedInputStream(
        metadataIn, BUFFER_SIZE));

    try {
      //read metadata file
      final BlockMetadataHeader header = BlockMetadataHeader.readHeader(checksumIn);
      final DataChecksum checksum = header.getChecksum(); 
      final int bytesPerCRC = checksum.getBytesPerChecksum();
      final long crcPerBlock = (metadataIn.getLength()
          - BlockMetadataHeader.getHeaderSize())/checksum.getChecksumSize();
      
      //compute block checksum
      final MD5Hash md5 = MD5Hash.digest(checksumIn);

      if (LOG.isDebugEnabled()) {
        LOG.debug("block=" + block + ", bytesPerCRC=" + bytesPerCRC
            + ", crcPerBlock=" + crcPerBlock + ", md5=" + md5);
      }

      //write reply
      out.writeShort(DataTransferProtocol.OP_STATUS_SUCCESS);
      out.writeInt(bytesPerCRC);
      out.writeLong(crcPerBlock);
      md5.write(out);
      out.flush();
    } finally {
      IOUtils.closeStream(out);
      IOUtils.closeStream(checksumIn);
      IOUtils.closeStream(metadataIn);
    }
  }

  /**
   * Read a block from the disk and then sends it to a destination.
   * 
   * @param in The stream to read from
   * @throws IOException
   */
  private void copyBlock(DataInputStream in) throws IOException {
    // Read in the header
    long blockId = in.readLong(); // read block id
    Block block = new Block(blockId, 0, in.readLong());
    Token<BlockTokenIdentifier> accessToken = new Token<BlockTokenIdentifier>();
    accessToken.readFields(in);
    if (datanode.isBlockTokenEnabled) {
      try {
        datanode.blockTokenSecretManager.checkAccess(accessToken, null, block,
            BlockTokenSecretManager.AccessMode.COPY);
      } catch (InvalidToken e) {
        LOG.warn("Invalid access token in request from "
            + remoteAddress + " for OP_COPY_BLOCK for block " + block);
        sendResponse(s,
            (short) DataTransferProtocol.OP_STATUS_ERROR_ACCESS_TOKEN,
            datanode.socketWriteTimeout);
        return;
      }
    }

    if (!dataXceiverServer.balanceThrottler.acquire()) { // not able to start
      LOG.info("Not able to copy block " + blockId + " to " 
          + s.getRemoteSocketAddress() + " because threads quota is exceeded.");
      sendResponse(s, (short)DataTransferProtocol.OP_STATUS_ERROR, 
          datanode.socketWriteTimeout);
      return;
    }

    BlockSender blockSender = null;
    DataOutputStream reply = null;
    boolean isOpSuccess = true;

    try {
      // check if the block exists or not
      blockSender = new BlockSender(block, 0, -1, false, false, false, 
          datanode);

      // set up response stream
      OutputStream baseStream = NetUtils.getOutputStream(
          s, datanode.socketWriteTimeout);
      reply = new DataOutputStream(new BufferedOutputStream(
          baseStream, SMALL_BUFFER_SIZE));

      // send status first
      reply.writeShort((short)DataTransferProtocol.OP_STATUS_SUCCESS);
      // send block content to the target
      long read = blockSender.sendBlock(reply, baseStream, 
                                        dataXceiverServer.balanceThrottler);

      datanode.myMetrics.incrBytesRead((int) read);
      datanode.myMetrics.incrBlocksRead();
      
      LOG.info("Copied block " + block + " to " + s.getRemoteSocketAddress());
    } catch (IOException ioe) {
      isOpSuccess = false;
      throw ioe;
    } finally {
      dataXceiverServer.balanceThrottler.release();
      if (isOpSuccess) {
        try {
          // send one last byte to indicate that the resource is cleaned.
          reply.writeChar('d');
        } catch (IOException ignored) {
        }
      }
      IOUtils.closeStream(reply);
      IOUtils.closeStream(blockSender);
    }
  }

  /**
   * Receive a block and write it to disk, it then notifies the namenode to
   * remove the copy from the source.
   * 
   * @param in The stream to read from
   * @throws IOException
   */
  private void replaceBlock(DataInputStream in) throws IOException {
    /* read header */
    long blockId = in.readLong();
    Block block = new Block(blockId, dataXceiverServer.estimateBlockSize,
        in.readLong()); // block id & generation stamp
    String sourceID = Text.readString(in); // read del hint
    DatanodeInfo proxySource = new DatanodeInfo(); // read proxy source
    proxySource.readFields(in);
    Token<BlockTokenIdentifier> accessToken = new Token<BlockTokenIdentifier>();
    accessToken.readFields(in);
    if (datanode.isBlockTokenEnabled) {
      try {
        datanode.blockTokenSecretManager.checkAccess(accessToken, null, block,
            BlockTokenSecretManager.AccessMode.REPLACE);
      } catch (InvalidToken e) {
        LOG.warn("Invalid access token in request from "
            + remoteAddress + " for OP_REPLACE_BLOCK for block " + block);
        sendResponse(s, (short)DataTransferProtocol.OP_STATUS_ERROR_ACCESS_TOKEN,
            datanode.socketWriteTimeout);
        return;
      }
    }

    if (!dataXceiverServer.balanceThrottler.acquire()) { // not able to start
      LOG.warn("Not able to receive block " + blockId + " from " 
          + s.getRemoteSocketAddress() + " because threads quota is exceeded.");
      sendResponse(s, (short)DataTransferProtocol.OP_STATUS_ERROR, 
          datanode.socketWriteTimeout);
      return;
    }

    Socket proxySock = null;
    DataOutputStream proxyOut = null;
    short opStatus = DataTransferProtocol.OP_STATUS_SUCCESS;
    BlockReceiver blockReceiver = null;
    DataInputStream proxyReply = null;
    
    try {
      // get the output stream to the proxy
      InetSocketAddress proxyAddr = NetUtils.createSocketAddr(
          proxySource.getName());
      proxySock = datanode.newSocket();
      NetUtils.connect(proxySock, proxyAddr, datanode.socketTimeout);
      proxySock.setSoTimeout(datanode.socketTimeout);

      OutputStream baseStream = NetUtils.getOutputStream(proxySock, 
          datanode.socketWriteTimeout);
      proxyOut = new DataOutputStream(
                     new BufferedOutputStream(baseStream, SMALL_BUFFER_SIZE));

      /* send request to the proxy */
      proxyOut.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION); // transfer version
      proxyOut.writeByte(DataTransferProtocol.OP_COPY_BLOCK); // op code
      proxyOut.writeLong(block.getBlockId()); // block id
      proxyOut.writeLong(block.getGenerationStamp()); // block id
      accessToken.write(proxyOut);
      proxyOut.flush();

      // receive the response from the proxy
      proxyReply = new DataInputStream(new BufferedInputStream(
          NetUtils.getInputStream(proxySock), BUFFER_SIZE));
      short status = proxyReply.readShort();
      if (status != DataTransferProtocol.OP_STATUS_SUCCESS) {
        if (status == DataTransferProtocol.OP_STATUS_ERROR_ACCESS_TOKEN) {
          throw new IOException("Copy block " + block + " from "
              + proxySock.getRemoteSocketAddress()
              + " failed due to access token error");
        }
        throw new IOException("Copy block " + block + " from "
            + proxySock.getRemoteSocketAddress() + " failed");
      }
      // open a block receiver and check if the block does not exist
      blockReceiver = new BlockReceiver(
          block, proxyReply, proxySock.getRemoteSocketAddress().toString(),
          proxySock.getLocalSocketAddress().toString(),
          false, "", null, datanode);

      // receive a block
      blockReceiver.receiveBlock(null, null, null, null, 
          dataXceiverServer.balanceThrottler, -1);
                    
      // notify name node
      datanode.notifyNamenodeReceivedBlock(block, sourceID);

      LOG.info("Moved block " + block + 
          " from " + s.getRemoteSocketAddress());
      
    } catch (IOException ioe) {
      opStatus = DataTransferProtocol.OP_STATUS_ERROR;
      throw ioe;
    } finally {
      // receive the last byte that indicates the proxy released its thread resource
      if (opStatus == DataTransferProtocol.OP_STATUS_SUCCESS) {
        try {
          proxyReply.readChar();
        } catch (IOException ignored) {
        }
      }
      
      // now release the thread resource
      dataXceiverServer.balanceThrottler.release();
      
      // send response back
      try {
        sendResponse(s, opStatus, datanode.socketWriteTimeout);
      } catch (IOException ioe) {
        LOG.warn("Error writing reply back to " + s.getRemoteSocketAddress());
      }
      IOUtils.closeStream(proxyOut);
      IOUtils.closeStream(blockReceiver);
      IOUtils.closeStream(proxyReply);
    }
  }
  
  /**
   * Utility function for sending a response.
   * @param s socket to write to
   * @param opStatus status message to write
   * @param timeout send timeout
   **/
  private void sendResponse(Socket s, short opStatus, long timeout) 
                                                       throws IOException {
    DataOutputStream reply = 
      new DataOutputStream(NetUtils.getOutputStream(s, timeout));
    try {
      reply.writeShort(opStatus);
      reply.flush();
    } finally {
      IOUtils.closeStream(reply);
    }
  }
}
