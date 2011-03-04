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

import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.ERROR;
import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.ERROR_ACCESS_TOKEN;
import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.SUCCESS;
import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;

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
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.BlockConstructionStage;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface.MetaDataInputStream;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;

/**
 * Thread for processing incoming/outgoing data stream.
 */
class DataXceiver extends DataTransferProtocol.Receiver
    implements Runnable, FSConstants {
  public static final Log LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;
  
  private final Socket s;
  private final boolean isLocal; //is a local connection?
  private final String remoteAddress; // address of remote side
  private final String localAddress;  // local address of this daemon
  private final DataNode datanode;
  private final DataXceiverServer dataXceiverServer;

  private long opStartTime; //the start time of receiving an Op
  
  public DataXceiver(Socket s, DataNode datanode, 
      DataXceiverServer dataXceiverServer) {
    this.s = s;
    this.isLocal = s.getInetAddress().equals(s.getLocalAddress());
    this.datanode = datanode;
    this.dataXceiverServer = dataXceiverServer;
    dataXceiverServer.childSockets.put(s, s);
    remoteAddress = s.getRemoteSocketAddress().toString();
    localAddress = s.getLocalSocketAddress().toString();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Number of active connections is: "
          + datanode.getXceiverCount());
    }
  }

  /**
   * Update the current thread's name to contain the current status.
   * Use this only after this receiver has started on its thread, i.e.,
   * outside the constructor.
   */
  private void updateCurrentThreadName(String status) {
    StringBuilder sb = new StringBuilder();
    sb.append("DataXceiver for client ").append(remoteAddress);
    if (status != null) {
      sb.append(" [").append(status).append("]");
    }
    Thread.currentThread().setName(sb.toString());
  }

  /** Return the datanode object. */
  DataNode getDataNode() {return datanode;}

  /**
   * Read/write data from/to the DataXceiveServer.
   */
  public void run() {
    updateCurrentThreadName("Waiting for operation");

    DataInputStream in=null; 
    try {
      in = new DataInputStream(
          new BufferedInputStream(NetUtils.getInputStream(s), 
                                  SMALL_BUFFER_SIZE));
      final DataTransferProtocol.Op op = readOp(in);

      // Make sure the xciver count is not exceeded
      int curXceiverCount = datanode.getXceiverCount();
      if (curXceiverCount > dataXceiverServer.maxXceiverCount) {
        throw new IOException("xceiverCount " + curXceiverCount
                              + " exceeds the limit of concurrent xcievers "
                              + dataXceiverServer.maxXceiverCount);
      }

      opStartTime = now();
      processOp(op, in);
    } catch (Throwable t) {
      LOG.error(datanode.getMachineName() + ":DataXceiver",t);
    } finally {
      if (LOG.isDebugEnabled()) {
        LOG.debug(datanode.getMachineName() + ":Number of active connections is: "
            + datanode.getXceiverCount());
      }
      updateCurrentThreadName("Cleaning up");
      IOUtils.closeStream(in);
      IOUtils.closeSocket(s);
      dataXceiverServer.childSockets.remove(s);
    }
  }

  /**
   * Read a block from the disk.
   */
  @Override
  protected void opReadBlock(DataInputStream in, ExtendedBlock block,
      long startOffset, long length, String clientName,
      Token<BlockTokenIdentifier> blockToken) throws IOException {
    OutputStream baseStream = NetUtils.getOutputStream(s, 
        datanode.socketWriteTimeout);
    DataOutputStream out = new DataOutputStream(
                 new BufferedOutputStream(baseStream, SMALL_BUFFER_SIZE));

    if (datanode.isBlockTokenEnabled) {
      try {
        datanode.blockPoolTokenSecretManager.checkAccess(blockToken, null, block,
            BlockTokenSecretManager.AccessMode.READ);
      } catch (InvalidToken e) {
        try {
          ERROR_ACCESS_TOKEN.write(out);
          out.flush();
          LOG.warn("Block token verification failed, for client "
              + remoteAddress + " for OP_READ_BLOCK for block " + block + " : "
              + e.getLocalizedMessage());
          throw e;
        } finally {
          IOUtils.closeStream(out);
        }
      }
    }
  
    // send the block
    BlockSender blockSender = null;
    DatanodeRegistration dnR = 
      datanode.getDNRegistrationForBP(block.getBlockPoolId());
    final String clientTraceFmt =
      clientName.length() > 0 && ClientTraceLog.isInfoEnabled()
        ? String.format(DN_CLIENTTRACE_FORMAT, localAddress, remoteAddress,
            "%d", "HDFS_READ", clientName, "%d",
            dnR.getStorageID(), block, "%d")
        : dnR + " Served block " + block + " to " +
            s.getInetAddress();

    updateCurrentThreadName("Sending block " + block);
    try {
      try {
        blockSender = new BlockSender(block, startOffset, length,
            true, true, false, datanode, clientTraceFmt);
      } catch(IOException e) {
        ERROR.write(out);
        throw e;
      }

      SUCCESS.write(out); // send op status
      long read = blockSender.sendBlock(out, baseStream, null); // send data
      
      datanode.myMetrics.bytesRead.inc((int) read);
      datanode.myMetrics.blocksRead.inc();
    } catch ( SocketException ignored ) {
      // Its ok for remote side to close the connection anytime.
      datanode.myMetrics.blocksRead.inc();
    } catch ( IOException ioe ) {
      /* What exactly should we do here?
       * Earlier version shutdown() datanode if there is disk error.
       */
      LOG.warn(dnR +  ":Got exception while serving " + 
          block + " to " +
                s.getInetAddress() + ":\n" + 
                StringUtils.stringifyException(ioe) );
      throw ioe;
    } finally {
      IOUtils.closeStream(out);
      IOUtils.closeStream(blockSender);
    }

    //update metrics
    updateDuration(datanode.myMetrics.readBlockOp);
    updateCounter(datanode.myMetrics.readsFromLocalClient,
                  datanode.myMetrics.readsFromRemoteClient);
  }

  /**
   * Write a block to disk.
   */
  @Override
  protected void opWriteBlock(DataInputStream in, ExtendedBlock block, 
      int pipelineSize, BlockConstructionStage stage,
      long newGs, long minBytesRcvd, long maxBytesRcvd,
      String client, DatanodeInfo srcDataNode, DatanodeInfo[] targets,
      Token<BlockTokenIdentifier> blockToken) throws IOException {
    updateCurrentThreadName("Receiving block " + block + " client=" + client);

    if (LOG.isDebugEnabled()) {
      LOG.debug("writeBlock receive buf size " + s.getReceiveBufferSize() +
                " tcp no delay " + s.getTcpNoDelay());
    }

    // We later mutate block's generation stamp and length, but we need to
    // forward the original version of the block to downstream mirrors, so
    // make a copy here.
    final ExtendedBlock originalBlock = new ExtendedBlock(block);

    block.setNumBytes(dataXceiverServer.estimateBlockSize);
    LOG.info("Receiving block " + block + 
             " src: " + remoteAddress +
             " dest: " + localAddress);

    DataOutputStream replyOut = null;   // stream to prev target
    replyOut = new DataOutputStream(
                   NetUtils.getOutputStream(s, datanode.socketWriteTimeout));
    DatanodeRegistration dnR = 
      datanode.getDNRegistrationForBP(block.getBlockPoolId());
    if (datanode.isBlockTokenEnabled) {
      try {
        datanode.blockPoolTokenSecretManager.checkAccess(blockToken, null, block,
            BlockTokenSecretManager.AccessMode.WRITE);
      } catch (InvalidToken e) {
        try {
          if (client.length() != 0) {
            ERROR_ACCESS_TOKEN.write(replyOut);
            Text.writeString(replyOut, dnR.getName());
            replyOut.flush();
          }
          LOG.warn("Block token verification failed, for client "
              + remoteAddress + " for OP_WRITE_BLOCK for block " + block
              + " : " + e.getLocalizedMessage());
          throw e;
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
    DataTransferProtocol.Status mirrorInStatus = SUCCESS;
    try {
      if (client.length() == 0 || 
          stage != BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
        // open a block receiver
        blockReceiver = new BlockReceiver(block, in, 
            s.getRemoteSocketAddress().toString(),
            s.getLocalSocketAddress().toString(),
            stage, newGs, minBytesRcvd, maxBytesRcvd,
            client, srcDataNode, datanode);
      } else {
        datanode.data.recoverClose(block, newGs, minBytesRcvd);
      }

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
          int timeoutValue = datanode.socketTimeout
              + (HdfsConstants.READ_TIMEOUT_EXTENSION * targets.length);
          int writeTimeout = datanode.socketWriteTimeout + 
                      (HdfsConstants.WRITE_TIMEOUT_EXTENSION * targets.length);
          NetUtils.connect(mirrorSock, mirrorTarget, timeoutValue);
          mirrorSock.setSoTimeout(timeoutValue);
          mirrorSock.setSendBufferSize(DEFAULT_DATA_SOCKET_SIZE);
          mirrorOut = new DataOutputStream(
             new BufferedOutputStream(
                         NetUtils.getOutputStream(mirrorSock, writeTimeout),
                         SMALL_BUFFER_SIZE));
          mirrorIn = new DataInputStream(NetUtils.getInputStream(mirrorSock));

          // Write header: Copied from DFSClient.java!
          DataTransferProtocol.Sender.opWriteBlock(mirrorOut, originalBlock,
              pipelineSize, stage, newGs, minBytesRcvd, maxBytesRcvd, client,
              srcDataNode, targets, blockToken);

          if (blockReceiver != null) { // send checksum header
            blockReceiver.writeChecksumHeader(mirrorOut);
          }
          mirrorOut.flush();

          // read connect ack (only for clients, not for replication req)
          if (client.length() != 0) {
            mirrorInStatus = DataTransferProtocol.Status.read(mirrorIn);
            firstBadLink = Text.readString(mirrorIn);
            if (LOG.isDebugEnabled() || mirrorInStatus != SUCCESS) {
              LOG.info("Datanode " + targets.length +
                       " got response for connect ack " +
                       " from downstream datanode with firstbadlink as " +
                       firstBadLink);
            }
          }

        } catch (IOException e) {
          if (client.length() != 0) {
            ERROR.write(replyOut);
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
            LOG.info(dnR + ":Exception transfering block " +
                     block + " to mirror " + mirrorNode +
                     ". continuing without the mirror.\n" +
                     StringUtils.stringifyException(e));
          }
        }
      }

      // send connect ack back to source (only for clients)
      if (client.length() != 0) {
        if (LOG.isDebugEnabled() || mirrorInStatus != SUCCESS) {
          LOG.info("Datanode " + targets.length +
                   " forwarding connect ack to upstream firstbadlink is " +
                   firstBadLink);
        }
        mirrorInStatus.write(replyOut);
        Text.writeString(replyOut, firstBadLink);
        replyOut.flush();
      }

      // receive the block and mirror to the next target
      if (blockReceiver != null) {
        String mirrorAddr = (mirrorSock == null) ? null : mirrorNode;
        blockReceiver.receiveBlock(mirrorOut, mirrorIn, replyOut,
            mirrorAddr, null, targets.length);
      }

      // update its generation stamp
      if (client.length() != 0 && 
          stage == BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
        block.setGenerationStamp(newGs);
        block.setNumBytes(minBytesRcvd);
      }
      
      // if this write is for a replication request or recovering
      // a failed close for client, then confirm block. For other client-writes,
      // the block is finalized in the PacketResponder.
      if (client.length() == 0 || 
          stage == BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
        datanode.closeBlock(block, DataNode.EMPTY_DEL_HINT);
        LOG.info("Received block " + block + 
                 " src: " + remoteAddress +
                 " dest: " + localAddress +
                 " of size " + block.getNumBytes());
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

    //update metrics
    updateDuration(datanode.myMetrics.writeBlockOp);
    updateCounter(datanode.myMetrics.writesFromLocalClient,
                  datanode.myMetrics.writesFromRemoteClient);
  }

  /**
   * Get block checksum (MD5 of CRC32).
   */
  @Override
  protected void opBlockChecksum(DataInputStream in, ExtendedBlock block,
      Token<BlockTokenIdentifier> blockToken) throws IOException {
    DataOutputStream out = new DataOutputStream(NetUtils.getOutputStream(s,
        datanode.socketWriteTimeout));
    if (datanode.isBlockTokenEnabled) {
      try {
        datanode.blockPoolTokenSecretManager.checkAccess(blockToken, null,
            block, BlockTokenSecretManager.AccessMode.READ);
      } catch (InvalidToken e) {
        try {
          ERROR_ACCESS_TOKEN.write(out);
          out.flush();
          LOG.warn("Block token verification failed, for client "
              + remoteAddress + " for OP_BLOCK_CHECKSUM for block " + block
              + " : " + e.getLocalizedMessage());
          throw e;
        } finally {
          IOUtils.closeStream(out);
        }

      }
    }

    updateCurrentThreadName("Reading metadata for block " + block);
    final MetaDataInputStream metadataIn = 
      datanode.data.getMetaDataInputStream(block);
    final DataInputStream checksumIn = new DataInputStream(new BufferedInputStream(
        metadataIn, BUFFER_SIZE));

    updateCurrentThreadName("Getting checksum for block " + block);
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
      SUCCESS.write(out);
      out.writeInt(bytesPerCRC);
      out.writeLong(crcPerBlock);
      md5.write(out);
      out.flush();
    } finally {
      IOUtils.closeStream(out);
      IOUtils.closeStream(checksumIn);
      IOUtils.closeStream(metadataIn);
    }

    //update metrics
    updateDuration(datanode.myMetrics.blockChecksumOp);
  }

  /**
   * Read a block from the disk and then sends it to a destination.
   */
  @Override
  protected void opCopyBlock(DataInputStream in, ExtendedBlock block,
      Token<BlockTokenIdentifier> blockToken) throws IOException {
    updateCurrentThreadName("Copying block " + block);
    // Read in the header
    if (datanode.isBlockTokenEnabled) {
      try {
        datanode.blockPoolTokenSecretManager.checkAccess(blockToken, null, block,
            BlockTokenSecretManager.AccessMode.COPY);
      } catch (InvalidToken e) {
        LOG.warn("Invalid access token in request from " + remoteAddress
            + " for OP_COPY_BLOCK for block " + block + " : "
            + e.getLocalizedMessage());
        sendResponse(s, ERROR_ACCESS_TOKEN, datanode.socketWriteTimeout);
        return;
      }

    }

    if (!dataXceiverServer.balanceThrottler.acquire()) { // not able to start
      LOG.info("Not able to copy block " + block.getBlockId() + " to " 
          + s.getRemoteSocketAddress() + " because threads quota is exceeded.");
      sendResponse(s, ERROR, datanode.socketWriteTimeout);
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
      SUCCESS.write(reply);
      // send block content to the target
      long read = blockSender.sendBlock(reply, baseStream, 
                                        dataXceiverServer.balanceThrottler);

      datanode.myMetrics.bytesRead.inc((int) read);
      datanode.myMetrics.blocksRead.inc();
      
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

    //update metrics    
    updateDuration(datanode.myMetrics.copyBlockOp);
  }

  /**
   * Receive a block and write it to disk, it then notifies the namenode to
   * remove the copy from the source.
   */
  @Override
  protected void opReplaceBlock(DataInputStream in,
      ExtendedBlock block, String sourceID, DatanodeInfo proxySource,
      Token<BlockTokenIdentifier> blockToken) throws IOException {
    updateCurrentThreadName("Replacing block " + block + " from " + sourceID);

    /* read header */
    block.setNumBytes(dataXceiverServer.estimateBlockSize);
    if (datanode.isBlockTokenEnabled) {
      try {
        datanode.blockPoolTokenSecretManager.checkAccess(blockToken, null, block,
            BlockTokenSecretManager.AccessMode.REPLACE);
      } catch (InvalidToken e) {
        LOG.warn("Invalid access token in request from " + remoteAddress
            + " for OP_REPLACE_BLOCK for block " + block + " : "
            + e.getLocalizedMessage());
        sendResponse(s, ERROR_ACCESS_TOKEN, datanode.socketWriteTimeout);
        return;
      }
    }

    if (!dataXceiverServer.balanceThrottler.acquire()) { // not able to start
      LOG.warn("Not able to receive block " + block.getBlockId() + " from " 
          + s.getRemoteSocketAddress() + " because threads quota is exceeded.");
      sendResponse(s, ERROR, datanode.socketWriteTimeout);
      return;
    }

    Socket proxySock = null;
    DataOutputStream proxyOut = null;
    DataTransferProtocol.Status opStatus = SUCCESS;
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
      DataTransferProtocol.Sender.opCopyBlock(proxyOut, block, blockToken);

      // receive the response from the proxy
      proxyReply = new DataInputStream(new BufferedInputStream(
          NetUtils.getInputStream(proxySock), BUFFER_SIZE));
      final DataTransferProtocol.Status status
          = DataTransferProtocol.Status.read(proxyReply);
      if (status != SUCCESS) {
        if (status == ERROR_ACCESS_TOKEN) {
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
          null, 0, 0, 0, "", null, datanode);

      // receive a block
      blockReceiver.receiveBlock(null, null, null, null, 
          dataXceiverServer.balanceThrottler, -1);
                    
      // notify name node
      datanode.notifyNamenodeReceivedBlock(block, sourceID);

      LOG.info("Moved block " + block + 
          " from " + s.getRemoteSocketAddress());
      
    } catch (IOException ioe) {
      opStatus = ERROR;
      throw ioe;
    } finally {
      // receive the last byte that indicates the proxy released its thread resource
      if (opStatus == SUCCESS) {
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

    //update metrics
    updateDuration(datanode.myMetrics.replaceBlockOp);
  }

  private void updateDuration(MetricsTimeVaryingRate mtvr) {
    mtvr.inc(now() - opStartTime);
  }

  private void updateCounter(MetricsTimeVaryingInt localCounter,
      MetricsTimeVaryingInt remoteCounter) {
    (isLocal? localCounter: remoteCounter).inc();
  }

  /**
   * Utility function for sending a response.
   * @param s socket to write to
   * @param opStatus status message to write
   * @param timeout send timeout
   **/
  private void sendResponse(Socket s, DataTransferProtocol.Status opStatus,
      long timeout) throws IOException {
    DataOutputStream reply = 
      new DataOutputStream(NetUtils.getOutputStream(s, timeout));
    try {
      opStatus.write(reply);
      reply.flush();
    } finally {
      IOUtils.closeStream(reply);
    }
  }
}
