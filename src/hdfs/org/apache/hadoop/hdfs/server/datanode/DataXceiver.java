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
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface.MetaDataInputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
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
      if ( version != DATA_TRANSFER_VERSION ) {
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
      case OP_READ_BLOCK:
        readBlock( in );
        datanode.myMetrics.readBlockOp.inc(DataNode.now() - startTime);
        if (local)
          datanode.myMetrics.readsFromLocalClient.inc();
        else
          datanode.myMetrics.readsFromRemoteClient.inc();
        break;
      case OP_WRITE_BLOCK:
        writeBlock( in );
        datanode.myMetrics.writeBlockOp.inc(DataNode.now() - startTime);
        if (local)
          datanode.myMetrics.writesFromLocalClient.inc();
        else
          datanode.myMetrics.writesFromRemoteClient.inc();
        break;
      case OP_READ_METADATA:
        readMetadata( in );
        datanode.myMetrics.readMetadataOp.inc(DataNode.now() - startTime);
        break;
      case OP_REPLACE_BLOCK: // for balancing purpose; send to a destination
        replaceBlock(in);
        datanode.myMetrics.replaceBlockOp.inc(DataNode.now() - startTime);
        break;
      case OP_COPY_BLOCK: // for balancing purpose; send to a proxy source
        copyBlock(in);
        datanode.myMetrics.copyBlockOp.inc(DataNode.now() - startTime);
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
    // send the block
    OutputStream baseStream = NetUtils.getOutputStream(s, 
        datanode.socketWriteTimeout);
    DataOutputStream out = new DataOutputStream(
                 new BufferedOutputStream(baseStream, SMALL_BUFFER_SIZE));
    
    BlockSender blockSender = null;
    final String clientTraceFmt =
      clientName.length() > 0 && ClientTraceLog.isInfoEnabled()
        ? String.format(DN_CLIENTTRACE_FORMAT, localAddress, remoteAddress,
            "%d", "HDFS_READ", clientName,
            datanode.dnRegistration.getStorageID(), block)
        : datanode.dnRegistration + " Served block " + block + " to " +
            s.getInetAddress();
    try {
      try {
        blockSender = new BlockSender(block, startOffset, length,
            true, true, false, datanode, clientTraceFmt);
      } catch(IOException e) {
        out.writeShort(OP_STATUS_ERROR);
        throw e;
      }

      out.writeShort(DataNode.OP_STATUS_SUCCESS); // send op status
      long read = blockSender.sendBlock(out, baseStream, null); // send data

      if (blockSender.isBlockReadFully()) {
        // See if client verification succeeded. 
        // This is an optional response from client.
        try {
          if (in.readShort() == OP_STATUS_CHECKSUM_OK  && 
              datanode.blockScanner != null) {
            datanode.blockScanner.verifiedByClient(block);
          }
        } catch (IOException ignored) {}
      }
      
      datanode.myMetrics.bytesRead.inc((int) read);
      datanode.myMetrics.blocksRead.inc();
    } catch ( SocketException ignored ) {
      // Its ok for remote side to close the connection anytime.
      datanode.myMetrics.blocksRead.inc();
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

    DataOutputStream mirrorOut = null;  // stream to next target
    DataInputStream mirrorIn = null;    // reply from next target
    DataOutputStream replyOut = null;   // stream to prev target
    Socket mirrorSock = null;           // socket to next target
    BlockReceiver blockReceiver = null; // responsible for data handling
    String mirrorNode = null;           // the name:port of next target
    String firstBadLink = "";           // first datanode that failed in connection setup
    try {
      // open a block receiver and check if the block does not exist
      blockReceiver = new BlockReceiver(block, in, 
          s.getRemoteSocketAddress().toString(),
          s.getLocalSocketAddress().toString(),
          isRecovery, client, srcDataNode, datanode);

      // get a connection back to the previous target
      replyOut = new DataOutputStream(
                     NetUtils.getOutputStream(s, datanode.socketWriteTimeout));

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
                             (WRITE_TIMEOUT_EXTENSION * numTargets);
          mirrorSock.connect(mirrorTarget, timeoutValue);
          mirrorSock.setSoTimeout(timeoutValue);
          mirrorSock.setSendBufferSize(DEFAULT_DATA_SOCKET_SIZE);
          mirrorOut = new DataOutputStream(
             new BufferedOutputStream(
                         NetUtils.getOutputStream(mirrorSock, writeTimeout),
                         SMALL_BUFFER_SIZE));
          mirrorIn = new DataInputStream(NetUtils.getInputStream(mirrorSock));

          // Write header: Copied from DFSClient.java!
          mirrorOut.writeShort( DATA_TRANSFER_VERSION );
          mirrorOut.write( OP_WRITE_BLOCK );
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

          blockReceiver.writeChecksumHeader(mirrorOut);
          mirrorOut.flush();

          // read connect ack (only for clients, not for replication req)
          if (client.length() != 0) {
            firstBadLink = Text.readString(mirrorIn);
            if (LOG.isDebugEnabled() || firstBadLink.length() > 0) {
              LOG.info("Datanode " + targets.length +
                       " got response for connect ack " +
                       " from downstream datanode with firstbadlink as " +
                       firstBadLink);
            }
          }

        } catch (IOException e) {
          if (client.length() != 0) {
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
        if (LOG.isDebugEnabled() || firstBadLink.length() > 0) {
          LOG.info("Datanode " + targets.length +
                   " forwarding connect ack to upstream firstbadlink is " +
                   firstBadLink);
        }
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
   * Reads the metadata and sends the data in one 'DATA_CHUNK'.
   * @param in
   */
  void readMetadata(DataInputStream in) throws IOException {
    Block block = new Block( in.readLong(), 0 , in.readLong());
    MetaDataInputStream checksumIn = null;
    DataOutputStream out = null;
    
    try {

      checksumIn = datanode.data.getMetaDataInputStream(block);
      
      long fileSize = checksumIn.getLength();

      if (fileSize >= 1L<<31 || fileSize <= 0) {
          throw new IOException("Unexpected size for checksumFile of block" +
                  block);
      }

      byte [] buf = new byte[(int)fileSize];
      IOUtils.readFully(checksumIn, buf, 0, buf.length);
      
      out = new DataOutputStream(
                NetUtils.getOutputStream(s, datanode.socketWriteTimeout));
      
      out.writeByte(OP_STATUS_SUCCESS);
      out.writeInt(buf.length);
      out.write(buf);
      
      //last DATA_CHUNK
      out.writeInt(0);
    } finally {
      IOUtils.closeStream(out);
      IOUtils.closeStream(checksumIn);
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
    String source = Text.readString(in); // read del hint
    DatanodeInfo target = new DatanodeInfo(); // read target
    target.readFields(in);

    Socket targetSock = null;
    short opStatus = OP_STATUS_SUCCESS;
    BlockSender blockSender = null;
    DataOutputStream targetOut = null;
    try {
      datanode.balancingSem.acquireUninterruptibly();
      
      // check if the block exists or not
      blockSender = new BlockSender(block, 0, -1, false, false, false, 
          datanode);

      // get the output stream to the target
      InetSocketAddress targetAddr = NetUtils.createSocketAddr(
          target.getName());
      targetSock = datanode.newSocket();
      targetSock.connect(targetAddr, datanode.socketTimeout);
      targetSock.setSoTimeout(datanode.socketTimeout);

      OutputStream baseStream = NetUtils.getOutputStream(targetSock, 
          datanode.socketWriteTimeout);
      targetOut = new DataOutputStream(
                     new BufferedOutputStream(baseStream, SMALL_BUFFER_SIZE));

      /* send request to the target */
      // fist write header info
      targetOut.writeShort(DATA_TRANSFER_VERSION); // transfer version
      targetOut.writeByte(OP_REPLACE_BLOCK); // op code
      targetOut.writeLong(block.getBlockId()); // block id
      targetOut.writeLong(block.getGenerationStamp()); // block id
      Text.writeString( targetOut, source); // del hint

      // then send data
      long read = blockSender.sendBlock(targetOut, baseStream, 
                                        datanode.balancingThrottler);

      datanode.myMetrics.bytesRead.inc((int) read);
      datanode.myMetrics.blocksRead.inc();
      
      // check the response from target
      receiveResponse(targetSock, 1);

      LOG.info("Copied block " + block + " to " + targetAddr);
    } catch (IOException ioe) {
      opStatus = OP_STATUS_ERROR;
      LOG.warn("Got exception while serving " + block + " to "
          + target.getName() + ": " + StringUtils.stringifyException(ioe));
      throw ioe;
    } finally {
      /* send response to the requester */
      try {
        sendResponse(s, opStatus, datanode.socketWriteTimeout);
      } catch (IOException replyE) {
        LOG.warn("Error writing the response back to "+
            s.getRemoteSocketAddress() + "\n" +
            StringUtils.stringifyException(replyE) );
      }
      IOUtils.closeStream(targetOut);
      IOUtils.closeStream(blockSender);
      datanode.balancingSem.release();
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
    datanode.balancingSem.acquireUninterruptibly();

    /* read header */
    Block block = new Block(in.readLong(), dataXceiverServer.estimateBlockSize,
        in.readLong()); // block id & len
    String sourceID = Text.readString(in);

    short opStatus = OP_STATUS_SUCCESS;
    BlockReceiver blockReceiver = null;
    try {
      // open a block receiver and check if the block does not exist
       blockReceiver = new BlockReceiver(
          block, in, s.getRemoteSocketAddress().toString(),
          s.getLocalSocketAddress().toString(), false, "", null, datanode);

      // receive a block
      blockReceiver.receiveBlock(null, null, null, null, 
          datanode.balancingThrottler, -1);
                    
      // notify name node
      datanode.notifyNamenodeReceivedBlock(block, sourceID);

      LOG.info("Moved block " + block + 
          " from " + s.getRemoteSocketAddress());
    } catch (IOException ioe) {
      opStatus = OP_STATUS_ERROR;
      throw ioe;
    } finally {
      // send response back
      try {
        sendResponse(s, opStatus, datanode.socketWriteTimeout);
      } catch (IOException ioe) {
        LOG.warn("Error writing reply back to " + s.getRemoteSocketAddress());
      }
      IOUtils.closeStream(blockReceiver);
      datanode.balancingSem.release();
    }
  }
  
  /**
   *  Utility function for receiving a response.
   *  @param s socket to read from
   *  @param numTargets number of responses to read
   **/
  private void receiveResponse(Socket s, int numTargets) throws IOException {
    // check the response
    DataInputStream reply = new DataInputStream(new BufferedInputStream(
                                NetUtils.getInputStream(s), BUFFER_SIZE));
    try {
      for (int i = 0; i < numTargets; i++) {
        short opStatus = reply.readShort();
        if(opStatus != OP_STATUS_SUCCESS) {
          throw new IOException("operation failed at "+
              s.getInetAddress());
        } 
      }
    } finally {
      IOUtils.closeStream(reply);
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
