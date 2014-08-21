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

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR_ACCESS_TOKEN;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR_INVALID;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR_UNSUPPORTED;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;
import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;
import static org.apache.hadoop.util.Time.now;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.security.MessageDigest;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.Receiver;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientReadStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReadOpChecksumInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReleaseShortCircuitAccessResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitShmResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode.ShortCircuitFdsUnsupportedException;
import org.apache.hadoop.hdfs.server.datanode.DataNode.ShortCircuitFdsVersionException;
import org.apache.hadoop.hdfs.server.datanode.ShortCircuitRegistry.NewShmInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;


/**
 * Thread for processing incoming/outgoing data stream.
 */
class DataXceiver extends Receiver implements Runnable {
  public static final Log LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;
  
  private Peer peer;
  private final String remoteAddress; // address of remote side
  private final String localAddress;  // local address of this daemon
  private final DataNode datanode;
  private final DNConf dnConf;
  private final DataXceiverServer dataXceiverServer;
  private final boolean connectToDnViaHostname;
  private long opStartTime; //the start time of receiving an Op
  private final InputStream socketIn;
  private OutputStream socketOut;
  private BlockReceiver blockReceiver = null;
  
  /**
   * Client Name used in previous operation. Not available on first request
   * on the socket.
   */
  private String previousOpClientName;
  
  public static DataXceiver create(Peer peer, DataNode dn,
      DataXceiverServer dataXceiverServer) throws IOException {
    return new DataXceiver(peer, dn, dataXceiverServer);
  }
  
  private DataXceiver(Peer peer, DataNode datanode,
      DataXceiverServer dataXceiverServer) throws IOException {

    this.peer = peer;
    this.dnConf = datanode.getDnConf();
    this.socketIn = peer.getInputStream();
    this.socketOut = peer.getOutputStream();
    this.datanode = datanode;
    this.dataXceiverServer = dataXceiverServer;
    this.connectToDnViaHostname = datanode.getDnConf().connectToDnViaHostname;
    remoteAddress = peer.getRemoteAddressString();
    localAddress = peer.getLocalAddressString();

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
    sb.append("DataXceiver for client ");
    if (previousOpClientName != null) {
      sb.append(previousOpClientName).append(" at ");
    }
    sb.append(remoteAddress);
    if (status != null) {
      sb.append(" [").append(status).append("]");
    }
    Thread.currentThread().setName(sb.toString());
  }

  /** Return the datanode object. */
  DataNode getDataNode() {return datanode;}
  
  private OutputStream getOutputStream() {
    return socketOut;
  }

  public void sendOOB() throws IOException, InterruptedException {
    LOG.info("Sending OOB to peer: " + peer);
    if(blockReceiver!=null)
      blockReceiver.sendOOB();
  }
  
  /**
   * Read/write data from/to the DataXceiverServer.
   */
  @Override
  public void run() {
    int opsProcessed = 0;
    Op op = null;

    try {
      dataXceiverServer.addPeer(peer, Thread.currentThread(), this);
      peer.setWriteTimeout(datanode.getDnConf().socketWriteTimeout);
      InputStream input = socketIn;
      IOStreamPair saslStreams = datanode.saslServer.receive(peer, socketOut,
        socketIn, datanode.getDatanodeId());
      input = new BufferedInputStream(saslStreams.in,
        HdfsConstants.SMALL_BUFFER_SIZE);
      socketOut = saslStreams.out;
      
      super.initialize(new DataInputStream(input));
      
      // We process requests in a loop, and stay around for a short timeout.
      // This optimistic behaviour allows the other end to reuse connections.
      // Setting keepalive timeout to 0 disable this behavior.
      do {
        updateCurrentThreadName("Waiting for operation #" + (opsProcessed + 1));

        try {
          if (opsProcessed != 0) {
            assert dnConf.socketKeepaliveTimeout > 0;
            peer.setReadTimeout(dnConf.socketKeepaliveTimeout);
          } else {
            peer.setReadTimeout(dnConf.socketTimeout);
          }
          op = readOp();
        } catch (InterruptedIOException ignored) {
          // Time out while we wait for client rpc
          break;
        } catch (IOException err) {
          // Since we optimistically expect the next op, it's quite normal to get EOF here.
          if (opsProcessed > 0 &&
              (err instanceof EOFException || err instanceof ClosedChannelException)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Cached " + peer + " closing after " + opsProcessed + " ops");
            }
          } else {
            throw err;
          }
          break;
        }

        // restore normal timeout
        if (opsProcessed != 0) {
          peer.setReadTimeout(dnConf.socketTimeout);
        }

        opStartTime = now();
        processOp(op);
        ++opsProcessed;
      } while ((peer != null) &&
          (!peer.isClosed() && dnConf.socketKeepaliveTimeout > 0));
    } catch (Throwable t) {
      String s = datanode.getDisplayName() + ":DataXceiver error processing "
          + ((op == null) ? "unknown" : op.name()) + " operation "
          + " src: " + remoteAddress + " dst: " + localAddress;
      if (op == Op.WRITE_BLOCK && t instanceof ReplicaAlreadyExistsException) {
        // For WRITE_BLOCK, it is okay if the replica already exists since
        // client and replication may write the same block to the same datanode
        // at the same time.
        if (LOG.isTraceEnabled()) {
          LOG.trace(s, t);
        } else {
          LOG.info(s + "; " + t);
        }
      } else {
        LOG.error(s, t);
      }
    } finally {
      if (LOG.isDebugEnabled()) {
        LOG.debug(datanode.getDisplayName() + ":Number of active connections is: "
            + datanode.getXceiverCount());
      }
      updateCurrentThreadName("Cleaning up");
      if (peer != null) {
        dataXceiverServer.closePeer(peer);
        IOUtils.closeStream(in);
      }
    }
  }

  @Override
  public void requestShortCircuitFds(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> token,
      SlotId slotId, int maxVersion) throws IOException {
    updateCurrentThreadName("Passing file descriptors for block " + blk);
    BlockOpResponseProto.Builder bld = BlockOpResponseProto.newBuilder();
    FileInputStream fis[] = null;
    try {
      if (peer.getDomainSocket() == null) {
        throw new IOException("You cannot pass file descriptors over " +
            "anything but a UNIX domain socket.");
      }
      if (slotId != null) {
        boolean isCached = datanode.data.
            isCached(blk.getBlockPoolId(), blk.getBlockId());
        datanode.shortCircuitRegistry.registerSlot(
            ExtendedBlockId.fromExtendedBlock(blk), slotId, isCached);
      }
      try {
        fis = datanode.requestShortCircuitFdsForRead(blk, token, maxVersion);
      } finally {
        if ((fis == null) && (slotId != null)) {
          datanode.shortCircuitRegistry.unregisterSlot(slotId);
        }
      }
      bld.setStatus(SUCCESS);
      bld.setShortCircuitAccessVersion(DataNode.CURRENT_BLOCK_FORMAT_VERSION);
    } catch (ShortCircuitFdsVersionException e) {
      bld.setStatus(ERROR_UNSUPPORTED);
      bld.setShortCircuitAccessVersion(DataNode.CURRENT_BLOCK_FORMAT_VERSION);
      bld.setMessage(e.getMessage());
    } catch (ShortCircuitFdsUnsupportedException e) {
      bld.setStatus(ERROR_UNSUPPORTED);
      bld.setMessage(e.getMessage());
    } catch (InvalidToken e) {
      bld.setStatus(ERROR_ACCESS_TOKEN);
      bld.setMessage(e.getMessage());
    } catch (IOException e) {
      bld.setStatus(ERROR);
      bld.setMessage(e.getMessage());
    }
    try {
      bld.build().writeDelimitedTo(socketOut);
      if (fis != null) {
        FileDescriptor fds[] = new FileDescriptor[fis.length];
        for (int i = 0; i < fds.length; i++) {
          fds[i] = fis[i].getFD();
        }
        byte buf[] = new byte[] { (byte)0 };
        peer.getDomainSocket().
          sendFileDescriptors(fds, buf, 0, buf.length);
      }
    } finally {
      if (ClientTraceLog.isInfoEnabled()) {
        DatanodeRegistration dnR = datanode.getDNRegistrationForBP(blk
            .getBlockPoolId());
        BlockSender.ClientTraceLog.info(String.format(
            "src: 127.0.0.1, dest: 127.0.0.1, op: REQUEST_SHORT_CIRCUIT_FDS," +
            " blockid: %s, srvID: %s, success: %b",
            blk.getBlockId(), dnR.getDatanodeUuid(), (fis != null)
          ));
      }
      if (fis != null) {
        IOUtils.cleanup(LOG, fis);
      }
    }
  }

  @Override
  public void releaseShortCircuitFds(SlotId slotId) throws IOException {
    boolean success = false;
    try {
      String error;
      Status status;
      try {
        datanode.shortCircuitRegistry.unregisterSlot(slotId);
        error = null;
        status = Status.SUCCESS;
      } catch (UnsupportedOperationException e) {
        error = "unsupported operation";
        status = Status.ERROR_UNSUPPORTED;
      } catch (Throwable e) {
        error = e.getMessage();
        status = Status.ERROR_INVALID;
      }
      ReleaseShortCircuitAccessResponseProto.Builder bld =
          ReleaseShortCircuitAccessResponseProto.newBuilder();
      bld.setStatus(status);
      if (error != null) {
        bld.setError(error);
      }
      bld.build().writeDelimitedTo(socketOut);
      success = true;
    } finally {
      if (ClientTraceLog.isInfoEnabled()) {
        BlockSender.ClientTraceLog.info(String.format(
            "src: 127.0.0.1, dest: 127.0.0.1, op: RELEASE_SHORT_CIRCUIT_FDS," +
            " shmId: %016x%016x, slotIdx: %d, srvID: %s, success: %b",
            slotId.getShmId().getHi(), slotId.getShmId().getLo(),
            slotId.getSlotIdx(), datanode.getDatanodeUuid(), success));
      }
    }
  }

  private void sendShmErrorResponse(Status status, String error)
      throws IOException {
    ShortCircuitShmResponseProto.newBuilder().setStatus(status).
        setError(error).build().writeDelimitedTo(socketOut);
  }

  private void sendShmSuccessResponse(DomainSocket sock, NewShmInfo shmInfo)
      throws IOException {
    ShortCircuitShmResponseProto.newBuilder().setStatus(SUCCESS).
        setId(PBHelper.convert(shmInfo.shmId)).build().
        writeDelimitedTo(socketOut);
    // Send the file descriptor for the shared memory segment.
    byte buf[] = new byte[] { (byte)0 };
    FileDescriptor shmFdArray[] =
        new FileDescriptor[] { shmInfo.stream.getFD() };
    sock.sendFileDescriptors(shmFdArray, buf, 0, buf.length);
  }

  @Override
  public void requestShortCircuitShm(String clientName) throws IOException {
    NewShmInfo shmInfo = null;
    boolean success = false;
    DomainSocket sock = peer.getDomainSocket();
    try {
      if (sock == null) {
        sendShmErrorResponse(ERROR_INVALID, "Bad request from " +
            peer + ": must request a shared " +
            "memory segment over a UNIX domain socket.");
        return;
      }
      try {
        shmInfo = datanode.shortCircuitRegistry.
            createNewMemorySegment(clientName, sock);
        // After calling #{ShortCircuitRegistry#createNewMemorySegment}, the
        // socket is managed by the DomainSocketWatcher, not the DataXceiver.
        releaseSocket();
      } catch (UnsupportedOperationException e) {
        sendShmErrorResponse(ERROR_UNSUPPORTED, 
            "This datanode has not been configured to support " +
            "short-circuit shared memory segments.");
        return;
      } catch (IOException e) {
        sendShmErrorResponse(ERROR,
            "Failed to create shared file descriptor: " + e.getMessage());
        return;
      }
      sendShmSuccessResponse(sock, shmInfo);
      success = true;
    } finally {
      if (ClientTraceLog.isInfoEnabled()) {
        if (success) {
          BlockSender.ClientTraceLog.info(String.format(
              "cliID: %s, src: 127.0.0.1, dest: 127.0.0.1, " +
              "op: REQUEST_SHORT_CIRCUIT_SHM," +
              " shmId: %016x%016x, srvID: %s, success: true",
              clientName, shmInfo.shmId.getHi(), shmInfo.shmId.getLo(),
              datanode.getDatanodeUuid()));
        } else {
          BlockSender.ClientTraceLog.info(String.format(
              "cliID: %s, src: 127.0.0.1, dest: 127.0.0.1, " +
              "op: REQUEST_SHORT_CIRCUIT_SHM, " +
              "shmId: n/a, srvID: %s, success: false",
              clientName, datanode.getDatanodeUuid()));
        }
      }
      if ((!success) && (peer == null)) {
        // If we failed to pass the shared memory segment to the client,
        // close the UNIX domain socket now.  This will trigger the 
        // DomainSocketWatcher callback, cleaning up the segment.
        IOUtils.cleanup(null, sock);
      }
      IOUtils.cleanup(null, shmInfo);
    }
  }

  void releaseSocket() {
    dataXceiverServer.releasePeer(peer);
    peer = null;
  }

  @Override
  public void readBlock(final ExtendedBlock block,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final long blockOffset,
      final long length,
      final boolean sendChecksum,
      final CachingStrategy cachingStrategy) throws IOException {
    previousOpClientName = clientName;

    OutputStream baseStream = getOutputStream();
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
        baseStream, HdfsConstants.SMALL_BUFFER_SIZE));
    checkAccess(out, true, block, blockToken,
        Op.READ_BLOCK, BlockTokenSecretManager.AccessMode.READ);
  
    // send the block
    BlockSender blockSender = null;
    DatanodeRegistration dnR = 
      datanode.getDNRegistrationForBP(block.getBlockPoolId());
    final String clientTraceFmt =
      clientName.length() > 0 && ClientTraceLog.isInfoEnabled()
        ? String.format(DN_CLIENTTRACE_FORMAT, localAddress, remoteAddress,
            "%d", "HDFS_READ", clientName, "%d",
            dnR.getDatanodeUuid(), block, "%d")
        : dnR + " Served block " + block + " to " +
            remoteAddress;

    updateCurrentThreadName("Sending block " + block);
    try {
      try {
        blockSender = new BlockSender(block, blockOffset, length,
            true, false, sendChecksum, datanode, clientTraceFmt,
            cachingStrategy);
      } catch(IOException e) {
        String msg = "opReadBlock " + block + " received exception " + e; 
        LOG.info(msg);
        sendResponse(ERROR, msg);
        throw e;
      }
      
      // send op status
      writeSuccessWithChecksumInfo(blockSender, new DataOutputStream(getOutputStream()));

      long read = blockSender.sendBlock(out, baseStream, null); // send data

      if (blockSender.didSendEntireByteRange()) {
        // If we sent the entire range, then we should expect the client
        // to respond with a Status enum.
        try {
          ClientReadStatusProto stat = ClientReadStatusProto.parseFrom(
              PBHelper.vintPrefixed(in));
          if (!stat.hasStatus()) {
            LOG.warn("Client " + peer.getRemoteAddressString() +
                " did not send a valid status code after reading. " +
                "Will close connection.");
            IOUtils.closeStream(out);
          }
        } catch (IOException ioe) {
          LOG.debug("Error reading client status response. Will close connection.", ioe);
          IOUtils.closeStream(out);
        }
      } else {
        IOUtils.closeStream(out);
      }
      datanode.metrics.incrBytesRead((int) read);
      datanode.metrics.incrBlocksRead();
    } catch ( SocketException ignored ) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(dnR + ":Ignoring exception while serving " + block + " to " +
            remoteAddress, ignored);
      }
      // Its ok for remote side to close the connection anytime.
      datanode.metrics.incrBlocksRead();
      IOUtils.closeStream(out);
    } catch ( IOException ioe ) {
      /* What exactly should we do here?
       * Earlier version shutdown() datanode if there is disk error.
       */
      LOG.warn(dnR + ":Got exception while serving " + block + " to "
          + remoteAddress, ioe);
      throw ioe;
    } finally {
      IOUtils.closeStream(blockSender);
    }

    //update metrics
    datanode.metrics.addReadBlockOp(elapsed());
    datanode.metrics.incrReadsFromClient(peer.isLocal());
  }

  @Override
  public void writeBlock(final ExtendedBlock block,
      final StorageType storageType, 
      final Token<BlockTokenIdentifier> blockToken,
      final String clientname,
      final DatanodeInfo[] targets,
      final StorageType[] targetStorageTypes, 
      final DatanodeInfo srcDataNode,
      final BlockConstructionStage stage,
      final int pipelineSize,
      final long minBytesRcvd,
      final long maxBytesRcvd,
      final long latestGenerationStamp,
      DataChecksum requestedChecksum,
      CachingStrategy cachingStrategy) throws IOException {
    previousOpClientName = clientname;
    updateCurrentThreadName("Receiving block " + block);
    final boolean isDatanode = clientname.length() == 0;
    final boolean isClient = !isDatanode;
    final boolean isTransfer = stage == BlockConstructionStage.TRANSFER_RBW
        || stage == BlockConstructionStage.TRANSFER_FINALIZED;

    // check single target for transfer-RBW/Finalized 
    if (isTransfer && targets.length > 0) {
      throw new IOException(stage + " does not support multiple targets "
          + Arrays.asList(targets));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("opWriteBlock: stage=" + stage + ", clientname=" + clientname 
      		+ "\n  block  =" + block + ", newGs=" + latestGenerationStamp
      		+ ", bytesRcvd=[" + minBytesRcvd + ", " + maxBytesRcvd + "]"
          + "\n  targets=" + Arrays.asList(targets)
          + "; pipelineSize=" + pipelineSize + ", srcDataNode=" + srcDataNode
          );
      LOG.debug("isDatanode=" + isDatanode
          + ", isClient=" + isClient
          + ", isTransfer=" + isTransfer);
      LOG.debug("writeBlock receive buf size " + peer.getReceiveBufferSize() +
                " tcp no delay " + peer.getTcpNoDelay());
    }

    // We later mutate block's generation stamp and length, but we need to
    // forward the original version of the block to downstream mirrors, so
    // make a copy here.
    final ExtendedBlock originalBlock = new ExtendedBlock(block);
    if (block.getNumBytes() == 0) {
      block.setNumBytes(dataXceiverServer.estimateBlockSize);
    }
    LOG.info("Receiving " + block + " src: " + remoteAddress + " dest: "
        + localAddress);

    // reply to upstream datanode or client 
    final DataOutputStream replyOut = new DataOutputStream(
        new BufferedOutputStream(
            getOutputStream(),
            HdfsConstants.SMALL_BUFFER_SIZE));
    checkAccess(replyOut, isClient, block, blockToken,
        Op.WRITE_BLOCK, BlockTokenSecretManager.AccessMode.WRITE);

    DataOutputStream mirrorOut = null;  // stream to next target
    DataInputStream mirrorIn = null;    // reply from next target
    Socket mirrorSock = null;           // socket to next target
    String mirrorNode = null;           // the name:port of next target
    String firstBadLink = "";           // first datanode that failed in connection setup
    Status mirrorInStatus = SUCCESS;
    final String storageUuid;
    try {
      if (isDatanode || 
          stage != BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
        // open a block receiver
        blockReceiver = new BlockReceiver(block, storageType, in,
            peer.getRemoteAddressString(),
            peer.getLocalAddressString(),
            stage, latestGenerationStamp, minBytesRcvd, maxBytesRcvd,
            clientname, srcDataNode, datanode, requestedChecksum,
            cachingStrategy);
        
        storageUuid = blockReceiver.getStorageUuid();
      } else {
        storageUuid = datanode.data.recoverClose(
            block, latestGenerationStamp, minBytesRcvd);
      }

      //
      // Connect to downstream machine, if appropriate
      //
      if (targets.length > 0) {
        InetSocketAddress mirrorTarget = null;
        // Connect to backup machine
        mirrorNode = targets[0].getXferAddr(connectToDnViaHostname);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connecting to datanode " + mirrorNode);
        }
        mirrorTarget = NetUtils.createSocketAddr(mirrorNode);
        mirrorSock = datanode.newSocket();
        try {
          int timeoutValue = dnConf.socketTimeout
              + (HdfsServerConstants.READ_TIMEOUT_EXTENSION * targets.length);
          int writeTimeout = dnConf.socketWriteTimeout + 
                      (HdfsServerConstants.WRITE_TIMEOUT_EXTENSION * targets.length);
          NetUtils.connect(mirrorSock, mirrorTarget, timeoutValue);
          mirrorSock.setSoTimeout(timeoutValue);
          mirrorSock.setSendBufferSize(HdfsConstants.DEFAULT_DATA_SOCKET_SIZE);
          
          OutputStream unbufMirrorOut = NetUtils.getOutputStream(mirrorSock,
              writeTimeout);
          InputStream unbufMirrorIn = NetUtils.getInputStream(mirrorSock);
          DataEncryptionKeyFactory keyFactory =
            datanode.getDataEncryptionKeyFactoryForBlock(block);
          IOStreamPair saslStreams = datanode.saslClient.socketSend(mirrorSock,
            unbufMirrorOut, unbufMirrorIn, keyFactory, blockToken, targets[0]);
          unbufMirrorOut = saslStreams.out;
          unbufMirrorIn = saslStreams.in;
          mirrorOut = new DataOutputStream(new BufferedOutputStream(unbufMirrorOut,
              HdfsConstants.SMALL_BUFFER_SIZE));
          mirrorIn = new DataInputStream(unbufMirrorIn);

          new Sender(mirrorOut).writeBlock(originalBlock, targetStorageTypes[0],
              blockToken, clientname, targets, targetStorageTypes, srcDataNode,
              stage, pipelineSize, minBytesRcvd, maxBytesRcvd,
              latestGenerationStamp, requestedChecksum, cachingStrategy);

          mirrorOut.flush();

          // read connect ack (only for clients, not for replication req)
          if (isClient) {
            BlockOpResponseProto connectAck =
              BlockOpResponseProto.parseFrom(PBHelper.vintPrefixed(mirrorIn));
            mirrorInStatus = connectAck.getStatus();
            firstBadLink = connectAck.getFirstBadLink();
            if (LOG.isDebugEnabled() || mirrorInStatus != SUCCESS) {
              LOG.info("Datanode " + targets.length +
                       " got response for connect ack " +
                       " from downstream datanode with firstbadlink as " +
                       firstBadLink);
            }
          }

        } catch (IOException e) {
          if (isClient) {
            BlockOpResponseProto.newBuilder()
              .setStatus(ERROR)
               // NB: Unconditionally using the xfer addr w/o hostname
              .setFirstBadLink(targets[0].getXferAddr())
              .build()
              .writeDelimitedTo(replyOut);
            replyOut.flush();
          }
          IOUtils.closeStream(mirrorOut);
          mirrorOut = null;
          IOUtils.closeStream(mirrorIn);
          mirrorIn = null;
          IOUtils.closeSocket(mirrorSock);
          mirrorSock = null;
          if (isClient) {
            LOG.error(datanode + ":Exception transfering block " +
                      block + " to mirror " + mirrorNode + ": " + e);
            throw e;
          } else {
            LOG.info(datanode + ":Exception transfering " +
                     block + " to mirror " + mirrorNode +
                     "- continuing without the mirror", e);
          }
        }
      }

      // send connect-ack to source for clients and not transfer-RBW/Finalized
      if (isClient && !isTransfer) {
        if (LOG.isDebugEnabled() || mirrorInStatus != SUCCESS) {
          LOG.info("Datanode " + targets.length +
                   " forwarding connect ack to upstream firstbadlink is " +
                   firstBadLink);
        }
        BlockOpResponseProto.newBuilder()
          .setStatus(mirrorInStatus)
          .setFirstBadLink(firstBadLink)
          .build()
          .writeDelimitedTo(replyOut);
        replyOut.flush();
      }

      // receive the block and mirror to the next target
      if (blockReceiver != null) {
        String mirrorAddr = (mirrorSock == null) ? null : mirrorNode;
        blockReceiver.receiveBlock(mirrorOut, mirrorIn, replyOut,
            mirrorAddr, null, targets, false);

        // send close-ack for transfer-RBW/Finalized 
        if (isTransfer) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("TRANSFER: send close-ack");
          }
          writeResponse(SUCCESS, null, replyOut);
        }
      }

      // update its generation stamp
      if (isClient && 
          stage == BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
        block.setGenerationStamp(latestGenerationStamp);
        block.setNumBytes(minBytesRcvd);
      }
      
      // if this write is for a replication request or recovering
      // a failed close for client, then confirm block. For other client-writes,
      // the block is finalized in the PacketResponder.
      if (isDatanode ||
          stage == BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
        datanode.closeBlock(block, DataNode.EMPTY_DEL_HINT, storageUuid);
        LOG.info("Received " + block + " src: " + remoteAddress + " dest: "
            + localAddress + " of size " + block.getNumBytes());
      }

      
    } catch (IOException ioe) {
      LOG.info("opWriteBlock " + block + " received exception " + ioe);
      throw ioe;
    } finally {
      // close all opened streams
      IOUtils.closeStream(mirrorOut);
      IOUtils.closeStream(mirrorIn);
      IOUtils.closeStream(replyOut);
      IOUtils.closeSocket(mirrorSock);
      IOUtils.closeStream(blockReceiver);
      blockReceiver = null;
    }

    //update metrics
    datanode.metrics.addWriteBlockOp(elapsed());
    datanode.metrics.incrWritesFromClient(peer.isLocal());
  }

  @Override
  public void transferBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final DatanodeInfo[] targets,
      final StorageType[] targetStorageTypes) throws IOException {
    checkAccess(socketOut, true, blk, blockToken,
        Op.TRANSFER_BLOCK, BlockTokenSecretManager.AccessMode.COPY);
    previousOpClientName = clientName;
    updateCurrentThreadName(Op.TRANSFER_BLOCK + " " + blk);

    final DataOutputStream out = new DataOutputStream(
        getOutputStream());
    try {
      datanode.transferReplicaForPipelineRecovery(blk, targets,
          targetStorageTypes, clientName);
      writeResponse(Status.SUCCESS, null, out);
    } finally {
      IOUtils.closeStream(out);
    }
  }

  private MD5Hash calcPartialBlockChecksum(ExtendedBlock block,
      long requestLength, DataChecksum checksum, DataInputStream checksumIn)
      throws IOException {
    final int bytesPerCRC = checksum.getBytesPerChecksum();
    final int csize = checksum.getChecksumSize();
    final byte[] buffer = new byte[4*1024];
    MessageDigest digester = MD5Hash.getDigester();

    long remaining = requestLength / bytesPerCRC * csize;
    for (int toDigest = 0; remaining > 0; remaining -= toDigest) {
      toDigest = checksumIn.read(buffer, 0,
          (int) Math.min(remaining, buffer.length));
      if (toDigest < 0) {
        break;
      }
      digester.update(buffer, 0, toDigest);
    }
    
    int partialLength = (int) (requestLength % bytesPerCRC);
    if (partialLength > 0) {
      byte[] buf = new byte[partialLength];
      final InputStream blockIn = datanode.data.getBlockInputStream(block,
          requestLength - partialLength);
      try {
        // Get the CRC of the partialLength.
        IOUtils.readFully(blockIn, buf, 0, partialLength);
      } finally {
        IOUtils.closeStream(blockIn);
      }
      checksum.update(buf, 0, partialLength);
      byte[] partialCrc = new byte[csize];
      checksum.writeValue(partialCrc, 0, true);
      digester.update(partialCrc);
    }
    return new MD5Hash(digester.digest());
  }

  @Override
  public void blockChecksum(final ExtendedBlock block,
      final Token<BlockTokenIdentifier> blockToken) throws IOException {
    final DataOutputStream out = new DataOutputStream(
        getOutputStream());
    checkAccess(out, true, block, blockToken,
        Op.BLOCK_CHECKSUM, BlockTokenSecretManager.AccessMode.READ);
    // client side now can specify a range of the block for checksum
    long requestLength = block.getNumBytes();
    Preconditions.checkArgument(requestLength >= 0);
    long visibleLength = datanode.data.getReplicaVisibleLength(block);
    boolean partialBlk = requestLength < visibleLength;

    updateCurrentThreadName("Reading metadata for block " + block);
    final LengthInputStream metadataIn = datanode.data
        .getMetaDataInputStream(block);
    
    final DataInputStream checksumIn = new DataInputStream(
        new BufferedInputStream(metadataIn, HdfsConstants.IO_FILE_BUFFER_SIZE));
    updateCurrentThreadName("Getting checksum for block " + block);
    try {
      //read metadata file
      final BlockMetadataHeader header = BlockMetadataHeader
          .readHeader(checksumIn);
      final DataChecksum checksum = header.getChecksum();
      final int csize = checksum.getChecksumSize();
      final int bytesPerCRC = checksum.getBytesPerChecksum();
      final long crcPerBlock = csize <= 0 ? 0 : 
        (metadataIn.getLength() - BlockMetadataHeader.getHeaderSize()) / csize;

      final MD5Hash md5 = partialBlk && crcPerBlock > 0 ? 
          calcPartialBlockChecksum(block, requestLength, checksum, checksumIn)
            : MD5Hash.digest(checksumIn);
      if (LOG.isDebugEnabled()) {
        LOG.debug("block=" + block + ", bytesPerCRC=" + bytesPerCRC
            + ", crcPerBlock=" + crcPerBlock + ", md5=" + md5);
      }

      //write reply
      BlockOpResponseProto.newBuilder()
        .setStatus(SUCCESS)
        .setChecksumResponse(OpBlockChecksumResponseProto.newBuilder()             
          .setBytesPerCrc(bytesPerCRC)
          .setCrcPerBlock(crcPerBlock)
          .setMd5(ByteString.copyFrom(md5.getDigest()))
          .setCrcType(PBHelper.convert(checksum.getChecksumType())))
        .build()
        .writeDelimitedTo(out);
      out.flush();
    } finally {
      IOUtils.closeStream(out);
      IOUtils.closeStream(checksumIn);
      IOUtils.closeStream(metadataIn);
    }

    //update metrics
    datanode.metrics.addBlockChecksumOp(elapsed());
  }

  @Override
  public void copyBlock(final ExtendedBlock block,
      final Token<BlockTokenIdentifier> blockToken) throws IOException {
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
        sendResponse(ERROR_ACCESS_TOKEN, "Invalid access token");
        return;
      }

    }

    if (!dataXceiverServer.balanceThrottler.acquire()) { // not able to start
      String msg = "Not able to copy block " + block.getBlockId() + " " +
          "to " + peer.getRemoteAddressString() + " because threads " +
          "quota is exceeded.";
      LOG.info(msg);
      sendResponse(ERROR, msg);
      return;
    }

    BlockSender blockSender = null;
    DataOutputStream reply = null;
    boolean isOpSuccess = true;

    try {
      // check if the block exists or not
      blockSender = new BlockSender(block, 0, -1, false, false, true, datanode, 
          null, CachingStrategy.newDropBehind());

      // set up response stream
      OutputStream baseStream = getOutputStream();
      reply = new DataOutputStream(new BufferedOutputStream(
          baseStream, HdfsConstants.SMALL_BUFFER_SIZE));

      // send status first
      writeSuccessWithChecksumInfo(blockSender, reply);
      // send block content to the target
      long read = blockSender.sendBlock(reply, baseStream, 
                                        dataXceiverServer.balanceThrottler);

      datanode.metrics.incrBytesRead((int) read);
      datanode.metrics.incrBlocksRead();
      
      LOG.info("Copied " + block + " to " + peer.getRemoteAddressString());
    } catch (IOException ioe) {
      isOpSuccess = false;
      LOG.info("opCopyBlock " + block + " received exception " + ioe);
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
    datanode.metrics.addCopyBlockOp(elapsed());
  }

  @Override
  public void replaceBlock(final ExtendedBlock block,
      final StorageType storageType, 
      final Token<BlockTokenIdentifier> blockToken,
      final String delHint,
      final DatanodeInfo proxySource) throws IOException {
    updateCurrentThreadName("Replacing block " + block + " from " + delHint);

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
        sendResponse(ERROR_ACCESS_TOKEN, "Invalid access token");
        return;
      }
    }

    if (!dataXceiverServer.balanceThrottler.acquire()) { // not able to start
      String msg = "Not able to receive block " + block.getBlockId() +
          " from " + peer.getRemoteAddressString() + " because threads " +
          "quota is exceeded.";
      LOG.warn(msg);
      sendResponse(ERROR, msg);
      return;
    }

    Socket proxySock = null;
    DataOutputStream proxyOut = null;
    Status opStatus = SUCCESS;
    String errMsg = null;
    BlockReceiver blockReceiver = null;
    DataInputStream proxyReply = null;
    DataOutputStream replyOut = new DataOutputStream(getOutputStream());
    try {
      // get the output stream to the proxy
      final String dnAddr = proxySource.getXferAddr(connectToDnViaHostname);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Connecting to datanode " + dnAddr);
      }
      InetSocketAddress proxyAddr = NetUtils.createSocketAddr(dnAddr);
      proxySock = datanode.newSocket();
      NetUtils.connect(proxySock, proxyAddr, dnConf.socketTimeout);
      proxySock.setSoTimeout(dnConf.socketTimeout);

      OutputStream unbufProxyOut = NetUtils.getOutputStream(proxySock,
          dnConf.socketWriteTimeout);
      InputStream unbufProxyIn = NetUtils.getInputStream(proxySock);
      DataEncryptionKeyFactory keyFactory =
        datanode.getDataEncryptionKeyFactoryForBlock(block);
      IOStreamPair saslStreams = datanode.saslClient.socketSend(proxySock,
        unbufProxyOut, unbufProxyIn, keyFactory, blockToken, proxySource);
      unbufProxyOut = saslStreams.out;
      unbufProxyIn = saslStreams.in;
      
      proxyOut = new DataOutputStream(new BufferedOutputStream(unbufProxyOut, 
          HdfsConstants.SMALL_BUFFER_SIZE));
      proxyReply = new DataInputStream(new BufferedInputStream(unbufProxyIn,
          HdfsConstants.IO_FILE_BUFFER_SIZE));

      /* send request to the proxy */
      new Sender(proxyOut).copyBlock(block, blockToken);

      // receive the response from the proxy
      
      BlockOpResponseProto copyResponse = BlockOpResponseProto.parseFrom(
          PBHelper.vintPrefixed(proxyReply));

      if (copyResponse.getStatus() != SUCCESS) {
        if (copyResponse.getStatus() == ERROR_ACCESS_TOKEN) {
          throw new IOException("Copy block " + block + " from "
              + proxySock.getRemoteSocketAddress()
              + " failed due to access token error");
        }
        throw new IOException("Copy block " + block + " from "
            + proxySock.getRemoteSocketAddress() + " failed");
      }
      
      // get checksum info about the block we're copying
      ReadOpChecksumInfoProto checksumInfo = copyResponse.getReadOpChecksumInfo();
      DataChecksum remoteChecksum = DataTransferProtoUtil.fromProto(
          checksumInfo.getChecksum());
      // open a block receiver and check if the block does not exist
      blockReceiver = new BlockReceiver(block, storageType,
          proxyReply, proxySock.getRemoteSocketAddress().toString(),
          proxySock.getLocalSocketAddress().toString(),
          null, 0, 0, 0, "", null, datanode, remoteChecksum,
          CachingStrategy.newDropBehind());

      // receive a block
      blockReceiver.receiveBlock(null, null, replyOut, null, 
          dataXceiverServer.balanceThrottler, null, true);
                    
      // notify name node
      datanode.notifyNamenodeReceivedBlock(
          block, delHint, blockReceiver.getStorageUuid());

      LOG.info("Moved " + block + " from " + peer.getRemoteAddressString()
          + ", delHint=" + delHint);
      
    } catch (IOException ioe) {
      opStatus = ERROR;
      errMsg = "opReplaceBlock " + block + " received exception " + ioe; 
      LOG.info(errMsg);
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
        sendResponse(opStatus, errMsg);
      } catch (IOException ioe) {
        LOG.warn("Error writing reply back to " + peer.getRemoteAddressString());
      }
      IOUtils.closeStream(proxyOut);
      IOUtils.closeStream(blockReceiver);
      IOUtils.closeStream(proxyReply);
      IOUtils.closeStream(replyOut);
    }

    //update metrics
    datanode.metrics.addReplaceBlockOp(elapsed());
  }

  private long elapsed() {
    return now() - opStartTime;
  }

  /**
   * Utility function for sending a response.
   * 
   * @param status status message to write
   * @param message message to send to the client or other DN
   */
  private void sendResponse(Status status,
      String message) throws IOException {
    writeResponse(status, message, getOutputStream());
  }

  private static void writeResponse(Status status, String message, OutputStream out)
  throws IOException {
    BlockOpResponseProto.Builder response = BlockOpResponseProto.newBuilder()
      .setStatus(status);
    if (message != null) {
      response.setMessage(message);
    }
    response.build().writeDelimitedTo(out);
    out.flush();
  }
  
  private void writeSuccessWithChecksumInfo(BlockSender blockSender,
      DataOutputStream out) throws IOException {

    ReadOpChecksumInfoProto ckInfo = ReadOpChecksumInfoProto.newBuilder()
      .setChecksum(DataTransferProtoUtil.toProto(blockSender.getChecksum()))
      .setChunkOffset(blockSender.getOffset())
      .build();
      
    BlockOpResponseProto response = BlockOpResponseProto.newBuilder()
      .setStatus(SUCCESS)
      .setReadOpChecksumInfo(ckInfo)
      .build();
    response.writeDelimitedTo(out);
    out.flush();
  }
  

  private void checkAccess(OutputStream out, final boolean reply, 
      final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> t,
      final Op op,
      final BlockTokenSecretManager.AccessMode mode) throws IOException {
    if (datanode.isBlockTokenEnabled) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Checking block access token for block '" + blk.getBlockId()
            + "' with mode '" + mode + "'");
      }
      try {
        datanode.blockPoolTokenSecretManager.checkAccess(t, null, blk, mode);
      } catch(InvalidToken e) {
        try {
          if (reply) {
            BlockOpResponseProto.Builder resp = BlockOpResponseProto.newBuilder()
              .setStatus(ERROR_ACCESS_TOKEN);
            if (mode == BlockTokenSecretManager.AccessMode.WRITE) {
              DatanodeRegistration dnR = 
                datanode.getDNRegistrationForBP(blk.getBlockPoolId());
              // NB: Unconditionally using the xfer addr w/o hostname
              resp.setFirstBadLink(dnR.getXferAddr());
            }
            resp.build().writeDelimitedTo(out);
            out.flush();
          }
          LOG.warn("Block token verification failed: op=" + op
              + ", remoteAddress=" + remoteAddress
              + ", message=" + e.getLocalizedMessage());
          throw e;
        } finally {
          IOUtils.closeStream(out);
        }
      }
    }
  }
}
