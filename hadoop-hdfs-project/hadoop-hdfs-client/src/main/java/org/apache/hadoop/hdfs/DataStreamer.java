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

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.BlockWrite;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.tracing.Span;
import org.apache.hadoop.tracing.SpanContext;
import org.apache.hadoop.tracing.TraceScope;
import org.apache.hadoop.tracing.Tracer;

import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.thirdparty.com.google.common.cache.RemovalListener;
import org.apache.hadoop.thirdparty.com.google.common.cache.RemovalNotification;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

/*********************************************************************
 *
 * The DataStreamer class is responsible for sending data packets to the
 * datanodes in the pipeline. It retrieves a new blockid and block locations
 * from the namenode, and starts streaming packets to the pipeline of
 * Datanodes. Every packet has a sequence number associated with
 * it. When all the packets for a block are sent out and acks for each
 * if them are received, the DataStreamer closes the current block.
 *
 * The DataStreamer thread picks up packets from the dataQueue, sends it to
 * the first datanode in the pipeline and moves it from the dataQueue to the
 * ackQueue. The ResponseProcessor receives acks from the datanodes. When an
 * successful ack for a packet is received from all datanodes, the
 * ResponseProcessor removes the corresponding packet from the ackQueue.
 *
 * In case of error, all outstanding packets are moved from ackQueue. A new
 * pipeline is setup by eliminating the bad datanode from the original
 * pipeline. The DataStreamer now starts sending packets from the dataQueue.
 *
 *********************************************************************/

@InterfaceAudience.Private
class DataStreamer extends Daemon {
  static final Logger LOG = LoggerFactory.getLogger(DataStreamer.class);

  private class RefetchEncryptionKeyPolicy {
    private int fetchEncryptionKeyTimes = 0;
    private InvalidEncryptionKeyException lastException;
    private final DatanodeInfo src;

    RefetchEncryptionKeyPolicy(DatanodeInfo src) {
      this.src = src;
    }
    boolean continueRetryingOrThrow() throws InvalidEncryptionKeyException {
      if (fetchEncryptionKeyTimes >= 2) {
        // hit the same exception twice connecting to the node, so
        // throw the exception and exclude the node.
        throw lastException;
      }
      // Don't exclude this node just yet.
      // Try again with a new encryption key.
      LOG.info("Will fetch a new encryption key and retry, "
          + "encryption key was invalid when connecting to "
          + this.src + ": ", lastException);
      // The encryption key used is invalid.
      dfsClient.clearDataEncryptionKey();
      return true;
    }

    /**
     * Record a connection exception.
     */
    void recordFailure(final InvalidEncryptionKeyException e)
        throws InvalidEncryptionKeyException {
      fetchEncryptionKeyTimes++;
      lastException = e;
    }
  }

  private class StreamerStreams implements java.io.Closeable {
    private Socket sock = null;
    private DataOutputStream out = null;
    private DataInputStream in = null;

    StreamerStreams(final DatanodeInfo src,
        final long writeTimeout, final long readTimeout,
        final Token<BlockTokenIdentifier> blockToken)
        throws IOException {
      sock = createSocketForPipeline(src, 2, dfsClient);

      OutputStream unbufOut = NetUtils.getOutputStream(sock, writeTimeout);
      InputStream unbufIn = NetUtils.getInputStream(sock, readTimeout);
      IOStreamPair saslStreams = dfsClient.saslClient
          .socketSend(sock, unbufOut, unbufIn, dfsClient, blockToken, src);
      unbufOut = saslStreams.out;
      unbufIn = saslStreams.in;
      out = new DataOutputStream(new BufferedOutputStream(unbufOut,
          DFSUtilClient.getSmallBufferSize(dfsClient.getConfiguration())));
      in = new DataInputStream(unbufIn);
    }

    void sendTransferBlock(final DatanodeInfo[] targets,
        final StorageType[] targetStorageTypes,
        final String[] targetStorageIDs,
        final Token<BlockTokenIdentifier> blockToken) throws IOException {
      //send the TRANSFER_BLOCK request
      new Sender(out).transferBlock(block.getCurrentBlock(), blockToken,
          dfsClient.clientName, targets, targetStorageTypes,
          targetStorageIDs);
      out.flush();
      //ack
      BlockOpResponseProto transferResponse = BlockOpResponseProto
          .parseFrom(PBHelperClient.vintPrefixed(in));
      if (SUCCESS != transferResponse.getStatus()) {
        throw new IOException("Failed to add a datanode. Response status: "
            + transferResponse.getStatus());
      }
    }

    @Override
    public void close() throws IOException {
      IOUtils.closeStream(in);
      IOUtils.closeStream(out);
      IOUtils.closeSocket(sock);
    }
  }

  static class BlockToWrite {
    private ExtendedBlock currentBlock;

    BlockToWrite(ExtendedBlock block) {
      setCurrentBlock(block);
    }

    synchronized ExtendedBlock getCurrentBlock() {
      return currentBlock == null ? null : new ExtendedBlock(currentBlock);
    }

    synchronized long getNumBytes() {
      return currentBlock == null ? 0 : currentBlock.getNumBytes();
    }

    synchronized void setCurrentBlock(ExtendedBlock block) {
      currentBlock = (block == null || block.getLocalBlock() == null) ?
          null : new ExtendedBlock(block);
    }

    synchronized void setNumBytes(long numBytes) {
      assert currentBlock != null;
      currentBlock.setNumBytes(numBytes);
    }

    synchronized void setGenerationStamp(long generationStamp) {
      assert currentBlock != null;
      currentBlock.setGenerationStamp(generationStamp);
    }

    @Override
    public synchronized String toString() {
      return currentBlock == null ? "null" : currentBlock.toString();
    }
  }

  /**
   * Create a socket for a write pipeline
   *
   * @param first the first datanode
   * @param length the pipeline length
   * @param client client
   * @return the socket connected to the first datanode
   */
  static Socket createSocketForPipeline(final DatanodeInfo first,
      final int length, final DFSClient client) throws IOException {
    final DfsClientConf conf = client.getConf();
    final String dnAddr = first.getXferAddr(conf.isConnectToDnViaHostname());
    LOG.debug("Connecting to datanode {}", dnAddr);
    final InetSocketAddress isa = NetUtils.createSocketAddr(dnAddr);
    final Socket sock = client.socketFactory.createSocket();
    final int timeout = client.getDatanodeReadTimeout(length);
    NetUtils.connect(sock, isa, client.getRandomLocalInterfaceAddr(),
        conf.getSocketTimeout());
    sock.setTcpNoDelay(conf.getDataTransferTcpNoDelay());
    sock.setSoTimeout(timeout);
    sock.setKeepAlive(true);
    if (conf.getSocketSendBufferSize() > 0) {
      sock.setSendBufferSize(conf.getSocketSendBufferSize());
    }
    LOG.debug("Send buf size {}", sock.getSendBufferSize());
    return sock;
  }

  /**
   * if this file is lazy persist
   *
   * @param stat the HdfsFileStatus of a file
   * @return if this file is lazy persist
   */
  static boolean isLazyPersist(HdfsFileStatus stat) {
    return stat.getStoragePolicy() == HdfsConstants.MEMORY_STORAGE_POLICY_ID;
  }

  /**
   * release a list of packets to ByteArrayManager
   *
   * @param packets packets to be release
   * @param bam ByteArrayManager
   */
  private static void releaseBuffer(List<DFSPacket> packets, ByteArrayManager bam) {
    for(DFSPacket p : packets) {
      p.releaseBuffer(bam);
    }
    packets.clear();
  }

  class LastExceptionInStreamer extends ExceptionLastSeen {
    /**
     * Check if there already is an exception.
     */
    @Override
    synchronized void check(boolean resetToNull) throws IOException {
      final IOException thrown = get();
      if (thrown != null) {
        if (LOG.isTraceEnabled()) {
          // wrap and print the exception to know when the check is called
          LOG.trace("Got Exception while checking, " + DataStreamer.this,
              new Throwable(thrown));
        }
        super.check(resetToNull);
      }
    }
  }

  enum ErrorType {
    NONE, INTERNAL, EXTERNAL
  }

  static class ErrorState {
    ErrorType error = ErrorType.NONE;
    private int badNodeIndex = -1;
    private boolean waitForRestart = true;
    private int restartingNodeIndex = -1;
    private long restartingNodeDeadline = 0;
    private final long datanodeRestartTimeout;

    ErrorState(long datanodeRestartTimeout) {
      this.datanodeRestartTimeout = datanodeRestartTimeout;
    }

    synchronized void resetInternalError() {
      if (hasInternalError()) {
        error = ErrorType.NONE;
      }
      badNodeIndex = -1;
      restartingNodeIndex = -1;
      restartingNodeDeadline = 0;
      waitForRestart = true;
    }

    synchronized void reset() {
      error = ErrorType.NONE;
      badNodeIndex = -1;
      restartingNodeIndex = -1;
      restartingNodeDeadline = 0;
      waitForRestart = true;
    }

    synchronized boolean hasInternalError() {
      return error == ErrorType.INTERNAL;
    }

    synchronized boolean hasExternalError() {
      return error == ErrorType.EXTERNAL;
    }

    synchronized boolean hasError() {
      return error != ErrorType.NONE;
    }

    synchronized boolean hasDatanodeError() {
      return error == ErrorType.INTERNAL && isNodeMarked();
    }

    synchronized void setInternalError() {
      this.error = ErrorType.INTERNAL;
    }

    synchronized void setExternalError() {
      if (!hasInternalError()) {
        this.error = ErrorType.EXTERNAL;
      }
    }

    synchronized void setBadNodeIndex(int index) {
      this.badNodeIndex = index;
    }

    synchronized int getBadNodeIndex() {
      return badNodeIndex;
    }

    synchronized int getRestartingNodeIndex() {
      return restartingNodeIndex;
    }

    synchronized void initRestartingNode(int i, String message,
        boolean shouldWait) {
      restartingNodeIndex = i;
      if (shouldWait) {
        restartingNodeDeadline = Time.monotonicNow() + datanodeRestartTimeout;
        // If the data streamer has already set the primary node
        // bad, clear it. It is likely that the write failed due to
        // the DN shutdown. Even if it was a real failure, the pipeline
        // recovery will take care of it.
        badNodeIndex = -1;
      } else {
        this.waitForRestart = false;
      }
      LOG.info(message);
    }

    synchronized boolean isRestartingNode() {
      return restartingNodeIndex >= 0;
    }

    synchronized boolean isNodeMarked() {
      return badNodeIndex >= 0 || (isRestartingNode() && doWaitForRestart());
    }

    /**
     * This method is used when no explicit error report was received, but
     * something failed. The first node is a suspect or unsure about the cause
     * so that it is marked as failed.
     */
    synchronized void markFirstNodeIfNotMarked() {
      // There should be no existing error and no ongoing restart.
      if (!isNodeMarked()) {
        badNodeIndex = 0;
      }
    }

    synchronized void adjustState4RestartingNode() {
      // Just took care of a node error while waiting for a node restart
      if (restartingNodeIndex >= 0) {
        // If the error came from a node further away than the restarting
        // node, the restart must have been complete.
        if (badNodeIndex > restartingNodeIndex) {
          restartingNodeIndex = -1;
        } else if (badNodeIndex < restartingNodeIndex) {
          // the node index has shifted.
          restartingNodeIndex--;
        } else if (waitForRestart) {
          throw new IllegalStateException("badNodeIndex = " + badNodeIndex
              + " = restartingNodeIndex = " + restartingNodeIndex);
        }
      }

      if (!isRestartingNode()) {
        error = ErrorType.NONE;
      }
      badNodeIndex = -1;
    }

    synchronized void checkRestartingNodeDeadline(DatanodeInfo[] nodes) {
      if (restartingNodeIndex >= 0) {
        if (error == ErrorType.NONE) {
          throw new IllegalStateException("error=false while checking" +
              " restarting node deadline");
        }

        // check badNodeIndex
        if (badNodeIndex == restartingNodeIndex) {
          // ignore, if came from the restarting node
          badNodeIndex = -1;
        }
        // not within the deadline
        if (Time.monotonicNow() >= restartingNodeDeadline) {
          // expired. declare the restarting node dead
          restartingNodeDeadline = 0;
          final int i = restartingNodeIndex;
          restartingNodeIndex = -1;
          LOG.warn("Datanode " + i + " did not restart within "
              + datanodeRestartTimeout + "ms: " + nodes[i]);
          // Mark the restarting node as failed. If there is any other failed
          // node during the last pipeline construction attempt, it will not be
          // overwritten/dropped. In this case, the restarting node will get
          // excluded in the following attempt, if it still does not come up.
          if (badNodeIndex == -1) {
            badNodeIndex = i;
          }
        }
      }
    }

    boolean doWaitForRestart() {
      return waitForRestart;
    }
  }

  private volatile boolean streamerClosed = false;
  protected final BlockToWrite block; // its length is number of bytes acked
  protected Token<BlockTokenIdentifier> accessToken;
  private DataOutputStream blockStream;
  private DataInputStream blockReplyStream;
  private ResponseProcessor response = null;
  private final Object nodesLock = new Object();
  private volatile DatanodeInfo[] nodes = null; // list of targets for current block
  private volatile StorageType[] storageTypes = null;
  private volatile String[] storageIDs = null;
  private final ErrorState errorState;

  private volatile BlockConstructionStage stage;  // block construction stage
  protected long bytesSent = 0; // number of bytes that've been sent
  private final boolean isLazyPersistFile;
  private long lastPacket;

  /** Nodes have been used in the pipeline before and have failed. */
  private final List<DatanodeInfo> failed = new ArrayList<>();
  /** Restarting Nodes */
  private List<DatanodeInfo> restartingNodes = new ArrayList<>();
  /** The times have retried to recover pipeline, for the same packet. */
  private volatile int pipelineRecoveryCount = 0;
  /** Has the current block been hflushed? */
  private boolean isHflushed = false;
  /** Append on an existing block? */
  private final boolean isAppend;

  private long currentSeqno = 0;
  private long lastQueuedSeqno = -1;
  private long lastAckedSeqno = -1;
  private long bytesCurBlock = 0; // bytes written in current block
  private final LastExceptionInStreamer lastException = new LastExceptionInStreamer();
  private Socket s;

  protected final DFSClient dfsClient;
  protected final String src;
  /** Only for DataTransferProtocol.writeBlock(..) */
  final DataChecksum checksum4WriteBlock;
  final Progressable progress;
  protected final HdfsFileStatus stat;
  // appending to existing partial block
  private volatile boolean appendChunk = false;
  // both dataQueue and ackQueue are protected by dataQueue lock
  protected final LinkedList<DFSPacket> dataQueue = new LinkedList<>();
  private final Map<Long, Long> packetSendTime = new HashMap<>();
  private final LinkedList<DFSPacket> ackQueue = new LinkedList<>();
  private final AtomicReference<CachingStrategy> cachingStrategy;
  private final ByteArrayManager byteArrayManager;
  //persist blocks on namenode
  private final AtomicBoolean persistBlocks = new AtomicBoolean(false);
  private boolean failPacket = false;
  private final long dfsclientSlowLogThresholdMs;
  private long artificialSlowdown = 0;
  // List of congested data nodes. The stream will back off if the DataNodes
  // are congested
  private final List<DatanodeInfo> congestedNodes = new ArrayList<>();
  private final Map<DatanodeInfo, Integer> slowNodeMap = new HashMap<>();
  private int congestionBackOffMeanTimeInMs;
  private int congestionBackOffMaxTimeInMs;
  private int lastCongestionBackoffTime;
  private int maxPipelineRecoveryRetries;
  private int markSlowNodeAsBadNodeThreshold;

  protected final LoadingCache<DatanodeInfo, DatanodeInfo> excludedNodes;
  private final String[] favoredNodes;
  private final EnumSet<AddBlockFlag> addBlockFlags;

  private volatile boolean needEndBlockInAdvance = false;
  private volatile boolean endBlockFlag = false;
  private volatile boolean skipHandleSlowNode = false;

  private DataStreamer(HdfsFileStatus stat, ExtendedBlock block,
                       DFSClient dfsClient, String src,
                       Progressable progress, DataChecksum checksum,
                       AtomicReference<CachingStrategy> cachingStrategy,
                       ByteArrayManager byteArrayManage,
                       boolean isAppend, String[] favoredNodes,
                       EnumSet<AddBlockFlag> flags) {
    this.block = new BlockToWrite(block);
    this.dfsClient = dfsClient;
    this.src = src;
    this.progress = progress;
    this.stat = stat;
    this.checksum4WriteBlock = checksum;
    this.cachingStrategy = cachingStrategy;
    this.byteArrayManager = byteArrayManage;
    this.isLazyPersistFile = isLazyPersist(stat);
    this.isAppend = isAppend;
    this.favoredNodes = favoredNodes;
    final DfsClientConf conf = dfsClient.getConf();
    this.dfsclientSlowLogThresholdMs = conf.getSlowIoWarningThresholdMs();
    this.excludedNodes = initExcludedNodes(conf.getExcludedNodesCacheExpiry());
    this.errorState = new ErrorState(conf.getDatanodeRestartTimeout());
    this.addBlockFlags = flags;
    this.maxPipelineRecoveryRetries = conf.getMaxPipelineRecoveryRetries();
    this.markSlowNodeAsBadNodeThreshold = conf.getMarkSlowNodeAsBadNodeThreshold();
    congestionBackOffMeanTimeInMs = dfsClient.getConfiguration().getInt(
        HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MEAN_TIME,
        HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MEAN_TIME_DEFAULT);
    congestionBackOffMaxTimeInMs = dfsClient.getConfiguration().getInt(
        HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MAX_TIME,
        HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MAX_TIME_DEFAULT);
    if (congestionBackOffMeanTimeInMs <= 0) {
      LOG.warn("Configuration: {} is not appropriate, using default value: {}",
          HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MEAN_TIME,
          HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MEAN_TIME_DEFAULT);
    }
    if (congestionBackOffMaxTimeInMs <= 0) {
      LOG.warn("Configuration: {} is not appropriate, using default value: {}",
          HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MAX_TIME,
          HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MAX_TIME_DEFAULT);
    }
    if (congestionBackOffMaxTimeInMs < congestionBackOffMeanTimeInMs) {
      LOG.warn("Configuration: {} can not less than {}, using their default values.",
          HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MAX_TIME,
          HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MEAN_TIME);
    }
    if (congestionBackOffMeanTimeInMs <= 0 || congestionBackOffMaxTimeInMs <= 0 ||
        congestionBackOffMaxTimeInMs < congestionBackOffMeanTimeInMs) {
      congestionBackOffMeanTimeInMs =
          HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MEAN_TIME_DEFAULT;
      congestionBackOffMaxTimeInMs =
          HdfsClientConfigKeys.DFS_CLIENT_CONGESTION_BACKOFF_MAX_TIME_DEFAULT;
    }

  }

  /**
   * construction with tracing info
   */
  DataStreamer(HdfsFileStatus stat, ExtendedBlock block, DFSClient dfsClient,
               String src, Progressable progress, DataChecksum checksum,
               AtomicReference<CachingStrategy> cachingStrategy,
               ByteArrayManager byteArrayManage, String[] favoredNodes,
               EnumSet<AddBlockFlag> flags) {
    this(stat, block, dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManage, false, favoredNodes, flags);
    stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
  }

  /**
   * Construct a data streamer for appending to the last partial block
   * @param lastBlock last block of the file to be appended
   * @param stat status of the file to be appended
   */
  DataStreamer(LocatedBlock lastBlock, HdfsFileStatus stat, DFSClient dfsClient,
               String src, Progressable progress, DataChecksum checksum,
               AtomicReference<CachingStrategy> cachingStrategy,
               ByteArrayManager byteArrayManage) {
    this(stat, lastBlock.getBlock(), dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManage, true, null, null);
    stage = BlockConstructionStage.PIPELINE_SETUP_APPEND;
    bytesSent = block.getNumBytes();
    accessToken = lastBlock.getBlockToken();
  }

  /**
   * Set pipeline in construction
   *
   * @param lastBlock the last block of a file
   * @throws IOException
   */
  void setPipelineInConstruction(LocatedBlock lastBlock) throws IOException{
    // setup pipeline to append to the last block XXX retries??
    setPipeline(lastBlock);
    if (nodes.length < 1) {
      throw new IOException("Unable to retrieve blocks locations " +
          " for last block " + block + " of file " + src);
    }
  }

  void setAccessToken(Token<BlockTokenIdentifier> t) {
    this.accessToken = t;
  }

  protected void setPipeline(LocatedBlock lb) {
    setPipeline(lb.getLocations(), lb.getStorageTypes(), lb.getStorageIDs());
  }

  protected void setPipeline(DatanodeInfo[] newNodes, StorageType[] newStorageTypes,
                           String[] newStorageIDs) {
    synchronized (nodesLock) {
      this.nodes = newNodes;
    }
    this.storageTypes = newStorageTypes;
    this.storageIDs = newStorageIDs;
  }

  /**
   * Initialize for data streaming
   */
  private void initDataStreaming() {
    this.setName("DataStreamer for file " + src +
        " block " + block);
    if (LOG.isDebugEnabled()) {
      LOG.debug("nodes {} storageTypes {} storageIDs {}",
          Arrays.toString(nodes),
          Arrays.toString(storageTypes),
          Arrays.toString(storageIDs));
    }
    response = new ResponseProcessor(nodes);
    response.start();
    stage = BlockConstructionStage.DATA_STREAMING;
    lastPacket = Time.monotonicNow();
  }

  protected void endBlock() {
    LOG.debug("Closing old block {}", block);
    this.setName("DataStreamer for file " + src);
    closeResponder();
    closeStream();
    skipHandleSlowNode = false;
    setPipeline(null, null, null);
    stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
  }

  private boolean shouldStop() {
    return streamerClosed || errorState.hasError() || !dfsClient.clientRunning;
  }

  /*
   * streamer thread is the only thread that opens streams to datanode,
   * and closes them. Any error recovery is also done by this thread.
   */
  @Override
  public void run() {
    TraceScope scope = null;
    while (!streamerClosed && dfsClient.clientRunning) {
      // if the Responder encountered an error, shutdown Responder
      if (!needEndBlockInAdvance && errorState.hasError()) {
        closeResponder();
      }

      DFSPacket one;
      try {
        // process datanode IO errors if any
        boolean doSleep = processDatanodeOrExternalError();

        synchronized (dataQueue) {
          // wait for a packet to be sent.
          while ((!shouldStop() && dataQueue.isEmpty()) || doSleep) {
            long timeout = 1000;
            if (stage == BlockConstructionStage.DATA_STREAMING) {
              timeout = sendHeartbeat();
            }
            try {
              dataQueue.wait(timeout);
            } catch (InterruptedException  e) {
              LOG.debug("Thread interrupted", e);
            }
            doSleep = false;
          }
          if (shouldStop()) {
            continue;
          }
          // get packet to be sent.
          one = dataQueue.getFirst(); // regular data packet
          SpanContext[] parents = one.getTraceParents();
          if (parents != null && parents.length > 0) {
            // The original code stored multiple parents in the DFSPacket, and
            // use them ALL here when creating a new Span. We only use the
            // last one FOR NOW. Moreover, we don't activate the Span for now.
            scope = dfsClient.getTracer().
                newScope("dataStreamer", parents[0], false);
            //scope.getSpan().setParents(parents);
          }
        }

        // The DataStreamer has to release the dataQueue before sleeping,
        // otherwise it will cause the ResponseProcessor to accept the ACK delay.
        try {
          backOffIfNecessary();
        } catch (InterruptedException e) {
          LOG.debug("Thread interrupted", e);
        }

        // get new block from namenode.
        LOG.debug("stage={}, {}", stage, this);

        if (stage == BlockConstructionStage.PIPELINE_SETUP_CREATE) {
          LOG.debug("Allocating new block: {}", this);
          setupPipelineForCreate();
          initDataStreaming();
        } else if (stage == BlockConstructionStage.PIPELINE_SETUP_APPEND) {
          LOG.debug("Append to block {}", block);
          setupPipelineForAppendOrRecovery();
          if (streamerClosed) {
            continue;
          }
          initDataStreaming();
        }

        long lastByteOffsetInBlock = one.getLastByteOffsetBlock();
        if (lastByteOffsetInBlock > stat.getBlockSize()) {
          throw new IOException("BlockSize " + stat.getBlockSize() +
              " < lastByteOffsetInBlock, " + this + ", " + one);
        }

        if (one.isLastPacketInBlock()) {
          // wait for all data packets have been successfully acked
          waitForAllAcks();
          if(shouldStop()) {
            continue;
          }
          stage = BlockConstructionStage.PIPELINE_CLOSE;
          // shouldAdjustOffsetInBlock = false;
        }

        // send the packet
        SpanContext spanContext = null;
        synchronized (dataQueue) {
          // move packet from dataQueue to ackQueue
          if (!one.isHeartbeatPacket()) {
            if (scope != null) {
              one.setSpan(scope.span());
              spanContext = scope.span().getContext();
              scope.close();
            }
            scope = null;
            dataQueue.removeFirst();
            ackQueue.addLast(one);
            packetSendTime.put(one.getSeqno(), Time.monotonicNowNanos());
            dataQueue.notifyAll();
          }
        }

        LOG.debug("{} sending {}", this, one);

        // write out data to remote datanode
        try (TraceScope ignored = dfsClient.getTracer().
            newScope("DataStreamer#writeTo", spanContext)) {
          sendPacket(one);
        } catch (IOException e) {
          // HDFS-3398 treat primary DN is down since client is unable to
          // write to primary DN. If a failed or restarting node has already
          // been recorded by the responder, the following call will have no
          // effect. Pipeline recovery can handle only one node error at a
          // time. If the primary node fails again during the recovery, it
          // will be taken out then.
          errorState.markFirstNodeIfNotMarked();
          throw e;
        }

        // update bytesSent
        long tmpBytesSent = one.getLastByteOffsetBlock();
        if (bytesSent < tmpBytesSent) {
          bytesSent = tmpBytesSent;
        }

        if (shouldStop()) {
          continue;
        }

        // Is this block full?
        if (one.isLastPacketInBlock()) {
          // wait for the close packet has been acked
          try {
            waitForAllAcks();
            // TODO ZHB move to finally
          } catch (IOException ioe) {
            // No need to do a close recovery if the last packet was acked.
            // i.e. ackQueue is empty.  waitForAllAcks() can get an exception
            // (e.g. connection reset) while sending a heartbeat packet,
            // if the DN sends the final ack and closes the connection.
            synchronized (dataQueue) {
              if (!ackQueue.isEmpty()) {
                throw ioe;
              }
            }
          }
          if (shouldStop()) {
            continue;
          }

          endBlock();
        }
        if (progress != null) { progress.progress(); }

        // This is used by unit test to trigger race conditions.
        if (artificialSlowdown != 0 && dfsClient.clientRunning) {
          Thread.sleep(artificialSlowdown);
        }
      } catch (Throwable e) {
        // Log warning if there was a real error.
        if (!errorState.isRestartingNode()) {
          // Since their messages are descriptive enough, do not always
          // log a verbose stack-trace WARN for quota exceptions.
          if (e instanceof QuotaExceededException) {
            LOG.debug("DataStreamer Quota Exception", e);
          } else {
            LOG.warn("DataStreamer Exception", e);
          }
        }
        lastException.set(e);
        assert !(e instanceof NullPointerException);
        errorState.setInternalError();
        if (!errorState.isNodeMarked()) {
          // Not a datanode issue
          streamerClosed = true;
        }
      } finally {
        if (scope != null) {
          scope.close();
          scope = null;
        }
      }
    }
    closeInternal();
  }

  private void waitForAllAcks() throws IOException {
    // wait until all data packets have been successfully acked
    synchronized (dataQueue) {
      while (!shouldStop() && !ackQueue.isEmpty()) {
        try {
          // wait for acks to arrive from datanodes
          dataQueue.wait(sendHeartbeat());
        } catch (InterruptedException  e) {
          LOG.debug("Thread interrupted ", e);
        }
      }
    }
  }

  private void sendPacket(DFSPacket packet) throws IOException {
    // write out data to remote datanode
    try {
      packet.writeTo(blockStream);
      blockStream.flush();
    } catch (IOException e) {
      // HDFS-3398 treat primary DN is down since client is unable to
      // write to primary DN. If a failed or restarting node has already
      // been recorded by the responder, the following call will have no
      // effect. Pipeline recovery can handle only one node error at a
      // time. If the primary node fails again during the recovery, it
      // will be taken out then.
      errorState.markFirstNodeIfNotMarked();
      throw e;
    }
    lastPacket = Time.monotonicNow();
  }

  private long sendHeartbeat() throws IOException {
    final long heartbeatInterval = dfsClient.getConf().getSocketTimeout()/2;
    long timeout = heartbeatInterval - (Time.monotonicNow() - lastPacket);
    if (timeout <= 0) {
      sendPacket(createHeartbeatPacket());
      timeout = heartbeatInterval;
    }
    return timeout;
  }

  private void closeInternal() {
    closeResponder();       // close and join
    closeStream();
    streamerClosed = true;
    release();
    synchronized (dataQueue) {
      dataQueue.notifyAll();
    }
  }

  /**
   * release the DFSPackets in the two queues
   *
   */
  void release() {
    synchronized (dataQueue) {
      releaseBuffer(dataQueue, byteArrayManager);
      releaseBuffer(ackQueue, byteArrayManager);
    }
  }

  /**
   * wait for the ack of seqno
   *
   * @param seqno the sequence number to be acked
   * @throws IOException
   */
  void waitForAckedSeqno(long seqno) throws IOException {
    try (TraceScope ignored = dfsClient.getTracer().
        newScope("waitForAckedSeqno")) {
      LOG.debug("{} waiting for ack for: {}", this, seqno);
      int dnodes;
      synchronized (nodesLock) {
        dnodes = nodes != null ? nodes.length : 3;
      }
      int writeTimeout = dfsClient.getDatanodeWriteTimeout(dnodes);
      long begin = Time.monotonicNowNanos();
      try {
        synchronized (dataQueue) {
          while (!streamerClosed) {
            checkClosed();
            if (lastAckedSeqno >= seqno) {
              break;
            }
            try {
              dataQueue.wait(1000); // when we receive an ack, we notify on
              long duration = Time.monotonicNowNanos() - begin;
              if (TimeUnit.NANOSECONDS.toMillis(duration) > writeTimeout) {
                LOG.error("No ack received, took {}ms (threshold={}ms). "
                    + "File being written: {}, block: {}, "
                    + "Write pipeline datanodes: {}.",
                    TimeUnit.NANOSECONDS.toMillis(duration), writeTimeout, src, block, nodes);
                throw new InterruptedIOException("No ack received after " +
                    TimeUnit.NANOSECONDS.toSeconds(duration) + "s and a timeout of " +
                    writeTimeout / 1000 + "s");
              }
              // dataQueue
            } catch (InterruptedException ie) {
              throw new InterruptedIOException(
                  "Interrupted while waiting for data to be acknowledged by pipeline");
            }
          }
        }
        checkClosed();
      } catch (ClosedChannelException cce) {
        LOG.debug("Closed channel exception", cce);
      }
      long duration = Time.monotonicNowNanos() - begin;
      if (TimeUnit.NANOSECONDS.toMillis(duration) > dfsclientSlowLogThresholdMs) {
        LOG.warn("Slow waitForAckedSeqno took {}ms (threshold={}ms). File being"
                + " written: {}, block: {}, Write pipeline datanodes: {}.",
            TimeUnit.NANOSECONDS.toMillis(duration), dfsclientSlowLogThresholdMs,
            src, block, nodes);
      }
    }
  }

  /**
   * wait for space of dataQueue and queue the packet
   *
   * @param packet  the DFSPacket to be queued
   * @throws IOException
   */
  void waitAndQueuePacket(DFSPacket packet) throws IOException {
    synchronized (dataQueue) {
      try {
        // If queue is full, then wait till we have enough space
        boolean firstWait = true;
        try {
          while (!streamerClosed && dataQueue.size() + ackQueue.size() >
              dfsClient.getConf().getWriteMaxPackets()) {
            if (firstWait) {
              Span span = Tracer.getCurrentSpan();
              if (span != null) {
                span.addTimelineAnnotation("dataQueue.wait");
              }
              firstWait = false;
            }
            try {
              dataQueue.wait();
            } catch (InterruptedException e) {
              // If we get interrupted while waiting to queue data, we still need to get rid
              // of the current packet. This is because we have an invariant that if
              // currentPacket gets full, it will get queued before the next writeChunk.
              //
              // Rather than wait around for space in the queue, we should instead try to
              // return to the caller as soon as possible, even though we slightly overrun
              // the MAX_PACKETS length.
              Thread.currentThread().interrupt();
              break;
            }
          }
        } finally {
          Span span = Tracer.getCurrentSpan();
          if ((span != null) && (!firstWait)) {
            span.addTimelineAnnotation("end.wait");
          }
        }
        checkClosed();
        queuePacket(packet);
      } catch (ClosedChannelException cce) {
        LOG.debug("Closed channel exception", cce);
      }
    }
  }

  /*
   * close the streamer, should be called only by an external thread
   * and only after all data to be sent has been flushed to datanode.
   *
   * Interrupt this data streamer if force is true
   *
   * @param force if this data stream is forced to be closed
   */
  void close(boolean force) {
    streamerClosed = true;
    synchronized (dataQueue) {
      dataQueue.notifyAll();
    }
    if (force) {
      this.interrupt();
    }
  }

  void setStreamerAsClosed() {
    streamerClosed = true;
  }

  private void checkClosed() throws IOException {
    if (streamerClosed) {
      lastException.throwException4Close();
    }
  }

  private void closeResponder() {
    if (response != null) {
      try {
        response.close();
        response.join();
      } catch (InterruptedException  e) {
        LOG.debug("Thread interrupted", e);
        Thread.currentThread().interrupt();
      } finally {
        response = null;
      }
    }
  }

  void closeStream() {
    final MultipleIOException.Builder b = new MultipleIOException.Builder();

    if (blockStream != null) {
      try {
        blockStream.close();
      } catch (IOException e) {
        b.add(e);
      } finally {
        blockStream = null;
      }
    }
    if (blockReplyStream != null) {
      try {
        blockReplyStream.close();
      } catch (IOException e) {
        b.add(e);
      } finally {
        blockReplyStream = null;
      }
    }
    if (null != s) {
      try {
        s.close();
      } catch (IOException e) {
        b.add(e);
      } finally {
        s = null;
      }
    }

    final IOException ioe = b.build();
    if (ioe != null) {
      lastException.set(ioe);
    }
  }

  /**
   * Examine whether it is worth waiting for a node to restart.
   * @param index the node index
   */
  boolean shouldWaitForRestart(int index) {
    // Only one node in the pipeline.
    if (nodes.length == 1) {
      return true;
    }

    /*
     * Treat all nodes as remote for test when skip enabled.
     */
    if (DFSClientFaultInjector.get().skipRollingRestartWait()) {
      return false;
    }

    // Is it a local node?
    InetAddress addr = null;
    try {
      addr = InetAddress.getByName(nodes[index].getIpAddr());
    } catch (java.net.UnknownHostException e) {
      // we are passing an ip address. this should not happen.
      assert false;
    }

    return addr != null && NetUtils.isLocalAddress(addr);
  }

  //
  // Processes responses from the datanodes.  A packet is removed
  // from the ackQueue when its response arrives.
  //
  private class ResponseProcessor extends Daemon {

    private volatile boolean responderClosed = false;
    private DatanodeInfo[] targets = null;
    private boolean isLastPacketInBlock = false;

    ResponseProcessor (DatanodeInfo[] targets) {
      this.targets = targets;
    }

    @Override
    public void run() {

      setName("ResponseProcessor for block " + block);
      PipelineAck ack = new PipelineAck();

      TraceScope scope = null;
      while (!responderClosed && dfsClient.clientRunning && !isLastPacketInBlock) {
        // process responses from datanodes.
        try {
          // read an ack from the pipeline
          ack.readFields(blockReplyStream);
          if (ack.getSeqno() != DFSPacket.HEART_BEAT_SEQNO) {
            Long begin = packetSendTime.get(ack.getSeqno());
            if (begin != null) {
              long duration = Time.monotonicNowNanos() - begin;
              if (TimeUnit.NANOSECONDS.toMillis(duration) > dfsclientSlowLogThresholdMs) {
                LOG.info("Slow ReadProcessor read fields for block " + block
                    + " took " + TimeUnit.NANOSECONDS.toMillis(duration) + "ms (threshold="
                    + dfsclientSlowLogThresholdMs + "ms); ack: " + ack
                    + ", targets: " + Arrays.asList(targets));
              }
            }
          }

          LOG.debug("DFSClient {}", ack);

          long seqno = ack.getSeqno();
          // processes response status from datanodes.
          ArrayList<DatanodeInfo> congestedNodesFromAck = new ArrayList<>();
          ArrayList<DatanodeInfo> slownodesFromAck = new ArrayList<>();
          for (int i = ack.getNumOfReplies()-1; i >=0  && dfsClient.clientRunning; i--) {
            final Status reply = PipelineAck.getStatusFromHeader(ack
                .getHeaderFlag(i));
            if (PipelineAck.getECNFromHeader(ack.getHeaderFlag(i)) ==
                PipelineAck.ECN.CONGESTED) {
              congestedNodesFromAck.add(targets[i]);
            }
            if (PipelineAck.getSLOWFromHeader(ack.getHeaderFlag(i)) ==
                PipelineAck.SLOW.SLOW) {
              slownodesFromAck.add(targets[i]);
            }
            // Restart will not be treated differently unless it is
            // the local node or the only one in the pipeline.
            if (PipelineAck.isRestartOOBStatus(reply)) {
              final String message = "Datanode " + i + " is restarting: "
                  + targets[i];
              errorState.initRestartingNode(i, message,
                  shouldWaitForRestart(i));
              throw new IOException(message);
            }
            // node error
            if (reply != SUCCESS) {
              errorState.setBadNodeIndex(i); // mark bad datanode
              throw new IOException("Bad response " + reply +
                  " for " + block + " from datanode " + targets[i]);
            }
          }

          if (!congestedNodesFromAck.isEmpty()) {
            synchronized (congestedNodes) {
              congestedNodes.clear();
              congestedNodes.addAll(congestedNodesFromAck);
            }
          } else {
            synchronized (congestedNodes) {
              congestedNodes.clear();
              lastCongestionBackoffTime = 0;
            }
          }
          if (slownodesFromAck.isEmpty()) {
            if (!slowNodeMap.isEmpty()) {
              slowNodeMap.clear();
            }
          } else {
            markSlowNode(slownodesFromAck);
          }

          assert seqno != PipelineAck.UNKOWN_SEQNO :
              "Ack for unknown seqno should be a failed ack: " + ack;
          if (seqno == DFSPacket.HEART_BEAT_SEQNO) {  // a heartbeat ack
            continue;
          }

          // a success ack for a data packet
          DFSPacket one;
          synchronized (dataQueue) {
            one = ackQueue.getFirst();
          }
          if (one.getSeqno() != seqno) {
            throw new IOException("ResponseProcessor: Expecting seqno " +
                one.getSeqno() + " for block " + block +
                " but received " + seqno);
          }
          isLastPacketInBlock = one.isLastPacketInBlock();

          // Fail the packet write for testing in order to force a
          // pipeline recovery.
          if (DFSClientFaultInjector.get().failPacket() &&
              isLastPacketInBlock) {
            failPacket = true;
            throw new IOException(
                "Failing the last packet for testing.");
          }

          // update bytesAcked
          block.setNumBytes(one.getLastByteOffsetBlock());

          synchronized (dataQueue) {
            if (one.getSpan() != null) {
              scope = new TraceScope(new Span());
              // TODO: Use scope = Tracer.curThreadTracer().activateSpan ?
              one.setSpan(null);
            }
            lastAckedSeqno = seqno;
            pipelineRecoveryCount = 0;
            ackQueue.removeFirst();
            packetSendTime.remove(seqno);
            dataQueue.notifyAll();

            one.releaseBuffer(byteArrayManager);
          }
        } catch (Throwable e) {
          if (!responderClosed) {
            lastException.set(e);
            errorState.setInternalError();
            errorState.markFirstNodeIfNotMarked();
            synchronized (dataQueue) {
              dataQueue.notifyAll();
            }
            if (!errorState.isRestartingNode()) {
              LOG.warn("Exception for " + block, e);
            }
            responderClosed = true;
          }
        } finally {
          if (scope != null) {
            scope.close();
          }
          scope = null;
        }
      }
    }

    void markSlowNode(List<DatanodeInfo> slownodesFromAck) throws IOException {
      Set<DatanodeInfo> discontinuousNodes = new HashSet<>(slowNodeMap.keySet());
      for (DatanodeInfo slowNode : slownodesFromAck) {
        if (!slowNodeMap.containsKey(slowNode)) {
          slowNodeMap.put(slowNode, 1);
        } else {
          int oldCount = slowNodeMap.get(slowNode);
          slowNodeMap.put(slowNode, ++oldCount);
        }
        discontinuousNodes.remove(slowNode);
      }
      for (DatanodeInfo discontinuousNode : discontinuousNodes) {
        slowNodeMap.remove(discontinuousNode);
      }

      if (!slowNodeMap.isEmpty()) {
        for (Map.Entry<DatanodeInfo, Integer> entry : slowNodeMap.entrySet()) {
          if (entry.getValue() >= markSlowNodeAsBadNodeThreshold) {
            DatanodeInfo slowNode = entry.getKey();
            int index = getDatanodeIndex(slowNode);
            if (index >= 0) {
              if (skipHandleSlowNode) {
                return;
              }
              String exceptionMsg = "Receive reply from slowNode " + slowNode +
                  " for continuous " + markSlowNodeAsBadNodeThreshold +
                  " times, treating it as badNode";
              if (!isEndBlockFlag() && getBytesCurBlock() << 1
                  > dfsClient.getConf().getDefaultBlockSize()) {
                LOG.warn(exceptionMsg);
                needEndBlockInAdvance = true;
                skipHandleSlowNode = true;
                errorState.setInternalError();
                errorState.setBadNodeIndex(index);
                return;
              } else {
                throw new IOException(exceptionMsg);
              }
            }
            slowNodeMap.remove(entry.getKey());
          }
        }
      }
    }

    void close() {
      responderClosed = true;
      this.interrupt();
    }

    int getDatanodeIndex(DatanodeInfo datanodeInfo) {
      for (int i = 0; i < targets.length; i++) {
        if (targets[i].equals(datanodeInfo)) {
          return i;
        }
      }
      return -1;
    }
  }

  private boolean shouldHandleExternalError(){
    return errorState.hasExternalError() && blockStream != null;
  }

  /**
   * If this stream has encountered any errors, shutdown threads
   * and mark the stream as closed.
   *
   * @return true if it should sleep for a while after returning.
   */
  private boolean processDatanodeOrExternalError() throws IOException {
    if (!errorState.hasDatanodeError() && !shouldHandleExternalError()) {
      return false;
    }

    if (needEndBlockInAdvance && !(this instanceof StripedDataStreamer)) {
      final int badNodeIndex = errorState.getBadNodeIndex();
      LOG.warn("End block {} in advance because written bytes are greater than half of block size" +
              " and {} is bad. in pipeline datanode: {}. Bad datanode index is {}",
          block, nodes[badNodeIndex], Arrays.toString(nodes), badNodeIndex);
      excludedNodes.put(nodes[badNodeIndex], nodes[badNodeIndex]);
      errorState.resetInternalError();
      lastException.clear();
      endBlockFlag = true;
      needEndBlockInAdvance = false;
      return false;
    }
    
    LOG.debug("start process datanode/external error, {}", this);
    if (response != null) {
      LOG.info("Error Recovery for " + block +
          " waiting for responder to exit. ");
      return true;
    }
    closeStream();

    // move packets from ack queue to front of the data queue
    synchronized (dataQueue) {
      dataQueue.addAll(0, ackQueue);
      ackQueue.clear();
      packetSendTime.clear();
    }

    // If we had to recover the pipeline more than the value
    // defined by maxPipelineRecoveryRetries in a row for the
    // same packet, this client likely has corrupt data or corrupting
    // during transmission.
    if (!errorState.isRestartingNode() && ++pipelineRecoveryCount >
        maxPipelineRecoveryRetries) {
      LOG.warn("Error recovering pipeline for writing " +
          block + ". Already retried " + maxPipelineRecoveryRetries
          + " times for the same packet.");
      lastException.set(new IOException("Failing write. Tried pipeline " +
          "recovery " + maxPipelineRecoveryRetries
          + " times without success."));
      streamerClosed = true;
      return false;
    }

    setupPipelineForAppendOrRecovery();

    if (!streamerClosed && dfsClient.clientRunning) {
      if (stage == BlockConstructionStage.PIPELINE_CLOSE) {

        // If we had an error while closing the pipeline, we go through a fast-path
        // where the BlockReceiver does not run. Instead, the DataNode just finalizes
        // the block immediately during the 'connect ack' process. So, we want to pull
        // the end-of-block packet from the dataQueue, since we don't actually have
        // a true pipeline to send it over.
        //
        // We also need to set lastAckedSeqno to the end-of-block Packet's seqno, so that
        // a client waiting on close() will be aware that the flush finished.
        synchronized (dataQueue) {
          DFSPacket endOfBlockPacket = dataQueue.remove();  // remove the end of block packet
          // Close any trace span associated with this Packet
          Span span = endOfBlockPacket.getSpan();
          if (span != null) {
            span.finish();
            endOfBlockPacket.setSpan(null);
          }
          assert endOfBlockPacket.isLastPacketInBlock();
          assert lastAckedSeqno == endOfBlockPacket.getSeqno() - 1;
          lastAckedSeqno = endOfBlockPacket.getSeqno();
          pipelineRecoveryCount = 0;
          dataQueue.notifyAll();
        }
        endBlock();
      } else {
        initDataStreaming();
      }
    }

    return false;
  }

  void setHflush() {
    isHflushed = true;
  }

  private int findNewDatanode(final DatanodeInfo[] original
  ) throws IOException {
    if (nodes.length != original.length + 1) {
      throw new IOException(
          "Failed to replace a bad datanode on the existing pipeline "
              + "due to no more good datanodes being available to try. "
              + "(Nodes: current=" + Arrays.asList(nodes)
              + ", original=" + Arrays.asList(original) + "). "
              + "The current failed datanode replacement policy is "
              + dfsClient.dtpReplaceDatanodeOnFailure
              + ", and a client may configure this via '"
              + BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY
              + "' in its configuration.");
    }
    for(int i = 0; i < nodes.length; i++) {
      int j = 0;
      for(; j < original.length && !nodes[i].equals(original[j]); j++);
      if (j == original.length) {
        return i;
      }
    }
    throw new IOException("Failed: new datanode not found: nodes="
        + Arrays.asList(nodes) + ", original=" + Arrays.asList(original));
  }

  private void addDatanode2ExistingPipeline() throws IOException {
    DataTransferProtocol.LOG.debug("lastAckedSeqno = {}", lastAckedSeqno);
      /*
       * Is data transfer necessary?  We have the following cases.
       *
       * Case 1: Failure in Pipeline Setup
       * - Append
       *    + Transfer the stored replica, which may be a RBW or a finalized.
       * - Create
       *    + If no data, then no transfer is required.
       *    + If there are data written, transfer RBW. This case may happens
       *      when there are streaming failure earlier in this pipeline.
       *
       * Case 2: Failure in Streaming
       * - Append/Create:
       *    + transfer RBW
       */
    if (!isAppend && lastAckedSeqno < 0
        && stage == BlockConstructionStage.PIPELINE_SETUP_CREATE) {
      //no data have been written
      return;
    }

    int tried = 0;
    final DatanodeInfo[] original = nodes;
    final StorageType[] originalTypes = storageTypes;
    final String[] originalIDs = storageIDs;
    IOException caughtException = null;
    ArrayList<DatanodeInfo> exclude = new ArrayList<>(failed);
    while (tried < 3) {
      LocatedBlock lb;
      //get a new datanode
      lb = dfsClient.namenode.getAdditionalDatanode(
          src, stat.getFileId(), block.getCurrentBlock(), nodes, storageIDs,
          exclude.toArray(new DatanodeInfo[exclude.size()]),
          1, dfsClient.clientName);
      // a new node was allocated by the namenode. Update nodes.
      setPipeline(lb);

      //find the new datanode
      final int d;
      try {
        d = findNewDatanode(original);
      } catch (IOException ioe) {
        // check the minimal number of nodes available to decide whether to
        // continue the write.

        //if live block location datanodes is greater than or equal to
        // HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.
        // MIN_REPLICATION threshold value, continue writing to the
        // remaining nodes. Otherwise throw exception.
        //
        // If HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.
        // MIN_REPLICATION is set to 0 or less than zero, an exception will be
        // thrown if a replacement could not be found.

        if (dfsClient.dtpReplaceDatanodeOnFailureReplication > 0 && nodes.length
            >= dfsClient.dtpReplaceDatanodeOnFailureReplication) {
          DFSClient.LOG.warn(
              "Failed to find a new datanode to add to the write pipeline,"
                  + " continue to write to the pipeline with " + nodes.length
                  + " nodes since it's no less than minimum replication: "
                  + dfsClient.dtpReplaceDatanodeOnFailureReplication
                  + " configured by "
                  + BlockWrite.ReplaceDatanodeOnFailure.MIN_REPLICATION
                  + ".", ioe);
          return;
        }
        throw ioe;
      }
      //transfer replica. pick a source from the original nodes
      final DatanodeInfo src = original[tried % original.length];
      final DatanodeInfo[] targets = {nodes[d]};
      final StorageType[] targetStorageTypes = {storageTypes[d]};
      final String[] targetStorageIDs = {storageIDs[d]};

      try {
        transfer(src, targets, targetStorageTypes, targetStorageIDs,
            lb.getBlockToken());
      } catch (IOException ioe) {
        DFSClient.LOG.warn("Error transferring data from " + src + " to " +
            nodes[d] + ": " + ioe.getMessage());
        caughtException = ioe;
        // add the allocated node to the exclude list.
        exclude.add(nodes[d]);
        setPipeline(original, originalTypes, originalIDs);
        tried++;
        continue;
      }
      return; // finished successfully
    }
    // All retries failed
    throw (caughtException != null) ? caughtException :
        new IOException("Failed to add a node");
  }

  private long computeTransferWriteTimeout() {
    return dfsClient.getDatanodeWriteTimeout(2);
  }
  private long computeTransferReadTimeout() {
    // transfer timeout multiplier based on the transfer size
    // One per 200 packets = 12.8MB. Minimum is 2.
    int multi = 2
        + (int) (bytesSent / dfsClient.getConf().getWritePacketSize()) / 200;
    return dfsClient.getDatanodeReadTimeout(multi);
  }

  private void transfer(final DatanodeInfo src, final DatanodeInfo[] targets,
                        final StorageType[] targetStorageTypes,
                        final String[] targetStorageIDs,
                        final Token<BlockTokenIdentifier> blockToken)
      throws IOException {
    //transfer replica to the new datanode
    RefetchEncryptionKeyPolicy policy = new RefetchEncryptionKeyPolicy(src);
    do {
      StreamerStreams streams = null;
      try {
        final long writeTimeout = computeTransferWriteTimeout();
        final long readTimeout = computeTransferReadTimeout();

        streams = new StreamerStreams(src, writeTimeout, readTimeout,
            blockToken);
        streams.sendTransferBlock(targets, targetStorageTypes,
            targetStorageIDs, blockToken);
        return;
      } catch (InvalidEncryptionKeyException e) {
        policy.recordFailure(e);
      } finally {
        IOUtils.closeStream(streams);
      }
    } while (policy.continueRetryingOrThrow());
  }

  /**
   * Open a DataStreamer to a DataNode pipeline so that
   * it can be written to.
   * This happens when a file is appended or data streaming fails
   * It keeps on trying until a pipeline is setup
   *
   * Returns boolean whether pipeline was setup successfully or not.
   * This boolean is used upstream on whether to continue creating pipeline or throw exception
   */
  private boolean setupPipelineForAppendOrRecovery() throws IOException {
    // Check number of datanodes. Note that if there is no healthy datanode,
    // this must be internal error because we mark external error in striped
    // outputstream only when all the streamers are in the DATA_STREAMING stage
    if (nodes == null || nodes.length == 0) {
      String msg = "Could not get block locations. " + "Source file \""
          + src + "\" - Aborting..." + this;
      LOG.warn(msg);
      lastException.set(new IOException(msg));
      streamerClosed = true;
      return false;
    }
    return setupPipelineInternal(nodes, storageTypes, storageIDs);
  }

  protected boolean setupPipelineInternal(DatanodeInfo[] datanodes,
      StorageType[] nodeStorageTypes, String[] nodeStorageIDs)
      throws IOException {
    boolean success = false;
    long newGS = 0L;
    boolean isCreateStage = BlockConstructionStage.PIPELINE_SETUP_CREATE == stage;
    while (!success && !streamerClosed && dfsClient.clientRunning) {
      if (!handleRestartingDatanode()) {
        return false;
      }

      final boolean isRecovery = errorState.hasInternalError() && !isCreateStage;


      if (!handleBadDatanode()) {
        return false;
      }

      handleDatanodeReplacement();

      // During create stage, min replication should still be satisfied.
      if (isCreateStage && !(dfsClient.dtpReplaceDatanodeOnFailureReplication > 0 &&
          nodes.length  >= dfsClient.dtpReplaceDatanodeOnFailureReplication)) {
        return false;
      }

      // get a new generation stamp and an access token
      final LocatedBlock lb = updateBlockForPipeline();
      newGS = lb.getBlock().getGenerationStamp();
      accessToken = lb.getBlockToken();

      if (isCreateStage) {
        block.setCurrentBlock(lb.getBlock());
      }

      // set up the pipeline again with the remaining nodes
      success = createBlockOutputStream(nodes, storageTypes, storageIDs, newGS,
          isRecovery);

      failPacket4Testing();

      errorState.checkRestartingNodeDeadline(nodes);
    } // while

    if (success) {
      updatePipeline(newGS);
    }
    return success;
  }

  /**
   * Sleep if a node is restarting.
   * This process is repeated until the deadline or the node starts back up.
   * @return true if it should continue.
   */
  boolean handleRestartingDatanode() {
    if (errorState.isRestartingNode()) {
      if (!errorState.doWaitForRestart()) {
        // If node is restarting and not worth to wait for restart then can go
        // ahead with error recovery considering it as bad node for now. Later
        // it should be able to re-consider the same node for future pipeline
        // updates.
        errorState.setBadNodeIndex(errorState.getRestartingNodeIndex());
        return true;
      }
      // 4 seconds or the configured deadline period, whichever is shorter.
      // This is the retry interval and recovery will be retried in this
      // interval until timeout or success.
      final long delay = Math.min(errorState.datanodeRestartTimeout, 4000L);
      try {
        Thread.sleep(delay);
      } catch (InterruptedException ie) {
        lastException.set(new IOException(
            "Interrupted while waiting for restarting "
            + nodes[errorState.getRestartingNodeIndex()]));
        streamerClosed = true;
        return false;
      }
    }
    return true;
  }

  /**
   * Remove bad node from list of nodes if badNodeIndex was set.
   * @return true if it should continue.
   */
  boolean handleBadDatanode() {
    final int badNodeIndex = errorState.getBadNodeIndex();
    if (badNodeIndex >= 0) {
      if (nodes.length <= 1) {
        lastException.set(new IOException("All datanodes "
            + Arrays.toString(nodes) + " are bad. Aborting..."));
        streamerClosed = true;
        return false;
      }

      String reason = "bad.";
      if (errorState.getRestartingNodeIndex() == badNodeIndex) {
        reason = "restarting.";
        restartingNodes.add(nodes[badNodeIndex]);
      }
      LOG.warn("Error Recovery for " + block + " in pipeline "
          + Arrays.toString(nodes) + ": datanode " + badNodeIndex
          + "("+ nodes[badNodeIndex] + ") is " + reason);
      failed.add(nodes[badNodeIndex]);

      DatanodeInfo[] newnodes = new DatanodeInfo[nodes.length-1];
      arraycopy(nodes, newnodes, badNodeIndex);

      final StorageType[] newStorageTypes = new StorageType[newnodes.length];
      arraycopy(storageTypes, newStorageTypes, badNodeIndex);

      final String[] newStorageIDs = new String[newnodes.length];
      arraycopy(storageIDs, newStorageIDs, badNodeIndex);

      setPipeline(newnodes, newStorageTypes, newStorageIDs);

      errorState.adjustState4RestartingNode();
      lastException.clear();
    }
    return true;
  }

  /** Add a datanode if replace-datanode policy is satisfied. */
  private void handleDatanodeReplacement() throws IOException {
    if (dfsClient.dtpReplaceDatanodeOnFailure.satisfy(stat.getReplication(),
        nodes, isAppend, isHflushed)) {
      try {
        addDatanode2ExistingPipeline();
      } catch(IOException ioe) {
        if (!dfsClient.dtpReplaceDatanodeOnFailure.isBestEffort()) {
          throw ioe;
        }
        LOG.warn("Failed to replace datanode."
            + " Continue with the remaining datanodes since "
            + BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY
            + " is set to true.", ioe);
      }
    }
  }

  void failPacket4Testing() {
    if (failPacket) { // for testing
      failPacket = false;
      try {
        // Give DNs time to send in bad reports. In real situations,
        // good reports should follow bad ones, if client committed
        // with those nodes.
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        LOG.debug("Thread interrupted", e);
      }
    }
  }

  private LocatedBlock updateBlockForPipeline() throws IOException {
    return dfsClient.namenode.updateBlockForPipeline(block.getCurrentBlock(),
        dfsClient.clientName);
  }

  void updateBlockGS(final long newGS) {
    block.setGenerationStamp(newGS);
  }

  /** update pipeline at the namenode */
  @VisibleForTesting
  public void updatePipeline(long newGS) throws IOException {
    final ExtendedBlock oldBlock = block.getCurrentBlock();
    // the new GS has been propagated to all DN, it should be ok to update the
    // local block state
    updateBlockGS(newGS);
    dfsClient.namenode.updatePipeline(dfsClient.clientName, oldBlock,
        block.getCurrentBlock(), nodes, storageIDs);
  }

  DatanodeInfo[] getExcludedNodes() {
    return excludedNodes.getAllPresent(excludedNodes.asMap().keySet())
            .keySet().toArray(DatanodeInfo.EMPTY_ARRAY);
  }

  /**
   * Open a DataStreamer to a DataNode so that it can be written to.
   * This happens when a file is created and each time a new block is allocated.
   * Must get block ID and the IDs of the destinations from the namenode.
   * Returns the list of target datanodes.
   */
  protected void setupPipelineForCreate() throws IOException {
    LocatedBlock lb;
    DatanodeInfo[] nodes;
    StorageType[] nextStorageTypes;
    String[] nextStorageIDs;
    int count = dfsClient.getConf().getNumBlockWriteRetry();
    boolean success;
    final ExtendedBlock oldBlock = block.getCurrentBlock();
    do {
      errorState.resetInternalError();
      lastException.clear();
      streamerClosed = false;

      DatanodeInfo[] excluded = getExcludedNodes();
      lb = locateFollowingBlock(
          excluded.length > 0 ? excluded : null, oldBlock);
      block.setCurrentBlock(lb.getBlock());
      block.setNumBytes(0);
      bytesSent = 0;
      accessToken = lb.getBlockToken();
      nodes = lb.getLocations();
      nextStorageTypes = lb.getStorageTypes();
      nextStorageIDs = lb.getStorageIDs();
      setPipeline(lb);
      try {
        // Connect to first DataNode in the list.
        success = createBlockOutputStream(nodes, nextStorageTypes, nextStorageIDs, 0L, false)
            || setupPipelineForAppendOrRecovery();

      } catch(IOException ie) {
        LOG.warn("Exception in setupPipelineForCreate " + this, ie);
        success = false;
      }
      if (!success) {
        LOG.warn("Abandoning " + block);
        dfsClient.namenode.abandonBlock(block.getCurrentBlock(),
            stat.getFileId(), src, dfsClient.clientName);
        block.setCurrentBlock(null);
        final DatanodeInfo badNode = errorState.getBadNodeIndex() == -1
              ? Iterables.getLast(failed)
              : nodes[errorState.getBadNodeIndex()];
        LOG.warn("Excluding datanode " + badNode);
        excludedNodes.put(badNode, badNode);
        setPipeline(null, null, null);
      }
    } while (!success && --count >= 0);

    if (!success) {
      throw new IOException("Unable to create new block.");
    }
  }

  // connects to the first datanode in the pipeline
  // Returns true if success, otherwise return failure.
  //
  boolean createBlockOutputStream(DatanodeInfo[] nodes,
      StorageType[] nodeStorageTypes, String[] nodeStorageIDs,
      long newGS, boolean recoveryFlag) {
    if (nodes.length == 0) {
      LOG.info("nodes are empty for write pipeline of " + block);
      return false;
    }
    String firstBadLink = "";
    boolean checkRestart = false;
    if (LOG.isDebugEnabled()) {
      LOG.debug("pipeline = " + Arrays.toString(nodes) + ", " + this);
    }

    // persist blocks on namenode on next flush
    persistBlocks.set(true);

    int refetchEncryptionKey = 1;
    while (true) {
      boolean result = false;
      DataOutputStream out = null;
      try {
        assert null == s : "Previous socket unclosed";
        assert null == blockReplyStream : "Previous blockReplyStream unclosed";
        s = createSocketForPipeline(nodes[0], nodes.length, dfsClient);
        long writeTimeout = dfsClient.getDatanodeWriteTimeout(nodes.length);
        long readTimeout = dfsClient.getDatanodeReadTimeout(nodes.length);

        OutputStream unbufOut = NetUtils.getOutputStream(s, writeTimeout);
        InputStream unbufIn = NetUtils.getInputStream(s, readTimeout);
        IOStreamPair saslStreams = dfsClient.saslClient.socketSend(s,
            unbufOut, unbufIn, dfsClient, accessToken, nodes[0]);
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            DFSUtilClient.getSmallBufferSize(dfsClient.getConfiguration())));
        blockReplyStream = new DataInputStream(unbufIn);

        //
        // Xmit header info to datanode
        //

        BlockConstructionStage bcs = recoveryFlag ?
            stage.getRecoveryStage() : stage;

        // We cannot change the block length in 'block' as it counts the number
        // of bytes ack'ed.
        ExtendedBlock blockCopy = block.getCurrentBlock();
        blockCopy.setNumBytes(stat.getBlockSize());

        boolean[] targetPinnings = getPinnings(nodes);
        // send the request
        new Sender(out).writeBlock(blockCopy, nodeStorageTypes[0], accessToken,
            dfsClient.clientName, nodes, nodeStorageTypes, null, bcs,
            nodes.length, block.getNumBytes(), bytesSent, newGS,
            checksum4WriteBlock, cachingStrategy.get(), isLazyPersistFile,
            (targetPinnings != null && targetPinnings[0]), targetPinnings,
            nodeStorageIDs[0], nodeStorageIDs);

        // receive ack for connect
        BlockOpResponseProto resp = BlockOpResponseProto.parseFrom(
            PBHelperClient.vintPrefixed(blockReplyStream));
        Status pipelineStatus = resp.getStatus();
        firstBadLink = resp.getFirstBadLink();

        // Got an restart OOB ack.
        // If a node is already restarting, this status is not likely from
        // the same node. If it is from a different node, it is not
        // from the local datanode. Thus it is safe to treat this as a
        // regular node error.
        if (PipelineAck.isRestartOOBStatus(pipelineStatus) &&
            !errorState.isRestartingNode()) {
          checkRestart = true;
          throw new IOException("A datanode is restarting.");
        }

        String logInfo = "ack with firstBadLink as " + firstBadLink;
        DataTransferProtoUtil.checkBlockOpStatus(resp, logInfo);

        assert null == blockStream : "Previous blockStream unclosed";
        blockStream = out;
        result =  true; // success
        errorState.resetInternalError();
        lastException.clear();
        // remove all restarting nodes from failed nodes list
        failed.removeAll(restartingNodes);
        restartingNodes.clear();
      } catch (IOException ie) {
        if (!errorState.isRestartingNode()) {
          LOG.warn("Exception in createBlockOutputStream " + this, ie);
        }
        if (ie instanceof InvalidEncryptionKeyException &&
            refetchEncryptionKey > 0) {
          LOG.info("Will fetch a new encryption key and retry, "
              + "encryption key was invalid when connecting to "
              + nodes[0] + " : " + ie);
          // The encryption key used is invalid.
          refetchEncryptionKey--;
          dfsClient.clearDataEncryptionKey();
          // Don't close the socket/exclude this node just yet. Try again with
          // a new encryption key.
          continue;
        }

        // find the datanode that matches
        if (firstBadLink.length() != 0) {
          for (int i = 0; i < nodes.length; i++) {
            // NB: Unconditionally using the xfer addr w/o hostname
            if (firstBadLink.equals(nodes[i].getXferAddr())) {
              errorState.setBadNodeIndex(i);
              break;
            }
          }
        } else {
          assert !checkRestart;
          errorState.setBadNodeIndex(0);
        }

        final int i = errorState.getBadNodeIndex();
        // Check whether there is a restart worth waiting for.
        if (checkRestart) {
          errorState.initRestartingNode(i,
              "Datanode " + i + " is restarting: " + nodes[i],
              shouldWaitForRestart(i));
        }
        errorState.setInternalError();
        lastException.set(ie);
        result =  false;  // error
      } finally {
        if (!result) {
          IOUtils.closeSocket(s);
          s = null;
          IOUtils.closeStream(out);
          IOUtils.closeStream(blockReplyStream);
          blockReplyStream = null;
        }
      }
      return result;
    }
  }

  private boolean[] getPinnings(DatanodeInfo[] nodes) {
    if (favoredNodes == null) {
      return null;
    } else {
      boolean[] pinnings = new boolean[nodes.length];
      HashSet<String> favoredSet = new HashSet<>(Arrays.asList(favoredNodes));
      for (int i = 0; i < nodes.length; i++) {
        pinnings[i] = favoredSet.remove(nodes[i].getXferAddrWithHostname());
        LOG.debug("{} was chosen by name node (favored={}).",
            nodes[i].getXferAddrWithHostname(), pinnings[i]);
      }
      if (!favoredSet.isEmpty()) {
        // There is one or more favored nodes that were not allocated.
        LOG.warn("These favored nodes were specified but not chosen: "
            + favoredSet + " Specified favored nodes: "
            + Arrays.toString(favoredNodes));

      }
      return pinnings;
    }
  }

  private LocatedBlock locateFollowingBlock(DatanodeInfo[] excluded,
      ExtendedBlock oldBlock) throws IOException {
    return DFSOutputStream.addBlock(excluded, dfsClient, src, oldBlock,
        stat.getFileId(), favoredNodes, addBlockFlags);
  }

  /**
   * This function sleeps for a certain amount of time when the writing
   * pipeline is congested. The function calculates the time based on a
   * decorrelated filter.
   *
   * @see
   * <a href="http://www.awsarchitectureblog.com/2015/03/backoff.html">
   *   http://www.awsarchitectureblog.com/2015/03/backoff.html</a>
   */
  private void backOffIfNecessary() throws InterruptedException {
    int t = 0;
    synchronized (congestedNodes) {
      if (!congestedNodes.isEmpty()) {
        StringBuilder sb = new StringBuilder("DataNode");
        for (DatanodeInfo i : congestedNodes) {
          sb.append(' ').append(i);
        }
        int range = Math.abs(lastCongestionBackoffTime * 3 -
            congestionBackOffMeanTimeInMs);
        int base = Math.min(lastCongestionBackoffTime * 3,
            congestionBackOffMeanTimeInMs);
        t = Math.min(congestionBackOffMaxTimeInMs,
                     (int)(base + Math.random() * range));
        lastCongestionBackoffTime = t;
        sb.append(" are congested. Backing off for ").append(t).append(" ms");
        LOG.info(sb.toString());
        congestedNodes.clear();
      }
    }
    if (t != 0) {
      Thread.sleep(t);
    }
  }

  /**
   * get the block this streamer is writing to
   *
   * @return the block this streamer is writing to
   */
  ExtendedBlock getBlock() {
    return block.getCurrentBlock();
  }

  /**
   * return the target datanodes in the pipeline
   *
   * @return the target datanodes in the pipeline
   */
  DatanodeInfo[] getNodes() {
    return nodes;
  }

  String[] getStorageIDs() {
    return storageIDs;
  }

  BlockConstructionStage getStage() {
    return stage;
  }

  /**
   * return the token of the block
   *
   * @return the token of the block
   */
  Token<BlockTokenIdentifier> getBlockToken() {
    return accessToken;
  }

  ErrorState getErrorState() {
    return errorState;
  }

  /**
   * Put a packet to the data queue
   *
   * @param packet the packet to be put into the data queued
   */
  void queuePacket(DFSPacket packet) {
    synchronized (dataQueue) {
      if (packet == null) return;
      packet.addTraceParent(Tracer.getCurrentSpan());
      dataQueue.addLast(packet);
      lastQueuedSeqno = packet.getSeqno();
      LOG.debug("Queued {}, {}", packet, this);
      dataQueue.notifyAll();
    }
  }

  /**
   * For heartbeat packets, create buffer directly by new byte[]
   * since heartbeats should not be blocked.
   */
  private DFSPacket createHeartbeatPacket() {
    final byte[] buf = new byte[PacketHeader.PKT_MAX_HEADER_LEN];
    return new DFSPacket(buf, 0, 0, DFSPacket.HEART_BEAT_SEQNO, 0, false);
  }

  private static LoadingCache<DatanodeInfo, DatanodeInfo> initExcludedNodes(
      long excludedNodesCacheExpiry) {
    return CacheBuilder.newBuilder()
        .expireAfterWrite(excludedNodesCacheExpiry, TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener<DatanodeInfo, DatanodeInfo>() {
          @Override
          public void onRemoval(
              @Nonnull RemovalNotification<DatanodeInfo, DatanodeInfo>
                  notification) {
            LOG.info("Removing node " + notification.getKey()
                + " from the excluded nodes list");
          }
        }).build(new CacheLoader<DatanodeInfo, DatanodeInfo>() {
          @Override
          public DatanodeInfo load(DatanodeInfo key) throws Exception {
            return key;
          }
        });
  }

  private static <T> void arraycopy(T[] srcs, T[] dsts, int skipIndex) {
    System.arraycopy(srcs, 0, dsts, 0, skipIndex);
    System.arraycopy(srcs, skipIndex+1, dsts, skipIndex, dsts.length-skipIndex);
  }

  /**
   * check if to persist blocks on namenode
   *
   * @return if to persist blocks on namenode
   */
  AtomicBoolean getPersistBlocks(){
    return persistBlocks;
  }

  /**
   * check if to append a chunk
   *
   * @param appendChunk if to append a chunk
   */
  void setAppendChunk(boolean appendChunk){
    this.appendChunk = appendChunk;
  }

  /**
   * get if to append a chunk
   *
   * @return if to append a chunk
   */
  boolean getAppendChunk(){
    return appendChunk;
  }

  /**
   * @return the last exception
   */
  LastExceptionInStreamer getLastException(){
    return lastException;
  }

  /**
   * set socket to null
   */
  void setSocketToNull() {
    this.s = null;
  }

  /**
   * return current sequence number and then increase it by 1
   *
   * @return current sequence number before increasing
   */
  long getAndIncCurrentSeqno() {
    long old = this.currentSeqno;
    this.currentSeqno++;
    return old;
  }

  /**
   * get last queued sequence number
   *
   * @return last queued sequence number
   */
  long getLastQueuedSeqno() {
    return lastQueuedSeqno;
  }

  /**
   * get the number of bytes of current block
   *
   * @return the number of bytes of current block
   */
  long getBytesCurBlock() {
    return bytesCurBlock;
  }

  /**
   * set the bytes of current block that have been written
   *
   * @param bytesCurBlock bytes of current block that have been written
   */
  void setBytesCurBlock(long bytesCurBlock) {
    this.bytesCurBlock = bytesCurBlock;
  }

  /**
   * increase bytes of current block by len.
   *
   * @param len how many bytes to increase to current block
   */
  void incBytesCurBlock(long len) {
    this.bytesCurBlock += len;
  }

  /**
   * set artificial slow down for unit test
   *
   * @param period artificial slow down
   */
  void setArtificialSlowdown(long period) {
    this.artificialSlowdown = period;
  }

  /**
   * if this streamer is to terminate
   *
   * @return if this streamer is to terminate
   */
  boolean streamerClosed(){
    return streamerClosed;
  }

  /**
   * @return The times have retried to recover pipeline, for the same packet.
   */
  @VisibleForTesting
  int getPipelineRecoveryCount() {
    return pipelineRecoveryCount;
  }

  void closeSocket() throws IOException {
    if (s != null) {
      s.close();
    }
  }

  @Override
  public String toString() {
    final ExtendedBlock extendedBlock = block.getCurrentBlock();
    return extendedBlock == null ?
        "block==null" : "" + extendedBlock.getLocalBlock();
  }

  public boolean isEndBlockFlag() {
    return endBlockFlag;
  }

  public void setEndBlockFlag(boolean endBlockFlag) {
    this.endBlockFlag = endBlockFlag;
  }
}
