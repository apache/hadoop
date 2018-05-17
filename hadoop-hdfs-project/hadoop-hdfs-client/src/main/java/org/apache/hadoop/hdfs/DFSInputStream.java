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

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.ByteBufferUtil;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.HasEnhancedByteBufferAccess;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hdfs.DFSUtilClient.CorruptedBlocks;
import org.apache.hadoop.hdfs.client.impl.BlockReaderFactory;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.shortcircuit.ClientMmap;
import org.apache.hadoop.hdfs.util.IOUtilsClient;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.IdentityHashStore;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;

/****************************************************************
 * DFSInputStream provides bytes from a named file.  It handles
 * negotiation of the namenode and various datanodes as necessary.
 ****************************************************************/
@InterfaceAudience.Private
public class DFSInputStream extends FSInputStream
    implements ByteBufferReadable, CanSetDropBehind, CanSetReadahead,
               HasEnhancedByteBufferAccess, CanUnbuffer, StreamCapabilities {
  @VisibleForTesting
  public static boolean tcpReadsDisabledForTesting = false;
  private long hedgedReadOpsLoopNumForTesting = 0;
  protected final DFSClient dfsClient;
  protected AtomicBoolean closed = new AtomicBoolean(false);
  protected final String src;
  protected final boolean verifyChecksum;

  // state by stateful read only:
  // (protected by lock on this)
  /////
  private DatanodeInfo currentNode = null;
  protected LocatedBlock currentLocatedBlock = null;
  protected long pos = 0;
  protected long blockEnd = -1;
  private BlockReader blockReader = null;
  ////

  // state shared by stateful and positional read:
  // (protected by lock on infoLock)
  ////
  protected LocatedBlocks locatedBlocks = null;
  private long lastBlockBeingWrittenLength = 0;
  private FileEncryptionInfo fileEncryptionInfo = null;
  protected CachingStrategy cachingStrategy;
  ////

  protected final ReadStatistics readStatistics = new ReadStatistics();
  // lock for state shared between read and pread
  // Note: Never acquire a lock on <this> with this lock held to avoid deadlocks
  //       (it's OK to acquire this lock when the lock on <this> is held)
  protected final Object infoLock = new Object();

  /**
   * Track the ByteBuffers that we have handed out to readers.
   *
   * The value type can be either ByteBufferPool or ClientMmap, depending on
   * whether we this is a memory-mapped buffer or not.
   */
  private IdentityHashStore<ByteBuffer, Object> extendedReadBuffers;

  private synchronized IdentityHashStore<ByteBuffer, Object>
        getExtendedReadBuffers() {
    if (extendedReadBuffers == null) {
      extendedReadBuffers = new IdentityHashStore<>(0);
    }
    return extendedReadBuffers;
  }

  /**
   * This variable tracks the number of failures since the start of the
   * most recent user-facing operation. That is to say, it should be reset
   * whenever the user makes a call on this stream, and if at any point
   * during the retry logic, the failure count exceeds a threshold,
   * the errors will be thrown back to the operation.
   *
   * Specifically this counts the number of times the client has gone
   * back to the namenode to get a new list of block locations, and is
   * capped at maxBlockAcquireFailures
   */
  protected int failures = 0;

  /* XXX Use of CocurrentHashMap is temp fix. Need to fix
   * parallel accesses to DFSInputStream (through ptreads) properly */
  private final ConcurrentHashMap<DatanodeInfo, DatanodeInfo> deadNodes =
             new ConcurrentHashMap<>();

  private byte[] oneByteBuf; // used for 'int read()'

  void addToDeadNodes(DatanodeInfo dnInfo) {
    deadNodes.put(dnInfo, dnInfo);
  }

  DFSInputStream(DFSClient dfsClient, String src, boolean verifyChecksum,
      LocatedBlocks locatedBlocks) throws IOException {
    this.dfsClient = dfsClient;
    this.verifyChecksum = verifyChecksum;
    this.src = src;
    synchronized (infoLock) {
      this.cachingStrategy = dfsClient.getDefaultReadCachingStrategy();
    }
    this.locatedBlocks = locatedBlocks;
    openInfo(false);
  }

  @VisibleForTesting
  public long getlastBlockBeingWrittenLengthForTesting() {
    return lastBlockBeingWrittenLength;
  }

  /**
   * Grab the open-file info from namenode
   * @param refreshLocatedBlocks whether to re-fetch locatedblocks
   */
  void openInfo(boolean refreshLocatedBlocks) throws IOException {
    final DfsClientConf conf = dfsClient.getConf();
    synchronized(infoLock) {
      lastBlockBeingWrittenLength =
          fetchLocatedBlocksAndGetLastBlockLength(refreshLocatedBlocks);
      int retriesForLastBlockLength = conf.getRetryTimesForGetLastBlockLength();
      while (retriesForLastBlockLength > 0) {
        // Getting last block length as -1 is a special case. When cluster
        // restarts, DNs may not report immediately. At this time partial block
        // locations will not be available with NN for getting the length. Lets
        // retry for 3 times to get the length.
        if (lastBlockBeingWrittenLength == -1) {
          DFSClient.LOG.warn("Last block locations not available. "
              + "Datanodes might not have reported blocks completely."
              + " Will retry for " + retriesForLastBlockLength + " times");
          waitFor(conf.getRetryIntervalForGetLastBlockLength());
          lastBlockBeingWrittenLength =
              fetchLocatedBlocksAndGetLastBlockLength(true);
        } else {
          break;
        }
        retriesForLastBlockLength--;
      }
      if (lastBlockBeingWrittenLength == -1
          && retriesForLastBlockLength == 0) {
        throw new IOException("Could not obtain the last block locations.");
      }
    }
  }

  private void waitFor(int waitTime) throws IOException {
    try {
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException(
          "Interrupted while getting the last block length.");
    }
  }

  private long fetchLocatedBlocksAndGetLastBlockLength(boolean refresh)
      throws IOException {
    LocatedBlocks newInfo = locatedBlocks;
    if (locatedBlocks == null || refresh) {
      newInfo = dfsClient.getLocatedBlocks(src, 0);
    }
    DFSClient.LOG.debug("newInfo = {}", newInfo);
    if (newInfo == null) {
      throw new IOException("Cannot open filename " + src);
    }

    if (locatedBlocks != null) {
      Iterator<LocatedBlock> oldIter = locatedBlocks.getLocatedBlocks().iterator();
      Iterator<LocatedBlock> newIter = newInfo.getLocatedBlocks().iterator();
      while (oldIter.hasNext() && newIter.hasNext()) {
        if (!oldIter.next().getBlock().equals(newIter.next().getBlock())) {
          throw new IOException("Blocklist for " + src + " has changed!");
        }
      }
    }
    locatedBlocks = newInfo;
    long lastBlockBeingWrittenLength = 0;
    if (!locatedBlocks.isLastBlockComplete()) {
      final LocatedBlock last = locatedBlocks.getLastLocatedBlock();
      if (last != null) {
        if (last.getLocations().length == 0) {
          if (last.getBlockSize() == 0) {
            // if the length is zero, then no data has been written to
            // datanode. So no need to wait for the locations.
            return 0;
          }
          return -1;
        }
        final long len = readBlockLength(last);
        last.getBlock().setNumBytes(len);
        lastBlockBeingWrittenLength = len;
      }
    }

    fileEncryptionInfo = locatedBlocks.getFileEncryptionInfo();

    return lastBlockBeingWrittenLength;
  }

  /** Read the block length from one of the datanodes. */
  private long readBlockLength(LocatedBlock locatedblock) throws IOException {
    assert locatedblock != null : "LocatedBlock cannot be null";
    int replicaNotFoundCount = locatedblock.getLocations().length;

    final DfsClientConf conf = dfsClient.getConf();
    final int timeout = conf.getSocketTimeout();
    LinkedList<DatanodeInfo> nodeList = new LinkedList<DatanodeInfo>(
        Arrays.asList(locatedblock.getLocations()));
    LinkedList<DatanodeInfo> retryList = new LinkedList<DatanodeInfo>();
    boolean isRetry = false;
    StopWatch sw = new StopWatch();
    while (nodeList.size() > 0) {
      DatanodeInfo datanode = nodeList.pop();
      ClientDatanodeProtocol cdp = null;
      try {
        cdp = DFSUtilClient.createClientDatanodeProtocolProxy(datanode,
            dfsClient.getConfiguration(), timeout,
            conf.isConnectToDnViaHostname(), locatedblock);

        final long n = cdp.getReplicaVisibleLength(locatedblock.getBlock());

        if (n >= 0) {
          return n;
        }
      } catch (IOException ioe) {
        checkInterrupted(ioe);
        if (ioe instanceof RemoteException) {
          if (((RemoteException) ioe).unwrapRemoteException() instanceof
              ReplicaNotFoundException) {
            // replica is not on the DN. We will treat it as 0 length
            // if no one actually has a replica.
            replicaNotFoundCount--;
          } else if (((RemoteException) ioe).unwrapRemoteException() instanceof
              RetriableException) {
            // add to the list to be retried if necessary.
            retryList.add(datanode);
          }
        }
        DFSClient.LOG.debug("Failed to getReplicaVisibleLength from datanode {}"
              + " for block {}", datanode, locatedblock.getBlock(), ioe);
      } finally {
        if (cdp != null) {
          RPC.stopProxy(cdp);
        }
      }

      // Ran out of nodes, but there are retriable nodes.
      if (nodeList.size() == 0 && retryList.size() > 0) {
        nodeList.addAll(retryList);
        retryList.clear();
        isRetry = true;
      }

      if (isRetry) {
        // start the stop watch if not already running.
        if (!sw.isRunning()) {
          sw.start();
        }
        try {
          Thread.sleep(500); // delay between retries.
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException(
              "Interrupted while getting the length.");
        }
      }

      // see if we ran out of retry time
      if (sw.isRunning() && sw.now(TimeUnit.MILLISECONDS) > timeout) {
        break;
      }
    }

    // Namenode told us about these locations, but none know about the replica
    // means that we hit the race between pipeline creation start and end.
    // we require all 3 because some other exception could have happened
    // on a DN that has it.  we want to report that error
    if (replicaNotFoundCount == 0) {
      return 0;
    }

    throw new IOException("Cannot obtain block length for " + locatedblock);
  }

  public long getFileLength() {
    synchronized(infoLock) {
      return locatedBlocks == null? 0:
          locatedBlocks.getFileLength() + lastBlockBeingWrittenLength;
    }
  }

  // Short circuit local reads are forbidden for files that are
  // under construction.  See HDFS-2757.
  boolean shortCircuitForbidden() {
    synchronized(infoLock) {
      return locatedBlocks.isUnderConstruction();
    }
  }

  /**
   * Returns the datanode from which the stream is currently reading.
   */
  public synchronized DatanodeInfo getCurrentDatanode() {
    return currentNode;
  }

  /**
   * Returns the block containing the target position.
   */
  synchronized public ExtendedBlock getCurrentBlock() {
    if (currentLocatedBlock == null){
      return null;
    }
    return currentLocatedBlock.getBlock();
  }

  /**
   * Return collection of blocks that has already been located.
   */
  public List<LocatedBlock> getAllBlocks() throws IOException {
    return getBlockRange(0, getFileLength());
  }

  /**
   * Get block at the specified position.
   * Fetch it from the namenode if not cached.
   *
   * @param offset block corresponding to this offset in file is returned
   * @return located block
   * @throws IOException
   */
  protected LocatedBlock getBlockAt(long offset) throws IOException {
    synchronized(infoLock) {
      assert (locatedBlocks != null) : "locatedBlocks is null";

      final LocatedBlock blk;

      //check offset
      if (offset < 0 || offset >= getFileLength()) {
        throw new IOException("offset < 0 || offset >= getFileLength(), offset="
            + offset
            + ", locatedBlocks=" + locatedBlocks);
      }
      else if (offset >= locatedBlocks.getFileLength()) {
        // offset to the portion of the last block,
        // which is not known to the name-node yet;
        // getting the last block
        blk = locatedBlocks.getLastLocatedBlock();
      }
      else {
        // search cached blocks first
        blk = fetchBlockAt(offset, 0, true);
      }
      return blk;
    }
  }

  /** Fetch a block from namenode and cache it */
  protected LocatedBlock fetchBlockAt(long offset) throws IOException {
    return fetchBlockAt(offset, 0, false); // don't use cache
  }

  /** Fetch a block from namenode and cache it */
  private LocatedBlock fetchBlockAt(long offset, long length, boolean useCache)
      throws IOException {
    synchronized(infoLock) {
      int targetBlockIdx = locatedBlocks.findBlock(offset);
      if (targetBlockIdx < 0) { // block is not cached
        targetBlockIdx = LocatedBlocks.getInsertIndex(targetBlockIdx);
        useCache = false;
      }
      if (!useCache) { // fetch blocks
        final LocatedBlocks newBlocks = (length == 0)
            ? dfsClient.getLocatedBlocks(src, offset)
            : dfsClient.getLocatedBlocks(src, offset, length);
        if (newBlocks == null || newBlocks.locatedBlockCount() == 0) {
          throw new EOFException("Could not find target position " + offset);
        }
        locatedBlocks.insertRange(targetBlockIdx, newBlocks.getLocatedBlocks());
      }
      return locatedBlocks.get(targetBlockIdx);
    }
  }

  /**
   * Get blocks in the specified range.
   * Fetch them from the namenode if not cached. This function
   * will not get a read request beyond the EOF.
   * @param offset starting offset in file
   * @param length length of data
   * @return consequent segment of located blocks
   * @throws IOException
   */
  private List<LocatedBlock> getBlockRange(long offset,
      long length)  throws IOException {
    // getFileLength(): returns total file length
    // locatedBlocks.getFileLength(): returns length of completed blocks
    if (offset >= getFileLength()) {
      throw new IOException("Offset: " + offset +
        " exceeds file length: " + getFileLength());
    }
    synchronized(infoLock) {
      final List<LocatedBlock> blocks;
      final long lengthOfCompleteBlk = locatedBlocks.getFileLength();
      final boolean readOffsetWithinCompleteBlk = offset < lengthOfCompleteBlk;
      final boolean readLengthPastCompleteBlk = offset + length > lengthOfCompleteBlk;

      if (readOffsetWithinCompleteBlk) {
        //get the blocks of finalized (completed) block range
        blocks = getFinalizedBlockRange(offset,
          Math.min(length, lengthOfCompleteBlk - offset));
      } else {
        blocks = new ArrayList<>(1);
      }

      // get the blocks from incomplete block range
      if (readLengthPastCompleteBlk) {
        blocks.add(locatedBlocks.getLastLocatedBlock());
      }

      return blocks;
    }
  }

  /**
   * Get blocks in the specified range.
   * Includes only the complete blocks.
   * Fetch them from the namenode if not cached.
   */
  private List<LocatedBlock> getFinalizedBlockRange(
      long offset, long length) throws IOException {
    synchronized(infoLock) {
      assert (locatedBlocks != null) : "locatedBlocks is null";
      List<LocatedBlock> blockRange = new ArrayList<>();
      // search cached blocks first
      long remaining = length;
      long curOff = offset;
      while(remaining > 0) {
        LocatedBlock blk = fetchBlockAt(curOff, remaining, true);
        assert curOff >= blk.getStartOffset() : "Block not found";
        blockRange.add(blk);
        long bytesRead = blk.getStartOffset() + blk.getBlockSize() - curOff;
        remaining -= bytesRead;
        curOff += bytesRead;
      }
      return blockRange;
    }
  }

  /**
   * Open a DataInputStream to a DataNode so that it can be read from.
   * We get block ID and the IDs of the destinations at startup, from the namenode.
   */
  private synchronized DatanodeInfo blockSeekTo(long target)
      throws IOException {
    if (target >= getFileLength()) {
      throw new IOException("Attempted to read past end of file");
    }

    // Will be getting a new BlockReader.
    closeCurrentBlockReaders();

    //
    // Connect to best DataNode for desired Block, with potential offset
    //
    DatanodeInfo chosenNode;
    int refetchToken = 1; // only need to get a new access token once
    int refetchEncryptionKey = 1; // only need to get a new encryption key once

    boolean connectFailedOnce = false;

    while (true) {
      //
      // Compute desired block
      //
      LocatedBlock targetBlock = getBlockAt(target);

      // update current position
      this.pos = target;
      this.blockEnd = targetBlock.getStartOffset() +
            targetBlock.getBlockSize() - 1;
      this.currentLocatedBlock = targetBlock;

      long offsetIntoBlock = target - targetBlock.getStartOffset();

      DNAddrPair retval = chooseDataNode(targetBlock, null);
      chosenNode = retval.info;
      InetSocketAddress targetAddr = retval.addr;
      StorageType storageType = retval.storageType;
      // Latest block if refreshed by chooseDatanode()
      targetBlock = retval.block;

      try {
        blockReader = getBlockReader(targetBlock, offsetIntoBlock,
            targetBlock.getBlockSize() - offsetIntoBlock, targetAddr,
            storageType, chosenNode);
        if(connectFailedOnce) {
          DFSClient.LOG.info("Successfully connected to " + targetAddr +
                             " for " + targetBlock.getBlock());
        }
        return chosenNode;
      } catch (IOException ex) {
        checkInterrupted(ex);
        if (ex instanceof InvalidEncryptionKeyException && refetchEncryptionKey > 0) {
          DFSClient.LOG.info("Will fetch a new encryption key and retry, "
              + "encryption key was invalid when connecting to " + targetAddr
              + " : " + ex);
          // The encryption key used is invalid.
          refetchEncryptionKey--;
          dfsClient.clearDataEncryptionKey();
        } else if (refetchToken > 0 && tokenRefetchNeeded(ex, targetAddr)) {
          refetchToken--;
          fetchBlockAt(target);
        } else {
          connectFailedOnce = true;
          DFSClient.LOG.warn("Failed to connect to {} for block {}, " +
              "add to deadNodes and continue. ", targetAddr,
              targetBlock.getBlock(), ex);
          // Put chosen node into dead list, continue
          addToDeadNodes(chosenNode);
        }
      }
    }
  }

  private void checkInterrupted(IOException e) throws IOException {
    if (Thread.currentThread().isInterrupted() &&
        (e instanceof ClosedByInterruptException ||
            e instanceof InterruptedIOException)) {
      DFSClient.LOG.debug("The reading thread has been interrupted.", e);
      throw e;
    }
  }

  protected BlockReader getBlockReader(LocatedBlock targetBlock,
      long offsetInBlock, long length, InetSocketAddress targetAddr,
      StorageType storageType, DatanodeInfo datanode) throws IOException {
    ExtendedBlock blk = targetBlock.getBlock();
    Token<BlockTokenIdentifier> accessToken = targetBlock.getBlockToken();
    CachingStrategy curCachingStrategy;
    boolean shortCircuitForbidden;
    synchronized (infoLock) {
      curCachingStrategy = cachingStrategy;
      shortCircuitForbidden = shortCircuitForbidden();
    }
    return new BlockReaderFactory(dfsClient.getConf()).
        setInetSocketAddress(targetAddr).
        setRemotePeerFactory(dfsClient).
        setDatanodeInfo(datanode).
        setStorageType(storageType).
        setFileName(src).
        setBlock(blk).
        setBlockToken(accessToken).
        setStartOffset(offsetInBlock).
        setVerifyChecksum(verifyChecksum).
        setClientName(dfsClient.clientName).
        setLength(length).
        setCachingStrategy(curCachingStrategy).
        setAllowShortCircuitLocalReads(!shortCircuitForbidden).
        setClientCacheContext(dfsClient.getClientContext()).
        setUserGroupInformation(dfsClient.ugi).
        setConfiguration(dfsClient.getConfiguration()).
        setTracer(dfsClient.getTracer()).
        build();
  }

  /**
   * Close it down!
   */
  @Override
  public synchronized void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      DFSClient.LOG.debug("DFSInputStream has been closed already");
      return;
    }
    dfsClient.checkOpen();

    if ((extendedReadBuffers != null) && (!extendedReadBuffers.isEmpty())) {
      final StringBuilder builder = new StringBuilder();
      extendedReadBuffers.visitAll(new IdentityHashStore.Visitor<ByteBuffer, Object>() {
        private String prefix = "";
        @Override
        public void accept(ByteBuffer k, Object v) {
          builder.append(prefix).append(k);
          prefix = ", ";
        }
      });
      DFSClient.LOG.warn("closing file " + src + ", but there are still " +
          "unreleased ByteBuffers allocated by read().  " +
          "Please release " + builder.toString() + ".");
    }
    closeCurrentBlockReaders();
    super.close();
  }

  @Override
  public synchronized int read() throws IOException {
    if (oneByteBuf == null) {
      oneByteBuf = new byte[1];
    }
    int ret = read(oneByteBuf, 0, 1);
    return (ret <= 0) ? -1 : (oneByteBuf[0] & 0xff);
  }

  /* This is a used by regular read() and handles ChecksumExceptions.
   * name readBuffer() is chosen to imply similarity to readBuffer() in
   * ChecksumFileSystem
   */
  private synchronized int readBuffer(ReaderStrategy reader, int len,
                                      CorruptedBlocks corruptedBlocks)
      throws IOException {
    IOException ioe;

    /* we retry current node only once. So this is set to true only here.
     * Intention is to handle one common case of an error that is not a
     * failure on datanode or client : when DataNode closes the connection
     * since client is idle. If there are other cases of "non-errors" then
     * then a datanode might be retried by setting this to true again.
     */
    boolean retryCurrentNode = true;

    while (true) {
      // retry as many times as seekToNewSource allows.
      try {
        return reader.readFromBlock(blockReader, len);
      } catch (ChecksumException ce) {
        DFSClient.LOG.warn("Found Checksum error for "
            + getCurrentBlock() + " from " + currentNode
            + " at " + ce.getPos());
        ioe = ce;
        retryCurrentNode = false;
        // we want to remember which block replicas we have tried
        corruptedBlocks.addCorruptedBlock(getCurrentBlock(), currentNode);
      } catch (IOException e) {
        if (!retryCurrentNode) {
          DFSClient.LOG.warn("Exception while reading from "
              + getCurrentBlock() + " of " + src + " from "
              + currentNode, e);
        }
        ioe = e;
      }
      boolean sourceFound;
      if (retryCurrentNode) {
        /* possibly retry the same node so that transient errors don't
         * result in application level failures (e.g. Datanode could have
         * closed the connection because the client is idle for too long).
         */
        sourceFound = seekToBlockSource(pos);
      } else {
        addToDeadNodes(currentNode);
        sourceFound = seekToNewSource(pos);
      }
      if (!sourceFound) {
        throw ioe;
      }
      retryCurrentNode = false;
    }
  }

  protected synchronized int readWithStrategy(ReaderStrategy strategy)
      throws IOException {
    dfsClient.checkOpen();
    if (closed.get()) {
      throw new IOException("Stream closed");
    }

    int len = strategy.getTargetLength();
    CorruptedBlocks corruptedBlocks = new CorruptedBlocks();
    failures = 0;
    if (pos < getFileLength()) {
      int retries = 2;
      while (retries > 0) {
        try {
          // currentNode can be left as null if previous read had a checksum
          // error on the same block. See HDFS-3067
          if (pos > blockEnd || currentNode == null) {
            currentNode = blockSeekTo(pos);
          }
          int realLen = (int) Math.min(len, (blockEnd - pos + 1L));
          synchronized(infoLock) {
            if (locatedBlocks.isLastBlockComplete()) {
              realLen = (int) Math.min(realLen,
                  locatedBlocks.getFileLength() - pos);
            }
          }
          int result = readBuffer(strategy, realLen, corruptedBlocks);

          if (result >= 0) {
            pos += result;
          } else {
            // got a EOS from reader though we expect more data on it.
            throw new IOException("Unexpected EOS from the reader");
          }
          return result;
        } catch (ChecksumException ce) {
          throw ce;
        } catch (IOException e) {
          checkInterrupted(e);
          if (retries == 1) {
            DFSClient.LOG.warn("DFS Read", e);
          }
          blockEnd = -1;
          if (currentNode != null) {
            addToDeadNodes(currentNode);
          }
          if (--retries == 0) {
            throw e;
          }
        } finally {
          // Check if need to report block replicas corruption either read
          // was successful or ChecksumException occurred.
          reportCheckSumFailure(corruptedBlocks,
              getCurrentBlockLocationsLength(), false);
        }
      }
    }
    return -1;
  }

  protected int getCurrentBlockLocationsLength() {
    int len = 0;
    if (currentLocatedBlock == null) {
      DFSClient.LOG.info("Found null currentLocatedBlock. pos={}, "
          + "blockEnd={}, fileLength={}", pos, blockEnd, getFileLength());
    } else {
      len = currentLocatedBlock.getLocations().length;
    }
    return len;
  }

  /**
   * Read the entire buffer.
   */
  @Override
  public synchronized int read(@Nonnull final byte buf[], int off, int len)
      throws IOException {
    validatePositionedReadArgs(pos, buf, off, len);
    if (len == 0) {
      return 0;
    }
    ReaderStrategy byteArrayReader =
        new ByteArrayStrategy(buf, off, len, readStatistics, dfsClient);
    try (TraceScope scope =
             dfsClient.newReaderTraceScope("DFSInputStream#byteArrayRead",
                 src, getPos(), len)) {
      int retLen = readWithStrategy(byteArrayReader);
      if (retLen < len) {
        dfsClient.addRetLenToReaderScope(scope, retLen);
      }
      return retLen;
    }
  }

  @Override
  public synchronized int read(final ByteBuffer buf) throws IOException {
    ReaderStrategy byteBufferReader =
        new ByteBufferStrategy(buf, readStatistics, dfsClient);
    int reqLen = buf.remaining();
    try (TraceScope scope =
             dfsClient.newReaderTraceScope("DFSInputStream#byteBufferRead",
                 src, getPos(), reqLen)){
      int retLen = readWithStrategy(byteBufferReader);
      if (retLen < reqLen) {
        dfsClient.addRetLenToReaderScope(scope, retLen);
      }
      return retLen;
    }
  }

  private DNAddrPair chooseDataNode(LocatedBlock block,
      Collection<DatanodeInfo> ignoredNodes) throws IOException {
    return chooseDataNode(block, ignoredNodes, true);
  }

  /**
   * Choose datanode to read from.
   *
   * @param block             Block to choose datanode addr from
   * @param ignoredNodes      Ignored nodes inside.
   * @param refetchIfRequired Whether to refetch if no nodes to chose
   *                          from.
   * @return Returns chosen DNAddrPair; Can be null if refetchIfRequired is
   * false.
   */
  private DNAddrPair chooseDataNode(LocatedBlock block,
      Collection<DatanodeInfo> ignoredNodes, boolean refetchIfRequired)
      throws IOException {
    while (true) {
      DNAddrPair result = getBestNodeDNAddrPair(block, ignoredNodes);
      if (result != null) {
        return result;
      } else if (refetchIfRequired) {
        block = refetchLocations(block, ignoredNodes);
      } else {
        return null;
      }
    }
  }

  private LocatedBlock refetchLocations(LocatedBlock block,
      Collection<DatanodeInfo> ignoredNodes) throws IOException {
    String errMsg = getBestNodeDNAddrPairErrorString(block.getLocations(),
        deadNodes, ignoredNodes);
    String blockInfo = block.getBlock() + " file=" + src;
    if (failures >= dfsClient.getConf().getMaxBlockAcquireFailures()) {
      String description = "Could not obtain block: " + blockInfo;
      DFSClient.LOG.warn(description + errMsg
          + ". Throwing a BlockMissingException");
      throw new BlockMissingException(src, description,
          block.getStartOffset());
    }

    DatanodeInfo[] nodes = block.getLocations();
    if (nodes == null || nodes.length == 0) {
      DFSClient.LOG.info("No node available for " + blockInfo);
    }
    DFSClient.LOG.info("Could not obtain " + block.getBlock()
        + " from any node: " + errMsg
        + ". Will get new block locations from namenode and retry...");
    try {
      // Introducing a random factor to the wait time before another retry.
      // The wait time is dependent on # of failures and a random factor.
      // At the first time of getting a BlockMissingException, the wait time
      // is a random number between 0..3000 ms. If the first retry
      // still fails, we will wait 3000 ms grace period before the 2nd retry.
      // Also at the second retry, the waiting window is expanded to 6000 ms
      // alleviating the request rate from the server. Similarly the 3rd retry
      // will wait 6000ms grace period before retry and the waiting window is
      // expanded to 9000ms.
      final int timeWindow = dfsClient.getConf().getTimeWindow();
      // grace period for the last round of attempt
      double waitTime = timeWindow * failures +
          // expanding time window for each failure
          timeWindow * (failures + 1) *
          ThreadLocalRandom.current().nextDouble();
      DFSClient.LOG.warn("DFS chooseDataNode: got # " + (failures + 1) +
          " IOException, will wait for " + waitTime + " msec.");
      Thread.sleep((long)waitTime);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException(
          "Interrupted while choosing DataNode for read.");
    }
    deadNodes.clear(); //2nd option is to remove only nodes[blockId]
    openInfo(true);
    block = refreshLocatedBlock(block);
    failures++;
    return block;
  }

  /**
   * Get the best node from which to stream the data.
   * @param block LocatedBlock, containing nodes in priority order.
   * @param ignoredNodes Do not choose nodes in this array (may be null)
   * @return The DNAddrPair of the best node. Null if no node can be chosen.
   */
  protected DNAddrPair getBestNodeDNAddrPair(LocatedBlock block,
      Collection<DatanodeInfo> ignoredNodes) {
    DatanodeInfo[] nodes = block.getLocations();
    StorageType[] storageTypes = block.getStorageTypes();
    DatanodeInfo chosenNode = null;
    StorageType storageType = null;
    if (nodes != null) {
      for (int i = 0; i < nodes.length; i++) {
        if (!deadNodes.containsKey(nodes[i])
            && (ignoredNodes == null || !ignoredNodes.contains(nodes[i]))) {
          chosenNode = nodes[i];
          // Storage types are ordered to correspond with nodes, so use the same
          // index to get storage type.
          if (storageTypes != null && i < storageTypes.length) {
            storageType = storageTypes[i];
          }
          break;
        }
      }
    }
    if (chosenNode == null) {
      reportLostBlock(block, ignoredNodes);
      return null;
    }
    final String dnAddr =
        chosenNode.getXferAddr(dfsClient.getConf().isConnectToDnViaHostname());
    DFSClient.LOG.debug("Connecting to datanode {}", dnAddr);
    InetSocketAddress targetAddr = NetUtils.createSocketAddr(dnAddr);
    return new DNAddrPair(chosenNode, targetAddr, storageType, block);
  }

  /**
   * Warn the user of a lost block
   */
  protected void reportLostBlock(LocatedBlock lostBlock,
      Collection<DatanodeInfo> ignoredNodes) {
    DatanodeInfo[] nodes = lostBlock.getLocations();
    DFSClient.LOG.warn("No live nodes contain block " + lostBlock.getBlock() +
        " after checking nodes = " + Arrays.toString(nodes) +
        ", ignoredNodes = " + ignoredNodes);
  }

  private static String getBestNodeDNAddrPairErrorString(
      DatanodeInfo nodes[], AbstractMap<DatanodeInfo,
      DatanodeInfo> deadNodes, Collection<DatanodeInfo> ignoredNodes) {
    StringBuilder errMsgr = new StringBuilder(
        " No live nodes contain current block ");
    errMsgr.append("Block locations:");
    for (DatanodeInfo datanode : nodes) {
      errMsgr.append(" ");
      errMsgr.append(datanode.toString());
    }
    errMsgr.append(" Dead nodes: ");
    for (DatanodeInfo datanode : deadNodes.keySet()) {
      errMsgr.append(" ");
      errMsgr.append(datanode.toString());
    }
    if (ignoredNodes != null) {
      errMsgr.append(" Ignored nodes: ");
      for (DatanodeInfo datanode : ignoredNodes) {
        errMsgr.append(" ");
        errMsgr.append(datanode.toString());
      }
    }
    return errMsgr.toString();
  }

  protected void fetchBlockByteRange(LocatedBlock block, long start, long end,
      ByteBuffer buf, CorruptedBlocks corruptedBlocks)
      throws IOException {
    while (true) {
      DNAddrPair addressPair = chooseDataNode(block, null);
      // Latest block, if refreshed internally
      block = addressPair.block;
      try {
        actualGetFromOneDataNode(addressPair, start, end, buf,
            corruptedBlocks);
        return;
      } catch (IOException e) {
        checkInterrupted(e); // check if the read has been interrupted
        // Ignore other IOException. Already processed inside the function.
        // Loop through to try the next node.
      }
    }
  }

  private Callable<ByteBuffer> getFromOneDataNode(final DNAddrPair datanode,
      final LocatedBlock block, final long start, final long end,
      final ByteBuffer bb,
      final CorruptedBlocks corruptedBlocks,
      final int hedgedReadId) {
    final SpanId parentSpanId = Tracer.getCurrentSpanId();
    return new Callable<ByteBuffer>() {
      @Override
      public ByteBuffer call() throws Exception {
        DFSClientFaultInjector.get().sleepBeforeHedgedGet();
        try (TraceScope ignored = dfsClient.getTracer().
            newScope("hedgedRead" + hedgedReadId, parentSpanId)) {
          actualGetFromOneDataNode(datanode, start, end, bb, corruptedBlocks);
          return bb;
        }
      }
    };
  }

  /**
   * Read data from one DataNode.
   *
   * @param datanode          the datanode from which to read data
   * @param startInBlk        the startInBlk offset of the block
   * @param endInBlk          the endInBlk offset of the block
   * @param buf               the given byte buffer into which the data is read
   * @param corruptedBlocks   map recording list of datanodes with corrupted
   *                          block replica
   */
  void actualGetFromOneDataNode(final DNAddrPair datanode, final long startInBlk,
      final long endInBlk, ByteBuffer buf, CorruptedBlocks corruptedBlocks)
      throws IOException {
    DFSClientFaultInjector.get().startFetchFromDatanode();
    int refetchToken = 1; // only need to get a new access token once
    int refetchEncryptionKey = 1; // only need to get a new encryption key once
    final int len = (int) (endInBlk - startInBlk + 1);
    LocatedBlock block = datanode.block;
    while (true) {
      BlockReader reader = null;
      try {
        DFSClientFaultInjector.get().fetchFromDatanodeException();
        reader = getBlockReader(block, startInBlk, len, datanode.addr,
            datanode.storageType, datanode.info);

        //Behave exactly as the readAll() call
        ByteBuffer tmp = buf.duplicate();
        tmp.limit(tmp.position() + len);
        tmp = tmp.slice();
        int nread = 0;
        int ret;
        while (true) {
          ret = reader.read(tmp);
          if (ret <= 0) {
            break;
          }
          nread += ret;
        }
        buf.position(buf.position() + nread);

        IOUtilsClient.updateReadStatistics(readStatistics, nread, reader);
        dfsClient.updateFileSystemReadStats(
            reader.getNetworkDistance(), nread);
        if (nread != len) {
          throw new IOException("truncated return from reader.read(): " +
              "excpected " + len + ", got " + nread);
        }
        DFSClientFaultInjector.get().readFromDatanodeDelay();
        return;
      } catch (ChecksumException e) {
        String msg = "fetchBlockByteRange(). Got a checksum exception for "
            + src + " at " + block.getBlock() + ":" + e.getPos() + " from "
            + datanode.info;
        DFSClient.LOG.warn(msg);
        // we want to remember what we have tried
        corruptedBlocks.addCorruptedBlock(block.getBlock(), datanode.info);
        addToDeadNodes(datanode.info);
        throw new IOException(msg);
      } catch (IOException e) {
        checkInterrupted(e);
        if (e instanceof InvalidEncryptionKeyException && refetchEncryptionKey > 0) {
          DFSClient.LOG.info("Will fetch a new encryption key and retry, "
              + "encryption key was invalid when connecting to " + datanode.addr
              + " : " + e);
          // The encryption key used is invalid.
          refetchEncryptionKey--;
          dfsClient.clearDataEncryptionKey();
        } else if (refetchToken > 0 && tokenRefetchNeeded(e, datanode.addr)) {
          refetchToken--;
          try {
            fetchBlockAt(block.getStartOffset());
          } catch (IOException fbae) {
            // ignore IOE, since we can retry it later in a loop
          }
        } else {
          String msg = "Failed to connect to " + datanode.addr + " for file "
              + src + " for block " + block.getBlock() + ":" + e;
          DFSClient.LOG.warn("Connection failure: " + msg, e);
          addToDeadNodes(datanode.info);
          throw new IOException(msg);
        }
        // Refresh the block for updated tokens in case of token failures or
        // encryption key failures.
        block = refreshLocatedBlock(block);
      } finally {
        if (reader != null) {
          reader.close();
        }
      }
    }
  }

  /**
   * Refresh cached block locations.
   * @param block The currently cached block locations
   * @return Refreshed block locations
   * @throws IOException
   */
  protected LocatedBlock refreshLocatedBlock(LocatedBlock block)
      throws IOException {
    return getBlockAt(block.getStartOffset());
  }

  /**
   * Like {@link #fetchBlockByteRange}except we start up a second, parallel,
   * 'hedged' read if the first read is taking longer than configured amount of
   * time. We then wait on which ever read returns first.
   */
  private void hedgedFetchBlockByteRange(LocatedBlock block, long start,
      long end, ByteBuffer buf, CorruptedBlocks corruptedBlocks)
      throws IOException {
    final DfsClientConf conf = dfsClient.getConf();
    ArrayList<Future<ByteBuffer>> futures = new ArrayList<>();
    CompletionService<ByteBuffer> hedgedService =
        new ExecutorCompletionService<>(dfsClient.getHedgedReadsThreadPool());
    ArrayList<DatanodeInfo> ignored = new ArrayList<>();
    ByteBuffer bb;
    int len = (int) (end - start + 1);
    int hedgedReadId = 0;
    while (true) {
      // see HDFS-6591, this metric is used to verify/catch unnecessary loops
      hedgedReadOpsLoopNumForTesting++;
      DNAddrPair chosenNode = null;
      // there is no request already executing.
      if (futures.isEmpty()) {
        // chooseDataNode is a commitment. If no node, we go to
        // the NN to reget block locations. Only go here on first read.
        chosenNode = chooseDataNode(block, ignored);
        // Latest block, if refreshed internally
        block = chosenNode.block;
        bb = ByteBuffer.allocate(len);
        Callable<ByteBuffer> getFromDataNodeCallable = getFromOneDataNode(
            chosenNode, block, start, end, bb,
            corruptedBlocks, hedgedReadId++);
        Future<ByteBuffer> firstRequest = hedgedService
            .submit(getFromDataNodeCallable);
        futures.add(firstRequest);
        Future<ByteBuffer> future = null;
        try {
          future = hedgedService.poll(
              conf.getHedgedReadThresholdMillis(), TimeUnit.MILLISECONDS);
          if (future != null) {
            ByteBuffer result = future.get();
            result.flip();
            buf.put(result);
            return;
          }
          DFSClient.LOG.debug("Waited {}ms to read from {}; spawning hedged "
              + "read", conf.getHedgedReadThresholdMillis(), chosenNode.info);
          dfsClient.getHedgedReadMetrics().incHedgedReadOps();
          // continue; no need to refresh block locations
        } catch (ExecutionException e) {
          futures.remove(future);
        } catch (InterruptedException e) {
          throw new InterruptedIOException(
              "Interrupted while waiting for reading task");
        }
        // Ignore this node on next go around.
        // If poll timeout and the request still ongoing, don't consider it
        // again. If read data failed, don't consider it either.
        ignored.add(chosenNode.info);
      } else {
        // We are starting up a 'hedged' read. We have a read already
        // ongoing. Call getBestNodeDNAddrPair instead of chooseDataNode.
        // If no nodes to do hedged reads against, pass.
        boolean refetch = false;
        try {
          chosenNode = chooseDataNode(block, ignored, false);
          if (chosenNode != null) {
            // Latest block, if refreshed internally
            block = chosenNode.block;
            bb = ByteBuffer.allocate(len);
            Callable<ByteBuffer> getFromDataNodeCallable =
                getFromOneDataNode(chosenNode, block, start, end, bb,
                    corruptedBlocks, hedgedReadId++);
            Future<ByteBuffer> oneMoreRequest =
                hedgedService.submit(getFromDataNodeCallable);
            futures.add(oneMoreRequest);
          } else {
            refetch = true;
          }
        } catch (IOException ioe) {
          DFSClient.LOG.debug("Failed getting node for hedged read: {}",
              ioe.getMessage());
        }
        // if not succeeded. Submit callables for each datanode in a loop, wait
        // for a fixed interval and get the result from the fastest one.
        try {
          ByteBuffer result = getFirstToComplete(hedgedService, futures);
          // cancel the rest.
          cancelAll(futures);
          dfsClient.getHedgedReadMetrics().incHedgedReadWins();
          result.flip();
          buf.put(result);
          return;
        } catch (InterruptedException ie) {
          // Ignore and retry
        }
        if (refetch) {
          refetchLocations(block, ignored);
        }
        // We got here if exception. Ignore this node on next go around IFF
        // we found a chosenNode to hedge read against.
        if (chosenNode != null && chosenNode.info != null) {
          ignored.add(chosenNode.info);
        }
      }
    }
  }

  @VisibleForTesting
  public long getHedgedReadOpsLoopNumForTesting() {
    return hedgedReadOpsLoopNumForTesting;
  }

  private ByteBuffer getFirstToComplete(
      CompletionService<ByteBuffer> hedgedService,
      ArrayList<Future<ByteBuffer>> futures) throws InterruptedException {
    if (futures.isEmpty()) {
      throw new InterruptedException("let's retry");
    }
    Future<ByteBuffer> future = null;
    try {
      future = hedgedService.take();
      ByteBuffer bb = future.get();
      futures.remove(future);
      return bb;
    } catch (ExecutionException | CancellationException e) {
      // already logged in the Callable
      futures.remove(future);
    }

    throw new InterruptedException("let's retry");
  }

  private void cancelAll(List<Future<ByteBuffer>> futures) {
    for (Future<ByteBuffer> future : futures) {
      // Unfortunately, hdfs reads do not take kindly to interruption.
      // Threads return a variety of interrupted-type exceptions but
      // also complaints about invalid pbs -- likely because read
      // is interrupted before gets whole pb.  Also verbose WARN
      // logging.  So, for now, do not interrupt running read.
      future.cancel(false);
    }
  }

  /**
   * Should the block access token be refetched on an exception
   *
   * @param ex Exception received
   * @param targetAddr Target datanode address from where exception was received
   * @return true if block access token has expired or invalid and it should be
   *         refetched
   */
  protected static boolean tokenRefetchNeeded(IOException ex,
      InetSocketAddress targetAddr) {
    /*
     * Get a new access token and retry. Retry is needed in 2 cases. 1)
     * When both NN and DN re-started while DFSClient holding a cached
     * access token. 2) In the case that NN fails to update its
     * access key at pre-set interval (by a wide margin) and
     * subsequently restarts. In this case, DN re-registers itself with
     * NN and receives a new access key, but DN will delete the old
     * access key from its memory since it's considered expired based on
     * the estimated expiration date.
     */
    if (ex instanceof InvalidBlockTokenException ||
        ex instanceof InvalidToken) {
      DFSClient.LOG.debug(
          "Access token was invalid when connecting to {}: {}",
          targetAddr, ex);
      return true;
    }
    return false;
  }

  /**
   * Read bytes starting from the specified position.
   *
   * @param position start read from this position
   * @param buffer read buffer
   * @param offset offset into buffer
   * @param length number of bytes to read
   *
   * @return actual number of bytes read
   */
  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    validatePositionedReadArgs(position, buffer, offset, length);
    if (length == 0) {
      return 0;
    }
    try (TraceScope scope = dfsClient.
        newReaderTraceScope("DFSInputStream#byteArrayPread",
            src, position, length)) {
      ByteBuffer bb = ByteBuffer.wrap(buffer, offset, length);
      int retLen = pread(position, bb);
      if (retLen < length) {
        dfsClient.addRetLenToReaderScope(scope, retLen);
      }
      return retLen;
    }
  }

  private int pread(long position, ByteBuffer buffer)
      throws IOException {
    // sanity checks
    dfsClient.checkOpen();
    if (closed.get()) {
      throw new IOException("Stream closed");
    }
    failures = 0;
    long filelen = getFileLength();
    if ((position < 0) || (position >= filelen)) {
      return -1;
    }
    int length = buffer.remaining();
    int realLen = length;
    if ((position + length) > filelen) {
      realLen = (int)(filelen - position);
    }

    // determine the block and byte range within the block
    // corresponding to position and realLen
    List<LocatedBlock> blockRange = getBlockRange(position, realLen);
    int remaining = realLen;
    CorruptedBlocks corruptedBlocks = new CorruptedBlocks();
    for (LocatedBlock blk : blockRange) {
      long targetStart = position - blk.getStartOffset();
      int bytesToRead = (int) Math.min(remaining,
          blk.getBlockSize() - targetStart);
      long targetEnd = targetStart + bytesToRead - 1;
      try {
        if (dfsClient.isHedgedReadsEnabled() && !blk.isStriped()) {
          hedgedFetchBlockByteRange(blk, targetStart,
              targetEnd, buffer, corruptedBlocks);
        } else {
          fetchBlockByteRange(blk, targetStart, targetEnd,
              buffer, corruptedBlocks);
        }
      } finally {
        // Check and report if any block replicas are corrupted.
        // BlockMissingException may be caught if all block replicas are
        // corrupted.
        reportCheckSumFailure(corruptedBlocks, blk.getLocations().length,
            false);
      }

      remaining -= bytesToRead;
      position += bytesToRead;
    }
    assert remaining == 0 : "Wrong number of bytes read.";
    return realLen;
  }

  /**
   * DFSInputStream reports checksum failure.
   * For replicated blocks, we have the following logic:
   * Case I : client has tried multiple data nodes and at least one of the
   * attempts has succeeded. We report the other failures as corrupted block to
   * namenode.
   * Case II: client has tried out all data nodes, but all failed. We
   * only report if the total number of replica is 1. We do not
   * report otherwise since this maybe due to the client is a handicapped client
   * (who can not read).
   *
   * For erasure-coded blocks, each block in corruptedBlockMap is an internal
   * block in a block group, and there is usually only one DataNode
   * corresponding to each internal block. For this case we simply report the
   * corrupted blocks to NameNode and ignore the above logic.
   *
   * @param corruptedBlocks map of corrupted blocks
   * @param dataNodeCount number of data nodes who contains the block replicas
   */
  protected void reportCheckSumFailure(CorruptedBlocks corruptedBlocks,
      int dataNodeCount, boolean isStriped) {

    Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap =
        corruptedBlocks.getCorruptionMap();
    if (corruptedBlockMap.isEmpty()) {
      return;
    }
    List<LocatedBlock> reportList = new ArrayList<>(corruptedBlockMap.size());
    for (Map.Entry<ExtendedBlock, Set<DatanodeInfo>> entry :
        corruptedBlockMap.entrySet()) {
      ExtendedBlock blk = entry.getKey();
      Set<DatanodeInfo> dnSet = entry.getValue();
      if (isStriped || ((dnSet.size() < dataNodeCount) && (dnSet.size() > 0))
          || ((dataNodeCount == 1) && (dnSet.size() == dataNodeCount))) {
        DatanodeInfo[] locs = new DatanodeInfo[dnSet.size()];
        int i = 0;
        for (DatanodeInfo dn:dnSet) {
          locs[i++] = dn;
        }
        reportList.add(new LocatedBlock(blk, locs));
      }
    }
    if (reportList.size() > 0) {
      dfsClient.reportChecksumFailure(src,
          reportList.toArray(new LocatedBlock[reportList.size()]));
    }
    corruptedBlockMap.clear();
  }

  @Override
  public long skip(long n) throws IOException {
    if (n > 0) {
      long curPos = getPos();
      long fileLen = getFileLength();
      if (n+curPos > fileLen) {
        n = fileLen - curPos;
      }
      seek(curPos+n);
      return n;
    }
    return n < 0 ? -1 : 0;
  }

  /**
   * Seek to a new arbitrary location
   */
  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (targetPos > getFileLength()) {
      throw new EOFException("Cannot seek after EOF");
    }
    if (targetPos < 0) {
      throw new EOFException("Cannot seek to negative offset");
    }
    if (closed.get()) {
      throw new IOException("Stream is closed!");
    }
    boolean done = false;
    if (pos <= targetPos && targetPos <= blockEnd) {
      //
      // If this seek is to a positive position in the current
      // block, and this piece of data might already be lying in
      // the TCP buffer, then just eat up the intervening data.
      //
      int diff = (int)(targetPos - pos);
      if (diff <= blockReader.available()) {
        try {
          pos += blockReader.skip(diff);
          if (pos == targetPos) {
            done = true;
          } else {
            // The range was already checked. If the block reader returns
            // something unexpected instead of throwing an exception, it is
            // most likely a bug.
            String errMsg = "BlockReader failed to seek to " +
                targetPos + ". Instead, it seeked to " + pos + ".";
            DFSClient.LOG.warn(errMsg);
            throw new IOException(errMsg);
          }
        } catch (IOException e) {//make following read to retry
          DFSClient.LOG.debug("Exception while seek to {} from {} of {} from "
              + "{}", targetPos, getCurrentBlock(), src, currentNode, e);
          checkInterrupted(e);
        }
      }
    }
    if (!done) {
      pos = targetPos;
      blockEnd = -1;
    }
  }

  /**
   * Same as {@link #seekToNewSource(long)} except that it does not exclude
   * the current datanode and might connect to the same node.
   */
  private boolean seekToBlockSource(long targetPos)
                                                 throws IOException {
    currentNode = blockSeekTo(targetPos);
    return true;
  }

  /**
   * Seek to given position on a node other than the current node.  If
   * a node other than the current node is found, then returns true.
   * If another node could not be found, then returns false.
   */
  @Override
  public synchronized boolean seekToNewSource(long targetPos)
      throws IOException {
    if (currentNode == null) {
      return seekToBlockSource(targetPos);
    }
    boolean markedDead = deadNodes.containsKey(currentNode);
    addToDeadNodes(currentNode);
    DatanodeInfo oldNode = currentNode;
    DatanodeInfo newNode = blockSeekTo(targetPos);
    if (!markedDead) {
      /* remove it from deadNodes. blockSeekTo could have cleared
       * deadNodes and added currentNode again. Thats ok. */
      deadNodes.remove(oldNode);
    }
    if (!oldNode.getDatanodeUuid().equals(newNode.getDatanodeUuid())) {
      currentNode = newNode;
      return true;
    } else {
      return false;
    }
  }

  /**
   */
  @Override
  public synchronized long getPos() {
    return pos;
  }

  /** Return the size of the remaining available bytes
   * if the size is less than or equal to {@link Integer#MAX_VALUE},
   * otherwise, return {@link Integer#MAX_VALUE}.
   */
  @Override
  public synchronized int available() throws IOException {
    if (closed.get()) {
      throw new IOException("Stream closed");
    }

    final long remaining = getFileLength() - pos;
    return remaining <= Integer.MAX_VALUE? (int)remaining: Integer.MAX_VALUE;
  }

  /**
   * We definitely don't support marks
   */
  @Override
  public boolean markSupported() {
    return false;
  }
  @Override
  public void mark(int readLimit) {
  }
  @Override
  public void reset() throws IOException {
    throw new IOException("Mark/reset not supported");
  }

  /** Utility class to encapsulate data node info and its address. */
  static final class DNAddrPair {
    final DatanodeInfo info;
    final InetSocketAddress addr;
    final StorageType storageType;
    final LocatedBlock block;

    DNAddrPair(DatanodeInfo info, InetSocketAddress addr,
        StorageType storageType, LocatedBlock block) {
      this.info = info;
      this.addr = addr;
      this.storageType = storageType;
      this.block = block;
    }
  }

  /**
   * Get statistics about the reads which this DFSInputStream has done.
   */
  public ReadStatistics getReadStatistics() {
    return readStatistics;
  }

  /**
   * Clear statistics about the reads which this DFSInputStream has done.
   */
  public void clearReadStatistics() {
    readStatistics.clear();
  }

  public FileEncryptionInfo getFileEncryptionInfo() {
    synchronized(infoLock) {
      return fileEncryptionInfo;
    }
  }

  protected void closeCurrentBlockReaders() {
    if (blockReader == null) return;
    // Close the current block reader so that the new caching settings can
    // take effect immediately.
    try {
      blockReader.close();
    } catch (IOException e) {
      DFSClient.LOG.error("error closing blockReader", e);
    }
    blockReader = null;
    blockEnd = -1;
  }

  @Override
  public synchronized void setReadahead(Long readahead)
      throws IOException {
    synchronized (infoLock) {
      this.cachingStrategy =
          new CachingStrategy.Builder(this.cachingStrategy).
              setReadahead(readahead).build();
    }
    closeCurrentBlockReaders();
  }

  @Override
  public synchronized void setDropBehind(Boolean dropBehind)
      throws IOException {
    synchronized (infoLock) {
      this.cachingStrategy =
          new CachingStrategy.Builder(this.cachingStrategy).
              setDropBehind(dropBehind).build();
    }
    closeCurrentBlockReaders();
  }

  /**
   * The immutable empty buffer we return when we reach EOF when doing a
   * zero-copy read.
   */
  private static final ByteBuffer EMPTY_BUFFER =
      ByteBuffer.allocateDirect(0).asReadOnlyBuffer();

  @Override
  public synchronized ByteBuffer read(ByteBufferPool bufferPool,
      int maxLength, EnumSet<ReadOption> opts)
          throws IOException, UnsupportedOperationException {
    if (maxLength == 0) {
      return EMPTY_BUFFER;
    } else if (maxLength < 0) {
      throw new IllegalArgumentException("can't read a negative " +
          "number of bytes.");
    }
    if ((blockReader == null) || (blockEnd == -1)) {
      if (pos >= getFileLength()) {
        return null;
      }
      /*
       * If we don't have a blockReader, or the one we have has no more bytes
       * left to read, we call seekToBlockSource to get a new blockReader and
       * recalculate blockEnd.  Note that we assume we're not at EOF here
       * (we check this above).
       */
      if ((!seekToBlockSource(pos)) || (blockReader == null)) {
        throw new IOException("failed to allocate new BlockReader " +
            "at position " + pos);
      }
    }
    ByteBuffer buffer = null;
    if (dfsClient.getConf().getShortCircuitConf().isShortCircuitMmapEnabled()) {
      buffer = tryReadZeroCopy(maxLength, opts);
    }
    if (buffer != null) {
      return buffer;
    }
    buffer = ByteBufferUtil.fallbackRead(this, bufferPool, maxLength);
    if (buffer != null) {
      getExtendedReadBuffers().put(buffer, bufferPool);
    }
    return buffer;
  }

  private synchronized ByteBuffer tryReadZeroCopy(int maxLength,
      EnumSet<ReadOption> opts) throws IOException {
    // Copy 'pos' and 'blockEnd' to local variables to make it easier for the
    // JVM to optimize this function.
    final long curPos = pos;
    final long curEnd = blockEnd;
    final long blockStartInFile = currentLocatedBlock.getStartOffset();
    final long blockPos = curPos - blockStartInFile;

    // Shorten this read if the end of the block is nearby.
    long length63;
    if ((curPos + maxLength) <= (curEnd + 1)) {
      length63 = maxLength;
    } else {
      length63 = 1 + curEnd - curPos;
      if (length63 <= 0) {
        DFSClient.LOG.debug("Unable to perform a zero-copy read from offset {}"
                + " of {}; {} bytes left in block. blockPos={}; curPos={};"
                + "curEnd={}",
            curPos, src, length63, blockPos, curPos, curEnd);
        return null;
      }
      DFSClient.LOG.debug("Reducing read length from {} to {} to avoid going "
              + "more than one byte past the end of the block.  blockPos={}; "
              +" curPos={}; curEnd={}",
          maxLength, length63, blockPos, curPos, curEnd);
    }
    // Make sure that don't go beyond 31-bit offsets in the MappedByteBuffer.
    int length;
    if (blockPos + length63 <= Integer.MAX_VALUE) {
      length = (int)length63;
    } else {
      long length31 = Integer.MAX_VALUE - blockPos;
      if (length31 <= 0) {
        // Java ByteBuffers can't be longer than 2 GB, because they use
        // 4-byte signed integers to represent capacity, etc.
        // So we can't mmap the parts of the block higher than the 2 GB offset.
        // FIXME: we could work around this with multiple memory maps.
        // See HDFS-5101.
        DFSClient.LOG.debug("Unable to perform a zero-copy read from offset {} "
            + " of {}; 31-bit MappedByteBuffer limit exceeded.  blockPos={}, "
            + "curEnd={}", curPos, src, blockPos, curEnd);
        return null;
      }
      length = (int)length31;
      DFSClient.LOG.debug("Reducing read length from {} to {} to avoid 31-bit "
          + "limit.  blockPos={}; curPos={}; curEnd={}",
          maxLength, length, blockPos, curPos, curEnd);
    }
    final ClientMmap clientMmap = blockReader.getClientMmap(opts);
    if (clientMmap == null) {
      DFSClient.LOG.debug("unable to perform a zero-copy read from offset {} of"
          + " {}; BlockReader#getClientMmap returned null.", curPos, src);
      return null;
    }
    boolean success = false;
    ByteBuffer buffer;
    try {
      seek(curPos + length);
      buffer = clientMmap.getMappedByteBuffer().asReadOnlyBuffer();
      buffer.position((int)blockPos);
      buffer.limit((int)(blockPos + length));
      getExtendedReadBuffers().put(buffer, clientMmap);
      readStatistics.addZeroCopyBytes(length);
      DFSClient.LOG.debug("readZeroCopy read {} bytes from offset {} via the "
          + "zero-copy read path.  blockEnd = {}", length, curPos, blockEnd);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeQuietly(clientMmap);
      }
    }
    return buffer;
  }

  @Override
  public synchronized void releaseBuffer(ByteBuffer buffer) {
    if (buffer == EMPTY_BUFFER) return;
    Object val = getExtendedReadBuffers().remove(buffer);
    if (val == null) {
      throw new IllegalArgumentException("tried to release a buffer " +
          "that was not created by this stream, " + buffer);
    }
    if (val instanceof ClientMmap) {
      IOUtils.closeQuietly((ClientMmap)val);
    } else if (val instanceof ByteBufferPool) {
      ((ByteBufferPool)val).putBuffer(buffer);
    }
  }

  @Override
  public synchronized void unbuffer() {
    closeCurrentBlockReaders();
  }

  @Override
  public boolean hasCapability(String capability) {
    switch (StringUtils.toLowerCase(capability)) {
    case StreamCapabilities.READAHEAD:
    case StreamCapabilities.DROPBEHIND:
    case StreamCapabilities.UNBUFFER:
      return true;
    default:
      return false;
    }
  }
}
