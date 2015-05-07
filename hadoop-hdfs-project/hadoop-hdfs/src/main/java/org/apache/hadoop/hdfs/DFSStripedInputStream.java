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

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.ReadPortion;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.planReadPortions;

import org.apache.hadoop.net.NetUtils;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;


/******************************************************************************
 * DFSStripedInputStream reads from striped block groups, illustrated below:
 *
 * | <- Striped Block Group -> |
 *  blk_0      blk_1       blk_2   <- A striped block group has
 *    |          |           |          {@link #dataBlkNum} blocks
 *    v          v           v
 * +------+   +------+   +------+
 * |cell_0|   |cell_1|   |cell_2|  <- The logical read order should be
 * +------+   +------+   +------+       cell_0, cell_1, ...
 * |cell_3|   |cell_4|   |cell_5|
 * +------+   +------+   +------+
 * |cell_6|   |cell_7|   |cell_8|
 * +------+   +------+   +------+
 * |cell_9|
 * +------+  <- A cell contains {@link #cellSize} bytes of data
 *
 * Three styles of read will eventually be supported:
 *   1. Stateful read
 *   2. pread without decode support
 *     This is implemented by calculating the portion of read from each block and
 *     issuing requests to each DataNode in parallel.
 *   3. pread with decode support: TODO: will be supported after HDFS-7678
 *****************************************************************************/
public class DFSStripedInputStream extends DFSInputStream {

  private static class ReaderRetryPolicy {
    private int fetchEncryptionKeyTimes = 1;
    private int fetchTokenTimes = 1;

    void refetchEncryptionKey() {
      fetchEncryptionKeyTimes--;
    }

    void refetchToken() {
      fetchTokenTimes--;
    }

    boolean shouldRefetchEncryptionKey() {
      return fetchEncryptionKeyTimes > 0;
    }

    boolean shouldRefetchToken() {
      return fetchTokenTimes > 0;
    }
  }

  /** Used to indicate the buffered data's range in the block group */
  private static class StripeRange {
    /** start offset in the block group (inclusive) */
    final long offsetInBlock;
    /** length of the stripe range */
    final long length;

    StripeRange(long offsetInBlock, long length) {
      Preconditions.checkArgument(offsetInBlock >= 0 && length >= 0);
      this.offsetInBlock = offsetInBlock;
      this.length = length;
    }

    boolean include(long pos) {
      return pos >= offsetInBlock && pos < offsetInBlock + length;
    }
  }

  private final short groupSize = HdfsConstants.NUM_DATA_BLOCKS;
  private final BlockReader[] blockReaders = new BlockReader[groupSize];
  private final DatanodeInfo[] currentNodes = new DatanodeInfo[groupSize];
  private final int cellSize;
  private final short dataBlkNum;
  private final short parityBlkNum;
  /** the buffer for a complete stripe */
  private ByteBuffer curStripeBuf;
  /**
   * indicate the start/end offset of the current buffered stripe in the
   * block group
   */
  private StripeRange curStripeRange;
  private final CompletionService<Integer> readingService;

  DFSStripedInputStream(DFSClient dfsClient, String src, boolean verifyChecksum,
      ErasureCodingInfo ecInfo) throws IOException {
    super(dfsClient, src, verifyChecksum);
    // ECInfo is restored from NN just before reading striped file.
    assert ecInfo != null;
    cellSize = ecInfo.getSchema().getChunkSize();
    dataBlkNum = (short) ecInfo.getSchema().getNumDataUnits();
    parityBlkNum = (short) ecInfo.getSchema().getNumParityUnits();
    curStripeRange = new StripeRange(0, 0);
    readingService =
        new ExecutorCompletionService<>(dfsClient.getStripedReadsThreadPool());
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Creating an striped input stream for file " + src);
    }
  }

  private void resetCurStripeBuffer() {
    if (curStripeBuf == null) {
      curStripeBuf = ByteBuffer.allocateDirect(cellSize * dataBlkNum);
    }
    curStripeBuf.clear();
    curStripeRange = new StripeRange(0, 0);
  }

  @Override
  public synchronized int read(final ByteBuffer buf) throws IOException {
    ReaderStrategy byteBufferReader = new ByteBufferStrategy(buf);
    TraceScope scope =
        dfsClient.getPathTraceScope("DFSInputStream#byteBufferRead", src);
    try {
      return readWithStrategy(byteBufferReader, 0, buf.remaining());
    } finally {
      scope.close();
    }
  }

  /**
   * When seeking into a new block group, create blockReader for each internal
   * block in the group.
   */
  private synchronized void blockSeekTo(long target) throws IOException {
    if (target >= getFileLength()) {
      throw new IOException("Attempted to read past end of file");
    }

    // Will be getting a new BlockReader.
    closeCurrentBlockReaders();

    // Compute desired striped block group
    LocatedStripedBlock targetBlockGroup = getBlockGroupAt(target);
    // Update current position
    this.pos = target;
    this.blockEnd = targetBlockGroup.getStartOffset() +
        targetBlockGroup.getBlockSize() - 1;
    currentLocatedBlock = targetBlockGroup;

    final long offsetIntoBlockGroup = getOffsetInBlockGroup();
    LocatedBlock[] targetBlocks = StripedBlockUtil.parseStripedBlockGroup(
        targetBlockGroup, cellSize, dataBlkNum, parityBlkNum);
    // The purpose is to get start offset into each block
    ReadPortion[] readPortions = planReadPortions(groupSize, cellSize,
        offsetIntoBlockGroup, 0, 0);

    final ReaderRetryPolicy retry = new ReaderRetryPolicy();
    for (int i = 0; i < groupSize; i++) {
      LocatedBlock targetBlock = targetBlocks[i];
      if (targetBlock != null) {
        DNAddrPair retval = getBestNodeDNAddrPair(targetBlock, null);
        if (retval != null) {
          currentNodes[i] = retval.info;
          blockReaders[i] = getBlockReaderWithRetry(targetBlock,
              readPortions[i].getStartOffsetInBlock(),
              targetBlock.getBlockSize() - readPortions[i].getStartOffsetInBlock(),
              retval.addr, retval.storageType, retval.info, target, retry);
        }
      }
    }
  }

  private BlockReader getBlockReaderWithRetry(LocatedBlock targetBlock,
      long offsetInBlock, long length, InetSocketAddress targetAddr,
      StorageType storageType, DatanodeInfo datanode, long offsetInFile,
      ReaderRetryPolicy retry) throws IOException {
    // only need to get a new access token or a new encryption key once
    while (true) {
      try {
        return getBlockReader(targetBlock, offsetInBlock, length, targetAddr,
            storageType, datanode);
      } catch (IOException e) {
        if (e instanceof InvalidEncryptionKeyException &&
            retry.shouldRefetchEncryptionKey()) {
          DFSClient.LOG.info("Will fetch a new encryption key and retry, "
              + "encryption key was invalid when connecting to " + targetAddr
              + " : " + e);
          dfsClient.clearDataEncryptionKey();
          retry.refetchEncryptionKey();
        } else if (retry.shouldRefetchToken() &&
            tokenRefetchNeeded(e, targetAddr)) {
          fetchBlockAt(offsetInFile);
          retry.refetchToken();
        } else {
          DFSClient.LOG.warn("Failed to connect to " + targetAddr + " for block"
              + ", add to deadNodes and continue.", e);
          // Put chosen node into dead list, continue
          addToDeadNodes(datanode);
          return null;
        }
      }
    }
  }

  /**
   * Extend the super method with the logic of switching between cells.
   * When reaching the end of a cell, proceed to the next cell and read it
   * with the next blockReader.
   */
  @Override
  protected void closeCurrentBlockReaders() {
    resetCurStripeBuffer();
    if (blockReaders ==  null || blockReaders.length == 0) {
      return;
    }
    for (int i = 0; i < groupSize; i++) {
      if (blockReaders[i] != null) {
        try {
          blockReaders[i].close();
        } catch (IOException e) {
          DFSClient.LOG.error("error closing blockReader", e);
        }
        blockReaders[i] = null;
      }
      currentNodes[i] = null;
    }
    blockEnd = -1;
  }

  private long getOffsetInBlockGroup() {
    return pos - currentLocatedBlock.getStartOffset();
  }

  /**
   * Read a new stripe covering the current position, and store the data in the
   * {@link #curStripeBuf}.
   */
  private void readOneStripe(
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    resetCurStripeBuffer();

    // compute stripe range based on pos
    final long offsetInBlockGroup = getOffsetInBlockGroup();
    final long stripeLen = cellSize * dataBlkNum;
    int stripeIndex = (int) (offsetInBlockGroup / stripeLen);
    curStripeRange = new StripeRange(stripeIndex * stripeLen,
        Math.min(currentLocatedBlock.getBlockSize() - (stripeIndex * stripeLen),
            stripeLen));
    final int numCell = (int) ((curStripeRange.length - 1) / cellSize + 1);

    // read the whole stripe in parallel
    Map<Future<Integer>, Integer> futures = new HashMap<>();
    for (int i = 0; i < numCell; i++) {
      curStripeBuf.position(cellSize * i);
      curStripeBuf.limit((int) Math.min(cellSize * (i + 1),
          curStripeRange.length));
      ByteBuffer buf = curStripeBuf.slice();
      ByteBufferStrategy strategy = new ByteBufferStrategy(buf);
      final int targetLength = buf.remaining();
      Callable<Integer> readCallable = readCell(blockReaders[i],
          currentNodes[i], strategy, targetLength, corruptedBlockMap);
      Future<Integer> request = readingService.submit(readCallable);
      futures.put(request, i);
    }
    while (!futures.isEmpty()) {
      try {
        waitNextCompletion(readingService, futures);
        // TODO: decode and record bad reader if necessary
      } catch (InterruptedException ignored) {
        // ignore and retry
      }
    }
  }

  private Callable<Integer> readCell(final BlockReader reader,
      final DatanodeInfo datanode, final ByteBufferStrategy strategy,
      final int targetLength,
      final Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap) {
    return new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        int result = 0;
        while (result < targetLength) {
          int ret = readBuffer(reader, datanode, strategy, corruptedBlockMap);
          if (ret < 0) {
            throw new IOException("Unexpected EOS from the reader");
          }
          result += ret;
        }
        updateReadStatistics(readStatistics, targetLength, reader);
        return result;
      }
    };
  }

  @Override
  protected synchronized int readWithStrategy(ReaderStrategy strategy,
      int off, int len) throws IOException {
    dfsClient.checkOpen();
    if (closed.get()) {
      throw new IOException("Stream closed");
    }
    Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap =
        new ConcurrentHashMap<>();
    failures = 0;
    if (pos < getFileLength()) {
      try {
        if (pos > blockEnd) {
          blockSeekTo(pos);
        }
        int realLen = (int) Math.min(len, (blockEnd - pos + 1L));
        synchronized (infoLock) {
          if (locatedBlocks.isLastBlockComplete()) {
            realLen = (int) Math.min(realLen,
                locatedBlocks.getFileLength() - pos);
          }
        }

        /** Number of bytes already read into buffer */
        int result = 0;
        while (result < realLen) {
          if (!curStripeRange.include(getOffsetInBlockGroup())) {
            readOneStripe(corruptedBlockMap);
          }
          int ret = copy(strategy, off + result, realLen - result);
          result += ret;
          pos += ret;
        }
        if (dfsClient.stats != null) {
          dfsClient.stats.incrementBytesRead(result);
        }
        return result;
      } finally {
        // Check if need to report block replicas corruption either read
        // was successful or ChecksumException occured.
        reportCheckSumFailure(corruptedBlockMap,
            currentLocatedBlock.getLocations().length);
      }
    }
    return -1;
  }

  private int readBuffer(BlockReader blockReader,
      DatanodeInfo currentNode, ByteBufferStrategy readerStrategy,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap) {
    try {
      return readerStrategy.doRead(blockReader, 0, 0);
    } catch ( ChecksumException ce ) {
      DFSClient.LOG.warn("Found Checksum error for "
          + getCurrentBlock() + " from " + currentNode
          + " at " + ce.getPos());
      // we want to remember which block replicas we have tried
      addIntoCorruptedBlockMap(getCurrentBlock(), currentNode,
          corruptedBlockMap);
    } catch (IOException e) {
      DFSClient.LOG.warn("Exception while reading from "
          + getCurrentBlock() + " of " + src + " from "
          + currentNode, e);
    }
    return -1;
  }

  /**
   * Copy the data from {@link #curStripeBuf} into the given buffer
   * @param strategy the ReaderStrategy containing the given buffer
   * @param offset the offset of the given buffer. Used only when strategy is
   *               a ByteArrayStrategy
   * @param length target length
   * @return number of bytes copied
   */
  private int copy(ReaderStrategy strategy, int offset, int length) {
    final long stripeLen = cellSize * dataBlkNum;
    final long offsetInBlk = pos - currentLocatedBlock.getStartOffset();
    // compute the position in the curStripeBuf based on "pos"
    int bufOffset = (int) (offsetInBlk % stripeLen);
    curStripeBuf.position(bufOffset);
    return strategy.copyFrom(curStripeBuf, offset,
        Math.min(length, curStripeBuf.remaining()));
  }

  /**
   * | <--------- LocatedStripedBlock (ID = 0) ---------> |
   * LocatedBlock (0) | LocatedBlock (1) | LocatedBlock (2)
   *                      ^
   *                    offset
   * On a striped file, the super method {@link DFSInputStream#getBlockAt}
   * treats a striped block group as a single {@link LocatedBlock} object,
   * which includes target in its range. This method adds the logic of:
   *   1. Analyzing the index of required block based on offset
   *   2. Parsing the block group to obtain the block location on that index
   */
  @Override
  protected LocatedBlock getBlockAt(long blkStartOffset) throws IOException {
    LocatedBlock lb = super.getBlockAt(blkStartOffset);
    assert lb instanceof LocatedStripedBlock : "NameNode should return a " +
        "LocatedStripedBlock for a striped file";

    int idx = (int) (((blkStartOffset - lb.getStartOffset()) / cellSize)
        % dataBlkNum);
    // If indexing information is returned, iterate through the index array
    // to find the entry for position idx in the group
    LocatedStripedBlock lsb = (LocatedStripedBlock) lb;
    int i = 0;
    for (; i < lsb.getBlockIndices().length; i++) {
      if (lsb.getBlockIndices()[i] == idx) {
        break;
      }
    }
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("getBlockAt for striped blocks, offset="
          + blkStartOffset + ". Obtained block " + lb + ", idx=" + idx);
    }
    return StripedBlockUtil.constructInternalBlock(lsb, i, cellSize, dataBlkNum, idx);
  }

  private LocatedStripedBlock getBlockGroupAt(long offset) throws IOException {
    LocatedBlock lb = super.getBlockAt(offset);
    assert lb instanceof LocatedStripedBlock : "NameNode" +
        " should return a LocatedStripedBlock for a striped file";
    return (LocatedStripedBlock)lb;
  }

  /**
   * Real implementation of pread.
   */
  @Override
  protected void fetchBlockByteRange(long blockStartOffset, long start,
      long end, byte[] buf, int offset,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    Map<Future<Void>, Integer> futures = new HashMap<>();
    CompletionService<Void> stripedReadsService =
        new ExecutorCompletionService<>(dfsClient.getStripedReadsThreadPool());
    int len = (int) (end - start + 1);

    // Refresh the striped block group
    LocatedStripedBlock blockGroup = getBlockGroupAt(blockStartOffset);


    // Planning the portion of I/O for each shard
    ReadPortion[] readPortions = planReadPortions(dataBlkNum, cellSize, start,
        len, offset);

    // Parse group to get chosen DN location
    LocatedBlock[] blks = StripedBlockUtil.
        parseStripedBlockGroup(blockGroup, cellSize, dataBlkNum, parityBlkNum);

    for (short i = 0; i < dataBlkNum; i++) {
      ReadPortion rp = readPortions[i];
      if (rp.getReadLength() <= 0) {
        continue;
      }
      DatanodeInfo loc = blks[i].getLocations()[0];
      StorageType type = blks[i].getStorageTypes()[0];
      DNAddrPair dnAddr = new DNAddrPair(loc, NetUtils.createSocketAddr(
          loc.getXferAddr(dfsClient.getConf().isConnectToDnViaHostname())),
          type);
      Callable<Void> readCallable = getFromOneDataNode(dnAddr,
          blks[i].getStartOffset(), rp.getStartOffsetInBlock(),
          rp.getStartOffsetInBlock() + rp.getReadLength() - 1, buf,
          rp.getOffsets(), rp.getLengths(), corruptedBlockMap, i);
      Future<Void> getFromDNRequest = stripedReadsService.submit(readCallable);
      DFSClient.LOG.debug("Submitting striped read request for " + blks[i]);
      futures.put(getFromDNRequest, (int) i);
    }
    while (!futures.isEmpty()) {
      try {
        waitNextCompletion(stripedReadsService, futures);
      } catch (InterruptedException ie) {
        // Ignore and retry
      }
    }
  }

  private Callable<Void> getFromOneDataNode(final DNAddrPair datanode,
      final long blockStartOffset, final long start, final long end,
      final byte[] buf, final int[] offsets, final int[] lengths,
      final Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap,
      final int hedgedReadId) {
    final Span parentSpan = Trace.currentSpan();
    return new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        TraceScope scope =
            Trace.startSpan("Parallel reading " + hedgedReadId, parentSpan);
        try {
          actualGetFromOneDataNode(datanode, blockStartOffset, start,
              end, buf, offsets, lengths, corruptedBlockMap);
        } finally {
          scope.close();
        }
        return null;
      }
    };
  }

  private <T> void waitNextCompletion(CompletionService<T> service,
      Map<Future<T>, Integer> futures) throws InterruptedException {
    if (futures.isEmpty()) {
      throw new InterruptedException("Futures already empty");
    }
    Future<T> future = null;
    try {
      future = service.take();
      future.get();
      futures.remove(future);
    } catch (ExecutionException | CancellationException e) {
      // already logged in the Callable
      futures.remove(future);
    }
    throw new InterruptedException("let's retry");
  }
}
