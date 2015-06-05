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
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.ByteBufferPool;

import static org.apache.hadoop.hdfs.util.StripedBlockUtil.convertIndex4Decode;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.divideByteRangeIntoStripes;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.finalizeDecodeInputs;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.decodeAndFillBuffer;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.getNextCompletedStripedRead;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.getStartOffsetsForInternalBlocks;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.initDecodeInputs;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.parseStripedBlockGroup;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.AlignedStripe;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.StripingChunk;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.StripingChunkReadResult;

import org.apache.hadoop.io.erasurecode.ECSchema;

import org.apache.hadoop.io.erasurecode.rawcoder.RSRawDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.net.NetUtils;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * DFSStripedInputStream reads from striped block groups
 */
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

  private final BlockReader[] blockReaders;
  /**
   * when initializing block readers, their starting offsets are set to the same
   * number: the smallest internal block offsets among all the readers. This is
   * because it is possible that for some internal blocks we have to read
   * "backwards" for decoding purpose. We thus use this offset array to track
   * offsets for all the block readers so that we can skip data if necessary.
   */
  private final long[] blockReaderOffsets;
  private final DatanodeInfo[] currentNodes;
  private final int cellSize;
  private final short dataBlkNum;
  private final short parityBlkNum;
  private final int groupSize;
  /** the buffer for a complete stripe */
  private ByteBuffer curStripeBuf;
  private final ECSchema schema;
  private final RawErasureDecoder decoder;

  /**
   * indicate the start/end offset of the current buffered stripe in the
   * block group
   */
  private StripeRange curStripeRange;
  private final CompletionService<Void> readingService;
  private ReaderRetryPolicy retry;

  DFSStripedInputStream(DFSClient dfsClient, String src, boolean verifyChecksum,
      ECSchema schema, int cellSize) throws IOException {
    super(dfsClient, src, verifyChecksum);

    assert schema != null;
    this.schema = schema;
    this.cellSize = cellSize;
    dataBlkNum = (short) schema.getNumDataUnits();
    parityBlkNum = (short) schema.getNumParityUnits();
    groupSize = dataBlkNum + parityBlkNum;
    blockReaders = new BlockReader[groupSize];
    blockReaderOffsets = new long[groupSize];
    currentNodes = new DatanodeInfo[groupSize];
    curStripeRange = new StripeRange(0, 0);
    readingService =
        new ExecutorCompletionService<>(dfsClient.getStripedReadsThreadPool());
    decoder = new RSRawDecoder(dataBlkNum, parityBlkNum);
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
    LocatedBlock[] targetBlocks = parseStripedBlockGroup(
        targetBlockGroup, cellSize, dataBlkNum, parityBlkNum);
    // The purpose is to get start offset into each block.
    long[] offsetsForInternalBlocks = getStartOffsetsForInternalBlocks(schema,
        cellSize, targetBlockGroup, offsetIntoBlockGroup);
    Preconditions.checkState(
        offsetsForInternalBlocks.length == dataBlkNum + parityBlkNum);
    long minOffset = offsetsForInternalBlocks[dataBlkNum];

    retry = new ReaderRetryPolicy();
    for (int i = 0; i < dataBlkNum; i++) {
      LocatedBlock targetBlock = targetBlocks[i];
      if (targetBlock != null) {
        DNAddrPair retval = getBestNodeDNAddrPair(targetBlock, null);
        if (retval != null) {
          currentNodes[i] = retval.info;
          blockReaders[i] = getBlockReaderWithRetry(targetBlock,
              minOffset, targetBlock.getBlockSize() - minOffset,
              retval.addr, retval.storageType, retval.info, target, retry);
          blockReaderOffsets[i] = minOffset;
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
      closeReader(i);
      currentNodes[i] = null;
    }
    blockEnd = -1;
  }

  private void closeReader(int index) {
    if (blockReaders[index] != null) {
      try {
        blockReaders[index].close();
      } catch (IOException e) {
        DFSClient.LOG.error("error closing blockReader " + index, e);
      }
      blockReaders[index] = null;
    }
    blockReaderOffsets[index] = 0;
  }

  private long getOffsetInBlockGroup() {
    return getOffsetInBlockGroup(pos);
  }

  private long getOffsetInBlockGroup(long pos) {
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
    final int stripeIndex = (int) (offsetInBlockGroup / stripeLen);
    final int stripeBufOffset = (int) (offsetInBlockGroup % stripeLen);
    final int stripeLimit = (int) Math.min(currentLocatedBlock.getBlockSize()
        - (stripeIndex * stripeLen), stripeLen);
    curStripeRange = new StripeRange(offsetInBlockGroup,
        stripeLimit - stripeBufOffset);

    LocatedStripedBlock blockGroup = (LocatedStripedBlock) currentLocatedBlock;
    AlignedStripe[] stripes = StripedBlockUtil.divideOneStripe(schema, cellSize,
        blockGroup, offsetInBlockGroup,
        offsetInBlockGroup + curStripeRange.length - 1, curStripeBuf);
    // TODO handle null elements in blks (e.g., NN does not know locations for
    // all the internal blocks)
    final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(
        blockGroup, cellSize, dataBlkNum, parityBlkNum);
    // read the whole stripe
    for (AlignedStripe stripe : stripes) {
      // Parse group to get chosen DN location
      StripeReader sreader = new StatefulStripeReader(readingService, stripe,
          blks);
      sreader.readStripe(blks, corruptedBlockMap);
    }
    curStripeBuf.position(stripeBufOffset);
    curStripeBuf.limit(stripeLimit);
  }

  private Callable<Void> readCell(final BlockReader reader,
      final DatanodeInfo datanode, final long currentReaderOffset,
      final long targetReaderOffset, final ByteBufferStrategy strategy,
      final int targetLength,
      final Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap) {
    return new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        // reader can be null if getBlockReaderWithRetry failed or
        // the reader hit exception before
        if (reader == null) {
          throw new IOException("The BlockReader is null. " +
              "The BlockReader creation failed or the reader hit exception.");
        }
        Preconditions.checkState(currentReaderOffset <= targetReaderOffset);
        if (currentReaderOffset < targetReaderOffset) {
          long skipped = reader.skip(targetReaderOffset - currentReaderOffset);
          Preconditions.checkState(
              skipped == targetReaderOffset - currentReaderOffset);
        }
        int result = 0;
        while (result < targetLength) {
          int ret = readToBuffer(reader, datanode, strategy, corruptedBlockMap);
          if (ret < 0) {
            throw new IOException("Unexpected EOS from the reader");
          }
          result += ret;
        }
        updateReadStatistics(readStatistics, targetLength, reader);
        return null;
      }
    };
  }

  private int readToBuffer(BlockReader blockReader,
      DatanodeInfo currentNode, ByteBufferStrategy readerStrategy,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    try {
      return readerStrategy.doRead(blockReader, 0, 0);
    } catch (ChecksumException ce) {
      DFSClient.LOG.warn("Found Checksum error for "
          + getCurrentBlock() + " from " + currentNode
          + " at " + ce.getPos());
      // we want to remember which block replicas we have tried
      addIntoCorruptedBlockMap(getCurrentBlock(), currentNode,
          corruptedBlockMap);
      throw ce;
    } catch (IOException e) {
      DFSClient.LOG.warn("Exception while reading from "
          + getCurrentBlock() + " of " + src + " from "
          + currentNode, e);
      throw e;
    }
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
    if (targetPos <= blockEnd) {
      final long targetOffsetInBlk = getOffsetInBlockGroup(targetPos);
      if (curStripeRange.include(targetOffsetInBlk)) {
        int bufOffset = getStripedBufOffset(targetOffsetInBlk);
        curStripeBuf.position(bufOffset);
        pos = targetPos;
        return;
      }
    }
    pos = targetPos;
    blockEnd = -1;
  }

  private int getStripedBufOffset(long offsetInBlockGroup) {
    final long stripeLen = cellSize * dataBlkNum;
    // compute the position in the curStripeBuf based on "pos"
    return (int) (offsetInBlockGroup % stripeLen);
  }

  @Override
  public synchronized boolean seekToNewSource(long targetPos)
      throws IOException {
    return false;
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
          int ret = copyToTargetBuf(strategy, off + result, realLen - result);
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

  /**
   * Copy the data from {@link #curStripeBuf} into the given buffer
   * @param strategy the ReaderStrategy containing the given buffer
   * @param offset the offset of the given buffer. Used only when strategy is
   *               a ByteArrayStrategy
   * @param length target length
   * @return number of bytes copied
   */
  private int copyToTargetBuf(ReaderStrategy strategy, int offset, int length) {
    final long offsetInBlk = getOffsetInBlockGroup();
    int bufOffset = getStripedBufOffset(offsetInBlk);
    curStripeBuf.position(bufOffset);
    return strategy.copyFrom(curStripeBuf, offset,
        Math.min(length, curStripeBuf.remaining()));
  }

  /**
   * The super method {@link DFSInputStream#refreshLocatedBlock} refreshes
   * cached LocatedBlock by executing {@link DFSInputStream#getBlockAt} again.
   * This method extends the logic by first remembering the index of the
   * internal block, and re-parsing the refreshed block group with the same
   * index.
   */
  @Override
  protected LocatedBlock refreshLocatedBlock(LocatedBlock block)
      throws IOException {
    int idx = BlockIdManager.getBlockIndex(block.getBlock().getLocalBlock());
    LocatedBlock lb = getBlockGroupAt(block.getStartOffset());
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
      DFSClient.LOG.debug("refreshLocatedBlock for striped blocks, offset="
          + block.getStartOffset() + ". Obtained block " + lb + ", idx=" + idx);
    }
    return StripedBlockUtil.constructInternalBlock(
        lsb, i, cellSize, dataBlkNum, idx);
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
  protected void fetchBlockByteRange(LocatedBlock block, long start,
      long end, byte[] buf, int offset,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    // Refresh the striped block group
    LocatedStripedBlock blockGroup = getBlockGroupAt(block.getStartOffset());

    AlignedStripe[] stripes = divideByteRangeIntoStripes(schema, cellSize,
        blockGroup, start, end, buf, offset);
    CompletionService<Void> readService = new ExecutorCompletionService<>(
        dfsClient.getStripedReadsThreadPool());
    // TODO handle null elements in blks (e.g., NN does not know locations for
    // all the internal blocks)
    final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(
        blockGroup, cellSize, dataBlkNum, parityBlkNum);
    for (AlignedStripe stripe : stripes) {
      // Parse group to get chosen DN location
      StripeReader preader = new PositionStripeReader(readService, stripe);
      preader.readStripe(blks, corruptedBlockMap);
    }
  }

  private Callable<Void> getFromOneDataNode(final DNAddrPair datanode,
      final LocatedBlock block, final long start, final long end,
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
          actualGetFromOneDataNode(datanode, block, start,
              end, buf, offsets, lengths, corruptedBlockMap);
        } finally {
          scope.close();
        }
        return null;
      }
    };
  }

  private abstract class StripeReader {
    final Map<Future<Void>, Integer> futures = new HashMap<>();
    final AlignedStripe alignedStripe;
    final CompletionService<Void> service;

    StripeReader(CompletionService<Void> service, AlignedStripe alignedStripe) {
      this.service = service;
      this.alignedStripe = alignedStripe;
    }

    /** submit reading chunk task */
    abstract void readChunk(final CompletionService<Void> service,
        final LocatedBlock block, int chunkIndex,
        Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap);

    /**
     * When seeing first missing block, initialize decode input buffers.
     * Also prepare the reading for data blocks outside of the reading range.
     */
    abstract void prepareDecodeInputs() throws IOException;

    /**
     * Prepare reading for one more parity chunk.
     */
    abstract void prepareParityChunk() throws IOException;

    abstract void decode();

    abstract void updateState4SuccessRead(StripingChunkReadResult result);

    /** read the whole stripe. do decoding if necessary */
    void readStripe(LocatedBlock[] blocks,
        Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
        throws IOException {
      assert alignedStripe.getSpanInBlock() > 0;
      for (short i = 0; i < dataBlkNum; i++) {
        if (alignedStripe.chunks[i] != null
            && alignedStripe.chunks[i].state != StripingChunk.ALLZERO) {
          readChunk(service, blocks[i], i, corruptedBlockMap);
        }
      }

      // Input buffers for potential decode operation, which remains null until
      // first read failure
      while (!futures.isEmpty()) {
        try {
          StripingChunkReadResult r = getNextCompletedStripedRead(service,
              futures, 0);
          if (DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("Read task returned: " + r + ", for stripe "
                + alignedStripe);
          }
          StripingChunk returnedChunk = alignedStripe.chunks[r.index];
          Preconditions.checkNotNull(returnedChunk);
          Preconditions.checkState(returnedChunk.state == StripingChunk.PENDING);

          if (r.state == StripingChunkReadResult.SUCCESSFUL) {
            returnedChunk.state = StripingChunk.FETCHED;
            alignedStripe.fetchedChunksNum++;
            updateState4SuccessRead(r);
            if (alignedStripe.fetchedChunksNum == dataBlkNum) {
              clearFutures(futures.keySet());
              break;
            }
          } else {
            returnedChunk.state = StripingChunk.MISSING;
            alignedStripe.missingChunksNum++;
            if (alignedStripe.missingChunksNum > parityBlkNum) {
              clearFutures(futures.keySet());
              throw new IOException("Too many blocks are missing: "
                  + alignedStripe);
            }

            prepareDecodeInputs();
            prepareParityChunk();
            // close the corresponding reader
            closeReader(r.index);

            for (int i = 0; i < alignedStripe.chunks.length; i++) {
              StripingChunk chunk = alignedStripe.chunks[i];
              if (chunk != null && chunk.state == StripingChunk.REQUESTED) {
                readChunk(service, blocks[i], i, corruptedBlockMap);
              }
            }
          }
        } catch (InterruptedException ie) {
          String err = "Read request interrupted";
          DFSClient.LOG.error(err);
          clearFutures(futures.keySet());
          // Don't decode if read interrupted
          throw new InterruptedIOException(err);
        }
      }

      if (alignedStripe.missingChunksNum > 0) {
        decode();
      }
    }
  }

  class PositionStripeReader extends StripeReader {
    private byte[][] decodeInputs = null;

    PositionStripeReader(CompletionService<Void> service,
        AlignedStripe alignedStripe) {
      super(service, alignedStripe);
    }

    @Override
    void readChunk(final CompletionService<Void> service,
        final LocatedBlock block, int chunkIndex,
        Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap) {
      DatanodeInfo loc = block.getLocations()[0];
      StorageType type = block.getStorageTypes()[0];
      DNAddrPair dnAddr = new DNAddrPair(loc, NetUtils.createSocketAddr(
          loc.getXferAddr(dfsClient.getConf().isConnectToDnViaHostname())),
          type);
      StripingChunk chunk = alignedStripe.chunks[chunkIndex];
      chunk.state = StripingChunk.PENDING;
      Callable<Void> readCallable = getFromOneDataNode(dnAddr,
          block, alignedStripe.getOffsetInBlock(),
          alignedStripe.getOffsetInBlock() + alignedStripe.getSpanInBlock() - 1,
          chunk.byteArray.buf(), chunk.byteArray.getOffsets(),
          chunk.byteArray.getLengths(), corruptedBlockMap, chunkIndex);
      Future<Void> getFromDNRequest = service.submit(readCallable);
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("Submitting striped read request for " + chunkIndex
            + ". Info of the block: " + block + ", offset in block is "
            + alignedStripe.getOffsetInBlock() + ", end is "
            + (alignedStripe.getOffsetInBlock()
            + alignedStripe.getSpanInBlock() - 1));
      }
      futures.put(getFromDNRequest, chunkIndex);
    }

    @Override
    void updateState4SuccessRead(StripingChunkReadResult r) {}

    @Override
    void prepareDecodeInputs() {
      if (decodeInputs == null) {
        decodeInputs = initDecodeInputs(alignedStripe, dataBlkNum, parityBlkNum);
      }
    }

    @Override
    void prepareParityChunk() {
      for (int i = dataBlkNum; i < dataBlkNum + parityBlkNum; i++) {
        if (alignedStripe.chunks[i] == null) {
          final int decodeIndex = convertIndex4Decode(i, dataBlkNum, parityBlkNum);
          alignedStripe.chunks[i] = new StripingChunk(decodeInputs[decodeIndex]);
          alignedStripe.chunks[i].addByteArraySlice(0,
              (int) alignedStripe.getSpanInBlock());
          break;
        }
      }
    }

    @Override
    void decode() {
      finalizeDecodeInputs(decodeInputs, dataBlkNum, parityBlkNum,
          alignedStripe);
      decodeAndFillBuffer(decodeInputs, alignedStripe, dataBlkNum,
          parityBlkNum, decoder);
    }
  }

  class StatefulStripeReader extends StripeReader {
    ByteBuffer[] decodeInputs;
    final LocatedBlock[] targetBlocks;

    StatefulStripeReader(CompletionService<Void> service,
        AlignedStripe alignedStripe, LocatedBlock[] targetBlocks) {
      super(service, alignedStripe);
      this.targetBlocks = targetBlocks;
    }

    @Override
    void readChunk(final CompletionService<Void> service,
        final LocatedBlock block, int chunkIndex, Map<ExtendedBlock,
        Set<DatanodeInfo>> corruptedBlockMap) {
      StripingChunk chunk = alignedStripe.chunks[chunkIndex];
      chunk.state = StripingChunk.PENDING;
      ByteBufferStrategy strategy = new ByteBufferStrategy(chunk.byteBuffer);
      Callable<Void> readCallable = readCell(blockReaders[chunkIndex],
          currentNodes[chunkIndex], blockReaderOffsets[chunkIndex],
          alignedStripe.getOffsetInBlock(), strategy,
          chunk.byteBuffer.remaining(), corruptedBlockMap);
      Future<Void> request = readingService.submit(readCallable);
      futures.put(request, chunkIndex);
    }

    @Override
    void updateState4SuccessRead(StripingChunkReadResult result) {
      Preconditions.checkArgument(
          result.state == StripingChunkReadResult.SUCCESSFUL);
      blockReaderOffsets[result.index] =
          alignedStripe.getOffsetInBlock() + alignedStripe.getSpanInBlock();
    }

    @Override
    void prepareDecodeInputs() throws IOException {
      if (decodeInputs == null) {
        decodeInputs = new ByteBuffer[dataBlkNum + parityBlkNum];
        ByteBuffer cur = curStripeBuf.duplicate();
        StripedBlockUtil.VerticalRange range = alignedStripe.range;
        for (int i = 0; i < dataBlkNum; i++) {
          cur.limit(cur.capacity());
          int pos = (int) (range.offsetInBlock % cellSize + cellSize * i);
          cur.position(pos);
          cur.limit((int) (pos + range.spanInBlock));
          final int decodeIndex = convertIndex4Decode(i, dataBlkNum,
              parityBlkNum);
          decodeInputs[decodeIndex] = cur.slice();
          if (alignedStripe.chunks[i] == null) {
            alignedStripe.chunks[i] =
                new StripingChunk(decodeInputs[decodeIndex]);
          }
        }
      }
    }

    @Override
    void prepareParityChunk() throws IOException {
      for (int i = dataBlkNum; i < dataBlkNum + parityBlkNum; i++) {
        if (alignedStripe.chunks[i] == null) {
          final int decodeIndex = convertIndex4Decode(i, dataBlkNum,
              parityBlkNum);
          decodeInputs[decodeIndex] = ByteBuffer.allocateDirect(
              (int) alignedStripe.range.spanInBlock);
          alignedStripe.chunks[i] = new StripingChunk(decodeInputs[decodeIndex]);
          if (blockReaders[i] == null) {
            prepareParityBlockReader(i);
          }
          break;
        }
      }
    }

    private void prepareParityBlockReader(int i) throws IOException {
      // prepare the block reader for the parity chunk
      LocatedBlock targetBlock = targetBlocks[i];
      if (targetBlock != null) {
        final long offsetInBlock = alignedStripe.getOffsetInBlock();
        DNAddrPair retval = getBestNodeDNAddrPair(targetBlock, null);
        if (retval != null) {
          currentNodes[i] = retval.info;
          blockReaders[i] = getBlockReaderWithRetry(targetBlock,
              offsetInBlock, targetBlock.getBlockSize() - offsetInBlock,
              retval.addr, retval.storageType, retval.info,
              DFSStripedInputStream.this.getPos(), retry);
          blockReaderOffsets[i] = offsetInBlock;
        }
      }
    }

    @Override
    void decode() {
      // TODO no copy for data chunks. this depends on HADOOP-12047 for some
      // decoders to work
      final int span = (int) alignedStripe.getSpanInBlock();
      for (int i = 0; i < alignedStripe.chunks.length; i++) {
        final int decodeIndex = convertIndex4Decode(i, dataBlkNum, parityBlkNum);
        if (alignedStripe.chunks[i] != null &&
            alignedStripe.chunks[i].state == StripingChunk.ALLZERO) {
          for (int j = 0; j < span; j++) {
            decodeInputs[decodeIndex].put((byte) 0);
          }
          decodeInputs[decodeIndex].flip();
        } else if (alignedStripe.chunks[i] != null &&
            alignedStripe.chunks[i].state == StripingChunk.FETCHED) {
          decodeInputs[decodeIndex].position(0);
          decodeInputs[decodeIndex].limit(span);
        }
      }
      int[] decodeIndices = new int[parityBlkNum];
      int pos = 0;
      for (int i = 0; i < alignedStripe.chunks.length; i++) {
        if (alignedStripe.chunks[i] != null &&
            alignedStripe.chunks[i].state == StripingChunk.MISSING) {
          decodeIndices[pos++] = convertIndex4Decode(i, dataBlkNum, parityBlkNum);
        }
      }
      decodeIndices = Arrays.copyOf(decodeIndices, pos);

      final int decodeChunkNum = decodeIndices.length;
      ByteBuffer[] outputs = new ByteBuffer[decodeChunkNum];
      for (int i = 0; i < decodeChunkNum; i++) {
        outputs[i] = decodeInputs[decodeIndices[i]];
        outputs[i].position(0);
        outputs[i].limit((int) alignedStripe.range.spanInBlock);
        decodeInputs[decodeIndices[i]] = null;
      }

      decoder.decode(decodeInputs, decodeIndices, outputs);
    }
  }

  /**
   * May need online read recovery, zero-copy read doesn't make
   * sense, so don't support it.
   */
  @Override
  public synchronized ByteBuffer read(ByteBufferPool bufferPool,
      int maxLength, EnumSet<ReadOption> opts)
          throws IOException, UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Not support enhanced byte buffer access.");
  }

  @Override
  public synchronized void releaseBuffer(ByteBuffer buffer) {
    throw new UnsupportedOperationException(
        "Not support enhanced byte buffer access.");
  }

  /** A variation to {@link DFSInputStream#cancelAll} */
  private void clearFutures(Collection<Future<Void>> futures) {
    for (Future<Void> future : futures) {
      future.cancel(false);
    }
    futures.clear();
  }
}
