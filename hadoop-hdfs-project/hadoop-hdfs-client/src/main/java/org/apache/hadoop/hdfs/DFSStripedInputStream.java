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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.DFSUtilClient.CorruptedBlocks;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.ByteBufferPool;

import static org.apache.hadoop.hdfs.util.StripedBlockUtil.AlignedStripe;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.StripingChunk;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.StripingChunkReadResult;

import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;

import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.util.DirectBufferPool;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
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
@InterfaceAudience.Private
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

  private static class BlockReaderInfo {
    final BlockReader reader;
    final DatanodeInfo datanode;
    /**
     * when initializing block readers, their starting offsets are set to the same
     * number: the smallest internal block offsets among all the readers. This is
     * because it is possible that for some internal blocks we have to read
     * "backwards" for decoding purpose. We thus use this offset array to track
     * offsets for all the block readers so that we can skip data if necessary.
     */
    long blockReaderOffset;
    /**
     * We use this field to indicate whether we should use this reader. In case
     * we hit any issue with this reader, we set this field to true and avoid
     * using it for the next stripe.
     */
    boolean shouldSkip = false;

    BlockReaderInfo(BlockReader reader, DatanodeInfo dn, long offset) {
      this.reader = reader;
      this.datanode = dn;
      this.blockReaderOffset = offset;
    }

    void setOffset(long offset) {
      this.blockReaderOffset = offset;
    }

    void skip() {
      this.shouldSkip = true;
    }
  }

  private static final DirectBufferPool bufferPool = new DirectBufferPool();

  private final BlockReaderInfo[] blockReaders;
  private final int cellSize;
  private final short dataBlkNum;
  private final short parityBlkNum;
  private final int groupSize;
  /** the buffer for a complete stripe */
  private ByteBuffer curStripeBuf;
  private ByteBuffer parityBuf;
  private final ErasureCodingPolicy ecPolicy;
  private final RawErasureDecoder decoder;

  /**
   * indicate the start/end offset of the current buffered stripe in the
   * block group
   */
  private StripeRange curStripeRange;
  private final CompletionService<Void> readingService;

  /**
   * When warning the user of a lost block in striping mode, we remember the
   * dead nodes we've logged. All other striping blocks on these nodes can be
   * considered lost too, and we don't want to log a warning for each of them.
   * This is to prevent the log from being too verbose. Refer to HDFS-8920.
   *
   * To minimize the overhead, we only store the datanodeUuid in this set
   */
  private final Set<String> warnedNodes = Collections.newSetFromMap(
      new ConcurrentHashMap<String, Boolean>());

  DFSStripedInputStream(DFSClient dfsClient, String src,
      boolean verifyChecksum, ErasureCodingPolicy ecPolicy,
      LocatedBlocks locatedBlocks) throws IOException {
    super(dfsClient, src, verifyChecksum, locatedBlocks);

    assert ecPolicy != null;
    this.ecPolicy = ecPolicy;
    this.cellSize = ecPolicy.getCellSize();
    dataBlkNum = (short) ecPolicy.getNumDataUnits();
    parityBlkNum = (short) ecPolicy.getNumParityUnits();
    groupSize = dataBlkNum + parityBlkNum;
    blockReaders = new BlockReaderInfo[groupSize];
    curStripeRange = new StripeRange(0, 0);
    readingService =
        new ExecutorCompletionService<>(dfsClient.getStripedReadsThreadPool());
    ErasureCoderOptions coderOptions = new ErasureCoderOptions(
        dataBlkNum, parityBlkNum);
    decoder = CodecUtil.createRawDecoder(dfsClient.getConfiguration(),
        ecPolicy.getCodecName(), coderOptions);
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Creating an striped input stream for file " + src);
    }
  }

  private void resetCurStripeBuffer() {
    if (curStripeBuf == null) {
      curStripeBuf = bufferPool.getBuffer(cellSize * dataBlkNum);
    }
    curStripeBuf.clear();
    curStripeRange = new StripeRange(0, 0);
  }

  private ByteBuffer getParityBuffer() {
    if (parityBuf == null) {
      parityBuf = bufferPool.getBuffer(cellSize * parityBlkNum);
    }
    parityBuf.clear();
    return parityBuf;
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
  }

  @Override
  public synchronized void close() throws IOException {
    super.close();
    if (curStripeBuf != null) {
      bufferPool.returnBuffer(curStripeBuf);
      curStripeBuf = null;
    }
    if (parityBuf != null) {
      bufferPool.returnBuffer(parityBuf);
      parityBuf = null;
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
      closeReader(blockReaders[i]);
      blockReaders[i] = null;
    }
    blockEnd = -1;
  }

  private void closeReader(BlockReaderInfo readerInfo) {
    if (readerInfo != null) {
      if (readerInfo.reader != null) {
        try {
          readerInfo.reader.close();
        } catch (Throwable ignored) {
        }
      }
      readerInfo.skip();
    }
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
  private void readOneStripe(CorruptedBlocks corruptedBlocks)
      throws IOException {
    resetCurStripeBuffer();

    // compute stripe range based on pos
    final long offsetInBlockGroup = getOffsetInBlockGroup();
    final long stripeLen = cellSize * dataBlkNum;
    final int stripeIndex = (int) (offsetInBlockGroup / stripeLen);
    final int stripeBufOffset = (int) (offsetInBlockGroup % stripeLen);
    final int stripeLimit = (int) Math.min(currentLocatedBlock.getBlockSize()
        - (stripeIndex * stripeLen), stripeLen);
    StripeRange stripeRange = new StripeRange(offsetInBlockGroup,
        stripeLimit - stripeBufOffset);

    LocatedStripedBlock blockGroup = (LocatedStripedBlock) currentLocatedBlock;
    AlignedStripe[] stripes = StripedBlockUtil.divideOneStripe(ecPolicy, cellSize,
        blockGroup, offsetInBlockGroup,
        offsetInBlockGroup + stripeRange.length - 1, curStripeBuf);
    final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(
        blockGroup, cellSize, dataBlkNum, parityBlkNum);
    // read the whole stripe
    for (AlignedStripe stripe : stripes) {
      // Parse group to get chosen DN location
      StripeReader sreader = new StatefulStripeReader(readingService, stripe,
          blks, blockReaders, corruptedBlocks);
      sreader.readStripe();
    }
    curStripeBuf.position(stripeBufOffset);
    curStripeBuf.limit(stripeLimit);
    curStripeRange = stripeRange;
  }

  private Callable<Void> readCells(final BlockReader reader,
      final DatanodeInfo datanode, final long currentReaderOffset,
      final long targetReaderOffset, final ByteBufferStrategy[] strategies,
      final ExtendedBlock currentBlock,
      final CorruptedBlocks corruptedBlocks) {
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
        for (ByteBufferStrategy strategy : strategies) {
          result += readToBuffer(reader, datanode, strategy, currentBlock,
              corruptedBlocks);
        }
        return null;
      }
    };
  }

  private int readToBuffer(BlockReader blockReader,
      DatanodeInfo currentNode, ByteBufferStrategy strategy,
      ExtendedBlock currentBlock,
      CorruptedBlocks corruptedBlocks)
      throws IOException {
    final int targetLength = strategy.buf.remaining();
    int length = 0;
    try {
      while (length < targetLength) {
        int ret = strategy.doRead(blockReader, 0, 0);
        if (ret < 0) {
          throw new IOException("Unexpected EOS from the reader");
        }
        length += ret;
      }
      return length;
    } catch (ChecksumException ce) {
      DFSClient.LOG.warn("Found Checksum error for "
          + currentBlock + " from " + currentNode
          + " at " + ce.getPos());
      // we want to remember which block replicas we have tried
      corruptedBlocks.addCorruptedBlock(currentBlock, currentNode);
      throw ce;
    } catch (IOException e) {
      DFSClient.LOG.warn("Exception while reading from "
          + currentBlock + " of " + src + " from "
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

    CorruptedBlocks corruptedBlocks = new CorruptedBlocks();
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
            readOneStripe(corruptedBlocks);
          }
          int ret = copyToTargetBuf(strategy, off + result, realLen - result);
          result += ret;
          pos += ret;
        }
        return result;
      } finally {
        // Check if need to report block replicas corruption either read
        // was successful or ChecksumException occured.
        reportCheckSumFailure(corruptedBlocks,
            currentLocatedBlock.getLocations().length, true);
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
    int idx = StripedBlockUtil.getBlockIndex(block.getBlock().getLocalBlock());
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
      long end, byte[] buf, int offset, CorruptedBlocks corruptedBlocks)
      throws IOException {
    // Refresh the striped block group
    LocatedStripedBlock blockGroup = getBlockGroupAt(block.getStartOffset());

    AlignedStripe[] stripes = StripedBlockUtil.divideByteRangeIntoStripes(
        ecPolicy, cellSize, blockGroup, start, end, buf, offset);
    CompletionService<Void> readService = new ExecutorCompletionService<>(
        dfsClient.getStripedReadsThreadPool());
    final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(
        blockGroup, cellSize, dataBlkNum, parityBlkNum);
    final BlockReaderInfo[] preaderInfos = new BlockReaderInfo[groupSize];
    try {
      for (AlignedStripe stripe : stripes) {
        // Parse group to get chosen DN location
        StripeReader preader = new PositionStripeReader(readService, stripe,
            blks, preaderInfos, corruptedBlocks);
        preader.readStripe();
      }
    } finally {
      for (BlockReaderInfo preaderInfo : preaderInfos) {
        closeReader(preaderInfo);
      }
    }
  }

  @Override
  protected void reportLostBlock(LocatedBlock lostBlock,
      Collection<DatanodeInfo> ignoredNodes) {
    DatanodeInfo[] nodes = lostBlock.getLocations();
    if (nodes != null && nodes.length > 0) {
      List<String> dnUUIDs = new ArrayList<>();
      for (DatanodeInfo node : nodes) {
        dnUUIDs.add(node.getDatanodeUuid());
      }
      if (!warnedNodes.containsAll(dnUUIDs)) {
        DFSClient.LOG.warn(Arrays.toString(nodes) + " are unavailable and " +
            "all striping blocks on them are lost. " +
            "IgnoredNodes = " + ignoredNodes);
        warnedNodes.addAll(dnUUIDs);
      }
    } else {
      super.reportLostBlock(lostBlock, ignoredNodes);
    }
  }

  /**
   * The reader for reading a complete {@link AlignedStripe}. Note that an
   * {@link AlignedStripe} may cross multiple stripes with cellSize width.
   */
  private abstract class StripeReader {
    final Map<Future<Void>, Integer> futures = new HashMap<>();
    final AlignedStripe alignedStripe;
    final CompletionService<Void> service;
    final LocatedBlock[] targetBlocks;
    final CorruptedBlocks corruptedBlocks;
    final BlockReaderInfo[] readerInfos;

    StripeReader(CompletionService<Void> service, AlignedStripe alignedStripe,
        LocatedBlock[] targetBlocks, BlockReaderInfo[] readerInfos,
                 CorruptedBlocks corruptedBlocks) {
      this.service = service;
      this.alignedStripe = alignedStripe;
      this.targetBlocks = targetBlocks;
      this.readerInfos = readerInfos;
      this.corruptedBlocks = corruptedBlocks;
    }

    /** prepare all the data chunks */
    abstract void prepareDecodeInputs();

    /** prepare the parity chunk and block reader if necessary */
    abstract boolean prepareParityChunk(int index);

    abstract void decode();

    void updateState4SuccessRead(StripingChunkReadResult result) {
      Preconditions.checkArgument(
          result.state == StripingChunkReadResult.SUCCESSFUL);
      readerInfos[result.index].setOffset(alignedStripe.getOffsetInBlock()
          + alignedStripe.getSpanInBlock());
    }

    private void checkMissingBlocks() throws IOException {
      if (alignedStripe.missingChunksNum > parityBlkNum) {
        clearFutures(futures.keySet());
        throw new IOException(alignedStripe.missingChunksNum
            + " missing blocks, the stripe is: " + alignedStripe
            + "; locatedBlocks is: " + locatedBlocks);
      }
    }

    /**
     * We need decoding. Thus go through all the data chunks and make sure we
     * submit read requests for all of them.
     */
    private void readDataForDecoding() throws IOException {
      prepareDecodeInputs();
      for (int i = 0; i < dataBlkNum; i++) {
        Preconditions.checkNotNull(alignedStripe.chunks[i]);
        if (alignedStripe.chunks[i].state == StripingChunk.REQUESTED) {
          if (!readChunk(targetBlocks[i], i)) {
            alignedStripe.missingChunksNum++;
          }
        }
      }
      checkMissingBlocks();
    }

    void readParityChunks(int num) throws IOException {
      for (int i = dataBlkNum, j = 0; i < dataBlkNum + parityBlkNum && j < num;
           i++) {
        if (alignedStripe.chunks[i] == null) {
          if (prepareParityChunk(i) && readChunk(targetBlocks[i], i)) {
            j++;
          } else {
            alignedStripe.missingChunksNum++;
          }
        }
      }
      checkMissingBlocks();
    }

    boolean createBlockReader(LocatedBlock block, int chunkIndex)
        throws IOException {
      BlockReader reader = null;
      final ReaderRetryPolicy retry = new ReaderRetryPolicy();
      DNAddrPair dnInfo = new DNAddrPair(null, null, null);

      while(true) {
        try {
          // the cached block location might have been re-fetched, so always
          // get it from cache.
          block = refreshLocatedBlock(block);
          targetBlocks[chunkIndex] = block;

          // internal block has one location, just rule out the deadNodes
          dnInfo = getBestNodeDNAddrPair(block, null);
          if (dnInfo == null) {
            break;
          }
          reader = getBlockReader(block, alignedStripe.getOffsetInBlock(),
              block.getBlockSize() - alignedStripe.getOffsetInBlock(),
              dnInfo.addr, dnInfo.storageType, dnInfo.info);
        } catch (IOException e) {
          if (e instanceof InvalidEncryptionKeyException &&
              retry.shouldRefetchEncryptionKey()) {
            DFSClient.LOG.info("Will fetch a new encryption key and retry, "
                + "encryption key was invalid when connecting to " + dnInfo.addr
                + " : " + e);
            dfsClient.clearDataEncryptionKey();
            retry.refetchEncryptionKey();
          } else if (retry.shouldRefetchToken() &&
              tokenRefetchNeeded(e, dnInfo.addr)) {
            fetchBlockAt(block.getStartOffset());
            retry.refetchToken();
          } else {
            //TODO: handles connection issues
            DFSClient.LOG.warn("Failed to connect to " + dnInfo.addr + " for " +
                "block" + block.getBlock(), e);
            // re-fetch the block in case the block has been moved
            fetchBlockAt(block.getStartOffset());
            addToDeadNodes(dnInfo.info);
          }
        }
        if (reader != null) {
          readerInfos[chunkIndex] = new BlockReaderInfo(reader, dnInfo.info,
              alignedStripe.getOffsetInBlock());
          return true;
        }
      }
      return false;
    }

    private ByteBufferStrategy[] getReadStrategies(StripingChunk chunk) {
      if (chunk.byteBuffer != null) {
        ByteBufferStrategy strategy = new ByteBufferStrategy(chunk.byteBuffer);
        return new ByteBufferStrategy[]{strategy};
      } else {
        ByteBufferStrategy[] strategies =
            new ByteBufferStrategy[chunk.byteArray.getOffsets().length];
        for (int i = 0; i < strategies.length; i++) {
          ByteBuffer buffer = ByteBuffer.wrap(chunk.byteArray.buf(),
              chunk.byteArray.getOffsets()[i], chunk.byteArray.getLengths()[i]);
          strategies[i] = new ByteBufferStrategy(buffer);
        }
        return strategies;
      }
    }

    boolean readChunk(final LocatedBlock block, int chunkIndex)
        throws IOException {
      final StripingChunk chunk = alignedStripe.chunks[chunkIndex];
      if (block == null) {
        chunk.state = StripingChunk.MISSING;
        return false;
      }
      if (readerInfos[chunkIndex] == null) {
        if (!createBlockReader(block, chunkIndex)) {
          chunk.state = StripingChunk.MISSING;
          return false;
        }
      } else if (readerInfos[chunkIndex].shouldSkip) {
        chunk.state = StripingChunk.MISSING;
        return false;
      }

      chunk.state = StripingChunk.PENDING;
      Callable<Void> readCallable = readCells(readerInfos[chunkIndex].reader,
          readerInfos[chunkIndex].datanode,
          readerInfos[chunkIndex].blockReaderOffset,
          alignedStripe.getOffsetInBlock(), getReadStrategies(chunk),
          block.getBlock(), corruptedBlocks);

      Future<Void> request = service.submit(readCallable);
      futures.put(request, chunkIndex);
      return true;
    }

    /** read the whole stripe. do decoding if necessary */
    void readStripe() throws IOException {
      for (int i = 0; i < dataBlkNum; i++) {
        if (alignedStripe.chunks[i] != null &&
            alignedStripe.chunks[i].state != StripingChunk.ALLZERO) {
          if (!readChunk(targetBlocks[i], i)) {
            alignedStripe.missingChunksNum++;
          }
        }
      }
      // There are missing block locations at this stage. Thus we need to read
      // the full stripe and one more parity block.
      if (alignedStripe.missingChunksNum > 0) {
        checkMissingBlocks();
        readDataForDecoding();
        // read parity chunks
        readParityChunks(alignedStripe.missingChunksNum);
      }
      // TODO: for a full stripe we can start reading (dataBlkNum + 1) chunks

      // Input buffers for potential decode operation, which remains null until
      // first read failure
      while (!futures.isEmpty()) {
        try {
          StripingChunkReadResult r = StripedBlockUtil
              .getNextCompletedStripedRead(service, futures, 0);
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
            // close the corresponding reader
            closeReader(readerInfos[r.index]);

            final int missing = alignedStripe.missingChunksNum;
            alignedStripe.missingChunksNum++;
            checkMissingBlocks();

            readDataForDecoding();
            readParityChunks(alignedStripe.missingChunksNum - missing);
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
        AlignedStripe alignedStripe, LocatedBlock[] targetBlocks,
        BlockReaderInfo[] readerInfos, CorruptedBlocks corruptedBlocks) {
      super(service, alignedStripe, targetBlocks, readerInfos,
          corruptedBlocks);
    }

    @Override
    void prepareDecodeInputs() {
      if (decodeInputs == null) {
        decodeInputs = StripedBlockUtil.initDecodeInputs(alignedStripe,
            dataBlkNum, parityBlkNum);
      }
    }

    @Override
    boolean prepareParityChunk(int index) {
      Preconditions.checkState(index >= dataBlkNum &&
          alignedStripe.chunks[index] == null);
      alignedStripe.chunks[index] = new StripingChunk(decodeInputs[index]);
      alignedStripe.chunks[index].addByteArraySlice(0,
          (int) alignedStripe.getSpanInBlock());
      return true;
    }

    @Override
    void decode() {
      StripedBlockUtil.finalizeDecodeInputs(decodeInputs, alignedStripe);
      StripedBlockUtil.decodeAndFillBuffer(decodeInputs, alignedStripe,
          dataBlkNum, parityBlkNum, decoder);
    }
  }

  class StatefulStripeReader extends StripeReader {
    ByteBuffer[] decodeInputs;

    StatefulStripeReader(CompletionService<Void> service,
        AlignedStripe alignedStripe, LocatedBlock[] targetBlocks,
        BlockReaderInfo[] readerInfos, CorruptedBlocks corruptedBlocks) {
      super(service, alignedStripe, targetBlocks, readerInfos,
          corruptedBlocks);
    }

    @Override
    void prepareDecodeInputs() {
      if (decodeInputs == null) {
        decodeInputs = new ByteBuffer[dataBlkNum + parityBlkNum];
        final ByteBuffer cur;
        synchronized (DFSStripedInputStream.this) {
          cur = curStripeBuf.duplicate();
        }
        StripedBlockUtil.VerticalRange range = alignedStripe.range;
        for (int i = 0; i < dataBlkNum; i++) {
          cur.limit(cur.capacity());
          int pos = (int) (range.offsetInBlock % cellSize + cellSize * i);
          cur.position(pos);
          cur.limit((int) (pos + range.spanInBlock));
          decodeInputs[i] = cur.slice();
          if (alignedStripe.chunks[i] == null) {
            alignedStripe.chunks[i] = new StripingChunk(decodeInputs[i]);
          }
        }
      }
    }

    @Override
    boolean prepareParityChunk(int index) {
      Preconditions.checkState(index >= dataBlkNum
          && alignedStripe.chunks[index] == null);
      if (blockReaders[index] != null && blockReaders[index].shouldSkip) {
        alignedStripe.chunks[index] = new StripingChunk(StripingChunk.MISSING);
        // we have failed the block reader before
        return false;
      }
      final int parityIndex = index - dataBlkNum;
      ByteBuffer buf = getParityBuffer().duplicate();
      buf.position(cellSize * parityIndex);
      buf.limit(cellSize * parityIndex + (int) alignedStripe.range.spanInBlock);
      decodeInputs[index] = buf.slice();
      alignedStripe.chunks[index] = new StripingChunk(decodeInputs[index]);
      return true;
    }

    @Override
    void decode() {
      final int span = (int) alignedStripe.getSpanInBlock();
      for (int i = 0; i < alignedStripe.chunks.length; i++) {
        if (alignedStripe.chunks[i] != null &&
            alignedStripe.chunks[i].state == StripingChunk.ALLZERO) {
          for (int j = 0; j < span; j++) {
            decodeInputs[i].put((byte) 0);
          }
          decodeInputs[i].flip();
        } else if (alignedStripe.chunks[i] != null &&
            alignedStripe.chunks[i].state == StripingChunk.FETCHED) {
          decodeInputs[i].position(0);
          decodeInputs[i].limit(span);
        }
      }
      int[] decodeIndices = new int[parityBlkNum];
      int pos = 0;
      for (int i = 0; i < alignedStripe.chunks.length; i++) {
        if (alignedStripe.chunks[i] != null &&
            alignedStripe.chunks[i].state == StripingChunk.MISSING) {
          if (i < dataBlkNum) {
            decodeIndices[pos++] = i;
          } else {
            decodeInputs[i] = null;
          }
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
