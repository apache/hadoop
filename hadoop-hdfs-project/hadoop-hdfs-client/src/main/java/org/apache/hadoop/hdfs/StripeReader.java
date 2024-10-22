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

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.BlockReadStats;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.StripingChunk;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.AlignedStripe;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.StripingChunkReadResult;
import org.apache.hadoop.io.erasurecode.ECChunk;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.hdfs.DFSUtilClient.CorruptedBlocks;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

/**
 * The reader for reading a complete {@link StripedBlockUtil.AlignedStripe}.
 * Note that an {@link StripedBlockUtil.AlignedStripe} may cross multiple
 * stripes with cellSize width.
 */
abstract class StripeReader {

  static class ReaderRetryPolicy {
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

  static class BlockReaderInfo {
    final BlockReader reader;
    final DatanodeInfo datanode;
    /**
     * when initializing block readers, their starting offsets are set to the
     * same number: the smallest internal block offsets among all the readers.
     * This is because it is possible that for some internal blocks we have to
     * read "backwards" for decoding purpose. We thus use this offset array to
     * track offsets for all the block readers so that we can skip data if
     * necessary.
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

  private final Map<Future<BlockReadStats>, Integer> futures =
      new HashMap<>();
  protected final AlignedStripe alignedStripe;
  private final CompletionService<BlockReadStats> service;
  protected final LocatedBlock[] targetBlocks;
  protected final CorruptedBlocks corruptedBlocks;
  protected final BlockReaderInfo[] readerInfos;
  protected final ErasureCodingPolicy ecPolicy;
  protected final short dataBlkNum;
  protected final short parityBlkNum;
  protected final int cellSize;
  protected final RawErasureDecoder decoder;
  protected final DFSStripedInputStream dfsStripedInputStream;
  private long readTo = -1;
  private final int readDNMaxAttempts;

  protected ECChunk[] decodeInputs;

  StripeReader(AlignedStripe alignedStripe,
      ErasureCodingPolicy ecPolicy, LocatedBlock[] targetBlocks,
      BlockReaderInfo[] readerInfos, CorruptedBlocks corruptedBlocks,
      RawErasureDecoder decoder,
      DFSStripedInputStream dfsStripedInputStream) {
    this.alignedStripe = alignedStripe;
    this.ecPolicy = ecPolicy;
    this.dataBlkNum = (short)ecPolicy.getNumDataUnits();
    this.parityBlkNum = (short)ecPolicy.getNumParityUnits();
    this.cellSize = ecPolicy.getCellSize();
    this.targetBlocks = targetBlocks;
    this.readerInfos = readerInfos;
    this.corruptedBlocks = corruptedBlocks;
    this.decoder = decoder;
    this.dfsStripedInputStream = dfsStripedInputStream;
    this.readDNMaxAttempts = dfsStripedInputStream.getDFSClient()
        .getConf().getStripedReadDnMaxAttempts();

    service = new ExecutorCompletionService<>(
            dfsStripedInputStream.getStripedReadsThreadPool());
  }

  /**
   * Prepare all the data chunks.
   */
  abstract void prepareDecodeInputs();

  /**
   * Prepare the parity chunk and block reader if necessary.
   */
  abstract boolean prepareParityChunk(int index);

  /**
   * Decode to get the missing data.
   * @throws IOException if the decoder is closed.
   */
  abstract void decode() throws IOException;

  /*
   * Default close do nothing.
   */
  void close() {
  }

  void updateState4SuccessRead(StripingChunkReadResult result) {
    Preconditions.checkArgument(
        result.state == StripingChunkReadResult.SUCCESSFUL);
    readerInfos[result.index].setOffset(alignedStripe.getOffsetInBlock()
        + alignedStripe.getSpanInBlock());
  }

  private void checkMissingBlocks() throws IOException {
    if (alignedStripe.missingChunksNum > parityBlkNum) {
      clearFutures();
      throw new IOException(alignedStripe.missingChunksNum
          + " missing blocks, the stripe is: " + alignedStripe
          + "; locatedBlocks is: " + dfsStripedInputStream.getLocatedBlocks());
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

  private ByteBufferStrategy[] getReadStrategies(StripingChunk chunk) {
    if (chunk.useByteBuffer()) {
      ByteBufferStrategy strategy = new ByteBufferStrategy(
          chunk.getByteBuffer(), dfsStripedInputStream.getReadStatistics(),
          dfsStripedInputStream.getDFSClient());
      return new ByteBufferStrategy[]{strategy};
    }

    ByteBufferStrategy[] strategies =
        new ByteBufferStrategy[chunk.getChunkBuffer().getSlices().size()];
    for (int i = 0; i < strategies.length; i++) {
      ByteBuffer buffer = chunk.getChunkBuffer().getSlice(i);
      strategies[i] = new ByteBufferStrategy(buffer,
              dfsStripedInputStream.getReadStatistics(),
              dfsStripedInputStream.getDFSClient());
    }
    return strategies;
  }

  private int readToBuffer(BlockReader blockReader,
      DatanodeInfo currentNode, ByteBufferStrategy strategy,
      LocatedBlock currentBlock, int chunkIndex, long offsetInBlock)
      throws IOException {
    final int targetLength = strategy.getTargetLength();
    int curAttempts = 0;
    while (true) {
      curAttempts++;
      int length = 0;
      try {
        while (length < targetLength) {
          int ret = strategy.readFromBlock(blockReader);
          if (ret < 0) {
            throw new IOException("Unexpected EOS from the reader");
          }
          length += ret;
        }
        return length;
      } catch (ChecksumException ce) {
        DFSClient.LOG.warn("Found Checksum error for {} from {} at {}",
             currentBlock, currentNode, ce.getPos());
        //Clear buffer to make next decode success
        strategy.getReadBuffer().clear();
        // we want to remember which block replicas we have tried
        corruptedBlocks.addCorruptedBlock(currentBlock.getBlock(), currentNode);
        throw ce;
      } catch (IOException e) {
        //Clear buffer to make next decode success
        strategy.getReadBuffer().clear();
        if (curAttempts < readDNMaxAttempts) {
          if (readerInfos[chunkIndex].reader != null) {
            readerInfos[chunkIndex].reader.close();
          }
          if (dfsStripedInputStream.createBlockReader(currentBlock,
              offsetInBlock, targetBlocks,
              readerInfos, chunkIndex, readTo)) {
            blockReader = readerInfos[chunkIndex].reader;
            DFSClient.LOG.warn("Reconnect to {} for block {}",
                currentNode.getInfoAddr(), currentBlock.getBlock());
            continue;
          }
        }
        DFSClient.LOG.warn("Exception while reading from {} of {} from {}",
             currentBlock, dfsStripedInputStream.getSrc(), currentNode, e);
        throw e;
      }
    }
  }

  private Callable<BlockReadStats> readCells(final BlockReader reader,
      final DatanodeInfo datanode, final long currentReaderOffset,
      final long targetReaderOffset, final ByteBufferStrategy[] strategies,
      final LocatedBlock currentBlock, final int chunkIndex) {
    return () -> {
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

      int ret = 0;
      for (ByteBufferStrategy strategy : strategies) {
        int bytesRead = readToBuffer(reader, datanode, strategy, currentBlock,
            chunkIndex, alignedStripe.getOffsetInBlock() + ret);
        ret += bytesRead;
      }
      return new BlockReadStats(ret, reader.isShortCircuit(),
          reader.getNetworkDistance());
    };
  }

  boolean readChunk(final LocatedBlock block, int chunkIndex)
      throws IOException {
    final StripingChunk chunk = alignedStripe.chunks[chunkIndex];
    if (block == null) {
      chunk.state = StripingChunk.MISSING;
      return false;
    }

    if (readerInfos[chunkIndex] == null) {
      if (!dfsStripedInputStream.createBlockReader(block,
          alignedStripe.getOffsetInBlock(), targetBlocks,
          readerInfos, chunkIndex, readTo)) {
        chunk.state = StripingChunk.MISSING;
        return false;
      }
    } else if (readerInfos[chunkIndex].shouldSkip) {
      chunk.state = StripingChunk.MISSING;
      return false;
    }

    chunk.state = StripingChunk.PENDING;
    Callable<BlockReadStats> readCallable =
        readCells(readerInfos[chunkIndex].reader,
        readerInfos[chunkIndex].datanode,
        readerInfos[chunkIndex].blockReaderOffset,
        alignedStripe.getOffsetInBlock(), getReadStrategies(chunk),
        block, chunkIndex);

    Future<BlockReadStats> request = service.submit(readCallable);
    futures.put(request, chunkIndex);
    return true;
  }

  /**
   * read the whole stripe. do decoding if necessary
   */
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
        long beginReadMS = Time.monotonicNow();
        StripingChunkReadResult r = StripedBlockUtil
            .getNextCompletedStripedRead(service, futures, 0);
        long readTimeMS = Time.monotonicNow() - beginReadMS;

        dfsStripedInputStream.updateReadStats(r.getReadStats(), readTimeMS);
        DFSClient.LOG.debug("Read task returned: {}, for stripe {}",
            r, alignedStripe);
        StripingChunk returnedChunk = alignedStripe.chunks[r.index];
        Preconditions.checkNotNull(returnedChunk);
        Preconditions.checkState(returnedChunk.state == StripingChunk.PENDING);

        if (r.state == StripingChunkReadResult.SUCCESSFUL) {
          returnedChunk.state = StripingChunk.FETCHED;
          alignedStripe.fetchedChunksNum++;
          updateState4SuccessRead(r);
          if (alignedStripe.fetchedChunksNum == dataBlkNum) {
            clearFutures();
            break;
          }
        } else {
          returnedChunk.state = StripingChunk.MISSING;
          // close the corresponding reader
          dfsStripedInputStream.closeReader(readerInfos[r.index]);

          final int missing = alignedStripe.missingChunksNum;
          alignedStripe.missingChunksNum++;
          checkMissingBlocks();

          readDataForDecoding();
          readParityChunks(alignedStripe.missingChunksNum - missing);
        }
      } catch (InterruptedException ie) {
        String err = "Read request interrupted";
        DFSClient.LOG.error(err);
        clearFutures();
        // Don't decode if read interrupted
        throw new InterruptedIOException(err);
      }
    }

    if (alignedStripe.missingChunksNum > 0) {
      decode();
    }
  }

  /**
   * Some fetched {@link StripingChunk} might be stored in original application
   * buffer instead of prepared decode input buffers. Some others are beyond
   * the range of the internal blocks and should correspond to all zero bytes.
   * When all pending requests have returned, this method should be called to
   * finalize decode input buffers.
   */

  void finalizeDecodeInputs() {
    for (int i = 0; i < alignedStripe.chunks.length; i++) {
      final StripingChunk chunk = alignedStripe.chunks[i];
      if (chunk != null && chunk.state == StripingChunk.FETCHED) {
        if (chunk.useChunkBuffer()) {
          chunk.getChunkBuffer().copyTo(decodeInputs[i].getBuffer());
        } else {
          chunk.getByteBuffer().flip();
        }
      } else if (chunk != null && chunk.state == StripingChunk.ALLZERO) {
        decodeInputs[i].setAllZero(true);
      }
    }
  }

  /**
   * Decode based on the given input buffers and erasure coding policy.
   */
  void decodeAndFillBuffer(boolean fillBuffer) throws IOException {
    // Step 1: prepare indices and output buffers for missing data units
    int[] decodeIndices = prepareErasedIndices();

    final int decodeChunkNum = decodeIndices.length;
    ECChunk[] outputs = new ECChunk[decodeChunkNum];
    for (int i = 0; i < decodeChunkNum; i++) {
      outputs[i] = decodeInputs[decodeIndices[i]];
      decodeInputs[decodeIndices[i]] = null;
    }

    long start = Time.monotonicNow();
    // Step 2: decode into prepared output buffers
    decoder.decode(decodeInputs, decodeIndices, outputs);

    // Step 3: fill original application buffer with decoded data
    if (fillBuffer) {
      for (int i = 0; i < decodeIndices.length; i++) {
        int missingBlkIdx = decodeIndices[i];
        StripingChunk chunk = alignedStripe.chunks[missingBlkIdx];
        if (chunk.state == StripingChunk.MISSING && chunk.useChunkBuffer()) {
          chunk.getChunkBuffer().copyFrom(outputs[i].getBuffer());
        }
      }
    }
    long end = Time.monotonicNow();
    // Decoding time includes CPU time on erasure coding and memory copying of
    // decoded data.
    dfsStripedInputStream.readStatistics.addErasureCodingDecodingTime(
        end - start);
  }

  /**
   * Prepare erased indices.
   */
  int[] prepareErasedIndices() {
    int[] decodeIndices = new int[parityBlkNum];
    int pos = 0;
    for (int i = 0; i < alignedStripe.chunks.length; i++) {
      if (alignedStripe.chunks[i] != null &&
          alignedStripe.chunks[i].state == StripingChunk.MISSING){
        decodeIndices[pos++] = i;
      }
    }

    int[] erasedIndices = Arrays.copyOf(decodeIndices, pos);
    return erasedIndices;
  }

  void clearFutures() {
    for (Future future : futures.keySet()) {
      future.cancel(false);
    }
    futures.clear();
  }

  boolean useDirectBuffer() {
    return decoder.preferDirectBuffer();
  }

  public void setReadTo(long readTo) {
    this.readTo = readTo;
  }

}
