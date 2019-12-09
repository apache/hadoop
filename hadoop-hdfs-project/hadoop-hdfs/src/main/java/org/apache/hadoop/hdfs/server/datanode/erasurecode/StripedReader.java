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
package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient.CorruptedBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.BlockReadStats;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.StripingChunkReadResult;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;

/**
 * Manage striped readers that performs reading of block data from remote to
 * serve input data for the erasure decoding.
 */
@InterfaceAudience.Private
class StripedReader {
  private static final Logger LOG = DataNode.LOG;

  private final int stripedReadTimeoutInMills;
  private final int stripedReadBufferSize;

  private StripedReconstructor reconstructor;
  private final DataNode datanode;
  private final Configuration conf;

  private final int dataBlkNum;
  private final int parityBlkNum;


  private DataChecksum checksum;
  // Striped read buffer size
  private int bufferSize;
  private int[] successList;

  private final int minRequiredSources;
  // the number of xmits used by the re-construction task.
  private final int xmits;
  // The buffers and indices for striped blocks whose length is 0
  private ByteBuffer[] zeroStripeBuffers;
  private short[] zeroStripeIndices;

  // sources
  private final byte[] liveIndices;
  private final DatanodeInfo[] sources;

  private final List<StripedBlockReader> readers;

  private final Map<Future<BlockReadStats>, Integer> futures = new HashMap<>();
  private final CompletionService<BlockReadStats> readService;

  StripedReader(StripedReconstructor reconstructor, DataNode datanode,
      Configuration conf, StripedReconstructionInfo stripedReconInfo) {
    stripedReadTimeoutInMills = conf.getInt(
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_TIMEOUT_MILLIS_KEY,
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_TIMEOUT_MILLIS_DEFAULT);
    stripedReadBufferSize = conf.getInt(
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_KEY,
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_DEFAULT);

    this.reconstructor = reconstructor;
    this.datanode = datanode;
    this.conf = conf;

    dataBlkNum = stripedReconInfo.getEcPolicy().getNumDataUnits();
    parityBlkNum = stripedReconInfo.getEcPolicy().getNumParityUnits();

    int cellsNum = (int) ((stripedReconInfo.getBlockGroup().getNumBytes() - 1)
        / stripedReconInfo.getEcPolicy().getCellSize() + 1);
    minRequiredSources = Math.min(cellsNum, dataBlkNum);

    if (minRequiredSources < dataBlkNum) {
      int zeroStripNum = dataBlkNum - minRequiredSources;
      zeroStripeBuffers = new ByteBuffer[zeroStripNum];
      zeroStripeIndices = new short[zeroStripNum];
    }

    // It is calculated by the maximum number of connections from either sources
    // or targets.
    xmits = Math.max(minRequiredSources,
        stripedReconInfo.getTargets() != null ?
        stripedReconInfo.getTargets().length : 0);

    this.liveIndices = stripedReconInfo.getLiveIndices();
    assert liveIndices != null;
    this.sources = stripedReconInfo.getSources();
    assert sources != null;

    readers = new ArrayList<>(sources.length);
    readService = reconstructor.createReadService();

    Preconditions.checkArgument(liveIndices.length >= minRequiredSources,
        "No enough live striped blocks.");
    Preconditions.checkArgument(liveIndices.length == sources.length,
        "liveBlockIndices and source datanodes should match");
  }

  void init() throws IOException {
    initReaders();

    initBufferSize();

    initZeroStrip();
  }

  private void initReaders() throws IOException {
    // Store the array indices of source DNs we have read successfully.
    // In each iteration of read, the successList list may be updated if
    // some source DN is corrupted or slow. And use the updated successList
    // list of DNs for next iteration read.
    successList = new int[minRequiredSources];

    StripedBlockReader reader;
    int nSuccess = 0;
    for (int i = 0; i < sources.length && nSuccess < minRequiredSources; i++) {
      reader = createReader(i, 0);
      readers.add(reader);
      if (reader.getBlockReader() != null) {
        initOrVerifyChecksum(reader);
        successList[nSuccess++] = i;
      }
    }

    if (nSuccess < minRequiredSources) {
      String error = "Can't find minimum sources required by "
          + "reconstruction, block id: "
          + reconstructor.getBlockGroup().getBlockId();
      throw new IOException(error);
    }
  }

  StripedBlockReader createReader(int idxInSources, long offsetInBlock) {
    return new StripedBlockReader(this, datanode,
        conf, liveIndices[idxInSources],
        reconstructor.getBlock(liveIndices[idxInSources]),
        sources[idxInSources], offsetInBlock);
  }

  private void initBufferSize() {
    int bytesPerChecksum = checksum.getBytesPerChecksum();
    // The bufferSize is flat to divide bytesPerChecksum
    int readBufferSize = stripedReadBufferSize;
    bufferSize = readBufferSize < bytesPerChecksum ? bytesPerChecksum :
        readBufferSize - readBufferSize % bytesPerChecksum;
  }

  // init checksum from block reader
  private void initOrVerifyChecksum(StripedBlockReader reader) {
    if (checksum == null) {
      checksum = reader.getBlockReader().getDataChecksum();
    } else {
      assert reader.getBlockReader().getDataChecksum().equals(checksum);
    }
  }

  protected ByteBuffer allocateReadBuffer() {
    return reconstructor.allocateBuffer(getBufferSize());
  }

  private void initZeroStrip() {
    if (zeroStripeBuffers != null) {
      for (int i = 0; i < zeroStripeBuffers.length; i++) {
        zeroStripeBuffers[i] = reconstructor.allocateBuffer(bufferSize);
      }
    }

    BitSet bitset = reconstructor.getLiveBitSet();
    int k = 0;
    for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
      if (!bitset.get(i)) {
        if (reconstructor.getBlockLen(i) <= 0) {
          zeroStripeIndices[k++] = (short)i;
        }
      }
    }
  }

  private int getReadLength(int index, int reconstructLength) {
    // the reading length should not exceed the length for reconstruction
    long blockLen = reconstructor.getBlockLen(index);
    long remaining = blockLen - reconstructor.getPositionInBlock();
    return (int) Math.min(remaining, reconstructLength);
  }

  ByteBuffer[] getInputBuffers(int toReconstructLen) {
    ByteBuffer[] inputs = new ByteBuffer[dataBlkNum + parityBlkNum];

    for (int i = 0; i < successList.length; i++) {
      int index = successList[i];
      StripedBlockReader reader = getReader(index);
      ByteBuffer buffer = reader.getReadBuffer();
      paddingBufferToLen(buffer, toReconstructLen);
      inputs[reader.getIndex()] = (ByteBuffer)buffer.flip();
    }

    if (successList.length < dataBlkNum) {
      for (int i = 0; i < zeroStripeBuffers.length; i++) {
        ByteBuffer buffer = zeroStripeBuffers[i];
        paddingBufferToLen(buffer, toReconstructLen);
        int index = zeroStripeIndices[i];
        inputs[index] = (ByteBuffer)buffer.flip();
      }
    }

    return inputs;
  }

  private void paddingBufferToLen(ByteBuffer buffer, int len) {
    if (len > buffer.limit()) {
      buffer.limit(len);
    }
    int toPadding = len - buffer.position();
    for (int i = 0; i < toPadding; i++) {
      buffer.put((byte) 0);
    }
  }

  /**
   * Read from minimum source DNs required for reconstruction in the iteration.
   * First try the success list which we think they are the best DNs
   * If source DN is corrupt or slow, try to read some other source DN,
   * and will update the success list.
   *
   * Remember the updated success list and return it for following
   * operations and next iteration read.
   *
   * @param reconstructLength the length to reconstruct.
   * @return updated success list of source DNs we do real read
   * @throws IOException
   */
  void readMinimumSources(int reconstructLength) throws IOException {
    CorruptedBlocks corruptedBlocks = new CorruptedBlocks();
    try {
      successList = doReadMinimumSources(reconstructLength, corruptedBlocks);
    } finally {
      // report corrupted blocks to NN
      datanode.reportCorruptedBlocks(corruptedBlocks);
    }
  }

  int[] doReadMinimumSources(int reconstructLength,
                             CorruptedBlocks corruptedBlocks)
      throws IOException {
    Preconditions.checkArgument(reconstructLength >= 0 &&
        reconstructLength <= bufferSize);
    int nSuccess = 0;
    int[] newSuccess = new int[minRequiredSources];
    BitSet usedFlag = new BitSet(sources.length);
    /*
     * Read from minimum source DNs required, the success list contains
     * source DNs which we think best.
     */
    for (int i = 0; i < minRequiredSources; i++) {
      StripedBlockReader reader = readers.get(successList[i]);
      int toRead = getReadLength(liveIndices[successList[i]],
          reconstructLength);
      if (toRead > 0) {
        Callable<BlockReadStats> readCallable =
            reader.readFromBlock(toRead, corruptedBlocks);
        Future<BlockReadStats> f = readService.submit(readCallable);
        futures.put(f, successList[i]);
      } else {
        // If the read length is 0, we don't need to do real read
        reader.getReadBuffer().position(0);
        newSuccess[nSuccess++] = successList[i];
      }
      usedFlag.set(successList[i]);
    }

    while (!futures.isEmpty()) {
      try {
        StripingChunkReadResult result =
            StripedBlockUtil.getNextCompletedStripedRead(
                readService, futures, stripedReadTimeoutInMills);
        int resultIndex = -1;
        if (result.state == StripingChunkReadResult.SUCCESSFUL) {
          resultIndex = result.index;
        } else if (result.state == StripingChunkReadResult.FAILED) {
          // If read failed for some source DN, we should not use it anymore
          // and schedule read from another source DN.
          StripedBlockReader failedReader = readers.get(result.index);
          failedReader.closeBlockReader();
          resultIndex = scheduleNewRead(usedFlag,
              reconstructLength, corruptedBlocks);
        } else if (result.state == StripingChunkReadResult.TIMEOUT) {
          // If timeout, we also schedule a new read.
          resultIndex = scheduleNewRead(usedFlag,
              reconstructLength, corruptedBlocks);
        }
        if (resultIndex >= 0) {
          newSuccess[nSuccess++] = resultIndex;
          if (nSuccess >= minRequiredSources) {
            // cancel remaining reads if we read successfully from minimum
            // number of source DNs required by reconstruction.
            cancelReads(futures.keySet());
            futures.clear();
            break;
          }
        }
      } catch (InterruptedException e) {
        LOG.info("Read data interrupted.", e);
        cancelReads(futures.keySet());
        futures.clear();
        break;
      }
    }

    if (nSuccess < minRequiredSources) {
      String error = "Can't read data from minimum number of sources "
          + "required by reconstruction, block id: " +
          reconstructor.getBlockGroup().getBlockId();
      throw new IOException(error);
    }

    return newSuccess;
  }

  /**
   * Schedule a read from some new source DN if some DN is corrupted
   * or slow, this is called from the read iteration.
   * Initially we may only have <code>minRequiredSources</code> number of
   * StripedBlockReader.
   * If the position is at the end of target block, don't need to do
   * real read, and return the array index of source DN, otherwise -1.
   *
   * @param used the used source DNs in this iteration.
   * @return the array index of source DN if don't need to do real read.
   */
  private int scheduleNewRead(BitSet used, int reconstructLength,
                              CorruptedBlocks corruptedBlocks) {
    StripedBlockReader reader = null;
    // step1: initially we may only have <code>minRequiredSources</code>
    // number of StripedBlockReader, and there may be some source DNs we never
    // read before, so will try to create StripedBlockReader for one new source
    // DN and try to read from it. If found, go to step 3.
    int m = readers.size();
    int toRead = 0;
    while (reader == null && m < sources.length) {
      reader = createReader(m, reconstructor.getPositionInBlock());
      readers.add(reader);
      toRead = getReadLength(liveIndices[m], reconstructLength);
      if (toRead > 0) {
        if (reader.getBlockReader() == null) {
          reader = null;
          m++;
        }
      } else {
        used.set(m);
        return m;
      }
    }

    // step2: if there is no new source DN we can use, try to find a source
    // DN we ever read from but because some reason, e.g., slow, it
    // is not in the success DN list at the begin of this iteration, so
    // we have not tried it in this iteration. Now we have a chance to
    // revisit it again.
    for (int i = 0; reader == null && i < readers.size(); i++) {
      if (!used.get(i)) {
        StripedBlockReader stripedReader = readers.get(i);
        toRead = getReadLength(liveIndices[i], reconstructLength);
        if (toRead > 0) {
          stripedReader.closeBlockReader();
          stripedReader.resetBlockReader(reconstructor.getPositionInBlock());
          if (stripedReader.getBlockReader() != null) {
            stripedReader.getReadBuffer().position(0);
            m = i;
            reader = stripedReader;
          }
        } else {
          used.set(i);
          stripedReader.getReadBuffer().position(0);
          return i;
        }
      }
    }

    // step3: schedule if find a correct source DN and need to do real read.
    if (reader != null) {
      Callable<BlockReadStats> readCallable =
          reader.readFromBlock(toRead, corruptedBlocks);
      Future<BlockReadStats> f = readService.submit(readCallable);
      futures.put(f, m);
      used.set(m);
    }

    return -1;
  }

  // Cancel all reads.
  private static void cancelReads(Collection<Future<BlockReadStats>> futures) {
    for (Future<BlockReadStats> future : futures) {
      future.cancel(true);
    }
  }

  void close() {
    if (zeroStripeBuffers != null) {
      for (ByteBuffer zeroStripeBuffer : zeroStripeBuffers) {
        reconstructor.freeBuffer(zeroStripeBuffer);
      }
    }
    zeroStripeBuffers = null;

    for (StripedBlockReader reader : readers) {
      reconstructor.freeBuffer(reader.getReadBuffer());
      reader.freeReadBuffer();
      reader.closeBlockReader();
    }
  }

  StripedReconstructor getReconstructor() {
    return reconstructor;
  }

  StripedBlockReader getReader(int i) {
    return readers.get(i);
  }

  int getBufferSize() {
    return bufferSize;
  }

  DataChecksum getChecksum() {
    return checksum;
  }

  void clearBuffers() {
    if (zeroStripeBuffers != null) {
      for (ByteBuffer zeroStripeBuffer : zeroStripeBuffers) {
        zeroStripeBuffer.clear();
      }
    }

    for (StripedBlockReader reader : readers) {
      if (reader.getReadBuffer() != null) {
        reader.getReadBuffer().clear();
      }
    }
  }

  InetSocketAddress getSocketAddress4Transfer(DatanodeInfo dnInfo) {
    return reconstructor.getSocketAddress4Transfer(dnInfo);
  }

  CachingStrategy getCachingStrategy() {
    return reconstructor.getCachingStrategy();
  }

  /**
   * Return the xmits of this EC reconstruction task.
   * <p>
   * DN uses it to coordinate with NN to adjust the speed of scheduling the
   * EC reconstruction tasks to this DN.
   *
   * @return the xmits of this reconstruction task.
   */
  int getXmits() {
    return xmits;
  }
}
