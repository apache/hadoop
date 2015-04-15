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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletionService;
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
 *   1. Stateful read: TODO: HDFS-8033
 *   2. pread without decode support
 *     This is implemented by calculating the portion of read from each block and
 *     issuing requests to each DataNode in parallel.
 *   3. pread with decode support: TODO: will be supported after HDFS-7678
 *****************************************************************************/
public class DFSStripedInputStream extends DFSInputStream {
  /**
   * This method plans the read portion from each block in the stripe
   * @param dataBlkNum The number of data blocks in the striping group
   * @param cellSize The size of each striping cell
   * @param startInBlk Starting offset in the striped block
   * @param len Length of the read request
   * @param bufOffset  Initial offset in the result buffer
   * @return array of {@link ReadPortion}, each representing the portion of I/O
   *         for an individual block in the group
   */
  @VisibleForTesting
  static ReadPortion[] planReadPortions(final int dataBlkNum,
      final int cellSize, final long startInBlk, final int len, int bufOffset) {
    ReadPortion[] results = new ReadPortion[dataBlkNum];
    for (int i = 0; i < dataBlkNum; i++) {
      results[i] = new ReadPortion();
    }

    // cellIdxInBlk is the index of the cell in the block
    // E.g., cell_3 is the 2nd cell in blk_0
    int cellIdxInBlk = (int) (startInBlk / (cellSize * dataBlkNum));

    // blkIdxInGroup is the index of the block in the striped block group
    // E.g., blk_2 is the 3rd block in the group
    final int blkIdxInGroup = (int) (startInBlk / cellSize % dataBlkNum);
    results[blkIdxInGroup].startOffsetInBlock = cellSize * cellIdxInBlk +
        startInBlk % cellSize;
    boolean crossStripe = false;
    for (int i = 1; i < dataBlkNum; i++) {
      if (blkIdxInGroup + i >= dataBlkNum && !crossStripe) {
        cellIdxInBlk++;
        crossStripe = true;
      }
      results[(blkIdxInGroup + i) % dataBlkNum].startOffsetInBlock =
          cellSize * cellIdxInBlk;
    }

    int firstCellLen = Math.min(cellSize - (int) (startInBlk % cellSize), len);
    results[blkIdxInGroup].offsetsInBuf.add(bufOffset);
    results[blkIdxInGroup].lengths.add(firstCellLen);
    results[blkIdxInGroup].readLength += firstCellLen;

    int i = (blkIdxInGroup + 1) % dataBlkNum;
    for (int done = firstCellLen; done < len; done += cellSize) {
      ReadPortion rp = results[i];
      rp.offsetsInBuf.add(done + bufOffset);
      final int readLen = Math.min(len - done, cellSize);
      rp.lengths.add(readLen);
      rp.readLength += readLen;
      i = (i + 1) % dataBlkNum;
    }
    return results;
  }

  private int cellSize = HdfsConstants.BLOCK_STRIPED_CELL_SIZE;
  private final short dataBlkNum = HdfsConstants.NUM_DATA_BLOCKS;
  private final short parityBlkNum = HdfsConstants.NUM_PARITY_BLOCKS;

  DFSStripedInputStream(DFSClient dfsClient, String src, boolean verifyChecksum)
      throws IOException {
    super(dfsClient, src, verifyChecksum);
    DFSClient.LOG.debug("Creating an striped input stream for file " + src);
  }

  @Override
  public synchronized int read(final ByteBuffer buf) throws IOException {
    throw new UnsupportedActionException("Stateful read is not supported");
  }

  @Override
  public synchronized int read(final byte buf[], int off, int len)
      throws IOException {
    throw new UnsupportedActionException("Stateful read is not supported");
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
    return StripedBlockUtil.constructInternalBlock(lsb, i, cellSize,
        dataBlkNum, idx);
  }

  private LocatedBlock getBlockGroupAt(long offset) throws IOException {
    return super.getBlockAt(offset);
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
    LocatedBlock block = getBlockGroupAt(blockStartOffset);
    assert block instanceof LocatedStripedBlock : "NameNode" +
        " should return a LocatedStripedBlock for a striped file";
    LocatedStripedBlock blockGroup = (LocatedStripedBlock) block;

    // Planning the portion of I/O for each shard
    ReadPortion[] readPortions = planReadPortions(dataBlkNum, cellSize, start,
        len, offset);

    // Parse group to get chosen DN location
    LocatedBlock[] blks = StripedBlockUtil.
        parseStripedBlockGroup(blockGroup, cellSize, dataBlkNum, parityBlkNum);

    for (short i = 0; i < dataBlkNum; i++) {
      ReadPortion rp = readPortions[i];
      if (rp.readLength <= 0) {
        continue;
      }
      DatanodeInfo loc = blks[i].getLocations()[0];
      StorageType type = blks[i].getStorageTypes()[0];
      DNAddrPair dnAddr = new DNAddrPair(loc, NetUtils.createSocketAddr(
          loc.getXferAddr(dfsClient.getConf().isConnectToDnViaHostname())),
          type);
      Callable<Void> readCallable = getFromOneDataNode(dnAddr,
          blks[i].getStartOffset(), rp.startOffsetInBlock,
          rp.startOffsetInBlock + rp.readLength - 1, buf,
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

  private void waitNextCompletion(CompletionService<Void> stripedReadsService,
      Map<Future<Void>, Integer> futures) throws InterruptedException {
    if (futures.isEmpty()) {
      throw new InterruptedException("Futures already empty");
    }
    Future<Void> future = null;
    try {
      future = stripedReadsService.take();
      future.get();
      futures.remove(future);
    } catch (ExecutionException | CancellationException e) {
      // already logged in the Callable
      futures.remove(future);
    }
    throw new InterruptedException("let's retry");
  }

  public void setCellSize(int cellSize) {
    this.cellSize = cellSize;
  }

  /**
   * This class represents the portion of I/O associated with each block in the
   * striped block group.
   */
  static class ReadPortion {
    /**
     * startOffsetInBlock
     *     |
     *     v
     *     |<-lengths[0]->|<-  lengths[1]  ->|<-lengths[2]->|
     * +------------------+------------------+----------------+
     * |      cell_0      |      cell_3      |     cell_6     |  <- blk_0
     * +------------------+------------------+----------------+
     *   _/                \_______________________
     *  |                                          |
     *  v offsetsInBuf[0]                          v offsetsInBuf[1]
     * +------------------------------------------------------+
     * |  cell_0     |      cell_1 and cell_2      |cell_3 ...|   <- buf
     * |  (partial)  |    (from blk_1 and blk_2)   |          |
     * +------------------------------------------------------+
     */
    private long startOffsetInBlock = 0;
    private long readLength = 0;
    private final List<Integer> offsetsInBuf = new ArrayList<>();
    private final List<Integer> lengths = new ArrayList<>();

    int[] getOffsets() {
      int[] offsets = new int[offsetsInBuf.size()];
      for (int i = 0; i < offsets.length; i++) {
        offsets[i] = offsetsInBuf.get(i);
      }
      return offsets;
    }

    int[] getLengths() {
      int[] lens = new int[this.lengths.size()];
      for (int i = 0; i < lens.length; i++) {
        lens[i] = this.lengths.get(i);
      }
      return lens;
    }

    long getReadLength() {
      return readLength;
    }

    long getStartOffsetInBlock() {
      return startOffsetInBlock;
    }
  }
}
