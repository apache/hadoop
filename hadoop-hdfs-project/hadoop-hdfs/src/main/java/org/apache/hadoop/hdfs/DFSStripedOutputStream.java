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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.hdfs.protocol.ECInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;


/****************************************************************
 * The DFSStripedOutputStream class supports writing files in striped
 * layout. Each stripe contains a sequence of cells and multiple
 * {@link StripedDataStreamer}s in DFSStripedOutputStream are responsible
 * for writing the cells to different datanodes.
 *
 ****************************************************************/

@InterfaceAudience.Private
public class DFSStripedOutputStream extends DFSOutputStream {

  private final List<StripedDataStreamer> streamers;
  /**
   * Size of each striping cell, must be a multiple of bytesPerChecksum
   */
  private final ECInfo ecInfo;
  private final int cellSize;
  // checksum buffer, we only need to calculate checksum for parity blocks
  private byte[] checksumBuf;
  private ByteBuffer[] cellBuffers;

  private final short numAllBlocks;
  private final short numDataBlocks;

  private int curIdx = 0;
  /* bytes written in current block group */
  //private long currentBlockGroupBytes = 0;

  //TODO: Use ErasureCoder interface (HDFS-7781)
  private RawErasureEncoder encoder;

  private StripedDataStreamer getLeadingStreamer() {
    return streamers.get(0);
  }

  private long getBlockGroupSize() {
    return blockSize * numDataBlocks;
  }

  /** Construct a new output stream for creating a file. */
  DFSStripedOutputStream(DFSClient dfsClient, String src, HdfsFileStatus stat,
                         EnumSet<CreateFlag> flag, Progressable progress,
                         DataChecksum checksum, String[] favoredNodes)
                         throws IOException {
    super(dfsClient, src, stat, flag, progress, checksum, favoredNodes);
    DFSClient.LOG.info("Creating striped output stream");

    // ECInfo is restored from NN just before writing striped files.
    ecInfo = dfsClient.getErasureCodingInfo(src);
    cellSize = ecInfo.getSchema().getChunkSize();
    numAllBlocks = (short)(ecInfo.getSchema().getNumDataUnits()
        + ecInfo.getSchema().getNumParityUnits());
    numDataBlocks = (short)ecInfo.getSchema().getNumDataUnits();

    checkConfiguration();

    checksumBuf = new byte[getChecksumSize() * (cellSize / bytesPerChecksum)];
    cellBuffers = new ByteBuffer[numAllBlocks];
    List<BlockingQueue<LocatedBlock>> stripeBlocks = new ArrayList<>();

    for (int i = 0; i < numAllBlocks; i++) {
      stripeBlocks.add(new LinkedBlockingQueue<LocatedBlock>(numAllBlocks));
      try {
        cellBuffers[i] = ByteBuffer.wrap(byteArrayManager.newByteArray(cellSize));
      } catch (InterruptedException ie) {
        final InterruptedIOException iioe = new InterruptedIOException(
            "create cell buffers");
        iioe.initCause(ie);
        throw iioe;
      }
    }
    encoder = new RSRawEncoder();
    encoder.initialize(numDataBlocks,
        numAllBlocks - numDataBlocks, cellSize);

    List<StripedDataStreamer> s = new ArrayList<>(numAllBlocks);
    for (short i = 0; i < numAllBlocks; i++) {
      StripedDataStreamer streamer = new StripedDataStreamer(stat, null,
          dfsClient, src, progress, checksum, cachingStrategy, byteArrayManager,
          i, stripeBlocks, favoredNodes);
      s.add(streamer);
    }
    streamers = Collections.unmodifiableList(s);

    refreshStreamer();
  }

  private void checkConfiguration() {
    if (cellSize % bytesPerChecksum != 0) {
      throw new HadoopIllegalArgumentException("Invalid values: "
          + DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY + " (=" + bytesPerChecksum
          + ") must divide cell size (=" + cellSize + ").");
    }
  }

  private void refreshStreamer() {
    streamer = streamers.get(curIdx);
  }

  private void moveToNextStreamer() {
    curIdx = (curIdx + 1) % numAllBlocks;
    refreshStreamer();
  }

  /**
   * encode the buffers.
   * After encoding, flip each buffer.
   *
   * @param buffers data buffers + parity buffers
   */
  private void encode(ByteBuffer[] buffers) {
    ByteBuffer[] dataBuffers = new ByteBuffer[numDataBlocks];
    ByteBuffer[] parityBuffers = new ByteBuffer[numAllBlocks - numDataBlocks];
    for (int i = 0; i < numAllBlocks; i++) {
      if (i < numDataBlocks) {
        dataBuffers[i] = buffers[i];
      } else {
        parityBuffers[i - numDataBlocks] = buffers[i];
      }
    }
    encoder.encode(dataBuffers, parityBuffers);
  }

  /**
   * Generate packets from a given buffer. This is only used for streamers
   * writing parity blocks.
   *
   * @param byteBuffer the given buffer to generate packets
   * @return packets generated
   * @throws IOException
   */
  private List<DFSPacket> generatePackets(ByteBuffer byteBuffer)
      throws IOException{
    List<DFSPacket> packets = new ArrayList<>();
    assert byteBuffer.hasArray();
    getDataChecksum().calculateChunkedSums(byteBuffer.array(), 0,
        byteBuffer.remaining(), checksumBuf, 0);
    int ckOff = 0;
    while (byteBuffer.remaining() > 0) {
      DFSPacket p = createPacket(packetSize, chunksPerPacket,
          streamer.getBytesCurBlock(),
          streamer.getAndIncCurrentSeqno(), false);
      int maxBytesToPacket = p.getMaxChunks() * bytesPerChecksum;
      int toWrite = byteBuffer.remaining() > maxBytesToPacket ?
          maxBytesToPacket: byteBuffer.remaining();
      int ckLen = ((toWrite - 1) / bytesPerChecksum + 1) * getChecksumSize();
      p.writeChecksum(checksumBuf, ckOff, ckLen);
      ckOff += ckLen;
      p.writeData(byteBuffer, toWrite);
      streamer.incBytesCurBlock(toWrite);
      packets.add(p);
    }
    return packets;
  }

  @Override
  protected synchronized void writeChunk(byte[] b, int offset, int len,
      byte[] checksum, int ckoff, int cklen) throws IOException {
    super.writeChunk(b, offset, len, checksum, ckoff, cklen);

    if (getSizeOfCellnBuffer(curIdx) <= cellSize) {
      addToCellBuffer(b, offset, len);
    } else {
      String msg = "Writing a chunk should not overflow the cell buffer.";
      DFSClient.LOG.info(msg);
      throw new IOException(msg);
    }

    // If current packet has not been enqueued for transmission,
    // but the cell buffer is full, we need to enqueue the packet
    if (currentPacket != null && getSizeOfCellnBuffer(curIdx) == cellSize) {
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("DFSClient writeChunk cell buffer full seqno=" +
            currentPacket.getSeqno() +
            ", curIdx=" + curIdx +
            ", src=" + src +
            ", bytesCurBlock=" + streamer.getBytesCurBlock() +
            ", blockSize=" + blockSize +
            ", appendChunk=" + streamer.getAppendChunk());
      }
      streamer.waitAndQueuePacket(currentPacket);
      currentPacket = null;
      adjustChunkBoundary();
      endBlock();
    }

    // Two extra steps are needed when a striping cell is full:
    // 1. Forward the current index pointer
    // 2. Generate parity packets if a full stripe of data cells are present
    if (getSizeOfCellnBuffer(curIdx) == cellSize) {
      //move curIdx to next cell
      moveToNextStreamer();
      //When all data cells in a stripe are ready, we need to encode
      //them and generate some parity cells. These cells will be
      //converted to packets and put to their DataStreamer's queue.
      if (curIdx == numDataBlocks) {
        //encode the data cells
        for (int k = 0; k < numDataBlocks; k++) {
          cellBuffers[k].flip();
        }
        encode(cellBuffers);
        for (int i = numDataBlocks; i < numAllBlocks; i++) {
          ByteBuffer parityBuffer = cellBuffers[i];
          List<DFSPacket> packets = generatePackets(parityBuffer);
          for (DFSPacket p : packets) {
            currentPacket = p;
            streamer.waitAndQueuePacket(currentPacket);
            currentPacket = null;
          }
          endBlock();
          moveToNextStreamer();
        }
        //read next stripe to cellBuffers
        clearCellBuffers();
      }
    }
  }

  private void addToCellBuffer(byte[] b, int off, int len) {
    cellBuffers[curIdx].put(b, off, len);
  }

  private int getSizeOfCellnBuffer(int cellIndex) {
    return cellBuffers[cellIndex].position();
  }

  private void clearCellBuffers() {
    for (int i = 0; i< numAllBlocks; i++) {
      cellBuffers[i].clear();
      if (i >= numDataBlocks) {
        Arrays.fill(cellBuffers[i].array(), (byte) 0);
      }
    }
  }

  private int stripeDataSize() {
    return numDataBlocks * cellSize;
  }

  private void notSupported(String headMsg)
      throws IOException{
      throw new IOException(
          headMsg + " is now not supported for striping layout.");
  }

  @Override
  public void hflush() throws IOException {
    notSupported("hflush");
  }

  @Override
  public void hsync() throws IOException {
    notSupported("hsync");
  }

  @Override
  protected synchronized void start() {
    for (StripedDataStreamer streamer : streamers) {
      streamer.start();
    }
  }

  @Override
  synchronized void abort() throws IOException {
    if (isClosed()) {
      return;
    }
    for (StripedDataStreamer streamer : streamers) {
      streamer.getLastException().set(new IOException("Lease timeout of "
          + (dfsClient.getConf().getHdfsTimeout()/1000) +
          " seconds expired."));
    }
    closeThreads(true);
    dfsClient.endFileLease(fileId);
  }

  //TODO: Handle slow writers (HDFS-7786)
  //Cuurently only check if the leading streamer is terminated
  boolean isClosed() {
    return closed || getLeadingStreamer().streamerClosed();
  }

  // shutdown datastreamer and responseprocessor threads.
  // interrupt datastreamer if force is true
  @Override
  protected void closeThreads(boolean force) throws IOException {
    int index = 0;
    boolean exceptionOccurred = false;
    for (StripedDataStreamer streamer : streamers) {
      try {
        streamer.close(force);
        streamer.join();
        streamer.closeSocket();
      } catch (InterruptedException | IOException e) {
        DFSClient.LOG.error("Failed to shutdown streamer: name="
            + streamer.getName() + ", index=" + index + ", file=" + src, e);
        exceptionOccurred = true;
      } finally {
        streamer.setSocketToNull();
        setClosed();
        index++;
      }
    }
    if (exceptionOccurred) {
      throw new IOException("Failed to shutdown streamer");
    }
  }

  /**
   * Simply add bytesCurBlock together. Note that this result is not accurately
   * the size of the block group.
   */
  private long getCurrentSumBytes() {
    long sum = 0;
    for (int i = 0; i < numDataBlocks; i++) {
      sum += streamers.get(i).getBytesCurBlock();
    }
    return sum;
  }

  private void writeParityCellsForLastStripe() throws IOException {
    final long currentBlockGroupBytes = getCurrentSumBytes();
    if (currentBlockGroupBytes % stripeDataSize() == 0) {
      return;
    }
    long firstCellSize = getLeadingStreamer().getBytesCurBlock() % cellSize;
    long parityCellSize = firstCellSize > 0 && firstCellSize < cellSize ?
        firstCellSize : cellSize;

    for (int i = 0; i < numAllBlocks; i++) {
      // Pad zero bytes to make all cells exactly the size of parityCellSize
      // If internal block is smaller than parity block, pad zero bytes.
      // Also pad zero bytes to all parity cells
      int position = cellBuffers[i].position();
      assert position <= parityCellSize : "If an internal block is smaller" +
          " than parity block, then its last cell should be small than last" +
          " parity cell";
      for (int j = 0; j < parityCellSize - position; j++) {
        cellBuffers[i].put((byte) 0);
      }
      cellBuffers[i].flip();
    }
    encode(cellBuffers);

    // write parity cells
    curIdx = numDataBlocks;
    refreshStreamer();
    for (int i = numDataBlocks; i < numAllBlocks; i++) {
      ByteBuffer parityBuffer = cellBuffers[i];
      List<DFSPacket> packets = generatePackets(parityBuffer);
      for (DFSPacket p : packets) {
        currentPacket = p;
        streamer.waitAndQueuePacket(currentPacket);
        currentPacket = null;
      }
      endBlock();
      moveToNextStreamer();
    }

    clearCellBuffers();
  }

  @Override
  void setClosed() {
    super.setClosed();
    for (int i = 0; i < numAllBlocks; i++) {
      byteArrayManager.release(cellBuffers[i].array());
      streamers.get(i).release();
    }
  }

  @Override
  protected synchronized void closeImpl() throws IOException {
    if (isClosed()) {
      getLeadingStreamer().getLastException().check(true);
      return;
    }

    try {
      // flush from all upper layers
      flushBuffer();
      if (currentPacket != null) {
        streamer.waitAndQueuePacket(currentPacket);
        currentPacket = null;
      }
      // if the last stripe is incomplete, generate and write parity cells
      writeParityCellsForLastStripe();

      for (int i = 0; i < numAllBlocks; i++) {
        curIdx = i;
        refreshStreamer();
        if (streamer.getBytesCurBlock() > 0) {
          // send an empty packet to mark the end of the block
          currentPacket = createPacket(0, 0, streamer.getBytesCurBlock(),
              streamer.getAndIncCurrentSeqno(), true);
          currentPacket.setSyncBlock(shouldSyncBlock);
        }
        // flush all data to Datanode
        flushInternal();
      }

      closeThreads(false);
      final ExtendedBlock lastBlock = getCommittedBlock();
      TraceScope scope = Trace.startSpan("completeFile", Sampler.NEVER);
      try {
        completeFile(lastBlock);
      } finally {
        scope.close();
      }
      dfsClient.endFileLease(fileId);
    } catch (ClosedChannelException ignored) {
    } finally {
      setClosed();
    }
  }

  /**
   * Generate the block which is reported and will be committed in NameNode.
   * Need to go through all the streamers writing data blocks and add their
   * bytesCurBlock together. Note that at this time all streamers have been
   * closed. Also this calculation can cover streamers with writing failures.
   *
   * @return An ExtendedBlock with size of the whole block group.
   */
  ExtendedBlock getCommittedBlock() throws IOException {
    ExtendedBlock b = getLeadingStreamer().getBlock();
    if (b == null) {
      return null;
    }
    final ExtendedBlock block = new ExtendedBlock(b);
    final boolean atBlockGroupBoundary =
        getLeadingStreamer().getBytesCurBlock() == 0 &&
            getLeadingStreamer().getBlock() != null &&
            getLeadingStreamer().getBlock().getNumBytes() > 0;
    for (int i = 1; i < numDataBlocks; i++) {
      block.setNumBytes(block.getNumBytes() +
          (atBlockGroupBoundary ? streamers.get(i).getBlock().getNumBytes() :
              streamers.get(i).getBytesCurBlock()));
    }
    return block;
  }
}
