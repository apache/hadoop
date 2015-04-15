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
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
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
  private int cellSize = HdfsConstants.BLOCK_STRIPED_CELL_SIZE;
  private ByteBuffer[] cellBuffers;
  private final short blockGroupBlocks = HdfsConstants.NUM_DATA_BLOCKS
      + HdfsConstants.NUM_PARITY_BLOCKS;
  private final short blockGroupDataBlocks = HdfsConstants.NUM_DATA_BLOCKS;
  private int curIdx = 0;
  /* bytes written in current block group */
  private long currentBlockGroupBytes = 0;

  //TODO: Use ErasureCoder interface (HDFS-7781)
  private RawErasureEncoder encoder;

  private StripedDataStreamer getLeadingStreamer() {
    return streamers.get(0);
  }

  private long getBlockGroupSize() {
    return blockSize * HdfsConstants.NUM_DATA_BLOCKS;
  }

  /** Construct a new output stream for creating a file. */
  DFSStripedOutputStream(DFSClient dfsClient, String src, HdfsFileStatus stat,
                         EnumSet<CreateFlag> flag, Progressable progress,
                         DataChecksum checksum, String[] favoredNodes)
                         throws IOException {
    super(dfsClient, src, stat, flag, progress, checksum, favoredNodes);
    DFSClient.LOG.info("Creating striped output stream");
    if (blockGroupBlocks <= 1) {
      throw new IOException("The block group must contain more than one block.");
    }

    cellBuffers = new ByteBuffer[blockGroupBlocks];
    List<BlockingQueue<LocatedBlock>> stripeBlocks = new ArrayList<>();

    for (int i = 0; i < blockGroupBlocks; i++) {
      stripeBlocks.add(new LinkedBlockingQueue<LocatedBlock>(blockGroupBlocks));
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
    encoder.initialize(blockGroupDataBlocks,
        blockGroupBlocks - blockGroupDataBlocks, cellSize);

    streamers = new ArrayList<>(blockGroupBlocks);
    for (short i = 0; i < blockGroupBlocks; i++) {
      StripedDataStreamer streamer = new StripedDataStreamer(stat, null,
          dfsClient, src, progress, checksum, cachingStrategy, byteArrayManager,
          i, stripeBlocks);
      if (favoredNodes != null && favoredNodes.length != 0) {
        streamer.setFavoredNodes(favoredNodes);
      }
      streamers.add(streamer);
    }

    refreshStreamer();
  }

  private void refreshStreamer() {
    streamer = streamers.get(curIdx);
  }

  private void moveToNextStreamer() {
    curIdx = (curIdx + 1) % blockGroupBlocks;
    refreshStreamer();
  }

  /**
   * encode the buffers.
   * After encoding, flip each buffer.
   *
   * @param buffers data buffers + parity buffers
   */
  private void encode(ByteBuffer[] buffers) {
    ByteBuffer[] dataBuffers = new ByteBuffer[blockGroupDataBlocks];
    ByteBuffer[] parityBuffers = new ByteBuffer[blockGroupBlocks - blockGroupDataBlocks];
    for (int i = 0; i < blockGroupBlocks; i++) {
      if (i < blockGroupDataBlocks) {
        dataBuffers[i] = buffers[i];
      } else {
        parityBuffers[i - blockGroupDataBlocks] = buffers[i];
      }
    }
    encoder.encode(dataBuffers, parityBuffers);
  }

  /**
   * Generate packets from a given buffer
   *
   * @param byteBuffer the given buffer to generate packets
   * @return packets generated
   * @throws IOException
   */
  private List<DFSPacket> generatePackets(ByteBuffer byteBuffer)
      throws IOException{
    List<DFSPacket> packets = new ArrayList<>();
    while (byteBuffer.remaining() > 0) {
      DFSPacket p = createPacket(packetSize, chunksPerPacket,
          streamer.getBytesCurBlock(),
          streamer.getAndIncCurrentSeqno(), false);
      int maxBytesToPacket = p.getMaxChunks() * bytesPerChecksum;
      int toWrite = byteBuffer.remaining() > maxBytesToPacket ?
          maxBytesToPacket: byteBuffer.remaining();
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
      if (curIdx == blockGroupDataBlocks) {
        //encode the data cells
        for (int k = 0; k < blockGroupDataBlocks; k++) {
          cellBuffers[k].flip();
        }
        encode(cellBuffers);
        for (int i = blockGroupDataBlocks; i < blockGroupBlocks; i++) {
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
    for (int i = 0; i< blockGroupBlocks; i++) {
      cellBuffers[i].clear();
    }
  }

  private int stripeDataSize() {
    return blockGroupDataBlocks * cellSize;
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
      streamer.setLastException(new IOException("Lease timeout of "
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
    StripedDataStreamer leadingStreamer = null;
    for (StripedDataStreamer streamer : streamers) {
      try {
        streamer.close(force);
        streamer.join();
        streamer.closeSocket();
        if (streamer.isLeadingStreamer()) {
          leadingStreamer = streamer;
        }
      } catch (InterruptedException e) {
        throw new IOException("Failed to shutdown streamer");
      } finally {
        streamer.setSocketToNull();
        setClosed();
      }
    }
    assert leadingStreamer != null : "One streamer should be leader";
    leadingStreamer.countTailingBlockGroupBytes();
  }

  @Override
  public synchronized void write(int b) throws IOException {
    super.write(b);
    currentBlockGroupBytes = (currentBlockGroupBytes + 1) % getBlockGroupSize();
  }

  @Override
  public synchronized void write(byte b[], int off, int len)
      throws IOException {
    super.write(b, off, len);
    currentBlockGroupBytes = (currentBlockGroupBytes + len) % getBlockGroupSize();
  }

  private void writeParityCellsForLastStripe() throws IOException{
    long parityBlkSize = StripedBlockUtil.getInternalBlockLength(
        currentBlockGroupBytes, cellSize, blockGroupDataBlocks,
        blockGroupDataBlocks + 1);
    if (parityBlkSize == 0 || currentBlockGroupBytes % stripeDataSize() == 0) {
      return;
    }
    int parityCellSize = parityBlkSize % cellSize == 0 ? cellSize :
                        (int) (parityBlkSize % cellSize);

    for (int i = 0; i < blockGroupBlocks; i++) {
      long internalBlkLen = StripedBlockUtil.getInternalBlockLength(
          currentBlockGroupBytes, cellSize, blockGroupDataBlocks, i);
      // Pad zero bytes to make all cells exactly the size of parityCellSize
      // If internal block is smaller than parity block, pad zero bytes.
      // Also pad zero bytes to all parity cells
      if (internalBlkLen < parityBlkSize || i >= blockGroupDataBlocks) {
        int position = cellBuffers[i].position();
        assert position <= parityCellSize : "If an internal block is smaller" +
            " than parity block, then its last cell should be small than last" +
            " parity cell";
        for (int j = 0; j < parityCellSize - position; j++) {
          cellBuffers[i].put((byte) 0);
        }
      }
      cellBuffers[i].flip();
    }
    encode(cellBuffers);

    //write parity cells
    curIdx = blockGroupDataBlocks;
    refreshStreamer();
    for (int i = blockGroupDataBlocks; i < blockGroupBlocks; i++) {
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
    for (int i = 0; i < blockGroupBlocks; i++) {
      byteArrayManager.release(cellBuffers[i].array());
      streamers.get(i).release();
    }
  }

  @Override
  protected synchronized void closeImpl() throws IOException {
    if (isClosed()) {
      IOException e = getLeadingStreamer().getLastException().getAndSet(null);
      if (e == null)
        return;
      else
        throw e;
    }

    try {
      // flush from all upper layers
      flushBuffer();
      if (currentPacket != null) {
        streamer.waitAndQueuePacket(currentPacket);
        currentPacket = null;
      }
      //if the last stripe is incomplete, generate and write parity cells
      writeParityCellsForLastStripe();

      for (int i = 0; i < blockGroupBlocks; i++) {
        curIdx = i;
        refreshStreamer();
        if (streamer.getBytesCurBlock()!= 0 ||
            currentBlockGroupBytes < getBlockGroupSize()) {
          // send an empty packet to mark the end of the block
          currentPacket = createPacket(0, 0, streamer.getBytesCurBlock(),
              streamer.getAndIncCurrentSeqno(), true);
          currentPacket.setSyncBlock(shouldSyncBlock);
        }
        // flush all data to Datanode
        flushInternal();
      }

      // get last block before destroying the streamer
      ExtendedBlock lastBlock = streamers.get(0).getBlock();
      closeThreads(false);
      TraceScope scope = Trace.startSpan("completeFile", Sampler.NEVER);
      try {
        completeFile(lastBlock);
      } finally {
        scope.close();
      }
      dfsClient.endFileLease(fileId);
    } catch (ClosedChannelException e) {
    } finally {
      setClosed();
    }
  }

}
