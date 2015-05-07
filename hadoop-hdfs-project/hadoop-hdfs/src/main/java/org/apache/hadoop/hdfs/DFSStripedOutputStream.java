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
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import com.google.common.base.Preconditions;


/****************************************************************
 * The DFSStripedOutputStream class supports writing files in striped
 * layout. Each stripe contains a sequence of cells and multiple
 * {@link StripedDataStreamer}s in DFSStripedOutputStream are responsible
 * for writing the cells to different datanodes.
 *
 ****************************************************************/

@InterfaceAudience.Private
public class DFSStripedOutputStream extends DFSOutputStream {
  /** Coordinate the communication between the streamers. */
  static class Coordinator {
    private final List<BlockingQueue<ExtendedBlock>> endBlocks;
    private final List<BlockingQueue<LocatedBlock>> stripedBlocks;
    private volatile boolean shouldLocateFollowingBlock = false;

    Coordinator(final int numDataBlocks, final int numAllBlocks) {
      endBlocks = new ArrayList<>(numDataBlocks);
      for (int i = 0; i < numDataBlocks; i++) {
        endBlocks.add(new LinkedBlockingQueue<ExtendedBlock>(1));
      }

      stripedBlocks = new ArrayList<>(numAllBlocks);
      for (int i = 0; i < numAllBlocks; i++) {
        stripedBlocks.add(new LinkedBlockingQueue<LocatedBlock>(1));
      }
    }

    boolean shouldLocateFollowingBlock() {
      return shouldLocateFollowingBlock;
    }

    void putEndBlock(int i, ExtendedBlock block) {
      shouldLocateFollowingBlock = true;

      final boolean b = endBlocks.get(i).offer(block);
      Preconditions.checkState(b, "Failed to add " + block
          + " to endBlocks queue, i=" + i);
    }

    ExtendedBlock getEndBlock(int i) throws InterruptedIOException {
      try {
        return endBlocks.get(i).poll(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw DFSUtil.toInterruptedIOException(
            "getEndBlock interrupted, i=" + i, e);
      }
    }

    void setBytesEndBlock(int i, long newBytes, ExtendedBlock block) {
      ExtendedBlock b = endBlocks.get(i).peek();
      if (b == null) {
        // streamer just has failed, put end block and continue
        b = block;
        putEndBlock(i, b);
      }
      b.setNumBytes(newBytes);
    }

    void putStripedBlock(int i, LocatedBlock block) throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("putStripedBlock " + block + ", i=" + i);
      }
      final boolean b = stripedBlocks.get(i).offer(block);
      if (!b) {
        throw new IOException("Failed: " + block + ", i=" + i);
      }
    }

    LocatedBlock getStripedBlock(int i) throws IOException {
      final LocatedBlock lb;
      try {
        lb = stripedBlocks.get(i).poll(90, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw DFSUtil.toInterruptedIOException("getStripedBlock interrupted", e);
      }

      if (lb == null) {
        throw new IOException("Failed: i=" + i);
      }
      return lb;
    }
  }

  /** Buffers for writing the data and parity cells of a strip. */
  class CellBuffers {
    private final ByteBuffer[] buffers;
    private final byte[][] checksumArrays;

    CellBuffers(int numParityBlocks) throws InterruptedException{
      if (cellSize % bytesPerChecksum != 0) {
        throw new HadoopIllegalArgumentException("Invalid values: "
            + DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY + " (="
            + bytesPerChecksum + ") must divide cell size (=" + cellSize + ").");
      }

      checksumArrays = new byte[numParityBlocks][];
      final int size = getChecksumSize() * (cellSize / bytesPerChecksum);
      for (int i = 0; i < checksumArrays.length; i++) {
        checksumArrays[i] = new byte[size];
      }

      buffers = new ByteBuffer[numAllBlocks];
      for (int i = 0; i < buffers.length; i++) {
        buffers[i] = ByteBuffer.wrap(byteArrayManager.newByteArray(cellSize));
      }
    }

    private ByteBuffer[] getBuffers() {
      return buffers;
    }

    byte[] getChecksumArray(int i) {
      return checksumArrays[i - numDataBlocks];
    }

    private int addTo(int i, byte[] b, int off, int len) {
      final ByteBuffer buf = buffers[i];
      final int pos = buf.position() + len;
      Preconditions.checkState(pos <= cellSize);
      buf.put(b, off, len);
      return pos;
    }

    private void clear() {
      for (int i = 0; i< numAllBlocks; i++) {
        buffers[i].clear();
        if (i >= numDataBlocks) {
          Arrays.fill(buffers[i].array(), (byte) 0);
        }
      }
    }

    private void release() {
      for (int i = 0; i < numAllBlocks; i++) {
        byteArrayManager.release(buffers[i].array());
      }
    }

    private void flipDataBuffers() {
      for (int i = 0; i < numDataBlocks; i++) {
        buffers[i].flip();
      }
    }
  }

  private final Coordinator coordinator;
  private final CellBuffers cellBuffers;
  private final RawErasureEncoder encoder;
  private final List<StripedDataStreamer> streamers;

  /** Size of each striping cell, must be a multiple of bytesPerChecksum */
  private final int cellSize;
  private final int numAllBlocks;
  private final int numDataBlocks;

  private StripedDataStreamer getLeadingStreamer() {
    return streamers.get(0);
  }

  /** Construct a new output stream for creating a file. */
  DFSStripedOutputStream(DFSClient dfsClient, String src, HdfsFileStatus stat,
                         EnumSet<CreateFlag> flag, Progressable progress,
                         DataChecksum checksum, String[] favoredNodes)
                         throws IOException {
    super(dfsClient, src, stat, flag, progress, checksum, favoredNodes);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating DFSStripedOutputStream for " + src);
    }

    final ECSchema schema = stat.getECSchema();
    final int numParityBlocks = schema.getNumParityUnits();
    cellSize = schema.getChunkSize();
    numDataBlocks = schema.getNumDataUnits();
    numAllBlocks = numDataBlocks + numParityBlocks;

    encoder = new RSRawEncoder();
    encoder.initialize(numDataBlocks, numParityBlocks, cellSize);

    coordinator = new Coordinator(numDataBlocks, numAllBlocks);
    try {
      cellBuffers = new CellBuffers(numParityBlocks);
    } catch (InterruptedException ie) {
      throw DFSUtil.toInterruptedIOException("Failed to create cell buffers", ie);
    }

    List<StripedDataStreamer> s = new ArrayList<>(numAllBlocks);
    for (short i = 0; i < numAllBlocks; i++) {
      StripedDataStreamer streamer = new StripedDataStreamer(stat,
          dfsClient, src, progress, checksum, cachingStrategy, byteArrayManager,
          favoredNodes, i, coordinator);
      s.add(streamer);
    }
    streamers = Collections.unmodifiableList(s);
    setCurrentStreamer(0);
  }

  StripedDataStreamer getStripedDataStreamer(int i) {
    return streamers.get(i);
  }

  int getCurrentIndex() {
    return getCurrentStreamer().getIndex();
  }

  StripedDataStreamer getCurrentStreamer() {
    return (StripedDataStreamer)streamer;
  }

  private StripedDataStreamer setCurrentStreamer(int i) {
    streamer = streamers.get(i);
    return getCurrentStreamer();
  }

  /**
   * Encode the buffers, i.e. compute parities.
   *
   * @param buffers data buffers + parity buffers
   */
  private static void encode(RawErasureEncoder encoder, int numData,
      ByteBuffer[] buffers) {
    final ByteBuffer[] dataBuffers = new ByteBuffer[numData];
    final ByteBuffer[] parityBuffers = new ByteBuffer[buffers.length - numData];
    System.arraycopy(buffers, 0, dataBuffers, 0, dataBuffers.length);
    System.arraycopy(buffers, numData, parityBuffers, 0, parityBuffers.length);

    encoder.encode(dataBuffers, parityBuffers);
  }


  private void checkStreamers() throws IOException {
    int count = 0;
    for(StripedDataStreamer s : streamers) {
      if (!s.isFailed()) {
        count++;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("checkStreamers: " + streamers);
      LOG.debug("count=" + count);
    }
    if (count < numDataBlocks) {
      throw new IOException("Failed: the number of remaining blocks = "
          + count + " < the number of data blocks = " + numDataBlocks);
    }
  }

  private void handleStreamerFailure(String err, Exception e) throws IOException {
    LOG.warn("Failed: " + err + ", " + this, e);
    getCurrentStreamer().setIsFailed(true);
    checkStreamers();
    currentPacket = null;
  }

  /**
   * Generate packets from a given buffer. This is only used for streamers
   * writing parity blocks.
   *
   * @param byteBuffer the given buffer to generate packets
   * @param checksumBuf the checksum buffer
   * @return packets generated
   * @throws IOException
   */
  private List<DFSPacket> generatePackets(
      ByteBuffer byteBuffer, byte[] checksumBuf) throws IOException{
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
  protected synchronized void writeChunk(byte[] bytes, int offset, int len,
      byte[] checksum, int ckoff, int cklen) throws IOException {
    final int index = getCurrentIndex();
    final StripedDataStreamer current = getCurrentStreamer();
    final int pos = cellBuffers.addTo(index, bytes, offset, len);
    final boolean cellFull = pos == cellSize;

    final long oldBytes = current.getBytesCurBlock();
    if (!current.isFailed()) {
      try {
        super.writeChunk(bytes, offset, len, checksum, ckoff, cklen);

        // cell is full and current packet has not been enqueued,
        if (cellFull && currentPacket != null) {
          enqueueCurrentPacketFull();
        }
      } catch(Exception e) {
        handleStreamerFailure("offset=" + offset + ", length=" + len, e);
      }
    }

    if (current.isFailed()) {
      final long newBytes = oldBytes + len;
      coordinator.setBytesEndBlock(index, newBytes, current.getBlock());
      current.setBytesCurBlock(newBytes);
    }

    // Two extra steps are needed when a striping cell is full:
    // 1. Forward the current index pointer
    // 2. Generate parity packets if a full stripe of data cells are present
    if (cellFull) {
      int next = index + 1;
      //When all data cells in a stripe are ready, we need to encode
      //them and generate some parity cells. These cells will be
      //converted to packets and put to their DataStreamer's queue.
      if (next == numDataBlocks) {
        cellBuffers.flipDataBuffers();
        writeParityCells();
        next = 0;
      }
      setCurrentStreamer(next);
    }
  }

  private int stripeDataSize() {
    return numDataBlocks * cellSize;
  }

  @Override
  public void hflush() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void hsync() {
    throw new UnsupportedOperationException();
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

  @Override
  protected void closeThreads(boolean force) throws IOException {
    final MultipleIOException.Builder b = new MultipleIOException.Builder();
    for (StripedDataStreamer streamer : streamers) {
      try {
        streamer.close(force);
        streamer.join();
        streamer.closeSocket();
      } catch(Exception e) {
        try {
          handleStreamerFailure("force=" + force, e);
        } catch(IOException ioe) {
          b.add(ioe);
        }
      } finally {
        streamer.setSocketToNull();
        setClosed();
      }
    }
    final IOException ioe = b.build();
    if (ioe != null) {
      throw ioe;
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

    final int firstCellSize = (int)(getStripedDataStreamer(0).getBytesCurBlock() % cellSize);
    final int parityCellSize = firstCellSize > 0 && firstCellSize < cellSize?
        firstCellSize : cellSize;
    final ByteBuffer[] buffers = cellBuffers.getBuffers();

    for (int i = 0; i < numAllBlocks; i++) {
      // Pad zero bytes to make all cells exactly the size of parityCellSize
      // If internal block is smaller than parity block, pad zero bytes.
      // Also pad zero bytes to all parity cells
      final int position = buffers[i].position();
      assert position <= parityCellSize : "If an internal block is smaller" +
          " than parity block, then its last cell should be small than last" +
          " parity cell";
      for (int j = 0; j < parityCellSize - position; j++) {
        buffers[i].put((byte) 0);
      }
      buffers[i].flip();
    }

    writeParityCells();
  }

  void writeParityCells() throws IOException {
    final ByteBuffer[] buffers = cellBuffers.getBuffers();
    //encode the data cells
    encode(encoder, numDataBlocks, buffers);
    for (int i = numDataBlocks; i < numAllBlocks; i++) {
      writeParity(i, buffers[i], cellBuffers.getChecksumArray(i));
    }
    cellBuffers.clear();
  }

  void writeParity(int index, ByteBuffer buffer, byte[] checksumBuf
      ) throws IOException {
    final StripedDataStreamer current = setCurrentStreamer(index);
    final int len = buffer.limit();

    final long oldBytes = current.getBytesCurBlock();
    if (!current.isFailed()) {
      try {
        for (DFSPacket p : generatePackets(buffer, checksumBuf)) {
          streamer.waitAndQueuePacket(p);
        }
        endBlock();
      } catch(Exception e) {
        handleStreamerFailure("oldBytes=" + oldBytes + ", len=" + len, e);
      }
    }

    if (current.isFailed()) {
      final long newBytes = oldBytes + len;
      current.setBytesCurBlock(newBytes);
    }
  }

  @Override
  void setClosed() {
    super.setClosed();
    for (int i = 0; i < numAllBlocks; i++) {
      streamers.get(i).release();
    }
    cellBuffers.release();
  }

  @Override
  protected synchronized void closeImpl() throws IOException {
    if (isClosed()) {
      getLeadingStreamer().getLastException().check(true);
      return;
    }

    try {
      // flush from all upper layers
      try {
        flushBuffer();
        if (currentPacket != null) {
          enqueueCurrentPacket();
        }
      } catch(Exception e) {
        handleStreamerFailure("closeImpl", e);
      }

      // if the last stripe is incomplete, generate and write parity cells
      writeParityCellsForLastStripe();

      for (int i = 0; i < numAllBlocks; i++) {
        final StripedDataStreamer s = setCurrentStreamer(i);
        if (!s.isFailed()) {
          try {
            if (s.getBytesCurBlock() > 0) {
              setCurrentPacket2Empty();
            }
            // flush all data to Datanode
            flushInternal();
          } catch(Exception e) {
            handleStreamerFailure("closeImpl", e);
          }
        }
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
