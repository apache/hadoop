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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.ClientMmap;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitReplica;
import org.apache.hadoop.util.DirectBufferPool;
import org.apache.hadoop.util.DataChecksum;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * BlockReaderLocal enables local short circuited reads. If the DFS client is on
 * the same machine as the datanode, then the client can read files directly
 * from the local file system rather than going through the datanode for better
 * performance. <br>
 * {@link BlockReaderLocal} works as follows:
 * <ul>
 * <li>The client performing short circuit reads must be configured at the
 * datanode.</li>
 * <li>The client gets the file descriptors for the metadata file and the data 
 * file for the block using
 * {@link org.apache.hadoop.hdfs.server.datanode.DataXceiver#requestShortCircuitFds}.
 * </li>
 * <li>The client reads the file descriptors.</li>
 * </ul>
 */
class BlockReaderLocal implements BlockReader {
  static final Log LOG = LogFactory.getLog(BlockReaderLocal.class);

  private static final DirectBufferPool bufferPool = new DirectBufferPool();

  public static class Builder {
    private final int bufferSize;
    private boolean verifyChecksum;
    private int maxReadahead;
    private String filename;
    private ShortCircuitReplica replica;
    private long dataPos;
    private ExtendedBlock block;

    public Builder(Conf conf) {
      this.maxReadahead = Integer.MAX_VALUE;
      this.verifyChecksum = !conf.skipShortCircuitChecksums;
      this.bufferSize = conf.shortCircuitBufferSize;
    }

    public Builder setVerifyChecksum(boolean verifyChecksum) {
      this.verifyChecksum = verifyChecksum;
      return this;
    }

    public Builder setCachingStrategy(CachingStrategy cachingStrategy) {
      long readahead = cachingStrategy.getReadahead() != null ?
          cachingStrategy.getReadahead() :
              DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT;
      this.maxReadahead = (int)Math.min(Integer.MAX_VALUE, readahead);
      return this;
    }

    public Builder setFilename(String filename) {
      this.filename = filename;
      return this;
    }

    public Builder setShortCircuitReplica(ShortCircuitReplica replica) {
      this.replica = replica;
      return this;
    }

    public Builder setStartOffset(long startOffset) {
      this.dataPos = Math.max(0, startOffset);
      return this;
    }

    public Builder setBlock(ExtendedBlock block) {
      this.block = block;
      return this;
    }

    public BlockReaderLocal build() {
      Preconditions.checkNotNull(replica);
      return new BlockReaderLocal(this);
    }
  }

  private boolean closed = false;

  /**
   * Pair of streams for this block.
   */
  private final ShortCircuitReplica replica;

  /**
   * The data FileChannel.
   */
  private final FileChannel dataIn;

  /**
   * The next place we'll read from in the block data FileChannel.
   *
   * If data is buffered in dataBuf, this offset will be larger than the
   * offset of the next byte which a read() operation will give us.
   */
  private long dataPos;

  /**
   * The Checksum FileChannel.
   */
  private final FileChannel checksumIn;
  
  /**
   * Checksum type and size.
   */
  private final DataChecksum checksum;

  /**
   * If false, we will always skip the checksum.
   */
  private final boolean verifyChecksum;

  /**
   * Name of the block, for logging purposes.
   */
  private final String filename;
  
  /**
   * Block ID and Block Pool ID.
   */
  private final ExtendedBlock block;
  
  /**
   * Cache of Checksum#bytesPerChecksum.
   */
  private final int bytesPerChecksum;

  /**
   * Cache of Checksum#checksumSize.
   */
  private final int checksumSize;

  /**
   * Maximum number of chunks to allocate.
   *
   * This is used to allocate dataBuf and checksumBuf, in the event that
   * we need them.
   */
  private final int maxAllocatedChunks;

  /**
   * True if zero readahead was requested.
   */
  private final boolean zeroReadaheadRequested;

  /**
   * Maximum amount of readahead we'll do.  This will always be at least the,
   * size of a single chunk, even if {@link #zeroReadaheadRequested} is true.
   * The reason is because we need to do a certain amount of buffering in order
   * to do checksumming.
   * 
   * This determines how many bytes we'll use out of dataBuf and checksumBuf.
   * Why do we allocate buffers, and then (potentially) only use part of them?
   * The rationale is that allocating a lot of buffers of different sizes would
   * make it very difficult for the DirectBufferPool to re-use buffers. 
   */
  private final int maxReadaheadLength;

  /**
   * Buffers data starting at the current dataPos and extending on
   * for dataBuf.limit().
   *
   * This may be null if we don't need it.
   */
  private ByteBuffer dataBuf;

  /**
   * Buffers checksums starting at the current checksumPos and extending on
   * for checksumBuf.limit().
   *
   * This may be null if we don't need it.
   */
  private ByteBuffer checksumBuf;

  private BlockReaderLocal(Builder builder) {
    this.replica = builder.replica;
    this.dataIn = replica.getDataStream().getChannel();
    this.dataPos = builder.dataPos;
    this.checksumIn = replica.getMetaStream().getChannel();
    BlockMetadataHeader header = builder.replica.getMetaHeader();
    this.checksum = header.getChecksum();
    this.verifyChecksum = builder.verifyChecksum &&
        (this.checksum.getChecksumType().id != DataChecksum.CHECKSUM_NULL);
    this.filename = builder.filename;
    this.block = builder.block;
    this.bytesPerChecksum = checksum.getBytesPerChecksum();
    this.checksumSize = checksum.getChecksumSize();

    this.maxAllocatedChunks = (bytesPerChecksum == 0) ? 0 :
        ((builder.bufferSize + bytesPerChecksum - 1) / bytesPerChecksum);
    // Calculate the effective maximum readahead.
    // We can't do more readahead than there is space in the buffer.
    int maxReadaheadChunks = (bytesPerChecksum == 0) ? 0 :
        ((Math.min(builder.bufferSize, builder.maxReadahead) +
            bytesPerChecksum - 1) / bytesPerChecksum);
    if (maxReadaheadChunks == 0) {
      this.zeroReadaheadRequested = true;
      maxReadaheadChunks = 1;
    } else {
      this.zeroReadaheadRequested = false;
    }
    this.maxReadaheadLength = maxReadaheadChunks * bytesPerChecksum;
  }

  private synchronized void createDataBufIfNeeded() {
    if (dataBuf == null) {
      dataBuf = bufferPool.getBuffer(maxAllocatedChunks * bytesPerChecksum);
      dataBuf.position(0);
      dataBuf.limit(0);
    }
  }

  private synchronized void freeDataBufIfExists() {
    if (dataBuf != null) {
      // When disposing of a dataBuf, we have to move our stored file index
      // backwards.
      dataPos -= dataBuf.remaining();
      dataBuf.clear();
      bufferPool.returnBuffer(dataBuf);
      dataBuf = null;
    }
  }

  private synchronized void createChecksumBufIfNeeded() {
    if (checksumBuf == null) {
      checksumBuf = bufferPool.getBuffer(maxAllocatedChunks * checksumSize);
      checksumBuf.position(0);
      checksumBuf.limit(0);
    }
  }

  private synchronized void freeChecksumBufIfExists() {
    if (checksumBuf != null) {
      checksumBuf.clear();
      bufferPool.returnBuffer(checksumBuf);
      checksumBuf = null;
    }
  }

  private synchronized int drainDataBuf(ByteBuffer buf) {
    if (dataBuf == null) return -1;
    int oldLimit = dataBuf.limit();
    int nRead = Math.min(dataBuf.remaining(), buf.remaining());
    if (nRead == 0) {
      return (dataBuf.remaining() == 0) ? -1 : 0;
    }
    try {
      dataBuf.limit(dataBuf.position() + nRead);
      buf.put(dataBuf);
    } finally {
      dataBuf.limit(oldLimit);
    }
    return nRead;
  }

  /**
   * Read from the block file into a buffer.
   *
   * This function overwrites checksumBuf.  It will increment dataPos.
   *
   * @param buf   The buffer to read into.  May be dataBuf.
   *              The position and limit of this buffer should be set to
   *              multiples of the checksum size.
   * @param canSkipChecksum  True if we can skip checksumming.
   *
   * @return      Total bytes read.  0 on EOF.
   */
  private synchronized int fillBuffer(ByteBuffer buf, boolean canSkipChecksum)
      throws IOException {
    int total = 0;
    long startDataPos = dataPos;
    int startBufPos = buf.position();
    while (buf.hasRemaining()) {
      int nRead = dataIn.read(buf, dataPos);
      if (nRead < 0) {
        break;
      }
      dataPos += nRead;
      total += nRead;
    }
    if (canSkipChecksum) {
      freeChecksumBufIfExists();
      return total;
    }
    if (total > 0) {
      try {
        buf.limit(buf.position());
        buf.position(startBufPos);
        createChecksumBufIfNeeded();
        int checksumsNeeded = (total + bytesPerChecksum - 1) / bytesPerChecksum;
        checksumBuf.clear();
        checksumBuf.limit(checksumsNeeded * checksumSize);
        long checksumPos =
          7 + ((startDataPos / bytesPerChecksum) * checksumSize);
        while (checksumBuf.hasRemaining()) {
          int nRead = checksumIn.read(checksumBuf, checksumPos);
          if (nRead < 0) {
            throw new IOException("Got unexpected checksum file EOF at " +
                checksumPos + ", block file position " + startDataPos + " for " +
                "block " + block + " of file " + filename);
          }
          checksumPos += nRead;
        }
        checksumBuf.flip();
  
        checksum.verifyChunkedSums(buf, checksumBuf, filename, startDataPos);
      } finally {
        buf.position(buf.limit());
      }
    }
    return total;
  }

  private boolean createNoChecksumContext() {
    if (verifyChecksum) {
      return replica.addNoChecksumAnchor();
    } else {
      return true;
    }
  }

  private void releaseNoChecksumContext() {
    if (verifyChecksum) {
      replica.removeNoChecksumAnchor();
    }
  }

  @Override
  public synchronized int read(ByteBuffer buf) throws IOException {
    boolean canSkipChecksum = createNoChecksumContext();
    try {
      String traceString = null;
      if (LOG.isTraceEnabled()) {
        traceString = new StringBuilder().
            append("read(").
            append("buf.remaining=").append(buf.remaining()).
            append(", block=").append(block).
            append(", filename=").append(filename).
            append(", canSkipChecksum=").append(canSkipChecksum).
            append(")").toString();
        LOG.info(traceString + ": starting");
      }
      int nRead;
      try {
        if (canSkipChecksum && zeroReadaheadRequested) {
          nRead = readWithoutBounceBuffer(buf);
        } else {
          nRead = readWithBounceBuffer(buf, canSkipChecksum);
        }
      } catch (IOException e) {
        if (LOG.isTraceEnabled()) {
          LOG.info(traceString + ": I/O error", e);
        }
        throw e;
      }
      if (LOG.isTraceEnabled()) {
        LOG.info(traceString + ": returning " + nRead);
      }
      return nRead;
    } finally {
      if (canSkipChecksum) releaseNoChecksumContext();
    }
  }

  private synchronized int readWithoutBounceBuffer(ByteBuffer buf)
      throws IOException {
    freeDataBufIfExists();
    freeChecksumBufIfExists();
    int total = 0;
    while (buf.hasRemaining()) {
      int nRead = dataIn.read(buf, dataPos);
      if (nRead <= 0) break;
      dataPos += nRead;
      total += nRead;
    }
    return (total == 0 && (dataPos == dataIn.size())) ? -1 : total;
  }

  /**
   * Fill the data buffer.  If necessary, validate the data against the
   * checksums.
   * 
   * We always want the offsets of the data contained in dataBuf to be
   * aligned to the chunk boundary.  If we are validating checksums, we
   * accomplish this by seeking backwards in the file until we're on a
   * chunk boundary.  (This is necessary because we can't checksum a
   * partial chunk.)  If we are not validating checksums, we simply only
   * fill the latter part of dataBuf.
   * 
   * @param canSkipChecksum  true if we can skip checksumming.
   * @return                 true if we hit EOF.
   * @throws IOException
   */
  private synchronized boolean fillDataBuf(boolean canSkipChecksum)
      throws IOException {
    createDataBufIfNeeded();
    final int slop = (int)(dataPos % bytesPerChecksum);
    final long oldDataPos = dataPos;
    dataBuf.limit(maxReadaheadLength);
    if (canSkipChecksum) {
      dataBuf.position(slop);
      fillBuffer(dataBuf, canSkipChecksum);
    } else {
      dataPos -= slop;
      dataBuf.position(0);
      fillBuffer(dataBuf, canSkipChecksum);
    }
    dataBuf.limit(dataBuf.position());
    dataBuf.position(Math.min(dataBuf.position(), slop));
    if (LOG.isTraceEnabled()) {
      LOG.trace("loaded " + dataBuf.remaining() + " bytes into bounce " +
          "buffer from offset " + oldDataPos + " of " + block);
    }
    return dataBuf.limit() != maxReadaheadLength;
  }

  /**
   * Read using the bounce buffer.
   *
   * A 'direct' read actually has three phases. The first drains any
   * remaining bytes from the slow read buffer. After this the read is
   * guaranteed to be on a checksum chunk boundary. If there are still bytes
   * to read, the fast direct path is used for as many remaining bytes as
   * possible, up to a multiple of the checksum chunk size. Finally, any
   * 'odd' bytes remaining at the end of the read cause another slow read to
   * be issued, which involves an extra copy.
   *
   * Every 'slow' read tries to fill the slow read buffer in one go for
   * efficiency's sake. As described above, all non-checksum-chunk-aligned
   * reads will be served from the slower read path.
   *
   * @param buf              The buffer to read into. 
   * @param canSkipChecksum  True if we can skip checksums.
   */
  private synchronized int readWithBounceBuffer(ByteBuffer buf,
        boolean canSkipChecksum) throws IOException {
    int total = 0;
    int bb = drainDataBuf(buf); // drain bounce buffer if possible
    if (bb >= 0) {
      total += bb;
      if (buf.remaining() == 0) return total;
    }
    boolean eof = true, done = false;
    do {
      if (buf.isDirect() && (buf.remaining() >= maxReadaheadLength)
            && ((dataPos % bytesPerChecksum) == 0)) {
        // Fast lane: try to read directly into user-supplied buffer, bypassing
        // bounce buffer.
        int oldLimit = buf.limit();
        int nRead;
        try {
          buf.limit(buf.position() + maxReadaheadLength);
          nRead = fillBuffer(buf, canSkipChecksum);
        } finally {
          buf.limit(oldLimit);
        }
        if (nRead < maxReadaheadLength) {
          done = true;
        }
        if (nRead > 0) {
          eof = false;
        }
        total += nRead;
      } else {
        // Slow lane: refill bounce buffer.
        if (fillDataBuf(canSkipChecksum)) {
          done = true;
        }
        bb = drainDataBuf(buf); // drain bounce buffer if possible
        if (bb >= 0) {
          eof = false;
          total += bb;
        }
      }
    } while ((!done) && (buf.remaining() > 0));
    return (eof && total == 0) ? -1 : total;
  }

  @Override
  public synchronized int read(byte[] arr, int off, int len)
        throws IOException {
    boolean canSkipChecksum = createNoChecksumContext();
    int nRead;
    try {
      String traceString = null;
      if (LOG.isTraceEnabled()) {
        traceString = new StringBuilder().
            append("read(arr.length=").append(arr.length).
            append(", off=").append(off).
            append(", len=").append(len).
            append(", filename=").append(filename).
            append(", block=").append(block).
            append(", canSkipChecksum=").append(canSkipChecksum).
            append(")").toString();
        LOG.trace(traceString + ": starting");
      }
      try {
        if (canSkipChecksum && zeroReadaheadRequested) {
          nRead = readWithoutBounceBuffer(arr, off, len);
        } else {
          nRead = readWithBounceBuffer(arr, off, len, canSkipChecksum);
        }
      } catch (IOException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(traceString + ": I/O error", e);
        }
        throw e;
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace(traceString + ": returning " + nRead);
      }
    } finally {
      if (canSkipChecksum) releaseNoChecksumContext();
    }
    return nRead;
  }

  private synchronized int readWithoutBounceBuffer(byte arr[], int off,
        int len) throws IOException {
    freeDataBufIfExists();
    freeChecksumBufIfExists();
    int nRead = dataIn.read(ByteBuffer.wrap(arr, off, len), dataPos);
    if (nRead > 0) {
      dataPos += nRead;
    } else if ((nRead == 0) && (dataPos == dataIn.size())) {
      return -1;
    }
    return nRead;
  }

  private synchronized int readWithBounceBuffer(byte arr[], int off, int len,
        boolean canSkipChecksum) throws IOException {
    createDataBufIfNeeded();
    if (!dataBuf.hasRemaining()) {
      dataBuf.position(0);
      dataBuf.limit(maxReadaheadLength);
      fillDataBuf(canSkipChecksum);
    }
    if (dataBuf.remaining() == 0) return -1;
    int toRead = Math.min(dataBuf.remaining(), len);
    dataBuf.get(arr, off, toRead);
    return toRead;
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    int discardedFromBuf = 0;
    long remaining = n;
    if ((dataBuf != null) && dataBuf.hasRemaining()) {
      discardedFromBuf = (int)Math.min(dataBuf.remaining(), n);
      dataBuf.position(dataBuf.position() + discardedFromBuf);
      remaining -= discardedFromBuf;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("skip(n=" + n + ", block=" + block + ", filename=" + 
        filename + "): discarded " + discardedFromBuf + " bytes from " +
        "dataBuf and advanced dataPos by " + remaining);
    }
    dataPos += remaining;
    return n;
  }

  @Override
  public int available() throws IOException {
    // We never do network I/O in BlockReaderLocal.
    return Integer.MAX_VALUE;
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) return;
    closed = true;
    if (LOG.isTraceEnabled()) {
      LOG.trace("close(filename=" + filename + ", block=" + block + ")");
    }
    replica.unref();
    freeDataBufIfExists();
    freeChecksumBufIfExists();
  }

  @Override
  public synchronized void readFully(byte[] arr, int off, int len)
      throws IOException {
    BlockReaderUtil.readFully(this, arr, off, len);
  }

  @Override
  public synchronized int readAll(byte[] buf, int off, int len)
      throws IOException {
    return BlockReaderUtil.readAll(this, buf, off, len);
  }

  @Override
  public boolean isLocal() {
    return true;
  }

  @Override
  public boolean isShortCircuit() {
    return true;
  }

  /**
   * Get or create a memory map for this replica.
   * 
   * There are two kinds of ClientMmap objects we could fetch here: one that 
   * will always read pre-checksummed data, and one that may read data that
   * hasn't been checksummed.
   *
   * If we fetch the former, "safe" kind of ClientMmap, we have to increment
   * the anchor count on the shared memory slot.  This will tell the DataNode
   * not to munlock the block until this ClientMmap is closed.
   * If we fetch the latter, we don't bother with anchoring.
   *
   * @param opts     The options to use, such as SKIP_CHECKSUMS.
   * 
   * @return         null on failure; the ClientMmap otherwise.
   */
  @Override
  public ClientMmap getClientMmap(EnumSet<ReadOption> opts) {
    boolean anchor = verifyChecksum &&
        (opts.contains(ReadOption.SKIP_CHECKSUMS) == false);
    if (anchor) {
      if (!createNoChecksumContext()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("can't get an mmap for " + block + " of " + filename + 
              " since SKIP_CHECKSUMS was not given, " +
              "we aren't skipping checksums, and the block is not mlocked.");
        }
        return null;
      }
    }
    ClientMmap clientMmap = null;
    try {
      clientMmap = replica.getOrCreateClientMmap(anchor);
    } finally {
      if ((clientMmap == null) && anchor) {
        releaseNoChecksumContext();
      }
    }
    return clientMmap;
  }
  
  @VisibleForTesting
  boolean getVerifyChecksum() {
    return this.verifyChecksum;
  }

  @VisibleForTesting
  int getMaxReadaheadLength() {
    return this.maxReadaheadLength;
  }
  
  /**
   * Make the replica anchorable.  Normally this can only be done by the
   * DataNode.  This method is only for testing.
   */
  @VisibleForTesting
  void forceAnchorable() {
    replica.getSlot().makeAnchorable();
  }

  /**
   * Make the replica unanchorable.  Normally this can only be done by the
   * DataNode.  This method is only for testing.
   */
  @VisibleForTesting
  void forceUnanchorable() {
    replica.getSlot().makeUnanchorable();
  }
}
