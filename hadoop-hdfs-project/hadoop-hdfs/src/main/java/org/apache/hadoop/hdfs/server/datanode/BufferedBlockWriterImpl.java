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
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * {@code BufferedBlockWriterImpl} is a concrete implementation of {@link BufferedBlockWriter}
 * that manages buffered block writes to disk using DSYNC-enabled
 * {@link FileChannel}.
 *
 * <p>
 * This class provides a mechanism for memory-efficient, concurrent, and
 * fsync-safe writes from Netty's off-heap buffers to disk files (used by
 * {@link BlockReceiver} in the DataNode write pipeline).
 *
 * <p>
 * Key features:
 * <ul>
 * <li>Limits concurrent memory buffer usage via semaphores</li>
 * <li>Uses DSYNC mode to ensure data durability on flush</li>
 * <li>Drops page cache after completion to free kernel memory</li>
 * </ul>
 */
public class BufferedBlockWriterImpl implements BufferedBlockWriter {

  public static final Logger LOG =
      LoggerFactory.getLogger(BufferedBlockWriterImpl.class);

  private ByteBuf nettyBuf;
  private volatile FileChannel fc;
  private final File file;
  private final FsVolumeImpl volume;
  private final BlockReceiver blockReceiver;
  private final Semaphore writeMemoryBufferMaxConcurrentWrites;
  private final Semaphore flushWritesSemaphore;
  private final ExecutorService volumeExecutor;
  private volatile long totalFlushedBytes;
  private AtomicBoolean isClosed = new AtomicBoolean(false);
  private long lastFilePos = 0; // capture last written file position
  private String blockName;

  /**
   * Initializes a new {@code BufferedBlockWriterImpl} for buffered writes.
   *
   * @param blockReceiver the associated {@link BlockReceiver} handling the
   *          block stream
   * @param file the file representing the target block
   * @param volume the backing {@link FsVolumeImpl}
   * @param writeMemoryBufferMaxConcurrentWrites semaphore to limit total memory
   *          usage
   * @throws IOException if file channel initialization fails
   */
  public BufferedBlockWriterImpl(BlockReceiver blockReceiver, File file,
      FsVolumeImpl volume, Semaphore writeMemoryBufferMaxConcurrentWrites)
      throws IOException {
    this.file = file;
    this.volume = volume;
    this.blockReceiver = blockReceiver;
    this.blockName = blockReceiver.getBlock().getBlockName();
    this.writeMemoryBufferMaxConcurrentWrites =
        writeMemoryBufferMaxConcurrentWrites;
    this.flushWritesSemaphore =
        volume.getBufferResources().getFlushPermitSemaphore().orElse(null);
    this.volumeExecutor = volume.getBufferResources().getVolumeExecutor();

    // Enforce the DataNode-wide write-buffer memory cap BEFORE allocating the
    // off-heap buffer. If we allocated first, many concurrent writers could
    // each grab a buffer before blocking, blowing past the configured cap.
    acquirePermit();
    boolean initialized = false;
    try {
      this.nettyBuf = PooledByteBufAllocator.DEFAULT
          .buffer(volume.getMaxWriteBufferCapacityBytes());
      try {
        fc = FileChannel.open(file.toPath(), StandardOpenOption.CREATE,
            StandardOpenOption.WRITE, StandardOpenOption.DSYNC);
      } catch (IOException e) {
        // Release the pooled buffer so a channel-open failure does not leak
        // pooled/direct memory.
        nettyBuf.release();
        nettyBuf = null;
        throw e;
      }
      initialized = true;
    } finally {
      if (!initialized) {
        // Give back the memory-cap permit acquired above; release() will not
        // be called because construction failed.
        writeMemoryBufferMaxConcurrentWrites.release();
      }
    }
  }

  /**
   * Writes data from the given {@link ByteBuffer} into the internal Netty
   * buffer. Automatically flushes when the buffer becomes full.
   */
  public synchronized void writeData(ByteBuffer dataBuf, int startByteToDisk,
      int numBytesToDisk) throws IOException {
    int size = 0;
    int len = numBytesToDisk;
    byte[] data = dataBuf.array();
    while (size < len) {
      int writable = Math.min(nettyBuf.writableBytes(), numBytesToDisk);
      size += writable;
      nettyBuf.writeBytes(data, startByteToDisk, writable);
      startByteToDisk += writable;
      numBytesToDisk -= writable;

      if (nettyBuf.writableBytes() == 0) {
        flushOrSync(true, true, false);
      }
    }
  }

  /**
   * Flushes buffered data to disk, limiting concurrent flush operations per
   * volume to prevent excessive I/O contention.
   *
   * <p>This method is {@code synchronized} so that the buffer monitor is always
   * taken <em>before</em> the per-volume flush permit ({@code M -> P}). The
   * full-buffer flush path also runs under the buffer monitor
   * ({@code writeData} is synchronized and calls back into this flush), so
   * enforcing the same {@code M -> P} order in every flush caller (idle-flush
   * scheduler, hsync, close, responder) removes the lock-ordering inversion
   * that could otherwise deadlock when {@code flushWritesSemaphore} is enabled
   * (a receiver thread holding the monitor and waiting for the permit while a
   * flusher thread holds the permit and waits for the monitor).</p>
   */
  @Override
  public synchronized void flush() throws IOException {
    boolean acquired = aquireFlushPermit();
    try {
      flushInternal();
    } finally {
      if (acquired) {
        releaseFlushPermit();
      }
    }
  }

  public void flushOrSync(boolean fsync, boolean bufferFlush,
      boolean blockClosed)
      throws IOException {
    blockReceiver.flushOrSync(fsync,
        false /* fsync checksum during closing the block */, bufferFlush,
        blockClosed);
  }

  /**
   * Releases the allocated buffer, semaphore permits, and closes the
   * underlying file channel.
   */
  @Override
  public synchronized void release() {
    if (!isClosed.compareAndSet(false, true)) {
      return;
    }
    writeMemoryBufferMaxConcurrentWrites.release();
    closeFileChannelAndDropPageCache();
    if (nettyBuf != null) {
      nettyBuf.release();
      nettyBuf = null;
    }
  }

  /**
   * Attempts to acquire a permit for flush concurrency, ensuring only a limited
   * number of flush operations run per volume at a time.
   *
   * @return {@code true} if a permit was actually acquired (and must be
   *         released by the caller); {@code false} if there is no flush
   *         semaphore or the wait was interrupted (in which case NO permit is
   *         held, so the caller must NOT release one -- otherwise the permit
   *         count would inflate and defeat the flush-concurrency limit).
   */
  private boolean aquireFlushPermit() {
    if (flushWritesSemaphore == null) {
      return false;
    }
    if (flushWritesSemaphore.availablePermits() <= 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Restricting flush concurrency on {}", volume.getBaseURI());
      }
    }
    try {
      flushWritesSemaphore.acquire();
      return true;
    } catch (InterruptedException e) {
      LOG.info("Interrupted while acquiring flush concurrency on disk= {}",
          volume.getBaseURI());
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /** Releases the flush concurrency permit. */
  private void releaseFlushPermit() {
    if (flushWritesSemaphore != null) {
      flushWritesSemaphore.release();
    }
  }

  /**
   * Writes buffered Netty data to the file channel. Uses DSYNC to ensure
   * durability while maintaining performance.
   */
  public synchronized void flushInternal() throws IOException {
    if (nettyBuf == null || nettyBuf.readableBytes() == 0) {
      return;
    }
    nettyBuf.markReaderIndex(); // mark start of unread data

    boolean success = false;

    try {
      writeBufferToChannel();
      success = true;
    } catch (ClosedByInterruptException e) {
      boolean isInterrupted = Thread.currentThread().isInterrupted();
      // This happens when upstream fails and receiver thread is interrupted in
      // that case, clear the flag and retry to complete the write
      LOG.warn("Flush failed, retrying once from file position {} for block "
              + "{}, interrupted={}",
          lastFilePos, blockName, isInterrupted, e);

      // restore interrupt flag if thread was interrupted
      Thread.interrupted();

      fc.close();

      // Retry once
      try {
        fc = FileChannel.open(file.toPath(), StandardOpenOption.CREATE,
            StandardOpenOption.WRITE, StandardOpenOption.DSYNC);

        fc.position(lastFilePos); // resume from last position
        writeBufferToChannel();
        success = true;
      } catch (IOException ex) {
        LOG.error("Retry flush failed for block {}",
            blockName, ex);
        throw ex;
      }
      // re-interrupt to restore the state
      if (isInterrupted) {
        Thread.currentThread().interrupt();
      }
    } finally {
      if (success) {
        nettyBuf.clear(); // fully flushed
      } else {
        nettyBuf.resetReaderIndex(); // keep buffer for next attempt
      }
    }
  }

  private synchronized void writeBufferToChannel() throws IOException {
    int readableBytes = nettyBuf.readableBytes();
    if (nettyBuf.hasArray()) {
      ByteBuffer[] nioBufs = nettyBuf.nioBuffers(
          nettyBuf.readerIndex(), readableBytes);
      long remaining = readableBytes;
      while (remaining > 0) {
        long written = fc.write(nioBufs);
        remaining -= written;
      }
    } else {
      ByteBuffer nioBuf = nettyBuf.nioBuffer(
          nettyBuf.readerIndex(), nettyBuf.readableBytes());
      while (nioBuf.hasRemaining()) {
        fc.write(nioBuf);
      }
    }
    lastFilePos = fc.position();
    totalFlushedBytes += readableBytes;
  }

  /**
   * Sync data for a block to disk if the block is closed.
   *
   * <p>Propagates fsync failures to the caller so that an hsync or block close
   * is not reported as successful (and its bytes are not acked as durable) when
   * the underlying {@code fsync} actually failed.
   */
  @Override
  public void syncData(String ignoredBlockName, boolean blockClosed)
      throws IOException {
    fc.force(false);
  }

  /**
   * Closes the file channel and schedules a page cache drop to release
   * kernel-level disk cache after block completion.
   */
  private void closeFileChannelAndDropPageCache() {
    try {
      fc.close();
      // drop the page cache
      volumeExecutor.execute(() -> {
        // Open read-only ("r"). This task runs asynchronously and may fire
        // after the block has been finalized (the RBW block file is renamed to
        // its finalized location on close). "rw" would RE-CREATE the moved-away
        // path as a zero-byte blk_* file (inode leak + a stale replica artifact
        // the directory/block scanner would trip over). "r" never creates: if
        // the file is gone it throws FileNotFoundException, which we treat as a
        // no-op (nothing left to advise). posix_fadvise(DONTNEED) does not need
        // write access.
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
          FileDescriptor fd = raf.getFD();
          NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(blockName,
              fd, 0, 0, POSIX_FADV_DONTNEED);
        } catch (java.io.FileNotFoundException e) {
          // Block file already finalized/removed; nothing to drop.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping page-cache drop for {}; block file {} no longer "
                + "present (already finalized/removed).", blockName, file);
          }
        } catch (Exception e) {
          LOG.warn("Failed to drop the cache", e);
        }
      });
    } catch (Exception e) {
      LOG.warn("Failed to close file channel", e);
    }
  }

  /**
   * Acquires a permit to use the write buffer, blocking if the system has
   * reached its memory buffer concurrency limit.
   *
   * @throws InterruptedIOException if interrupted while waiting for a permit;
   *         in that case NO permit is held, so the caller must abort
   *         construction (avoids an over-release later in {@link #release()}).
   */
  private void acquirePermit() throws InterruptedIOException {
    try {
      if (writeMemoryBufferMaxConcurrentWrites.availablePermits() <= 0) {
        LOG.info(
            "Max concurrent write reached (increase size of {}).. blocking incoming requests..",
            DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MAX_CAPACITY_MB);
      }
      writeMemoryBufferMaxConcurrentWrites.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException(
          "Interrupted while acquiring write-buffer permit for block "
              + blockName);
    }
  }

  @Override
  public long getFlushedBytes() {
    return totalFlushedBytes;
  }

  /**
   * Best-effort, lock-free check used by the idle buffer-flush task. A racy
   * read of the buffer is fine here: it only gates whether to attempt a
   * flush, and {@link #flushInternal()} re-checks under the monitor.
   */
  @Override
  public boolean hasPendingData() {
    ByteBuf buf = nettyBuf;
    return buf != null && buf.isReadable();
  }

}
