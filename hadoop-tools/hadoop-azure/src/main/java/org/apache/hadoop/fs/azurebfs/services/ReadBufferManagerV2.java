/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.JvmUniqueIdProvider;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.fs.azurebfs.utils.ResourceUtilizationUtils;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.HUNDRED_D;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.SCALE_DIRECTION_DOWN;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.SCALE_DIRECTION_NO_ACTION_NEEDED;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.SCALE_DIRECTION_NO_DOWN_AT_MIN;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.SCALE_DIRECTION_NO_UP_AT_MAX;
import static org.apache.hadoop.fs.azurebfs.constants.MetricsConstants.SCALE_DIRECTION_UP;

/**
 * The Improved Read Buffer Manager for Rest AbfsClient.
 */
public final class ReadBufferManagerV2 extends ReadBufferManager {

  // Internal constants
  private static final ReentrantLock LOCK = new ReentrantLock();

  // Thread Pool Configurations
  private static int minThreadPoolSize;

  private static int maxThreadPoolSize;

  private static int cpuMonitoringIntervalInMilliSec;

  private static long cpuThreshold;

  private static int threadPoolUpscalePercentage;

  private static int threadPoolDownscalePercentage;

  private static int executorServiceKeepAliveTimeInMilliSec;

  private static final double THREAD_POOL_REQUIREMENT_BUFFER = 1.2;
      // 20% more threads than the queue size

  private static boolean isDynamicScalingEnabled;

  private ScheduledExecutorService cpuMonitorThread;

  private ThreadPoolExecutor workerPool;

  private final List<ReadBufferWorker> workerRefs = new ArrayList<>();

  // Buffer Pool Configurations
  private static int minBufferPoolSize;

  private static int maxBufferPoolSize;

  private static int memoryMonitoringIntervalInMilliSec;

  private static long memoryThreshold;

  private final AtomicInteger numberOfActiveBuffers = new AtomicInteger(0);

  private byte[][] bufferPool;

  /*
   * List of buffer indexes that are currently free and can be assigned to new read-ahead requests.
   * Using a thread safe data structure as multiple threads can access this concurrently.
   */
  private final ConcurrentSkipListSet<Integer> removedBufferList = new ConcurrentSkipListSet<>();
  private ConcurrentSkipListSet<Integer> freeList = new ConcurrentSkipListSet<>();

  private ScheduledExecutorService memoryMonitorThread;

  // Buffer Manager Structures
  private static ReadBufferManagerV2 bufferManager;

  private static AtomicBoolean isConfigured = new AtomicBoolean(false);

  /* Metrics collector for monitoring the performance of the ABFS read thread pool.  */
  private final AbfsReadResourceUtilizationMetrics readThreadPoolMetrics;
  /* The ABFSCounters instance for updating read buffer manager related metrics. */
  private final AbfsCounters abfsCounters;
  /* Tracks the last scale direction applied, or empty if none. */
  private volatile String lastScaleDirection = EMPTY_STRING;
  /* Maximum CPU utilization observed during the monitoring interval. */
  private volatile long maxJvmCpuUtilization = 0L;

  /**
   * Private constructor to prevent instantiation as this needs to be singleton.
   *
   * @param abfsCounters the {@link AbfsCounters} used for managing read operations.
   */
  private ReadBufferManagerV2(AbfsCounters abfsCounters) {
    this.abfsCounters = abfsCounters;
    readThreadPoolMetrics = abfsCounters.getAbfsReadResourceUtilizationMetrics();
    printTraceLog("Creating Read Buffer Manager V2 with HADOOP-18546 patch");
  }

  /**
   * Returns the singleton instance of {@code ReadBufferManagerV2}.
   *
   * @param abfsCounters the {@link AbfsCounters} used for read operations.
   * @return the singleton instance of {@code ReadBufferManagerV2}.
   */
  static ReadBufferManagerV2 getBufferManager(AbfsCounters abfsCounters) {
    if (!isConfigured.get()) {
      throw new IllegalStateException("ReadBufferManagerV2 is not configured. "
          + "Please call setReadBufferManagerConfigs() before calling getBufferManager().");
    }
    if (bufferManager == null) {
      LOCK.lock();
      try {
        if (bufferManager == null) {
          bufferManager = new ReadBufferManagerV2(abfsCounters);
          bufferManager.init();
          LOGGER.trace("ReadBufferManagerV2 singleton initialized");
        }
      } finally {
        LOCK.unlock();
      }
    }
    return bufferManager;
  }

  /**
   * Set the ReadBufferManagerV2 configurations based on the provided before singleton initialization.
   * @param readAheadBlockSize the read-ahead block size to set for the ReadBufferManagerV2.
   * @param abfsConfiguration the configuration to set for the ReadBufferManagerV2.
   */
  public static void setReadBufferManagerConfigs(final int readAheadBlockSize,
      final AbfsConfiguration abfsConfiguration) {
    // Set Configs only before initializations.
    if (bufferManager == null && !isConfigured.get()) {
      LOCK.lock();
      try {
        if (bufferManager == null && !isConfigured.get()) {
          minThreadPoolSize = abfsConfiguration.getMinReadAheadV2ThreadPoolSize();
          maxThreadPoolSize = abfsConfiguration.getMaxReadAheadV2ThreadPoolSize();
          cpuMonitoringIntervalInMilliSec
              = abfsConfiguration.getReadAheadV2CpuMonitoringIntervalMillis();
          cpuThreshold = abfsConfiguration.getReadAheadV2CpuUsageThresholdPercent();
          threadPoolUpscalePercentage
              = abfsConfiguration.getReadAheadV2ThreadPoolUpscalePercentage();
          threadPoolDownscalePercentage
              = abfsConfiguration.getReadAheadV2ThreadPoolDownscalePercentage();
          executorServiceKeepAliveTimeInMilliSec
              = abfsConfiguration.getReadAheadExecutorServiceTTLInMillis();

          minBufferPoolSize = abfsConfiguration.getMinReadAheadV2BufferPoolSize();
          maxBufferPoolSize = abfsConfiguration.getMaxReadAheadV2BufferPoolSize();
          memoryMonitoringIntervalInMilliSec
              = abfsConfiguration.getReadAheadV2MemoryMonitoringIntervalMillis();
          memoryThreshold =
              abfsConfiguration.getReadAheadV2MemoryUsageThresholdPercent();
          setThresholdAgeMilliseconds(
              abfsConfiguration.getReadAheadV2CachedBufferTTLMillis());
          isDynamicScalingEnabled
              = abfsConfiguration.isReadAheadV2DynamicScalingEnabled();
          setReadAheadBlockSize(readAheadBlockSize);
          setIsConfigured(true);
        }
      } finally {
        LOCK.unlock();
      }
    }
  }

  /**
   * Initialize the singleton ReadBufferManagerV2.
   */
  @Override
  void init() {
    // Initialize Buffer Pool. Size can never be more than max pool size
    bufferPool = new byte[maxBufferPoolSize][];
    for (int i = 0; i < minBufferPoolSize; i++) {
      // Start with just minimum number of buffers.
      bufferPool[i]
          = new byte[getReadAheadBlockSize()];  // same buffers are reused. The byte array never goes back to GC
      pushToFreeList(i);
      numberOfActiveBuffers.getAndIncrement();
    }
    memoryMonitorThread = Executors.newSingleThreadScheduledExecutor(
        runnable -> {
          Thread t = new Thread(runnable, "ReadAheadV2-Memory-Monitor");
          t.setDaemon(true);
          return t;
        });
    memoryMonitorThread.scheduleAtFixedRate(this::scheduledEviction,
        getMemoryMonitoringIntervalInMilliSec(),
        getMemoryMonitoringIntervalInMilliSec(), TimeUnit.MILLISECONDS);

    // Initialize a Fixed Size Thread Pool with minThreadPoolSize threads
    workerPool = new ThreadPoolExecutor(
        minThreadPoolSize,
        maxThreadPoolSize,
        executorServiceKeepAliveTimeInMilliSec,
        TimeUnit.MILLISECONDS,
        new SynchronousQueue<>(),
        workerThreadFactory);
    workerPool.allowCoreThreadTimeOut(true);
    for (int i = 0; i < minThreadPoolSize; i++) {
      ReadBufferWorker worker = new ReadBufferWorker(i, getBufferManager(abfsCounters));
      workerRefs.add(worker);
      workerPool.submit(worker);
    }
    ReadBufferWorker.UNLEASH_WORKERS.countDown();

    if (isDynamicScalingEnabled) {
      cpuMonitorThread = Executors.newSingleThreadScheduledExecutor(
          runnable -> {
            Thread t = new Thread(runnable, "ReadAheadV2-CPU-Monitor");
            t.setDaemon(true);
            return t;
          });
      cpuMonitorThread.scheduleAtFixedRate(this::adjustThreadPool,
          getCpuMonitoringIntervalInMilliSec(),
          getCpuMonitoringIntervalInMilliSec(),
          TimeUnit.MILLISECONDS);
    }

    printTraceLog(
        "ReadBufferManagerV2 initialized with {} buffers and {} worker threads",
        numberOfActiveBuffers.get(), workerRefs.size());
  }

  /**
   * {@link AbfsInputStream} calls this method to queueing read-ahead.
   * @param stream which read-ahead is requested from.
   * @param requestedOffset The offset in the file which should be read.
   * @param requestedLength The length to read.
   */
  @Override
  public void queueReadAhead(final AbfsInputStream stream,
      final long requestedOffset,
      final int requestedLength,
      TracingContext tracingContext) {
    printTraceLog(
        "Start Queueing readAhead for file: {}, with eTag: {}, "
            + "offset: {}, length: {}, triggered by stream: {}",
        stream.getPath(), stream.getETag(), requestedOffset, requestedLength,
        stream.hashCode());
    ReadBuffer buffer;
    synchronized (this) {
      if (isAlreadyQueued(stream.getETag(), requestedOffset)) {
        // Already queued for this offset, so skip queuing.
        printTraceLog(
            "Skipping queuing readAhead for file: {}, with eTag: {}, "
                + "offset: {}, triggered by stream: {} as it is already queued",
            stream.getPath(), stream.getETag(), requestedOffset,
            stream.hashCode());
        return;
      }
      if (isFreeListEmpty() && !tryMemoryUpscale() && !tryEvict()) {
        // No buffers are available and more buffers cannot be created. Skip queuing.
        printTraceLog(
            "Skipping queuing readAhead for file: {}, with eTag: {}, offset: {}, triggered by stream: {} as no buffers are available",
            stream.getPath(), stream.getETag(), requestedOffset,
            stream.hashCode());
        return;
      }

      // Create a new ReadBuffer to keep the prefetched data and queue.
      buffer = new ReadBuffer();
      buffer.setStream(stream); // To map buffer with stream that requested it
      buffer.setETag(stream.getETag()); // To map buffer with file it belongs to
      buffer.setPath(stream.getPath());
      buffer.setOffset(requestedOffset);
      buffer.setLength(0);
      buffer.setRequestedLength(requestedLength);
      buffer.setStatus(ReadBufferStatus.NOT_AVAILABLE);
      buffer.setLatch(new CountDownLatch(1));
      buffer.setTracingContext(tracingContext);

      if (isFreeListEmpty()) {
        /*
         * By now there should be at least one buffer available.
         * This is to double sure that after upscaling or eviction,
         * we still have free buffer available. If not, we skip queueing.
         */
        return;
      }
      Integer bufferIndex = popFromFreeList();
      if (bufferIndex > bufferPool.length) {
        // This should never happen.
        printTraceLog(
            "Skipping queuing readAhead for file: {}, with eTag: {}, offset: {}, triggered by stream: {} as invalid buffer index popped from free list",
            stream.getPath(), stream.getETag(), requestedOffset,
            stream.hashCode());
        return;
      }
      buffer.setBuffer(bufferPool[bufferIndex]);
      buffer.setBufferindex(bufferIndex);
      getReadAheadQueue().add(buffer);
      notifyAll();
      printTraceLog(
          "Done q-ing readAhead for file: {}, with eTag:{}, offset: {}, "
              + "buffer idx: {}, triggered by stream: {}",
          stream.getPath(), stream.getETag(), requestedOffset,
          buffer.getBufferindex(), stream.hashCode());
    }
  }

  /**
   * {@link AbfsInputStream} calls this method read any bytes already available in a buffer (thereby saving a
   * remote read). This returns the bytes if the data already exists in buffer. If there is a buffer that is reading
   * the requested offset, then this method blocks until that read completes. If the data is queued in a read-ahead
   * but not picked up by a worker thread yet, then it cancels that read-ahead and reports cache miss. This is because
   * depending on worker thread availability, the read-ahead may take a while - the calling thread can do its own
   * read to get the data faster (compared to the read waiting in queue for an indeterminate amount of time).
   *
   * @param stream of the file to read bytes for
   * @param position the offset in the file to do a read for
   * @param length   the length to read
   * @param buffer   the buffer to read data into. Note that the buffer will be written into from offset 0.
   * @return the number of bytes read
   */
  @Override
  public int getBlock(final AbfsInputStream stream,
      final long position,
      final int length,
      final byte[] buffer)
      throws IOException {
    // not synchronized, so have to be careful with locking
    printTraceLog(
        "getBlock request for file: {}, with eTag: {}, for position: {} "
            + "for length: {} received from stream: {}",
        stream.getPath(), stream.getETag(), position, length,
        stream.hashCode());

    String requestedETag = stream.getETag();
    boolean isFirstRead = stream.isFirstRead();

    // Wait for any in-progress read to complete.
    waitForProcess(requestedETag, position, isFirstRead);

    int bytesRead = 0;
    synchronized (this) {
      bytesRead = getBlockFromCompletedQueue(requestedETag, position, length,
          buffer);
    }
    if (bytesRead > 0) {
      printTraceLog(
          "Done read from Cache for the file with eTag: {}, position: {}, length: {}, requested by stream: {}",
          requestedETag, position, bytesRead, stream.hashCode());
      return bytesRead;
    }

    // otherwise, just say we got nothing - calling thread can do its own read
    return 0;
  }

  /**
   * {@link ReadBufferWorker} thread calls this to get the next buffer that it should work on.
   * @return {@link ReadBuffer}
   * @throws InterruptedException if thread is interrupted
   */
  @Override
  public ReadBuffer getNextBlockToRead() throws InterruptedException {
    ReadBuffer buffer = null;
    synchronized (this) {
      // Blocking Call to wait for prefetch to be queued.
      while (getReadAheadQueue().size() == 0) {
        wait();
      }

      buffer = getReadAheadQueue().remove();
      notifyAll();
      if (buffer == null) {
        return null;
      }
      buffer.setStatus(ReadBufferStatus.READING_IN_PROGRESS);
      getInProgressList().add(buffer);
    }
    printTraceLog(
        "ReadBufferWorker picked file: {}, with eTag: {}, for offset: {}, "
            + "queued by stream: {}",
        buffer.getPath(), buffer.getETag(), buffer.getOffset(),
        buffer.getStream().hashCode());
    return buffer;
  }

  /**
   * {@link ReadBufferWorker} thread calls this method to post completion.   *
   * @param buffer            the buffer whose read was completed
   * @param result            the {@link ReadBufferStatus} after the read operation in the worker thread
   * @param bytesActuallyRead the number of bytes that the worker thread was actually able to read
   */
  @Override
  public void doneReading(final ReadBuffer buffer,
      final ReadBufferStatus result,
      final int bytesActuallyRead) {
    printTraceLog(
        "ReadBufferWorker completed prefetch for file: {} with eTag: {}, for offset: {}, queued by stream: {}, with status: {} and bytes read: {}",
        buffer.getPath(), buffer.getETag(), buffer.getOffset(),
        buffer.getStream().hashCode(), result, bytesActuallyRead);
    synchronized (this) {
      // If this buffer has already been purged during
      // close of InputStream then we don't update the lists.
      if (getInProgressList().contains(buffer)) {
        getInProgressList().remove(buffer);
        if (result == ReadBufferStatus.AVAILABLE && bytesActuallyRead > 0) {
          // Successful read, so update the buffer status and length
          buffer.setStatus(ReadBufferStatus.AVAILABLE);
          buffer.setLength(bytesActuallyRead);
        } else {
          // Failed read, reuse buffer for next read, this buffer will be
          // evicted later based on eviction policy.
          pushToFreeList(buffer.getBufferindex());
        }
        // completed list also contains FAILED read buffers
        // for sending exception message to clients.
        buffer.setStatus(result);
        buffer.setTimeStamp(currentTimeMillis());
        getCompletedReadList().add(buffer);
      }
    }

    //outside the synchronized, since anyone receiving a wake-up from the latch must see safe-published results
    buffer.getLatch().countDown(); // wake up waiting threads (if any)
  }

  /**
   * Purging the buffers associated with an {@link AbfsInputStream}
   * from {@link ReadBufferManagerV2} when stream is closed.
   * @param stream input stream.
   */
  public synchronized void purgeBuffersForStream(AbfsInputStream stream) {
    printDebugLog("Purging stale buffers for AbfsInputStream {} ", stream);
    getReadAheadQueue().removeIf(
        readBuffer -> readBuffer.getStream() == stream);
    purgeList(stream, getCompletedReadList());
  }

  /**
   * Check if any buffer is already queued for the requested offset.
   * @param eTag the eTag of the file
   * @param requestedOffset the requested offset
   * @return whether any buffer is already queued
   */
  private boolean isAlreadyQueued(final String eTag,
      final long requestedOffset) {
    // returns true if any part of the buffer is already queued
    return (isInList(getReadAheadQueue(), eTag, requestedOffset)
        || isInList(getInProgressList(), eTag, requestedOffset)
        || isInList(getCompletedReadList(), eTag, requestedOffset));
  }

  /**
   * Check if any buffer in the list contains the requested offset.
   * @param list the list to check
   * @param eTag the eTag of the file
   * @param requestedOffset the requested offset
   * @return whether any buffer in the list contains the requested offset
   */
  private boolean isInList(final Collection<ReadBuffer> list, final String eTag,
      final long requestedOffset) {
    return (getFromList(list, eTag, requestedOffset) != null);
  }

  /**
   * Get the buffer from the list that contains the requested offset.
   * @param list the list to check
   * @param eTag the eTag of the file
   * @param requestedOffset the requested offset
   * @return the buffer if found, null otherwise
   */
  private ReadBuffer getFromList(final Collection<ReadBuffer> list,
      final String eTag,
      final long requestedOffset) {
    for (ReadBuffer buffer : list) {
      if (eTag.equals(buffer.getETag())) {
        if (buffer.getStatus() == ReadBufferStatus.AVAILABLE
            && requestedOffset >= buffer.getOffset()
            && requestedOffset < buffer.getOffset() + buffer.getLength()) {
          return buffer;
        } else if (requestedOffset >= buffer.getOffset()
            && requestedOffset
            < buffer.getOffset() + buffer.getRequestedLength()) {
          return buffer;
        }
      }
    }
    return null;
  }

  /**
   * If any buffer in the completed list can be reclaimed then reclaim it and return the buffer to free list.
   * The objective is to find just one buffer - there is no advantage to evicting more than one.
   * @return whether the eviction succeeded - i.e., were we able to free up one buffer
   */
  private synchronized boolean tryEvict() {
    ReadBuffer nodeToEvict = null;
    if (getCompletedReadList().size() <= 0) {
      return false;  // there are no evict-able buffers
    }

    long currentTimeInMs = currentTimeMillis();

    // first, try buffers where all bytes have been consumed (approximated as first and last bytes consumed)
    for (ReadBuffer buf : getCompletedReadList()) {
      if (buf.isFullyConsumed()) {
        nodeToEvict = buf;
        break;
      }
    }
    if (nodeToEvict != null) {
      return manualEviction(nodeToEvict);
    }

    // next, try buffers where any bytes have been consumed (maybe a bad idea? have to experiment and see)
    for (ReadBuffer buf : getCompletedReadList()) {
      if (buf.isAnyByteConsumed()) {
        nodeToEvict = buf;
        break;
      }
    }

    if (nodeToEvict != null) {
      return manualEviction(nodeToEvict);
    }

    // next, try any old nodes that have not been consumed
    // Failed read buffers (with buffer index=-1) that are older than
    // thresholdAge should be cleaned up, but at the same time should not
    // report successful eviction.
    // Queue logic expects that a buffer is freed up for read ahead when
    // eviction is successful, whereas a failed ReadBuffer would have released
    // its buffer when its status was set to READ_FAILED.
    long earliestBirthday = Long.MAX_VALUE;
    ArrayList<ReadBuffer> oldFailedBuffers = new ArrayList<>();
    for (ReadBuffer buf : getCompletedReadList()) {
      if ((buf.getBufferindex() != -1)
          && (buf.getTimeStamp() < earliestBirthday)) {
        nodeToEvict = buf;
        earliestBirthday = buf.getTimeStamp();
      } else if ((buf.getBufferindex() == -1)
          && (currentTimeInMs - buf.getTimeStamp())
          > getThresholdAgeMilliseconds()) {
        oldFailedBuffers.add(buf);
      }
    }

    for (ReadBuffer buf : oldFailedBuffers) {
      manualEviction(buf);
    }

    if ((currentTimeInMs - earliestBirthday > getThresholdAgeMilliseconds())
        && (nodeToEvict != null)) {
      return manualEviction(nodeToEvict);
    }

    printTraceLog("No buffer eligible for eviction");
    // nothing can be evicted
    return false;
  }

  /**
   * Evict the given buffer.
   * @param buf the buffer to evict
   * @return whether the eviction succeeded
   */
  private boolean evict(final ReadBuffer buf) {
    if (buf.getRefCount() > 0) {
      // If the buffer is still being read, then we cannot evict it.
      printTraceLog(
          "Cannot evict buffer with index: {}, file: {}, with eTag: {}, offset: {} as it is still being read by some input stream",
          buf.getBufferindex(), buf.getPath(), buf.getETag(), buf.getOffset());
      return false;
    }
    // As failed ReadBuffers (bufferIndx = -1) are saved in getCompletedReadList(),
    // avoid adding it to availableBufferList.
    if (buf.getBufferindex() != -1) {
      pushToFreeList(buf.getBufferindex());
    }
    getCompletedReadList().remove(buf);
    buf.setTracingContext(null);
    printTraceLog(
        "Eviction of Buffer Completed for BufferIndex: {}, file: {}, with eTag: {}, offset: {}, is fully consumed: {}, is partially consumed: {}",
        buf.getBufferindex(), buf.getPath(), buf.getETag(), buf.getOffset(),
        buf.isFullyConsumed(), buf.isAnyByteConsumed());
    return true;
  }

  /**
   * Wait for any in-progress read for the requested offset to complete.
   * @param eTag the eTag of the file
   * @param position the requested offset
   * @param isFirstRead whether this is the first read of the stream
   */
  private void waitForProcess(final String eTag,
      final long position,
      boolean isFirstRead) {
    ReadBuffer readBuf;
    synchronized (this) {
      readBuf = clearFromReadAheadQueue(eTag, position, isFirstRead);
      if (readBuf == null) {
        readBuf = getFromList(getInProgressList(), eTag, position);
      }
    }
    if (readBuf != null) {         // if in in-progress queue, then block for it
      try {
        printTraceLog(
            "A relevant read buffer for file: {}, with eTag: {}, offset: {}, "
                + "queued by stream: {}, having buffer idx: {} is being prefetched, waiting for latch",
            readBuf.getPath(), readBuf.getETag(), readBuf.getOffset(),
            readBuf.getStream().hashCode(), readBuf.getBufferindex());
        readBuf.getLatch()
            .await();  // blocking wait on the caller stream's thread
        // Note on correctness: readBuf gets out of getInProgressList() only in 1 place: after worker thread
        // is done processing it (in doneReading). There, the latch is set after removing the buffer from
        // getInProgressList(). So this latch is safe to be outside the synchronized block.
        // Putting it in synchronized would result in a deadlock, since this thread would be holding the lock
        // while waiting, so no one will be able to  change any state. If this becomes more complex in the future,
        // then the latch can be removed and replaced with wait/notify whenever getInProgressList() is touched.
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
      printTraceLog("Latch done for file: {}, with eTag: {}, for offset: {}, "
              + "buffer index: {} queued by stream: {}", readBuf.getPath(),
          readBuf.getETag(),
          readBuf.getOffset(), readBuf.getBufferindex(),
          readBuf.getStream().hashCode());
    }
  }

  /**
   * Clear the buffer from read-ahead queue if it exists.
   * @param eTag the eTag of the file
   * @param requestedOffset the requested offset
   * @param isFirstRead whether this is the first read of the stream
   * @return the buffer if found, null otherwise
   */
  private ReadBuffer clearFromReadAheadQueue(final String eTag,
      final long requestedOffset,
      boolean isFirstRead) {
    ReadBuffer buffer = getFromList(getReadAheadQueue(), eTag, requestedOffset);
    /*
     * If this prefetch was triggered by first read of this input stream,
     * we should not remove it from queue and let it complete by backend threads.
     */
    if (buffer != null && isFirstRead) {
      return buffer;
    }
    if (buffer != null) {
      getReadAheadQueue().remove(buffer);
      notifyAll();   // lock is held in calling method
      pushToFreeList(buffer.getBufferindex());
    }
    return null;
  }

  /**
   * Get the block from completed queue if it exists.
   * @param eTag the eTag of the file
   * @param position the requested offset
   * @param length the length to read
   * @param buffer the buffer to read data into
   * @return the number of bytes read
   * @throws IOException if an I/O error occurs
   */
  private int getBlockFromCompletedQueue(final String eTag, final long position,
      final int length, final byte[] buffer) throws IOException {
    ReadBuffer buf = getBufferFromCompletedQueue(eTag, position);

    if (buf == null) {
      return 0;
    }

    buf.startReading(); // atomic increment of refCount.

    if (buf.getStatus() == ReadBufferStatus.READ_FAILED) {
      // To prevent new read requests to fail due to old read-ahead attempts,
      // return exception only from buffers that failed within last getThresholdAgeMilliseconds()
      if ((currentTimeMillis() - (buf.getTimeStamp())
          < getThresholdAgeMilliseconds())) {
        throw buf.getErrException();
      } else {
        return 0;
      }
    }

    if ((buf.getStatus() != ReadBufferStatus.AVAILABLE)
        || (position >= buf.getOffset() + buf.getLength())) {
      return 0;
    }

    int cursor = (int) (position - buf.getOffset());
    int availableLengthInBuffer = buf.getLength() - cursor;
    int lengthToCopy = Math.min(length, availableLengthInBuffer);
    System.arraycopy(buf.getBuffer(), cursor, buffer, 0, lengthToCopy);
    if (cursor == 0) {
      buf.setFirstByteConsumed(true);
    }
    if (cursor + lengthToCopy == buf.getLength()) {
      buf.setLastByteConsumed(true);
    }
    buf.setAnyByteConsumed(true);

    buf.endReading(); // atomic decrement of refCount
    return lengthToCopy;
  }

  /**
   * Get the buffer from completed queue that contains the requested offset.
   * @param eTag the eTag of the file
   * @param requestedOffset the requested offset
   * @return the buffer if found, null otherwise
   */
  private ReadBuffer getBufferFromCompletedQueue(final String eTag,
      final long requestedOffset) {
    for (ReadBuffer buffer : getCompletedReadList()) {
      // Buffer is returned if the requestedOffset is at or above buffer's
      // offset but less than buffer's length or the actual requestedLength
      if (eTag.equals(buffer.getETag())
          && (requestedOffset >= buffer.getOffset())
          && ((requestedOffset < buffer.getOffset() + buffer.getLength())
          || (requestedOffset
          < buffer.getOffset() + buffer.getRequestedLength()))) {
        return buffer;
      }
    }
    return null;
  }

  /**
   * Try to upscale memory by adding more buffers to the pool if memory usage is below threshold.
   * @return whether the upscale succeeded
   */
  private synchronized boolean tryMemoryUpscale() {
    if (!isDynamicScalingEnabled) {
      printTraceLog("Dynamic scaling is disabled, skipping memory upscale");
      return false; // Dynamic scaling is disabled, so no upscaling.
    }
    double memoryLoad = ResourceUtilizationUtils.getMemoryLoad();
    if (memoryLoad < memoryThreshold && getNumBuffers() < maxBufferPoolSize) {
      // Create and Add more buffers in getFreeList().
      int nextIndx = getNumBuffers();
      if (removedBufferList.isEmpty()) {
        if (nextIndx >= bufferPool.length) {
          printTraceLog("Invalid next index: {}. Current buffer pool size: {}",
              nextIndx, bufferPool.length);
          return false;
        }
        bufferPool[nextIndx] = new byte[getReadAheadBlockSize()];
        pushToFreeList(nextIndx);
      } else {
        // Reuse a removed buffer index.
        int freeIndex = removedBufferList.pollFirst();
        if (freeIndex >= bufferPool.length || bufferPool[freeIndex] != null) {
          printTraceLog("Invalid free index: {}. Current buffer pool size: {}",
              freeIndex, bufferPool.length);
          return false;
        }
        bufferPool[freeIndex] = new byte[getReadAheadBlockSize()];
        pushToFreeList(freeIndex);
      }
      incrementActiveBufferCount();
      printTraceLog(
          "Current Memory Load: {}. Incrementing buffer pool size to {}",
          memoryLoad, getNumBuffers());
      return true;
    }
    printTraceLog("Could not Upscale memory. Total buffers: {} Memory Load: {}",
        getNumBuffers(), memoryLoad);
    return false;
  }

  /**
   * Scheduled Eviction task that runs periodically to evict old buffers.
   */
  private void scheduledEviction() {
    for (ReadBuffer buf : getCompletedReadList()) {
      if (currentTimeMillis() - buf.getTimeStamp()
          > getThresholdAgeMilliseconds()) {
        // If the buffer is older than thresholdAge, evict it.
        printTraceLog(
            "Scheduled Eviction of Buffer Triggered for BufferIndex: {}, "
                + "file: {}, with eTag: {}, offset: {}, length: {}, queued by stream: {}",
            buf.getBufferindex(), buf.getPath(), buf.getETag(), buf.getOffset(),
            buf.getLength(), buf.getStream().hashCode());
        evict(buf);
      }
    }

    double memoryLoad = ResourceUtilizationUtils.getMemoryLoad();
    if (isDynamicScalingEnabled && memoryLoad > memoryThreshold && getNumBuffers() > minBufferPoolSize) {
      synchronized (this) {
        if (isFreeListEmpty()) {
          printTraceLog(
              "No free buffers available. Skipping downscale of buffer pool");
          return; // No free buffers available, so cannot downscale.
        }
        int freeIndex = popFromFreeList();
        if (freeIndex > bufferPool.length || bufferPool[freeIndex] == null) {
          printTraceLog("Invalid free index: {}. Current buffer pool size: {}",
              freeIndex, bufferPool.length);
          return;
        }
        bufferPool[freeIndex] = null;
        removedBufferList.add(freeIndex);
        decrementActiveBufferCount();
        printTraceLog(
            "Current Memory Load: {}. Decrementing buffer pool size to {}",
            memoryLoad, getNumBuffers());
      }
    }
  }

  /**
   * Manual Eviction of a buffer.
   * @param buf the buffer to evict
   * @return whether the eviction succeeded
   */
  private boolean manualEviction(final ReadBuffer buf) {
    printTraceLog(
        "Manual Eviction of Buffer Triggered for BufferIndex: {}, file: {}, with eTag: {}, offset: {}, queued by stream: {}",
        buf.getBufferindex(), buf.getPath(), buf.getETag(), buf.getOffset(),
        buf.getStream().hashCode());
    return evict(buf);
  }

  /**
   * Adjust the thread pool size based on CPU load and queue size.
   */
  private void adjustThreadPool() {
    int currentPoolSize = workerRefs.size();
    long cpuLoad = ResourceUtilizationUtils.getJvmCpuLoad();
    if (cpuLoad > maxJvmCpuUtilization) {
      maxJvmCpuUtilization = cpuLoad;
    }
    int requiredPoolSize = getRequiredThreadPoolSize();
    int newThreadPoolSize;
    printTraceLog(
        "Current CPU load: {}, Current worker pool size: {}, Current queue size: {}",
        cpuLoad, currentPoolSize, requiredPoolSize);
    if (currentPoolSize < requiredPoolSize && cpuLoad < cpuThreshold) {
      // Submit more background tasks.
      newThreadPoolSize = Math.min(maxThreadPoolSize,
          (int) Math.ceil(
              (currentPoolSize * (HUNDRED_D + threadPoolUpscalePercentage))
                  / HUNDRED_D));
      if (currentPoolSize == maxThreadPoolSize || newThreadPoolSize == currentPoolSize) {
        lastScaleDirection = SCALE_DIRECTION_NO_UP_AT_MAX;   // Already full, cannot scale up
      } else {
        lastScaleDirection = SCALE_DIRECTION_UP;
      }
      // Create new Worker Threads
      if (SCALE_DIRECTION_UP.equals(lastScaleDirection)) {
        for (int i = currentPoolSize; i < newThreadPoolSize; i++) {
          ReadBufferWorker worker = new ReadBufferWorker(i, getBufferManager(abfsCounters));
          workerRefs.add(worker);
          workerPool.submit(worker);
        }
      }
      // Capture the latest thread pool statistics (pool size, CPU, memory, etc.)
      ReadThreadPoolStats stats = getCurrentStats(cpuLoad);
      // Update the read thread pool metrics with the latest statistics snapshot.
      readThreadPoolMetrics.update(stats);
      printTraceLog("Increased worker pool size from {} to {}", currentPoolSize,
          newThreadPoolSize);
    } else if (cpuLoad < cpuThreshold && currentPoolSize > requiredPoolSize) {
      lastScaleDirection = SCALE_DIRECTION_NO_ACTION_NEEDED;
      // Capture the latest thread pool statistics (pool size, CPU, memory, etc.)
      ReadThreadPoolStats stats = getCurrentStats(cpuLoad);
      // Update the read thread pool metrics with the latest statistics snapshot.
      readThreadPoolMetrics.update(stats);
    } else if (cpuLoad > cpuThreshold || currentPoolSize > requiredPoolSize) {
      newThreadPoolSize = Math.max(minThreadPoolSize,
          (int) Math.ceil(
              (currentPoolSize * (HUNDRED_D - threadPoolDownscalePercentage))
                  / HUNDRED_D));
      if (currentPoolSize == minThreadPoolSize || newThreadPoolSize == currentPoolSize) {
        lastScaleDirection = SCALE_DIRECTION_NO_DOWN_AT_MIN;  // Already at minimum, cannot scale down
      } else {
        lastScaleDirection = SCALE_DIRECTION_DOWN;
      }
      if (SCALE_DIRECTION_DOWN.equals(lastScaleDirection)) {
        // Signal the extra workers to stop
        while (workerRefs.size() > newThreadPoolSize) {
          ReadBufferWorker worker = workerRefs.remove(workerRefs.size() - 1);
          worker.stop();
        }
      }
      // Capture the latest thread pool statistics (pool size, CPU, memory, etc.)
      ReadThreadPoolStats stats = getCurrentStats(cpuLoad);
      // Update the read thread pool metrics with the latest statistics snapshot.
      readThreadPoolMetrics.update(stats);
      printTraceLog("Decreased worker pool size from {} to {}", currentPoolSize,
          newThreadPoolSize);
    } else {
      lastScaleDirection = EMPTY_STRING;
      printTraceLog("No change in worker pool size. CPU load: {} Pool size: {}",
          cpuLoad, currentPoolSize);
      if (cpuLoad >= maxJvmCpuUtilization) {
        ReadThreadPoolStats stats = getCurrentStats(cpuLoad);
        readThreadPoolMetrics.update(stats); // publish snapshot
      }
    }
  }

  /**
   * Similar to System.currentTimeMillis, except implemented with System.nanoTime().
   * System.currentTimeMillis can go backwards when system clock is changed (e.g., with NTP time synchronization),
   * making it unsuitable for measuring time intervals. nanotime is strictly monotonically increasing per CPU core.
   * Note: it is not monotonic across Sockets, and even within a CPU, its only the
   * more recent parts which share a clock across all cores.
   *
   * @return current time in milliseconds
   */
  private long currentTimeMillis() {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
  }

  /**
   * Purge all buffers associated with the given stream from the given list.
   * @param stream the stream whose buffers are to be purged
   * @param list the list to purge from
   */
  private void purgeList(AbfsInputStream stream, LinkedList<ReadBuffer> list) {
    for (Iterator<ReadBuffer> it = list.iterator(); it.hasNext();) {
      ReadBuffer readBuffer = it.next();
      if (readBuffer.getStream() == stream) {
        it.remove();
        // As failed ReadBuffers (bufferIndex = -1) are already pushed to free
        // list in doneReading method, we will skip adding those here again.
        if (readBuffer.getBufferindex() != -1) {
          pushToFreeList(readBuffer.getBufferindex());
        }
      }
    }
  }

  /**
   * Test method that can clean up the current state of readAhead buffers and
   * the lists. Will also trigger a fresh init.
   */
  @VisibleForTesting
  @Override
  public void testResetReadBufferManager() {
    synchronized (this) {
      ArrayList<ReadBuffer> completedBuffers = new ArrayList<>();
      for (ReadBuffer buf : getCompletedReadList()) {
        if (buf != null) {
          completedBuffers.add(buf);
        }
      }

      for (ReadBuffer buf : completedBuffers) {
        manualEviction(buf);
      }

      getReadAheadQueue().clear();
      getInProgressList().clear();
      getCompletedReadList().clear();
      clearFreeList();
      for (int i = 0; i < maxBufferPoolSize; i++) {
        bufferPool[i] = null;
      }
      bufferPool = null;
      if (cpuMonitorThread != null) {
        cpuMonitorThread.shutdownNow();
      }
      if (memoryMonitorThread != null) {
        memoryMonitorThread.shutdownNow();
      }
      if (workerPool != null) {
        workerPool.shutdownNow();
      }
      resetBufferManager();
    }
  }

  @VisibleForTesting
  @Override
  public void testResetReadBufferManager(int readAheadBlockSize,
      int thresholdAgeMilliseconds) {
    setReadAheadBlockSize(readAheadBlockSize);
    setThresholdAgeMilliseconds(thresholdAgeMilliseconds);
    testResetReadBufferManager();
  }

  @VisibleForTesting
  public void callTryEvict() {
    tryEvict();
  }

  @VisibleForTesting
  public int getNumBuffers() {
    return numberOfActiveBuffers.get();
  }

  @Override
  void resetBufferManager() {
    setBufferManager(null); // reset the singleton instance
    setIsConfigured(false);
  }

  @Override
  protected List<Integer> getFreeListCopy() {
    return new ArrayList<>(freeList);
  }

  @Override
  protected void clearFreeList() {
    freeList.clear();
  }

  private static void setBufferManager(ReadBufferManagerV2 manager) {
    bufferManager = manager;
  }

  private static void setIsConfigured(boolean configured) {
    isConfigured.set(configured);
  }

  private final ThreadFactory workerThreadFactory = new ThreadFactory() {
    private int count = 0;

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new SubjectInheritingThread(r, "ReadAheadV2-WorkerThread-" + count++);
      t.setDaemon(true);
      return t;
    }
  };

  private void printTraceLog(String message, Object... args) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(message, args);
    }
  }

  private void printDebugLog(String message, Object... args) {
    LOGGER.debug(message, args);
  }

  @VisibleForTesting
  synchronized static ReadBufferManagerV2 getInstance() {
    return bufferManager;
  }

  @VisibleForTesting
  public int getMinBufferPoolSize() {
    return minBufferPoolSize;
  }

  @VisibleForTesting
  public void setMinBufferPoolSize(int size) {
    this.minBufferPoolSize = size;
  }

  @VisibleForTesting
  public int getMaxBufferPoolSize() {
    return maxBufferPoolSize;
  }

  /**
   * Gets the maximum buffer pool size.
   * @return size of the maximum buffer pool
   */
  @VisibleForTesting
  public int getCurrentThreadPoolSize() {
    return workerRefs.size();
  }

  @VisibleForTesting
  public int getCpuMonitoringIntervalInMilliSec() {
    return cpuMonitoringIntervalInMilliSec;
  }

  @VisibleForTesting
  public int getMemoryMonitoringIntervalInMilliSec() {
    return memoryMonitoringIntervalInMilliSec;
  }

  /**
   * Returns the scheduled executor service used for CPU monitoring.
   * @return the ScheduledExecutorService for CPU monitoring tasks
   */
  @VisibleForTesting
  public ScheduledExecutorService getCpuMonitoringThread() {
    return cpuMonitorThread;
  }

  /**
   * Returns the maximum JVM CPU utilization observed during the current
   * monitoring interval or since the last reset.
   *
   * @return the highest JVM CPU utilization percentage recorded
   */
  @VisibleForTesting
  public long getMaxJvmCpuUtilization() {
    return maxJvmCpuUtilization;
  }

  /**
   * Calculates the required thread pool size based on the current
   * read-ahead queue size and in-progress list size, applying a buffer
   * to accommodate workload fluctuations.
   *
   * @return the calculated required thread pool size
   */
  public int getRequiredThreadPoolSize() {
    return (int) Math.ceil(THREAD_POOL_REQUIREMENT_BUFFER
        * (getReadAheadQueue().size()
        + getInProgressList().size())); // 20% more for buffer
  }

  private boolean isFreeListEmpty() {
    return this.freeList.isEmpty();
  }

  private Integer popFromFreeList() {
    return this.freeList.pollFirst();
  }

  private void pushToFreeList(int idx) {
    this.freeList.add(idx);
  }

  private void incrementActiveBufferCount() {
    numberOfActiveBuffers.getAndIncrement();
  }

  private void decrementActiveBufferCount() {
    numberOfActiveBuffers.getAndDecrement();
  }

  /**
   * Represents current statistics of the read thread pool and system.
   */
  public static class ReadThreadPoolStats extends ResourceUtilizationStats {

    /**
     * Constructs a {@link ReadThreadPoolStats} instance containing thread pool
     * metrics and JVM/system resource utilization details.
     *
     * @param currentPoolSize the current number of threads in the pool
     * @param maxPoolSize the maximum number of threads permitted in the pool
     * @param activeThreads the number of threads actively executing tasks
     * @param idleThreads the number of idle threads in the pool
     * @param jvmCpuLoad the current JVM CPU load (0.0–1.0)
     * @param systemCpuUtilization the current system-wide CPU utilization (0.0–1.0)
     * @param availableHeapGB the available heap memory in gigabytes
     * @param committedHeapGB the committed heap memory in gigabytes
     * @param usedHeapGB the available heap memory in gigabytes
     * @param maxHeapGB the committed heap memory in gigabytes
     * @param memoryLoad the JVM memory load (used / max)
     * @param lastScaleDirection the last scaling action performed: "I" (increase),
     * "D" (decrease), or empty if no scaling occurred
     * @param maxCpuUtilization the peak JVM CPU utilization observed during this interval
     * 9*+@param jvmProcessId the process ID of the JVM
     */
    public ReadThreadPoolStats(int currentPoolSize,
        int maxPoolSize, int activeThreads, int idleThreads,
        long jvmCpuLoad,
        long systemCpuUtilization, long availableHeapGB,
        long committedHeapGB, long usedHeapGB, long maxHeapGB, long memoryLoad,
        String lastScaleDirection, long maxCpuUtilization, long jvmProcessId) {
      super(currentPoolSize, maxPoolSize, activeThreads, idleThreads,
          jvmCpuLoad, systemCpuUtilization, availableHeapGB,
          committedHeapGB, usedHeapGB, maxHeapGB, memoryLoad, lastScaleDirection,
          maxCpuUtilization, jvmProcessId);
    }
  }

  /**
   * Creates and returns a snapshot of the current read thread pool and system metrics.
   * This method captures live values such as pool size, active threads, JVM CPU load,
   * system CPU utilization, available heap memory, and the maximum CPU utilization
   * observed during the current interval.
   *
   * @param jvmCpuLoad the current JVM process CPU utilization percentage
   * @return a {@link ReadThreadPoolStats} object containing the current thread pool
   *         and system resource statistics
   */
  synchronized ReadThreadPoolStats getCurrentStats(long jvmCpuLoad) {
    if (workerPool == null) {
      return new ReadThreadPoolStats(ZERO, ZERO, ZERO, ZERO, ZERO, ZERO,
          ZERO, ZERO, ZERO, ZERO, ZERO, EMPTY_STRING, ZERO, ZERO);
    }

    ThreadPoolExecutor exec = this.workerPool;
    String currentScaleDirection = lastScaleDirection;
    lastScaleDirection = EMPTY_STRING;

    int poolSize = exec.getPoolSize();
    int activeThreads = exec.getActiveCount();
    int idleThreads = poolSize - activeThreads;

    return new ReadThreadPoolStats(
        poolSize,                      // Current thread count
        exec.getMaximumPoolSize(),     // Max allowed threads
        activeThreads,                 // Busy threads
        idleThreads,                   // Idle threads
        jvmCpuLoad,             // JVM CPU usage (ratio)
        ResourceUtilizationUtils.getSystemCpuLoad(),     // System CPU usage (ratio)
        ResourceUtilizationUtils.getAvailableHeapMemory(),      // Free heap (GB)
        ResourceUtilizationUtils.getCommittedHeapMemory(),      // Committed heap (GB)
        ResourceUtilizationUtils.getUsedHeapMemory(),   // Used heap (GB)
        ResourceUtilizationUtils.getMaxHeapMemory(),    // Max heap (GB)
        ResourceUtilizationUtils.getMemoryLoad(),                    // used/max
        currentScaleDirection,         // "I", "D", or ""
        getMaxJvmCpuUtilization(),             // Peak JVM CPU usage so far,
        JvmUniqueIdProvider.getJvmId()           // JVM process id.
    );
  }
}
