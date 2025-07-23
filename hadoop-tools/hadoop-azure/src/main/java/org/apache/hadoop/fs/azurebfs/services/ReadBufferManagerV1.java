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

import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.classification.VisibleForTesting;

/**
 * The Read Buffer Manager for Rest AbfsClient.
 * V1 implementation of ReadBufferManager.
 */
final class ReadBufferManagerV1 extends ReadBufferManager {

  private static final int NUM_BUFFERS = 16;
  private static final int NUM_THREADS = 8;
  private static final int DEFAULT_THRESHOLD_AGE_MILLISECONDS = 3000;

  private Thread[] threads = new Thread[NUM_THREADS];
  private byte[][] buffers;
  private static  ReadBufferManagerV1 bufferManager;

  // hide instance constructor
  private ReadBufferManagerV1() {
    LOGGER.trace("Creating readbuffer manager with HADOOP-18546 patch");
  }

  /**
   * Sets the read buffer manager configurations.
   * @param readAheadBlockSize the size of the read-ahead block in bytes
   */
  static void setReadBufferManagerConfigs(int readAheadBlockSize) {
    if (bufferManager == null) {
      LOGGER.debug(
          "ReadBufferManagerV1 not initialized yet. Overriding readAheadBlockSize as {}",
          readAheadBlockSize);
      setReadAheadBlockSize(readAheadBlockSize);
      setThresholdAgeMilliseconds(DEFAULT_THRESHOLD_AGE_MILLISECONDS);
    }
  }

  /**
   * Returns the singleton instance of ReadBufferManagerV1.
   * @return the singleton instance of ReadBufferManagerV1
   */
  static ReadBufferManagerV1 getBufferManager() {
    if (bufferManager == null) {
      LOCK.lock();
      try {
        if (bufferManager == null) {
          bufferManager = new ReadBufferManagerV1();
          bufferManager.init();
        }
      } finally {
        LOCK.unlock();
      }
    }
    return bufferManager;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  void init() {
    buffers = new byte[NUM_BUFFERS][];
    for (int i = 0; i < NUM_BUFFERS; i++) {
      buffers[i] = new byte[getReadAheadBlockSize()];  // same buffers are reused. These byte arrays are never garbage collected
      getFreeList().add(i);
    }
    for (int i = 0; i < NUM_THREADS; i++) {
      Thread t = new Thread(new ReadBufferWorker(i, this));
      t.setDaemon(true);
      threads[i] = t;
      t.setName("ABFS-prefetch-" + i);
      t.start();
    }
    ReadBufferWorker.UNLEASH_WORKERS.countDown();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void queueReadAhead(final AbfsInputStream stream, final long requestedOffset, final int requestedLength,
      TracingContext tracingContext) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Start Queueing readAhead for {} offset {} length {}",
          stream.getPath(), requestedOffset, requestedLength);
    }
    ReadBuffer buffer;
    synchronized (this) {
      if (isAlreadyQueued(stream, requestedOffset)) {
        return; // already queued, do not queue again
      }
      if (getFreeList().isEmpty() && !tryEvict()) {
        return; // no buffers available, cannot queue anything
      }

      buffer = new ReadBuffer();
      buffer.setStream(stream);
      buffer.setOffset(requestedOffset);
      buffer.setLength(0);
      buffer.setRequestedLength(requestedLength);
      buffer.setStatus(ReadBufferStatus.NOT_AVAILABLE);
      buffer.setLatch(new CountDownLatch(1));
      buffer.setTracingContext(tracingContext);

      Integer bufferIndex = getFreeList().pop();  // will return a value, since we have checked size > 0 already

      buffer.setBuffer(buffers[bufferIndex]);
      buffer.setBufferindex(bufferIndex);
      getReadAheadQueue().add(buffer);
      notifyAll();
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Done q-ing readAhead for file {} offset {} buffer idx {}",
            stream.getPath(), requestedOffset, buffer.getBufferindex());
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getBlock(final AbfsInputStream stream, final long position, final int length, final byte[] buffer)
      throws IOException {
    // not synchronized, so have to be careful with locking
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("getBlock for file {}  position {}  thread {}",
          stream.getPath(), position, Thread.currentThread().getName());
    }

    waitForProcess(stream, position);

    int bytesRead = 0;
    synchronized (this) {
      bytesRead = getBlockFromCompletedQueue(stream, position, length, buffer);
    }
    if (bytesRead > 0) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Done read from Cache for {} position {} length {}",
            stream.getPath(), position, bytesRead);
      }
      return bytesRead;
    }

    // otherwise, just say we got nothing - calling thread can do its own read
    return 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ReadBuffer getNextBlockToRead() throws InterruptedException {
    ReadBuffer buffer = null;
    synchronized (this) {
      while (getReadAheadQueue().isEmpty()) {
        wait();
      }
      buffer = getReadAheadQueue().remove();
      notifyAll();
      if (buffer == null) {
        return null;            // should never happen
      }
      buffer.setStatus(ReadBufferStatus.READING_IN_PROGRESS);
      getInProgressList().add(buffer);
    }
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("ReadBufferWorker picked file {} for offset {}",
          buffer.getStream().getPath(), buffer.getOffset());
    }
    return buffer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void doneReading(final ReadBuffer buffer, final ReadBufferStatus result, final int bytesActuallyRead) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("ReadBufferWorker completed read file {} for offset {} outcome {} bytes {}",
          buffer.getStream().getPath(),  buffer.getOffset(), result, bytesActuallyRead);
    }
    synchronized (this) {
      // If this buffer has already been purged during
      // close of InputStream then we don't update the lists.
      if (getInProgressList().contains(buffer)) {
        getInProgressList().remove(buffer);
        if (result == ReadBufferStatus.AVAILABLE && bytesActuallyRead > 0) {
          buffer.setStatus(ReadBufferStatus.AVAILABLE);
          buffer.setLength(bytesActuallyRead);
        } else {
          getFreeList().push(buffer.getBufferindex());
          // buffer will be deleted as per the eviction policy.
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
   * {@inheritDoc}
   */
  @Override
  public synchronized void purgeBuffersForStream(AbfsInputStream stream) {
    LOGGER.debug("Purging stale buffers for AbfsInputStream {} ", stream);
    getReadAheadQueue().removeIf(readBuffer -> readBuffer.getStream() == stream);
    purgeList(stream, getCompletedReadList());
  }

  /**
   * Waits for the process to complete for the given stream and position.
   * If the buffer is in progress, it waits for the latch to be released.
   * If the buffer is not in progress, it clears it from the read-ahead queue.
   *
   * @param stream    the AbfsInputStream associated with the read request
   * @param position  the position in the stream to wait for
   */
  private void waitForProcess(final AbfsInputStream stream, final long position) {
    ReadBuffer readBuf;
    synchronized (this) {
      clearFromReadAheadQueue(stream, position);
      readBuf = getFromList(getInProgressList(), stream, position);
    }
    if (readBuf != null) {         // if in in-progress queue, then block for it
      try {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("got a relevant read buffer for file {} offset {} buffer idx {}",
              stream.getPath(), readBuf.getOffset(), readBuf.getBufferindex());
        }
        readBuf.getLatch().await();  // blocking wait on the caller stream's thread
        // Note on correctness: readBuf gets out of inProgressList only in 1 place: after worker thread
        // is done processing it (in doneReading). There, the latch is set after removing the buffer from
        // inProgressList. So this latch is safe to be outside the synchronized block.
        // Putting it in synchronized would result in a deadlock, since this thread would be holding the lock
        // while waiting, so no one will be able to  change any state. If this becomes more complex in the future,
        // then the latch cane be removed and replaced with wait/notify whenever inProgressList is touched.
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("latch done for file {} buffer idx {} length {}",
            stream.getPath(), readBuf.getBufferindex(), readBuf.getLength());
      }
    }
  }

  /**
   * If any buffer in the completedlist can be reclaimed then reclaim it and return the buffer to free list.
   * The objective is to find just one buffer - there is no advantage to evicting more than one.
   *
   * @return whether the eviction succeeeded - i.e., were we able to free up one buffer
   */
  private synchronized boolean tryEvict() {
    ReadBuffer nodeToEvict = null;
    if (getCompletedReadList().size() <= 0) {
      return false;  // there are no evict-able buffers
    }

    long currentTimeInMs = currentTimeMillis();

    // first, try buffers where all bytes have been consumed (approximated as first and last bytes consumed)
    for (ReadBuffer buf : getCompletedReadList()) {
      if (buf.isFirstByteConsumed() && buf.isLastByteConsumed()) {
        nodeToEvict = buf;
        break;
      }
    }
    if (nodeToEvict != null) {
      return evict(nodeToEvict);
    }

    // next, try buffers where any bytes have been consumed (maybe a bad idea? have to experiment and see)
    for (ReadBuffer buf : getCompletedReadList()) {
      if (buf.isAnyByteConsumed()) {
        nodeToEvict = buf;
        break;
      }
    }

    if (nodeToEvict != null) {
      return evict(nodeToEvict);
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
          && (currentTimeInMs - buf.getTimeStamp()) > getThresholdAgeMilliseconds()) {
        oldFailedBuffers.add(buf);
      }
    }

    for (ReadBuffer buf : oldFailedBuffers) {
      evict(buf);
    }

    if ((currentTimeInMs - earliestBirthday > getThresholdAgeMilliseconds()) && (nodeToEvict != null)) {
      return evict(nodeToEvict);
    }

    LOGGER.trace("No buffer eligible for eviction");
    // nothing can be evicted
    return false;
  }

  /**
   * Evicts the given buffer by removing it from the completedReadList and adding its index to the freeList.
   *
   * @param buf the ReadBuffer to evict
   * @return true if eviction was successful, false otherwise
   */
  private boolean evict(final ReadBuffer buf) {
    // As failed ReadBuffers (bufferIndx = -1) are saved in completedReadList,
    // avoid adding it to freeList.
    if (buf.getBufferindex() != -1) {
      getFreeList().push(buf.getBufferindex());
    }

    getCompletedReadList().remove(buf);
    buf.setTracingContext(null);
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Evicting buffer idx {}; was used for file {} offset {} length {}",
          buf.getBufferindex(), buf.getStream().getPath(), buf.getOffset(), buf.getLength());
    }
    return true;
  }

  /**
   * Checks if the requested offset is already queued in any of the lists:
   * @param stream the AbfsInputStream associated with the read request
   * @param requestedOffset the offset in the stream to check
   * @return true if the requested offset is already queued in any of the lists,
   */
  private boolean isAlreadyQueued(final AbfsInputStream stream, final long requestedOffset) {
    // returns true if any part of the buffer is already queued
    return (isInList(getReadAheadQueue(), stream, requestedOffset)
        || isInList(getInProgressList(), stream, requestedOffset)
        || isInList(getCompletedReadList(), stream, requestedOffset));
  }

  /**
   * Checks if the requested offset is in the given list.
   * @param list the collection of ReadBuffer to check against
   * @param stream the AbfsInputStream associated with the read request
   * @param requestedOffset the offset in the stream to check
   * @return true if the requested offset is in the list,
   */
  private boolean isInList(final Collection<ReadBuffer> list, final AbfsInputStream stream, final long requestedOffset) {
    return (getFromList(list, stream, requestedOffset) != null);
  }

  /**
   * Returns the ReadBuffer from the given list that matches the stream and requested offset.
   * If the buffer is found, it checks if the requested offset is within the buffer's range.
   * @param list the collection of ReadBuffer to search in
   * @param stream the AbfsInputStream associated with the read request
   * @param requestedOffset the offset in the stream to check
   * @return the ReadBuffer if found, null otherwise
   */
  private ReadBuffer getFromList(final Collection<ReadBuffer> list, final AbfsInputStream stream, final long requestedOffset) {
    for (ReadBuffer buffer : list) {
      if (buffer.getStream() == stream) {
        if (buffer.getStatus() == ReadBufferStatus.AVAILABLE
            && requestedOffset >= buffer.getOffset()
            && requestedOffset < buffer.getOffset() + buffer.getLength()) {
          return buffer;
        } else if (requestedOffset >= buffer.getOffset()
            && requestedOffset < buffer.getOffset() + buffer.getRequestedLength()) {
          return buffer;
        }
      }
    }
    return null;
  }

  /**
   * Returns a ReadBuffer from the completedReadList that matches the stream and requested offset.
   * The buffer is returned if the requestedOffset is at or above buffer's offset but less than buffer's length
   * or the actual requestedLength.
   *
   * @param stream the AbfsInputStream associated with the read request
   * @param requestedOffset the offset in the stream to check
   * @return the ReadBuffer if found, null otherwise
   */
  private ReadBuffer getBufferFromCompletedQueue(final AbfsInputStream stream, final long requestedOffset) {
    for (ReadBuffer buffer : getCompletedReadList()) {
      // Buffer is returned if the requestedOffset is at or above buffer's
      // offset but less than buffer's length or the actual requestedLength
      if ((buffer.getStream() == stream)
          && (requestedOffset >= buffer.getOffset())
          && ((requestedOffset < buffer.getOffset() + buffer.getLength())
          || (requestedOffset < buffer.getOffset() + buffer.getRequestedLength()))) {
        return buffer;
      }
    }

    return null;
  }

  /**
   * Clears the buffer from the read-ahead queue for the given stream and requested offset.
   * This method is called when the stream is waiting for a process to complete.
   * It removes the buffer from the read-ahead queue and adds its index back to the free list.
   *
   * @param stream the AbfsInputStream associated with the read request
   * @param requestedOffset the offset in the stream to check
   */
  private void clearFromReadAheadQueue(final AbfsInputStream stream, final long requestedOffset) {
    ReadBuffer buffer = getFromList(getReadAheadQueue(), stream, requestedOffset);
    if (buffer != null) {
      getReadAheadQueue().remove(buffer);
      notifyAll();   // lock is held in calling method
      getFreeList().push(buffer.getBufferindex());
    }
  }

  /**
   * Gets a block of data from the completed read buffers.
   * If the buffer is found, it copies the data to the provided buffer and updates the status of the ReadBuffer.
   * If the buffer is not found or not available, it returns 0.
   *
   * @param stream the AbfsInputStream associated with the read request
   * @param position the position in the file to read from
   * @param length the number of bytes to read
   * @param buffer the buffer to store the read data
   * @return the number of bytes actually read
   * @throws IOException if an I/O error occurs while reading from the buffer
   */
  private int getBlockFromCompletedQueue(final AbfsInputStream stream, final long position, final int length,
      final byte[] buffer) throws IOException {
    ReadBuffer buf = getBufferFromCompletedQueue(stream, position);

    if (buf == null) {
      return 0;
    }

    if (buf.getStatus() == ReadBufferStatus.READ_FAILED) {
      // To prevent new read requests to fail due to old read-ahead attempts,
      // return exception only from buffers that failed within last thresholdAgeMilliseconds
      if ((currentTimeMillis() - (buf.getTimeStamp()) < getThresholdAgeMilliseconds())) {
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
    return lengthToCopy;
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
    return System.nanoTime() / 1000 / 1000;
  }

  /**
   * Method to remove buffers associated with a {@link AbfsInputStream}
   * when its close method is called.
   * NOTE: This method is not threadsafe and must be called inside a
   * synchronised block. See caller.
   * @param stream associated input stream.
   * @param list list of buffers like completedReadList or inProgressList
   */
  private void purgeList(AbfsInputStream stream, LinkedList<ReadBuffer> list) {
    for (Iterator<ReadBuffer> it = list.iterator(); it.hasNext();) {
      ReadBuffer readBuffer = it.next();
      if (readBuffer.getStream() == stream) {
        it.remove();
        // As failed ReadBuffers (bufferIndex = -1) are already pushed to free
        // list in doneReading method, we will skip adding those here again.
        if (readBuffer.getBufferindex() != -1) {
          getFreeList().push(readBuffer.getBufferindex());
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @VisibleForTesting
  @Override
  public int getNumBuffers() {
    return NUM_BUFFERS;
  }

  /**
   * {@inheritDoc}
   */
  @VisibleForTesting
  @Override
  public void callTryEvict() {
    tryEvict();
  }

  /**
   * {@inheritDoc}
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
        evict(buf);
      }

      getReadAheadQueue().clear();
      getInProgressList().clear();
      getCompletedReadList().clear();
      getFreeList().clear();
      for (int i = 0; i < NUM_BUFFERS; i++) {
        buffers[i] = null;
      }
      buffers = null;
      resetBufferManager();
    }
  }

  /**
   * {@inheritDoc}
   */
  @VisibleForTesting
  @Override
  public void testResetReadBufferManager(int readAheadBlockSize, int thresholdAgeMilliseconds) {
    setReadAheadBlockSize(readAheadBlockSize);
    setThresholdAgeMilliseconds(thresholdAgeMilliseconds);
    testResetReadBufferManager();
  }

  @Override
  void resetBufferManager() {
    setBufferManager(null); // reset the singleton instance
  }

  private static void setBufferManager(ReadBufferManagerV1 manager) {
    bufferManager = manager;
  }
}
