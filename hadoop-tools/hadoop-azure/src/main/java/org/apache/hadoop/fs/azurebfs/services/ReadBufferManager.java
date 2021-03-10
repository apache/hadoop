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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * The Read Buffer Manager for Rest AbfsClient.
 */
final class ReadBufferManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReadBufferManager.class);
  private static final int ONE_KB = 1024;
  private static final int ONE_MB = ONE_KB * ONE_KB;

  private static final int NUM_BUFFERS = 16;
  private static final int NUM_THREADS = 8;
  private static final int DEFAULT_THRESHOLD_AGE_MILLISECONDS = 3000; // have to see if 3 seconds is a good threshold

  private static int blockSize = 4 * ONE_MB;
  private static int thresholdAgeMilliseconds = DEFAULT_THRESHOLD_AGE_MILLISECONDS;
  private Thread[] threads = new Thread[NUM_THREADS];
  private byte[][] buffers;    // array of byte[] buffers, to hold the data that is read
  private Stack<Integer> freeList = new Stack<>();   // indices in buffers[] array that are available

  private Queue<ReadBuffer> readAheadQueue = new LinkedList<>(); // queue of requests that are not picked up by any worker thread yet
  private LinkedList<ReadBuffer> inProgressList = new LinkedList<>(); // requests being processed by worker threads
  private LinkedList<ReadBuffer> completedReadList = new LinkedList<>(); // buffers available for reading
  private static ReadBufferManager bufferManager; // singleton, initialized in static initialization block
  private static final ReentrantLock LOCK = new ReentrantLock();

  static ReadBufferManager getBufferManager() {
    if (bufferManager == null) {
      LOCK.lock();
      try {
        if (bufferManager == null) {
          bufferManager = new ReadBufferManager();
          bufferManager.init();
        }
      } finally {
        LOCK.unlock();
      }
    }
    return bufferManager;
  }

  static void setReadBufferManagerConfigs(int readAheadBlockSize) {
    if (bufferManager == null) {
      LOGGER.debug(
          "ReadBufferManager not initialized yet. Overriding readAheadBlockSize as {}",
          readAheadBlockSize);
      blockSize = readAheadBlockSize;
    }
  }

  private void init() {
    buffers = new byte[NUM_BUFFERS][];
    for (int i = 0; i < NUM_BUFFERS; i++) {
      buffers[i] = new byte[blockSize];  // same buffers are reused. The byte array never goes back to GC
      freeList.add(i);
    }
    for (int i = 0; i < NUM_THREADS; i++) {
      Thread t = new Thread(new ReadBufferWorker(i));
      t.setDaemon(true);
      threads[i] = t;
      t.setName("ABFS-prefetch-" + i);
      t.start();
    }
    ReadBufferWorker.UNLEASH_WORKERS.countDown();
  }

  // hide instance constructor
  private ReadBufferManager() {
  }


  /*
   *
   *  AbfsInputStream-facing methods
   *
   */


  /**
   * {@link AbfsInputStream} calls this method to queue read-aheads.
   *
   * @param stream          The {@link AbfsInputStream} for which to do the read-ahead
   * @param requestedOffset The offset in the file which shoukd be read
   * @param requestedLength The length to read
   */
  void queueReadAhead(final AbfsInputStream stream, final long requestedOffset, final int requestedLength) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Start Queueing readAhead for {} offset {} length {}",
          stream.getPath(), requestedOffset, requestedLength);
    }
    ReadBuffer buffer;
    synchronized (this) {
      if (isAlreadyQueued(stream, requestedOffset)) {
        return; // already queued, do not queue again
      }
      if (freeList.isEmpty() && !tryEvict()) {
        return; // no buffers available, cannot queue anything
      }

      buffer = new ReadBuffer();
      buffer.setStream(stream);
      buffer.setOffset(requestedOffset);
      buffer.setLength(0);
      buffer.setRequestedLength(requestedLength);
      buffer.setStatus(ReadBufferStatus.NOT_AVAILABLE);
      buffer.setLatch(new CountDownLatch(1));

      Integer bufferIndex = freeList.pop();  // will return a value, since we have checked size > 0 already

      buffer.setBuffer(buffers[bufferIndex]);
      buffer.setBufferindex(bufferIndex);
      readAheadQueue.add(buffer);
      notifyAll();
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Done q-ing readAhead for file {} offset {} buffer idx {}",
            stream.getPath(), requestedOffset, buffer.getBufferindex());
      }
    }
  }


  /**
   * {@link AbfsInputStream} calls this method read any bytes already available in a buffer (thereby saving a
   * remote read). This returns the bytes if the data already exists in buffer. If there is a buffer that is reading
   * the requested offset, then this method blocks until that read completes. If the data is queued in a read-ahead
   * but not picked up by a worker thread yet, then it cancels that read-ahead and reports cache miss. This is because
   * depending on worker thread availability, the read-ahead may take a while - the calling thread can do it's own
   * read to get the data faster (copmared to the read waiting in queue for an indeterminate amount of time).
   *
   * @param stream   the file to read bytes for
   * @param position the offset in the file to do a read for
   * @param length   the length to read
   * @param buffer   the buffer to read data into. Note that the buffer will be written into from offset 0.
   * @return the number of bytes read
   */
  int getBlock(final AbfsInputStream stream, final long position, final int length, final byte[] buffer)
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

  /*
   *
   *  Internal methods
   *
   */

  private void waitForProcess(final AbfsInputStream stream, final long position) {
    ReadBuffer readBuf;
    synchronized (this) {
      clearFromReadAheadQueue(stream, position);
      readBuf = getFromList(inProgressList, stream, position);
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
    if (completedReadList.size() <= 0) {
      return false;  // there are no evict-able buffers
    }

    long currentTimeInMs = currentTimeMillis();

    // first, try buffers where all bytes have been consumed (approximated as first and last bytes consumed)
    for (ReadBuffer buf : completedReadList) {
      if (buf.isFirstByteConsumed() && buf.isLastByteConsumed()) {
        nodeToEvict = buf;
        break;
      }
    }
    if (nodeToEvict != null) {
      return evict(nodeToEvict);
    }

    // next, try buffers where any bytes have been consumed (may be a bad idea? have to experiment and see)
    for (ReadBuffer buf : completedReadList) {
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
    for (ReadBuffer buf : completedReadList) {
      if ((buf.getBufferindex() != -1)
          && (buf.getTimeStamp() < earliestBirthday)) {
        nodeToEvict = buf;
        earliestBirthday = buf.getTimeStamp();
      } else if ((buf.getBufferindex() == -1)
          && (currentTimeInMs - buf.getTimeStamp()) > thresholdAgeMilliseconds) {
        oldFailedBuffers.add(buf);
      }
    }

    for (ReadBuffer buf : oldFailedBuffers) {
      evict(buf);
    }

    if ((currentTimeInMs - earliestBirthday > thresholdAgeMilliseconds) && (nodeToEvict != null)) {
      return evict(nodeToEvict);
    }

    LOGGER.trace("No buffer eligible for eviction");
    // nothing can be evicted
    return false;
  }

  private boolean evict(final ReadBuffer buf) {
    // As failed ReadBuffers (bufferIndx = -1) are saved in completedReadList,
    // avoid adding it to freeList.
    if (buf.getBufferindex() != -1) {
      freeList.push(buf.getBufferindex());
    }

    completedReadList.remove(buf);
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Evicting buffer idx {}; was used for file {} offset {} length {}",
          buf.getBufferindex(), buf.getStream().getPath(), buf.getOffset(), buf.getLength());
    }
    return true;
  }

  private boolean isAlreadyQueued(final AbfsInputStream stream, final long requestedOffset) {
    // returns true if any part of the buffer is already queued
    return (isInList(readAheadQueue, stream, requestedOffset)
        || isInList(inProgressList, stream, requestedOffset)
        || isInList(completedReadList, stream, requestedOffset));
  }

  private boolean isInList(final Collection<ReadBuffer> list, final AbfsInputStream stream, final long requestedOffset) {
    return (getFromList(list, stream, requestedOffset) != null);
  }

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
   * Returns buffers that failed or passed from completed queue.
   * @param stream
   * @param requestedOffset
   * @return
   */
  private ReadBuffer getBufferFromCompletedQueue(final AbfsInputStream stream, final long requestedOffset) {
    for (ReadBuffer buffer : completedReadList) {
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

  private void clearFromReadAheadQueue(final AbfsInputStream stream, final long requestedOffset) {
    ReadBuffer buffer = getFromList(readAheadQueue, stream, requestedOffset);
    if (buffer != null) {
      readAheadQueue.remove(buffer);
      notifyAll();   // lock is held in calling method
      freeList.push(buffer.getBufferindex());
    }
  }

  private int getBlockFromCompletedQueue(final AbfsInputStream stream, final long position, final int length,
                                         final byte[] buffer) throws IOException {
    ReadBuffer buf = getBufferFromCompletedQueue(stream, position);

    if (buf == null) {
      return 0;
    }

    if (buf.getStatus() == ReadBufferStatus.READ_FAILED) {
      // To prevent new read requests to fail due to old read-ahead attempts,
      // return exception only from buffers that failed within last thresholdAgeMilliseconds
      if ((currentTimeMillis() - (buf.getTimeStamp()) < thresholdAgeMilliseconds)) {
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

  /*
   *
   *  ReadBufferWorker-thread-facing methods
   *
   */

  /**
   * ReadBufferWorker thread calls this to get the next buffer that it should work on.
   *
   * @return {@link ReadBuffer}
   * @throws InterruptedException if thread is interrupted
   */
  ReadBuffer getNextBlockToRead() throws InterruptedException {
    ReadBuffer buffer = null;
    synchronized (this) {
      //buffer = readAheadQueue.take();  // blocking method
      while (readAheadQueue.size() == 0) {
        wait();
      }
      buffer = readAheadQueue.remove();
      notifyAll();
      if (buffer == null) {
        return null;            // should never happen
      }
      buffer.setStatus(ReadBufferStatus.READING_IN_PROGRESS);
      inProgressList.add(buffer);
    }
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("ReadBufferWorker picked file {} for offset {}",
          buffer.getStream().getPath(), buffer.getOffset());
    }
    return buffer;
  }

  /**
   * ReadBufferWorker thread calls this method to post completion.
   *
   * @param buffer            the buffer whose read was completed
   * @param result            the {@link ReadBufferStatus} after the read operation in the worker thread
   * @param bytesActuallyRead the number of bytes that the worker thread was actually able to read
   */
  void doneReading(final ReadBuffer buffer, final ReadBufferStatus result, final int bytesActuallyRead) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("ReadBufferWorker completed file {} for offset {} bytes {}",
          buffer.getStream().getPath(),  buffer.getOffset(), bytesActuallyRead);
    }
    synchronized (this) {
      inProgressList.remove(buffer);
      if (result == ReadBufferStatus.AVAILABLE && bytesActuallyRead > 0) {
        buffer.setStatus(ReadBufferStatus.AVAILABLE);
        buffer.setLength(bytesActuallyRead);
      } else {
        freeList.push(buffer.getBufferindex());
        // buffer will be deleted as per the eviction policy.
      }

      buffer.setStatus(result);
      buffer.setTimeStamp(currentTimeMillis());
      completedReadList.add(buffer);
    }

    //outside the synchronized, since anyone receiving a wake-up from the latch must see safe-published results
    buffer.getLatch().countDown(); // wake up waiting threads (if any)
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

  @VisibleForTesting
  int getThresholdAgeMilliseconds() {
    return thresholdAgeMilliseconds;
  }

  @VisibleForTesting
  static void setThresholdAgeMilliseconds(int thresholdAgeMs) {
    thresholdAgeMilliseconds = thresholdAgeMs;
  }

  @VisibleForTesting
  int getCompletedReadListSize() {
    return completedReadList.size();
  }

  @VisibleForTesting
  void callTryEvict() {
    tryEvict();
  }

  /**
   * Test method that can clean up the current state of readAhead buffers and
   * the lists. Will also trigger a fresh init.
   */
  @VisibleForTesting
  void testResetReadBufferManager() {
    synchronized (this) {
      ArrayList<ReadBuffer> completedBuffers = new ArrayList<>();
      for (ReadBuffer buf : completedReadList) {
        if (buf != null) {
          completedBuffers.add(buf);
        }
      }

      for (ReadBuffer buf : completedBuffers) {
        evict(buf);
      }

      readAheadQueue.clear();
      inProgressList.clear();
      completedReadList.clear();
      freeList.clear();
      for (int i = 0; i < NUM_BUFFERS; i++) {
        buffers[i] = null;
      }
      buffers = null;
      resetBufferManager();
    }
  }

  /**
   * Reset buffer manager to null.
   */
  @VisibleForTesting
  static void resetBufferManager() {
    bufferManager = null;
  }

  /**
   * Reset readAhead buffer to needed readAhead block size and
   * thresholdAgeMilliseconds.
   * @param readAheadBlockSize
   * @param thresholdAgeMilliseconds
   */
  @VisibleForTesting
  void testResetReadBufferManager(int readAheadBlockSize, int thresholdAgeMilliseconds) {
    setBlockSize(readAheadBlockSize);
    setThresholdAgeMilliseconds(thresholdAgeMilliseconds);
    testResetReadBufferManager();
  }

  @VisibleForTesting
  static void setBlockSize(int readAheadBlockSize) {
    blockSize = readAheadBlockSize;
  }

  @VisibleForTesting
  int getReadAheadBlockSize() {
    return blockSize;
  }

  /**
   * Test method that can mimic no free buffers scenario and also add a ReadBuffer
   * into completedReadList. This readBuffer will get picked up by TryEvict()
   * next time a new queue request comes in.
   * @param buf that needs to be added to completedReadlist
   */
  @VisibleForTesting
  void testMimicFullUseAndAddFailedBuffer(ReadBuffer buf) {
    freeList.clear();
    completedReadList.add(buf);
  }
}
