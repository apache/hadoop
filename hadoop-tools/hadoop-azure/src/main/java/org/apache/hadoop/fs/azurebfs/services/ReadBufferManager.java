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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.classification.VisibleForTesting;

import static org.apache.hadoop.util.Preconditions.checkState;

/**
 * The Read Buffer Manager for Rest AbfsClient.
 */
final class ReadBufferManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReadBufferManager.class);
  private static final int ONE_KB = 1024;
  private static final int ONE_MB = ONE_KB * ONE_KB;

  static final int NUM_BUFFERS = 16;
  private static final int NUM_THREADS = 8;
  private static final int DEFAULT_THRESHOLD_AGE_MILLISECONDS = 3000; // have to see if 3 seconds is a good threshold

  private static int blockSize = 4 * ONE_MB;
  private static int thresholdAgeMilliseconds = DEFAULT_THRESHOLD_AGE_MILLISECONDS;
  private Thread[] threads = new Thread[NUM_THREADS];
  private byte[][] buffers;    // array of byte[] buffers, to hold the data that is read

  /**
   * map of {@link #buffers} to {@link ReadBuffer} using it.
   */
  private ReadBuffer[] bufferOwners;

  private Stack<Integer> freeList = new Stack<>();   // indices in buffers[] array that are available

  private Queue<ReadBuffer> readAheadQueue = new LinkedList<>(); // queue of requests that are not picked up by any worker thread yet
  private LinkedList<ReadBuffer> inProgressList = new LinkedList<>(); // requests being processed by worker threads
  private LinkedList<ReadBuffer> completedReadList = new LinkedList<>(); // buffers available for reading
  private static ReadBufferManager bufferManager; // singleton, initialized in static initialization block
  private static final ReentrantLock LOCK = new ReentrantLock();

  /**
   * How many completed blocks were discarded when a stream was closed?
   */
  private final AtomicLong completedBlocksDiscarded = new AtomicLong();

  /**
   * How many queued blocks were discarded when a stream was closed?
   */
  private final AtomicLong queuedBlocksDiscarded = new AtomicLong();

  /**
   * How many in progress blocks were discarded when a stream was closed?
   */
  private final AtomicLong inProgressBlocksDiscarded = new AtomicLong();

  public static ReadBufferManager getBufferManager() {
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
    bufferOwners = new ReadBuffer[NUM_BUFFERS];
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
   * @param stream The {@link AbfsInputStream} for which to do the read-ahead
   * @param requestedOffset The offset in the file which shoukd be read
   * @param requestedLength The length to read
   * @return the queued read
   */
  @VisibleForTesting
  ReadBuffer queueReadAhead(final ReadBufferStreamOperations stream, final long requestedOffset, final int requestedLength,
                      TracingContext tracingContext) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Start Queueing readAhead for {} offset {} length {}",
          stream.getPath(), requestedOffset, requestedLength);
    }
    ReadBuffer buffer;
    synchronized (this) {
      if (isAlreadyQueued(stream, requestedOffset)) {
        return null;
      }
      if (freeList.isEmpty() && !tryEvict()) {
        return null;
      }

      buffer = new ReadBuffer();
      buffer.setStream(stream);
      buffer.setOffset(requestedOffset);
      buffer.setLength(0);
      buffer.setRequestedLength(requestedLength);
      buffer.setStatus(ReadBufferStatus.NOT_AVAILABLE);
      buffer.setLatch(new CountDownLatch(1));
      buffer.setTracingContext(tracingContext);

      int bufferIndex = freeList.pop();  // will return a value, since we have checked size > 0 already
      takeOwnershipOfBufferAtIndex(buffer, bufferIndex);
      readAheadQueue.add(buffer);
      notifyAll();
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Done q-ing readAhead for file {} offset {} buffer idx {}",
            stream.getPath(), requestedOffset, buffer.getBufferindex());
      }
    }
    return buffer;
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
        LOGGER.trace("latch done for file {} buffer {}",
            stream.getPath(), readBuf);
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
    if (completedReadList.isEmpty()) {
      return false;  // there are no evict-able buffers
    }

    long currentTimeInMs = currentTimeMillis();

    // first, look for easy targets
    for (ReadBuffer buf : completedReadList) {
      if (buf.isStreamClosed() && buf.hasIndexedBuffer()) {
        // the stream was closed since this was added to the completed
        // list (it would not have a buffer if the stream was closed before)
        nodeToEvict = buf;
        break;
      }
      if (buf.isFirstByteConsumed() && buf.isLastByteConsumed()) {
        // buffers where all bytes have been consumed (approximated as first and last bytes consumed)
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
      if (buf.hasIndexedBuffer()
          && (buf.getTimeStamp() < earliestBirthday)) {
        nodeToEvict = buf;
        earliestBirthday = buf.getTimeStamp();
      } else if (!buf.hasIndexedBuffer()
          && ((currentTimeInMs - buf.getTimeStamp()) > thresholdAgeMilliseconds)
              || buf.isStreamClosed()) {
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
    buf.setTracingContext(null);
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Evicting buffer {}", buf);
    }
    completedReadList.remove(buf);
    // As failed ReadBuffers (bufferIndx = -1) are saved in completedReadList,
    // avoid adding it to freeList.
    if (buf.hasIndexedBuffer()) {
      placeBufferOnFreeList("eviction", buf);
    }
    // tell the buffer it was evicted so should update its statistics
    buf.evicted();
    return true;
  }

  private boolean isAlreadyQueued(final ReadBufferStreamOperations stream, final long requestedOffset) {
    // returns true if any part of the buffer is already queued
    return (isInList(readAheadQueue, stream, requestedOffset)
        || isInList(inProgressList, stream, requestedOffset)
        || isInList(completedReadList, stream, requestedOffset));
  }

  private boolean isInList(final Collection<ReadBuffer> list, final ReadBufferStreamOperations stream, final long requestedOffset) {
    return (getFromList(list, stream, requestedOffset) != null);
  }

  private ReadBuffer getFromList(final Collection<ReadBuffer> list, final ReadBufferStreamOperations stream, final long requestedOffset) {
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
  private ReadBuffer getBufferFromCompletedQueue(final ReadBufferStreamOperations stream, final long requestedOffset) {
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

  private void clearFromReadAheadQueue(final ReadBufferStreamOperations stream, final long requestedOffset) {
    ReadBuffer buffer = getFromList(readAheadQueue, stream, requestedOffset);
    if (buffer != null) {
      readAheadQueue.remove(buffer);
      notifyAll();   // lock is held in calling method
      placeBufferOnFreeList("clear from readahead", buffer);
    }
  }

  /**
   * Add a buffer to the free list.
   * @param reason reason for eviction
   * @param readBuffer read buffer which owns the buffer to free
   */
  private void placeBufferOnFreeList(final String reason, final ReadBuffer readBuffer) {
    int index = readBuffer.getBufferindex();
    LOGGER.debug("Returning buffer index {} to free list for '{}'; owner {}",
        index, reason, readBuffer);
    checkState(readBuffer.hasIndexedBuffer(),
        "ReadBuffer buffer release for %s has no allocated buffer to free: %s",
        reason, readBuffer);
    checkState(!freeList.contains(index),
        "Duplicate buffer %d added to free buffer list for '%s' by %s",
        index, reason, readBuffer);
    verifyReadBufferOwnsBufferAtIndex(readBuffer, index);
    // declare it as unowned
    bufferOwners[index] = null;
    // set the buffer to null, because it is now free
    // for other operations.
    readBuffer.releaseBuffer();
    // once it is not owned/referenced, make available to others
    freeList.push(index);
  }

  /**
   * Verify that at given read ReadBuffer a buffer.
   * @param readBuffer read operation taking ownership
   * @param index index of buffer in buffer array
   */
  void verifyReadBufferOwnsBufferAtIndex(final ReadBuffer readBuffer, final int index) {
    checkState(readBuffer == bufferOwner(index),
        "ReadBufferManager buffer %s is not owned by %s", index, readBuffer);
  }

  /**
   * Verify a read buffer owns its indexed buffer.
   * @param readBuffer read buffer to validate.
   * @throws IllegalStateException if ownership not satisified.
   */
  void verifyIsOwnerOfBuffer(final ReadBuffer readBuffer) {
    checkState(readBuffer.hasIndexedBuffer(), "no buffer owned by %s", readBuffer);
    verifyReadBufferOwnsBufferAtIndex(readBuffer, readBuffer.getBufferindex());
  }

  /**
   * Take ownership of a buffer.
   * This updates the {@link #bufferOwners} array.
   * @param readBuffer read operation taking ownership
   * @param index index of buffer in buffer array
   */
  private void takeOwnershipOfBufferAtIndex(final ReadBuffer readBuffer, int index) {
    checkState(null == bufferOwners[index],
        "Buffer %d requested by %s already owned by %s",
        index, readBuffer, bufferOwners[index]);
    readBuffer.setBuffer(buffers[index]);
    readBuffer.setBufferindex(index);
    bufferOwners[index] = readBuffer;
  }

  /**
   * Verify a buffer is not in use anywhere.
   * @param index buffer index.
   * @throws IllegalStateException if the state is invalid.
   */
  private void verifyByteBufferNotInUse(final int index) {
    verifyByteBufferNotInCollection("completedReadList", index, completedReadList);
    verifyByteBufferNotInCollection("inProgressList", index, inProgressList);
    verifyByteBufferNotInCollection("readAheadQueue", index, readAheadQueue);
  }

  /**
   * Verify that a buffer is not referenced in the supplied collection.
   * @param name collection name for exceptions.
   * @param index buffer index.
   * @param collection collection to validate
   * @throws IllegalStateException if the state is invalid.
   */
  private void verifyByteBufferNotInCollection(String name, int index,
      Collection<ReadBuffer> collection) {
    checkState(collection.stream().noneMatch(rb -> rb.getBufferindex() == index),
        "Buffer index %d found in buffer collection %s", index, name);
  }

  /**
   * Verify that a read buffer is not referenced in the supplied collection.
   * @param name collection name for exceptions.
   * @param rb read buffer
   * @param collection collection to validate
   * @throws IllegalStateException if the state is invalid.
   */
  private void verifyReadBufferNotInCollection(String name,
      ReadBuffer rb,
      Collection<ReadBuffer> collection) {
    checkState(!collection.contains(rb),
        "Collection %s contains buffer %s", name, rb);
  }

  /**
   * Validate all invariants of the read manager state.
   * @throws IllegalStateException if the state is invalid.
   */
  @VisibleForTesting
  public synchronized void validateReadManagerState() {
    // all buffers in free list are not in any of the other lists
    freeList.forEach(this::verifyByteBufferNotInUse);

    // there is no in progress buffer in the other queues
    inProgressList.forEach(rb -> {
      verifyReadBufferNotInCollection("completedReadList", rb,
          completedReadList);
      verifyReadBufferNotInCollection("readAheadQueue", rb, readAheadQueue);
    });
    // nothing completed is in the readahead queue
    completedReadList.forEach(rb -> {
      verifyReadBufferNotInCollection("readAheadQueue", rb, readAheadQueue);
    });

  }

  private int getBlockFromCompletedQueue(final ReadBufferStreamOperations stream, final long position, final int length,
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
    buf.dataConsumedByStream(cursor, lengthToCopy);
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
      // verify buffer index is owned.
      verifyIsOwnerOfBuffer(buffer);

      buffer.setStatus(ReadBufferStatus.READING_IN_PROGRESS);
      inProgressList.add(buffer);
    }
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("ReadBufferWorker picked file {} for offset {}",
          buffer.getStream().getPath(), buffer.getOffset());
    }
    validateReadManagerState();
    // update stream gauge.
    buffer.prefetchStarted();
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
      LOGGER.trace("ReadBufferWorker completed file {} for offset {} bytes {}; {}",
          buffer.getStream().getPath(),  buffer.getOffset(), bytesActuallyRead, buffer);
    }
    // decrement counter.
    buffer.prefetchFinished();

    try {
      synchronized (this) {
        // remove from the list
        if (!inProgressList.remove(buffer)) {
          // this is a sign of inconsistent state, so a major problem, such as
          // double invocation of this method with the same buffer.
          String message =
              String.format("Read completed from an operation not declared as in progress %s",
                  buffer);
          LOGGER.warn(message);
          if (buffer.hasIndexedBuffer()) {
            // release the buffer (which may raise an exception)
            placeBufferOnFreeList("read not in progress", buffer);
          }
          // report the failure
          throw new IllegalStateException(message);
        }

        // does the buffer own the array it has just written to?
        // this is a significant issue.
        verifyIsOwnerOfBuffer(buffer);
        // should the read buffer be added to the completed list?
        boolean addCompleted;
        // flag to indicate buffer should be freed
        boolean shouldFreeBuffer = false;
        // and the reason (for logging)
        String freeBufferReason = "";

        buffer.setStatus(result);
        buffer.setTimeStamp(currentTimeMillis());
        // did the read return any data?
        if (result == ReadBufferStatus.AVAILABLE) {
          if (bytesActuallyRead > 0) {

            // successful read of data;
            addCompleted = true;

            // update buffer state.
            buffer.setLength(bytesActuallyRead);
          } else {
            // there was no data; the buffer can be returned to the free list.
            shouldFreeBuffer = true;
            freeBufferReason = "no data";
            // don't
            addCompleted = false;
          }
        } else {
          // read failed or there was no data; the buffer can be returned to the free list.
          shouldFreeBuffer = true;
          freeBufferReason = "failed read";
          // completed list also contains FAILED read buffers
          // for sending exception message to clients.
          // NOTE: checks for closed state may update this.
          addCompleted = true;
        }

        // now check for closed streams, which are discarded
        // and the buffers freed immediately, irrespective of
        // outcome
        if (buffer.isStreamClosed()) {
          // stream was closed during the read.
          // even if there is data, it should be discarded
          LOGGER.trace("Discarding prefetch on closed stream {}", buffer);
          inProgressBlocksDiscarded.incrementAndGet();
          // don't add
          addCompleted = false;
          // and the buffer recycled
          shouldFreeBuffer = true;
          freeBufferReason = "stream closed";
        }
        if (shouldFreeBuffer) {
          buffer.discarded();
          // buffer should be returned to the free list.
          placeBufferOnFreeList(freeBufferReason, buffer);
        }
        if (addCompleted) {
          // add to the completed list.
          LOGGER.trace("Adding buffer to completed list {}", buffer);
          completedReadList.add(buffer);
        }
      }
    } catch (IllegalStateException e) {
      // update this for tests to validate
      buffer.setStatus(ReadBufferStatus.READ_FAILED);
      buffer.setErrException(new IOException(e));
      throw e;
    } finally {
      //outside the synchronized, since anyone receiving a wake-up from the latch must see safe-published results
      LOGGER.trace("releasing latch {}", buffer.getLatch());
      buffer.getLatch().countDown(); // wake up waiting threads (if any)
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
  public synchronized List<ReadBuffer> getCompletedReadListCopy() {
    return new ArrayList<>(completedReadList);
  }

  @VisibleForTesting
  public synchronized List<Integer> getFreeListCopy() {
    return new ArrayList<>(freeList);
  }

  @VisibleForTesting
  public synchronized List<ReadBuffer> getReadAheadQueueCopy() {
    return new ArrayList<>(readAheadQueue);
  }

  @VisibleForTesting
  public synchronized List<ReadBuffer> getInProgressCopiedList() {
    return new ArrayList<>(inProgressList);
  }

  @VisibleForTesting
  boolean callTryEvict() {
    return tryEvict();
  }

  /**
   * Purging the buffers associated with an {@link AbfsInputStream}
   * from {@link ReadBufferManager} when stream is closed.
   * Before HADOOP-18521 this would purge in progress reads, which
   * would return the active buffer to the free pool while it was
   * still in use.
   * @param stream input stream.
   */
  public synchronized void purgeBuffersForStream(ReadBufferStreamOperations stream) {
    LOGGER.debug("Purging stale buffers for AbfsInputStream {}/{}",
        stream.getStreamID(), stream.getPath());

    // remove from the queue
    int before = readAheadQueue.size();
    readAheadQueue.removeIf(readBuffer -> readBuffer.getStream() == stream);
    int readaheadPurged = readAheadQueue.size() - before;
    queuedBlocksDiscarded.addAndGet(readaheadPurged);

    // all completed entries
    int completedPurged = purgeCompletedReads(stream);
    completedBlocksDiscarded.addAndGet(completedPurged);

    // print a summary
    LOGGER.debug("Purging outcome readahead={}, completed={} for {}",
        readaheadPurged, completedPurged, stream);
    validateReadManagerState();
  }

  /**
   * Method to remove buffers associated with a {@link AbfsInputStream}
   * from the completed read list.
   * Any allocated buffers will be returned to the pool.
   * The buffers will have {@link ReadBuffer#evicted()} called to let
   * them update their statistics.
   * NOTE: This method is not threadsafe and must be called inside a
   * synchronised block. See caller.
   * @param stream associated input stream.
   */
  private int purgeCompletedReads(ReadBufferStreamOperations stream) {
    int purged = 0;
    for (Iterator<ReadBuffer> it = completedReadList.iterator(); it.hasNext();) {
      ReadBuffer readBuffer = it.next();
      if (readBuffer.getStream() == stream) {
        purged++;
        it.remove();
        // As failed ReadBuffers (bufferIndex = -1) are already pushed to free
        // list in doneReading method, we will skip adding those here again.
        if (readBuffer.hasIndexedBuffer()) {
          placeBufferOnFreeList("purge completed reads", readBuffer);
        }
        // tell the buffer it was evicted so should update its statistics
        readBuffer.evicted();
      }
    }
    return purged;
  }

  /**
   * Get the count of completed blocks discarded.
   * @return the number of completed blocks discarded.
   */
  @VisibleForTesting
  public long getCompletedBlocksDiscarded() {
    return completedBlocksDiscarded.get();
  }

  /**
   * Get the count of completed blocks discarded.
   * @return the number of queued block reads discarded.
   */
  @VisibleForTesting
  public long getQueuedBlocksDiscarded() {
    return queuedBlocksDiscarded.get();
  }

  /**
   * Get the count of in progress read blocks discarded.
   * @return the number of blocks being read discarded.
   */
  @VisibleForTesting
  public long getInProgressBlocksDiscarded() {
    return inProgressBlocksDiscarded.get();
  }

  @VisibleForTesting
  public void resetBlocksDiscardedCounters() {
    completedBlocksDiscarded.set(0);
    queuedBlocksDiscarded.set(0);
    inProgressBlocksDiscarded.set(0);
  }

  /**
   * Look up the owner of a buffer.
   * @param index index of the buffer.
   * @return the buffer owner, null if the buffer is free.
   */
  ReadBuffer bufferOwner(int index) {
    return bufferOwners[index];
  }

  @Override
  public String toString() {
    return "ReadBufferManager{" +
        "readAheadQueue=" + readAheadQueue.size()
        + ", inProgressList=" + inProgressList.size()
        + ", completedReadList=" + completedReadList.size()
        + ", completedBlocksDiscarded=" + completedBlocksDiscarded
        + ", queuedBlocksDiscarded=" + queuedBlocksDiscarded
        + ", inProgressBlocksDiscarded=" + inProgressBlocksDiscarded
        + '}';
  }

  /**
   * Test method that can clean up the current state of readAhead buffers and
   * the lists. Will also trigger a fresh init.
   * Note: will cause problems with the static shared class if multiple tests
   * are running in the same VM.
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
        bufferOwners[i] = null;
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

  @VisibleForTesting
  int getNumBuffers() {
    return NUM_BUFFERS;
  }
}
