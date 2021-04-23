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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.CachedSASToken;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;

import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_WRITE_WITHOUT_LEASE;
import static org.apache.hadoop.fs.impl.StoreImplementationUtils.isProbeForSyncable;
import static org.apache.hadoop.io.IOUtils.wrapException;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters.Mode.APPEND_MODE;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters.Mode.FLUSH_CLOSE_MODE;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters.Mode.FLUSH_MODE;

/**
 * The BlobFsOutputStream for Rest AbfsClient.
 */
public class AbfsOutputStream extends OutputStream implements Syncable,
    StreamCapabilities, IOStatisticsSource {

  private final AbfsClient client;
  private final String path;
  private long position;
  private boolean closed;
  private boolean supportFlush;
  private boolean disableOutputStreamFlush;
  private boolean enableSmallWriteOptimization;
  private boolean isAppendBlob;
  private volatile IOException lastError;

  private long lastFlushOffset;
  private long lastTotalAppendOffset = 0;

  private final int bufferSize;
  private byte[] buffer;
  private int bufferIndex;
  private int numOfAppendsToServerSinceLastFlush;
  private final int maxConcurrentRequestCount;
  private final int maxRequestsThatCanBeQueued;

  private ConcurrentLinkedDeque<WriteOperation> writeOperations;
  private final ThreadPoolExecutor threadExecutor;
  private final ExecutorCompletionService<Void> completionService;

  // SAS tokens can be re-used until they expire
  private CachedSASToken cachedSasToken;

  private AbfsLease lease;
  private String leaseId;

  /**
   * Queue storing buffers with the size of the Azure block ready for
   * reuse. The pool allows reusing the blocks instead of allocating new
   * blocks. After the data is sent to the service, the buffer is returned
   * back to the queue
   */
  private ElasticByteBufferPool byteBufferPool
          = new ElasticByteBufferPool();

  private final Statistics statistics;
  private final AbfsOutputStreamStatistics outputStreamStatistics;
  private IOStatistics ioStatistics;

  private static final Logger LOG =
      LoggerFactory.getLogger(AbfsOutputStream.class);

  public AbfsOutputStream(
          final AbfsClient client,
          final Statistics statistics,
          final String path,
          final long position,
          AbfsOutputStreamContext abfsOutputStreamContext) {
    this.client = client;
    this.statistics = statistics;
    this.path = path;
    this.position = position;
    this.closed = false;
    this.supportFlush = abfsOutputStreamContext.isEnableFlush();
    this.disableOutputStreamFlush = abfsOutputStreamContext
            .isDisableOutputStreamFlush();
    this.enableSmallWriteOptimization
        = abfsOutputStreamContext.isEnableSmallWriteOptimization();
    this.isAppendBlob = abfsOutputStreamContext.isAppendBlob();
    this.lastError = null;
    this.lastFlushOffset = 0;
    this.bufferSize = abfsOutputStreamContext.getWriteBufferSize();
    this.buffer = byteBufferPool.getBuffer(false, bufferSize).array();
    this.bufferIndex = 0;
    this.numOfAppendsToServerSinceLastFlush = 0;
    this.writeOperations = new ConcurrentLinkedDeque<>();
    this.outputStreamStatistics = abfsOutputStreamContext.getStreamStatistics();

    if (this.isAppendBlob) {
      this.maxConcurrentRequestCount = 1;
    } else {
      this.maxConcurrentRequestCount = abfsOutputStreamContext
          .getWriteMaxConcurrentRequestCount();
    }
    this.maxRequestsThatCanBeQueued = abfsOutputStreamContext
        .getMaxWriteRequestsToQueue();

    this.lease = abfsOutputStreamContext.getLease();
    this.leaseId = abfsOutputStreamContext.getLeaseId();

    this.threadExecutor
        = new ThreadPoolExecutor(maxConcurrentRequestCount,
        maxConcurrentRequestCount,
        10L,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>());
    this.completionService = new ExecutorCompletionService<>(this.threadExecutor);
    this.cachedSasToken = new CachedSASToken(
        abfsOutputStreamContext.getSasTokenRenewPeriodForStreamsInSeconds());
    if (outputStreamStatistics != null) {
      this.ioStatistics = outputStreamStatistics.getIOStatistics();
    }
  }

  /**
   * Query the stream for a specific capability.
   *
   * @param capability string to query the stream support for.
   * @return true for hsync and hflush.
   */
  @Override
  public boolean hasCapability(String capability) {
    return supportFlush && isProbeForSyncable(capability);
  }

  /**
   * Writes the specified byte to this output stream. The general contract for
   * write is that one byte is written to the output stream. The byte to be
   * written is the eight low-order bits of the argument b. The 24 high-order
   * bits of b are ignored.
   *
   * @param byteVal the byteValue to write.
   * @throws IOException if an I/O error occurs. In particular, an IOException may be
   *                     thrown if the output stream has been closed.
   */
  @Override
  public void write(final int byteVal) throws IOException {
    write(new byte[]{(byte) (byteVal & 0xFF)});
  }

  /**
   * Writes length bytes from the specified byte array starting at off to
   * this output stream.
   *
   * @param data   the byte array to write.
   * @param off the start off in the data.
   * @param length the number of bytes to write.
   * @throws IOException if an I/O error occurs. In particular, an IOException may be
   *                     thrown if the output stream has been closed.
   */
  @Override
  public synchronized void write(final byte[] data, final int off, final int length)
      throws IOException {
    maybeThrowLastError();

    Preconditions.checkArgument(data != null, "null data");

    if (off < 0 || length < 0 || length > data.length - off) {
      throw new IndexOutOfBoundsException();
    }

    if (hasLease() && isLeaseFreed()) {
      throw new PathIOException(path, ERR_WRITE_WITHOUT_LEASE);
    }

    int currentOffset = off;
    int writableBytes = bufferSize - bufferIndex;
    int numberOfBytesToWrite = length;

    while (numberOfBytesToWrite > 0) {
      if (writableBytes <= numberOfBytesToWrite) {
        System.arraycopy(data, currentOffset, buffer, bufferIndex, writableBytes);
        bufferIndex += writableBytes;
        writeCurrentBufferToService();
        currentOffset += writableBytes;
        numberOfBytesToWrite = numberOfBytesToWrite - writableBytes;
      } else {
        System.arraycopy(data, currentOffset, buffer, bufferIndex, numberOfBytesToWrite);
        bufferIndex += numberOfBytesToWrite;
        numberOfBytesToWrite = 0;
      }

      writableBytes = bufferSize - bufferIndex;
    }
    incrementWriteOps();
  }

  /**
   * Increment Write Operations.
   */
  private void incrementWriteOps() {
    if (statistics != null) {
      statistics.incrementWriteOps(1);
    }
  }

  /**
   * Throw the last error recorded if not null.
   * After the stream is closed, this is always set to
   * an exception, so acts as a guard against method invocation once
   * closed.
   * @throws IOException if lastError is set
   */
  private void maybeThrowLastError() throws IOException {
    if (lastError != null) {
      throw lastError;
    }
  }

  /**
   * Flushes this output stream and forces any buffered output bytes to be
   * written out. If any data remains in the payload it is committed to the
   * service. Data is queued for writing and forced out to the service
   * before the call returns.
   */
  @Override
  public void flush() throws IOException {
    if (!disableOutputStreamFlush) {
      flushInternalAsync();
    }
  }

  /** Similar to posix fsync, flush out the data in client's user buffer
   * all the way to the disk device (but the disk may have it in its cache).
   * @throws IOException if error occurs
   */
  @Override
  public void hsync() throws IOException {
    if (supportFlush) {
      flushInternal(false);
    }
  }

  /** Flush out the data in client's user buffer. After the return of
   * this call, new readers will see the data.
   * @throws IOException if any error occurs
   */
  @Override
  public void hflush() throws IOException {
    if (supportFlush) {
      flushInternal(false);
    }
  }

  /**
   * Force all data in the output stream to be written to Azure storage.
   * Wait to return until this is complete. Close the access to the stream and
   * shutdown the upload thread pool.
   * If the blob was created, its lease will be released.
   * Any error encountered caught in threads and stored will be rethrown here
   * after cleanup.
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    try {
      flushInternal(true);
      threadExecutor.shutdown();
    } catch (IOException e) {
      // Problems surface in try-with-resources clauses if
      // the exception thrown in a close == the one already thrown
      // -so we wrap any exception with a new one.
      // See HADOOP-16785
      throw wrapException(path, e.getMessage(), e);
    } finally {
      if (hasLease()) {
        lease.free();
        lease = null;
      }
      lastError = new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
      buffer = null;
      bufferIndex = 0;
      closed = true;
      writeOperations.clear();
      byteBufferPool = null;
      if (!threadExecutor.isShutdown()) {
        threadExecutor.shutdownNow();
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Closing AbfsOutputStream ", toString());
    }
  }

  private synchronized void flushInternal(boolean isClose) throws IOException {
    maybeThrowLastError();

    // if its a flush post write < buffersize, send flush parameter in append
    if (!isAppendBlob
        && enableSmallWriteOptimization
        && (numOfAppendsToServerSinceLastFlush == 0) // there are no ongoing store writes
        && (writeOperations.size() == 0) // double checking no appends in progress
        && (bufferIndex > 0)) { // there is some data that is pending to be written
      smallWriteOptimizedflushInternal(isClose);
      return;
    }

    writeCurrentBufferToService();
    flushWrittenBytesToService(isClose);
    numOfAppendsToServerSinceLastFlush = 0;
  }

  private synchronized void smallWriteOptimizedflushInternal(boolean isClose) throws IOException {
    // writeCurrentBufferToService will increment numOfAppendsToServerSinceLastFlush
    writeCurrentBufferToService(true, isClose);
    waitForAppendsToComplete();
    shrinkWriteOperationQueue();
    maybeThrowLastError();
    numOfAppendsToServerSinceLastFlush = 0;
  }

  private synchronized void flushInternalAsync() throws IOException {
    maybeThrowLastError();
    writeCurrentBufferToService();
    flushWrittenBytesToServiceAsync();
  }

  private void writeAppendBlobCurrentBufferToService() throws IOException {
    if (bufferIndex == 0) {
      return;
    }
    final byte[] bytes = buffer;
    final int bytesLength = bufferIndex;
    if (outputStreamStatistics != null) {
      outputStreamStatistics.writeCurrentBuffer();
      outputStreamStatistics.bytesToUpload(bytesLength);
    }
    buffer = byteBufferPool.getBuffer(false, bufferSize).array();
    bufferIndex = 0;
    final long offset = position;
    position += bytesLength;
    AbfsPerfTracker tracker = client.getAbfsPerfTracker();
    try (AbfsPerfInfo perfInfo = new AbfsPerfInfo(tracker,
            "writeCurrentBufferToService", "append")) {
      AppendRequestParameters reqParams = new AppendRequestParameters(offset, 0,
          bytesLength, APPEND_MODE, true, leaseId);
      AbfsRestOperation op = client.append(path, bytes, reqParams, cachedSasToken.get());
      cachedSasToken.update(op.getSasToken());
      if (outputStreamStatistics != null) {
        outputStreamStatistics.uploadSuccessful(bytesLength);
      }
      perfInfo.registerResult(op.getResult());
      byteBufferPool.putBuffer(ByteBuffer.wrap(bytes));
      perfInfo.registerSuccess(true);
      return;
    } catch (Exception ex) {
      if (ex instanceof AbfsRestOperationException) {
        if (((AbfsRestOperationException) ex).getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
          throw new FileNotFoundException(ex.getMessage());
        }
      }
      if (ex instanceof AzureBlobFileSystemException) {
        ex = (AzureBlobFileSystemException) ex;
      }
      lastError = new IOException(ex);
      throw lastError;
    }
  }

  private synchronized void writeCurrentBufferToService() throws IOException {
    writeCurrentBufferToService(false, false);
  }

  private synchronized void writeCurrentBufferToService(boolean isFlush, boolean isClose) throws IOException {
    if (this.isAppendBlob) {
      writeAppendBlobCurrentBufferToService();
      return;
    }

    if (bufferIndex == 0) {
      return;
    }
    numOfAppendsToServerSinceLastFlush++;

    final byte[] bytes = buffer;
    final int bytesLength = bufferIndex;
    if (outputStreamStatistics != null) {
      outputStreamStatistics.writeCurrentBuffer();
      outputStreamStatistics.bytesToUpload(bytesLength);
    }
    buffer = byteBufferPool.getBuffer(false, bufferSize).array();
    bufferIndex = 0;
    final long offset = position;
    position += bytesLength;

    if (threadExecutor.getQueue().size() >= maxRequestsThatCanBeQueued) {
      //Tracking time spent on waiting for task to complete.
      if (outputStreamStatistics != null) {
        try (DurationTracker ignored = outputStreamStatistics.timeSpentTaskWait()) {
          waitForTaskToComplete();
        }
      } else {
        waitForTaskToComplete();
      }
    }
    final Future<Void> job =
        completionService.submit(() -> {
          AbfsPerfTracker tracker =
              client.getAbfsPerfTracker();
          try (AbfsPerfInfo perfInfo = new AbfsPerfInfo(tracker,
              "writeCurrentBufferToService", "append")) {
            AppendRequestParameters.Mode
                mode = APPEND_MODE;
            if (isFlush & isClose) {
              mode = FLUSH_CLOSE_MODE;
            } else if (isFlush) {
              mode = FLUSH_MODE;
            }
            AppendRequestParameters reqParams = new AppendRequestParameters(
                offset, 0, bytesLength, mode, false, leaseId);
            AbfsRestOperation op = client.append(path, bytes, reqParams,
                cachedSasToken.get());
            cachedSasToken.update(op.getSasToken());
            perfInfo.registerResult(op.getResult());
            byteBufferPool.putBuffer(ByteBuffer.wrap(bytes));
            perfInfo.registerSuccess(true);
            return null;
          }
        });

    if (outputStreamStatistics != null) {
      if (job.isCancelled()) {
        outputStreamStatistics.uploadFailed(bytesLength);
      } else {
        outputStreamStatistics.uploadSuccessful(bytesLength);
      }
    }
    writeOperations.add(new WriteOperation(job, offset, bytesLength));

    // Try to shrink the queue
    shrinkWriteOperationQueue();
  }

  private synchronized void waitForAppendsToComplete() throws IOException {
    for (WriteOperation writeOperation : writeOperations) {
      try {
        writeOperation.task.get();
      } catch (Exception ex) {
        if (ex.getCause() instanceof AbfsRestOperationException) {
          if (((AbfsRestOperationException) ex.getCause()).getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
            throw new FileNotFoundException(ex.getMessage());
          }
        }

        if (ex.getCause() instanceof AzureBlobFileSystemException) {
          ex = (AzureBlobFileSystemException) ex.getCause();
        }
        lastError = new IOException(ex);
        throw lastError;
      }
    }
  }

  private synchronized void flushWrittenBytesToService(boolean isClose) throws IOException {
    waitForAppendsToComplete();
    flushWrittenBytesToServiceInternal(position, false, isClose);
  }

  private synchronized void flushWrittenBytesToServiceAsync() throws IOException {
    shrinkWriteOperationQueue();

    if (this.lastTotalAppendOffset > this.lastFlushOffset) {
      this.flushWrittenBytesToServiceInternal(this.lastTotalAppendOffset, true,
        false/*Async flush on close not permitted*/);
    }
  }

  private synchronized void flushWrittenBytesToServiceInternal(final long offset,
      final boolean retainUncommitedData, final boolean isClose) throws IOException {
    // flush is called for appendblob only on close
    if (this.isAppendBlob && !isClose) {
      return;
    }

    AbfsPerfTracker tracker = client.getAbfsPerfTracker();
    try (AbfsPerfInfo perfInfo = new AbfsPerfInfo(tracker,
            "flushWrittenBytesToServiceInternal", "flush")) {
      AbfsRestOperation op = client.flush(path, offset, retainUncommitedData, isClose,
          cachedSasToken.get(), leaseId);
      cachedSasToken.update(op.getSasToken());
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    } catch (AzureBlobFileSystemException ex) {
      if (ex instanceof AbfsRestOperationException) {
        if (((AbfsRestOperationException) ex).getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
          throw new FileNotFoundException(ex.getMessage());
        }
      }
      throw new IOException(ex);
    }
    this.lastFlushOffset = offset;
  }

  /**
   * Try to remove the completed write operations from the beginning of write
   * operation FIFO queue.
   */
  private synchronized void shrinkWriteOperationQueue() throws IOException {
    try {
      while (writeOperations.peek() != null && writeOperations.peek().task.isDone()) {
        writeOperations.peek().task.get();
        lastTotalAppendOffset += writeOperations.peek().length;
        writeOperations.remove();
        // Incrementing statistics to indicate queue has been shrunk.
        if (outputStreamStatistics != null) {
          outputStreamStatistics.queueShrunk();
        }
      }
    } catch (Exception e) {
      if (e.getCause() instanceof AzureBlobFileSystemException) {
        lastError = (AzureBlobFileSystemException) e.getCause();
      } else {
        lastError = new IOException(e);
      }
      throw lastError;
    }
  }

  private void waitForTaskToComplete() throws IOException {
    boolean completed;
    for (completed = false; completionService.poll() != null; completed = true) {
      // keep polling until there is no data
    }
    // for AppendBLob, jobs are not submitted to completion service
    if (isAppendBlob) {
      completed = true;
    }

    if (!completed) {
      try {
        completionService.take();
      } catch (InterruptedException e) {
        lastError = (IOException) new InterruptedIOException(e.toString()).initCause(e);
        throw lastError;
      }
    }
  }

  private static class WriteOperation {
    private final Future<Void> task;
    private final long startOffset;
    private final long length;

    WriteOperation(final Future<Void> task, final long startOffset, final long length) {
      Preconditions.checkNotNull(task, "task");
      Preconditions.checkArgument(startOffset >= 0, "startOffset");
      Preconditions.checkArgument(length >= 0, "length");

      this.task = task;
      this.startOffset = startOffset;
      this.length = length;
    }
  }

  @VisibleForTesting
  public synchronized void waitForPendingUploads() throws IOException {
    waitForTaskToComplete();
  }

  /**
   * Getter method for AbfsOutputStream statistics.
   *
   * @return statistics for AbfsOutputStream.
   */
  @VisibleForTesting
  public AbfsOutputStreamStatistics getOutputStreamStatistics() {
    return outputStreamStatistics;
  }

  /**
   * Getter to get the size of the task queue.
   *
   * @return the number of writeOperations in AbfsOutputStream.
   */
  @VisibleForTesting
  public int getWriteOperationsSize() {
    return writeOperations.size();
  }

  @VisibleForTesting
  int getMaxConcurrentRequestCount() {
    return this.maxConcurrentRequestCount;
  }

  @VisibleForTesting
  int getMaxRequestsThatCanBeQueued() {
    return maxRequestsThatCanBeQueued;
  }

  @VisibleForTesting
  Boolean isAppendBlobStream() {
    return isAppendBlob;
  }

  @Override
  public IOStatistics getIOStatistics() {
    return ioStatistics;
  }

  @VisibleForTesting
  public boolean isLeaseFreed() {
    if (lease == null) {
      return true;
    }
    return lease.isFreed();
  }

  @VisibleForTesting
  public boolean hasLease() {
    return lease != null;
  }

  /**
   * Appending AbfsOutputStream statistics to base toString().
   *
   * @return String with AbfsOutputStream statistics.
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(super.toString());
    if (outputStreamStatistics != null) {
      sb.append("AbfsOutputStream@").append(this.hashCode());
      sb.append("){");
      sb.append(outputStreamStatistics.toString());
      sb.append("}");
    }
    return sb.toString();
  }
}
