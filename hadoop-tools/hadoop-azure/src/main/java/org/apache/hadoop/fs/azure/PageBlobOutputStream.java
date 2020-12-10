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

package org.apache.hadoop.fs.azure;

import static org.apache.hadoop.fs.azure.PageBlobFormatHelpers.PAGE_DATA_SIZE;
import static org.apache.hadoop.fs.azure.PageBlobFormatHelpers.PAGE_HEADER_SIZE;
import static org.apache.hadoop.fs.azure.PageBlobFormatHelpers.PAGE_SIZE;
import static org.apache.hadoop.fs.azure.PageBlobFormatHelpers.fromShort;
import static org.apache.hadoop.fs.azure.PageBlobFormatHelpers.withMD5Checking;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.azure.StorageInterface.CloudPageBlobWrapper;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudPageBlob;


/**
 * An output stream that write file data to a page blob stored using ASV's
 * custom format.
 */
final class PageBlobOutputStream extends OutputStream implements Syncable, StreamCapabilities {
  /**
   * The maximum number of raw bytes Azure Storage allows us to upload in a
   * single request (4 MB).
   */
  private static final int MAX_RAW_BYTES_PER_REQUEST = 4 * 1024 * 1024;
  /**
   * The maximum number of pages Azure Storage allows us to upload in a
   * single request.
   */
  private static final int MAX_PAGES_IN_REQUEST =
      MAX_RAW_BYTES_PER_REQUEST / PAGE_SIZE;
  /**
   * The maximum number of data bytes (header not included) we can upload
   * in a single request. I'm limiting it to (N - 1) pages to account for
   * the possibility that we may have to rewrite the previous request's
   * last page.
   */
  private static final int MAX_DATA_BYTES_PER_REQUEST =
      PAGE_DATA_SIZE * (MAX_PAGES_IN_REQUEST - 1);

  private final CloudPageBlobWrapper blob;
  private final OperationContext opContext;

  /**
   * If the IO thread encounters an error, it'll store it here.
   */
  private volatile IOException lastError;

  /**
   * Current size of the page blob in bytes. It may be extended if the file
   * gets full.
   */
  private long currentBlobSize;
  /**
   * The current byte offset we're at in the blob (how many bytes we've
   * uploaded to the server).
   */
  private long currentBlobOffset;
  /**
   * The data in the last page that we wrote to the server, in case we have to
   * overwrite it in the new request.
   */
  private byte[] previousLastPageDataWritten = new byte[0];
  /**
   * The current buffer we're writing to before sending to the server.
   */
  private ByteArrayOutputStream outBuffer;
  /**
   * The task queue for writing to the server.
   */
  private final LinkedBlockingQueue<Runnable> ioQueue;
  /**
   * The thread pool we're using for writing to the server. Note that the IO
   * write is NOT designed for parallelism, so there can only be one thread
   * in that pool (I'm using the thread pool mainly for the lifetime management
   * capabilities, otherwise I'd have just used a simple Thread).
   */
  private final ThreadPoolExecutor ioThreadPool;

  // The last task given to the ioThreadPool to execute, to allow
  // waiting until it's done.
  private WriteRequest lastQueuedTask;
  // Whether the stream has been closed.
  private boolean closed = false;

  public static final Log LOG = LogFactory.getLog(AzureNativeFileSystemStore.class);

  // Set the minimum page blob file size to 128MB, which is >> the default
  // block size of 32MB. This default block size is often used as the
  // hbase.regionserver.hlog.blocksize.
  // The goal is to have a safe minimum size for HBase log files to allow them
  // to be filled and rolled without exceeding the minimum size. A larger size
  // can be used by setting the fs.azure.page.blob.size configuration variable.
  public static final long PAGE_BLOB_MIN_SIZE = 128L * 1024L * 1024L;

  // The default and minimum amount to extend a page blob by if it starts
  // to get full.
  public static final long
    PAGE_BLOB_DEFAULT_EXTENSION_SIZE = 128L * 1024L * 1024L;

  // The configured page blob extension size (either the default, or if greater,
  // the value configured in fs.azure.page.blob.extension.size
  private long configuredPageBlobExtensionSize;

  /**
   * Constructs an output stream over the given page blob.
   *
   * @param blob the blob that this stream is associated with.
   * @param opContext an object used to track the execution of the operation
   * @throws StorageException if anything goes wrong creating the blob.
   */
  public PageBlobOutputStream(final CloudPageBlobWrapper blob,
      final OperationContext opContext,
      final Configuration conf) throws StorageException {
    this.blob = blob;
    this.outBuffer = new ByteArrayOutputStream();
    this.opContext = opContext;
    this.lastQueuedTask = null;
    this.ioQueue = new LinkedBlockingQueue<Runnable>();

    // As explained above: the IO writes are not designed for parallelism,
    // so we only have one thread in this thread pool.
    this.ioThreadPool = new ThreadPoolExecutor(1, 1, 2, TimeUnit.SECONDS,
        ioQueue);



    // Make page blob files have a size that is the greater of a
    // minimum size, or the value of fs.azure.page.blob.size from configuration.
    long pageBlobConfigSize = conf.getLong("fs.azure.page.blob.size", 0);
    LOG.debug("Read value of fs.azure.page.blob.size as " + pageBlobConfigSize
        + " from configuration (0 if not present).");
    long pageBlobSize = Math.max(PAGE_BLOB_MIN_SIZE, pageBlobConfigSize);

    // Ensure that the pageBlobSize is a multiple of page size.
    if (pageBlobSize % PAGE_SIZE != 0) {
      pageBlobSize += PAGE_SIZE - pageBlobSize % PAGE_SIZE;
    }
    blob.create(pageBlobSize, new BlobRequestOptions(), opContext);
    currentBlobSize = pageBlobSize;

    // Set the page blob extension size. It must be a minimum of the default
    // value.
    configuredPageBlobExtensionSize =
        conf.getLong("fs.azure.page.blob.extension.size", 0);
    if (configuredPageBlobExtensionSize < PAGE_BLOB_DEFAULT_EXTENSION_SIZE) {
      configuredPageBlobExtensionSize = PAGE_BLOB_DEFAULT_EXTENSION_SIZE;
    }

    // make sure it is a multiple of the page size
    if (configuredPageBlobExtensionSize % PAGE_SIZE != 0) {
      configuredPageBlobExtensionSize +=
          PAGE_SIZE - configuredPageBlobExtensionSize % PAGE_SIZE;
    }
  }

  private void checkStreamState() throws IOException {
    if (lastError != null) {
      throw lastError;
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
    switch (capability.toLowerCase(Locale.ENGLISH)) {
      case StreamCapabilities.HSYNC:
      case StreamCapabilities.HFLUSH:
        return true;
      default:
        return false;
    }
  }

  /**
   * Closes this output stream and releases any system resources associated with
   * this stream. If any data remains in the buffer it is committed to the
   * service.
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    LOG.debug("Closing page blob output stream.");
    flush();
    checkStreamState();
    ioThreadPool.shutdown();
    try {
      LOG.debug(ioThreadPool.toString());
      if (!ioThreadPool.awaitTermination(10, TimeUnit.MINUTES)) {
        LOG.debug("Timed out after 10 minutes waiting for IO requests to finish");
        NativeAzureFileSystemHelper.logAllLiveStackTraces();
        LOG.debug(ioThreadPool.toString());
        throw new IOException("Timed out waiting for IO requests to finish");
      }
    } catch (InterruptedException e) {
      LOG.debug("Caught InterruptedException");

      // Restore the interrupted status
      Thread.currentThread().interrupt();
    }

    closed = true;
  }



  /**
   * A single write request for data to write to Azure storage.
   */
  private class WriteRequest implements Runnable {
    private final byte[] dataPayload;
    private final CountDownLatch doneSignal = new CountDownLatch(1);

    public WriteRequest(byte[] dataPayload) {
      this.dataPayload = dataPayload;
    }

    public void waitTillDone() throws InterruptedException {
      doneSignal.await();
    }

    @Override
    public void run() {
      try {
        LOG.debug("before runInternal()");
        runInternal();
        LOG.debug("after runInternal()");
      } finally {
        doneSignal.countDown();
      }
    }

    private void runInternal() {
      if (lastError != null) {
        // We're already in an error state, no point doing anything.
        return;
      }
      if (dataPayload.length == 0) {
        // Nothing to do.
        return;
      }

      // Since we have to rewrite the last request's last page's data
      // (may be empty), total data size is our data plus whatever was
      // left from there.
      final int totalDataBytes = dataPayload.length 
          + previousLastPageDataWritten.length;
      // Calculate the total number of pages we're writing to the server.
      final int numberOfPages = (totalDataBytes / PAGE_DATA_SIZE) 
          + (totalDataBytes % PAGE_DATA_SIZE == 0 ? 0 : 1);
      // Fill up the raw bytes we're writing.
      byte[] rawPayload = new byte[numberOfPages * PAGE_SIZE];
      // Keep track of the size of the last page we uploaded.
      int currentLastPageDataSize = -1;
      for (int page = 0; page < numberOfPages; page++) {
        // Our current byte offset in the data.
        int dataOffset = page * PAGE_DATA_SIZE;
        // Our current byte offset in the raw buffer.
        int rawOffset = page * PAGE_SIZE;
        // The size of the data in the current page.
        final short currentPageDataSize = (short) Math.min(PAGE_DATA_SIZE,
            totalDataBytes - dataOffset);
        // Save off this page's size as the potential last page's size.
        currentLastPageDataSize = currentPageDataSize;

        // Write out the page size in the header.
        final byte[] header = fromShort(currentPageDataSize);
        System.arraycopy(header, 0, rawPayload, rawOffset, header.length);
        rawOffset += header.length;

        int bytesToCopyFromDataPayload = currentPageDataSize;
        if (dataOffset < previousLastPageDataWritten.length) {
          // First write out the last page's data.
          final int bytesToCopyFromLastPage = Math.min(currentPageDataSize,
              previousLastPageDataWritten.length - dataOffset);
          System.arraycopy(previousLastPageDataWritten, dataOffset,
              rawPayload, rawOffset, bytesToCopyFromLastPage);
          bytesToCopyFromDataPayload -= bytesToCopyFromLastPage;
          rawOffset += bytesToCopyFromLastPage;
          dataOffset += bytesToCopyFromLastPage;
        }

        if (dataOffset >= previousLastPageDataWritten.length) {
          // Then write the current payload's data.
          System.arraycopy(dataPayload, 
        	  dataOffset - previousLastPageDataWritten.length,
              rawPayload, rawOffset, bytesToCopyFromDataPayload);
        }
      }

      // Raw payload constructed, ship it off to the server.
      writePayloadToServer(rawPayload);

      // Post-send bookkeeping.
      currentBlobOffset += rawPayload.length;
      if (currentLastPageDataSize < PAGE_DATA_SIZE) {
        // Partial page, save it off so it's overwritten in the next request.
        final int startOffset = (numberOfPages - 1) * PAGE_SIZE + PAGE_HEADER_SIZE;
        previousLastPageDataWritten = Arrays.copyOfRange(rawPayload,
            startOffset,
            startOffset + currentLastPageDataSize);
        // Since we're rewriting this page, set our current offset in the server
        // to that page's beginning.
        currentBlobOffset -= PAGE_SIZE;
      } else {
        // It wasn't a partial page, we won't need to rewrite it.
        previousLastPageDataWritten = new byte[0];
      }

      // Extend the file if we need more room in the file. This typically takes
      // less than 200 milliseconds if it has to actually be done,
      // so it is okay to include it in a write and won't cause a long pause.
      // Other writes can be queued behind this write in any case.
      conditionalExtendFile();
    }

    /**
     * Writes the given raw payload to Azure Storage at the current blob
     * offset.
     */
    private void writePayloadToServer(byte[] rawPayload) {
      final ByteArrayInputStream wrapperStream =
                  new ByteArrayInputStream(rawPayload);
      LOG.debug("writing payload of " + rawPayload.length + " bytes to Azure page blob");
      try {
        long start = System.currentTimeMillis();
        blob.uploadPages(wrapperStream, currentBlobOffset, rawPayload.length,
            withMD5Checking(), PageBlobOutputStream.this.opContext);
        long end = System.currentTimeMillis();
        LOG.trace("Azure uploadPages time for " + rawPayload.length + " bytes = " + (end - start));
      } catch (IOException ex) {
        LOG.debug(ExceptionUtils.getStackTrace(ex));
        lastError = ex;
      } catch (StorageException ex) {
        LOG.debug(ExceptionUtils.getStackTrace(ex));
        lastError = new IOException(ex);
      }
      if (lastError != null) {
        LOG.debug("Caught error in PageBlobOutputStream#writePayloadToServer()");
      }
    }
  }

  private synchronized void flushIOBuffers()  {
    if (outBuffer.size() == 0) {
      return;
    }
    lastQueuedTask = new WriteRequest(outBuffer.toByteArray());
    ioThreadPool.execute(lastQueuedTask);
    outBuffer = new ByteArrayOutputStream();
  }

  @VisibleForTesting
  synchronized void waitForLastFlushCompletion() throws IOException {
    try {
      if (lastQueuedTask != null) {
        lastQueuedTask.waitTillDone();
      }
    } catch (InterruptedException e1) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Extend the page blob file if we are close to the end.
   */
  private void conditionalExtendFile() {

    // maximum allowed size of an Azure page blob (1 terabyte)
    final long MAX_PAGE_BLOB_SIZE = 1024L * 1024L * 1024L * 1024L;

    // If blob is already at the maximum size, then don't try to extend it.
    if (currentBlobSize == MAX_PAGE_BLOB_SIZE) {
      return;
    }

    // If we are within the maximum write size of the end of the file,
    if (currentBlobSize - currentBlobOffset <= MAX_RAW_BYTES_PER_REQUEST) {

      // Extend the file. Retry up to 3 times with back-off.
      CloudPageBlob cloudPageBlob = (CloudPageBlob) blob.getBlob();
      long newSize = currentBlobSize + configuredPageBlobExtensionSize;

      // Make sure we don't exceed maximum blob size.
      if (newSize > MAX_PAGE_BLOB_SIZE) {
        newSize = MAX_PAGE_BLOB_SIZE;
      }
      final int MAX_RETRIES = 3;
      int retries = 1;
      boolean resizeDone = false;
      while(!resizeDone && retries <= MAX_RETRIES) {
        try {
          cloudPageBlob.resize(newSize);
          resizeDone = true;
          currentBlobSize = newSize;
        } catch (StorageException e) {
          LOG.warn("Failed to extend size of " + cloudPageBlob.getUri());
          try {

            // sleep 2, 8, 18 seconds for up to 3 retries
            Thread.sleep(2000 * retries * retries);
          } catch (InterruptedException e1) {

            // Restore the interrupted status
            Thread.currentThread().interrupt();
          }
        } finally {
          retries++;
        }
      }
    }
  }

  /**
   * Flushes this output stream and forces any buffered output bytes to be
   * written out. If any data remains in the buffer it is committed to the
   * service. Data is queued for writing but not forced out to the service
   * before the call returns.
   */
  @Override
  public void flush() throws IOException {
    checkStreamState();
    flushIOBuffers();
  }

  /**
   * Writes b.length bytes from the specified byte array to this output stream.
   *
   * @param data
   *          the byte array to write.
   *
   * @throws IOException
   *           if an I/O error occurs. In particular, an IOException may be
   *           thrown if the output stream has been closed.
   */
  @Override
  public void write(final byte[] data) throws IOException {
    write(data, 0, data.length);
  }

  /**
   * Writes length bytes from the specified byte array starting at offset to
   * this output stream.
   *
   * @param data
   *          the byte array to write.
   * @param offset
   *          the start offset in the data.
   * @param length
   *          the number of bytes to write.
   * @throws IOException
   *           if an I/O error occurs. In particular, an IOException may be
   *           thrown if the output stream has been closed.
   */
  @Override
  public void write(final byte[] data, final int offset, final int length)
      throws IOException {
    if (offset < 0 || length < 0 || length > data.length - offset) {
      throw new IndexOutOfBoundsException();
    }

    writeInternal(data, offset, length);
  }

  /**
   * Writes the specified byte to this output stream. The general contract for
   * write is that one byte is written to the output stream. The byte to be
   * written is the eight low-order bits of the argument b. The 24 high-order
   * bits of b are ignored.
   *
   * @param byteVal
   *          the byteValue to write.
   * @throws IOException
   *           if an I/O error occurs. In particular, an IOException may be
   *           thrown if the output stream has been closed.
   */
  @Override
  public void write(final int byteVal) throws IOException {
    write(new byte[] { (byte) (byteVal & 0xFF) });
  }

  /**
   * Writes the data to the buffer and triggers writes to the service as needed.
   *
   * @param data
   *          the byte array to write.
   * @param offset
   *          the start offset in the data.
   * @param length
   *          the number of bytes to write.
   * @throws IOException
   *           if an I/O error occurs. In particular, an IOException may be
   *           thrown if the output stream has been closed.
   */
  private synchronized void writeInternal(final byte[] data, int offset,
      int length) throws IOException {
    while (length > 0) {
      checkStreamState();
      final int availableBufferBytes = MAX_DATA_BYTES_PER_REQUEST
          - this.outBuffer.size();
      final int nextWrite = Math.min(availableBufferBytes, length);

      outBuffer.write(data, offset, nextWrite);
      offset += nextWrite;
      length -= nextWrite;

      if (outBuffer.size() > MAX_DATA_BYTES_PER_REQUEST) {
        throw new RuntimeException("Internal error: maximum write size " +
            Integer.toString(MAX_DATA_BYTES_PER_REQUEST) + "exceeded.");
      }

      if (outBuffer.size() == MAX_DATA_BYTES_PER_REQUEST) {
        flushIOBuffers();
      }
    }
  }

  /**
   * Force all data in the output stream to be written to Azure storage.
   * Wait to return until this is complete.
   */
  @Override
  public synchronized void hsync() throws IOException {
    LOG.debug("Entering PageBlobOutputStream#hsync().");
    long start = System.currentTimeMillis();
  	flush();
    LOG.debug(ioThreadPool.toString());
    try {
      if (lastQueuedTask != null) {
        lastQueuedTask.waitTillDone();
      }
    } catch (InterruptedException e1) {

      // Restore the interrupted status
      Thread.currentThread().interrupt();
    }
    checkStreamState();
    LOG.debug("Leaving PageBlobOutputStream#hsync(). Total hsync duration = "
  	  + (System.currentTimeMillis() - start) + " msec.");
  }

  @Override
  public void hflush() throws IOException {

    // hflush is required to force data to storage, so call hsync,
    // which does that.
    hsync();
  }

  @Deprecated
  public void sync() throws IOException {

    // Sync has been deprecated in favor of hflush.
    hflush();
  }

  // For unit testing purposes: kill the IO threads.
  @VisibleForTesting
  void killIoThreads() {
    ioThreadPool.shutdownNow();
  }
}
