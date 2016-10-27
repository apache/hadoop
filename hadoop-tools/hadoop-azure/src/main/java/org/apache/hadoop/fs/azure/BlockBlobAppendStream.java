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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.List;
import java.util.UUID;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.azure.StorageInterface.CloudBlockBlobWrapper;
import org.eclipse.jetty.util.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.BlockListingFilter;

/**
 * Stream object that implememnts append for Block Blobs in WASB.
 */
public class BlockBlobAppendStream extends OutputStream {

  private final String key;
  private final int bufferSize;
  private ByteArrayOutputStream outBuffer;
  private final CloudBlockBlobWrapper blob;
  private final OperationContext opContext;

  /**
   * Variable to track if the stream has been closed.
   */
  private boolean closed = false;

  /**
   * Variable to track if the append lease is released.
   */

  private volatile boolean leaseFreed;

  /**
   * Variable to track if the append stream has been
   * initialized.
   */

  private boolean initialized = false;

  /**
   *  Last IOException encountered
   */
  private volatile IOException lastError = null;

  /**
   * List to keep track of the uncommitted azure storage
   * block ids
   */
  private final List<BlockEntry> uncommittedBlockEntries;

  private static final int UNSET_BLOCKS_COUNT = -1;

  /**
   * Variable to hold the next block id to be used for azure
   * storage blocks.
   */
  private long nextBlockCount = UNSET_BLOCKS_COUNT;

  /**
   * Variable to hold the block id prefix to be used for azure
   * storage blocks from azure-storage-java sdk version 4.2.0 onwards
   */
  private String blockIdPrefix = null;

  private final Random sequenceGenerator = new Random();

  /**
   *  Time to wait to renew lease in milliseconds
   */
  private static final int LEASE_RENEWAL_PERIOD = 10000;

  /**
   *  Number of times to retry for lease renewal
   */
  private static final int MAX_LEASE_RENEWAL_RETRY_COUNT = 3;

  /**
   *  Time to wait before retrying to set the lease
   */
  private static final int LEASE_RENEWAL_RETRY_SLEEP_PERIOD = 500;

  /**
   *  Metadata key used on the blob to indicate append lease is active
   */
  public static final String APPEND_LEASE = "append_lease";

  /**
   * Timeout value for the append lease in millisecs. If the lease is not
   * renewed within 30 seconds then another thread can acquire the append lease
   * on the blob
   */
  public static final int APPEND_LEASE_TIMEOUT = 30000;

  /**
   *  Metdata key used on the blob to indicate last modified time of append lease
   */
  public static final String APPEND_LEASE_LAST_MODIFIED = "append_lease_last_modified";

  /**
   * Number of times block upload needs is retried.
   */
  private static final int MAX_BLOCK_UPLOAD_RETRIES = 3;

  /**
   * Wait time between block upload retries in millisecs.
   */
  private static final int BLOCK_UPLOAD_RETRY_INTERVAL = 1000;

  private static final Logger LOG = LoggerFactory.getLogger(BlockBlobAppendStream.class);

  private static final int MAX_BLOCK_COUNT = 100000;

  private ThreadPoolExecutor ioThreadPool;

  /**
   * Atomic integer to provide thread id for thread names for uploader threads.
   */
  private final AtomicInteger threadSequenceNumber;

  /**
   * Prefix to be used for thread names for uploader threads.
   */
  private static final String THREAD_ID_PREFIX = "BlockBlobAppendStream";

  private static final String UTC_STR = "UTC";

  public BlockBlobAppendStream(final CloudBlockBlobWrapper blob,
      final String aKey, final int bufferSize, final OperationContext opContext)
          throws IOException {

    if (null == aKey || 0 == aKey.length()) {
      throw new IllegalArgumentException(
          "Illegal argument: The key string is null or empty");
    }

    if (0 >= bufferSize) {
      throw new IllegalArgumentException(
          "Illegal argument bufferSize cannot be zero or negative");
    }


    this.blob = blob;
    this.opContext = opContext;
    this.key = aKey;
    this.bufferSize = bufferSize;
    this.threadSequenceNumber = new AtomicInteger(0);
    this.blockIdPrefix = null;
    setBlocksCountAndBlockIdPrefix();

    this.outBuffer = new ByteArrayOutputStream(bufferSize);
    this.uncommittedBlockEntries = new ArrayList<BlockEntry>();

    // Acquire append lease on the blob.
    try {
      //Set the append lease if the value of the append lease is false
      if (!updateBlobAppendMetadata(true, false)) {
        LOG.error("Unable to set Append Lease on the Blob : {} "
            + "Possibly because another client already has a create or append stream open on the Blob", key);
        throw new IOException("Unable to set Append lease on the Blob. "
            + "Possibly because another client already had an append stream open on the Blob.");
      }
    } catch (StorageException ex) {
      LOG.error("Encountered Storage exception while acquiring append "
          + "lease on blob : {}. Storage Exception : {} ErrorCode : {}",
          key, ex, ex.getErrorCode());

      throw new IOException(ex);
    }

    leaseFreed = false;
  }

  /**
   * Helper method that starts an Append Lease renewer thread and the
   * thread pool.
   */
  public synchronized void initialize() {

    if (initialized) {
      return;
    }
    /*
     * Start the thread for  Append lease renewer.
     */
    Thread appendLeaseRenewer = new Thread(new AppendRenewer());
    appendLeaseRenewer.setDaemon(true);
    appendLeaseRenewer.setName(String.format("%s-AppendLeaseRenewer", key));
    appendLeaseRenewer.start();

    /*
     * Parameters to ThreadPoolExecutor:
     * corePoolSize : the number of threads to keep in the pool, even if they are idle,
     *                unless allowCoreThreadTimeOut is set
     * maximumPoolSize : the maximum number of threads to allow in the pool
     * keepAliveTime - when the number of threads is greater than the core,
     *                 this is the maximum time that excess idle threads will
     *                 wait for new tasks before terminating.
     * unit - the time unit for the keepAliveTime argument
     * workQueue - the queue to use for holding tasks before they are executed
     *  This queue will hold only the Runnable tasks submitted by the execute method.
     */
    this.ioThreadPool = new ThreadPoolExecutor(4, 4, 2, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(), new UploaderThreadFactory());

    initialized = true;
  }

  /**
   * Get the blob name.
   *
   * @return String Blob name.
   */
  public String getKey() {
    return key;
  }

  /**
   * Get the backing blob.
   * @return buffer size of the stream.
   */
  public int getBufferSize() {
    return bufferSize;
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
      throw new IndexOutOfBoundsException("write API in append stream called with invalid arguments");
    }

    writeInternal(data, offset, length);
  }

  @Override
  public synchronized void close() throws IOException {

    if (!initialized) {
      throw new IOException("Trying to close an uninitialized Append stream");
    }

    if (closed) {
      return;
    }

    if (leaseFreed) {
      throw new IOException(String.format("Attempting to close an append stream on blob : %s "
          + " that does not have lease on the Blob. Failing close", key));
    }

    if (outBuffer.size() > 0) {
      uploadBlockToStorage(outBuffer.toByteArray());
    }

    ioThreadPool.shutdown();

    try {
      if (!ioThreadPool.awaitTermination(10, TimeUnit.MINUTES)) {
        LOG.error("Time out occured while waiting for IO request to finish in append"
            + " for blob : {}", key);
        NativeAzureFileSystemHelper.logAllLiveStackTraces();
        throw new IOException("Timed out waiting for IO requests to finish");
      }
    } catch(InterruptedException intrEx) {

      // Restore the interrupted status
      Thread.currentThread().interrupt();
      LOG.error("Upload block operation in append interrupted for blob {}. Failing close", key);
      throw new IOException("Append Commit interrupted.");
    }

    // Calling commit after all blocks are succesfully uploaded.
    if (lastError == null) {
      commitAppendBlocks();
    }

    // Perform cleanup.
    cleanup();

    if (lastError != null) {
      throw lastError;
    }
  }

  /**
   * Helper method that cleans up the append stream.
   */
  private synchronized void cleanup() {

    closed = true;

    try {
      // Set the value of append lease to false if the value is set to true.
        updateBlobAppendMetadata(false, true);
    } catch(StorageException ex) {
      LOG.debug("Append metadata update on the Blob : {} encountered Storage Exception : {} "
          + "Error Code : {}",
          key, ex, ex.getErrorCode());
      lastError = new IOException(ex);
    }

    leaseFreed = true;
  }

  /**
   * Method to commit all the uncommited blocks to azure storage.
   * If the commit fails then blocks are automatically cleaned up
   * by Azure storage.
   * @throws IOException
   */
  private synchronized void commitAppendBlocks() throws IOException {

    SelfRenewingLease lease = null;

    try {
      if (uncommittedBlockEntries.size() > 0) {

        //Acquiring lease on the blob.
        lease = new SelfRenewingLease(blob);

        // Downloading existing blocks
        List<BlockEntry> blockEntries =  blob.downloadBlockList(BlockListingFilter.COMMITTED,
            new BlobRequestOptions(), opContext);

        // Adding uncommitted blocks.
        blockEntries.addAll(uncommittedBlockEntries);

        AccessCondition accessCondition = new AccessCondition();
        accessCondition.setLeaseID(lease.getLeaseID());
        blob.commitBlockList(blockEntries, accessCondition, new BlobRequestOptions(), opContext);
        uncommittedBlockEntries.clear();
      }
    } catch(StorageException ex) {
      LOG.error("Storage exception encountered during block commit phase of append for blob"
          + " : {} Storage Exception : {} Error Code: {}", key, ex, ex.getErrorCode());
      throw new IOException("Encountered Exception while committing append blocks", ex);
    } finally {
      if (lease != null) {
        try {
          lease.free();
        } catch(StorageException ex) {
          LOG.debug("Exception encountered while releasing lease for "
              + "blob : {} StorageException : {} ErrorCode : {}", key, ex, ex.getErrorCode());
          // Swallowing exception here as the lease is cleaned up by the SelfRenewingLease object.
        }
      }
    }
  }

  /**
   * Helper method used to generate the blockIDs. The algorithm used is similar to the Azure
   * storage SDK.
   */
  private void setBlocksCountAndBlockIdPrefix() throws IOException {

    try {

      if (nextBlockCount == UNSET_BLOCKS_COUNT && blockIdPrefix==null) {

        List<BlockEntry> blockEntries =
            blob.downloadBlockList(BlockListingFilter.COMMITTED, new BlobRequestOptions(), opContext);

        String blockZeroBlockId = (blockEntries.size() > 0) ? blockEntries.get(0).getId() : "";
        String prefix = UUID.randomUUID().toString() + "-";
        String sampleNewerVersionBlockId = generateNewerVersionBlockId(prefix, 0);

        if (blockEntries.size() > 0 && blockZeroBlockId.length() < sampleNewerVersionBlockId.length()) {

          // If blob has already been created with 2.2.0, append subsequent blocks with older version (2.2.0) blockId
          // compute nextBlockCount, the way it was done before; and don't use blockIdPrefix
          this.blockIdPrefix = "";
          nextBlockCount = (long) (sequenceGenerator.nextInt(Integer.MAX_VALUE))
              + sequenceGenerator.nextInt(Integer.MAX_VALUE - MAX_BLOCK_COUNT);
          nextBlockCount += blockEntries.size();

        } else {

          // If there are no existing blocks, create the first block with newer version (4.2.0) blockId
          // If blob has already been created with 4.2.0, append subsequent blocks with newer version (4.2.0) blockId
          this.blockIdPrefix = prefix;
          nextBlockCount = blockEntries.size();

        }

      }

    } catch (StorageException ex) {
      LOG.debug("Encountered storage exception during setting next Block Count and BlockId prefix."
          + " StorageException : {} ErrorCode : {}", ex, ex.getErrorCode());
      throw new IOException(ex);
    }
  }

  /**
   * Helper method that generates the next block id for uploading a block to azure storage.
   * @return String representing the block ID generated.
   * @throws IOException
   */
  private String generateBlockId() throws IOException {

    if (nextBlockCount == UNSET_BLOCKS_COUNT) {
      throw new IOException("Append Stream in invalid state. nextBlockCount not set correctly");
    }

    if (this.blockIdPrefix == null) {
      throw new IOException("Append Stream in invalid state. blockIdPrefix not set correctly");
    }

    if (!this.blockIdPrefix.equals("")) {

      return generateNewerVersionBlockId(this.blockIdPrefix, nextBlockCount++);

    } else {

      return generateOlderVersionBlockId(nextBlockCount++);

    }

  }

  /**
   * Helper method that generates an older (2.2.0) version blockId
   * @return String representing the block ID generated.
   */
  private String generateOlderVersionBlockId(long id) {

    byte[] blockIdInBytes = getBytesFromLong(id);
    return new String(Base64.encodeBase64(blockIdInBytes), StandardCharsets.UTF_8);
  }

  /**
   * Helper method that generates an newer (4.2.0) version blockId
   * @return String representing the block ID generated.
   */
  private String generateNewerVersionBlockId(String prefix, long id) {

    String blockIdSuffix  = String.format("%06d", id);
    byte[] blockIdInBytes = (prefix + blockIdSuffix).getBytes(StandardCharsets.UTF_8);
    return new String(Base64.encodeBase64(blockIdInBytes), StandardCharsets.UTF_8);
  }

  /**
   * Returns a byte array that represents the data of a <code>long</code> value. This
   * utility method is copied from com.microsoft.azure.storage.core.Utility class.
   * This class is marked as internal, hence we clone the method here and not express
   * dependency on the Utility Class
   *
   * @param value
   *            The value from which the byte array will be returned.
   *
   * @return A byte array that represents the data of the specified <code>long</code> value.
   */
  private static byte[] getBytesFromLong(final long value) {

    final byte[] tempArray = new byte[8];

    for (int m = 0; m < 8; m++) {
      tempArray[7 - m] = (byte) ((value >> (8 * m)) & 0xFF);
    }

    return tempArray;
  }

  /**
   * Helper method that creates a thread to upload a block to azure storage.
   * @param payload
   * @throws IOException
   */
  private synchronized void uploadBlockToStorage(byte[] payload)
      throws IOException {

    // upload payload to azure storage
    String blockId = generateBlockId();

    // Since uploads of the Azure storage are done in parallel threads, we go ahead
    // add the blockId in the uncommitted list. If the upload of the block fails
    // we don't commit the blockIds.
    BlockEntry blockEntry = new BlockEntry(blockId);
    blockEntry.setSize(payload.length);
    uncommittedBlockEntries.add(blockEntry);
    ioThreadPool.execute(new WriteRequest(payload, blockId));
  }


  /**
   * Helper method to updated the Blob metadata during Append lease operations.
   * Blob metadata is updated to holdLease value only if the current lease
   * status is equal to testCondition and the last update on the blob metadata
   * is less that 30 secs old.
   * @param holdLease
   * @param testCondition
   * @return true if the updated lease operation was successful or false otherwise
   * @throws StorageException
   */
  private boolean updateBlobAppendMetadata(boolean holdLease, boolean testCondition)
      throws StorageException {

    SelfRenewingLease lease = null;
    StorageException lastStorageException = null;
    int leaseRenewalRetryCount = 0;

    /*
     * Updating the Blob metadata honours following algorithm based on
     *  1) If the append lease metadata is present
     *  2) Last updated time of the append lease
     *  3) Previous value of the Append lease metadata.
     *
     * The algorithm:
     *  1) If append lease metadata is not part of the Blob. In this case
     *     this is the first client to Append so we update the metadata.
     *  2) If append lease metadata is present and timeout has occurred.
     *     In this case irrespective of what the value of the append lease is we update the metadata.
     *  3) If append lease metadata is present and is equal to testCondition value (passed as parameter)
     *     and timeout has not occurred, we update the metadata.
     *  4) If append lease metadata is present and is not equal to testCondition value (passed as parameter)
     *     and timeout has not occurred, we do not update metadata and return false.
     *
     */
    while (leaseRenewalRetryCount < MAX_LEASE_RENEWAL_RETRY_COUNT) {

      lastStorageException = null;

      synchronized(this) {
        try {

          final Calendar currentCalendar = Calendar
              .getInstance(Locale.US);
          currentCalendar.setTimeZone(TimeZone.getTimeZone(UTC_STR));
          long currentTime = currentCalendar.getTime().getTime();

          // Acquire lease on the blob.
          lease = new SelfRenewingLease(blob);

          blob.downloadAttributes(opContext);
          HashMap<String, String> metadata = blob.getMetadata();

          if (metadata.containsKey(APPEND_LEASE)
              && currentTime - Long.parseLong(
                  metadata.get(APPEND_LEASE_LAST_MODIFIED)) <= BlockBlobAppendStream.APPEND_LEASE_TIMEOUT
              && !metadata.get(APPEND_LEASE).equals(Boolean.toString(testCondition))) {
            return false;
          }

          metadata.put(APPEND_LEASE, Boolean.toString(holdLease));
          metadata.put(APPEND_LEASE_LAST_MODIFIED, Long.toString(currentTime));
          blob.setMetadata(metadata);
          AccessCondition accessCondition = new AccessCondition();
          accessCondition.setLeaseID(lease.getLeaseID());
          blob.uploadMetadata(accessCondition, null, opContext);
          return true;

        } catch (StorageException ex) {

          lastStorageException = ex;
          LOG.debug("Lease renewal for Blob : {} encountered Storage Exception : {} "
              + "Error Code : {}",
              key, ex, ex.getErrorCode());
          leaseRenewalRetryCount++;

        } finally {

          if (lease != null) {
            try {
              lease.free();
            } catch(StorageException ex) {
              LOG.debug("Encountered Storage exception while releasing lease for Blob {} "
                  + "during Append  metadata operation. Storage Exception {} "
                  + "Error Code : {} ", key, ex, ex.getErrorCode());
            } finally {
              lease = null;
            }
          }
        }
      }

      if (leaseRenewalRetryCount == MAX_LEASE_RENEWAL_RETRY_COUNT) {
        throw lastStorageException;
      } else {
        try {
          Thread.sleep(LEASE_RENEWAL_RETRY_SLEEP_PERIOD);
        } catch(InterruptedException ex) {
          LOG.debug("Blob append metadata updated method interrupted");
          Thread.currentThread().interrupt();
        }
      }
    }

    // The code should not enter here because the while loop will
    // always be executed and if the while loop is executed we
    // would returning from the while loop.
    return false;
  }

  /**
   * This is the only method that should be writing to outBuffer to maintain consistency of the outBuffer.
   * @param data
   * @param offset
   * @param length
   * @throws IOException
   */
  private synchronized void writeInternal(final byte[] data, final int offset, final int length)
      throws IOException {

    if (!initialized) {
      throw new IOException("Trying to write to an un-initialized Append stream");
    }

    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    if (leaseFreed) {
      throw new IOException(String.format("Write called on a append stream not holding lease. Failing Write"));
    }

    byte[] currentData = new byte[length];
    System.arraycopy(data, offset, currentData, 0, length);

    // check to see if the data to be appended exceeds the
    // buffer size. If so we upload a block to azure storage.
    while ((outBuffer.size() + currentData.length) > bufferSize) {

      byte[] payload = new byte[bufferSize];

      // Add data from the existing buffer
      System.arraycopy(outBuffer.toByteArray(), 0, payload, 0, outBuffer.size());

      // Updating the available size in the payload
      int availableSpaceInPayload = bufferSize - outBuffer.size();

      // Adding data from the current call
      System.arraycopy(currentData, 0, payload, outBuffer.size(), availableSpaceInPayload);

      uploadBlockToStorage(payload);

      // updating the currentData buffer
      byte[] tempBuffer = new byte[currentData.length - availableSpaceInPayload];
      System.arraycopy(currentData, availableSpaceInPayload,
          tempBuffer, 0, currentData.length - availableSpaceInPayload);
      currentData = tempBuffer;
      outBuffer = new ByteArrayOutputStream(bufferSize);
    }

    outBuffer.write(currentData);
  }

  /**
   * Runnable instance that uploads the block of data to azure storage.
   *
   *
   */
  private class WriteRequest implements Runnable {
    private final byte[] dataPayload;
    private final String blockId;

    public WriteRequest(byte[] dataPayload, String blockId) {
      this.dataPayload = dataPayload;
      this.blockId = blockId;
    }

    @Override
    public void run() {

      int uploadRetryAttempts = 0;
      IOException lastLocalException = null;
      while (uploadRetryAttempts < MAX_BLOCK_UPLOAD_RETRIES) {
        try {

          blob.uploadBlock(blockId, new ByteArrayInputStream(dataPayload),
              dataPayload.length, new BlobRequestOptions(), opContext);
          break;
        } catch(Exception ioe) {
          Log.getLog().debug("Encountered exception during uploading block for Blob : {} Exception : {}", key, ioe);
          uploadRetryAttempts++;
          lastLocalException = new IOException("Encountered Exception while uploading block", ioe);
          try {
            Thread.sleep(BLOCK_UPLOAD_RETRY_INTERVAL);
          } catch(InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }

      if (uploadRetryAttempts == MAX_BLOCK_UPLOAD_RETRIES) {
        lastError = lastLocalException;
      }
    }
  }

  /**
   * A ThreadFactory that creates uploader thread with
   * meaningful names helpful for debugging purposes.
   */
  class UploaderThreadFactory implements ThreadFactory {

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setName(String.format("%s-%s-%d", THREAD_ID_PREFIX, key,
          threadSequenceNumber.getAndIncrement()));
      return t;
    }
  }

  /**
   * A deamon thread that renews the Append lease on the blob.
   * The thread sleeps for LEASE_RENEWAL_PERIOD time before renewing
   * the lease. If an error is encountered while renewing the lease
   * then an lease is released by this thread, which fails all other
   * operations.
   */
  private class AppendRenewer implements Runnable {

    @Override
    public void run() {

      while (!leaseFreed) {

        try {
          Thread.sleep(LEASE_RENEWAL_PERIOD);
        } catch (InterruptedException ie) {
          LOG.debug("Appender Renewer thread interrupted");
          Thread.currentThread().interrupt();
        }

        Log.getLog().debug("Attempting to renew append lease on {}", key);

        try {
          if (!leaseFreed) {
            // Update the blob metadata to renew the append lease
            if (!updateBlobAppendMetadata(true, true)) {
              LOG.error("Unable to re-acquire append lease on the Blob {} ", key);
              leaseFreed = true;
            }
          }
        } catch (StorageException ex) {

          LOG.debug("Lease renewal for Blob : {} encountered "
              + "Storage Exception : {} Error Code : {}", key, ex, ex.getErrorCode());

          // We swallow the exception here because if the blob metadata is not updated for
          // APPEND_LEASE_TIMEOUT period, another thread would be able to detect this and
          // continue forward if it needs to append.
          leaseFreed = true;
        }
      }
    }
  }
}