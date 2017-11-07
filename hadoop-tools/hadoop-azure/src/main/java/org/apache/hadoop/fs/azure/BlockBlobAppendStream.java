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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.azure.StorageInterface.CloudBlockBlobWrapper;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.BlockListingFilter;
import com.microsoft.azure.storage.blob.BlockSearchMode;

/**
 * Stream object that implements append for Block Blobs in WASB.
 *
 * The stream object implements hflush/hsync and block compaction. Block
 * compaction is the process of replacing a sequence of small blocks with one
 * big block. Azure Block blobs supports up to 50000 blocks and every
 * hflush/hsync generates one block. When the number of blocks is above 32000,
 * the process of compaction decreases the total number of blocks, if possible.
 * If compaction is disabled, hflush/hsync are empty functions.
 *
 * The stream object uses background threads for uploading the blocks and the
 * block blob list. Blocks can be uploaded concurrently. However, when the block
 * list is uploaded, block uploading should stop. If a block is uploaded before
 * the block list and the block id is not in the list, the block will be lost.
 * If the block is uploaded after the block list and the block id is in the
 * list, the block list upload will fail. The exclusive access for the block
 * list upload is managed by uploadingSemaphore.
 */

public class BlockBlobAppendStream extends OutputStream implements Syncable,
    StreamCapabilities {

  /**
   * The name of the blob/file.
   */
  private final String key;

  /**
   * This variable tracks if this is new blob or existing one.
   */
  private boolean blobExist;

  /**
   * When the blob exist, to to prevent concurrent write we take a lease.
   * Taking a lease is not necessary for new blobs.
   */
  private SelfRenewingLease lease = null;

  /**
   * The support for process of compaction is optional.
   */
  private final boolean compactionEnabled;

  /**
   * The number of blocks above each block compaction is triggered.
   */
  private static final int DEFAULT_ACTIVATE_COMPACTION_BLOCK_COUNT = 32000;

  /**
   * The number of blocks above each block compaction is triggered.
   */
  private int activateCompactionBlockCount
      = DEFAULT_ACTIVATE_COMPACTION_BLOCK_COUNT;

  /**
   * The size of the output buffer. Writes store the data in outBuffer until
   * either the size is above maxBlockSize or hflush/hsync is called.
   */
  private final AtomicInteger maxBlockSize;

  /**
   * The current buffer where writes are stored.
   */
  private ByteBuffer outBuffer;

  /**
   * The size of the blob that has been successfully stored in the Azure Blob
   * service.
   */
  private final AtomicLong committedBlobLength = new AtomicLong(0);

  /**
   * Position of last block in the blob.
   */
  private volatile long blobLength = 0;

  /**
   * Minutes waiting before the close operation timed out.
   */
  private static final int CLOSE_UPLOAD_DELAY = 10;

  /**
   * Keep alive time for the threadpool.
   */
  private static final int THREADPOOL_KEEP_ALIVE = 30;
  /**
   * Azure Block Blob used for the stream.
   */
  private final CloudBlockBlobWrapper blob;

  /**
   * Azure Storage operation context.
   */
  private final OperationContext opContext;

  /**
   * Commands send from client calls to the background thread pool.
   */
  private abstract class UploadCommand {

    // the blob offset for the command
    private final long commandBlobOffset;

    // command completion latch
    private final CountDownLatch completed = new CountDownLatch(1);

    UploadCommand(long offset) {
      this.commandBlobOffset = offset;
    }

    long getCommandBlobOffset() {
      return commandBlobOffset;
    }

    void await() throws InterruptedException {
      completed.await();
    }

    void awaitAsDependent() throws InterruptedException {
      await();
    }

    void setCompleted() {
      completed.countDown();
    }

    void execute() throws InterruptedException, IOException {}

    void dump() {}
  }

  /**
   * The list of recent commands. Before block list is committed, all the block
   * listed in the list must be uploaded. activeBlockCommands is used for
   * enumerating the blocks and waiting on the latch until the block is
   * uploaded.
   */
  private final ConcurrentLinkedQueue<UploadCommand> activeBlockCommands
      = new ConcurrentLinkedQueue<>();

  /**
   * Variable to track if the stream has been closed.
   */
  private volatile boolean closed = false;

  /**
   *  First IOException encountered.
   */
  private final AtomicReference<IOException> firstError
          = new AtomicReference<>();

  /**
   * Flag set when the first error has been thrown.
   */
  private boolean firstErrorThrown = false;

  /**
   * Semaphore for serializing block uploads with NativeAzureFileSystem.
   *
   * The semaphore starts with number of permits equal to the number of block
   * upload threads. Each block upload thread needs one permit to start the
   * upload. The put block list acquires all the permits before the block list
   * is committed.
   */
  private final Semaphore uploadingSemaphore = new Semaphore(
      MAX_NUMBER_THREADS_IN_THREAD_POOL,
      true);

  /**
   * Queue storing buffers with the size of the Azure block ready for
   * reuse. The pool allows reusing the blocks instead of allocating new
   * blocks. After the data is sent to the service, the buffer is returned
   * back to the queue
   */
  private final ElasticByteBufferPool poolReadyByteBuffers
          = new ElasticByteBufferPool();

  /**
   * The blob's block list.
   */
  private final List<BlockEntry> blockEntries = new ArrayList<>(
      DEFAULT_CAPACITY_BLOCK_ENTRIES);

  private static final int DEFAULT_CAPACITY_BLOCK_ENTRIES = 1024;

  /**
   * The uncommitted blob's block list.
   */
  private final ConcurrentLinkedDeque<BlockEntry> uncommittedBlockEntries
      = new ConcurrentLinkedDeque<>();

  /**
   * Variable to hold the next block id to be used for azure storage blocks.
   */
  private static final int UNSET_BLOCKS_COUNT = -1;
  private long nextBlockCount = UNSET_BLOCKS_COUNT;

  /**
   * Variable to hold the block id prefix to be used for azure storage blocks.
   */
  private String blockIdPrefix = null;

  /**
   *  Maximum number of threads in block upload thread pool.
   */
  private static final int MAX_NUMBER_THREADS_IN_THREAD_POOL = 4;

  /**
   * Number of times block upload needs is retried.
   */
  private static final int MAX_BLOCK_UPLOAD_RETRIES = 3;

  /**
   * Wait time between block upload retries in milliseconds.
   */
  private static final int BLOCK_UPLOAD_RETRY_INTERVAL = 1000;

  /**
   * Logger.
   */
  private static final Logger LOG =
          LoggerFactory.getLogger(BlockBlobAppendStream.class);

  /**
   * The absolute maximum of blocks for a blob. It includes committed and
   * temporary blocks.
   */
  private static final int MAX_BLOCK_COUNT = 100000;

  /**
   * The upload thread pool executor.
   */
  private ThreadPoolExecutor ioThreadPool;

  /**
   * Azure Storage access conditions for the blob.
   */
  private final AccessCondition accessCondition = new AccessCondition();

  /**
   * Atomic integer to provide thread id for thread names for uploader threads.
   */
  private final AtomicInteger threadSequenceNumber;

  /**
   * Prefix to be used for thread names for uploader threads.
   */
  private static final String THREAD_ID_PREFIX = "append-blockblob";

  /**
   * BlockBlobAppendStream constructor.
   *
   * @param blob
   *          Azure Block Blob
   * @param aKey
   *          blob's name
   * @param bufferSize
   *          the maximum size of a blob block.
   * @param compactionEnabled
   *          is the compaction process enabled for this blob
   * @param opContext
   *          Azure Store operation context for the blob
   * @throws IOException
   *           if an I/O error occurs. In particular, an IOException may be
   *           thrown if the output stream cannot be used for append operations
   */
  public BlockBlobAppendStream(final CloudBlockBlobWrapper blob,
                               final String aKey,
                               final int bufferSize,
                               final boolean compactionEnabled,
                               final OperationContext opContext)
          throws IOException {

    Preconditions.checkArgument(StringUtils.isNotEmpty(aKey));
    Preconditions.checkArgument(bufferSize >= 0);

    this.blob = blob;
    this.opContext = opContext;
    this.key = aKey;
    this.maxBlockSize = new AtomicInteger(bufferSize);
    this.threadSequenceNumber = new AtomicInteger(0);
    this.blockIdPrefix = null;
    this.compactionEnabled = compactionEnabled;
    this.blobExist = true;
    this.outBuffer = poolReadyByteBuffers.getBuffer(false, maxBlockSize.get());

    try {
      // download the block list
      blockEntries.addAll(
          blob.downloadBlockList(
              BlockListingFilter.COMMITTED,
              new BlobRequestOptions(),
              opContext));

      blobLength = blob.getProperties().getLength();

      committedBlobLength.set(blobLength);

      // Acquiring lease on the blob.
      lease = new SelfRenewingLease(blob, true);
      accessCondition.setLeaseID(lease.getLeaseID());

    } catch (StorageException ex) {
      if (ex.getErrorCode().equals(StorageErrorCodeStrings.BLOB_NOT_FOUND)) {
        blobExist = false;
      }
      else if (ex.getErrorCode().equals(
              StorageErrorCodeStrings.LEASE_ALREADY_PRESENT)) {
        throw new AzureException(
                "Unable to set Append lease on the Blob: " + ex, ex);
      }
      else {
        LOG.debug(
            "Encountered storage exception."
                + " StorageException : {} ErrorCode : {}",
            ex,
            ex.getErrorCode());
        throw new AzureException(ex);
      }
    }

    setBlocksCountAndBlockIdPrefix(blockEntries);

    this.ioThreadPool = new ThreadPoolExecutor(
        MAX_NUMBER_THREADS_IN_THREAD_POOL,
        MAX_NUMBER_THREADS_IN_THREAD_POOL,
        THREADPOOL_KEEP_ALIVE,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new UploaderThreadFactory());
  }

  /**
   * Set payload size of the stream.
   * It is intended to be used for unit testing purposes only.
   */
  @VisibleForTesting
  synchronized void setMaxBlockSize(int size) {
    maxBlockSize.set(size);

    // it is for testing only so we can abandon the previously allocated
    // payload
    this.outBuffer = ByteBuffer.allocate(maxBlockSize.get());
  }

  /**
   * Set compaction parameters.
   * It is intended to be used for unit testing purposes only.
   */
  @VisibleForTesting
  void setCompactionBlockCount(int activationCount) {
    activateCompactionBlockCount = activationCount;
  }

  /**
   * Get the list of block entries. It is used for testing purposes only.
   * @return List of block entries.
   */
  @VisibleForTesting
  List<BlockEntry> getBlockList() throws StorageException, IOException {
    return blob.downloadBlockList(
        BlockListingFilter.COMMITTED,
        new BlobRequestOptions(),
        opContext);
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
  public synchronized void write(final byte[] data, int offset, int length)
      throws IOException {
    Preconditions.checkArgument(data != null, "null data");

    if (offset < 0 || length < 0 || length > data.length - offset) {
      throw new IndexOutOfBoundsException();
    }

    if (closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    while (outBuffer.remaining() < length) {

      int remaining = outBuffer.remaining();
      outBuffer.put(data, offset, remaining);

      // upload payload to azure storage
      addBlockUploadCommand();

      offset += remaining;
      length -= remaining;
    }

    outBuffer.put(data, offset, length);
  }


  /**
   * Flushes this output stream and forces any buffered output bytes to be
   * written out. If any data remains in the payload it is committed to the
   * service. Data is queued for writing and forced out to the service
   * before the call returns.
   */
  @Override
  public void flush() throws IOException {

    if (closed) {
      // calling close() after the stream is closed starts with call to flush()
      return;
    }

    addBlockUploadCommand();

    if (committedBlobLength.get() < blobLength) {
      try {
        // wait until the block list is committed
        addFlushCommand().await();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Force all data in the output stream to be written to Azure storage.
   * Wait to return until this is complete.
   */
  @Override
  public void hsync() throws IOException {
    // when block compaction is disabled, hsync is empty function
    if (compactionEnabled) {
      flush();
    }
  }

  /**
   * Force all data in the output stream to be written to Azure storage.
   * Wait to return until this is complete.
   */
  @Override
  public void hflush() throws IOException {
    // when block compaction is disabled, hflush is empty function
    if (compactionEnabled) {
      flush();
    }
  }

  /**
   * The Synchronization capabilities of this stream depend upon the compaction
   * policy.
   * @param capability string to query the stream support for.
   * @return true for hsync and hflush when compaction is enabled.
   */
  @Override
  public boolean hasCapability(String capability) {
    if (!compactionEnabled) {
      return false;
    }
    switch (capability.toLowerCase(Locale.ENGLISH)) {
    case StreamCapabilities.HSYNC:
    case StreamCapabilities.HFLUSH:
      return true;
    default:
      return false;
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

    LOG.debug("close {} ", key);

    if (closed) {
      return;
    }

    // Upload the last block regardless of compactionEnabled flag
    flush();

    // Initiates an orderly shutdown in which previously submitted tasks are
    // executed.
    ioThreadPool.shutdown();

    try {
      // wait up to CLOSE_UPLOAD_DELAY minutes to upload all the blocks
      if (!ioThreadPool.awaitTermination(CLOSE_UPLOAD_DELAY, TimeUnit.MINUTES)) {
        LOG.error("Time out occurred while close() is waiting for IO request to"
            + " finish in append"
            + " for blob : {}",
            key);
        NativeAzureFileSystemHelper.logAllLiveStackTraces();
        throw new AzureException("Timed out waiting for IO requests to finish");
      }
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    // release the lease
    if (firstError.get() == null && blobExist) {
        try {
          lease.free();
        } catch (StorageException ex) {
          LOG.debug("Lease free update blob {} encountered Storage Exception:"
              + " {} Error Code : {}",
              key,
              ex,
              ex.getErrorCode());
          maybeSetFirstError(new AzureException(ex));
        }
    }

    closed = true;

    // finally, throw the first exception raised if it has not
    // been thrown elsewhere.
    if (firstError.get() != null && !firstErrorThrown) {
      throw firstError.get();
    }
  }

  /**
   * Helper method used to generate the blockIDs. The algorithm used is similar
   * to the Azure storage SDK.
   */
  private void setBlocksCountAndBlockIdPrefix(List<BlockEntry> blockEntries) {

    if (nextBlockCount == UNSET_BLOCKS_COUNT && blockIdPrefix == null) {

      Random sequenceGenerator = new Random();

      String blockZeroBlockId = (!blockEntries.isEmpty())
          ? blockEntries.get(0).getId()
          : "";
      String prefix = UUID.randomUUID().toString() + "-";
      String sampleNewerVersionBlockId = generateNewerVersionBlockId(prefix,
          0);

      if (!blockEntries.isEmpty()
          && blockZeroBlockId.length() < sampleNewerVersionBlockId.length()) {

        // If blob has already been created with 2.2.0, append subsequent blocks
        // with older version (2.2.0) blockId compute nextBlockCount, the way it
        // was done before; and don't use blockIdPrefix
        this.blockIdPrefix = "";
        nextBlockCount = (long) (sequenceGenerator.nextInt(Integer.MAX_VALUE))
            + sequenceGenerator.nextInt(
                Integer.MAX_VALUE - MAX_BLOCK_COUNT);
        nextBlockCount += blockEntries.size();

      } else {

        // If there are no existing blocks, create the first block with newer
        // version (4.2.0) blockId. If blob has already been created with 4.2.0,
        // append subsequent blocks with newer version (4.2.0) blockId
        this.blockIdPrefix = prefix;
        nextBlockCount = blockEntries.size();
      }
    }
  }

  /**
   * Helper method that generates the next block id for uploading a block to
   * azure storage.
   * @return String representing the block ID generated.
   * @throws IOException if the stream is in invalid state
   */
  private String generateBlockId() throws IOException {

    if (nextBlockCount == UNSET_BLOCKS_COUNT || blockIdPrefix == null) {
      throw new AzureException(
            "Append Stream in invalid state. nextBlockCount not set correctly");
    }

    return (!blockIdPrefix.isEmpty())
        ? generateNewerVersionBlockId(blockIdPrefix, nextBlockCount++)
        : generateOlderVersionBlockId(nextBlockCount++);
  }

  /**
   * Helper method that generates an older (2.2.0) version blockId.
   * @return String representing the block ID generated.
   */
  private String generateOlderVersionBlockId(long id) {

    byte[] blockIdInBytes = new byte[8];
    for (int m = 0; m < 8; m++) {
      blockIdInBytes[7 - m] = (byte) ((id >> (8 * m)) & 0xFF);
    }

    return new String(
            Base64.encodeBase64(blockIdInBytes),
            StandardCharsets.UTF_8);
  }

  /**
   * Helper method that generates an newer (4.2.0) version blockId.
   * @return String representing the block ID generated.
   */
  private String generateNewerVersionBlockId(String prefix, long id) {

    String blockIdSuffix  = String.format("%06d", id);
    byte[] blockIdInBytes =
            (prefix + blockIdSuffix).getBytes(StandardCharsets.UTF_8);
    return new String(Base64.encodeBase64(blockIdInBytes), StandardCharsets.UTF_8);
  }

  /**
   * This is shared between upload block Runnable and CommitBlockList. The
   * method captures retry logic
   * @param blockId block name
   * @param dataPayload block content
   */
  private void writeBlockRequestInternal(String blockId,
                                         ByteBuffer dataPayload,
                                         boolean bufferPoolBuffer) {
    IOException lastLocalException = null;

    int uploadRetryAttempts = 0;
    while (uploadRetryAttempts < MAX_BLOCK_UPLOAD_RETRIES) {
      try {
        long startTime = System.nanoTime();

        blob.uploadBlock(blockId, accessCondition, new ByteArrayInputStream(
            dataPayload.array()), dataPayload.position(),
            new BlobRequestOptions(), opContext);

        LOG.debug("upload block finished for {} ms. block {} ",
            TimeUnit.NANOSECONDS.toMillis(
                System.nanoTime() - startTime), blockId);
        break;

      } catch(Exception ioe) {
        LOG.debug("Encountered exception during uploading block for Blob {}"
            + " Exception : {}", key, ioe);
        uploadRetryAttempts++;
        lastLocalException = new AzureException(
            "Encountered Exception while uploading block: " + ioe, ioe);
        try {
          Thread.sleep(
              BLOCK_UPLOAD_RETRY_INTERVAL * (uploadRetryAttempts + 1));
        } catch(InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    if (bufferPoolBuffer) {
      poolReadyByteBuffers.putBuffer(dataPayload);
    }

    if (uploadRetryAttempts == MAX_BLOCK_UPLOAD_RETRIES) {
      maybeSetFirstError(lastLocalException);
    }
  }

  /**
   * Set {@link #firstError} to the exception if it is not already set.
   * @param exception exception to save
   */
  private void maybeSetFirstError(IOException exception) {
    firstError.compareAndSet(null, exception);
  }


  /**
   * Throw the first error caught if it has not been raised already
   * @throws IOException if one is caught and needs to be thrown.
   */
  private void maybeThrowFirstError() throws IOException {
    if (firstError.get() != null) {
      firstErrorThrown = true;
      throw firstError.get();
    }
  }

  /**
   * Write block list. The method captures retry logic
   */
  private void writeBlockListRequestInternal() {

    IOException lastLocalException = null;

    int uploadRetryAttempts = 0;
    while (uploadRetryAttempts < MAX_BLOCK_UPLOAD_RETRIES) {
      try {

        long startTime = System.nanoTime();

        blob.commitBlockList(blockEntries, accessCondition,
            new BlobRequestOptions(), opContext);

        LOG.debug("Upload block list took {} ms for blob {} ",
                TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - startTime), key);
        break;

      } catch(Exception ioe) {
        LOG.debug("Encountered exception during uploading block for Blob {}"
            + " Exception : {}", key, ioe);
        uploadRetryAttempts++;
        lastLocalException = new AzureException(
            "Encountered Exception while uploading block: " + ioe, ioe);
        try {
          Thread.sleep(
              BLOCK_UPLOAD_RETRY_INTERVAL * (uploadRetryAttempts + 1));
        } catch(InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    if (uploadRetryAttempts == MAX_BLOCK_UPLOAD_RETRIES) {
      maybeSetFirstError(lastLocalException);
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
      t.setName(String.format("%s-%d", THREAD_ID_PREFIX,
          threadSequenceNumber.getAndIncrement()));
      return t;
    }
  }

  /**
   * Upload block commands.
   */
  private class UploadBlockCommand extends UploadCommand {

    // the block content for upload
    private final ByteBuffer payload;

    // description of the block
    private final BlockEntry entry;

    UploadBlockCommand(String blockId, ByteBuffer payload) {

      super(blobLength);

      BlockEntry blockEntry = new BlockEntry(blockId);
      blockEntry.setSize(payload.position());
      blockEntry.setSearchMode(BlockSearchMode.LATEST);

      this.payload = payload;
      this.entry = blockEntry;

      uncommittedBlockEntries.add(blockEntry);
    }

    /**
     * Execute command.
     */
    void execute() throws InterruptedException {

      uploadingSemaphore.acquire(1);
      writeBlockRequestInternal(entry.getId(), payload, true);
      uploadingSemaphore.release(1);

    }

    void dump() {
      LOG.debug("upload block {} size: {} for blob {}",
          entry.getId(),
          entry.getSize(),
          key);
    }
  }

  /**
   * Upload blob block list commands.
   */
  private class UploadBlockListCommand extends UploadCommand {

    private BlockEntry lastBlock = null;

    UploadBlockListCommand() {
      super(blobLength);

      if (!uncommittedBlockEntries.isEmpty()) {
        lastBlock = uncommittedBlockEntries.getLast();
      }
    }

    void awaitAsDependent() throws InterruptedException {
      // empty. later commit block does not need to wait previous commit block
      // lists.
    }

    void dump() {
      LOG.debug("commit block list with {} blocks for blob {}",
          uncommittedBlockEntries.size(), key);
    }

    /**
     * Execute command.
     */
    public void execute() throws InterruptedException, IOException {

      if (committedBlobLength.get() >= getCommandBlobOffset()) {
        LOG.debug("commit already applied for {}", key);
        return;
      }

      if (lastBlock == null) {
        LOG.debug("nothing to commit for {}", key);
        return;
      }

      LOG.debug("active commands: {} for {}", activeBlockCommands.size(), key);

      for (UploadCommand activeCommand : activeBlockCommands) {
        if (activeCommand.getCommandBlobOffset() < getCommandBlobOffset()) {
          activeCommand.dump();
          activeCommand.awaitAsDependent();
        } else {
          break;
        }
      }

      // stop all uploads until the block list is committed
      uploadingSemaphore.acquire(MAX_NUMBER_THREADS_IN_THREAD_POOL);

      BlockEntry uncommittedBlock;
      do  {
        uncommittedBlock = uncommittedBlockEntries.poll();
        blockEntries.add(uncommittedBlock);
      } while (uncommittedBlock != lastBlock);

      if (blockEntries.size() > activateCompactionBlockCount) {
        LOG.debug("Block compaction: activated with {} blocks for {}",
            blockEntries.size(), key);

        // Block compaction
        long startCompaction = System.nanoTime();
        blockCompaction();
        LOG.debug("Block compaction finished for {} ms with {} blocks for {}",
                TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - startCompaction),
                blockEntries.size(), key);
      }

      writeBlockListRequestInternal();

      uploadingSemaphore.release(MAX_NUMBER_THREADS_IN_THREAD_POOL);

      // remove blocks previous commands
      for (Iterator<UploadCommand> it = activeBlockCommands.iterator();
           it.hasNext();) {
        UploadCommand activeCommand = it.next();
        if (activeCommand.getCommandBlobOffset() <= getCommandBlobOffset()) {
          it.remove();
        } else {
          break;
        }
      }

      committedBlobLength.set(getCommandBlobOffset());
    }

    /**
     * Internal output stream with read access to the internal buffer.
     */
    private class ByteArrayOutputStreamInternal extends ByteArrayOutputStream {

      ByteArrayOutputStreamInternal(int size) {
        super(size);
      }

      byte[] getByteArray() {
        return buf;
      }
    }

    /**
     * Block compaction process.
     *
     * Block compaction is only enabled when the number of blocks exceeds
     * activateCompactionBlockCount. The algorithm searches for the longest
     * segment [b..e) where (e-b) > 2 && |b| + |b+1| ... |e-1| < maxBlockSize
     * such that size(b1) + size(b2) + ... + size(bn) < maximum-block-size.
     * It then downloads the blocks in the sequence, concatenates the data to
     * form a single block, uploads this new block, and updates the block
     * list to replace the sequence of blocks with the new block.
     */
    private void blockCompaction() throws IOException {
      //current segment [segmentBegin, segmentEnd) and file offset/size of the
      // current segment
      int segmentBegin = 0, segmentEnd = 0;
      long segmentOffsetBegin = 0, segmentOffsetEnd = 0;

      //longest segment [maxSegmentBegin, maxSegmentEnd) and file offset/size of
      // the longest segment
      int maxSegmentBegin = 0, maxSegmentEnd = 0;
      long maxSegmentOffsetBegin = 0, maxSegmentOffsetEnd = 0;

      for (BlockEntry block : blockEntries) {
        segmentEnd++;
        segmentOffsetEnd += block.getSize();
        if (segmentOffsetEnd - segmentOffsetBegin > maxBlockSize.get()) {
          if (segmentEnd - segmentBegin > 2) {
            if (maxSegmentEnd - maxSegmentBegin < segmentEnd - segmentBegin) {
              maxSegmentBegin = segmentBegin;
              maxSegmentEnd = segmentEnd;
              maxSegmentOffsetBegin = segmentOffsetBegin;
              maxSegmentOffsetEnd = segmentOffsetEnd - block.getSize();
            }
          }
          segmentBegin = segmentEnd - 1;
          segmentOffsetBegin = segmentOffsetEnd - block.getSize();
        }
      }

      if (maxSegmentEnd - maxSegmentBegin > 1) {

        LOG.debug("Block compaction: {} blocks for {}",
            maxSegmentEnd - maxSegmentBegin, key);

        // download synchronously all the blocks from the azure storage
        ByteArrayOutputStreamInternal blockOutputStream
            = new ByteArrayOutputStreamInternal(maxBlockSize.get());

        try {
          long length = maxSegmentOffsetEnd - maxSegmentOffsetBegin;
          blob.downloadRange(maxSegmentOffsetBegin, length, blockOutputStream,
              new BlobRequestOptions(), opContext);
        } catch(StorageException ex) {
          LOG.error(
              "Storage exception encountered during block compaction phase"
                  + " : {} Storage Exception : {} Error Code: {}",
              key, ex, ex.getErrorCode());
          throw new AzureException(
              "Encountered Exception while committing append blocks " + ex, ex);
        }

        // upload synchronously new block to the azure storage
        String blockId = generateBlockId();

        ByteBuffer byteBuffer = ByteBuffer.wrap(
            blockOutputStream.getByteArray());
        byteBuffer.position(blockOutputStream.size());

        writeBlockRequestInternal(blockId, byteBuffer, false);

        // replace blocks from the longest segment with new block id
        blockEntries.subList(maxSegmentBegin + 1, maxSegmentEnd - 1).clear();
        BlockEntry newBlock = blockEntries.get(maxSegmentBegin);
        newBlock.setId(blockId);
        newBlock.setSearchMode(BlockSearchMode.LATEST);
        newBlock.setSize(maxSegmentOffsetEnd - maxSegmentOffsetBegin);
      }
    }
  }

  /**
   * Prepare block upload command and queue the command in thread pool executor.
   */
  private synchronized void addBlockUploadCommand() throws IOException {

    maybeThrowFirstError();

    if (blobExist && lease.isFreed()) {
      throw new AzureException(String.format(
          "Attempting to upload a block on blob : %s "
              + " that does not have lease on the Blob. Failing upload", key));
    }

    int blockSize = outBuffer.position();
    if (blockSize > 0) {
      UploadCommand command = new UploadBlockCommand(generateBlockId(),
          outBuffer);
      activeBlockCommands.add(command);

      blobLength += blockSize;
      outBuffer = poolReadyByteBuffers.getBuffer(false, maxBlockSize.get());

      ioThreadPool.execute(new WriteRequest(command));

    }
  }

  /**
   * Prepare block list commit command and queue the command in thread pool
   * executor.
   */
  private synchronized UploadCommand addFlushCommand() throws IOException {

    maybeThrowFirstError();

    if (blobExist && lease.isFreed()) {
      throw new AzureException(
          String.format("Attempting to upload block list on blob : %s"
              + " that does not have lease on the Blob. Failing upload", key));
    }

    UploadCommand command = new UploadBlockListCommand();
    activeBlockCommands.add(command);

    ioThreadPool.execute(new WriteRequest(command));

    return command;
  }

  /**
   * Runnable instance that uploads the block of data to azure storage.
   */
  private class WriteRequest implements Runnable {
    private final UploadCommand command;

    WriteRequest(UploadCommand command) {
      this.command = command;
    }

    @Override
    public void run() {

      try {
        command.dump();
        long startTime = System.nanoTime();
        command.execute();
        command.setCompleted();
        LOG.debug("command finished for {} ms",
            TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - startTime));
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      } catch (Exception ex) {
        LOG.debug(
                "Encountered exception during execution of command for Blob :"
                        + " {} Exception : {}", key, ex);
        firstError.compareAndSet(null, new AzureException(ex));
      }
    }
  }
}
