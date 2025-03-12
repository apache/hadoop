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

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidIngressServiceException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.store.DataBlocks;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPEND_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FALLBACK_APPEND;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FALLBACK_FLUSH;

/**
 * Handles the fallback mechanism for Azure Blob Ingress operations.
 */
public class AzureDfsToBlobIngressFallbackHandler extends AzureDFSIngressHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsOutputStream.class);

  private final AzureBlobBlockManager blobBlockManager;

  private final String eTag;

  private final Lock lock = new ReentrantLock();

  /**
   * Constructs an AzureDfsToBlobIngressFallbackHandler.
   *
   * @param abfsOutputStream the AbfsOutputStream.
   * @param blockFactory the block factory.
   * @param bufferSize the buffer size.
   * @param eTag the eTag.
   * @param clientHandler the client handler.
   * @throws AzureBlobFileSystemException if an error occurs.
   */
  public AzureDfsToBlobIngressFallbackHandler(AbfsOutputStream abfsOutputStream,
      DataBlocks.BlockFactory blockFactory,
      int bufferSize, String eTag, AbfsClientHandler clientHandler) throws AzureBlobFileSystemException {
    super(abfsOutputStream, clientHandler);
    this.eTag = eTag;
    this.blobBlockManager = new AzureBlobBlockManager(abfsOutputStream,
        blockFactory, bufferSize);
    LOG.trace(
        "Created a new BlobFallbackIngress Handler for AbfsOutputStream instance {} for path {}",
        abfsOutputStream.getStreamID(), abfsOutputStream.getPath());
  }

  /**
   * Buffers data into the specified block.
   *
   * @param block the block to buffer data into.
   * @param data  the data to be buffered.
   * @param off   the start offset in the data.
   * @param length the number of bytes to buffer.
   * @return the number of bytes buffered.
   * @throws IOException if an I/O error occurs.
   */
  @Override
  public int bufferData(AbfsBlock block,
      final byte[] data,
      final int off,
      final int length) throws IOException {
    LOG.trace("Buffering data of length {} to block at offset {}", length, off);
    return super.bufferData(block, data, off, length);
  }

  /**
   * Performs a remote write operation.
   *
   * @param blockToUpload the block to upload.
   * @param uploadData    the data to upload.
   * @param reqParams     the request parameters.
   * @param tracingContext the tracing context.
   * @return the resulting AbfsRestOperation.
   * @throws IOException if an I/O error occurs.
   */
  @Override
  protected AbfsRestOperation remoteWrite(AbfsBlock blockToUpload,
      DataBlocks.BlockUploadData uploadData,
      AppendRequestParameters reqParams,
      TracingContext tracingContext) throws IOException {
    AbfsRestOperation op;
    TracingContext tracingContextAppend = new TracingContext(tracingContext);
    String threadIdStr = String.valueOf(Thread.currentThread().getId());
    tracingContextAppend.setIngressHandler(FALLBACK_APPEND + " T " + threadIdStr);
    tracingContextAppend.setPosition(String.valueOf(blockToUpload.getOffset()));
    try {
      op = super.remoteWrite(blockToUpload, uploadData, reqParams,
          tracingContextAppend);
      blobBlockManager.updateEntry(blockToUpload);
    } catch (AbfsRestOperationException ex) {
      if (shouldIngressHandlerBeSwitched(ex)) {
        LOG.error("Error in remote write requiring handler switch for path {}", getAbfsOutputStream().getPath(), ex);
        throw getIngressHandlerSwitchException(ex);
      }
      LOG.error("Error in remote write for path {} and offset {}", getAbfsOutputStream().getPath(),
          blockToUpload.getOffset(), ex);
      throw ex;
    }
    return op;
  }

  /**
   * Flushes data to the remote store.
   *
   * @param offset               the offset to flush.
   * @param retainUncommitedData whether to retain uncommitted data.
   * @param isClose              whether this is a close operation.
   * @param leaseId              the lease ID.
   * @param tracingContext       the tracing context.
   * @return the resulting AbfsRestOperation.
   * @throws IOException if an I/O error occurs.
   */
  @Override
  protected synchronized AbfsRestOperation remoteFlush(final long offset,
      final boolean retainUncommitedData,
      final boolean isClose,
      final String leaseId,
      TracingContext tracingContext) throws IOException {
    AbfsRestOperation op;
    if (!blobBlockManager.hasBlocksToCommit()) {
      return null;
    }
    try {
      TracingContext tracingContextFlush = new TracingContext(tracingContext);
      tracingContextFlush.setIngressHandler(FALLBACK_FLUSH);
      tracingContextFlush.setPosition(String.valueOf(offset));
      op = super.remoteFlush(offset, retainUncommitedData, isClose, leaseId,
          tracingContextFlush);
    } catch (AbfsRestOperationException ex) {
      if (shouldIngressHandlerBeSwitched(ex)) {
        LOG.error("Error in remote flush requiring handler switch for path {}", getAbfsOutputStream().getPath(), ex);
        throw getIngressHandlerSwitchException(ex);
      }
      LOG.error("Error in remote flush for path {} and offset {}", getAbfsOutputStream().getPath(), offset, ex);
      throw ex;
    }
    return op;
  }

  /**
   * Gets the block manager.
   *
   * @return the block manager.
   */
  @Override
  public AzureBlockManager getBlockManager() {
    return blobBlockManager;
  }

  /**
   * Gets the eTag value of the blob.
   *
   * @return the eTag.
   */
  @VisibleForTesting
  public String getETag() {
    lock.lock();
    try {
      return eTag;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Appending the current active data block to the service. Clearing the active
   * data block and releasing all buffered data.
   *
   * @throws IOException if there is any failure while starting an upload for
   *                     the data block or while closing the BlockUploadData.
   */
  @Override
  protected void writeAppendBlobCurrentBufferToService() throws IOException {
    AbfsBlock activeBlock = blobBlockManager.getActiveBlock();

    // No data, return immediately.
    if (!getAbfsOutputStream().hasActiveBlockDataToUpload()) {
      return;
    }

    // Prepare data for upload.
    final int bytesLength = activeBlock.dataSize();
    DataBlocks.BlockUploadData uploadData = activeBlock.startUpload();

    // Clear active block and update statistics.
    if (blobBlockManager.hasActiveBlock()) {
      blobBlockManager.clearActiveBlock();
    }
    getAbfsOutputStream().getOutputStreamStatistics().writeCurrentBuffer();
    getAbfsOutputStream().getOutputStreamStatistics().bytesToUpload(bytesLength);

    // Update the stream position.
    final long offset = getAbfsOutputStream().getPosition();
    getAbfsOutputStream().setPosition(offset + bytesLength);

    // Perform the upload within a performance tracking context.
    try (AbfsPerfInfo perfInfo = new AbfsPerfInfo(
        getClient().getAbfsPerfTracker(),
        "writeCurrentBufferToService", APPEND_ACTION)) {
      LOG.trace("Writing current buffer to service at offset {} and path {}", offset, getAbfsOutputStream().getPath());
      AppendRequestParameters reqParams = new AppendRequestParameters(
          offset, 0, bytesLength, AppendRequestParameters.Mode.APPEND_MODE,
          true, getAbfsOutputStream().getLeaseId(), getAbfsOutputStream().isExpectHeaderEnabled());

      // Perform the remote write operation.
      AbfsRestOperation op;
      try {
        op = remoteAppendBlobWrite(getAbfsOutputStream().getPath(), uploadData,
            activeBlock, reqParams,
            new TracingContext(getAbfsOutputStream().getTracingContext()));
      } catch (InvalidIngressServiceException ex) {
        LOG.debug("InvalidIngressServiceException caught for path: {}, switching handler and retrying remoteAppendBlobWrite.", getAbfsOutputStream().getPath());
        getAbfsOutputStream().switchHandler();
        op = getAbfsOutputStream().getIngressHandler()
            .remoteAppendBlobWrite(getAbfsOutputStream().getPath(), uploadData,
                activeBlock, reqParams,
                new TracingContext(getAbfsOutputStream().getTracingContext()));
      } finally {
        // Ensure the upload data stream is closed.
        IOUtils.closeStreams(uploadData, activeBlock);
      }

      if (op != null) {
        // Update the SAS token and log the successful upload.
        getAbfsOutputStream().getCachedSasToken().update(op.getSasToken());
        getAbfsOutputStream().getOutputStreamStatistics()
            .uploadSuccessful(bytesLength);

        // Register performance information.
        perfInfo.registerResult(op.getResult());
        perfInfo.registerSuccess(true);
      }
    }
  }
}
