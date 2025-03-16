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
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidIngressServiceException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobAppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.store.DataBlocks;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPEND_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOB_APPEND;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOB_FLUSH;
import static org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient.generateBlockListXml;

public class AzureBlobIngressHandler extends AzureIngressHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsOutputStream.class);

  private volatile String eTag;

  private final AzureBlobBlockManager blobBlockManager;

  private final AbfsBlobClient blobClient;

  private final AbfsClientHandler clientHandler;

  /**
   * Constructs an AzureBlobIngressHandler.
   *
   * @param abfsOutputStream the AbfsOutputStream.
   * @param blockFactory the block factory.
   * @param bufferSize the buffer size.
   * @param eTag the eTag.
   * @param clientHandler the client handler.
   * @param blockManager the block manager.
   * @throws AzureBlobFileSystemException if an error occurs.
   */
  public AzureBlobIngressHandler(AbfsOutputStream abfsOutputStream,
      DataBlocks.BlockFactory blockFactory,
      int bufferSize, String eTag, AbfsClientHandler clientHandler, AzureBlockManager blockManager)
      throws AzureBlobFileSystemException {
    super(abfsOutputStream);
    this.eTag = eTag;
    if (blockManager instanceof AzureBlobBlockManager) {
      this.blobBlockManager = (AzureBlobBlockManager) blockManager;
    } else {
      this.blobBlockManager = new AzureBlobBlockManager(abfsOutputStream,
          blockFactory, bufferSize);
    }
    this.clientHandler = clientHandler;
    this.blobClient = clientHandler.getBlobClient();
    LOG.trace("Created a new BlobIngress Handler for AbfsOutputStream instance {} for path {}",
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
  protected int bufferData(AbfsBlock block,
      final byte[] data,
      final int off,
      final int length)
      throws IOException {
    LOG.trace("Buffering data of length {} to block at offset {}", length, off);
    return block.write(data, off, length);
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
      TracingContext tracingContext)
      throws IOException {
    BlobAppendRequestParameters blobParams = new BlobAppendRequestParameters(blockToUpload.getBlockId(), getETag());
    reqParams.setBlobParams(blobParams);
    AbfsRestOperation op;
    String threadIdStr = String.valueOf(Thread.currentThread().getId());
    TracingContext tracingContextAppend = new TracingContext(tracingContext);
    tracingContextAppend.setIngressHandler(BLOB_APPEND + " T " + threadIdStr);
    tracingContextAppend.setPosition(String.valueOf(blockToUpload.getOffset()));
    try {
      LOG.trace("Starting remote write for block with ID {} and offset {}",
          blockToUpload.getBlockId(), blockToUpload.getOffset());
      op = getClient().append(getAbfsOutputStream().getPath(), uploadData.toByteArray(),
          reqParams,
          getAbfsOutputStream().getCachedSasTokenString(),
          getAbfsOutputStream().getContextEncryptionAdapter(),
          tracingContextAppend);
      blobBlockManager.updateEntry(blockToUpload);
    } catch (AbfsRestOperationException ex) {
      LOG.error("Error in remote write requiring handler switch for path {}", getAbfsOutputStream().getPath(), ex);
      if (shouldIngressHandlerBeSwitched(ex)) {
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
      TracingContext tracingContext)
      throws IOException {
    AbfsRestOperation op;
    if (getAbfsOutputStream().isAppendBlob()) {
      return null;
    }
    if (!blobBlockManager.hasBlocksToCommit()) {
      return null;
    }
    try {
      // Generate the xml with the list of blockId's to generate putBlockList call.
      String blockListXml = generateBlockListXml(
          blobBlockManager.getBlockIdToCommit());
      TracingContext tracingContextFlush = new TracingContext(tracingContext);
      tracingContextFlush.setIngressHandler(BLOB_FLUSH);
      tracingContextFlush.setPosition(String.valueOf(offset));
      LOG.trace("Flushing data at offset {} for path {}", offset, getAbfsOutputStream().getPath());
      op = getClient().flush(blockListXml.getBytes(StandardCharsets.UTF_8),
          getAbfsOutputStream().getPath(),
          isClose, getAbfsOutputStream().getCachedSasTokenString(), leaseId,
          getETag(), getAbfsOutputStream().getContextEncryptionAdapter(), tracingContextFlush);
      setETag(op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG));
    } catch (AbfsRestOperationException ex) {
      LOG.error("Error in remote flush requiring handler switch for path {}", getAbfsOutputStream().getPath(), ex);
      if (shouldIngressHandlerBeSwitched(ex)) {
        throw getIngressHandlerSwitchException(ex);
      }
      LOG.error("Error in remote flush for path {} and offset {}", getAbfsOutputStream().getPath(), offset, ex);
      throw ex;
    }
    return op;
  }

  /**
   * Method to perform a remote write operation for appending data to an append blob in Azure Blob Storage.
   *
   * <p>This method is intended to be implemented by subclasses to handle the specific
   * case of appending data to an append blob. It takes in the path of the append blob,
   * the data to be uploaded, the block of data, and additional parameters required for
   * the append operation.</p>
   *
   * @param path           The path of the append blob to which data is to be appended.
   * @param uploadData     The data to be uploaded as part of the append operation.
   * @param block          The block of data to append.
   * @param reqParams      The additional parameters required for the append operation.
   * @param tracingContext The tracing context for the operation.
   * @return An {@link AbfsRestOperation} object representing the remote write operation.
   * @throws IOException If an I/O error occurs during the append operation.
   */
  protected AbfsRestOperation remoteAppendBlobWrite(String path,
      DataBlocks.BlockUploadData uploadData,
      AbfsBlock block,
      AppendRequestParameters reqParams,
      TracingContext tracingContext) throws IOException {
    // Perform the remote append operation using the blob client.
    AbfsRestOperation op = null;
    try {
      op = blobClient.appendBlock(path, reqParams, uploadData.toByteArray(), tracingContext);
    } catch (AbfsRestOperationException ex) {
      LOG.error("Error in remote write requiring handler switch for path {}",
          getAbfsOutputStream().getPath(), ex);
      if (shouldIngressHandlerBeSwitched(ex)) {
        throw getIngressHandlerSwitchException(ex);
      }
      LOG.error("Error in remote write for path {} and offset {}",
          getAbfsOutputStream().getPath(),
          block.getOffset(), ex);
      throw ex;
    }
    return op;
  }

  /**
   * Sets the eTag of the blob.
   *
   * @param eTag the eTag to set.
   */
  void setETag(String eTag) {
    this.eTag = eTag;
  }

  /**
   * Gets the eTag value of the blob.
   *
   * @return the eTag.
   */
  @VisibleForTesting
  @Override
  public String getETag() {
    return eTag;
  }

  /**
   * Writes the current buffer to the service. .
   *
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
        blobClient.getAbfsPerfTracker(),
        "writeCurrentBufferToService", APPEND_ACTION)) {
      LOG.trace("Writing current buffer to service at offset {} and path {}", offset, getAbfsOutputStream().getPath());
      AppendRequestParameters reqParams = new AppendRequestParameters(
          offset, 0, bytesLength, AppendRequestParameters.Mode.APPEND_MODE,
          true, getAbfsOutputStream().getLeaseId(), getAbfsOutputStream().isExpectHeaderEnabled());

      AbfsRestOperation op;
      try {
        op = remoteAppendBlobWrite(getAbfsOutputStream().getPath(), uploadData,
            activeBlock, reqParams,
            new TracingContext(getAbfsOutputStream().getTracingContext()));
      } catch (InvalidIngressServiceException ex) {
        LOG.debug("InvalidIngressServiceException caught for path: {}, switching handler and retrying remoteAppendBlobWrite.",
            getAbfsOutputStream().getPath());
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
   * Gets the blob client.
   *
   * @return the blob client.
   */
  @Override
  public AbfsBlobClient getClient() {
    return blobClient;
  }

  @VisibleForTesting
  public AbfsClientHandler getClientHandler() {
    return clientHandler;
  }
}
