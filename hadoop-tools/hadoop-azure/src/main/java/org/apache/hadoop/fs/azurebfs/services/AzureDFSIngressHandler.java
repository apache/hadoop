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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.store.DataBlocks;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPEND_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DFS_APPEND;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DFS_FLUSH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;

/**
 * The BlobFsOutputStream for Rest AbfsClient.
 */
public class AzureDFSIngressHandler extends AzureIngressHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsOutputStream.class);

  private AzureDFSBlockManager dfsBlockManager;

  private final AbfsDfsClient dfsClient;

  private String eTag;

  /**
   * Constructs an AzureDFSIngressHandler.
   *
   * @param abfsOutputStream the AbfsOutputStream instance.
   * @param clientHandler the AbfsClientHandler instance.
   */
  public AzureDFSIngressHandler(AbfsOutputStream abfsOutputStream,
      AbfsClientHandler clientHandler) {
    super(abfsOutputStream);
    this.dfsClient = clientHandler.getDfsClient();
  }

  /**
   * Constructs an AzureDFSIngressHandler with specified parameters.
   *
   * @param abfsOutputStream the AbfsOutputStream.
   * @param blockFactory the block factory.
   * @param bufferSize the buffer size.
   * @param eTag the eTag.
   * @param clientHandler the client handler.
   */
  public AzureDFSIngressHandler(AbfsOutputStream abfsOutputStream,
      DataBlocks.BlockFactory blockFactory,
      int bufferSize, String eTag, AbfsClientHandler clientHandler) {
    this(abfsOutputStream, clientHandler);
    this.eTag = eTag;
    this.dfsBlockManager = new AzureDFSBlockManager(abfsOutputStream,
        blockFactory, bufferSize);
    LOG.trace(
        "Created a new DFSIngress Handler for AbfsOutputStream instance {} for path {}",
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
      TracingContext tracingContext) throws IOException {
    TracingContext tracingContextAppend = new TracingContext(tracingContext);
    String threadIdStr = String.valueOf(Thread.currentThread().getId());
    if (tracingContextAppend.getIngressHandler().equals(EMPTY_STRING)) {
      tracingContextAppend.setIngressHandler(DFS_APPEND + " T " + threadIdStr);
      tracingContextAppend.setPosition(
          String.valueOf(blockToUpload.getOffset()));
    }
    LOG.trace("Starting remote write for block with offset {} and path {}",
        blockToUpload.getOffset(),
        getAbfsOutputStream().getPath());
    return getClient().append(getAbfsOutputStream().getPath(),
        uploadData.toByteArray(), reqParams,
        getAbfsOutputStream().getCachedSasTokenString(),
        getAbfsOutputStream().getContextEncryptionAdapter(),
        tracingContextAppend);
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
  @Override
  protected AbfsRestOperation remoteAppendBlobWrite(String path, DataBlocks.BlockUploadData uploadData,
      AbfsBlock block, AppendRequestParameters reqParams,
      TracingContext tracingContext) throws IOException {
    return remoteWrite(block, uploadData, reqParams, tracingContext);
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
    TracingContext tracingContextFlush = new TracingContext(tracingContext);
    if (tracingContextFlush.getIngressHandler().equals(EMPTY_STRING)) {
      tracingContextFlush.setIngressHandler(DFS_FLUSH);
      tracingContextFlush.setPosition(String.valueOf(offset));
    }
    LOG.trace("Flushing data at offset {} and path {}", offset, getAbfsOutputStream().getPath());
    return getClient()
        .flush(getAbfsOutputStream().getPath(), offset, retainUncommitedData,
            isClose,
            getAbfsOutputStream().getCachedSasTokenString(), leaseId,
            getAbfsOutputStream().getContextEncryptionAdapter(),
            tracingContextFlush);
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
    AbfsBlock activeBlock = dfsBlockManager.getActiveBlock();

    // No data, return immediately.
    if (!getAbfsOutputStream().hasActiveBlockDataToUpload()) {
      return;
    }

    // Prepare data for upload.
    final int bytesLength = activeBlock.dataSize();
    DataBlocks.BlockUploadData uploadData = activeBlock.startUpload();

    // Clear active block and update statistics.
    if (dfsBlockManager.hasActiveBlock()) {
      dfsBlockManager.clearActiveBlock();
    }
    getAbfsOutputStream().getOutputStreamStatistics().writeCurrentBuffer();
    getAbfsOutputStream().getOutputStreamStatistics().bytesToUpload(bytesLength);

    // Update the stream position.
    final long offset = getAbfsOutputStream().getPosition();
    getAbfsOutputStream().setPosition(offset + bytesLength);

    // Perform the upload within a performance tracking context.
    try (AbfsPerfInfo perfInfo = new AbfsPerfInfo(
        dfsClient.getAbfsPerfTracker(),
        "writeCurrentBufferToService", APPEND_ACTION)) {
      LOG.trace("Writing current buffer to service at offset {} and path {}", offset, getAbfsOutputStream().getPath());
      AppendRequestParameters reqParams = new AppendRequestParameters(
          offset, 0, bytesLength, AppendRequestParameters.Mode.APPEND_MODE,
          true, getAbfsOutputStream().getLeaseId(), getAbfsOutputStream().isExpectHeaderEnabled());

      // Perform the remote write operation.
      AbfsRestOperation op = remoteWrite(activeBlock, uploadData, reqParams,
          new TracingContext(getAbfsOutputStream().getTracingContext()));

      // Update the SAS token and log the successful upload.
      getAbfsOutputStream().getCachedSasToken().update(op.getSasToken());
      getAbfsOutputStream().getOutputStreamStatistics().uploadSuccessful(bytesLength);

      // Register performance information.
      perfInfo.registerResult(op.getResult());
      perfInfo.registerSuccess(true);
    } catch (Exception ex) {
      LOG.error("Failed to upload current buffer of length {} and path {}", bytesLength, getAbfsOutputStream().getPath(), ex);
      getAbfsOutputStream().getOutputStreamStatistics().uploadFailed(bytesLength);
      getAbfsOutputStream().failureWhileSubmit(ex);
    } finally {
      // Ensure the upload data stream is closed.
      IOUtils.closeStreams(uploadData, activeBlock);
    }
  }

  /**
   * Gets the block manager.
   *
   * @return the block manager.
   */
  @Override
  public AzureBlockManager getBlockManager() {
    return dfsBlockManager;
  }

  /**
   * Gets the dfs client.
   *
   * @return the dfs client.
   */
  @Override
  public AbfsDfsClient getClient() {
    return dfsClient;
  }

  /**
   * Gets the eTag value of the blob.
   *
   * @return the eTag.
   */
  @Override
  public String getETag() {
    return eTag;
  }
}
