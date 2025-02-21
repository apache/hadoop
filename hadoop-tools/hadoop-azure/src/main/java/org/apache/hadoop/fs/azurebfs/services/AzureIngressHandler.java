/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidIngressServiceException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.store.DataBlocks;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.BLOB_OPERATION_NOT_SUPPORTED;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.INVALID_APPEND_OPERATION;

/**
 * Abstract base class for handling ingress operations for Azure Data Lake Storage (ADLS).
 */
public abstract class AzureIngressHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsOutputStream.class);

  /** The output stream associated with this handler */
  private AbfsOutputStream abfsOutputStream;

  /**
   * Constructs an AzureIngressHandler.
   *
   * @param abfsOutputStream the output stream associated with this handler
   */
  protected AzureIngressHandler(AbfsOutputStream abfsOutputStream) {
    this.abfsOutputStream = abfsOutputStream;
  }

  /**
   * Gets the AbfsOutputStream associated with this handler.
   *
   * @return the AbfsOutputStream
   */
  public AbfsOutputStream getAbfsOutputStream() {
    return abfsOutputStream;
  }

  /**
   * Sets the AbfsOutputStream associated with this handler.
   *
   * @param abfsOutputStream the AbfsOutputStream to set
   */
  public void setAbfsOutputStream(final AbfsOutputStream abfsOutputStream) {
    this.abfsOutputStream = abfsOutputStream;
  }

  /**
   * Gets the eTag value of the blob.
   *
   * @return the eTag.
   */
  public abstract String getETag();

  /**
   * Buffers data into the specified block.
   *
   * @param block the block to buffer data into
   * @param data the data to buffer
   * @param off the start offset in the data
   * @param length the number of bytes to buffer
   * @return the number of bytes buffered
   * @throws IOException if an I/O error occurs
   */
  protected abstract int bufferData(AbfsBlock block,
      byte[] data, int off, int length) throws IOException;

  /**
   * Performs a remote write operation to upload a block.
   *
   * @param blockToUpload the block to upload
   * @param uploadData the data to upload
   * @param reqParams the request parameters for the append operation
   * @param tracingContext the tracing context
   * @return the result of the REST operation
   * @throws IOException if an I/O error occurs
   */
  protected abstract AbfsRestOperation remoteWrite(AbfsBlock blockToUpload,
      DataBlocks.BlockUploadData uploadData,
      AppendRequestParameters reqParams,
      TracingContext tracingContext) throws IOException;

  /**
   * Performs a remote flush operation.
   *
   * @param offset the offset to flush to
   * @param retainUncommittedData whether to retain uncommitted data
   * @param isClose whether this is a close operation
   * @param leaseId the lease ID
   * @param tracingContext the tracing context
   * @return the result of the REST operation
   * @throws IOException if an I/O error occurs
   */
  protected abstract AbfsRestOperation remoteFlush(long offset,
      boolean retainUncommittedData,
      boolean isClose,
      String leaseId,
      TracingContext tracingContext) throws IOException;

  /**
   * Writes the current buffer to the service for an append blob.
   *
   * @throws IOException if an I/O error occurs
   */
  protected abstract void writeAppendBlobCurrentBufferToService()
      throws IOException;

  /**
   * Abstract method to perform a remote write operation for appending data to an append blob in Azure Blob Storage.
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
  protected abstract AbfsRestOperation remoteAppendBlobWrite(String path,
      DataBlocks.BlockUploadData uploadData,
      AbfsBlock block,
      AppendRequestParameters reqParams,
      TracingContext tracingContext) throws IOException;

  /**
   * Determines if the ingress handler should be switched based on the given exception.
   *
   * @param ex the exception that occurred
   * @return true if the ingress handler should be switched, false otherwise
   */
  protected boolean shouldIngressHandlerBeSwitched(AbfsRestOperationException ex) {
    if (ex == null || ex.getErrorCode() == null) {
      return false;
    }
    String errorCode = ex.getErrorCode().getErrorCode();
    if (errorCode != null) {
      return ex.getStatusCode() == HTTP_CONFLICT
          && (Objects.equals(errorCode, AzureServiceErrorCode.BLOB_OPERATION_NOT_SUPPORTED.getErrorCode())
              || Objects.equals(errorCode, AzureServiceErrorCode.INVALID_APPEND_OPERATION.getErrorCode()));
    }
    return false;
  }

  /**
   * Constructs an InvalidIngressServiceException that includes the current handler class name in the exception message.
   *
   * @param e the original AbfsRestOperationException that triggered this exception.
   * @return an InvalidIngressServiceException with the status code, error code, original message, and handler class name.
   */
  protected InvalidIngressServiceException getIngressHandlerSwitchException(
      AbfsRestOperationException e) {
    if (e.getMessage().contains(BLOB_OPERATION_NOT_SUPPORTED)) {
      return new InvalidIngressServiceException(e.getStatusCode(),
          AzureServiceErrorCode.BLOB_OPERATION_NOT_SUPPORTED.getErrorCode(),
          BLOB_OPERATION_NOT_SUPPORTED + " " + getClass().getName(), e);
    } else {
      return new InvalidIngressServiceException(e.getStatusCode(),
          AzureServiceErrorCode.INVALID_APPEND_OPERATION.getErrorCode(),
          INVALID_APPEND_OPERATION + " " + getClass().getName(), e);
    }
  }

  /**
   * Gets the block manager associated with this handler.
   *
   * @return the block manager
   */
  protected abstract AzureBlockManager getBlockManager();

  /**
   * Gets the client associated with this handler.
   *
   * @return the block manager
   */
  public abstract AbfsClient getClient();
}
