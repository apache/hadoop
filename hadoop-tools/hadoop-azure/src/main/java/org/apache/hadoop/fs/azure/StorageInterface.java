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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.EnumSet;
import java.util.HashMap;

import org.apache.hadoop.classification.InterfaceAudience;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryPolicyFactory;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.BlockListingFilter;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CopyState;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.blob.PageRange;

/**
 * This is a very thin layer over the methods exposed by the Windows Azure
 * Storage SDK that we need for WASB implementation. This base class has a real
 * implementation that just simply redirects to the SDK, and a memory-backed one
 * that's used for unit tests.
 *
 * IMPORTANT: all the methods here must remain very simple redirects since code
 * written here can't be properly unit tested.
 */
@InterfaceAudience.Private
abstract class StorageInterface {

  /**
   * Sets the timeout to use when making requests to the storage service.
   * <p>
   * The server timeout interval begins at the time that the complete request
   * has been received by the service, and the server begins processing the
   * response. If the timeout interval elapses before the response is returned
   * to the client, the operation times out. The timeout interval resets with
   * each retry, if the request is retried.
   * 
   * The default timeout interval for a request made via the service client is
   * 90 seconds. You can change this value on the service client by setting this
   * property, so that all subsequent requests made via the service client will
   * use the new timeout interval. You can also change this value for an
   * individual request, by setting the
   * {@link com.microsoft.azure.storage.RequestOptions#timeoutIntervalInMs}
   * property.
   * 
   * If you are downloading a large blob, you should increase the value of the
   * timeout beyond the default value.
   * 
   * @param timeoutInMs
   *          The timeout, in milliseconds, to use when making requests to the
   *          storage service.
   */
  public abstract void setTimeoutInMs(int timeoutInMs);

  /**
   * Sets the RetryPolicyFactory object to use when making service requests.
   * 
   * @param retryPolicyFactory
   *          the RetryPolicyFactory object to use when making service requests.
   */
  public abstract void setRetryPolicyFactory(
      final RetryPolicyFactory retryPolicyFactory);

  /**
   * Creates a new Blob service client.
   *
   * @param account cloud storage account.
   */
  public abstract void createBlobClient(CloudStorageAccount account);

  /**
   * Creates an instance of the <code>CloudBlobClient</code> class using the
   * specified Blob service endpoint.
   * 
   * @param baseUri
   *          A <code>java.net.URI</code> object that represents the Blob
   *          service endpoint used to create the client.
   */
  public abstract void createBlobClient(URI baseUri);

  /**
   * Creates an instance of the <code>CloudBlobClient</code> class using the
   * specified Blob service endpoint and account credentials.
   * 
   * @param baseUri
   *          A <code>java.net.URI</code> object that represents the Blob
   *          service endpoint used to create the client.
   * @param credentials
   *          A {@link StorageCredentials} object that represents the account
   *          credentials.
   */
  public abstract void createBlobClient(URI baseUri,
      StorageCredentials credentials);

  /**
   * Returns the credentials for the Blob service, as configured for the storage
   * account.
   * 
   * @return A {@link StorageCredentials} object that represents the credentials
   *         for this storage account.
   */
  public abstract StorageCredentials getCredentials();

  /**
   * Returns a reference to a {@link CloudBlobContainerWrapper} object that
   * represents the cloud blob container for the specified address.
   * 
   * @param name
   *          A <code>String</code> that represents the name of the container.
   * @return A {@link CloudBlobContainerWrapper} object that represents a
   *         reference to the cloud blob container.
   * 
   * @throws URISyntaxException
   *           If the resource URI is invalid.
   * @throws StorageException
   *           If a storage service error occurred.
   */
  public abstract CloudBlobContainerWrapper getContainerReference(String name)
      throws URISyntaxException, StorageException;

  /**
   * A thin wrapper over the
   * {@link com.microsoft.azure.storage.blob.CloudBlobDirectory} class
   * that simply redirects calls to the real object except in unit tests.
   */
  @InterfaceAudience.Private
  public abstract static class CloudBlobDirectoryWrapper implements
      ListBlobItem {
    /**
     * Returns the URI for this directory.
     * 
     * @return A <code>java.net.URI</code> object that represents the URI for
     *         this directory.
     */
    public abstract URI getUri();

    /**
     * Returns an enumerable collection of blob items whose names begin with the
     * specified prefix, using the specified flat or hierarchical option,
     * listing details options, request options, and operation context.
     * 
     * @param prefix
     *          A <code>String</code> that represents the prefix of the blob
     *          name.
     * @param useFlatBlobListing
     *          <code>true</code> to indicate that the returned list will be
     *          flat; <code>false</code> to indicate that the returned list will
     *          be hierarchical.
     * @param listingDetails
     *          A <code>java.util.EnumSet</code> object that contains
     *          {@link BlobListingDetails} values that indicate whether
     *          snapshots, metadata, and/or uncommitted blocks are returned.
     *          Committed blocks are always returned.
     * @param options
     *          A {@link BlobRequestOptions} object that specifies any
     *          additional options for the request. Specifying <code>null</code>
     *          will use the default request options from the associated service
     *          client ({@link com.microsoft.azure.storage.blob.CloudBlobClient}).
     * @param opContext
     *          An {@link OperationContext} object that represents the context
     *          for the current operation. This object is used to track requests
     *          to the storage service, and to provide additional runtime
     *          information about the operation.
     * 
     * @return An enumerable collection of {@link ListBlobItem} objects that
     *         represent the block items whose names begin with the specified
     *         prefix in this directory.
     * 
     * @throws StorageException
     *           If a storage service error occurred.
     * @throws URISyntaxException
     *           If the resource URI is invalid.
     */
    public abstract Iterable<ListBlobItem> listBlobs(String prefix,
        boolean useFlatBlobListing, EnumSet<BlobListingDetails> listingDetails,
        BlobRequestOptions options, OperationContext opContext)
        throws URISyntaxException, StorageException;
  }

  /**
   * A thin wrapper over the
   * {@link com.microsoft.azure.storage.blob.CloudBlobContainer} class
   * that simply redirects calls to the real object except in unit tests.
   */
  @InterfaceAudience.Private
  public abstract static class CloudBlobContainerWrapper {
    /**
     * Returns the name of the container.
     * 
     * @return A <code>String</code> that represents the name of the container.
     */
    public abstract String getName();

    /**
     * Returns a value that indicates whether the container exists, using the
     * specified operation context.
     * 
     * @param opContext
     *          An {@link OperationContext} object that represents the context
     *          for the current operation. This object is used to track requests
     *          to the storage service, and to provide additional runtime
     *          information about the operation.
     * 
     * @return <code>true</code> if the container exists, otherwise
     *         <code>false</code>.
     * 
     * @throws StorageException
     *           If a storage service error occurred.
     */
    public abstract boolean exists(OperationContext opContext)
        throws StorageException;

    /**
     * Returns the metadata for the container.
     * 
     * @return A <code>java.util.HashMap</code> object that represents the
     *         metadata for the container.
     */
    public abstract HashMap<String, String> getMetadata();

    /**
     * Sets the metadata for the container.
     * 
     * @param metadata
     *          A <code>java.util.HashMap</code> object that represents the
     *          metadata being assigned to the container.
     */
    public abstract void setMetadata(HashMap<String, String> metadata);

    /**
     * Downloads the container's attributes, which consist of metadata and
     * properties, using the specified operation context.
     * 
     * @param opContext
     *          An {@link OperationContext} object that represents the context
     *          for the current operation. This object is used to track requests
     *          to the storage service, and to provide additional runtime
     *          information about the operation.
     * 
     * @throws StorageException
     *           If a storage service error occurred.
     */
    public abstract void downloadAttributes(OperationContext opContext)
        throws StorageException;

    /**
     * Uploads the container's metadata using the specified operation context.
     *
     * @param opContext
     *          An {@link OperationContext} object that represents the context
     *          for the current operation. This object is used to track requests
     *          to the storage service, and to provide additional runtime
     *          information about the operation.
     *
     * @throws StorageException
     *           If a storage service error occurred.
     */
    public abstract void uploadMetadata(OperationContext opContext)
        throws StorageException;

    /**
     * Creates the container using the specified operation context.
     * 
     * @param opContext
     *          An {@link OperationContext} object that represents the context
     *          for the current operation. This object is used to track requests
     *          to the storage service, and to provide additional runtime
     *          information about the operation.
     * 
     * @throws StorageException
     *           If a storage service error occurred.
     */
    public abstract void create(OperationContext opContext)
        throws StorageException;

    /**
     * Returns a wrapper for a CloudBlobDirectory.
     * 
     * @param relativePath
     *          A <code>String</code> that represents the name of the directory,
     *          relative to the container
     * 
     * @throws StorageException
     *           If a storage service error occurred.
     * 
     * @throws URISyntaxException
     *           If URI syntax exception occurred.
     */
    public abstract CloudBlobDirectoryWrapper getDirectoryReference(
        String relativePath) throws URISyntaxException, StorageException;

    /**
     * Returns a wrapper for a CloudBlockBlob.
     * 
     * @param relativePath
     *          A <code>String</code> that represents the name of the blob,
     *          relative to the container
     * 
     * @throws StorageException
     *           If a storage service error occurred.
     * 
     * @throws URISyntaxException
     *           If URI syntax exception occurred.
     */
    public abstract CloudBlobWrapper getBlockBlobReference(
        String relativePath) throws URISyntaxException, StorageException;
  
    /**
     * Returns a wrapper for a CloudPageBlob.
     *
     * @param relativePath
     *            A <code>String</code> that represents the name of the blob, relative to the container 
     *
     * @throws StorageException
     *             If a storage service error occurred.
     * 
     * @throws URISyntaxException
     *             If URI syntax exception occurred.            
     */
    public abstract CloudBlobWrapper getPageBlobReference(String relativePath)
        throws URISyntaxException, StorageException;
  }
  
  
  /**
   * A thin wrapper over the {@link CloudBlob} class that simply redirects calls
   * to the real object except in unit tests.
   */
  @InterfaceAudience.Private
  public interface CloudBlobWrapper extends ListBlobItem {
    /**
     * Returns the URI for this blob.
     * 
     * @return A <code>java.net.URI</code> object that represents the URI for
     *         the blob.
     */
    URI getUri();

    /**
     * Returns the metadata for the blob.
     * 
     * @return A <code>java.util.HashMap</code> object that represents the
     *         metadata for the blob.
     */
    HashMap<String, String> getMetadata();

    /**
     * Sets the metadata for the blob.
     * 
     * @param metadata
     *          A <code>java.util.HashMap</code> object that contains the
     *          metadata being assigned to the blob.
     */
    void setMetadata(HashMap<String, String> metadata);

    /**
     * Copies an existing blob's contents, properties, and metadata to this instance of the <code>CloudBlob</code>
     * class, using the specified operation context.
     *
     * @param sourceBlob
     *            A <code>CloudBlob</code> object that represents the source blob to copy.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     * @throws URISyntaxException
     *
     */
    public abstract void startCopyFromBlob(CloudBlobWrapper sourceBlob,
        BlobRequestOptions options, OperationContext opContext, boolean overwriteDestination)
        throws StorageException, URISyntaxException;
    
    /**
     * Returns the blob's copy state.
     * 
     * @return A {@link CopyState} object that represents the copy state of the
     *         blob.
     */
    CopyState getCopyState();

    /**
     * Downloads a range of bytes from the blob to the given byte buffer, using the specified request options and
     * operation context.
     *
     * @param offset
     *            The byte offset to use as the starting point for the source.
     * @param length
     *            The number of bytes to read.
     * @param buffer
     *            The byte buffer, as an array of bytes, to which the blob bytes are downloaded.
     * @param bufferOffset
     *            The byte offset to use as the starting point for the target.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     */
    void downloadRange(final long offset, final long length,
        final OutputStream outStream, final BlobRequestOptions options,
        final OperationContext opContext)
            throws StorageException, IOException;

    /**
     * Deletes the blob using the specified operation context.
     * <p>
     * A blob that has snapshots cannot be deleted unless the snapshots are also
     * deleted. If a blob has snapshots, use the
     * {@link DeleteSnapshotsOption#DELETE_SNAPSHOTS_ONLY} or
     * {@link DeleteSnapshotsOption#INCLUDE_SNAPSHOTS} value in the
     * <code>deleteSnapshotsOption</code> parameter to specify how the snapshots
     * should be handled when the blob is deleted.
     * 
     * @param opContext
     *          An {@link OperationContext} object that represents the context
     *          for the current operation. This object is used to track requests
     *          to the storage service, and to provide additional runtime
     *          information about the operation.
     * 
     * @throws StorageException
     *           If a storage service error occurred.
     */
    void delete(OperationContext opContext, SelfRenewingLease lease)
        throws StorageException;

    /**
     * Checks to see if the blob exists, using the specified operation context.
     * 
     * @param opContext
     *          An {@link OperationContext} object that represents the context
     *          for the current operation. This object is used to track requests
     *          to the storage service, and to provide additional runtime
     *          information about the operation.
     * 
     * @return <code>true</code> if the blob exists, otherwise
     *         <code>false</code>.
     * 
     * @throws StorageException
     *           If a storage service error occurred.
     */
    boolean exists(OperationContext opContext)
        throws StorageException;

    /**
     * Populates a blob's properties and metadata using the specified operation
     * context.
     * <p>
     * This method populates the blob's system properties and user-defined
     * metadata. Before reading a blob's properties or metadata, call this
     * method or its overload to retrieve the latest values for the blob's
     * properties and metadata from the Windows Azure storage service.
     * 
     * @param opContext
     *          An {@link OperationContext} object that represents the context
     *          for the current operation. This object is used to track requests
     *          to the storage service, and to provide additional runtime
     *          information about the operation.
     * 
     * @throws StorageException
     *           If a storage service error occurred.
     */
    void downloadAttributes(OperationContext opContext)
        throws StorageException;

    /**
     * Returns the blob's properties.
     * 
     * @return A {@link BlobProperties} object that represents the properties of
     *         the blob.
     */
    BlobProperties getProperties();

    /**
     * Opens a blob input stream to download the blob using the specified
     * operation context.
     * <p>
     * Use {@link CloudBlobClient#setStreamMinimumReadSizeInBytes} to configure
     * the read size.
     * 
     * @param opContext
     *          An {@link OperationContext} object that represents the context
     *          for the current operation. This object is used to track requests
     *          to the storage service, and to provide additional runtime
     *          information about the operation.
     * 
     * @return An <code>InputStream</code> object that represents the stream to
     *         use for reading from the blob.
     * 
     * @throws StorageException
     *           If a storage service error occurred.
     */
    InputStream openInputStream(BlobRequestOptions options,
        OperationContext opContext) throws StorageException;

    /**
     * Uploads the blob's metadata to the storage service using the specified
     * lease ID, request options, and operation context.
     * 
     * @param opContext
     *          An {@link OperationContext} object that represents the context
     *          for the current operation. This object is used to track requests
     *          to the storage service, and to provide additional runtime
     *          information about the operation.
     * 
     * @throws StorageException
     *           If a storage service error occurred.
     */
    void uploadMetadata(OperationContext opContext)
        throws StorageException;

    /**
     * Uploads the blob's metadata to the storage service using the specified
     * lease ID, request options, and operation context.
     *
     * @param accessCondition
     *           A {@link AccessCondition} object that represents the access conditions for the blob.
     *
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     *
     * @param opContext
     *          An {@link OperationContext} object that represents the context
     *          for the current operation. This object is used to track requests
     *          to the storage service, and to provide additional runtime
     *          information about the operation.
     *
     * @throws StorageException
     *           If a storage service error occurred.
     */
    void uploadMetadata(AccessCondition accessCondition, BlobRequestOptions options,
        OperationContext opContext) throws StorageException;

    void uploadProperties(OperationContext opContext,
        SelfRenewingLease lease)
        throws StorageException;

    SelfRenewingLease acquireLease() throws StorageException;

    /**
     * Gets the minimum read block size to use with this Blob.
     *
     * @return The minimum block size, in bytes, for reading from a block blob.
     */
    int getStreamMinimumReadSizeInBytes();

    /**
     * Sets the minimum read block size to use with this Blob.
     *
     * @param minimumReadSizeBytes
     *          The maximum block size, in bytes, for reading from a block blob
     *          while using a {@link BlobInputStream} object, ranging from 512
     *          bytes to 64 MB, inclusive.
     */
    void setStreamMinimumReadSizeInBytes(
        int minimumReadSizeBytes);

    /**
     * Sets the write block size to use with this Blob.
     * 
     * @param writeBlockSizeBytes
     *          The maximum block size, in bytes, for writing to a block blob
     *          while using a {@link BlobOutputStream} object, ranging from 1 MB
     *          to 4 MB, inclusive.
     * 
     * @throws IllegalArgumentException
     *           If <code>writeBlockSizeInBytes</code> is less than 1 MB or
     *           greater than 4 MB.
     */
    void setWriteBlockSizeInBytes(int writeBlockSizeBytes);

    CloudBlob getBlob();
  }

  /**
   * A thin wrapper over the
   * {@link com.microsoft.azure.storage.blob.CloudBlockBlob} class
   * that simply redirects calls to the real object except in unit tests.
   */
  public abstract interface CloudBlockBlobWrapper
      extends CloudBlobWrapper {
    /**
     * Creates and opens an output stream to write data to the block blob using the specified 
     * operation context.
     * 
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @return A {@link BlobOutputStream} object used to write data to the blob.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    OutputStream openOutputStream(
        BlobRequestOptions options,
        OperationContext opContext) throws StorageException;

    /**
     *
     * @param filter    A {@link BlockListingFilter} value that specifies whether to download
     *                  committed blocks, uncommitted blocks, or all blocks.
     * @param options   A {@link BlobRequestOptions} object that specifies any additional options for
     *                  the request. Specifying null will use the default request options from
     *                  the associated service client ( CloudBlobClient).
     * @param opContext An {@link OperationContext} object that represents the context for the current
     *                  operation. This object is used to track requests to the storage service,
     *                  and to provide additional runtime information about the operation.
     * @return          An ArrayList object of {@link BlockEntry} objects that represent the list
     *                  block items downloaded from the block blob.
     * @throws IOException  If an I/O error occurred.
     * @throws StorageException If a storage service error occurred.
     */
    List<BlockEntry> downloadBlockList(BlockListingFilter filter, BlobRequestOptions options,
        OperationContext opContext) throws IOException, StorageException;

    /**
     *
     * @param blockId      A String that represents the Base-64 encoded block ID. Note for a given blob
     *                     the length of all Block IDs must be identical.
     * @param accessCondition An {@link AccessCondition} object that represents the access conditions for the blob.
     * @param sourceStream An {@link InputStream} object that represents the input stream to write to the
     *                     block blob.
     * @param length       A long which represents the length, in bytes, of the stream data,
     *                     or -1 if unknown.
     * @param options      A {@link BlobRequestOptions} object that specifies any additional options for the
     *                     request. Specifying null will use the default request options from the
     *                     associated service client ( CloudBlobClient).
     * @param opContext    An {@link OperationContext} object that represents the context for the current operation.
     *                     This object is used to track requests to the storage service, and to provide
     *                     additional runtime information about the operation.
     * @throws IOException  If an I/O error occurred.
     * @throws StorageException If a storage service error occurred.
     */
    void uploadBlock(String blockId, AccessCondition accessCondition, InputStream sourceStream,
        long length, BlobRequestOptions options,
        OperationContext opContext) throws IOException, StorageException;

    /**
     *
     * @param blockList       An enumerable collection of {@link BlockEntry} objects that represents the list
     *                        block items being committed. The size field is ignored.
     * @param accessCondition An {@link AccessCondition} object that represents the access conditions for the blob.
     * @param options         A {@link BlobRequestOptions} object that specifies any additional options for the
     *                        request. Specifying null will use the default request options from the associated
     *                        service client ( CloudBlobClient).
     * @param opContext       An {@link OperationContext} object that represents the context for the current operation.
     *                        This object is used to track requests to the storage service, and to provide additional
     *                        runtime information about the operation.
     * @throws IOException      If an I/O error occurred.
     * @throws StorageException If a storage service error occurred.
     */
    void commitBlockList(List<BlockEntry> blockList, AccessCondition accessCondition, BlobRequestOptions options,
        OperationContext opContext) throws IOException, StorageException;

  }

  /**
   * A thin wrapper over the
   * {@link com.microsoft.azure.storage.blob.CloudPageBlob}
   * class that simply redirects calls to the real object except in unit tests.
   */
  public abstract interface CloudPageBlobWrapper
      extends CloudBlobWrapper {
    /**
     * Creates a page blob using the specified request options and operation context.
     *
     * @param length
     *            The size, in bytes, of the page blob.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     *
     * @throws IllegalArgumentException
     *             If the length is not a multiple of 512.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     */
    void create(final long length, BlobRequestOptions options,
            OperationContext opContext) throws StorageException;
    

    /**
     * Uploads a range of contiguous pages, up to 4 MB in size, at the specified offset in the page blob, using the
     * specified lease ID, request options, and operation context.
     * 
     * @param sourceStream
     *            An <code>InputStream</code> object that represents the input stream to write to the page blob.
     * @param offset
     *            The offset, in number of bytes, at which to begin writing the data. This value must be a multiple of
     *            512.
     * @param length
     *            The length, in bytes, of the data to write. This value must be a multiple of 512.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @throws IllegalArgumentException
     *             If the offset or length are not multiples of 512, or if the length is greater than 4 MB.
     * @throws IOException
     *             If an I/O exception occurred.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    void uploadPages(final InputStream sourceStream, final long offset,
        final long length, BlobRequestOptions options,
        OperationContext opContext) throws StorageException, IOException;

    /**
     * Returns a collection of page ranges and their starting and ending byte offsets using the specified request
     * options and operation context.
     *
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     *
     * @return An <code>ArrayList</code> object that represents the set of page ranges and their starting and ending
     *         byte offsets.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     */

    ArrayList<PageRange> downloadPageRanges(BlobRequestOptions options,
            OperationContext opContext) throws StorageException;
    
    void uploadMetadata(OperationContext opContext)
        throws StorageException; 
  }
}