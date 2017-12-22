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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.classification.InterfaceAudience;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryPolicyFactory;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageUri;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.BlockListingFilter;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import com.microsoft.azure.storage.blob.CopyState;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.blob.PageRange;

/**
 * A real implementation of the Azure interaction layer that just redirects
 * calls to the Windows Azure storage SDK.
 */
@InterfaceAudience.Private
class StorageInterfaceImpl extends StorageInterface {
  private CloudBlobClient serviceClient;
  private RetryPolicyFactory retryPolicyFactory;
  private int timeoutIntervalInMs;

  private void updateRetryPolicy() {
    if (serviceClient != null && retryPolicyFactory != null) {
      serviceClient.getDefaultRequestOptions().setRetryPolicyFactory(retryPolicyFactory);
    }
  }

  private void updateTimeoutInMs() {
    if (serviceClient != null && timeoutIntervalInMs > 0) {
      serviceClient.getDefaultRequestOptions().setTimeoutIntervalInMs(timeoutIntervalInMs);
    }
  }

  @Override
  public void setRetryPolicyFactory(final RetryPolicyFactory retryPolicyFactory) {
    this.retryPolicyFactory = retryPolicyFactory;
    updateRetryPolicy();
  }

  @Override
  public void setTimeoutInMs(int timeoutInMs) {
    timeoutIntervalInMs = timeoutInMs;
    updateTimeoutInMs();
  }

  @Override
  public void createBlobClient(CloudStorageAccount account) {
    serviceClient = account.createCloudBlobClient();
    updateRetryPolicy();
    updateTimeoutInMs();
  }

  @Override
  public void createBlobClient(URI baseUri) {
    createBlobClient(baseUri, (StorageCredentials)null);
  }

  @Override
  public void createBlobClient(URI baseUri, StorageCredentials credentials) {
    serviceClient = new CloudBlobClient(baseUri, credentials);
    updateRetryPolicy();
    updateTimeoutInMs();
  }

  @Override
  public StorageCredentials getCredentials() {
    return serviceClient.getCredentials();
  }

  @Override
  public CloudBlobContainerWrapper getContainerReference(String uri)
      throws URISyntaxException, StorageException {
    return new CloudBlobContainerWrapperImpl(
        serviceClient.getContainerReference(uri));
  }

  //
  // WrappingIterator
  //

  /**
   * This iterator wraps every ListBlobItem as they come from the listBlobs()
   * calls to their proper wrapping objects.
   */
  private static class WrappingIterator implements Iterator<ListBlobItem> {
    private final Iterator<ListBlobItem> present;

    public WrappingIterator(Iterator<ListBlobItem> present) {
      this.present = present;
    }

    public static Iterable<ListBlobItem> wrap(
        final Iterable<ListBlobItem> present) {
      return new Iterable<ListBlobItem>() {
        @Override
        public Iterator<ListBlobItem> iterator() {
          return new WrappingIterator(present.iterator());
        }
      };
    }

    @Override
    public boolean hasNext() {
      return present.hasNext();
    }

    @Override
    public ListBlobItem next() {
      ListBlobItem unwrapped = present.next();
      if (unwrapped instanceof CloudBlobDirectory) {
        return new CloudBlobDirectoryWrapperImpl((CloudBlobDirectory) unwrapped);
      } else if (unwrapped instanceof CloudBlockBlob) {
        return new CloudBlockBlobWrapperImpl((CloudBlockBlob) unwrapped);
      } else if (unwrapped instanceof CloudPageBlob) {
        return new CloudPageBlobWrapperImpl((CloudPageBlob) unwrapped);
      } else {
        return unwrapped;
      }
    }

    @Override
    public void remove() {
      present.remove();
    }
  }

  //
  // CloudBlobDirectoryWrapperImpl
  //
  @InterfaceAudience.Private
  static class CloudBlobDirectoryWrapperImpl extends CloudBlobDirectoryWrapper {
    private final CloudBlobDirectory directory;

    public CloudBlobDirectoryWrapperImpl(CloudBlobDirectory directory) {
      this.directory = directory;
    }

    @Override
    public URI getUri() {
      return directory.getUri();
    }

    @Override
    public Iterable<ListBlobItem> listBlobs(String prefix,
        boolean useFlatBlobListing, EnumSet<BlobListingDetails> listingDetails,
        BlobRequestOptions options, OperationContext opContext)
        throws URISyntaxException, StorageException {
      return WrappingIterator.wrap(directory.listBlobs(prefix,
          useFlatBlobListing, listingDetails, options, opContext));
    }

    @Override
    public CloudBlobContainer getContainer() throws URISyntaxException,
        StorageException {
      return directory.getContainer();
    }

    @Override
    public CloudBlobDirectory getParent() throws URISyntaxException,
        StorageException {
      return directory.getParent();
    }

    @Override
    public StorageUri getStorageUri() {
      return directory.getStorageUri();
    }

  }

  //
  // CloudBlobContainerWrapperImpl
  //
  @InterfaceAudience.Private
  static class CloudBlobContainerWrapperImpl extends CloudBlobContainerWrapper {
    private final CloudBlobContainer container;

    public CloudBlobContainerWrapperImpl(CloudBlobContainer container) {
      this.container = container;
    }

    @Override
    public String getName() {
      return container.getName();
    }

    @Override
    public boolean exists(OperationContext opContext) throws StorageException {
      return container.exists(AccessCondition.generateEmptyCondition(), null,
          opContext);
    }

    @Override
    public void create(OperationContext opContext) throws StorageException {
      container.create(null, opContext);
    }

    @Override
    public HashMap<String, String> getMetadata() {
      return container.getMetadata();
    }

    @Override
    public void setMetadata(HashMap<String, String> metadata) {
      container.setMetadata(metadata);
    }

    @Override
    public void downloadAttributes(OperationContext opContext)
        throws StorageException {
      container.downloadAttributes(AccessCondition.generateEmptyCondition(),
          null, opContext);
    }

    @Override
    public void uploadMetadata(OperationContext opContext)
        throws StorageException {
      container.uploadMetadata(AccessCondition.generateEmptyCondition(), null,
          opContext);
    }

    @Override
    public CloudBlobDirectoryWrapper getDirectoryReference(String relativePath)
        throws URISyntaxException, StorageException {

      CloudBlobDirectory dir = container.getDirectoryReference(relativePath);
      return new CloudBlobDirectoryWrapperImpl(dir);
    }

    @Override
    public CloudBlobWrapper getBlockBlobReference(String relativePath)
        throws URISyntaxException, StorageException {

      return new CloudBlockBlobWrapperImpl(container.getBlockBlobReference(relativePath));
    }

    @Override
    public CloudBlobWrapper getPageBlobReference(String relativePath)
        throws URISyntaxException, StorageException {
      return new CloudPageBlobWrapperImpl(
          container.getPageBlobReference(relativePath));
    }

  }

  abstract static class CloudBlobWrapperImpl implements CloudBlobWrapper {
    private final CloudBlob blob;

    @Override
    public CloudBlob getBlob() {
      return blob;
    }

    public URI getUri() {
      return getBlob().getUri();
    }

    protected CloudBlobWrapperImpl(CloudBlob blob) {
      this.blob = blob;
    }

    @Override
    public HashMap<String, String> getMetadata() {
      return getBlob().getMetadata();
    }

    @Override
    public void delete(OperationContext opContext, SelfRenewingLease lease)
        throws StorageException {
      getBlob().delete(DeleteSnapshotsOption.NONE, getLeaseCondition(lease),
          null, opContext);
    }

    /**
     * Return and access condition for this lease, or else null if
     * there's no lease.
     */
    private AccessCondition getLeaseCondition(SelfRenewingLease lease) {
      AccessCondition leaseCondition = null;
      if (lease != null) {
        leaseCondition = AccessCondition.generateLeaseCondition(lease.getLeaseID());
      }
      return leaseCondition;
    }

    @Override
    public boolean exists(OperationContext opContext)
        throws StorageException {
      return getBlob().exists(null, null, opContext);
    }

    @Override
    public void downloadAttributes(
        OperationContext opContext) throws StorageException {
      getBlob().downloadAttributes(null, null, opContext);
    }

    @Override
    public BlobProperties getProperties() {
      return getBlob().getProperties();
    }

    @Override
    public void setMetadata(HashMap<String, String> metadata) {
      getBlob().setMetadata(metadata);
    }

    @Override
    public InputStream openInputStream(
        BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      return getBlob().openInputStream(null, options, opContext);
    }

    public OutputStream openOutputStream(
        BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      return ((CloudBlockBlob) getBlob()).openOutputStream(null, options, opContext);
    }

    public void upload(InputStream sourceStream, OperationContext opContext)
        throws StorageException, IOException {
      getBlob().upload(sourceStream, 0, null, null, opContext);
    }

    @Override
    public CloudBlobContainer getContainer() throws URISyntaxException,
        StorageException {
      return getBlob().getContainer();
    }

    @Override
    public CloudBlobDirectory getParent() throws URISyntaxException,
        StorageException {
      return getBlob().getParent();
    }

    @Override
    public void uploadMetadata(OperationContext opContext)
        throws StorageException {
      uploadMetadata(null, null, opContext);
    }

    @Override
    public void uploadMetadata(AccessCondition accessConditions, BlobRequestOptions options,
        OperationContext opContext) throws StorageException{
      getBlob().uploadMetadata(accessConditions, options, opContext);
    }

    public void uploadProperties(OperationContext opContext, SelfRenewingLease lease)
        throws StorageException {

      // Include lease in request if lease not null.
      getBlob().uploadProperties(getLeaseCondition(lease), null, opContext);
    }

    @Override
    public int getStreamMinimumReadSizeInBytes() {
        return getBlob().getStreamMinimumReadSizeInBytes();
    }

    @Override
    public void setStreamMinimumReadSizeInBytes(int minimumReadSizeBytes) {
      getBlob().setStreamMinimumReadSizeInBytes(minimumReadSizeBytes);
    }

    @Override
    public void setWriteBlockSizeInBytes(int writeBlockSizeBytes) {
      getBlob().setStreamWriteSizeInBytes(writeBlockSizeBytes);
    }

    @Override
    public StorageUri getStorageUri() {
      return getBlob().getStorageUri();
    }

    @Override
    public CopyState getCopyState() {
      return getBlob().getCopyState();
    }

    @Override
    public void startCopyFromBlob(CloudBlobWrapper sourceBlob, BlobRequestOptions options,
        OperationContext opContext, boolean overwriteDestination)
            throws StorageException, URISyntaxException {
      AccessCondition dstAccessCondition =
          overwriteDestination
              ? null
              : AccessCondition.generateIfNotExistsCondition();
      getBlob().startCopy(sourceBlob.getBlob().getQualifiedUri(),
          null, dstAccessCondition, options, opContext);
    }

    @Override
    public void downloadRange(long offset, long length, OutputStream outStream,
        BlobRequestOptions options, OperationContext opContext)
            throws StorageException, IOException {

      getBlob().downloadRange(offset, length, outStream, null, options, opContext);
    }

    @Override
    public SelfRenewingLease acquireLease() throws StorageException {
      return new SelfRenewingLease(this, false);
    }
  }


  //
  // CloudBlockBlobWrapperImpl
  //

  static class CloudBlockBlobWrapperImpl extends CloudBlobWrapperImpl implements CloudBlockBlobWrapper {
    public CloudBlockBlobWrapperImpl(CloudBlockBlob blob) {
      super(blob);
    }

    public OutputStream openOutputStream(
        BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      return ((CloudBlockBlob) getBlob()).openOutputStream(null, options, opContext);
    }

    public void upload(InputStream sourceStream, OperationContext opContext)
        throws StorageException, IOException {
      getBlob().upload(sourceStream, 0, null, null, opContext);
    }

    public void uploadProperties(OperationContext opContext)
        throws StorageException {
      getBlob().uploadProperties(null, null, opContext);
    }

    @Override
    public List<BlockEntry> downloadBlockList(BlockListingFilter filter, BlobRequestOptions options,
        OperationContext opContext) throws IOException, StorageException {
      return ((CloudBlockBlob) getBlob()).downloadBlockList(filter, null, options, opContext);

    }

    @Override
    public void uploadBlock(String blockId, AccessCondition accessCondition, InputStream sourceStream,
        long length, BlobRequestOptions options,
        OperationContext opContext) throws IOException, StorageException {
      ((CloudBlockBlob) getBlob()).uploadBlock(blockId, sourceStream, length, accessCondition, options, opContext);
    }

    @Override
    public void commitBlockList(List<BlockEntry> blockList, AccessCondition accessCondition, BlobRequestOptions options,
        OperationContext opContext) throws IOException, StorageException {
      ((CloudBlockBlob) getBlob()).commitBlockList(blockList, accessCondition, options, opContext);
    }
  }

  static class CloudPageBlobWrapperImpl extends CloudBlobWrapperImpl implements CloudPageBlobWrapper {
    public CloudPageBlobWrapperImpl(CloudPageBlob blob) {
      super(blob);
    }

    public void create(final long length, BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      ((CloudPageBlob) getBlob()).create(length, null, options, opContext);
    }

    public void uploadPages(final InputStream sourceStream, final long offset,
        final long length, BlobRequestOptions options, OperationContext opContext)
        throws StorageException, IOException {
      ((CloudPageBlob) getBlob()).uploadPages(sourceStream, offset, length, null,
          options, opContext);
    }

    public ArrayList<PageRange> downloadPageRanges(BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      return ((CloudPageBlob) getBlob()).downloadPageRanges(
          null, options, opContext);
    }
  }
}
