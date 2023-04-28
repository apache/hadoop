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
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryPolicyFactory;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsToken;
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

  CloudBlobClient getServiceClient() {
    return serviceClient;
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

  public CloudBlobContainerWrapper getContainerReference(String uri, AccessTokenProvider tokenProvider)
      throws URISyntaxException, StorageException {
      return new CloudBlobContainerWrapperImpl(
          serviceClient.getContainerReference(uri), serviceClient, tokenProvider);
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
    private CloudBlobClient serviceClient = null;
    private AccessTokenProvider tokenProvider = null;

    public CloudBlobDirectory getDirectory() throws IOException {
      if (tokenProvider != null) {
        String token = tokenProvider.getToken().getAccessToken();
        ((StorageCredentialsToken) serviceClient.getCredentials()).updateToken(
            token);
      }
      return directory;
    }

    public CloudBlobDirectoryWrapperImpl(CloudBlobDirectory directory) {
      this.directory = directory;
    }

    public CloudBlobDirectoryWrapperImpl(CloudBlobDirectory directory, AccessTokenProvider tokenProvider, CloudBlobClient serviceClient) {
      this.directory = directory;
      this.tokenProvider = tokenProvider;
      this.serviceClient = serviceClient;
    }

    @Override
    public URI getUri() {
      try {
        return getDirectory().getUri();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }

    @Override
    public Iterable<ListBlobItem> listBlobs(String prefix,
        boolean useFlatBlobListing, EnumSet<BlobListingDetails> listingDetails,
        BlobRequestOptions options, OperationContext opContext)
        throws URISyntaxException, StorageException, IOException {
      return WrappingIterator.wrap(getDirectory().listBlobs(prefix,
          useFlatBlobListing, listingDetails, options, opContext));
    }

    @Override
    public CloudBlobContainer getContainer() throws URISyntaxException,
        StorageException {
      try {
        return getDirectory().getContainer();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }

    @Override
    public CloudBlobDirectory getParent() throws URISyntaxException,
        StorageException {
      try {
        return getDirectory().getParent();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }

    @Override
    public StorageUri getStorageUri() {
      try {
        return getDirectory().getStorageUri();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }
  }

  //
  // CloudBlobContainerWrapperImpl
  //
  @InterfaceAudience.Private
  static class CloudBlobContainerWrapperImpl extends CloudBlobContainerWrapper {
    private final CloudBlobContainer container;
    private CloudBlobClient serviceClient = null;
    private AccessTokenProvider tokenProvider = null;

    public CloudBlobContainer getContainer() throws IOException {
      if (tokenProvider != null) {
        String token = tokenProvider.getToken().getAccessToken();
        ((StorageCredentialsToken) serviceClient.getCredentials()).updateToken(
            token);
      }
      return container;
    }

    public CloudBlobContainerWrapperImpl(CloudBlobContainer container) {
      this.container = container;
    }

    public CloudBlobContainerWrapperImpl(CloudBlobContainer container, CloudBlobClient serviceClient, AccessTokenProvider tokenProvider) {
      this.container = container;
      this.serviceClient = serviceClient;
      this.tokenProvider = tokenProvider;
    }

    @Override
    public String getName() throws IOException {
      return getContainer().getName();
    }

    @Override
    public boolean exists(OperationContext opContext)
        throws StorageException, IOException {
      return getContainer().exists(AccessCondition.generateEmptyCondition(), null,
          opContext);
    }

    @Override
    public void create(OperationContext opContext)
        throws StorageException, IOException {
      getContainer().create(null, opContext);
    }

    @Override
    public HashMap<String, String> getMetadata() throws IOException {
      return getContainer().getMetadata();
    }

    @Override
    public void setMetadata(HashMap<String, String> metadata)
        throws IOException {
      getContainer().setMetadata(metadata);
    }

    @Override
    public void downloadAttributes(OperationContext opContext)
        throws StorageException, IOException {
      getContainer().downloadAttributes(AccessCondition.generateEmptyCondition(),
          null, opContext);
    }

    @Override
    public void uploadMetadata(OperationContext opContext)
        throws StorageException, IOException {
      getContainer().uploadMetadata(AccessCondition.generateEmptyCondition(), null,
          opContext);
    }

    @Override
    public CloudBlobDirectoryWrapper getDirectoryReference(String relativePath)
        throws URISyntaxException, StorageException, IOException {

      CloudBlobDirectory dir = getContainer().getDirectoryReference(relativePath);
      return new CloudBlobDirectoryWrapperImpl(dir);
    }

    @Override
    public CloudBlobDirectoryWrapper getDirectoryReference(String relativePath, AccessTokenProvider tokenProvider)
        throws URISyntaxException, StorageException, IOException {

      CloudBlobDirectory dir = getContainer().getDirectoryReference(relativePath);
      return new CloudBlobDirectoryWrapperImpl(dir, tokenProvider, serviceClient);
    }

    @Override
    public CloudBlobWrapper getBlockBlobReference(String relativePath)
        throws URISyntaxException, StorageException, IOException {
      return new CloudBlockBlobWrapperImpl(getContainer().getBlockBlobReference(relativePath));
    }

    @Override
    public CloudBlobWrapper getBlockBlobReference(String relativePath, AccessTokenProvider tokenProvider)
        throws URISyntaxException, StorageException, IOException {
      return new CloudBlockBlobWrapperImpl(getContainer().getBlockBlobReference(relativePath), tokenProvider, serviceClient);
    }

    @Override
    public CloudBlobWrapper getPageBlobReference(String relativePath)
        throws URISyntaxException, StorageException, IOException {
      return new CloudPageBlobWrapperImpl(
          getContainer().getPageBlobReference(relativePath));
    }

    @Override
    public CloudBlobWrapper getPageBlobReference(String relativePath, AccessTokenProvider tokenProvider)
        throws URISyntaxException, StorageException, IOException {
      return new CloudPageBlobWrapperImpl(
          getContainer().getPageBlobReference(relativePath), tokenProvider, serviceClient);
    }

  }

  abstract static class CloudBlobWrapperImpl implements CloudBlobWrapper {
    private final CloudBlob blob;
    private AccessTokenProvider tokenProvider = null;
    private CloudBlobClient serviceClient = null;

    @Override
    public CloudBlob getBlob() throws IOException {
      if (tokenProvider != null) {
        String token = tokenProvider.getToken().getAccessToken();
        ((StorageCredentialsToken) serviceClient.getCredentials()).updateToken(
            token);
      }
      return blob;
    }

    public URI getUri() {
      try {
        return getBlob().getUri();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }

    protected CloudBlobWrapperImpl(CloudBlob blob) {
      this.blob = blob;
    }

    protected CloudBlobWrapperImpl(CloudBlob blob, AccessTokenProvider tokenProvider, CloudBlobClient serviceClient) {
      this.blob = blob;
      this.tokenProvider = tokenProvider;
      this.serviceClient = serviceClient;
    }

    @Override
    public HashMap<String, String> getMetadata() throws IOException {
      return getBlob().getMetadata();
    }

    @Override
    public void delete(OperationContext opContext, SelfRenewingLease lease)
        throws StorageException, IOException {
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
        throws StorageException, IOException {
      return getBlob().exists(null, null, opContext);
    }

    @Override
    public void downloadAttributes(
        OperationContext opContext) throws StorageException, IOException {
      getBlob().downloadAttributes(null, null, opContext);
    }

    @Override
    public BlobProperties getProperties() throws IOException {
      return getBlob().getProperties();
    }

    @Override
    public void setMetadata(HashMap<String, String> metadata)
        throws IOException {
      getBlob().setMetadata(metadata);
    }

    @Override
    public InputStream openInputStream(
        BlobRequestOptions options,
        OperationContext opContext) throws StorageException, IOException {
      return getBlob().openInputStream(null, options, opContext);
    }

    public OutputStream openOutputStream(
        BlobRequestOptions options,
        OperationContext opContext) throws StorageException, IOException {
      return ((CloudBlockBlob) getBlob()).openOutputStream(null, options, opContext);
    }

    public void upload(InputStream sourceStream, OperationContext opContext)
        throws StorageException, IOException {
      getBlob().upload(sourceStream, 0, null, null, opContext);
    }

    @Override
    public CloudBlobContainer getContainer() throws URISyntaxException,
        StorageException {
      try {
        return getBlob().getContainer();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }

    @Override
    public CloudBlobDirectory getParent() throws URISyntaxException,
        StorageException {
      try {
        return getBlob().getParent();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }

    @Override
    public void uploadMetadata(OperationContext opContext)
        throws StorageException, IOException {
      uploadMetadata(null, null, opContext);
    }

    @Override
    public void uploadMetadata(AccessCondition accessConditions, BlobRequestOptions options,
        OperationContext opContext) throws StorageException, IOException {
      getBlob().uploadMetadata(accessConditions, options, opContext);
    }

    public void uploadProperties(OperationContext opContext, SelfRenewingLease lease)
        throws StorageException, IOException {

      // Include lease in request if lease not null.
      getBlob().uploadProperties(getLeaseCondition(lease), null, opContext);
    }

    @Override
    public int getStreamMinimumReadSizeInBytes() throws IOException {
        return getBlob().getStreamMinimumReadSizeInBytes();
    }

    @Override
    public void setStreamMinimumReadSizeInBytes(int minimumReadSizeBytes)
        throws IOException {
      getBlob().setStreamMinimumReadSizeInBytes(minimumReadSizeBytes);
    }

    @Override
    public void setWriteBlockSizeInBytes(int writeBlockSizeBytes)
        throws IOException {
      getBlob().setStreamWriteSizeInBytes(writeBlockSizeBytes);
    }

    @Override
    public StorageUri getStorageUri() {
      try {
        return getBlob().getStorageUri();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }

    @Override
    public CopyState getCopyState() throws IOException {
      return getBlob().getCopyState();
    }

    @Override
    public void startCopyFromBlob(CloudBlobWrapper sourceBlob, BlobRequestOptions options,
        OperationContext opContext, boolean overwriteDestination)
        throws StorageException, URISyntaxException, IOException {
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
    public SelfRenewingLease acquireLease() throws StorageException,
        IOException {
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

    public CloudBlockBlobWrapperImpl(CloudBlockBlob blob, AccessTokenProvider tokenProvider, CloudBlobClient serviceClient) {
      super(blob, tokenProvider, serviceClient);
    }

    public OutputStream openOutputStream(
        BlobRequestOptions options,
        OperationContext opContext) throws StorageException, IOException {
      return ((CloudBlockBlob) getBlob()).openOutputStream(null, options, opContext);
    }

    public void upload(InputStream sourceStream, OperationContext opContext)
        throws StorageException, IOException {
      getBlob().upload(sourceStream, 0, null, null, opContext);
    }

    public void uploadProperties(OperationContext opContext)
        throws StorageException, IOException {
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

    public CloudPageBlobWrapperImpl(CloudPageBlob blob, AccessTokenProvider tokenProvider, CloudBlobClient serviceClient) {
      super(blob, tokenProvider, serviceClient);
    }

    public void create(final long length, BlobRequestOptions options,
        OperationContext opContext) throws StorageException, IOException {
      ((CloudPageBlob) getBlob()).create(length, null, options, opContext);
    }

    public void uploadPages(final InputStream sourceStream, final long offset,
        final long length, BlobRequestOptions options, OperationContext opContext)
        throws StorageException, IOException {
      ((CloudPageBlob) getBlob()).uploadPages(sourceStream, offset, length, null,
          options, opContext);
    }

    public ArrayList<PageRange> downloadPageRanges(BlobRequestOptions options,
        OperationContext opContext) throws StorageException, IOException {
      return ((CloudPageBlob) getBlob()).downloadPageRanges(
          null, options, opContext);
    }
  }
}
