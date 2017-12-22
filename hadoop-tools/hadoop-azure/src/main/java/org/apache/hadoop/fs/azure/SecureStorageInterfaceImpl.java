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

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryPolicyFactory;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageUri;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.BlockListingFilter;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import com.microsoft.azure.storage.blob.CopyState;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.PageRange;
import com.microsoft.azure.storage.blob.BlockEntry;

import org.apache.hadoop.classification.InterfaceAudience;

/***
 * An implementation of the StorageInterface for SAS Key mode.
 *
 */

public class SecureStorageInterfaceImpl extends StorageInterface {

  public static final Logger LOG = LoggerFactory.getLogger(
      SecureStorageInterfaceImpl.class);
  public static final String SAS_ERROR_CODE = "SAS Error";
  private SASKeyGeneratorInterface sasKeyGenerator;
  private String storageAccount;
  private RetryPolicyFactory retryPolicy;
  private int timeoutIntervalInMs;
  private boolean useContainerSasKeyForAllAccess;

  /**
   * Configuration key to specify if containerSasKey should be used for all accesses
   */
  public static final String KEY_USE_CONTAINER_SASKEY_FOR_ALL_ACCESS =
      "fs.azure.saskey.usecontainersaskeyforallaccess";

  public SecureStorageInterfaceImpl(boolean useLocalSASKeyMode,
      Configuration conf) throws SecureModeException {

    if (useLocalSASKeyMode) {
      this.sasKeyGenerator = new LocalSASKeyGeneratorImpl(conf);
    } else {
      RemoteSASKeyGeneratorImpl remoteSasKeyGenerator =
          new RemoteSASKeyGeneratorImpl(conf);
      try {
        remoteSasKeyGenerator.initialize(conf);
      } catch (IOException ioe) {
        throw new SecureModeException("Remote SAS Key mode could"
            + " not be initialized", ioe);
      }
      this.sasKeyGenerator = remoteSasKeyGenerator;
    }
    this.useContainerSasKeyForAllAccess = conf.getBoolean(KEY_USE_CONTAINER_SASKEY_FOR_ALL_ACCESS, true);
  }

  @Override
  public void setTimeoutInMs(int timeoutInMs) {
    timeoutIntervalInMs = timeoutInMs;
  }

  @Override
  public void setRetryPolicyFactory(RetryPolicyFactory retryPolicyFactory) {
    retryPolicy = retryPolicyFactory;
  }

  @Override
  public void createBlobClient(CloudStorageAccount account) {
    String errorMsg = "createBlobClient is an invalid operation in"
        + " SAS Key Mode";
    LOG.error(errorMsg);
    throw new UnsupportedOperationException(errorMsg);
  }

  @Override
  public void createBlobClient(URI baseUri) {
    String errorMsg = "createBlobClient is an invalid operation in "
        + "SAS Key Mode";
    LOG.error(errorMsg);
    throw new UnsupportedOperationException(errorMsg);
  }

  @Override
  public void createBlobClient(URI baseUri, StorageCredentials credentials) {
    String errorMsg = "createBlobClient is an invalid operation in SAS "
        + "Key Mode";
    LOG.error(errorMsg);
    throw new UnsupportedOperationException(errorMsg);
  }

  @Override
  public StorageCredentials getCredentials() {
    String errorMsg = "getCredentials is an invalid operation in SAS "
        + "Key Mode";
    LOG.error(errorMsg);
    throw new UnsupportedOperationException(errorMsg);
  }

  @Override
  public CloudBlobContainerWrapper getContainerReference(String name)
      throws URISyntaxException, StorageException {

    try {
      CloudBlobContainer container = new CloudBlobContainer(sasKeyGenerator.getContainerSASUri(
          storageAccount, name));
      if (retryPolicy != null) {
        container.getServiceClient().getDefaultRequestOptions().setRetryPolicyFactory(retryPolicy);
      }
      if (timeoutIntervalInMs > 0) {
        container.getServiceClient().getDefaultRequestOptions().setTimeoutIntervalInMs(timeoutIntervalInMs);
      }
      return (useContainerSasKeyForAllAccess)
          ? new SASCloudBlobContainerWrapperImpl(storageAccount, container, null)
          : new SASCloudBlobContainerWrapperImpl(storageAccount, container, sasKeyGenerator);
    } catch (SASKeyGenerationException sasEx) {
      String errorMsg = "Encountered SASKeyGeneration exception while "
          + "generating SAS Key for container : " + name
          + " inside Storage account : " + storageAccount;
      LOG.error(errorMsg);
      throw new StorageException(SAS_ERROR_CODE, errorMsg, sasEx);
    }
  }

  public void setStorageAccountName(String storageAccount) {
    this.storageAccount = storageAccount;
  }

  @InterfaceAudience.Private
  static class SASCloudBlobContainerWrapperImpl
    extends CloudBlobContainerWrapper {

    private final CloudBlobContainer container;
    private String storageAccount;
    private SASKeyGeneratorInterface sasKeyGenerator;

    public SASCloudBlobContainerWrapperImpl(String storageAccount,
        CloudBlobContainer container, SASKeyGeneratorInterface sasKeyGenerator) {
      this.storageAccount = storageAccount;
      this.container = container;
      this.sasKeyGenerator = sasKeyGenerator;
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
      return new SASCloudBlobDirectoryWrapperImpl(dir);
    }

    @Override
    public CloudBlobWrapper getBlockBlobReference(String relativePath)
        throws URISyntaxException, StorageException {
      try {
        CloudBlockBlob blob = (sasKeyGenerator!=null)
          ? new CloudBlockBlob(sasKeyGenerator.getRelativeBlobSASUri(storageAccount, getName(), relativePath))
          : container.getBlockBlobReference(relativePath);
        blob.getServiceClient().setDefaultRequestOptions(
                container.getServiceClient().getDefaultRequestOptions());
        return new SASCloudBlockBlobWrapperImpl(blob);
      } catch (SASKeyGenerationException sasEx) {
        String errorMsg = "Encountered SASKeyGeneration exception while "
            + "generating SAS Key for relativePath : " + relativePath
            + " inside container : " + getName()  + " Storage account : " + storageAccount;
        LOG.error(errorMsg);
        throw new StorageException(SAS_ERROR_CODE, errorMsg, sasEx);
      }
    }

    @Override
    public CloudBlobWrapper getPageBlobReference(String relativePath)
        throws URISyntaxException, StorageException {
      try {
        CloudPageBlob blob   = (sasKeyGenerator!=null)
          ? new CloudPageBlob(sasKeyGenerator.getRelativeBlobSASUri(storageAccount, getName(), relativePath))
          : container.getPageBlobReference(relativePath);

        blob.getServiceClient().setDefaultRequestOptions(
                container.getServiceClient().getDefaultRequestOptions());
        return new SASCloudPageBlobWrapperImpl(blob);
      } catch (SASKeyGenerationException sasEx) {
        String errorMsg = "Encountered SASKeyGeneration exception while "
            + "generating SAS Key for relativePath : " + relativePath
            + " inside container : " + getName()
            + " Storage account : " + storageAccount;
        LOG.error(errorMsg);
        throw new StorageException(SAS_ERROR_CODE, errorMsg, sasEx);
      }
    }
  }

  //
  // WrappingIterator
  //

  /**
   * This iterator wraps every ListBlobItem as they come from the listBlobs()
   * calls to their proper wrapping objects.
   */
  private static class SASWrappingIterator implements Iterator<ListBlobItem> {
    private final Iterator<ListBlobItem> present;

    public SASWrappingIterator(Iterator<ListBlobItem> present) {
      this.present = present;
    }

    public static Iterable<ListBlobItem> wrap(
        final Iterable<ListBlobItem> present) {
      return new Iterable<ListBlobItem>() {
        @Override
        public Iterator<ListBlobItem> iterator() {
          return new SASWrappingIterator(present.iterator());
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
        return new SASCloudBlobDirectoryWrapperImpl((CloudBlobDirectory) unwrapped);
      } else if (unwrapped instanceof CloudBlockBlob) {
        return new SASCloudBlockBlobWrapperImpl((CloudBlockBlob) unwrapped);
      } else if (unwrapped instanceof CloudPageBlob) {
        return new SASCloudPageBlobWrapperImpl((CloudPageBlob) unwrapped);
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
  static class SASCloudBlobDirectoryWrapperImpl extends CloudBlobDirectoryWrapper {
    private final CloudBlobDirectory directory;

    public SASCloudBlobDirectoryWrapperImpl(CloudBlobDirectory directory) {
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
      return SASWrappingIterator.wrap(directory.listBlobs(prefix,
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

  abstract static class SASCloudBlobWrapperImpl implements CloudBlobWrapper {
    private final CloudBlob blob;
    @Override
    public CloudBlob getBlob() {
      return blob;
    }

    public URI getUri() {
      return getBlob().getUri();
    }

    protected SASCloudBlobWrapperImpl(CloudBlob blob) {
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

  static class SASCloudBlockBlobWrapperImpl extends SASCloudBlobWrapperImpl implements CloudBlockBlobWrapper {

    public SASCloudBlockBlobWrapperImpl(CloudBlockBlob blob) {
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
    public void uploadBlock(String blockId, AccessCondition accessCondition,
        InputStream sourceStream,
        long length, BlobRequestOptions options,
        OperationContext opContext) throws IOException, StorageException {
      ((CloudBlockBlob) getBlob()).uploadBlock(blockId, sourceStream, length,
          accessCondition, options, opContext);
    }

    @Override
    public void commitBlockList(List<BlockEntry> blockList, AccessCondition accessCondition, BlobRequestOptions options,
        OperationContext opContext) throws IOException, StorageException {
      ((CloudBlockBlob) getBlob()).commitBlockList(blockList, accessCondition, options, opContext);
    }
  }

  static class SASCloudPageBlobWrapperImpl extends SASCloudBlobWrapperImpl implements CloudPageBlobWrapper {
    public SASCloudPageBlobWrapperImpl(CloudPageBlob blob) {
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
