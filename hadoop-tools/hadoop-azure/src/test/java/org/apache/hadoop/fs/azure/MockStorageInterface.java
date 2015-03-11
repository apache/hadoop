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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TimeZone;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.lang.NotImplementedException;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryPolicyFactory;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageUri;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CopyState;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.blob.PageRange;

import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriBuilderException;

/**
 * A mock implementation of the Azure Storage interaction layer for unit tests.
 * Just does in-memory storage.
 */
public class MockStorageInterface extends StorageInterface {
  private InMemoryBlockBlobStore backingStore;
  private final ArrayList<PreExistingContainer> preExistingContainers =
      new ArrayList<MockStorageInterface.PreExistingContainer>();
  private String baseUriString;

  public InMemoryBlockBlobStore getBackingStore() {
    return backingStore;
  }

  /**
   * Mocks the situation where a container already exists before WASB comes in,
   * i.e. the situation where a user creates a container then mounts WASB on the
   * pre-existing container.
   * 
   * @param uri
   *          The URI of the container.
   * @param metadata
   *          The metadata on the container.
   */
  public void addPreExistingContainer(String uri,
      HashMap<String, String> metadata) {
    preExistingContainers.add(new PreExistingContainer(uri, metadata));
  }

  @Override
  public void setRetryPolicyFactory(final RetryPolicyFactory retryPolicyFactory) {
  }

  @Override
  public void setTimeoutInMs(int timeoutInMs) {
  }

  @Override
  public void createBlobClient(CloudStorageAccount account) {
    backingStore = new InMemoryBlockBlobStore();
  }

  @Override
  public void createBlobClient(URI baseUri) {
    backingStore = new InMemoryBlockBlobStore();
  }

  @Override
  public void createBlobClient(URI baseUri, StorageCredentials credentials) {
    this.baseUriString = baseUri.toString();
    backingStore = new InMemoryBlockBlobStore();
  }

  @Override
  public StorageCredentials getCredentials() {
    // Not implemented for mock interface.
    return null;
  }

  /**
   * Utility function used to convert a given URI to a decoded string
   * representation sent to the backing store. URIs coming as input
   * to this class will be encoded by the URI class, and we want
   * the underlying storage to store keys in their original UTF-8 form.
   */
  private static String convertUriToDecodedString(URI uri) {
    try {
      String result = URIUtil.decode(uri.toString());
      return result;
    } catch (URIException e) {
      throw new AssertionError("Failed to decode URI: " + uri.toString());
    }
  }

  private static URI convertKeyToEncodedUri(String key) {
    try {
      String encodedKey = URIUtil.encodePath(key);
      URI uri = new URI(encodedKey);
      return uri;
    } catch (URISyntaxException e) {
      throw new AssertionError("Failed to encode key: " + key);
    } catch (URIException e) {
      throw new AssertionError("Failed to encode key: " + key);
    }
  }

  @Override
  public CloudBlobContainerWrapper getContainerReference(String name)
      throws URISyntaxException, StorageException {
    String fullUri;
    try {
      fullUri = baseUriString + "/" + URIUtil.encodePath(name);
    } catch (URIException e) {
      throw new RuntimeException("problem encoding fullUri", e);
    }

    MockCloudBlobContainerWrapper container = new MockCloudBlobContainerWrapper(
        fullUri, name);
    // Check if we have a pre-existing container with that name, and prime
    // the wrapper with that knowledge if it's found.
    for (PreExistingContainer existing : preExistingContainers) {
      if (fullUri.equalsIgnoreCase(existing.containerUri)) {
        // We have a pre-existing container. Mark the wrapper as created and
        // make sure we use the metadata for it.
        container.created = true;
        backingStore.setContainerMetadata(existing.containerMetadata);
        break;
      }
    }
    return container;
  }

  class MockCloudBlobContainerWrapper extends CloudBlobContainerWrapper {
    private boolean created = false;
    private HashMap<String, String> metadata;
    private final String baseUri;
    private final String name;

    public MockCloudBlobContainerWrapper(String baseUri, String name) {
      this.baseUri = baseUri;
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public boolean exists(OperationContext opContext) throws StorageException {
      return created;
    }

    @Override
    public void create(OperationContext opContext) throws StorageException {
      created = true;
      backingStore.setContainerMetadata(metadata);
    }

    @Override
    public HashMap<String, String> getMetadata() {
      return metadata;
    }

    @Override
    public void setMetadata(HashMap<String, String> metadata) {
      this.metadata = metadata;
    }

    @Override
    public void downloadAttributes(OperationContext opContext)
        throws StorageException {
      metadata = backingStore.getContainerMetadata();
    }

    @Override
    public void uploadMetadata(OperationContext opContext)
        throws StorageException {
      backingStore.setContainerMetadata(metadata);
    }

    @Override
    public CloudBlobDirectoryWrapper getDirectoryReference(String relativePath)
        throws URISyntaxException, StorageException {
      return new MockCloudBlobDirectoryWrapper(new URI(fullUriString(
          relativePath, true)));
    }

    @Override
    public CloudBlockBlobWrapper getBlockBlobReference(String relativePath)
        throws URISyntaxException, StorageException {
      return new MockCloudBlockBlobWrapper(new URI(fullUriString(relativePath,
          false)), null, 0);
    }

    @Override
    public CloudPageBlobWrapper getPageBlobReference(String blobAddressUri)
        throws URISyntaxException, StorageException {
      return new MockCloudPageBlobWrapper(new URI(blobAddressUri), null, 0);
    }

    // helper to create full URIs for directory and blob.
    // use withTrailingSlash=true to get a good path for a directory.
    private String fullUriString(String relativePath, boolean withTrailingSlash) {
      String fullUri;

      String baseUri = this.baseUri;
      if (!baseUri.endsWith("/")) {
        baseUri += "/";
      }
      if (withTrailingSlash && !relativePath.equals("")
          && !relativePath.endsWith("/")) {
        relativePath += "/";
      }

      try {
        fullUri = baseUri + URIUtil.encodePath(relativePath);
      } catch (URIException e) {
        throw new RuntimeException("problem encoding fullUri", e);
      }

      return fullUri;
    }
  }

  private static class PreExistingContainer {
    final String containerUri;
    final HashMap<String, String> containerMetadata;

    public PreExistingContainer(String uri, HashMap<String, String> metadata) {
      this.containerUri = uri;
      this.containerMetadata = metadata;
    }
  }

  class MockCloudBlobDirectoryWrapper extends CloudBlobDirectoryWrapper {
    private URI uri;

    public MockCloudBlobDirectoryWrapper(URI uri) {
      this.uri = uri;
    }

    @Override
    public CloudBlobContainer getContainer() throws URISyntaxException,
        StorageException {
      return null;
    }

    @Override
    public CloudBlobDirectory getParent() throws URISyntaxException,
        StorageException {
      return null;
    }

    @Override
    public URI getUri() {
      return uri;
    }

    @Override
    public Iterable<ListBlobItem> listBlobs(String prefix,
        boolean useFlatBlobListing, EnumSet<BlobListingDetails> listingDetails,
        BlobRequestOptions options, OperationContext opContext)
        throws URISyntaxException, StorageException {
      ArrayList<ListBlobItem> ret = new ArrayList<ListBlobItem>();
      URI searchUri = null;
      if (prefix == null) {
        searchUri = uri;
      } else {
        try {
          searchUri = UriBuilder.fromUri(uri).path(prefix).build();
        } catch (UriBuilderException e) {
          throw new AssertionError("Failed to encode path: " + prefix);
        }
      }

      String fullPrefix = convertUriToDecodedString(searchUri);
      boolean includeMetadata = listingDetails.contains(BlobListingDetails.METADATA);
      HashSet<String> addedDirectories = new HashSet<String>();
      for (InMemoryBlockBlobStore.ListBlobEntry current : backingStore.listBlobs(
          fullPrefix, includeMetadata)) {
        int indexOfSlash = current.getKey().indexOf('/', fullPrefix.length());
        if (useFlatBlobListing || indexOfSlash < 0) {
          if (current.isPageBlob()) {
            ret.add(new MockCloudPageBlobWrapper(
                convertKeyToEncodedUri(current.getKey()),
                current.getMetadata(),
                current.getContentLength()));
          } else {
          ret.add(new MockCloudBlockBlobWrapper(
              convertKeyToEncodedUri(current.getKey()),
              current.getMetadata(),
              current.getContentLength()));
          }
        } else {
          String directoryName = current.getKey().substring(0, indexOfSlash);
          if (!addedDirectories.contains(directoryName)) {
            addedDirectories.add(current.getKey());
            ret.add(new MockCloudBlobDirectoryWrapper(new URI(
                directoryName + "/")));
          }
        }
      }
      return ret;
    }

    @Override
    public StorageUri getStorageUri() {
      throw new NotImplementedException();
    }
  }

  abstract class MockCloudBlobWrapper implements CloudBlobWrapper {
    protected final URI uri;
    protected HashMap<String, String> metadata =
        new HashMap<String, String>();
    protected BlobProperties properties;

    protected MockCloudBlobWrapper(URI uri, HashMap<String, String> metadata,
        int length) {
      this.uri = uri;
      this.metadata = metadata;
      this.properties = new BlobProperties();
      
      this.properties=updateLastModifed(this.properties);
      this.properties=updateLength(this.properties,length);
    }
    
    protected BlobProperties updateLastModifed(BlobProperties properties){
      try{
          Method setLastModified =properties.getClass().
            getDeclaredMethod("setLastModified", Date.class);
          setLastModified.setAccessible(true);
          setLastModified.invoke(this.properties,
            Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime());
      }catch(Exception e){
          throw new RuntimeException(e);
      }
      return properties;
    }
    
    protected BlobProperties updateLength(BlobProperties properties,int length) {
      try{
          Method setLength =properties.getClass().
            getDeclaredMethod("setLength", long.class);
          setLength.setAccessible(true);
          setLength.invoke(this.properties, length);
      }catch (Exception e){
         throw new RuntimeException(e);
      }
      return properties;
    }
    
    protected void refreshProperties(boolean getMetadata) {
      if (backingStore.exists(convertUriToDecodedString(uri))) {
        byte[] content = backingStore.getContent(convertUriToDecodedString(uri));
        properties = new BlobProperties();
        this.properties=updateLastModifed(this.properties);
        this.properties=updateLength(this.properties, content.length);
        if (getMetadata) {
          metadata = backingStore.getMetadata(convertUriToDecodedString(uri));
        }
      }
    }

    @Override
    public CloudBlobContainer getContainer() throws URISyntaxException,
        StorageException {
      return null;
    }

    @Override
    public CloudBlobDirectory getParent() throws URISyntaxException,
        StorageException {
      return null;
    }

    @Override
    public URI getUri() {
      return uri;
    }

    @Override
    public HashMap<String, String> getMetadata() {
      return metadata;
    }

    @Override
    public void setMetadata(HashMap<String, String> metadata) {
      this.metadata = metadata;
    }

    @Override
    public void startCopyFromBlob(URI source, BlobRequestOptions options,
        OperationContext opContext) throws StorageException, URISyntaxException {
      backingStore.copy(convertUriToDecodedString(source), convertUriToDecodedString(uri));
      //TODO: set the backingStore.properties.CopyState and
      //      update azureNativeFileSystemStore.waitForCopyToComplete
    }

    @Override
    public CopyState getCopyState() {
       return this.properties.getCopyState();
    }

    @Override
    public void delete(OperationContext opContext, SelfRenewingLease lease)
        throws StorageException {
      backingStore.delete(convertUriToDecodedString(uri));
    }

    @Override
    public boolean exists(OperationContext opContext) throws StorageException {
      return backingStore.exists(convertUriToDecodedString(uri));
    }

    @Override
    public void downloadAttributes(OperationContext opContext)
        throws StorageException {
      refreshProperties(true);
    }

    @Override
    public BlobProperties getProperties() {
      return properties;
    }

    @Override
    public InputStream openInputStream(BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      return new ByteArrayInputStream(
          backingStore.getContent(convertUriToDecodedString(uri)));
    }

    @Override
    public void uploadMetadata(OperationContext opContext)
        throws StorageException {
      backingStore.setMetadata(convertUriToDecodedString(uri), metadata);
    }

    @Override
    public void downloadRange(long offset, long length, OutputStream os,
        BlobRequestOptions options, OperationContext opContext)
        throws StorageException {
      throw new NotImplementedException();
    }
  }

  class MockCloudBlockBlobWrapper extends MockCloudBlobWrapper
    implements CloudBlockBlobWrapper {
    public MockCloudBlockBlobWrapper(URI uri, HashMap<String, String> metadata,
        int length) {
      super(uri, metadata, length);
    }

    @Override
    public OutputStream openOutputStream(BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      return backingStore.uploadBlockBlob(convertUriToDecodedString(uri),
          metadata);
    }

    @Override
    public void setStreamMinimumReadSizeInBytes(int minimumReadSizeBytes) {
    }

    @Override
    public void setWriteBlockSizeInBytes(int writeBlockSizeBytes) {
    }

    @Override
    public StorageUri getStorageUri() {
      return null;
    }

    @Override
    public void uploadProperties(OperationContext context, SelfRenewingLease lease) {
    }

    @Override
    public SelfRenewingLease acquireLease() {
      return null;
    }

    @Override
    public CloudBlob getBlob() {
      return null;
    }
  }

  class MockCloudPageBlobWrapper extends MockCloudBlobWrapper
    implements CloudPageBlobWrapper {
    public MockCloudPageBlobWrapper(URI uri, HashMap<String, String> metadata,
        int length) {
      super(uri, metadata, length);
    }

    @Override
    public void create(long length, BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      throw new NotImplementedException();
    }

    @Override
    public void uploadPages(InputStream sourceStream, long offset, long length,
        BlobRequestOptions options, OperationContext opContext)
        throws StorageException, IOException {
      throw new NotImplementedException();
    }

    @Override
    public ArrayList<PageRange> downloadPageRanges(BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      throw new NotImplementedException();
    }

    @Override
    public void setStreamMinimumReadSizeInBytes(int minimumReadSize) {
    }

    @Override
    public void setWriteBlockSizeInBytes(int writeBlockSizeInBytes) {
    }

    @Override
    public StorageUri getStorageUri() {
        throw new NotImplementedException();
    }

    @Override
    public void uploadProperties(OperationContext opContext,
        SelfRenewingLease lease)
        throws StorageException {
    }

    @Override
    public SelfRenewingLease acquireLease() {
      return null;
    }

    @Override
    public CloudBlob getBlob() {
      return null;
    }
  }
}
