/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ApiVersion;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ACQUIRE_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_JSON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_OCTET_STREAM;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_XML;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCK;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCKLIST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCK_BLOB_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCK_TYPE_COMMITTED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BREAK_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COMMA;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CONTAINER;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DEFAULT_LEASE_BREAK_PERIOD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_GET;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_HEAD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HUNDRED_CONTINUE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.LEASE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.METADATA;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.RELEASE_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.RENEW_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SINGLE_WHITE_SPACE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.STAR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TRUE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.XMS_PROPERTIES_ENCODING_ASCII;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.XMS_PROPERTIES_ENCODING_UNICODE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ZERO;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.ACCEPT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_LENGTH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.IF_MATCH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.IF_NONE_MATCH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.RANGE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.USER_AGENT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_BLOB_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_COPY_SOURCE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_BREAK_PERIOD;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_DURATION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_METADATA_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_META_HDI_ISFOLDER;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_PROPOSED_LEASE_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_SOURCE_LEASE_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_BLOCKID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_BLOCKLISTTYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_CLOSE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_COMP;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_RESTYPE;

/**
 * AbfsClient interacting with Blob endpoint.
 */
public class AbfsBlobClient extends AbfsClient implements Closeable {

  public AbfsBlobClient(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final AccessTokenProvider tokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    super(baseUrl, sharedKeyCredentials, abfsConfiguration, tokenProvider,
        encryptionContextProvider, abfsClientContext);
  }

  public AbfsBlobClient(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final SASTokenProvider sasTokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    super(baseUrl, sharedKeyCredentials, abfsConfiguration, sasTokenProvider,
        encryptionContextProvider, abfsClientContext);
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  /**
   * Create request headers for Rest Operation using the default API version.
   * @return default request headers.
   */
  @Override
  public List<AbfsHttpHeader> createDefaultHeaders() {
    return this.createDefaultHeaders(getxMsVersion());
  }

  /**
   * Create request headers for Rest Operation using the specified API version.
   * Blob Endpoint API responses are in JSON/XML format.
   * @param xMsVersion API version to be used.
   * @return default request headers
   */
  @Override
  public List<AbfsHttpHeader> createDefaultHeaders(ApiVersion xMsVersion) {
    List<AbfsHttpHeader> requestHeaders = super.createCommonHeaders(xMsVersion);
    requestHeaders.add(new AbfsHttpHeader(ACCEPT, APPLICATION_JSON
        + COMMA + SINGLE_WHITE_SPACE + APPLICATION_OCTET_STREAM
        + COMMA + SINGLE_WHITE_SPACE + APPLICATION_XML));
    return requestHeaders;
  }

  /**
   * Get Rest Operation for API
   * <a href=" https://learn.microsoft.com/en-us/rest/api/storageservices/create-container">
   *   Create Container</a>.
   * Creates a storage container as filesystem root.
   * @param tracingContext for tracing the service call.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation createFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = new AbfsUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESTYPE, CONTAINER);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.CreateContainer,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/set-container-metadata">
   *   Set Container Metadata</a>.
   * Sets user-defined properties of the filesystem.
   * @param properties comma separated list of metadata key-value pairs.
   * @param tracingContext for tracing the service call.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation setFilesystemProperties(final Hashtable<String, String> properties,
      TracingContext tracingContext) throws AzureBlobFileSystemException  {
    List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    /*
     * Blob Endpoint supports Unicode characters but DFS Endpoint only allow ASCII.
     * To match the behavior across endpoints, driver throws exception if non-ASCII characters are found.
     */
    try {
      List<AbfsHttpHeader> metadataRequestHeaders = getMetadataHeadersList(properties);
      requestHeaders.addAll(metadataRequestHeaders);
    } catch (CharacterCodingException ex) {
      throw new InvalidAbfsRestOperationException(ex);
    }

    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESTYPE, CONTAINER);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, METADATA);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetContainerMetadata,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/get-container-properties">
   *   Get Container Metadata</a>.
   * Gets all the properties of the filesystem.
   * @param tracingContext for tracing the service call.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   * */
  @Override
  public AbfsRestOperation getFilesystemProperties(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESTYPE, CONTAINER);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetContainerProperties,
        HTTP_METHOD_HEAD, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/delete-container">
   *   Delete Container</a>.
   * Deletes the Container acting as current filesystem.
   * @param tracingContext for tracing the service call.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation deleteFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESTYPE, CONTAINER);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.DeleteContainer,
        HTTP_METHOD_DELETE, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob">
   *   Put Blob</a>.
   * Creates a file or directory(marker file) at specified path.
   * @param path of the directory to be created.
   * @param tracingContext for tracing the service call.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation createPath(final String path,
      final boolean isFile,
      final boolean overwrite,
      final AzureBlobFileSystemStore.Permissions permissions,
      final boolean isAppendBlob,
      final String eTag,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, ZERO));
    requestHeaders.add(new AbfsHttpHeader(X_MS_BLOB_TYPE, BLOCK_BLOB_TYPE));
    if (!overwrite) {
      requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, STAR));
    }
    if (eTag != null && !eTag.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.IF_MATCH, eTag));
    }
    if (!isFile) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_META_HDI_ISFOLDER, TRUE));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    appendSASTokenToQuery(path, SASTokenProvider.CREATE_FILE_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.PutBlob,
        HTTP_METHOD_PUT, url, requestHeaders);
    try {
      op.execute(tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw ex;
      }
      if (!isFile && op.getResult().getStatusCode() == HTTP_CONFLICT) {
        // This ensures that we don't throw ex only for existing directory but if a blob exists we throw exception.
        final AbfsHttpOperation opResult = this.getPathStatus(
            path, true, tracingContext, null).getResult();
        if (checkIsDir(opResult)) {
          return op;
        }
      }
      throw ex;
    }
    return op;
  }

  /**
   * Get Rest Operation for API <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs"></a>.
   * @param relativePath to return only blobs with names that begin with the specified prefix.
   * @param recursive to return all blobs in the path, including those in subdirectories.
   * @param listMaxResults maximum number of blobs to return.
   * @param continuation marker to specify the continuation token.
   * @param tracingContext for tracing the service call.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation or response parsing fails.
   */
  @Override
  public AbfsRestOperation listPath(final String relativePath, final boolean recursive,
      final int listMaxResults, final String continuation, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    // Todo: [FnsOverBlob] To be implemented as part of response handling of blob endpoint APIs.
    throw new NotImplementedException("Blob Endpoint Support is not yet implemented");
  }

  /**
   * Get Rest Operation for API <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob">Lease Blob</a>.
   * @param path on which lease has to be acquired.
   * @param duration for which lease has to be acquired.
   * @param tracingContext for tracing the service call.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation acquireLease(final String path, final int duration,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, ACQUIRE_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_DURATION, Integer.toString(duration)));
    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPOSED_LEASE_ID, UUID.randomUUID().toString()));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, LEASE);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeaseBlob,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob">Lease Blob</a>.
   * @param path on which lease has to be renewed.
   * @param leaseId of the lease to be renewed.
   * @param tracingContext for tracing the service call.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation renewLease(final String path, final String leaseId,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, RENEW_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, LEASE);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeaseBlob,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob">Lease Blob</a>.
   * @param path on which lease has to be released.
   * @param leaseId of the lease to be released.
   * @param tracingContext for tracing the service call.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation releaseLease(final String path, final String leaseId,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, RELEASE_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, LEASE);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeaseBlob,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob">Lease Blob</a>.
   * @param path on which lease has to be broken.
   * @param tracingContext for tracing the service call.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation breakLease(final String path,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, BREAK_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_BREAK_PERIOD, DEFAULT_LEASE_BREAK_PERIOD));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, LEASE);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeaseBlob,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  @Override
  public AbfsClientRenameResult renamePath(final String source,
      final String destination,
      final String continuation,
      final TracingContext tracingContext,
      final String sourceEtag,
      final boolean isMetadataIncompleteState,
      final boolean isNamespaceEnabled) throws IOException {
    // Todo: [FnsOverBlob] To be implemented as part of rename-delete over blob endpoint work.
    throw new NotImplementedException("Rename operation on Blob endpoint will be implemented in future.");
  }

  /**
   * Get Rest Operation for API <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob">Get Blob</a>.
   * Read the contents of the file at specified path
   * @param path of the file to be read.
   * @param position in the file from where data has to be read.
   * @param buffer to store the data read.
   * @param bufferOffset offset in the buffer to start storing the data.
   * @param bufferLength length of data to be read.
   * @param eTag to specify conditional headers.
   * @param cachedSasToken to be used for the authenticating operation.
   * @param contextEncryptionAdapter to provide encryption context.
   * @param tracingContext for tracing the service call.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation read(final String path,
      final long position,
      final byte[] buffer,
      final int bufferOffset,
      final int bufferLength,
      final String eTag,
      final String cachedSasToken,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    AbfsHttpHeader rangeHeader = new AbfsHttpHeader(RANGE, String.format(
        "bytes=%d-%d", position, position + bufferLength - 1));
    requestHeaders.add(rangeHeader);
    requestHeaders.add(new AbfsHttpHeader(IF_MATCH, eTag));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.READ_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetBlob,
        HTTP_METHOD_GET, url, requestHeaders,
        buffer, bufferOffset, bufferLength,
        sasTokenForReuse);
    op.execute(tracingContext);

    return op;
  }

  /**
   * Get Rest Operation for API <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/put-block">Put Block</a>.
   * Uploads data to be appended to a file.
   * @param path to which data has to be appended.
   * @param buffer containing data to be appended.
   * @param reqParams containing parameters for append operation like offset, length etc.
   * @param cachedSasToken to be used for the authenticating operation.
   * @param contextEncryptionAdapter to provide encryption context.
   * @param tracingContext for tracing the service call.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation append(final String path,
      final byte[] buffer,
      final AppendRequestParameters reqParams,
      final String cachedSasToken,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(buffer.length)));
    requestHeaders.add(new AbfsHttpHeader(IF_MATCH, reqParams.getETag()));
    if (reqParams.getLeaseId() != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, reqParams.getLeaseId()));
    }
    if (reqParams.isExpectHeaderEnabled()) {
      requestHeaders.add(new AbfsHttpHeader(EXPECT, HUNDRED_CONTINUE));
    }

    if (reqParams.isRetryDueToExpect()) {
      String userAgentRetry = getUserAgent();
      userAgentRetry = userAgentRetry.replace(HUNDRED_CONTINUE_USER_AGENT, EMPTY_STRING);
      requestHeaders.removeIf(header -> header.getName().equalsIgnoreCase(USER_AGENT));
      requestHeaders.add(new AbfsHttpHeader(USER_AGENT, userAgentRetry));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, BLOCK);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_BLOCKID, reqParams.getBlockId());

    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.WRITE_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.PutBlock,
        HTTP_METHOD_PUT, url, requestHeaders,
        buffer, reqParams.getoffset(), reqParams.getLength(),
        sasTokenForReuse);

    try {
      op.execute(tracingContext);
    } catch (AzureBlobFileSystemException e) {
      /*
         If the http response code indicates a user error we retry
         the same append request with expect header being disabled.
         When "100-continue" header is enabled but a non Http 100 response comes,
         the response message might not get set correctly by the server.
         So, this handling is to avoid breaking of backward compatibility
         if someone has taken dependency on the exception message,
         which is created using the error string present in the response header.
      */
      int responseStatusCode = ((AbfsRestOperationException) e).getStatusCode();
      if (checkUserError(responseStatusCode) && reqParams.isExpectHeaderEnabled()) {
        LOG.debug("User error, retrying without 100 continue enabled for the given path {}", path);
        reqParams.setExpectHeaderEnabled(false);
        reqParams.setRetryDueToExpect(true);
        return this.append(path, buffer, reqParams, cachedSasToken,
            contextEncryptionAdapter, tracingContext);
      }
      else {
        throw e;
      }
    }
    return op;
  }

  /**
   * Redirect to flush specific to blob endpoint
   */
  @Override
  public AbfsRestOperation flush(final String path,
      final long position,
      final boolean retainUncommittedData,
      final boolean isClose,
      final String cachedSasToken,
      final String leaseId,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    return this.flush(new byte[0], path, isClose, cachedSasToken, leaseId, "",
        tracingContext);
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list">
   *   Put Block List</a>.
   * The flush operation to commit the blocks.
   * @param buffer This has the xml in byte format with the blockIds to be flushed.
   * @param path The path to flush the data to.
   * @param isClose True when the stream is closed.
   * @param cachedSasToken The cachedSasToken if available.
   * @param leaseId The leaseId of the blob if available.
   * @param eTag The etag of the blob.
   * @param tracingContext for tracing the service call.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation flush(byte[] buffer,
      final String path,
      boolean isClose,
      final String cachedSasToken,
      final String leaseId,
      final String eTag,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(buffer.length)));
    requestHeaders.add(new AbfsHttpHeader(CONTENT_TYPE, APPLICATION_XML));
    requestHeaders.add(new AbfsHttpHeader(IF_MATCH, eTag));
    if (leaseId != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, BLOCKLIST);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CLOSE, String.valueOf(isClose));
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.WRITE_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.PutBlockList,
        HTTP_METHOD_PUT, url, requestHeaders,
        buffer, 0, buffer.length,
        sasTokenForReuse);

    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/set-blob-metadata">
   *   Set Blob Metadata</a>.
   * Set the properties of a file or directory.
   * @param path on which properties have to be set.
   * @param properties comma separated list of metadata key-value pairs.
   * @param tracingContext for tracing the service call.
   * @param contextEncryptionAdapter to provide encryption context.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation setPathProperties(final String path,
      final Hashtable<String, String> properties,
      final TracingContext tracingContext,
      final ContextEncryptionAdapter contextEncryptionAdapter)
      throws AzureBlobFileSystemException {
    List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    /*
     * Blob Endpoint supports Unicode characters but DFS Endpoint only allow ASCII.
     * To match the behavior across endpoints, driver throws exception if non-ASCII characters are found.
     */
    try {
      List<AbfsHttpHeader> metadataRequestHeaders = getMetadataHeadersList(properties);
      requestHeaders.addAll(metadataRequestHeaders);
    } catch (CharacterCodingException ex) {
      throw new InvalidAbfsRestOperationException(ex);
    }

    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, METADATA);
    appendSASTokenToQuery(path, SASTokenProvider.SET_PROPERTIES_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetPathProperties,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob-properties">
   *   Get Blob Properties</a>.
   * Get the properties of a file or directory.
   * @param path of which properties have to be fetched.
   * @param includeProperties to include user defined properties.
   * @param tracingContext for tracing the service call.
   * @param contextEncryptionAdapter to provide encryption context.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation getPathStatus(final String path,
      final boolean includeProperties,
      final TracingContext tracingContext,
      final ContextEncryptionAdapter contextEncryptionAdapter)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN,
        String.valueOf(getAbfsConfiguration().isUpnUsed()));
    appendSASTokenToQuery(path, SASTokenProvider.GET_PROPERTIES_OPERATION,
        abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetPathStatus,
        HTTP_METHOD_HEAD, url, requestHeaders);
    try {
      op.execute(tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw ex;
      }
      if (op.getResult().getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
        // This path could be present as an implicit directory in FNS.
        AbfsRestOperation listOp = listPath(path, false, 1, null, tracingContext);
        // Todo: [FnsOverBlob] To be implemented as part of response handling of blob endpoint APIs.
        return listOp;
      }
      throw ex;
    }
    return op;
  }

  @Override
  public AbfsRestOperation deletePath(final String path,
      final boolean recursive,
      final String continuation,
      TracingContext tracingContext,
      final boolean isNamespaceEnabled) throws AzureBlobFileSystemException {
    // Todo: [FnsOverBlob] To be implemented as part of rename-delete over blob endpoint work.
    throw new NotImplementedException("Delete operation on Blob endpoint will be implemented in future.");
  }

  @Override
  public AbfsRestOperation setOwner(final String path,
      final String owner,
      final String group,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    throw new UnsupportedOperationException(
        "SetOwner operation is only supported on HNS enabled Accounts.");
  }

  @Override
  public AbfsRestOperation setPermission(final String path,
      final String permission,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    throw new UnsupportedOperationException(
        "SetPermission operation is only supported on HNS enabled Accounts.");
  }

  @Override
  public AbfsRestOperation setAcl(final String path,
      final String aclSpecString,
      final String eTag,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    throw new UnsupportedOperationException(
        "SetAcl operation is only supported on HNS enabled Accounts.");
  }

  @Override
  public AbfsRestOperation getAclStatus(final String path, final boolean useUPN,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    throw new UnsupportedOperationException(
        "GetAclStatus operation is only supported on HNS enabled Accounts.");
  }

  @Override
  public AbfsRestOperation checkAccess(String path, String rwx, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    throw new UnsupportedOperationException(
        "CheckAccess operation is only supported on HNS enabled Accounts.");
  }

  @Override
  public boolean checkIsDir(AbfsHttpOperation result) {
    boolean isDirectory = (result.getResponseHeader(X_MS_META_HDI_ISFOLDER) != null);
    if (isDirectory) {
      return true;
    }
    return false;
  }

  /**
   * Returns true if the status code lies in the range of user error.
   * In the case of HTTP_CONFLICT for PutBlockList we fall back to DFS and hence
   * this retry handling is not needed.
   * @param responseStatusCode http response status code.
   * @return True or False.
   */
  @Override
  public boolean checkUserError(int responseStatusCode) {
    return (responseStatusCode >= HttpURLConnection.HTTP_BAD_REQUEST
        && responseStatusCode < HttpURLConnection.HTTP_INTERNAL_ERROR
        && responseStatusCode != HttpURLConnection.HTTP_CONFLICT);
  }

  /**
   * Get Rest Operation for API <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/get-block-list">Get Block List</a>.
   * Get the list of committed block ids of the blob.
   * @param path The path to get the list of blockId's.
   * @param tracingContext for tracing the service call.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public AbfsRestOperation getBlockList(final String path, TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    String operation = SASTokenProvider.READ_OPERATION;
    appendSASTokenToQuery(path, operation, abfsUriQueryBuilder);

    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, BLOCKLIST);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_BLOCKLISTTYPE, BLOCK_TYPE_COMMITTED);
    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());

    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetBlockList, HTTP_METHOD_GET, url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/copy-blob">Copy Blob</a>.
   * This is an asynchronous API, it returns copyId and expects client
   * to poll the server on the destination and check the copy-progress.
   * @param sourceBlobPath path of source to be copied.
   * @param destinationBlobPath path of the destination.
   * @param srcLeaseId if source path has an active lease.
   * @param tracingContext for tracing the service call.
   * @return executed rest operation containing response from server.
   * This method owns the logic of triggering copyBlob API. The caller of this
   * method have to own the logic of polling the destination with the copyId
   * returned in the response from this method.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public AbfsRestOperation copyBlob(Path sourceBlobPath,
      Path destinationBlobPath,
      final String srcLeaseId, TracingContext tracingContext) throws AzureBlobFileSystemException {
    AbfsUriQueryBuilder abfsUriQueryBuilderDst = createDefaultUriQueryBuilder();
    AbfsUriQueryBuilder abfsUriQueryBuilderSrc = new AbfsUriQueryBuilder();
    String dstBlobRelativePath = destinationBlobPath.toUri().getPath();
    String srcBlobRelativePath = sourceBlobPath.toUri().getPath();
    appendSASTokenToQuery(dstBlobRelativePath,
        SASTokenProvider.WRITE_OPERATION, abfsUriQueryBuilderDst);
    appendSASTokenToQuery(srcBlobRelativePath,
        SASTokenProvider.READ_OPERATION, abfsUriQueryBuilderSrc);
    final URL url = createRequestUrl(dstBlobRelativePath,
        abfsUriQueryBuilderDst.toString());
    final String sourcePathUrl = createRequestUrl(srcBlobRelativePath,
        abfsUriQueryBuilderSrc.toString()).toString();
    List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    if (srcLeaseId != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_SOURCE_LEASE_ID, srcLeaseId));
    }
    requestHeaders.add(new AbfsHttpHeader(X_MS_COPY_SOURCE, sourcePathUrl));
    requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, STAR));

    final AbfsRestOperation op = getAbfsRestOperation(AbfsRestOperationType.CopyBlob, HTTP_METHOD_PUT,
        url, requestHeaders);

    return op;
  }

  /**
   * Get Rest Operation for API <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/delete-blob">Delete Blob</a>.
   * Deletes the blob at the given path.
   * @param blobPath path of the blob to be deleted.
   * @param leaseId if path has an active lease.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public AbfsRestOperation deleteBlobPath(final Path blobPath,
      final String leaseId, final TracingContext tracingContext) throws AzureBlobFileSystemException {
    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    String blobRelativePath = blobPath.toUri().getPath();
    appendSASTokenToQuery(blobRelativePath,
        SASTokenProvider.DELETE_OPERATION, abfsUriQueryBuilder);
    final URL url = createRequestUrl(blobRelativePath, abfsUriQueryBuilder.toString());
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    if (leaseId != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));
    }
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.DeleteBlob, HTTP_METHOD_DELETE, url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  private static String encodeMetadataAttribute(String value)
      throws UnsupportedEncodingException {
    return value == null ? null
        : URLEncoder.encode(value, XMS_PROPERTIES_ENCODING_UNICODE);
  }

  private static String decodeMetadataAttribute(String encoded)
      throws UnsupportedEncodingException {
    return encoded == null ? null
        : java.net.URLDecoder.decode(encoded, XMS_PROPERTIES_ENCODING_UNICODE);
  }

  /**
   * Checks if the value contains pure ASCII characters or not.
   * @param value
   * @return true if pureASCII.
   * @throws CharacterCodingException if not pure ASCII
   */
  private boolean isPureASCII(String value) throws CharacterCodingException {
    final CharsetEncoder encoder = Charset.forName(
        XMS_PROPERTIES_ENCODING_ASCII).newEncoder();
    boolean canEncodeValue = encoder.canEncode(value);
    if (!canEncodeValue) {
      throw new CharacterCodingException();
    }
    return true;
  }

  private List<AbfsHttpHeader> getMetadataHeadersList(final Hashtable<String, String> properties) throws AbfsRestOperationException,
      CharacterCodingException {
    List<AbfsHttpHeader> metadataRequestHeaders = new ArrayList<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = X_MS_METADATA_PREFIX + entry.getKey();
      String value = entry.getValue();
      // AzureBlobFileSystem supports only ASCII Characters in property values.
      if (isPureASCII(value)) {
        try {
          value = encodeMetadataAttribute(value);
        } catch (UnsupportedEncodingException e) {
          throw new InvalidAbfsRestOperationException(e);
        }
        metadataRequestHeaders.add(new AbfsHttpHeader(key, value));
      }
    }
    return metadataRequestHeaders;
  }
}
