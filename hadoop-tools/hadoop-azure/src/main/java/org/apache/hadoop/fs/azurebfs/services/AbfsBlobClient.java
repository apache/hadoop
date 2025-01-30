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

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ApiVersion;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsInvalidChecksumException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListXmlParser;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.StorageErrorResponseSchema;
import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ACQUIRE_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPEND_BLOB_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPEND_BLOCK;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_JSON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_OCTET_STREAM;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_XML;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCK;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCKLIST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCK_BLOB_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCK_LIST_END_TAG;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCK_LIST_START_TAG;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCK_TYPE_COMMITTED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BREAK_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COMMA;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CONTAINER;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DEFAULT_LEASE_BREAK_PERIOD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_GET;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_HEAD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HUNDRED_CONTINUE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.LATEST_BLOCK_FORMAT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.LEASE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.LIST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.METADATA;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.RELEASE_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.RENEW_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SINGLE_WHITE_SPACE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.STAR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TRUE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.XML_TAG_BLOB_ERROR_CODE_END_XML;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.XML_TAG_BLOB_ERROR_CODE_START_XML;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.XML_TAG_BLOB_ERROR_MESSAGE_END_XML;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.XML_TAG_BLOB_ERROR_MESSAGE_START_XML;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.XML_TAG_BLOCK_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.XML_TAG_COMMITTED_BLOCKS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.XML_TAG_HDI_ISFOLDER;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.XML_TAG_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.XML_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.XMS_PROPERTIES_ENCODING_ASCII;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.XMS_PROPERTIES_ENCODING_UNICODE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ZERO;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.ACCEPT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_LENGTH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_MD5;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.IF_MATCH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.IF_NONE_MATCH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.LAST_MODIFIED;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.RANGE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.USER_AGENT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_BLOB_CONTENT_MD5;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_BLOB_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_COPY_SOURCE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_BREAK_PERIOD;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_DURATION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_METADATA_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_META_HDI_ISFOLDER;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_PROPOSED_LEASE_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_RANGE_GET_CONTENT_MD5;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_SOURCE_LEASE_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_BLOCKID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_BLOCKLISTTYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_CLOSE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_COMP;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_DELIMITER;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_INCLUDE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_MARKER;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_MAX_RESULTS;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_RESTYPE;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.PATH_EXISTS;

/**
 * AbfsClient interacting with Blob endpoint.
 */
public class AbfsBlobClient extends AbfsClient {

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
   * <a href="../../../../site/markdown/blobEndpoint.md#create-container">Create Container</a>.
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
   * <a href="../../../../site/markdown/blobEndpoint.md#set-container-metadata">Set Container Metadata</a>.
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
   * <a href="../../../../site/markdown/blobEndpoint.md#get-container-properties">Get Container Metadata</a>.
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
   * <a href="../../../../site/markdown/blobEndpoint.md#delete-container">Delete Container</a>.
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
   * <a href="../../../../site/markdown/blobEndpoint.md#list-blobs">List Blobs</a>.
   * @param relativePath to return only blobs with names that begin with the specified prefix.
   * @param recursive to return all blobs in the path, including those in subdirectories.
   * @param listMaxResults maximum number of blobs to return.
   * @param continuation marker to specify the continuation token.
   * @param tracingContext for tracing the service call.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation or response parsing fails.
   */
  public AbfsRestOperation listPath(final String relativePath, final boolean recursive,
      final int listMaxResults, final String continuation, TracingContext tracingContext)
      throws IOException {
    return listPath(relativePath, recursive, listMaxResults, continuation, tracingContext, true);
  }

  public AbfsRestOperation listPath(final String relativePath, final boolean recursive,
      final int listMaxResults, final String continuation, TracingContext tracingContext,
      boolean is404CheckRequired) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESTYPE, CONTAINER);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, LIST);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_INCLUDE, METADATA);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_PREFIX, getDirectoryQueryParameter(relativePath));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_MARKER, continuation);
    if (!recursive) {
      abfsUriQueryBuilder.addQuery(QUERY_PARAM_DELIMITER, FORWARD_SLASH);
    }
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_MAX_RESULTS, String.valueOf(listMaxResults));
    appendSASTokenToQuery(relativePath, SASTokenProvider.FIXED_SAS_STORE_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.ListBlobs,
        HTTP_METHOD_GET,
        url,
        requestHeaders);

    op.execute(tracingContext);
    if (isEmptyListResults(op.getResult()) && is404CheckRequired) {
      // If the list operation returns no paths, we need to check if the path is a file.
      // If it is a file, we need to return the file in the list.
      // If it is a non-existing path, we need to throw a FileNotFoundException.
      if (relativePath.equals(ROOT_PATH)) {
        // Root Always exists as directory. It can be a empty listing.
        return op;
      }
      AbfsRestOperation pathStatus = this.getPathStatus(relativePath, tracingContext, null, false);
      BlobListResultSchema listResultSchema = getListResultSchemaFromPathStatus(relativePath, pathStatus);
      AbfsRestOperation listOp = getAbfsRestOperation(
          AbfsRestOperationType.ListBlobs,
          HTTP_METHOD_GET,
          url,
          requestHeaders);
      listOp.hardSetGetListStatusResult(HTTP_OK, listResultSchema);
      return listOp;
    }
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="../../../../site/markdown/blobEndpoint.md#put-blob">Put Blob</a>.
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
    return createPath(path, isFile, overwrite, permissions, isAppendBlob, eTag,
        contextEncryptionAdapter, tracingContext, false);
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob">Put Blob</a>.
   * Creates a file or directory (marker file) at the specified path.
   *
   * @param path the path of the directory to be created.
   * @param isFile whether the path is a file.
   * @param overwrite whether to overwrite if the path already exists.
   * @param permissions the permissions to set on the path.
   * @param isAppendBlob whether the path is an append blob.
   * @param eTag the eTag of the path.
   * @param contextEncryptionAdapter the context encryption adapter.
   * @param tracingContext the tracing context.
   * @param isCreateCalledFromMarkers whether the create is called from markers.
   * @return the executed rest operation containing the response from the server.
   * @throws AzureBlobFileSystemException if the rest operation fails.
   */
  public AbfsRestOperation createPath(final String path,
      final boolean isFile,
      final boolean overwrite,
      final AzureBlobFileSystemStore.Permissions permissions,
      final boolean isAppendBlob,
      final String eTag,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext,
      boolean isCreateCalledFromMarkers) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    if (!getIsNamespaceEnabled() && !isCreateCalledFromMarkers) {
      AbfsHttpOperation op1Result = null;
      try {
        op1Result = getPathStatus(path, tracingContext,
            null, true).getResult();
      } catch (AbfsRestOperationException ex) {
        if (ex.getStatusCode() == HTTP_NOT_FOUND) {
          LOG.debug("No directory/path found: {}", path);
        } else {
          LOG.debug("Failed to get path status for: {}", path, ex);
          throw ex;
        }
      }
      if (op1Result != null) {
        boolean isDir = checkIsDir(op1Result);
        if (isFile == isDir) {
          throw new AbfsRestOperationException(HTTP_CONFLICT,
              AzureServiceErrorCode.PATH_CONFLICT.getErrorCode(),
              PATH_EXISTS,
              null);
        }
      }
      Path parentPath = new Path(path).getParent();
      if (parentPath != null && !parentPath.isRoot()) {
        createMarkers(parentPath, overwrite, permissions, isAppendBlob, eTag,
            contextEncryptionAdapter, tracingContext);
      }
    }
    if (isFile) {
      addEncryptionKeyRequestHeaders(path, requestHeaders, true,
          contextEncryptionAdapter, tracingContext);
    } else {
      requestHeaders.add(new AbfsHttpHeader(X_MS_META_HDI_ISFOLDER, TRUE));
    }
    requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, ZERO));
    if (isAppendBlob) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_BLOB_TYPE, APPEND_BLOB_TYPE));
    } else {
      requestHeaders.add(new AbfsHttpHeader(X_MS_BLOB_TYPE, BLOCK_BLOB_TYPE));
    }
    if (!overwrite) {
      requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, AbfsHttpConstants.STAR));
    }
    if (eTag != null && !eTag.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.IF_MATCH, eTag));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    appendSASTokenToQuery(path, SASTokenProvider.FIXED_SAS_STORE_OPERATION, abfsUriQueryBuilder);

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
        AbfsHttpOperation opResult = null;
        try {
          opResult = this.getPathStatus(path, true, tracingContext, null).getResult();
        } catch (AbfsRestOperationException e) {
          if (opResult != null) {
            LOG.debug("Failed to get path status for: {} during blob type check", path, e);
            throw e;
          }
        }
        if (opResult != null && checkIsDir(opResult)) {
          return op;
        }
      }
      throw ex;
    }
    return op;
  }

  /**
   *  Creates marker blobs for the parent directories of the specified path.
   *
   * @param path The path for which parent directories need to be created.
   * @param overwrite A flag indicating whether existing directories should be overwritten.
   * @param permissions The permissions to be set for the created directories.
   * @param isAppendBlob A flag indicating whether the created blob should be of type APPEND_BLOB.
   * @param eTag The eTag to be matched for conditional requests.
   * @param contextEncryptionAdapter The encryption adapter for context encryption.
   * @param tracingContext The tracing context for the operation.
   * @throws AzureBlobFileSystemException If the creation of any parent directory fails.
   */
  private void createMarkers(final Path path,
      final boolean overwrite,
      final AzureBlobFileSystemStore.Permissions permissions,
      final boolean isAppendBlob,
      final String eTag,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    ArrayList<Path> keysToCreateAsFolder = new ArrayList<>();
    checkParentChainForFile(path, tracingContext,
        keysToCreateAsFolder);
    for (Path pathToCreate : keysToCreateAsFolder) {
      createPath(pathToCreate.toUri().getPath(), false, overwrite, permissions,
          isAppendBlob, eTag, contextEncryptionAdapter, tracingContext, true);
    }
  }

  /**
   * Checks for the entire parent hierarchy and returns if any directory exists and
   * throws an exception if any file exists.
   * @param path path to check the hierarchy for.
   * @param tracingContext the tracingcontext.
   */
  private void checkParentChainForFile(Path path, TracingContext tracingContext,
      List<Path> keysToCreateAsFolder) throws AzureBlobFileSystemException {
    AbfsHttpOperation opResult = null;
    Path current = path;
    do {
      try {
        opResult = getPathStatus(current.toUri().getPath(),
            tracingContext, null, false).getResult();
      } catch (AbfsRestOperationException ex) {
        if (ex.getStatusCode() == HTTP_NOT_FOUND) {
          LOG.debug("No explicit directory/path found: {}", current);
        } else {
          LOG.debug("Exception occurred while getting path status: {}", current, ex);
          throw ex;
        }
      }
      boolean isDirectory = opResult != null && checkIsDir(opResult);
      if (opResult != null && !isDirectory) {
        throw new AbfsRestOperationException(HTTP_CONFLICT,
            AzureServiceErrorCode.PATH_CONFLICT.getErrorCode(),
            PATH_EXISTS,
            null);
      }
      if (isDirectory) {
        return;
      }
      keysToCreateAsFolder.add(current);
      current = current.getParent();
    } while (current != null && !current.isRoot());
  }

  /**
   * Get Rest Operation for API
   * <a href="../../../../site/markdown/blobEndpoint.md#lease-blob">Lease Blob</a>.
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
    appendSASTokenToQuery(path, SASTokenProvider.FIXED_SAS_STORE_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeaseBlob,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="../../../../site/markdown/blobEndpoint.md#lease-blob">Lease Blob</a>.
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
    appendSASTokenToQuery(path, SASTokenProvider.FIXED_SAS_STORE_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeaseBlob,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="../../../../site/markdown/blobEndpoint.md#lease-blob">Lease Blob</a>.
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
    appendSASTokenToQuery(path, SASTokenProvider.FIXED_SAS_STORE_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeaseBlob,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="../../../../site/markdown/blobEndpoint.md#lease-blob">Lease Blob</a>.
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
    appendSASTokenToQuery(path, SASTokenProvider.FIXED_SAS_STORE_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeaseBlob,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get results for the rename operation.
   * @param source                    path to source file
   * @param destination               destination of rename.
   * @param continuation              continuation.
   * @param tracingContext            trace context
   * @param sourceEtag                etag of source file. may be null or empty
   * @param isMetadataIncompleteState was there a rename failure due to
   *                                  incomplete metadata state?
   * @param isNamespaceEnabled        whether namespace enabled account or not
   * @return result of rename operation
   * @throws IOException if rename operation fails.
   */
  @Override
  public AbfsClientRenameResult renamePath(final String source,
      final String destination,
      final String continuation,
      final TracingContext tracingContext,
      final String sourceEtag,
      final boolean isMetadataIncompleteState,
      final boolean isNamespaceEnabled) throws IOException {
    /**
     * TODO: [FnsOverBlob] To be implemented as part of rename-delete over blob endpoint work. <a href="https://issues.apache.org/jira/browse/HADOOP-19233">HADOOP-19233</a>.
     */
    throw new NotImplementedException("Rename operation on Blob endpoint yet to be implemented.");
  }

  /**
   * Get Rest Operation for API
   * <a href="../../../../site/markdown/blobEndpoint.md#put-block">Put Block</a>.
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
    addEncryptionKeyRequestHeaders(path, requestHeaders, false,
        contextEncryptionAdapter, tracingContext);
    requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(buffer.length)));
    if (reqParams.getLeaseId() != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, reqParams.getLeaseId()));
    }
    if (reqParams.isExpectHeaderEnabled()) {
      requestHeaders.add(new AbfsHttpHeader(EXPECT, HUNDRED_CONTINUE));
    }
    if (isChecksumValidationEnabled()) {
      addCheckSumHeaderForWrite(requestHeaders, reqParams, buffer);
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

    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.FIXED_SAS_STORE_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.PutBlock,
        HTTP_METHOD_PUT, url, requestHeaders,
        buffer, reqParams.getoffset(), reqParams.getLength(),
        sasTokenForReuse);

    try {
      op.execute(tracingContext);
    } catch (AbfsRestOperationException e) {
      /*
         If the http response code indicates a user error we retry
         the same append request with expect header being disabled.
         When "100-continue" header is enabled but a non Http 100 response comes,
         the response message might not get set correctly by the server.
         So, this handling is to avoid breaking of backward compatibility
         if someone has taken dependency on the exception message,
         which is created using the error string present in the response header.
      */
      int responseStatusCode = e.getStatusCode();
      if (checkUserError(responseStatusCode) && reqParams.isExpectHeaderEnabled()) {
        LOG.debug("User error, retrying without 100 continue enabled for the given path {}", path);
        reqParams.setExpectHeaderEnabled(false);
        reqParams.setRetryDueToExpect(true);
        return this.append(path, buffer, reqParams, cachedSasToken,
            contextEncryptionAdapter, tracingContext);
      }
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw e;
      }

      if (isMd5ChecksumError(e)) {
        throw new AbfsInvalidChecksumException(e);
      }

      throw e;
    }
    catch (AzureBlobFileSystemException e) {
      // Any server side issue will be returned as AbfsRestOperationException and will be handled above.
      LOG.debug("Append request failed with non server issues for path: {}, offset: {}, position: {}",
          path, reqParams.getoffset(), reqParams.getPosition());
      throw e;
    }
    return op;
  }

  /**
   * Appends a block to an append blob.
   * <a href="../../../../site/markdown/blobEndpoint.md#append-block">Append Block</a>.
   *
   * @param path the path of the append blob.
   * @param requestParameters the parameters for the append request.
   * @param data the data to be appended.
   * @param tracingContext the tracing context.
   * @return the executed rest operation containing the response from the server.
   * @throws AzureBlobFileSystemException if the rest operation fails.
   */
  public AbfsRestOperation appendBlock(final String path,
      AppendRequestParameters requestParameters,
      final byte[] data,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(data.length)));
    requestHeaders.add(new AbfsHttpHeader(X_MS_BLOB_TYPE, APPEND_BLOB_TYPE));
    if (requestParameters.getLeaseId() != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, requestParameters.getLeaseId()));
    }
    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, APPEND_BLOCK);
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.FIXED_SAS_STORE_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.AppendBlock,
        HTTP_METHOD_PUT,
        url,
        requestHeaders,
        data,
        requestParameters.getoffset(),
        requestParameters.getLength(),
        sasTokenForReuse);

    try {
      op.execute(tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      LOG.debug("Exception occurred during append block operation for path: {}", path, ex);
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw ex;
      }
      throw ex;
    }
    return op;
  }

  /**
   * Blob Endpoint needs blockIds to flush the data.
   * This method is not supported on Blob Endpoint.
   * @param path on which data has to be flushed.
   * @param position to which data has to be flushed.
   * @param retainUncommittedData whether to retain uncommitted data after flush.
   * @param isClose specify if this is the last flush to the file.
   * @param cachedSasToken to be used for the authenticating operation.
   * @param leaseId if there is an active lease on the path.
   * @param contextEncryptionAdapter to provide encryption context.
   * @param tracingContext for tracing the server calls.
   * @return exception as this operation is not supported on Blob Endpoint.
   * @throws UnsupportedOperationException always.
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
    throw new UnsupportedOperationException(
        "Flush without blockIds not supported on Blob Endpoint");
  }

  /**
   * Get Rest Operation for API
   * <a href="../../../../site/markdown/blobEndpoint.md#put-block-list">Put Block List</a>.
   * The flush operation to commit the blocks.
   * @param buffer This has the xml in byte format with the blockIds to be flushed.
   * @param path The path to flush the data to.
   * @param isClose True when the stream is closed.
   * @param cachedSasToken The cachedSasToken if available.
   * @param leaseId The leaseId of the blob if available.
   * @param eTag The etag of the blob.
   * @param contextEncryptionAdapter to provide encryption context.
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
      ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addEncryptionKeyRequestHeaders(path, requestHeaders, false,
        contextEncryptionAdapter, tracingContext);
    requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(buffer.length)));
    requestHeaders.add(new AbfsHttpHeader(CONTENT_TYPE, APPLICATION_XML));
    requestHeaders.add(new AbfsHttpHeader(IF_MATCH, eTag));
    if (leaseId != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));
    }
    String md5Hash = computeMD5Hash(buffer, 0, buffer.length);
    requestHeaders.add(new AbfsHttpHeader(X_MS_BLOB_CONTENT_MD5, md5Hash));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, BLOCKLIST);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CLOSE, String.valueOf(isClose));
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.FIXED_SAS_STORE_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.PutBlockList,
        HTTP_METHOD_PUT, url, requestHeaders,
        buffer, 0, buffer.length,
        sasTokenForReuse);
    try {
      op.execute(tracingContext);
    } catch (AbfsRestOperationException ex) {
      // If 412 Condition Not Met error is seen on retry it means it's either a
      // parallel write case or the previous request has failed due to network
      // issue and flush has actually succeeded in the backend. If MD5 hash of
      // blockIds matches with what was set by previous request, it means the
      // previous request itself was successful, else request will fail with 412 itself.
      if (op.getRetryCount() >= 1 && ex.getStatusCode() == HTTP_PRECON_FAILED) {
        AbfsRestOperation op1 = getPathStatus(path, true, tracingContext,
            contextEncryptionAdapter);
        String metadataMd5 = op1.getResult().getResponseHeader(CONTENT_MD5);
        if (!md5Hash.equals(metadataMd5)) {
          throw ex;
        }
        return op;
      }
      throw ex;
    }
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="../../../../site/markdown/blobEndpoint.md#set-blob-metadata">Set Blob Metadata</a>.
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
    appendSASTokenToQuery(path, SASTokenProvider.FIXED_SAS_STORE_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetPathProperties,
        HTTP_METHOD_PUT, url, requestHeaders);
    try {
      op.execute(tracingContext);
    } catch (AbfsRestOperationException ex) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw ex;
      }
      // This path could be present as an implicit directory in FNS.
      if (op.getResult().getStatusCode() == HTTP_NOT_FOUND && isNonEmptyListing(path, tracingContext)) {
        // Implicit path found, create a marker blob at this path and set properties.
        this.createPath(path, false, false, null, false, null,
            contextEncryptionAdapter, tracingContext, false);
        // Make sure hdi_isFolder is added to the list of properties to be set.
        boolean hdiIsFolderExists = properties.containsKey(XML_TAG_HDI_ISFOLDER);
        if (!hdiIsFolderExists) {
          properties.put(XML_TAG_HDI_ISFOLDER, TRUE);
        }
        return this.setPathProperties(path, properties, tracingContext, contextEncryptionAdapter);
      }
      throw ex;
    }
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="../../../../site/markdown/blobEndpoint.md#get-blob-properties">Get Blob Properties</a>.
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
    return this.getPathStatus(path, tracingContext,
        contextEncryptionAdapter, true);

  }

  /**
   * Get Rest Operation for API
   * <a href="../../../../site/markdown/blobEndpoint.md#get-blob-properties">Get Blob Properties</a>.
   * Get the properties of a file or directory.
   * @param path of which properties have to be fetched.
   * @param tracingContext for tracing the service call.
   * @param contextEncryptionAdapter to provide encryption context.
   * @param isImplicitCheckRequired specify if implicit check is required.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public AbfsRestOperation getPathStatus(final String path,
      final TracingContext tracingContext,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final boolean isImplicitCheckRequired)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN,
        String.valueOf(getAbfsConfiguration().isUpnUsed()));
    appendSASTokenToQuery(path, SASTokenProvider.FIXED_SAS_STORE_OPERATION,
        abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetBlobProperties,
        HTTP_METHOD_HEAD, url, requestHeaders);
    try {
      op.execute(tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw ex;
      }
      // This path could be present as an implicit directory in FNS.
      if (op.getResult().getStatusCode() == HTTP_NOT_FOUND
          && isImplicitCheckRequired && isNonEmptyListing(path, tracingContext)) {
        // Implicit path found.
        AbfsRestOperation successOp = getAbfsRestOperation(
            AbfsRestOperationType.GetPathStatus,
            HTTP_METHOD_HEAD, url, requestHeaders);
        successOp.hardSetGetFileStatusResult(HTTP_OK);
        return successOp;
      }
      if (op.getResult().getStatusCode() == HTTP_NOT_FOUND) {
        /*
         * Exception handling at AzureBlobFileSystem happens as per the error-code.
         * In case of HEAD call that gets 4XX status, error code is not parsed from the response.
         * Hence, we are throwing a new exception with error code and message.
         */
        throw new AbfsRestOperationException(HTTP_NOT_FOUND,
            AzureServiceErrorCode.BLOB_PATH_NOT_FOUND.getErrorCode(),
            ex.getMessage(), ex);
      }
      throw ex;
    }
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="../../../../site/markdown/blobEndpoint.md#get-blob">Get Blob</a>.
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

    // Add request header to fetch MD5 Hash of data returned by server.
    if (isChecksumValidationEnabled(requestHeaders, rangeHeader, bufferLength)) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_RANGE_GET_CONTENT_MD5, TRUE));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.FIXED_SAS_STORE_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetBlob,
        HTTP_METHOD_GET, url, requestHeaders,
        buffer, bufferOffset, bufferLength,
        sasTokenForReuse);
    op.execute(tracingContext);

    // Verify the MD5 hash returned by server holds valid on the data received.
    if (isChecksumValidationEnabled(requestHeaders, rangeHeader, bufferLength)) {
      verifyCheckSumForRead(buffer, op.getResult(), bufferOffset);
    }

    return op;
  }

  /**
   * Orchestration for delete operation to be implemented.
   * @param path to be deleted.
   * @param recursive if the path is a directory, delete recursively.
   * @param continuation to specify continuation token.
   * @param tracingContext for tracing the server calls.
   * @param isNamespaceEnabled specify if the namespace is enabled.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation deletePath(final String path,
      final boolean recursive,
      final String continuation,
      TracingContext tracingContext,
      final boolean isNamespaceEnabled) throws AzureBlobFileSystemException {
    // TODO: [FnsOverBlob][HADOOP-19233] To be implemented as part of rename-delete over blob endpoint work.
    throw new NotImplementedException("Delete operation on Blob endpoint will be implemented in future.");
  }

  /**
   * Set the owner of the file or directory.
   * Not supported for HNS-Disabled Accounts.
   * @param path on which owner has to be set.
   * @param owner to be set.
   * @param group to be set.
   * @param tracingContext for tracing the server calls.
   * @return exception as this operation is not supported on Blob Endpoint.
   * @throws UnsupportedOperationException always.
   */
  @Override
  public AbfsRestOperation setOwner(final String path,
      final String owner,
      final String group,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    throw new UnsupportedOperationException(
        "SetOwner operation is only supported on HNS enabled Accounts.");
  }

  /**
   * Set the permission of the file or directory.
   * Not supported for HNS-Disabled Accounts.
   * @param path on which permission has to be set.
   * @param permission to be set.
   * @param tracingContext for tracing the server calls.
   * @return exception as this operation is not supported on Blob Endpoint.
   * @throws UnsupportedOperationException always.
   */
  @Override
  public AbfsRestOperation setPermission(final String path,
      final String permission,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    throw new UnsupportedOperationException(
        "SetPermission operation is only supported on HNS enabled Accounts.");
  }

  /**
   * Set the ACL of the file or directory.
   * Not supported for HNS-Disabled Accounts.
   * @param path on which ACL has to be set.
   * @param aclSpecString to be set.
   * @param eTag to specify conditional headers. Set only if etag matches.
   * @param tracingContext for tracing the server calls.
   * @return exception as this operation is not supported on Blob Endpoint.
   * @throws UnsupportedOperationException always.
   */
  @Override
  public AbfsRestOperation setAcl(final String path,
      final String aclSpecString,
      final String eTag,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    throw new UnsupportedOperationException(
        "SetAcl operation is only supported on HNS enabled Accounts.");
  }

  /**
   * Get the ACL of the file or directory.
   * Not supported for HNS-Disabled Accounts.
   * @param path of which properties have to be fetched.
   * @param useUPN whether to use UPN with rest operation.
   * @param tracingContext for tracing the server calls.
   * @return exception as this operation is not supported on Blob Endpoint.
   * @throws UnsupportedOperationException always.
   */
  @Override
  public AbfsRestOperation getAclStatus(final String path,
      final boolean useUPN,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    throw new UnsupportedOperationException(
        "GetAclStatus operation is only supported on HNS enabled Accounts.");
  }

  /**
   * Check the access of the file or directory.
   * Not supported for HNS-Disabled Accounts.
   * @param path  Path for which access check needs to be performed
   * @param rwx   The permission to be checked on the path
   * @param tracingContext Tracks identifiers for request header
   * @return exception as this operation is not supported on Blob Endpoint.
   * @throws UnsupportedOperationException always.
   */
  @Override
  public AbfsRestOperation checkAccess(String path,
      String rwx,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    throw new UnsupportedOperationException(
        "CheckAccess operation is only supported on HNS enabled Accounts.");
  }

  /**
   * Get Rest Operation for API
   * <a href="../../../../site/markdown/blobEndpoint.md#get-block-list">Get Block List</a>.
   * Get the list of committed block ids of the blob.
   * @param path The path to get the list of blockId's.
   * @param tracingContext for tracing the service call.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public AbfsRestOperation getBlockList(final String path,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    String operation = SASTokenProvider.FIXED_SAS_STORE_OPERATION;
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
   * Get Rest Operation for API
   * <a href="../../../../site/markdown/blobEndpoint.md#copy-blob">Copy Blob</a>.
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
      final String srcLeaseId,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    AbfsUriQueryBuilder abfsUriQueryBuilderDst = createDefaultUriQueryBuilder();
    AbfsUriQueryBuilder abfsUriQueryBuilderSrc = new AbfsUriQueryBuilder();
    String dstBlobRelativePath = destinationBlobPath.toUri().getPath();
    String srcBlobRelativePath = sourceBlobPath.toUri().getPath();
    appendSASTokenToQuery(dstBlobRelativePath,
        SASTokenProvider.FIXED_SAS_STORE_OPERATION, abfsUriQueryBuilderDst);
    appendSASTokenToQuery(srcBlobRelativePath,
        SASTokenProvider.FIXED_SAS_STORE_OPERATION, abfsUriQueryBuilderSrc);
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
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="../../../../site/markdown/blobEndpoint.md#delete-blob">Delete Blob</a>.
   * Deletes the blob at the given path.
   * @param blobPath path of the blob to be deleted.
   * @param leaseId if path has an active lease.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public AbfsRestOperation deleteBlobPath(final Path blobPath,
      final String leaseId,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    String blobRelativePath = blobPath.toUri().getPath();
    appendSASTokenToQuery(blobRelativePath,
        SASTokenProvider.FIXED_SAS_STORE_OPERATION, abfsUriQueryBuilder);
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

  /**
   * Checks if the rest operation results indicate if the path is a directory.
   * @param result executed rest operation containing response from server.
   * @return True if the path is a directory, False otherwise.
   */
  @Override
  public boolean checkIsDir(AbfsHttpOperation result) {
    String resourceType = result.getResponseHeader(X_MS_META_HDI_ISFOLDER);
    return resourceType != null && resourceType.equals(TRUE);
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
   * Get the continuation token from the response from BLOB Endpoint Listing.
   * Continuation Token will be present in XML List response body.
   * @param result The response from the server.
   * @return The continuation token.
   */
  @Override
  public String getContinuationFromResponse(AbfsHttpOperation result) {
    BlobListResultSchema listResultSchema = (BlobListResultSchema) result.getListResultSchema();
    return listResultSchema.getNextMarker();
  }

  /**
   * Get the User-defined metadata on a path from response headers of
   * GetBlobProperties API on Blob Endpoint.
   * Blob Endpoint returns each metadata as a separate header.
   * @param result The response of GetBlobProperties call from the server.
   * @return Hashtable containing metadata key-value pairs.
   * @throws InvalidAbfsRestOperationException if parsing fails.
   */
  @Override
  public Hashtable<String, String> getXMSProperties(AbfsHttpOperation result)
      throws InvalidAbfsRestOperationException {
    Hashtable<String, String> properties = new Hashtable<>();
    Map<String, List<String>> responseHeaders = result.getResponseHeaders();
    for (Map.Entry<String, List<String>> entry : responseHeaders.entrySet()) {
      String name = entry.getKey();
      if (name != null && name.startsWith(X_MS_METADATA_PREFIX)) {
        String value;
        try {
          value = decodeMetadataAttribute(entry.getValue().get(0));
        } catch (UnsupportedEncodingException e) {
          throw new InvalidAbfsRestOperationException(e);
        }
        properties.put(name.substring(X_MS_METADATA_PREFIX.length()), value);
      }
    }
    return properties;
  }

  /**
   * Parse the XML response body returned by ListBlob API on Blob Endpoint.
   * @param stream InputStream contains the response from server.
   * @return BlobListResultSchema containing the list of entries.
   * @throws IOException if parsing fails.
   */
  @Override
  public ListResultSchema parseListPathResults(final InputStream stream) throws IOException {
    if (stream == null) {
      return null;
    }
    BlobListResultSchema listResultSchema;
    try {
      final SAXParser saxParser = saxParserThreadLocal.get();
      saxParser.reset();
      listResultSchema = new BlobListResultSchema();
      saxParser.parse(stream, new BlobListXmlParser(listResultSchema, getBaseUrl().toString()));
    } catch (SAXException | IOException e) {
      throw new RuntimeException(e);
    }

    return removeDuplicateEntries(listResultSchema);
  }

  /**
   * Parse the XML response body returned by GetBlockList API on Blob Endpoint.
   * @param stream InputStream contains the response from server.
   * @return List of blockIds.
   * @throws IOException if parsing fails.
   */
  @Override
  public List<String> parseBlockListResponse(final InputStream stream) throws IOException {
    List<String> blockIdList = new ArrayList<>();
    // Convert the input stream to a Document object

    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      Document doc = factory.newDocumentBuilder().parse(stream);

      // Find the CommittedBlocks element and extract the list of block IDs
      NodeList committedBlocksList = doc.getElementsByTagName(
          XML_TAG_COMMITTED_BLOCKS);
      if (committedBlocksList.getLength() > 0) {
        Node committedBlocks = committedBlocksList.item(0);
        NodeList blockList = committedBlocks.getChildNodes();
        for (int i = 0; i < blockList.getLength(); i++) {
          Node block = blockList.item(i);
          if (block.getNodeName().equals(XML_TAG_BLOCK_NAME)) {
            NodeList nameList = block.getChildNodes();
            for (int j = 0; j < nameList.getLength(); j++) {
              Node name = nameList.item(j);
              if (name.getNodeName().equals(XML_TAG_NAME)) {
                String blockId = name.getTextContent();
                blockIdList.add(blockId);
              }
            }
          }
        }
      }
    } catch (ParserConfigurationException | SAXException e) {
      throw new IOException(e);
    }

    return blockIdList;
  }

  /**
   * Parse the XML response body returned by error stream for all blob endpoint APIs.
   * @param stream ErrorStream contains the response from server.
   * @return StorageErrorResponseSchema containing the error code and message.
   * @throws IOException if parsing fails.
   */
  @Override
  public StorageErrorResponseSchema processStorageErrorResponse(final InputStream stream) throws IOException {
    final String data = IOUtils.toString(stream, StandardCharsets.UTF_8);
    String storageErrorCode = EMPTY_STRING;
    String storageErrorMessage = EMPTY_STRING;
    String expectedAppendPos = EMPTY_STRING;
    int codeStartFirstInstance = data.indexOf(XML_TAG_BLOB_ERROR_CODE_START_XML);
    int codeEndFirstInstance = data.indexOf(XML_TAG_BLOB_ERROR_CODE_END_XML);
    if (codeEndFirstInstance != -1 && codeStartFirstInstance != -1) {
      storageErrorCode = data.substring(codeStartFirstInstance,
          codeEndFirstInstance).replace(XML_TAG_BLOB_ERROR_CODE_START_XML, "");
    }

    int msgStartFirstInstance = data.indexOf(XML_TAG_BLOB_ERROR_MESSAGE_START_XML);
    int msgEndFirstInstance = data.indexOf(XML_TAG_BLOB_ERROR_MESSAGE_END_XML);
    if (msgEndFirstInstance != -1 && msgStartFirstInstance != -1) {
      storageErrorMessage = data.substring(msgStartFirstInstance,
          msgEndFirstInstance).replace(XML_TAG_BLOB_ERROR_MESSAGE_START_XML, "");
    }
    return new StorageErrorResponseSchema(storageErrorCode, storageErrorMessage, expectedAppendPos);
  }

  /**
   * Encode the value of the attribute to be set as metadata.
   * Blob Endpoint support Unicode characters in metadata values.
   * @param value to be encoded.
   * @return encoded value.
   * @throws UnsupportedEncodingException if encoding fails.
   */
  @Override
  public byte[] encodeAttribute(String value) throws UnsupportedEncodingException {
    return value.getBytes(XMS_PROPERTIES_ENCODING_UNICODE);
  }

  /**
   * Decode the value of the attribute from the metadata.
   * Blob Endpoint support Unicode characters in metadata values.
   * @param value to be decoded.
   * @return decoded value.
   * @throws UnsupportedEncodingException if decoding fails.
   */
  @Override
  public String decodeAttribute(byte[] value) throws UnsupportedEncodingException {
    return new String(value, XMS_PROPERTIES_ENCODING_UNICODE);
  }

  /**
   * Blob Endpoint Supports Delimiter based listing where the
   * directory path to be listed must end with a Forward Slash.
   * @param path directory path to be listed.
   * @return directory path with forward slash at end.
   */
  public static String getDirectoryQueryParameter(final String path) {
    String directory = AbfsClient.getDirectoryQueryParameter(path);
    if (directory.isEmpty()) {
      return directory;
    }
    if (!directory.endsWith(FORWARD_SLASH)) {
      directory = directory + FORWARD_SLASH;
    }
    return directory;
  }

  /**
   * Checks if the value contains pure ASCII characters or not.
   * @param value to be checked.
   * @return true if pureASCII.
   * @throws CharacterCodingException if not pure ASCII
   */
  private boolean isPureASCII(String value) throws CharacterCodingException {
    final CharsetEncoder encoder = Charset.forName(
        XMS_PROPERTIES_ENCODING_ASCII).newEncoder();
    boolean canEncodeValue = encoder.canEncode(value);
    if (!canEncodeValue) {
      LOG.debug("Value {} for ne of the metadata is not pure ASCII.", value);
      throw new CharacterCodingException();
    }
    return true;
  }

  /**
   * Get the list of headers to be set for metadata properties.
   * Blob Endpoint accepts each metadata as a separate header.
   * @param properties to be set as metadata
   * @return List of headers to be set.
   * @throws AbfsRestOperationException if encoding fails.
   * @throws CharacterCodingException if value is not pure ASCII.
   */
  private List<AbfsHttpHeader> getMetadataHeadersList(final Hashtable<String, String> properties)
      throws AbfsRestOperationException, CharacterCodingException {
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

  private final ThreadLocal<SAXParser> saxParserThreadLocal = ThreadLocal.withInitial(() -> {
    SAXParserFactory factory = SAXParserFactory.newInstance();
    factory.setNamespaceAware(true);
    try {
      return factory.newSAXParser();
    } catch (SAXException e) {
      throw new RuntimeException("Unable to create SAXParser", e);
    } catch (ParserConfigurationException e) {
      throw new RuntimeException("Check parser configuration", e);
    }
  });

  /**
   * This is to handle duplicate listing entries returned by Blob Endpoint for
   * implicit paths that also has a marker file created for them.
   * This will retain entry corresponding to marker file and remove the BlobPrefix entry.
   * @param listResultSchema List of entries returned by Blob Endpoint.
   * @return List of entries after removing duplicates.
   */
  private BlobListResultSchema removeDuplicateEntries(BlobListResultSchema listResultSchema) {
    List<BlobListResultEntrySchema> uniqueEntries = new ArrayList<>();
    TreeMap<String, BlobListResultEntrySchema> nameToEntryMap = new TreeMap<>();

    for (BlobListResultEntrySchema entry : listResultSchema.paths()) {
      if (StringUtils.isNotEmpty(entry.eTag())) {
        // This is a blob entry. It is either a file or a marker blob.
        // In both cases we will add this.
        nameToEntryMap.put(entry.name(), entry);
      } else {
        // This is a BlobPrefix entry. It is a directory with file inside
        // This might have already been added as a marker blob.
        if (!nameToEntryMap.containsKey(entry.name())) {
          nameToEntryMap.put(entry.name(), entry);
        }
      }
    }

    uniqueEntries.addAll(nameToEntryMap.values());
    listResultSchema.withPaths(uniqueEntries);
    return listResultSchema;
  }

  /**
   * When listing is done on a file, Blob Endpoint returns the empty listing
   * but DFS Endpoint returns the file status as one of the entries.
   * This is to convert file status into ListResultSchema.
   * @param relativePath
   * @param pathStatus
   * @return
   */
  private BlobListResultSchema getListResultSchemaFromPathStatus(String relativePath, AbfsRestOperation pathStatus) {
    BlobListResultSchema listResultSchema = new BlobListResultSchema();

    BlobListResultEntrySchema entrySchema = new BlobListResultEntrySchema();
    entrySchema.setUrl(pathStatus.getUrl().toString());
    entrySchema.setPath(new Path(relativePath));
    entrySchema.setName(relativePath.charAt(0) == '/' ? relativePath.substring(1) : relativePath);
    entrySchema.setIsDirectory(checkIsDir(pathStatus.getResult()));
    entrySchema.setContentLength(Long.parseLong(pathStatus.getResult().getResponseHeader(CONTENT_LENGTH)));
    entrySchema.setLastModifiedTime(
        pathStatus.getResult().getResponseHeader(LAST_MODIFIED));
    entrySchema.setETag(AzureBlobFileSystemStore.extractEtagHeader(pathStatus.getResult()));

    // If listing is done on explicit directory, do not include directory in the listing.
    if (!entrySchema.isDirectory()) {
      listResultSchema.paths().add(entrySchema);
    }
    return listResultSchema;
  }

  private static String encodeMetadataAttribute(String value)
      throws UnsupportedEncodingException {
    return value == null ? null
        : URLEncoder.encode(value, StandardCharsets.UTF_8.name());
  }

  private static String decodeMetadataAttribute(String encoded)
      throws UnsupportedEncodingException {
    return encoded == null ? null
        : java.net.URLDecoder.decode(encoded, StandardCharsets.UTF_8.name());
  }

  private boolean isNonEmptyListing(String path,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    AbfsRestOperation listOp = listPath(path, false, 1, null, tracingContext, false);
    return !isEmptyListResults(listOp.getResult());
  }

  /**
   * Check if the list call returned empty results without any continuation token.
   * @param result The response of listing API from the server.
   * @return True if empty results without continuation token.
   */
  private boolean isEmptyListResults(AbfsHttpOperation result) {
    boolean isEmptyList = result != null && result.getStatusCode() == HTTP_OK && // List Call was successful
        result.getListResultSchema() != null && // Parsing of list response was successful
        result.getListResultSchema().paths().isEmpty() && // No paths were returned
        result.getListResultSchema() instanceof BlobListResultSchema && // It is safe to typecast to BlobListResultSchema
        ((BlobListResultSchema) result.getListResultSchema()).getNextMarker() == null; // No continuation token was returned
    if (isEmptyList) {
      LOG.debug("List call returned empty results without any continuation token.");
      return true;
    } else if (result != null && !(result.getListResultSchema() instanceof BlobListResultSchema)) {
      throw new RuntimeException("List call returned unexpected results over Blob Endpoint.");
    }
    return false;
  }

  /**
   * Generates an XML string representing the block list.
   *
   * @param blockIds the set of block IDs
   * @return the generated XML string
   */
  public static String generateBlockListXml(List<String> blockIds) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(String.format(XML_VERSION));
    stringBuilder.append(String.format(BLOCK_LIST_START_TAG));
    for (String blockId : blockIds) {
      stringBuilder.append(String.format(LATEST_BLOCK_FORMAT, blockId));
    }
    stringBuilder.append(String.format(BLOCK_LIST_END_TAG));
    return stringBuilder.toString();
  }
}
