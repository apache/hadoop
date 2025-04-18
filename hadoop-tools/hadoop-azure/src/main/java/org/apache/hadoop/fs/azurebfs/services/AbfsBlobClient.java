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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ApiVersion;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsInvalidChecksumException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.ConcurrentWriteOperationDetectedException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListXmlParser;
import org.apache.hadoop.fs.azurebfs.contracts.services.StorageErrorResponseSchema;
import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CALL_GET_FILE_STATUS;
import static org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.extractEtagHeader;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ACQUIRE_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.AND_MARK;
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
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_BLOB_LIST_PARSING;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.PATH_EXISTS;
import static org.apache.hadoop.fs.azurebfs.utils.UriUtils.isKeyForDirectorySet;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ATOMIC_DIR_RENAME_RECOVERY_ON_GET_PATH_EXCEPTION;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_DELETE_BLOB;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_RENAME_BLOB;

/**
 * AbfsClient interacting with Blob endpoint.
 */
public class AbfsBlobClient extends AbfsClient {

  private final HashSet<String> azureAtomicRenameDirSet;

  public AbfsBlobClient(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final AccessTokenProvider tokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    super(baseUrl, sharedKeyCredentials, abfsConfiguration, tokenProvider,
        encryptionContextProvider, abfsClientContext);
    this.azureAtomicRenameDirSet = new HashSet<>(Arrays.asList(
        abfsConfiguration.getAzureAtomicRenameDirs()
            .split(AbfsHttpConstants.COMMA)));
  }

  public AbfsBlobClient(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final SASTokenProvider sasTokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    super(baseUrl, sharedKeyCredentials, abfsConfiguration, sasTokenProvider,
        encryptionContextProvider, abfsClientContext);
    this.azureAtomicRenameDirSet = new HashSet<>(Arrays.asList(
        abfsConfiguration.getAzureAtomicRenameDirs()
            .split(AbfsHttpConstants.COMMA)));
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
   * @param uri to be used for path conversion.
   * @return {@link ListResponseData}. containing listing response.
   * @throws AzureBlobFileSystemException if rest operation or response parsing fails.
   */
  @Override
  public ListResponseData listPath(final String relativePath, final boolean recursive,
      final int listMaxResults, final String continuation, TracingContext tracingContext, URI uri) throws IOException {
    return listPath(relativePath, recursive, listMaxResults, continuation, tracingContext, uri, true);
  }

  public ListResponseData listPath(final String relativePath, final boolean recursive,
      final int listMaxResults, final String continuation, TracingContext tracingContext, URI uri, boolean is404CheckRequired)
      throws AzureBlobFileSystemException {
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
    ListResponseData listResponseData = parseListPathResults(op.getResult(), uri);
    listResponseData.setOp(op);

    // Perform Pending Rename Redo Operation on Atomic Rename Paths.
    // Crashed HBase log rename recovery can be done by Filesystem.listStatus.
    if (tracingContext.getOpType() == FSOperationType.LISTSTATUS
        && op.getResult() != null
        && op.getResult().getStatusCode() == HTTP_OK) {
      boolean isRenameRecovered = retryRenameOnAtomicEntriesInListResults(tracingContext,
          listResponseData.getRenamePendingJsonPaths());
      if (isRenameRecovered) {
        LOG.debug("Retrying list operation after rename recovery.");
        // Retry the list operation to get the updated list of paths after rename recovery.
        AbfsRestOperation retryListOp = getAbfsRestOperation(
            AbfsRestOperationType.ListBlobs,
            HTTP_METHOD_GET,
            url,
            requestHeaders);
        retryListOp.execute(tracingContext);
        listResponseData = parseListPathResults(retryListOp.getResult(), uri);
        listResponseData.setOp(retryListOp);
      }
    }

    if (isEmptyListResults(listResponseData) && is404CheckRequired) {
      // If the list operation returns no paths, we need to check if the path is a file.
      // If it is a file, we need to return the file in the list.
      // If it is a non-existing path, we need to throw a FileNotFoundException.
      if (relativePath.equals(ROOT_PATH)) {
        // Root Always exists as directory. It can be an empty listing.
        return listResponseData;
      }
      AbfsRestOperation pathStatus = this.getPathStatus(relativePath, tracingContext, null, false);
      BlobListResultSchema listResultSchema = getListResultSchemaFromPathStatus(relativePath, pathStatus);
      LOG.debug("ListBlob attempted on a file path. Returning file status.");
      List<VersionedFileStatus> fileStatusList = new ArrayList<>();
      for (BlobListResultEntrySchema entry : listResultSchema.paths()) {
        fileStatusList.add(getVersionedFileStatusFromEntry(entry, uri));
      }
      AbfsRestOperation listOp = getAbfsRestOperation(
          AbfsRestOperationType.ListBlobs,
          HTTP_METHOD_GET,
          url,
          requestHeaders);
      listOp.hardSetGetListStatusResult(HTTP_OK, listResultSchema);
      listResponseData.setFileStatusList(fileStatusList);
      listResponseData.setContinuationToken(null);
      listResponseData.setRenamePendingJsonPaths(null);
      listResponseData.setOp(listOp);
      return listResponseData;
    }
    return listResponseData;
  }

  /**
   * Filter the paths for which no rename redo operation is performed.
   * Update BlobListResultSchema path with filtered entries.
   * @param tracingContext tracing context
   * @throws AzureBlobFileSystemException if rest operation or response parsing fails.
   */
  private boolean retryRenameOnAtomicEntriesInListResults(TracingContext tracingContext,
      Map<Path, Integer> renamePendingJsonPaths) throws AzureBlobFileSystemException {
    if (renamePendingJsonPaths == null || renamePendingJsonPaths.isEmpty()) {
      return false;
    }

    for (Map.Entry<Path, Integer> entry : renamePendingJsonPaths.entrySet()) {
      retryRenameOnAtomicKeyPath(entry.getKey(), entry.getValue(), tracingContext);
    }
    return true;
  }

  /**{@inheritDoc}*/
  @Override
  public void createNonRecursivePreCheck(Path parentPath,
      TracingContext tracingContext)
      throws IOException {
    try {
      if (isAtomicRenameKey(parentPath.toUri().getPath())) {
        takeGetPathStatusAtomicRenameKeyAction(parentPath, tracingContext);
      }
      try {
        getPathStatus(parentPath.toUri().getPath(), false,
            tracingContext, null);
      } finally {
        getAbfsCounters().incrementCounter(CALL_GET_FILE_STATUS, 1);
      }
    } catch (AbfsRestOperationException ex) {
      if (ex.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
        throw new FileNotFoundException("Cannot create file "
            + parentPath.toUri().getPath()
            + " because parent folder does not exist.");
      }
      throw ex;
    }
  }

  /**
   * Get Rest Operation for API
   * <a href="../../../../site/markdown/blobEndpoint.md#put-blob">Put Blob</a>.
   * Creates a file or directory (marker file) at the specified path.
   *
   * @param path the path of the directory to be created.
   * @param isFileCreation whether the path to create is a file.
   * @param overwrite whether to overwrite if the path already exists.
   * @param permissions the permissions to set on the path.
   * @param isAppendBlob whether the path is an append blob.
   * @param eTag the eTag of the path.
   * @param contextEncryptionAdapter the context encryption adapter.
   * @param tracingContext the tracing context.
   * @return the executed rest operation containing the response from the server.
   * @throws AzureBlobFileSystemException if the rest operation fails.
   */
  @Override
  public AbfsRestOperation createPath(final String path,
      final boolean isFileCreation,
      final boolean overwrite,
      final AzureBlobFileSystemStore.Permissions permissions,
      final boolean isAppendBlob,
      final String eTag,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    AbfsRestOperation op;
    if (isFileCreation) {
      // Create a file with the specified parameters
      op = createFile(path, overwrite, permissions, isAppendBlob, eTag,
          contextEncryptionAdapter, tracingContext);
    } else {
      // Create a directory with the specified parameters
      op = createDirectory(path, permissions, isAppendBlob, eTag,
          contextEncryptionAdapter, tracingContext);
    }
    return op;
  }

  /**
   * Creates a marker at the specified path.
   *
   * @param path the path where the marker is to be created.
   * @param eTag the eTag of the path.
   * @param contextEncryptionAdapter the context encryption adapter.
   * @param tracingContext the tracing context for the service call.
   *
   * @return the created AbfsRestOperation.
   *
   * @throws AzureBlobFileSystemException if an error occurs during the operation.
   */
  protected AbfsRestOperation createMarkerAtPath(final String path,
      final String eTag,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    return createPathRestOp(path, false, false, false, eTag,
        contextEncryptionAdapter, tracingContext);
  }

  /**
   * Get Rest Operation for API
   * <a href="../../../../site/markdown/blobEndpoint.md#put-blob">Put Blob</a>.
   * Creates a file or directory (marker file) at the specified path.
   *
   * @param path the path of the directory to be created.
   * @param isFile whether the path is a file.
   * @param overwrite whether to overwrite if the path already exists.
   * @param isAppendBlob whether the path is an append blob.
   * @param eTag the eTag of the path.
   * @param contextEncryptionAdapter the context encryption adapter.
   * @param tracingContext the tracing context for the service call.
   * @return the executed rest operation containing the response from the server.
   * @throws AzureBlobFileSystemException if the rest operation fails.
   */
  public AbfsRestOperation createPathRestOp(final String path,
      final boolean isFile,
      final boolean overwrite,
      final boolean isAppendBlob,
      final String eTag,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
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
    op.execute(tracingContext);
    return op;
  }

  /**
   * Conditionally creates or overwrites a file at the specified relative path.
   * This method ensures that the file is created or overwritten based on the provided parameters.
   *
   * @param relativePath The relative path of the file to be created or overwritten.
   * @param statistics The file system statistics to be updated.
   * @param permissions The permissions to be set on the file.
   * @param isAppendBlob Specifies if the file is an append blob.
   * @param contextEncryptionAdapter The encryption context adapter for handling encryption.
   * @param tracingContext The tracing context for tracking the operation.
   * @return An AbfsRestOperation object containing the result of the operation.
   * @throws IOException If an I/O error occurs during the operation.
   */
  @Override
  public AbfsRestOperation conditionalCreateOverwriteFile(String relativePath,
      FileSystem.Statistics statistics,
      AzureBlobFileSystemStore.Permissions permissions,
      boolean isAppendBlob,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext) throws IOException {
    if (!getIsNamespaceEnabled()) {
      // Check for non-empty directory at the path. The only pending validation is the check for an explicitly empty directory,
      // which is performed later to optimize TPS by delaying the lookup only if create with overwrite=false fails.
      if (isNonEmptyDirectory(relativePath, tracingContext)) {
        throw new AbfsRestOperationException(HTTP_CONFLICT,
            AzureServiceErrorCode.PATH_CONFLICT.getErrorCode(),
            PATH_EXISTS,
            null);
      }
      // Create markers for the parent hierarchy.
      tryMarkerCreation(relativePath, isAppendBlob, null,
          contextEncryptionAdapter, tracingContext);
    }
    AbfsRestOperation op;
    try {
      // Trigger a creation with overwrite=false first so that eTag fetch can be
      // avoided for cases when no pre-existing file is present (major portion
      // of create file traffic falls into the case of no pre-existing file).
      op = createPathRestOp(relativePath, true, false,
          isAppendBlob, null, contextEncryptionAdapter, tracingContext);
    } catch (AbfsRestOperationException e) {
      if (e.getStatusCode() == HTTP_CONFLICT) {
        // File pre-exists, fetch eTag
        try {
          op = getPathStatus(relativePath, tracingContext, null, false);
        } catch (AbfsRestOperationException ex) {
          if (ex.getStatusCode() == HTTP_NOT_FOUND) {
            // Is a parallel access case, as file which was found to be
            // present went missing by this request.
            throw new ConcurrentWriteOperationDetectedException("Parallel access to the create path detected. Failing request "
                + "as the path which existed before gives not found error");
          } else {
            throw ex;
          }
        }

        // If present as an explicit empty directory, we should throw conflict exception.
        boolean isExplicitDir = checkIsDir(op.getResult());
        if (isExplicitDir) {
          throw new AbfsRestOperationException(HTTP_CONFLICT,
              AzureServiceErrorCode.PATH_CONFLICT.getErrorCode(),
              PATH_EXISTS,
              null);
        }

        String eTag = extractEtagHeader(op.getResult());

        try {
          // overwrite only if eTag matches with the file properties fetched before
          op = createPathRestOp(relativePath, true, true,
              isAppendBlob, eTag, contextEncryptionAdapter, tracingContext);
        } catch (AbfsRestOperationException ex) {
          if (ex.getStatusCode() == HTTP_PRECON_FAILED) {
            // Is a parallel access case, as file with eTag was just queried
            // and precondition failure can happen only when another file with
            // different etag got created.
            throw new ConcurrentWriteOperationDetectedException("Parallel access to the create path detected. Failing request "
                + "due to precondition failure");
          } else {
            throw ex;
          }
        }
      } else {
        throw e;
      }
    }
    return op;
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
  public AbfsRestOperation acquireLease(final String path,
      final int duration,
      final String eTag,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, ACQUIRE_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_DURATION, Integer.toString(duration)));
    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPOSED_LEASE_ID, UUID.randomUUID().toString()));
    if (StringUtils.isNotEmpty(eTag)) {
      requestHeaders.add(new AbfsHttpHeader(IF_MATCH, eTag));
    }

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
   * Rename a file or directory.
   * If a source etag is passed in, the operation will attempt to recover
   * from a missing source file by probing the destination for
   * existence and comparing etags.
   * The second value in the result will be true to indicate that this
   * took place.
   * As rename recovery is only attempted if the source etag is non-empty,
   * in normal rename operations rename recovery will never happen.
   *
   * @param source                    path to source file
   * @param destination               destination of rename.
   * @param continuation              continuation.
   * @param tracingContext            trace context
   * @param sourceEtag                etag of source file. may be null or empty
   * @param isMetadataIncompleteState was there a rename failure due to
   *                                  incomplete metadata state?
   *
   * @return AbfsClientRenameResult result of rename operation indicating the
   * AbfsRest operation, rename recovery and incomplete metadata state failure.
   *
   * @throws IOException failure, excluding any recovery from overload failures.
   */
  @Override
  public AbfsClientRenameResult renamePath(final String source,
      final String destination,
      final String continuation,
      final TracingContext tracingContext,
      String sourceEtag,
      boolean isMetadataIncompleteState)
      throws IOException {
    BlobRenameHandler blobRenameHandler = getBlobRenameHandler(source,
        destination, sourceEtag, isAtomicRenameKey(source), tracingContext
    );
    try {
      if (blobRenameHandler.execute(false)) {
        final AbfsUriQueryBuilder abfsUriQueryBuilder
            = createDefaultUriQueryBuilder();
        final URL url = createRequestUrl(destination,
            abfsUriQueryBuilder.toString());
        final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
        final AbfsRestOperation successOp = getSuccessOp(
            AbfsRestOperationType.RenamePath, HTTP_METHOD_PUT,
            url, requestHeaders);
        return new AbfsClientRenameResult(successOp, true, false);
      } else {
        throw new AbfsRestOperationException(HTTP_INTERNAL_ERROR,
            AzureServiceErrorCode.UNKNOWN.getErrorCode(),
            ERR_RENAME_BLOB + source + SINGLE_WHITE_SPACE + AND_MARK
                + SINGLE_WHITE_SPACE + destination,
            null);
      }
    } finally {
      incrementAbfsRenamePath();
    }
  }

  @VisibleForTesting
  BlobRenameHandler getBlobRenameHandler(final String source,
      final String destination,
      final String sourceEtag,
      final boolean isAtomicRename,
      final TracingContext tracingContext) {
    return new BlobRenameHandler(source,
        destination, this, sourceEtag, isAtomicRename, false, tracingContext);
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
      if (op.getResult().getStatusCode() == HTTP_NOT_FOUND && isNonEmptyDirectory(path, tracingContext)) {
        // Implicit path found, create a marker blob at this path and set properties.
        this.createPathRestOp(path, false, false, false, null,
            contextEncryptionAdapter, tracingContext);
        // Make sure hdi_isFolder is added to the list of properties to be set.
        boolean hdiIsFolderExists = properties.keySet()
            .stream().anyMatch(XML_TAG_HDI_ISFOLDER::equalsIgnoreCase);
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
    AbfsRestOperation op = this.getPathStatus(path, tracingContext,
        contextEncryptionAdapter, true);
    // Crashed HBase log-folder rename can be recovered by FileSystem#getFileStatus
    if (tracingContext.getOpType() == FSOperationType.GET_FILESTATUS
        && op.getResult() != null && checkIsDir(op.getResult())) {
      takeGetPathStatusAtomicRenameKeyAction(new Path(path), tracingContext);
    }
    return op;
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
          && isImplicitCheckRequired && isNonEmptyDirectory(path, tracingContext)) {
        // Implicit path found.
        // Create a marker blob at this path.
        this.createMarkerAtPath(path, null, contextEncryptionAdapter, tracingContext);
        AbfsRestOperation successOp = getSuccessOp(
            AbfsRestOperationType.GetPathStatus, HTTP_METHOD_HEAD,
            url, requestHeaders);
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
   * Orchestration of delete path over Blob endpoint.
   * Delete the file or directory at specified path.
   * @param path to be deleted.
   * @param recursive if the path is a directory, delete recursively.
   * @param continuation to specify continuation token.
   * @param tracingContext TracingContext instance to track identifiers
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation deletePath(final String path,
      final boolean recursive,
      final String continuation,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    BlobDeleteHandler blobDeleteHandler = getBlobDeleteHandler(path, recursive,
        tracingContext);
    if (blobDeleteHandler.execute()) {
      final AbfsUriQueryBuilder abfsUriQueryBuilder
          = createDefaultUriQueryBuilder();
      final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
      final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
      return getSuccessOp(AbfsRestOperationType.DeletePath,
          HTTP_METHOD_DELETE, url, requestHeaders);
    } else {
      throw new AbfsRestOperationException(HTTP_INTERNAL_ERROR,
          AzureServiceErrorCode.UNKNOWN.getErrorCode(),
          ERR_DELETE_BLOB + path,
          null);
    }
  }

  @VisibleForTesting
  public BlobDeleteHandler getBlobDeleteHandler(final String path,
      final boolean recursive,
      final TracingContext tracingContext) {
    return new BlobDeleteHandler(new Path(path), recursive, this,
        tracingContext);
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
    final URL url = createRequestUrl(blobRelativePath,
        abfsUriQueryBuilder.toString());
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    if (leaseId != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));
    }
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.DeleteBlob, HTTP_METHOD_DELETE, url,
        requestHeaders);
    try {
      op.execute(tracingContext);
      return op;
    } catch (AzureBlobFileSystemException e) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw e;
      }
      final AbfsRestOperation idempotencyOp = deleteIdempotencyCheckOp(op);
      if (idempotencyOp.getResult().getStatusCode()
          == op.getResult().getStatusCode()) {
        // idempotency did not return different result
        // throw back the exception
        throw e;
      } else {
        return idempotencyOp;
      }
    }
  }

  /**
   * Checks if the rest operation results indicate if the path is a directory.
   * @param result executed rest operation containing response from server.
   * @return True if the path is a directory, False otherwise.
   */
  @Override
  public boolean checkIsDir(AbfsHttpOperation result) {
    String resourceType = result.getResponseHeaderIgnoreCase(X_MS_META_HDI_ISFOLDER);
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
        && responseStatusCode < HTTP_INTERNAL_ERROR
        && responseStatusCode != HTTP_CONFLICT);
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
   * @param result InputStream contains the response from server.
   * @param uri to be used for path conversion.
   * @return {@link ListResponseData}. containing listing response.
   * @throws AzureBlobFileSystemException if parsing fails.
   */
  @Override
  public ListResponseData parseListPathResults(AbfsHttpOperation result, URI uri)
      throws AzureBlobFileSystemException {
    try (InputStream stream = result.getListResultStream()) {
      try {
        BlobListResultSchema listResultSchema;
        final SAXParser saxParser = saxParserThreadLocal.get();
        saxParser.reset();
        listResultSchema = new BlobListResultSchema();
        saxParser.parse(stream,
            new BlobListXmlParser(listResultSchema, getBaseUrl().toString()));
        result.setListResultSchema(listResultSchema);
        LOG.debug("ListBlobs listed {} blobs with {} as continuation token",
            listResultSchema.paths().size(),
            listResultSchema.getNextMarker());
        return filterRenamePendingFiles(listResultSchema, uri);
      } catch (SAXException | IOException ex) {
        throw new AbfsDriverException(ERR_BLOB_LIST_PARSING, ex);
      }
    } catch (AbfsDriverException ex) {
      // Throw as it is to avoid multiple wrapping.
      LOG.error("Unable to deserialize list results for Uri {}", uri != null ? uri.toString(): "NULL", ex);
      throw ex;
    } catch (Exception ex) {
      LOG.error("Unable to get stream for list results for uri {}", uri != null ? uri.toString(): "NULL", ex);
      throw new AbfsDriverException(ERR_BLOB_LIST_PARSING, ex);
    }
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
   * Check if the path is present in the set of atomic rename keys.
   * @param key path to be checked.
   * @return true if path is present in the set else false.
   */
  public boolean isAtomicRenameKey(String key) {
    return isKeyForDirectorySet(key, azureAtomicRenameDirSet);
  }

  /**
   * Action to be taken when atomic-key is present on a getPathStatus path.
   *
   * @param path path of the pendingJson for the atomic path.
   * @param tracingContext tracing context.
   *
   * @throws AzureBlobFileSystemException server error or the path is renamePending json file and action is taken.
   */
  public void takeGetPathStatusAtomicRenameKeyAction(final Path path,
      final TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    if (path == null || path.isRoot() || !isAtomicRenameKey(
        path.toUri().getPath())) {
      return;
    }
    AbfsRestOperation pendingJsonFileStatus;
    Path pendingJsonPath = new Path(path.getParent(),
        path.toUri().getPath() + RenameAtomicity.SUFFIX);
    int pendingJsonFileContentLength = 0;
    try {
      pendingJsonFileStatus = getPathStatus(pendingJsonPath.toUri().getPath(),
          tracingContext, null, false);
      if (checkIsDir(pendingJsonFileStatus.getResult())) {
        return;
      }
      pendingJsonFileContentLength = Integer.parseInt(pendingJsonFileStatus.getResult().getResponseHeader(CONTENT_LENGTH));
    } catch (AbfsRestOperationException ex) {
      if (ex.getStatusCode() == HTTP_NOT_FOUND) {
        return;
      }
      throw ex;
    }

    boolean renameSrcHasChanged;
    try {
      RenameAtomicity renameAtomicity = getRedoRenameAtomicity(
          pendingJsonPath, pendingJsonFileContentLength, tracingContext);
      renameAtomicity.redo();
      renameSrcHasChanged = false;
    } catch (AbfsRestOperationException ex) {
      /*
       * At this point, the source marked by the renamePending json file, might have
       * already got renamed by some parallel thread, or at this point, the path
       * would have got modified which would result in eTag change, which would lead
       * to a HTTP_CONFLICT. In this case, no more operation needs to be taken, and
       * the calling getPathStatus can return this source path as result.
       */
      if (ex.getStatusCode() == HTTP_NOT_FOUND
          || ex.getStatusCode() == HTTP_CONFLICT) {
        renameSrcHasChanged = true;
      } else {
        throw ex;
      }
    }
    if (!renameSrcHasChanged) {
      throw new AbfsRestOperationException(
          AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
          AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
          ATOMIC_DIR_RENAME_RECOVERY_ON_GET_PATH_EXCEPTION,
          null);
    }
  }

  /**
   * Redo the rename operation when path is present in atomic directory list
   * or when path has {@link RenameAtomicity#SUFFIX} suffix.
   *
   * @param path path of the pendingJson for the atomic path.
   * @param renamePendingJsonLen length of the pendingJson file.
   * @param tracingContext tracing context.
   *
   * @throws AzureBlobFileSystemException server error
   */

  private void retryRenameOnAtomicKeyPath(final Path path,
      final int renamePendingJsonLen,
      final TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    try {
      RenameAtomicity renameAtomicity = getRedoRenameAtomicity(path,
          renamePendingJsonLen, tracingContext);
      renameAtomicity.redo();
    } catch (AbfsRestOperationException ex) {
      /*
       * At this point, the source marked by the renamePending json file, might have
       * already got renamed by some parallel thread, or at this point, the path
       * would have got modified which would result in eTag change, which would lead
       * to a HTTP_CONFLICT. In this case, no more operation needs to be taken, but
       * since this is a renamePendingJson file and would be deleted by the redo operation,
       * the calling listPath should not return this json path as result.
       */
      if (ex.getStatusCode() != HTTP_NOT_FOUND
          && ex.getStatusCode() != HTTP_CONFLICT) {
        throw ex;
      }
    }
  }

  @VisibleForTesting
  public RenameAtomicity getRedoRenameAtomicity(final Path renamePendingJsonPath,
      int fileLen,
      final TracingContext tracingContext) {
    return new RenameAtomicity(renamePendingJsonPath,
        fileLen,
        tracingContext,
        null,
        this);
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
   * This will filter out all the rename pending json files in listing output.
   * @param listResultSchema List of entries returned by Blob Endpoint.
   * @param uri URI to be used for path conversion.
   * @return List of entries after removing duplicates.
   * @throws IOException if path conversion fails.
   */
  @VisibleForTesting
  public ListResponseData filterRenamePendingFiles(
      BlobListResultSchema listResultSchema, URI uri) throws IOException {
    List<VersionedFileStatus> fileStatuses = new ArrayList<>();
    Map<Path, Integer> renamePendingJsonPaths = new HashMap<>();

    for (BlobListResultEntrySchema entry : listResultSchema.paths()) {
      if (isRenamePendingJsonPathEntry(entry)) {
        renamePendingJsonPaths.put(entry.path(), entry.contentLength().intValue());
      } else {
        fileStatuses.add(getVersionedFileStatusFromEntry(entry, uri));
      }
    }

    ListResponseData listResponseData = new ListResponseData();
    listResponseData.setFileStatusList(fileStatuses);
    listResponseData.setRenamePendingJsonPaths(renamePendingJsonPaths);
    listResponseData.setContinuationToken(listResultSchema.getNextMarker());
    return listResponseData;
  }

  /**
   * Check if the entry is a rename pending json file path.
   * @param entry to be checked.
   * @return true if it is a rename pending json file path.
   */
  private boolean isRenamePendingJsonPathEntry(BlobListResultEntrySchema entry) {
    String path = entry.path() != null ? entry.path().toUri().getPath() : null;
    return path != null
        && !entry.path().isRoot()
        && isAtomicRenameKey(path)
        && !entry.isDirectory()
        && path.endsWith(RenameAtomicity.SUFFIX);
  }

  /**
   * When listing is done on a file, Blob Endpoint returns the empty listing
   * but DFS Endpoint returns the file status as one of the entries.
   * This is to convert file status into ListResultSchema.
   * @param relativePath relative path of the file.
   * @param pathStatus AbfsRestOperation containing the file status.
   * @return BlobListResultSchema containing the file status.
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
    entrySchema.setETag(extractEtagHeader(pathStatus.getResult()));

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
        : URLDecoder.decode(encoded, StandardCharsets.UTF_8.name());
  }

  /**
   * Checks if the listing of the specified path is non-empty.
   *
   * @param path The path to be listed.
   * @param tracingContext The tracing context for tracking the operation.
   * @return True if the listing is non-empty, False otherwise.
   * @throws AzureBlobFileSystemException If an error occurs during the listing operation.
   */
  @VisibleForTesting
  public boolean isNonEmptyDirectory(String path,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    // This method is only called internally to determine state of a path
    // and hence don't need identity transformation to happen.
    ListResponseData listResponseData = listPath(path, false, 1, null, tracingContext, null, false);
    return !isEmptyListResults(listResponseData);
  }

  /**
   * Check if the list call returned empty results without any continuation token.
   * @param listResponseData The response of listing API from the server.
   * @return True if empty results without continuation token.
   */
  private boolean isEmptyListResults(ListResponseData listResponseData) {
    AbfsHttpOperation result = listResponseData.getOp().getResult();
    boolean isEmptyList = result != null && result.getStatusCode() == HTTP_OK && // List Call was successful
        result.getListResultSchema() != null && // Parsing of list response was successful
        listResponseData.getFileStatusList().isEmpty() && listResponseData.getRenamePendingJsonPaths().isEmpty() &&// No paths were returned
        StringUtils.isEmpty(listResponseData.getContinuationToken()); // No continuation token was returned
    if (isEmptyList) {
      LOG.debug("List call returned empty results without any continuation token.");
      return true;
    }
    return false;
  }

  /**
   * Generate the XML block list using a comma-separated string of block IDs.
   *
   * @param blockIdString The comma-separated block IDs.
   * @return the XML representation of the block list.
   */
  public static String generateBlockListXml(String blockIdString) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(String.format(XML_VERSION));
    stringBuilder.append(String.format(BLOCK_LIST_START_TAG));

    // Split the block ID string by commas and generate XML for each block ID
    if (!blockIdString.isEmpty()) {
      String[] blockIds = blockIdString.split(",");
      for (String blockId : blockIds) {
        stringBuilder.append(String.format(LATEST_BLOCK_FORMAT, blockId));
      }
    }

    stringBuilder.append(String.format(BLOCK_LIST_END_TAG));
    return stringBuilder.toString();
  }

  /**
   * Checks if the specified path exists as a directory.
   *
   * @param path the path of the directory to check.
   * @param tracingContext the tracing context for the service call.
   * @return true if the directory exists, false otherwise.
   * @throws AzureBlobFileSystemException if the rest operation fails.
   */
  private boolean isExistingDirectory(String path,
      TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    // Check if the directory contains any entries by listing its contents.
    if (isNonEmptyDirectory(path, tracingContext)) {
      // If the list result schema has any paths, it is a directory.
      return true;
    } else {
      // If the directory does not contain any entries, check if it exists as an empty directory.
      return isEmptyDirectory(path, tracingContext, true);
    }
  }

  /**
   * Checks the status of the path to determine if it exists and whether it is a file or directory.
   * Throws an exception if the path exists as a file.
   *
   * @param path the path to check
   * @param tracingContext the tracing context
   * @return true if the path exists and is a directory, false otherwise
   * @throws AbfsRestOperationException if the path exists as a file
   */
  private boolean isEmptyDirectory(final String path,
      final TracingContext tracingContext, boolean isDirCheck) throws AzureBlobFileSystemException {
    // If the call is to create a directory, there are 3 possible cases:
    // a) a file exists at that path
    // b) an empty directory exists
    // c) the path does not exist.
    AbfsRestOperation getPathStatusOp = null;
    try {
      // GetPathStatus call to check if path already exists.
      getPathStatusOp = getPathStatus(path, tracingContext, null, false);
    } catch (AbfsRestOperationException ex) {
      if (ex.getStatusCode() != HTTP_NOT_FOUND) {
        throw ex;
      }
    }
    if (getPathStatusOp != null) {
      // If path exists and is a directory, return true.
      boolean isDirectory = checkIsDir(getPathStatusOp.getResult());
      if (!isDirectory && isDirCheck) {
        // This indicates path exists as a file, hence throw conflict.
        throw new AbfsRestOperationException(HTTP_CONFLICT,
            AzureServiceErrorCode.PATH_CONFLICT.getErrorCode(),
            PATH_EXISTS,
            null);
      } else {
        return isDirectory;
      }
    }
    return false;
  }

  /**
   * Creates a successful AbfsRestOperation for the given path.
   *
   * @param path the path for which the operation is created.
   * @return the created AbfsRestOperation with a hard set result of HTTP_CREATED.
   * @throws AzureBlobFileSystemException if an error occurs during the operation creation.
   */
  private AbfsRestOperation createSuccessResponse(String path) throws AzureBlobFileSystemException {
    final AbfsRestOperation successOp = getAbfsRestOperation(
        AbfsRestOperationType.PutBlob,
        HTTP_METHOD_PUT, createRequestUrl(path, EMPTY_STRING),
        createDefaultHeaders());
    successOp.hardSetResult(HttpURLConnection.HTTP_CREATED);
    return successOp;
  }

  /**
   * Creates a directory at the specified path.
   *
   * @param path the path of the directory to be created.
   * @param permissions the permissions to be set for the directory.
   * @param isAppendBlob whether the directory is an append blob.
   * @param eTag the eTag of the directory.
   * @param contextEncryptionAdapter the encryption context adapter.
   * @param tracingContext the tracing context for the service call.
   * @return the executed rest operation containing the response from the server.
   * @throws AzureBlobFileSystemException if the rest operation fails.
   */
  private AbfsRestOperation createDirectory(final String path,
      final AzureBlobFileSystemStore.Permissions permissions,
      final boolean isAppendBlob,
      final String eTag,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled()) {
      try {
        if (isExistingDirectory(path, tracingContext)) {
          // we return a dummy success response and save TPS if directory already exists.
          return createSuccessResponse(path);
        }
      } catch (AzureBlobFileSystemException ex) {
        LOG.error("Path exists as file {} : {}", path, ex.getMessage());
        throw ex;
      }
      tryMarkerCreation(path, isAppendBlob, eTag,
          contextEncryptionAdapter, tracingContext);
    }
    return createPathRestOp(path, false, true,
        isAppendBlob, eTag, contextEncryptionAdapter, tracingContext);
  }

  /**
   * Creates a file at the specified path.
   *
   * @param path the path of the file to be created.
   * @param overwrite whether to overwrite if the file already exists.
   * @param permissions the permissions to set on the file.
   * @param isAppendBlob whether the file is an append blob.
   * @param eTag the eTag of the file.
   * @param contextEncryptionAdapter the context encryption adapter.
   * @param tracingContext the tracing context for the service call.
   * @return the executed rest operation containing the response from the server.
   * @throws AzureBlobFileSystemException if the rest operation fails.
   */
  private AbfsRestOperation createFile(final String path,
      final boolean overwrite,
      final AzureBlobFileSystemStore.Permissions permissions,
      final boolean isAppendBlob,
      final String eTag,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled()) {
      // Check if non-empty directory already exists at that path.
      if (isNonEmptyDirectory(path, tracingContext)) {
        throw new AbfsRestOperationException(HTTP_CONFLICT,
            AzureServiceErrorCode.PATH_CONFLICT.getErrorCode(),
            PATH_EXISTS,
            null);
      }
      // If the overwrite flag is true, we must verify whether an empty directory exists at the specified path.
      // However, if overwrite is false, we can skip this validation and proceed with blob creation,
      // which will fail with a conflict error if a file or directory already exists at the path.
      if (overwrite && isEmptyDirectory(path, tracingContext, false)) {
        throw new AbfsRestOperationException(HTTP_CONFLICT,
            AzureServiceErrorCode.PATH_CONFLICT.getErrorCode(),
            PATH_EXISTS,
            null);
      }
      tryMarkerCreation(path, isAppendBlob, eTag,
          contextEncryptionAdapter, tracingContext);
    }
    return createPathRestOp(path, true, overwrite,
        isAppendBlob, eTag, contextEncryptionAdapter, tracingContext);
  }

  /**
   * Retrieves the list of marker paths to be created for the specified path.
   *
   * @param path The path for which marker paths need to be created.
   * @param tracingContext The tracing context for the operation.
   * @return A list of paths that need to be created as markers.
   * @throws AzureBlobFileSystemException If an error occurs while finding parent paths for marker creation.
   */
  @VisibleForTesting
  public List<Path> getMarkerPathsTobeCreated(final Path path,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    ArrayList<Path> keysToCreateAsFolder = new ArrayList<>();
    findParentPathsForMarkerCreation(path, tracingContext, keysToCreateAsFolder);
    return keysToCreateAsFolder;
  }

  /**
   * Creates marker blobs for the parent directories of the specified path.
   *
   * @param path The path for which parent directories need to be created.
   * @param isAppendBlob A flag indicating whether the created blob should be of type APPEND_BLOB.
   * @param eTag The eTag to be matched for conditional requests.
   * @param contextEncryptionAdapter The encryption adapter for context encryption.
   * @param tracingContext The tracing context for the operation.
   *
   * @throws AzureBlobFileSystemException If the creation of any parent directory fails.
   */
  @VisibleForTesting
  public void tryMarkerCreation(String path,
      boolean isAppendBlob,
      String eTag,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    Path parentPath = new Path(path).getParent();
    if (parentPath != null && !parentPath.isRoot()) {
      List<Path> keysToCreateAsFolder = getMarkerPathsTobeCreated(parentPath,
          tracingContext);
      for (Path pathToCreate : keysToCreateAsFolder) {
        try {
          createPathRestOp(pathToCreate.toUri().getPath(), false, false,
              isAppendBlob, eTag, contextEncryptionAdapter, tracingContext);
        } catch (AbfsRestOperationException e) {
          LOG.debug("Swallow exception for failed marker creation");
        }
      }
    }
  }

  /**
   * Checks for the entire parent hierarchy and returns if any directory exists and
   * throws an exception if any file exists.
   * @param path path to check the hierarchy for.
   * @param tracingContext the tracingcontext.
   */
  private void findParentPathsForMarkerCreation(Path path, TracingContext tracingContext,
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
      if (opResult == null) {
        keysToCreateAsFolder.add(current);
        current = current.getParent();
        continue;
      }
      if (checkIsDir(opResult)) {
        // Explicit directory found, return from here.
        return;
      } else {
        // File found hence throw exception.
        throw new AbfsRestOperationException(HTTP_CONFLICT,
            AzureServiceErrorCode.PATH_CONFLICT.getErrorCode(),
            PATH_EXISTS,
            null);
      }
    } while (current != null && !current.isRoot());
  }
}
