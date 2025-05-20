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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ApiVersion;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsInvalidChecksumException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.ConcurrentWriteOperationDetectedException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidFileSystemPropertyException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.contracts.services.DfsListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.DfsListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.StorageErrorResponseSchema;
import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.utils.Base64;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.util.StringUtils;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CALL_GET_FILE_STATUS;
import static org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.extractEtagHeader;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ACQUIRE_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPEND_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPEND_BLOB_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_JSON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_OCTET_STREAM;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BREAK_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHECK_ACCESS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COMMA;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DEFAULT_LEASE_BREAK_PERIOD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DIRECTORY;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FILE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FILESYSTEM;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FLUSH_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.GET_ACCESS_CONTROL;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.GET_STATUS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_GET;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_HEAD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PATCH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_POST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HUNDRED_CONTINUE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.RELEASE_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.RENEW_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SET_ACCESS_CONTROL;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SET_PROPERTIES_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SINGLE_WHITE_SPACE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.STAR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TRUE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.XMS_PROPERTIES_ENCODING_ASCII;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.ACCEPT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.IF_MATCH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.IF_NONE_MATCH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.RANGE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.USER_AGENT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_HTTP_METHOD_OVERRIDE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_TRANSACTION_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_EXISTING_RESOURCE_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_BREAK_PERIOD;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_DURATION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_PROPERTIES;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_PROPOSED_LEASE_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_RANGE_GET_CONTENT_MD5;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_RENAME_SOURCE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_FS_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_BLOBTYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_CLOSE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_CONTINUATION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_DIRECTORY;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_FLUSH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_MAXRESULTS;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_PAGINATED;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_POSITION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_RECURSIVE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_RESOURCE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_RETAIN_UNCOMMITTED_DATA;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.RENAME_DESTINATION_PARENT_PATH_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.SOURCE_PATH_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.UNAUTHORIZED_BLOB_OVERWRITE;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_CREATE_RECOVERY;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_DFS_LIST_PARSING;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_FILE_ALREADY_EXISTS;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_RENAME_RECOVERY;

/**
 * AbfsClient interacting with the DFS Endpoint.
 */
public class AbfsDfsClient extends AbfsClient {

  public AbfsDfsClient(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final AccessTokenProvider tokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    super(baseUrl, sharedKeyCredentials, abfsConfiguration, tokenProvider,
        encryptionContextProvider, abfsClientContext);
  }

  public AbfsDfsClient(final URL baseUrl,
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
   * DFS Endpoint API responses are in JSON/Stream format.
   * @param xMsVersion API version to be used.
   * @return default request headers.
   */
  @Override
  public List<AbfsHttpHeader> createDefaultHeaders(ApiVersion xMsVersion) {
    List<AbfsHttpHeader> requestHeaders = createCommonHeaders(xMsVersion);
    requestHeaders.add(new AbfsHttpHeader(ACCEPT, APPLICATION_JSON
        + COMMA + SINGLE_WHITE_SPACE + APPLICATION_OCTET_STREAM));
    return requestHeaders;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/create">
   *   Filesystem - Create</a>.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation createFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = new AbfsUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.CreateFileSystem,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/set-properties">
   *   Filesystem - Set Properties</a>.
   * @param properties list of metadata key-value pairs.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation setFilesystemProperties(final Hashtable<String, String> properties,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final String commaSeparatedProperties;
    try {
      commaSeparatedProperties = convertXmsPropertiesToCommaSeparatedString(properties);
    } catch (CharacterCodingException ex) {
      throw new InvalidAbfsRestOperationException(ex);
    }

    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to work around the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
        HTTP_METHOD_PATCH));
    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPERTIES, commaSeparatedProperties));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetFileSystemProperties,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/get-properties">
   *   Filesystem - Get Properties</a>.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   * */
  @Override
  public AbfsRestOperation getFilesystemProperties(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetFileSystemProperties,
        HTTP_METHOD_HEAD, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/delete">
   *   Filesystem - Delete</a>.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation deleteFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.DeleteFileSystem,
        HTTP_METHOD_DELETE, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/list">
   *   Filesystem - List</a>.
   * List paths and their properties in the current filesystem.
   * @param relativePath to return only blobs within this directory.
   * @param recursive to return all blobs in the path, including those in subdirectories.
   * @param listMaxResults maximum number of blobs to return.
   * @param continuation marker to specify the continuation token.
   * @param tracingContext for tracing the server calls.
   * @param uri to be used for path conversion.
   * @return {@link ListResponseData}. containing listing response.
   * @throws AzureBlobFileSystemException if rest operation or response parsing fails.
   */
  @Override
  public ListResponseData listPath(final String relativePath,
      final boolean recursive,
      final int listMaxResults,
      final String continuation,
      TracingContext tracingContext, URI uri) throws IOException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_DIRECTORY,
        getDirectoryQueryParameter(relativePath));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RECURSIVE, String.valueOf(recursive));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CONTINUATION, continuation);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_MAXRESULTS,
        String.valueOf(listMaxResults));
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN,
        String.valueOf(getAbfsConfiguration().isUpnUsed()));
    appendSASTokenToQuery(relativePath, SASTokenProvider.LIST_OPERATION,
        abfsUriQueryBuilder);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.ListPaths,
        HTTP_METHOD_GET, url, requestHeaders);
    op.execute(tracingContext);
    ListResponseData listResponseData = parseListPathResults(op.getResult(), uri);
    listResponseData.setOp(op);
    return listResponseData;
  }

  /**
   * Non-functional implementation.
   * Client side handling to remove duplicates not needed in DFSClient.
   * @param relativePath on which listing was attempted.
   * @param fileStatuses result of listing operation.
   * @param tracingContext for tracing the server calls.
   * @param uri to be used for path conversion.
   * @return fileStatuses as it is without any processing.
   */
  @Override
  public List<FileStatus> postListProcessing(String relativePath,
      List<FileStatus> fileStatuses, TracingContext tracingContext, URI uri){
    return fileStatuses;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create">
   *   Path - Create</a>.
   * Create a path (file or directory) in the current filesystem.
   * @param path to be created inside the filesystem.
   * @param isFile to specify if the created path is file or directory.
   * @param overwrite to specify if the path should be overwritten if it already exists.
   * @param permissions to specify the permissions of the path.
   * @param isAppendBlob to specify if the path to be created is an append blob.
   * @param eTag to specify conditional headers.
   * @param contextEncryptionAdapter to provide encryption context.
   * @param tracingContext for tracing the server calls.
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
    if (isFile) {
      addEncryptionKeyRequestHeaders(path, requestHeaders, true,
          contextEncryptionAdapter, tracingContext);
    }
    if (!overwrite) {
      requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, STAR));
    }

    if (permissions.hasPermission()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_PERMISSIONS,
          permissions.getPermission()));
    }

    if (permissions.hasUmask()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_UMASK,
          permissions.getUmask()));
    }

    if (eTag != null && !eTag.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(IF_MATCH, eTag));
    }

    // Add the client transaction ID to the request headers.
    String clientTransactionId = addClientTransactionIdToHeader(requestHeaders);

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, isFile ? FILE : DIRECTORY);
    if (isAppendBlob) {
      abfsUriQueryBuilder.addQuery(QUERY_PARAM_BLOBTYPE, APPEND_BLOB_TYPE);
    }

    String operation = isFile
        ? SASTokenProvider.CREATE_FILE_OPERATION
        : SASTokenProvider.CREATE_DIRECTORY_OPERATION;
    appendSASTokenToQuery(path, operation, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.CreatePath,
        HTTP_METHOD_PUT, url, requestHeaders);
    try {
      op.execute(tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw ex;
      }
      if (!isFile) {
        if (op.getResult().getStatusCode() == HttpURLConnection.HTTP_CONFLICT) {
          String existingResource =
              op.getResult().getResponseHeader(X_MS_EXISTING_RESOURCE_TYPE);
          if (existingResource != null && existingResource.equals(DIRECTORY)) {
            //don't throw ex on mkdirs for existing directory
            return getSuccessOp(AbfsRestOperationType.CreatePath,
                HTTP_METHOD_PUT, url, requestHeaders);
          }
        }
      } else {
        // recovery using client transaction id only if it is a retried request.
        if (op.isARetriedRequest() && clientTransactionId != null
            && (op.getResult().getStatusCode() == HttpURLConnection.HTTP_CONFLICT
            || op.getResult().getStatusCode() == HttpURLConnection.HTTP_PRECON_FAILED)) {
          try {
            final AbfsHttpOperation getPathStatusOp =
                getPathStatus(path, false,
                    tracingContext, null).getResult();
            if (clientTransactionId.equals(
                getPathStatusOp.getResponseHeader(
                    X_MS_CLIENT_TRANSACTION_ID))) {
              return getSuccessOp(AbfsRestOperationType.CreatePath,
                  HTTP_METHOD_PUT, url, requestHeaders);
            }
          } catch (AzureBlobFileSystemException exception) {
            throw new AbfsDriverException(ERR_CREATE_RECOVERY, exception);
          }
        }
      }
      throw ex;
    }
    return op;
  }

  /** {@inheritDoc} */
  public void createNonRecursivePreCheck(Path parentPath,
      TracingContext tracingContext)
      throws IOException {
    try {
      getPathStatus(parentPath.toUri().getPath(), false,
          tracingContext, null);
    } catch (AbfsRestOperationException ex) {
      if (ex.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
        throw new FileNotFoundException("Cannot create file "
            + parentPath.toUri().getPath()
            + " because parent folder does not exist.");
      }
      throw ex;
    } finally {
      getAbfsCounters().incrementCounter(CALL_GET_FILE_STATUS, 1);
    }
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
  public AbfsRestOperation conditionalCreateOverwriteFile(String relativePath,
      FileSystem.Statistics statistics,
      AzureBlobFileSystemStore.Permissions permissions,
      boolean isAppendBlob,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext) throws IOException {
    AbfsRestOperation op;
    try {
      // Trigger a create with overwrite=false first so that eTag fetch can be
      // avoided for cases when no pre-existing file is present (major portion
      // of create file traffic falls into the case of no pre-existing file).
      op = createPath(relativePath, true, false, permissions,
          isAppendBlob, null, contextEncryptionAdapter, tracingContext);

    } catch (AbfsRestOperationException e) {
      if (e.getStatusCode() == HttpURLConnection.HTTP_CONFLICT) {
        // File pre-exists, fetch eTag
        try {
          op = getPathStatus(relativePath, false, tracingContext, null);
        } catch (AbfsRestOperationException ex) {
          if (ex.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
            // Is a parallel access case, as file which was found to be
            // present went missing by this request.
            throw new ConcurrentWriteOperationDetectedException();
          } else {
            throw ex;
          }
        }

        String eTag = extractEtagHeader(op.getResult());

        try {
          // overwrite only if eTag matches with the file properties fetched befpre
          op = createPath(relativePath, true, true, permissions,
              isAppendBlob, eTag, contextEncryptionAdapter, tracingContext);
        } catch (AbfsRestOperationException ex) {
          if (ex.getStatusCode() == HttpURLConnection.HTTP_PRECON_FAILED) {
            // Is a parallel access case, as file with eTag was just queried
            // and precondition failure can happen only when another file with
            // different etag got created.
            throw new ConcurrentWriteOperationDetectedException();
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
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/lease">
   *   Path - Lease</a>.
   * Acquire lease on specified path.
   * @param path on which lease has to be acquired.
   * @param duration for which lease has to be acquired.
   * @param tracingContext for tracing the server calls.
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
    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPOSED_LEASE_ID,
        UUID.randomUUID().toString()));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeasePath,
        HTTP_METHOD_POST, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/lease">
   *   Path - Lease</a>.
   * Renew lease on specified path.
   * @param path on which lease has to be renewed.
   * @param leaseId of the lease to be renewed.
   * @param tracingContext for tracing the server calls.
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

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeasePath,
        HTTP_METHOD_POST, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/lease">
   *   Path - Lease</a>.
   * Release lease on specified path.
   * @param path on which lease has to be released.
   * @param leaseId of the lease to be released.
   * @param tracingContext for tracing the server calls.
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

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeasePath,
        HTTP_METHOD_POST, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/lease">
   *   Path - Lease</a>.
   * Break lease on specified path.
   * @param path on which lease has to be broke.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation breakLease(final String path,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, BREAK_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_BREAK_PERIOD,
        DEFAULT_LEASE_BREAK_PERIOD));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeasePath,
        HTTP_METHOD_POST, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create">
   *   Path - Create</a>.
   * @param source path to source file
   * @param destination destination of rename.
   * @param continuation continuation.
   * @param tracingContext for tracing the server calls.
   * @param sourceEtag etag of source file. may be null or empty
   * @param isMetadataIncompleteState was there a rename failure due to incomplete metadata state
   * @return executed rest operation containing response from server.
   * @throws IOException if rest operation fails.
   */
  @Override
  public AbfsClientRenameResult renamePath(
      final String source,
      final String destination,
      final String continuation,
      final TracingContext tracingContext,
      String sourceEtag,
      boolean isMetadataIncompleteState) throws IOException {
    // Rename with client transaction id if namespace & client transaction id is enabled.
    if (getIsNamespaceEnabled()
        && getAbfsConfiguration().getIsClientTransactionIdEnabled()) {
      return renameWithCTIdRecovery(source, destination, continuation,
          tracingContext, sourceEtag, isMetadataIncompleteState);
    }
    // Rename with eTag in any other case.
    return renameWithETagRecovery(source, destination, continuation,
        tracingContext, sourceEtag, isMetadataIncompleteState);
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update">
   *   Path - Update</a>.
   * Uploads data to be appended to a file.
   * @param path to which data has to be appended.
   * @param buffer containing data to be appended.
   * @param reqParams containing parameters for append operation like offset, length etc.
   * @param cachedSasToken to be used for the authenticating operation.
   * @param contextEncryptionAdapter to provide encryption context.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation append(final String path,
      final byte[] buffer,
      AppendRequestParameters reqParams,
      final String cachedSasToken,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addEncryptionKeyRequestHeaders(path, requestHeaders, false,
        contextEncryptionAdapter, tracingContext);
    if (reqParams.isExpectHeaderEnabled()) {
      requestHeaders.add(new AbfsHttpHeader(EXPECT, HUNDRED_CONTINUE));
    }
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE, HTTP_METHOD_PATCH));
    if (reqParams.getLeaseId() != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, reqParams.getLeaseId()));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, APPEND_ACTION);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_POSITION,
        Long.toString(reqParams.getPosition()));

    if ((reqParams.getMode() == AppendRequestParameters.Mode.FLUSH_MODE) || (
        reqParams.getMode() == AppendRequestParameters.Mode.FLUSH_CLOSE_MODE)) {
      abfsUriQueryBuilder.addQuery(QUERY_PARAM_FLUSH, TRUE);
      if (reqParams.getMode() == AppendRequestParameters.Mode.FLUSH_CLOSE_MODE) {
        abfsUriQueryBuilder.addQuery(QUERY_PARAM_CLOSE, TRUE);
      }
    }

    // Check if the retry is with "Expect: 100-continue" header being present in the previous request.
    if (reqParams.isRetryDueToExpect()) {
      String userAgentRetry = getUserAgent();
      // Remove the specific marker related to "Expect: 100-continue" from the User-Agent string.
      userAgentRetry = userAgentRetry.replace(HUNDRED_CONTINUE_USER_AGENT, EMPTY_STRING);
      requestHeaders.removeIf(header -> header.getName().equalsIgnoreCase(USER_AGENT));
      requestHeaders.add(new AbfsHttpHeader(USER_AGENT, userAgentRetry));
    }

    // Add MD5 Hash of request content as request header if feature is enabled
    if (isChecksumValidationEnabled()) {
      addCheckSumHeaderForWrite(requestHeaders, reqParams, buffer);
    }

    // AbfsInputStream/AbfsOutputStream reuse SAS tokens for better performance
    String sasTokenForReuse = appendSASTokenToQuery(path,
        SASTokenProvider.WRITE_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.Append,
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
      if (checkUserError(responseStatusCode)
          && reqParams.isExpectHeaderEnabled()) {
        LOG.debug(
            "User error, retrying without 100 continue enabled for the given path {}",
            path);
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

      if (reqParams.isAppendBlob()
          && appendSuccessCheckOp(op, path,
          (reqParams.getPosition() + reqParams.getLength()), tracingContext)) {
        final AbfsRestOperation successOp = getAbfsRestOperation(
            AbfsRestOperationType.Append,
            HTTP_METHOD_PUT, url, requestHeaders,
            buffer, reqParams.getoffset(), reqParams.getLength(),
            sasTokenForReuse);
        successOp.hardSetResult(HttpURLConnection.HTTP_OK);
        return successOp;
      }
      throw e;
    } catch (AzureBlobFileSystemException e) {
      // Any server side issue will be returned as AbfsRestOperationException and will be handled above.
      LOG.debug(
          "Append request failed with non server issues for path: {}, offset: {}, position: {}",
          path, reqParams.getoffset(), reqParams.getPosition());
      throw e;
    }

    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update">
   *   Path - Update</a>.
   * Flush previously uploaded data to a file.
   * @param path on which data has to be flushed.
   * @param position to which data has to be flushed.
   * @param retainUncommittedData whether to retain uncommitted data after flush.
   * @param isClose specify if this is the last flush to the file.
   * @param cachedSasToken to be used for the authenticating operation.
   * @param leaseId if there is an active lease on the path.
   * @param contextEncryptionAdapter to provide encryption context.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation flush(final String path,
      final long position,
      boolean retainUncommittedData,
      boolean isClose,
      final String cachedSasToken,
      final String leaseId,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addEncryptionKeyRequestHeaders(path, requestHeaders, false,
        contextEncryptionAdapter, tracingContext);
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE, HTTP_METHOD_PATCH));
    if (leaseId != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, FLUSH_ACTION);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_POSITION, Long.toString(position));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RETAIN_UNCOMMITTED_DATA,
        String.valueOf(retainUncommittedData));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CLOSE, String.valueOf(isClose));
    // AbfsInputStream/AbfsOutputStream reuse SAS tokens for better performance
    String sasTokenForReuse = appendSASTokenToQuery(path,
        SASTokenProvider.WRITE_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.Flush,
        HTTP_METHOD_PUT, url, requestHeaders,
        sasTokenForReuse);
    op.execute(tracingContext);
    return op;
  }

  @Override
  public AbfsRestOperation flush(byte[] buffer,
      final String path,
      boolean isClose,
      final String cachedSasToken,
      final String leaseId,
      final String eTag,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    throw new UnsupportedOperationException(
        "Flush with blockIds not supported on DFS Endpoint");
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update">
   *   Path - Update</a>.
   * Set the properties of a file or directory.
   * @param path on which properties have to be set.
   * @param properties list of metadata key-value pairs.
   * @param tracingContext for tracing the server calls.
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
    final String commaSeparatedProperties;
    try {
      commaSeparatedProperties = convertXmsPropertiesToCommaSeparatedString(properties);
    } catch (CharacterCodingException ex) {
      throw new InvalidAbfsRestOperationException(ex);
    }

    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addEncryptionKeyRequestHeaders(path, requestHeaders, false,
        contextEncryptionAdapter, tracingContext);
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE, HTTP_METHOD_PATCH));
    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPERTIES, commaSeparatedProperties));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, SET_PROPERTIES_ACTION);
    appendSASTokenToQuery(path, SASTokenProvider.SET_PROPERTIES_OPERATION,
        abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetPathProperties,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/get-properties">
   *   Path - Get Properties</a>.
   * Get the properties of a file or directory.
   * @param path of which properties have to be fetched.
   * @param includeProperties to include user defined properties.
   * @param tracingContext for tracing the server calls.
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
    String operation = SASTokenProvider.GET_PROPERTIES_OPERATION;
    if (!includeProperties) {
      // The default action (operation) is implicitly to get properties and this action requires read permission
      // because it reads user defined properties.  If the action is getStatus or getAclStatus, then
      // only traversal (execute) permission is required.
      abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, GET_STATUS);
      operation = SASTokenProvider.GET_STATUS_OPERATION;
    } else {
      addEncryptionKeyRequestHeaders(path, requestHeaders, false,
          contextEncryptionAdapter, tracingContext);
    }
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN,
        String.valueOf(getAbfsConfiguration().isUpnUsed()));
    appendSASTokenToQuery(path, operation, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetPathStatus,
        HTTP_METHOD_HEAD, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/read">
   *   Path - Read</a>.
   * Read the contents of the file at specified path
   * @param path of the file to be read.
   * @param position in the file from where data has to be read.
   * @param buffer to store the data read.
   * @param bufferOffset offset in the buffer to start storing the data.
   * @param bufferLength length of data to be read.
   * @param eTag to specify conditional headers.
   * @param cachedSasToken to be used for the authenticating operation.
   * @param contextEncryptionAdapter to provide encryption context.
   * @param tracingContext for tracing the server calls.
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
      String cachedSasToken,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addEncryptionKeyRequestHeaders(path, requestHeaders, false,
        contextEncryptionAdapter, tracingContext);
    AbfsHttpHeader rangeHeader = new AbfsHttpHeader(RANGE,
        String.format("bytes=%d-%d", position, position + bufferLength - 1));
    requestHeaders.add(rangeHeader);
    requestHeaders.add(new AbfsHttpHeader(IF_MATCH, eTag));

    // Add request header to fetch MD5 Hash of data returned by server.
    if (isChecksumValidationEnabled(requestHeaders, rangeHeader, bufferLength)) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_RANGE_GET_CONTENT_MD5, TRUE));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    // AbfsInputStream/AbfsOutputStream reuse SAS tokens for better performance
    String sasTokenForReuse = appendSASTokenToQuery(path,
        SASTokenProvider.READ_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.ReadFile,
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
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/delete">
   *   Path - Delete</a>.
   * Delete the file or directory at specified path.
   * @param path to be deleted.
   * @param recursive if the path is a directory, delete recursively.
   * @param continuation to specify continuation token.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation deletePath(final String path,
      final boolean recursive,
      final String continuation,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    /*
     * If Pagination is enabled and current API version is old,
     * use the minimum required version for pagination.
     * If Pagination is enabled and current API version is later than minimum required
     * version for pagination, use current version only as azure service is backward compatible.
     * If pagination is disabled, use the current API version only.
     */
    final List<AbfsHttpHeader> requestHeaders = (isPaginatedDelete(recursive,
        getIsNamespaceEnabled()) && getxMsVersion().compareTo(
        ApiVersion.AUG_03_2023) < 0)
        ? createDefaultHeaders(ApiVersion.AUG_03_2023)
        : createDefaultHeaders();
    final AbfsUriQueryBuilder abfsUriQueryBuilder
        = createDefaultUriQueryBuilder();

    if (isPaginatedDelete(recursive, getIsNamespaceEnabled())) {
      // Add paginated query parameter
      abfsUriQueryBuilder.addQuery(QUERY_PARAM_PAGINATED, TRUE);
    }

    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RECURSIVE,
        String.valueOf(recursive));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CONTINUATION, continuation);
    String operation = recursive
        ? SASTokenProvider.DELETE_RECURSIVE_OPERATION
        : SASTokenProvider.DELETE_OPERATION;
    appendSASTokenToQuery(path, operation, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.DeletePath, this,
        HTTP_METHOD_DELETE, url, requestHeaders, getAbfsConfiguration());
    try {
      op.execute(tracingContext);
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

    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update">
   *   Path - Update</a>.
   * @param path on which owner has to be set.
   * @param owner to be set.
   * @param group to be set.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation setOwner(final String path,
      final String owner,
      final String group,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE, HTTP_METHOD_PATCH));
    if (owner != null && !owner.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_OWNER, owner));
    }
    if (group != null && !group.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_GROUP, group));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, SET_ACCESS_CONTROL);
    appendSASTokenToQuery(path, SASTokenProvider.SET_OWNER_OPERATION,
        abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetOwner,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update">
   *   Path - Update</a>.
   * @param path on which permission has to be set.
   * @param permission to be set.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation setPermission(final String path,
      final String permission,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE, HTTP_METHOD_PATCH));
    requestHeaders.add(new AbfsHttpHeader(
        HttpHeaderConfigurations.X_MS_PERMISSIONS, permission));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, SET_ACCESS_CONTROL);
    appendSASTokenToQuery(path, SASTokenProvider.SET_PERMISSION_OPERATION,
        abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetPermissions,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update">
   *   Path - Update</a>.
   * @param path on which ACL has to be set.
   * @param aclSpecString to be set.
   * @param eTag to specify conditional headers. Set only if etag matches.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation setAcl(final String path,
      final String aclSpecString,
      final String eTag,
      TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
        HTTP_METHOD_PATCH));
    requestHeaders.add(
        new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_ACL, aclSpecString));
    if (eTag != null && !eTag.isEmpty()) {
      requestHeaders.add(
          new AbfsHttpHeader(IF_MATCH, eTag));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder
        = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION,
        SET_ACCESS_CONTROL);
    appendSASTokenToQuery(path, SASTokenProvider.SET_ACL_OPERATION,
        abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetAcl,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/get-properties">
   *   Path - Get Properties</a>.
   * Retrieves the ACL properties of blob at specified path.
   * @param path of which properties have to be fetched.
   * @param useUPN whether to use UPN with rest operation.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation getAclStatus(final String path,
      final boolean useUPN,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, GET_ACCESS_CONTROL);
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN,
        String.valueOf(useUPN));
    appendSASTokenToQuery(path, SASTokenProvider.GET_ACL_OPERATION,
        abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetAcl,
        HTTP_METHOD_HEAD, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get Rest Operation for API
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/get-properties">
   *   Path - Get Properties</a>.
   * @param path Path for which access check needs to be performed
   * @param rwx The permission to be checked on the path
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  @Override
  public AbfsRestOperation checkAccess(String path,
      String rwx,
      TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, CHECK_ACCESS);
    abfsUriQueryBuilder.addQuery(QUERY_FS_ACTION, rwx);
    appendSASTokenToQuery(path, SASTokenProvider.CHECK_ACCESS_OPERATION,
        abfsUriQueryBuilder);

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.CheckAccess,
        HTTP_METHOD_HEAD, url, requestHeaders);
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
    String resourceType = result.getResponseHeader(
        HttpHeaderConfigurations.X_MS_RESOURCE_TYPE);
    return StringUtils.equalsIgnoreCase(resourceType, DIRECTORY);
  }

  /**
   * Returns true if the status code lies in the range of user error.
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
   * Get the continuation token from the response from DFS Endpoint Listing.
   * Continuation Token will be present as a response header.
   * @param result The response from the server.
   * @return The continuation token.
   */
  private String getContinuationFromResponse(AbfsHttpOperation result) {
    return result.getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);
  }

  /**
   * Get the user defined metadata from the response from DFS Endpoint API.
   * @param result response from server
   * @return user defined metadata as key value pairs
   * @throws InvalidFileSystemPropertyException if parsing fails
   * @throws InvalidAbfsRestOperationException if decoding fails
   */
  @Override
  public Hashtable<String, String> getXMSProperties(AbfsHttpOperation result)
      throws InvalidFileSystemPropertyException, InvalidAbfsRestOperationException {
    return parseCommaSeparatedXmsProperties(result.getResponseHeader(X_MS_PROPERTIES));
  }

  /**
   * Parse the list file response from DFS ListPath API in Json format
   * @param result InputStream contains the list results.
   * @param uri to be used for path conversion.
   * @return {@link ListResponseData}. containing listing response.
   * @throws AzureBlobFileSystemException if parsing fails.
   */
  @Override
  public ListResponseData parseListPathResults(AbfsHttpOperation result, URI uri)
      throws AzureBlobFileSystemException {
    try (InputStream stream = result.getListResultStream()) {
      try {
        DfsListResultSchema listResultSchema;
        final ObjectMapper objectMapper = new ObjectMapper();
        listResultSchema = objectMapper.readValue(stream, DfsListResultSchema.class);
        result.setListResultSchema(listResultSchema);
        LOG.debug("ListPath listed {} paths with {} as continuation token",
            listResultSchema.paths().size(),
            getContinuationFromResponse(result));
        List<VersionedFileStatus> fileStatuses = new ArrayList<>();
        for (DfsListResultEntrySchema entry : listResultSchema.paths()) {
          fileStatuses.add(getVersionedFileStatusFromEntry(entry, uri));
        }
        ListResponseData listResponseData = new ListResponseData();
        listResponseData.setFileStatusList(fileStatuses);
        listResponseData.setRenamePendingJsonPaths(null);
        listResponseData.setContinuationToken(
            getContinuationFromResponse(result));
        return listResponseData;
      } catch (JsonParseException | JsonMappingException ex) {
        throw new AbfsDriverException(ERR_DFS_LIST_PARSING, ex);
      }
    } catch (AbfsDriverException ex) {
      // Throw as it is to avoid multiple wrapping.
      LOG.error("Unable to deserialize list results for Uri {}", uri != null ? uri.toString(): "NULL", ex);
      throw ex;
    } catch (Exception ex) {
      LOG.error("Unable to deserialize list results for Uri {}", uri != null ? uri.toString(): "NULL", ex);
      throw new AbfsDriverException(ERR_DFS_LIST_PARSING, ex);
    }
  }

  @Override
  public List<String> parseBlockListResponse(final InputStream stream) throws IOException {
    return null;
  }

  /**
   * When the request fails, this function is used to parse the responseAbfsHttpClient.LOG.debug("ExpectedError: ", ex);
   * and extract the storageErrorCode and storageErrorMessage.  Any errors
   * encountered while attempting to process the error response are logged,
   * but otherwise ignored.
   *
   * For storage errors, the response body *usually* has the following format:
   *
   * {
   *   "error":
   *   {
   *     "code": "string",
   *     "message": "string"
   *   }
   * }
   *
   */
  @Override
  public StorageErrorResponseSchema processStorageErrorResponse(final InputStream stream) throws IOException {
    String storageErrorCode = "", storageErrorMessage = "", expectedAppendPos = "";
    try {
      JsonFactory jf = new JsonFactory();
      try (JsonParser jp = jf.createParser(stream)) {
        String fieldName, fieldValue;
        jp.nextToken();  // START_OBJECT - {
        jp.nextToken();  // FIELD_NAME - "error":
        jp.nextToken();  // START_OBJECT - {
        jp.nextToken();
        while (jp.hasCurrentToken()) {
          if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
            fieldName = jp.getCurrentName();
            jp.nextToken();
            fieldValue = jp.getText();
            switch (fieldName) {
            case "code":
              storageErrorCode = fieldValue;
              break;
            case "message":
              storageErrorMessage = fieldValue;
              break;
            case "ExpectedAppendPos":
              expectedAppendPos = fieldValue;
              break;
            default:
              break;
            }
          }
          jp.nextToken();
        }
      }
    } catch (IOException e) {
      throw e;
    }
    return new StorageErrorResponseSchema(storageErrorCode, storageErrorMessage, expectedAppendPos);
  }

  /**
   * Encode the value using ASCII encoding.
   * @param value to be encoded
   * @return encoded value
   * @throws UnsupportedEncodingException if encoding fails
   */
  @Override
  public byte[] encodeAttribute(String value) throws
      UnsupportedEncodingException {
    return value.getBytes(XMS_PROPERTIES_ENCODING_ASCII);
  }

  /**
   * Decode the value using ASCII encoding.
   * @param value to be decoded
   * @return decoded value
   * @throws UnsupportedEncodingException if encoding fails
   */
  @Override
  public String decodeAttribute(byte[] value) throws UnsupportedEncodingException {
    return new String(value, XMS_PROPERTIES_ENCODING_ASCII);
  }

  /**
   * Convert the properties hashtable to a comma separated string.
   * @param properties hashtable containing the properties key value pairs
   * @return comma separated string containing the properties key value pairs
   * @throws CharacterCodingException if encoding fails
   */
  private String convertXmsPropertiesToCommaSeparatedString(final Map<String,
        String> properties) throws CharacterCodingException {
    StringBuilder commaSeparatedProperties = new StringBuilder();

    final CharsetEncoder encoder = Charset.forName(XMS_PROPERTIES_ENCODING_ASCII).newEncoder();

    for (Map.Entry<String, String> propertyEntry : properties.entrySet()) {
      String key = propertyEntry.getKey();
      String value = propertyEntry.getValue();

      Boolean canEncodeValue = encoder.canEncode(value);
      if (!canEncodeValue) {
        LOG.error("Property value {} cannot be encoded using ASCII encoding", value);
        throw new CharacterCodingException();
      }

      String encodedPropertyValue = Base64.encode(encoder.encode(CharBuffer.wrap(value)).array());
      commaSeparatedProperties.append(key)
          .append(AbfsHttpConstants.EQUAL)
          .append(encodedPropertyValue);

      commaSeparatedProperties.append(AbfsHttpConstants.COMMA);
    }

    if (commaSeparatedProperties.length() != 0) {
      commaSeparatedProperties.deleteCharAt(commaSeparatedProperties.length() - 1);
    }

    return commaSeparatedProperties.toString();
  }

  /**
   * Parse the comma separated x-ms-properties string into a hashtable.
   * @param xMsProperties comma separated x-ms-properties string returned from server
   * @return hashtable containing the properties key value pairs
   * @throws InvalidFileSystemPropertyException if parsing fails
   * @throws InvalidAbfsRestOperationException if decoding fails
   */
  private Hashtable<String, String> parseCommaSeparatedXmsProperties(String xMsProperties) throws
      InvalidFileSystemPropertyException, InvalidAbfsRestOperationException {
    Hashtable<String, String> properties = new Hashtable<>();

    final CharsetDecoder decoder = Charset.forName(XMS_PROPERTIES_ENCODING_ASCII).newDecoder();

    if (xMsProperties != null && !xMsProperties.isEmpty()) {
      String[] userProperties = xMsProperties.split(AbfsHttpConstants.COMMA);

      if (userProperties.length == 0) {
        return properties;
      }

      for (String property : userProperties) {
        if (property.isEmpty()) {
          throw new InvalidFileSystemPropertyException(xMsProperties);
        }

        String[] nameValue = property.split(AbfsHttpConstants.EQUAL, 2);
        if (nameValue.length != 2) {
          throw new InvalidFileSystemPropertyException(xMsProperties);
        }

        byte[] decodedValue = Base64.decode(nameValue[1]);

        final String value;
        try {
          value = decoder.decode(ByteBuffer.wrap(decodedValue)).toString();
        } catch (CharacterCodingException ex) {
          throw new InvalidAbfsRestOperationException(ex);
        }
        properties.put(nameValue[0], value);
      }
    }

    return properties;
  }

  /**
   * Add the client transaction id to the request header
   * if {@link AbfsConfiguration#getIsClientTransactionIdEnabled()} is enabled.
   * @param requestHeaders list of headers to be sent with the request
   *
   * @return client transaction id
   */
  @VisibleForTesting
  public String addClientTransactionIdToHeader(List<AbfsHttpHeader> requestHeaders) {
    String clientTransactionId = null;
    // Set client transaction ID if the namespace and client transaction ID config are enabled.
    if (getIsNamespaceEnabled() && getAbfsConfiguration().getIsClientTransactionIdEnabled()) {
      clientTransactionId = UUID.randomUUID().toString();
      requestHeaders.add(
          new AbfsHttpHeader(X_MS_CLIENT_TRANSACTION_ID, clientTransactionId));
    }
    return clientTransactionId;
  }

  /**
   * Attempts to rename a path with client transaction ID (CTId) recovery mechanism.
   * If the initial rename attempt fails, it tries to recover using CTId or ETag
   * and retries the operation.
   *
   * @param source the source path to be renamed
   * @param destination the destination path for the rename
   * @param continuation the continuation token for the operation
   * @param tracingContext the context for tracing the operation
   * @param sourceEtag the ETag of the source path for conditional requests
   * @param isMetadataIncompleteState flag indicating if the metadata state is incomplete
   * @return an {@link AbfsClientRenameResult} containing the result of the rename operation
   * @throws IOException if an error occurs during the rename operation
   */
  private AbfsClientRenameResult renameWithCTIdRecovery(String source,
      String destination, String continuation, TracingContext tracingContext,
      String sourceEtag, boolean isMetadataIncompleteState) throws IOException {
    // Get request headers for rename operation
    List<AbfsHttpHeader> requestHeaders = getHeadersForRename(source);
    // Add client transaction ID to the request headers
    String clientTransactionId = addClientTransactionIdToHeader(requestHeaders);

    // Create the URL for the rename operation
    final URL url = createRequestUrl(destination,
        getRenameQueryBuilder(destination, continuation).toString());

    // Create the rename operation
    AbfsRestOperation op = createRenameRestOperation(url, requestHeaders);
    try {
      incrementAbfsRenamePath();
      op.execute(tracingContext);
      // If we successfully rename a path and isMetadataIncompleteState is true,
      // then the rename was recovered; otherwise, it wasn’t.
      // This is why isMetadataIncompleteState is used for renameRecovery (as the second parameter).
      return new AbfsClientRenameResult(op, isMetadataIncompleteState,
          isMetadataIncompleteState);
    } catch (AzureBlobFileSystemException e) {
      // Handle rename exceptions and retry if applicable
      handleRenameException(source, destination, continuation,
          tracingContext, sourceEtag, op, isMetadataIncompleteState, e);

      // Check if the operation is a retried request and if the error code indicates
      // that the source path was not found. If so, attempt recovery using CTId.
      if (op.isARetriedRequest()
          && SOURCE_PATH_NOT_FOUND.getErrorCode()
          .equalsIgnoreCase(op.getResult().getStorageErrorCode())) {
        if (recoveryUsingCTId(destination, tracingContext, clientTransactionId)) {
          return new AbfsClientRenameResult(
              getSuccessOp(AbfsRestOperationType.RenamePath,
                  HTTP_METHOD_PUT, url, requestHeaders),
              true, isMetadataIncompleteState);
        }
      }

      // Attempt recovery using ETag if applicable
      if (recoveryUsingEtag(source, destination, sourceEtag,
          op, tracingContext, true)) {
        return new AbfsClientRenameResult(
            getSuccessOp(AbfsRestOperationType.RenamePath,
                HTTP_METHOD_PUT, url, requestHeaders),
            true,
            isMetadataIncompleteState);
      }
      throw e;
    }
  }

  /**
   * Attempts to recover a rename operation using ETag. If the source ETag is not provided, it attempts
   * to fetch it and retry the operation. If recovery fails, it throws the exception.
   *
   * @param source the source path to be renamed
   * @param destination the destination path for the rename
   * @param continuation the continuation token for the operation
   * @param tracingContext the context for tracing the operation
   * @param sourceEtag the ETag of the source path for conditional requests
   * @param isMetadataIncompleteState flag indicating if the metadata state is incomplete
   * @return an {@link AbfsClientRenameResult} containing the result of the rename operation
   * @throws IOException if an error occurs during the rename operation or recovery
   */
  private AbfsClientRenameResult renameWithETagRecovery(String source,
      String destination, String continuation,
      TracingContext tracingContext, String sourceEtag,
      boolean isMetadataIncompleteState) throws IOException {
    boolean hasEtag = !isEmpty(sourceEtag);
    boolean shouldAttemptRecovery = isRenameResilience() && getIsNamespaceEnabled();
    if (!hasEtag && shouldAttemptRecovery) {
      // in case eTag is already not supplied to the API
      // and rename resilience is expected and it is an HNS enabled account
      // fetch the source etag to be used later in recovery
      try {
        final AbfsRestOperation srcStatusOp = getPathStatus(source,
            false, tracingContext, null);
        if (srcStatusOp.hasResult()) {
          final AbfsHttpOperation result = srcStatusOp.getResult();
          sourceEtag = extractEtagHeader(result);
          // and update the directory status.
          boolean isDir = checkIsDir(result);
          shouldAttemptRecovery = !isDir;
          LOG.debug(
              "Retrieved etag of source for rename recovery: {}; isDir={}",
              sourceEtag, isDir);
        }
      } catch (AbfsRestOperationException e) {
        throw new AbfsRestOperationException(e.getStatusCode(),
            SOURCE_PATH_NOT_FOUND.getErrorCode(), e.getMessage(), e);
      }
    }

    // Get request headers for rename operation
    List<AbfsHttpHeader> requestHeaders = getHeadersForRename(source);

    // Create the URL for the rename operation
    final URL url = createRequestUrl(destination,
        getRenameQueryBuilder(destination, continuation).toString());

    // Create the rename operation
    AbfsRestOperation op = createRenameRestOperation(url, requestHeaders);
    try {
      incrementAbfsRenamePath();
      op.execute(tracingContext);
      // If we successfully rename a path and isMetadataIncompleteState is true,
      // then the rename was recovered; otherwise, it wasn’t.
      // This is why isMetadataIncompleteState is used for renameRecovery (as the second parameter).
      return new AbfsClientRenameResult(op, isMetadataIncompleteState,
          isMetadataIncompleteState);
    } catch (AzureBlobFileSystemException e) {
      // Handle rename exceptions and retry if applicable
      handleRenameException(source, destination, continuation,
          tracingContext, sourceEtag, op, isMetadataIncompleteState, e);

      // Attempt recovery using ETag if applicable
      if (recoveryUsingEtag(source, destination, sourceEtag,
          op, tracingContext, shouldAttemptRecovery)) {
        return new AbfsClientRenameResult(
            getSuccessOp(AbfsRestOperationType.RenamePath,
                HTTP_METHOD_PUT, url, requestHeaders),
            true, isMetadataIncompleteState);
      }
      throw e;
    }
  }

  /**
   * Creates a list of HTTP headers required for a rename operation, including the encoded source path
   * and SAS token if applicable.
   *
   * @param source the source path for the rename operation
   * @return a list of {@link AbfsHttpHeader} containing the headers for the rename request
   * @throws IOException if an error occurs while creating the headers or encoding the source path
   */
  private List<AbfsHttpHeader> getHeadersForRename(final String source)
      throws IOException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    String encodedRenameSource = urlEncode(
        FORWARD_SLASH + this.getFileSystem() + source);

    if (getAuthType() == AuthType.SAS) {
      final AbfsUriQueryBuilder srcQueryBuilder = new AbfsUriQueryBuilder();
      appendSASTokenToQuery(source,
          SASTokenProvider.RENAME_SOURCE_OPERATION, srcQueryBuilder);
      encodedRenameSource += srcQueryBuilder.toString();
    }

    LOG.trace("Rename source queryparam added {}", encodedRenameSource);
    requestHeaders.add(new AbfsHttpHeader(X_MS_RENAME_SOURCE, encodedRenameSource));
    requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, STAR));
    return requestHeaders;
  }

  /**
   * Builds a query builder for the rename operation URL, including the continuation token and SAS token
   * for the destination path.
   *
   * @param destination the destination path for the rename operation
   * @param continuation the continuation token for the operation
   * @return an {@link AbfsUriQueryBuilder} containing the query parameters for the rename operation
   * @throws AzureBlobFileSystemException if an error occurs while appending the SAS token
   */
  private AbfsUriQueryBuilder getRenameQueryBuilder(final String destination,
      final String continuation) throws AzureBlobFileSystemException {
    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CONTINUATION, continuation);
    appendSASTokenToQuery(destination,
        SASTokenProvider.RENAME_DESTINATION_OPERATION, abfsUriQueryBuilder);
    return abfsUriQueryBuilder;
  }

  /**
   * Attempts to recover a rename operation using the client transaction ID (CTId).
   * It checks if the provided CTId matches the one in the response header for the destination path.
   *
   * @param destination the destination path for the rename operation
   * @param tracingContext the context for tracing the operation
   * @param clientTransactionId the client transaction ID to be used for recovery
   * @return true if the client transaction ID matches, indicating recovery can proceed; false otherwise
   * @throws AzureBlobFileSystemException if an error occurs while retrieving the path status
   */
  private boolean recoveryUsingCTId(String destination,
      TracingContext tracingContext, String clientTransactionId)
      throws AzureBlobFileSystemException {
    try {
      final AbfsHttpOperation abfsHttpOperation =
          getPathStatus(destination, false,
              tracingContext, null).getResult();
      return clientTransactionId.equals(
          abfsHttpOperation.getResponseHeader(X_MS_CLIENT_TRANSACTION_ID));
    } catch (AzureBlobFileSystemException exception) {
      throw new AbfsDriverException(ERR_RENAME_RECOVERY + destination, exception);
    }
  }

  /**
   * Attempts recovery using an ETag for the given source and destination.
   * If recovery is enabled and rename resilience is supported, performs an idempotency check
   * for the rename operation.
   *
   * @param source the source path to be renamed
   * @param destination the destination path for the rename
   * @param sourceEtag the ETag of the source path for conditional requests
   * @param op the AbfsRestOperation object for the rename operation
   * @param tracingContext the context for tracing the operation
   * @param shouldAttemptRecovery flag indicating whether recovery should be attempted
   * @return true if the recovery attempt was successful, false otherwise
   */
  private boolean recoveryUsingEtag(String source, String destination,
      String sourceEtag, AbfsRestOperation op, TracingContext tracingContext,
      boolean shouldAttemptRecovery) {
    if (shouldAttemptRecovery && isRenameResilience()) {
      return renameIdempotencyCheckOp(source, sourceEtag,
          op, destination, tracingContext);
    }
    return false;
  }

  /**
   * Checks for rename operation exceptions and handles them accordingly.
   * Throws an exception or retries the operation if certain error conditions are met,
   * such as unauthorized overwrite or missing destination parent path.
   *
   * @param source The source path for the rename operation.
   * @param destination The destination path for the rename operation.
   * @param continuation Continuation token for the operation, if applicable.
   * @param tracingContext The tracing context for tracking the operation.
   * @param sourceEtag The ETag of the source path for metadata validation.
   * @param op The ABFS operation result for the rename attempt.
   * @param isMetadataIncompleteState Flag indicating if metadata is incomplete.
   * @throws IOException If an I/O error occurs during the rename operation.
   * @throws FileAlreadyExistsException If the destination file already exists.
   */
  private void handleRenameException(final String source,
      final String destination, final String continuation,
      final TracingContext tracingContext, final String sourceEtag,
      final AbfsRestOperation op, boolean isMetadataIncompleteState,
      AzureBlobFileSystemException e) throws IOException {
    if (!op.hasResult()) {
      throw e;
    }

    // ref: HADOOP-19393. Write permission checks can occur before validating
    // rename operation's validity. If there is an existing destination path, it may be rejected
    // with an authorization error. Catching and throwing FileAlreadyExistsException instead.
    if (UNAUTHORIZED_BLOB_OVERWRITE.getErrorCode()
        .equals(op.getResult().getStorageErrorCode())) {
      throw new FileAlreadyExistsException(ERR_FILE_ALREADY_EXISTS);
    }

    // ref: HADOOP-18242. Rename failure occurring due to a rare case of
    // tracking metadata being in incomplete state.
    if (RENAME_DESTINATION_PARENT_PATH_NOT_FOUND.getErrorCode()
        .equals(op.getResult().getStorageErrorCode())
        && !isMetadataIncompleteState) {
      ABFS_METADATA_INCOMPLETE_RENAME_FAILURE.info(
          "Rename Failure attempting to resolve tracking metadata state and retrying.");
      isMetadataIncompleteState = true;
      String sourceEtagAfterFailure = sourceEtag;
      if (isEmpty(sourceEtagAfterFailure)) {
        // Doing a HEAD call resolves the incomplete metadata state and
        // then we can retry the rename operation.
        AbfsRestOperation sourceStatusOp = getPathStatus(source, false,
            tracingContext, null);
        // Extract the sourceEtag, using the status Op, and set it
        // for future rename recovery.
        AbfsHttpOperation sourceStatusResult = sourceStatusOp.getResult();
        sourceEtagAfterFailure = extractEtagHeader(sourceStatusResult);
      }

      // Retry the rename operation with the updated sourceEtag and isMetadataIncompleteState.
      renamePath(source, destination, continuation,
          tracingContext, sourceEtagAfterFailure, isMetadataIncompleteState);
    }
  }
}
