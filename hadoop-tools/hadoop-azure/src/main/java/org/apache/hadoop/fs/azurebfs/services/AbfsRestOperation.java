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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.hadoop.fs.azurebfs.authentication.AbfsAuthorizerResult;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsAuthorizationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsAuthorizerUnhandledException;
import org.apache.hadoop.fs.azurebfs.extensions.AbfsAuthorizer;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.authentication.AbfsStoreAuthenticator;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator.HttpException;

/**
 * The AbfsRestOperation for Rest AbfsClient.
 */
public class AbfsRestOperation {
  // The type of the REST operation (Append, ReadFile, etc)
  private final AbfsRestOperationType operationType;
  // Blob FS client, which has the credentials, retry policy, and logs.
  //private final AbfsClient client;
  // the HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE)
  private final String method;
  // full URL including query parameters
  private final URL url;
  // all the custom HTTP request headers provided by the caller
  private final List<AbfsHttpHeader> requestHeaders;

  // This is a simple operation class, where all the upload methods have a
  // request body and all the download methods have a response body.
  private final boolean hasRequestBody;

  private static final Logger LOG = LoggerFactory.getLogger(AbfsClient.class);

  // For uploads, this is the request entity body.  For downloads,
  // this will hold the response entity body.
  private byte[] buffer;
  private int bufferOffset;
  private int bufferLength;

  private AbfsHttpOperation result;
  private AbfsAuthorizer authorizer;
  private AbfsStoreAuthenticator storeAuthenticator;
  private AbfsPerfTracker abfsPerfTracker;
private final ExponentialRetryPolicy exponentialRetryPolicy;
private final AbfsFileSystemContext abfsFileSystemContext;

  public AbfsAuthorizerResult getAuthorizerResult() {
    return this.authorizerResult;
  }

  private AbfsAuthorizerResult authorizerResult;

private AbfsStoreAuthenticator abfsStoreAuthenticator;

  public AbfsHttpOperation getResult() {
    return result;
  }

  // TODO: Interact with authorizer
  /**
   * Initializes a new REST operation.
   *
   * @param method The HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE).
   * @param url The full URL including query string parameters.
   * @param requestHeaders The HTTP request headers.
   *//*
  AbfsRestOperation(
      final AbfsRestOperationType operationType,
      final AbfsClient client,
      final String method,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders,
      final AbfsStoreAuthenticator abfsStoreAuthenticator) {
    this.operationType = operationType;
    //this.client = client;
    this.method = method;
    this.url = url;
    this.requestHeaders = requestHeaders;
    this.hasRequestBody = (AbfsHttpConstants.HTTP_METHOD_PUT.equals(method)
        || AbfsHttpConstants.HTTP_METHOD_PATCH.equals(method));
    this.storeAuthenticator = abfsStoreAuthenticator;
    exponentialRetryPolicy = new ExponentialRetryPolicy();
  }
*/
  AbfsRestOperation(
      final AbfsRestOperationType operationType,
      final String method,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders,
      final AbfsFileSystemContext abfsFileSystemContext) {
    this.operationType = operationType;
    //this.client = client;
    this.method = method;
    this.url = url;
    this.requestHeaders = requestHeaders;
    this.hasRequestBody = (AbfsHttpConstants.HTTP_METHOD_PUT.equals(method)
        || AbfsHttpConstants.HTTP_METHOD_PATCH.equals(method));
    this.abfsFileSystemContext = abfsFileSystemContext;
    this.storeAuthenticator = abfsFileSystemContext.getAbfsStoreAuthenticator();
    this.abfsPerfTracker = abfsFileSystemContext.getAbfsPerfTracker();
    exponentialRetryPolicy = new ExponentialRetryPolicy();
  }

  AbfsRestOperation(AbfsRestOperationType operationType,
      String method,
      URL url,
      List<AbfsHttpHeader> requestHeaders,
      byte[] buffer,
      int bufferOffset,
      int bufferLength,
      final AbfsFileSystemContext abfsFileSystemContext) {
    this(operationType, method, url, requestHeaders, abfsFileSystemContext);
    this.buffer = buffer;
    this.bufferOffset = bufferOffset;
    this.bufferLength = bufferLength;
  }

  AbfsRestOperation(
      final AbfsRestOperationType operationType,
      final String method,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders,
      final AbfsFileSystemContext abfsFileSystemContext,
      final AbfsAuthorizerResult abfsAuthorizerResult) {
    this(operationType, method, url, requestHeaders, abfsFileSystemContext);
    processAbfsAuthorizerResult(abfsAuthorizerResult);
 }

  /**
   * Initializes a new REST operation.
   *
   * @param operationType The type of the REST operation (Append, ReadFile, etc).
   * @param method The HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE).
   * @param url The full URL including query string parameters.
   * @param requestHeaders The HTTP request headers.
   * @param buffer For uploads, this is the request entity body.  For downloads,
   *               this will hold the response entity body.
   * @param bufferOffset An offset into the buffer where the data beings.
   * @param bufferLength The length of the data in the buffer.
   */
  AbfsRestOperation(AbfsRestOperationType operationType,
      String method,
      URL url,
      List<AbfsHttpHeader> requestHeaders,
      byte[] buffer,
      int bufferOffset,
      int bufferLength,
      final AbfsFileSystemContext abfsFileSystemContext,
      final AbfsAuthorizerResult abfsAuthorizerResult) {
    this(operationType, method, url, requestHeaders,
        buffer, bufferOffset, bufferLength, abfsFileSystemContext);
    processAbfsAuthorizerResult(abfsAuthorizerResult);
  }

  private void processAbfsAuthorizerResult(AbfsAuthorizerResult abfsAuthorizerResult)
  {
    if (abfsAuthorizerResult.isAuthorizationStatusFetched())
    {
      this.authorizerResult = abfsAuthorizerResult;
      if (abfsAuthorizerResult.isAuthorized() && abfsAuthorizerResult.isSasTokenAvailable())
      {
        this.abfsStoreAuthenticator =
            AbfsStoreAuthenticator.getAbfsStoreAuthenticatorForSAS(abfsAuthorizerResult.getSASToken());
      }
      else {
        this.abfsStoreAuthenticator = abfsFileSystemContext.getAbfsStoreAuthenticator();
      }

    }
  }

  private void checkIfAuthorizedAndUpdateAbfsStoreAuthenticator()
      throws AzureBlobFileSystemException {
    // TODO: check code here
    String pathRelativeFromRoot = this.url.getPath();
    String abfsFSName = this.abfsFileSystemContext.getFileSystemName();
    int abfsFSIndex = pathRelativeFromRoot.indexOf(abfsFSName);
    pathRelativeFromRoot = pathRelativeFromRoot
        .substring(abfsFSIndex + abfsFSName.length());

    if (authorizer == null) {
      LOG.debug(
          "ABFS authorizer is not initialized. No authorization check will be"
              + " performed.");
    } else {
      if (!this.authorizerResult.isAuthorizationStatusFetched()) {
        updateAuthorizationStatus(abfsFileSystemContext.getAuthorizer(),
            operationType, pathRelativeFromRoot);
      }

      if (this.authorizerResult.isAuthorized() && this.authorizerResult
          .isSasTokenAvailable()) {
        this.abfsStoreAuthenticator = AbfsStoreAuthenticator
            .getAbfsStoreAuthenticatorForSAS(
                this.authorizerResult.getSASToken());
      }
    }
  }

  private void updateAuthorizationStatus(AbfsAuthorizer authorizer,
      AbfsRestOperationType operationType, String pathRelativeFromRoot)
      throws AzureBlobFileSystemException {

    LOG.debug("Auth check for action: {} on paths: {}", operationType,
        pathRelativeFromRoot);

    boolean isAuthorized = false;

    try {
      isAuthorized = !authorizer.isAuthorized(operationType, pathRelativeFromRoot);
    } catch (IOException e) {
      throw new AbfsAuthorizerUnhandledException(e);
    }

    this.authorizerResult.setAuthorizationStatusFetched(true);

    if (!isAuthorized) {
      throw new AbfsAuthorizationException(
          "User is not authorized for action " + operationType + " on paths: "
              + pathRelativeFromRoot, new IOException());
    }

    this.authorizerResult.setAuthorized(true);
    if (storeAuthenticator.getAuthType() == AuthType.SAS) {
      String sasToken = authorizer.getSASToken();
      if ((sasToken != null) && !(sasToken.isEmpty())) {
        this.authorizerResult.setSasTokenAvailable(true);
        this.authorizerResult.setSASToken(sasToken);
      } else {
        // TODO: different exception ?
        throw new AbfsAuthorizationException(
            "User is not authorized for action " + operationType + " on path: "
                + pathRelativeFromRoot, new IOException());
      }
    }
  }

  /**
   * Executes the REST operation with retry, by issuing one or more
   * HTTP operations.
   */
  void execute() throws AzureBlobFileSystemException  {
    // see if we have latency reports from the previous requests
    String latencyHeader =
        this.abfsFileSystemContext.getAbfsPerfTracker().getClientLatency();
    if (latencyHeader != null && !latencyHeader.isEmpty()) {
      AbfsHttpHeader httpHeader =
              new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_ABFS_CLIENT_LATENCY, latencyHeader);
      requestHeaders.add(httpHeader);
    }

    int retryCount = 0;

    try {
      checkIfAuthorizedAndUpdateAbfsStoreAuthenticator();
    }
    catch (AbfsAuthorizationException ex)
    {
      throw new AbfsRestOperationException(result.getStatusCode(),
          result.getStorageErrorCode(),
        result.getStorageErrorMessage(), null, result);
    }
    catch (IOException ex)
    {
      throw new AbfsRestOperationException(result.getStatusCode(),
          result.getStorageErrorCode(),
        result.getStorageErrorMessage(), null, result);
    }

    while (true) {
      try {
        if (!!executeHttpOperation(retryCount++))
          break;
      } catch (URISyntaxException e) {
        e.printStackTrace();
      }
      try {
        Thread.sleep(exponentialRetryPolicy.getRetryInterval(retryCount));
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }

    if (result.getStatusCode() >= HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new AbfsRestOperationException(result.getStatusCode(), result.getStorageErrorCode(),
          result.getStorageErrorMessage(), null, result);
    }
  }

  /**
   * Executes a single HTTP operation to complete the REST operation.  If it
   * fails, there may be a retry.  The retryCount is incremented with each
   * attempt.
   */
  private boolean executeHttpOperation(final int retryCount)
      throws AzureBlobFileSystemException, URISyntaxException {
    AbfsHttpOperation httpOperation = null;
    try {
      httpOperation = getHttpOperationWithAuthInfoApplied(url, method,
          requestHeaders);

      // dump the headers
      AbfsIoUtils.dumpHeadersToDebugLog("Request Headers",
          httpOperation.getConnection().getRequestProperties());
      AbfsClientThrottlingIntercept.sendingRequest(operationType);

      if (hasRequestBody) {
        // HttpUrlConnection requires
        httpOperation.sendRequest(buffer, bufferOffset, bufferLength);
      }

      httpOperation.processResponse(buffer, bufferOffset, bufferLength);
    } catch (IOException ex) {
      if (ex instanceof UnknownHostException) {
        LOG.warn(String.format("Unknown host name: %s. Retrying to resolve the host name...", httpOperation.getUrl().getHost()));
      }

      if (LOG.isDebugEnabled()) {
        if (httpOperation != null) {
          LOG.debug("HttpRequestFailure: " + httpOperation.toString(), ex);
        } else {
          LOG.debug("HttpRequestFailure: " + method + "," + url, ex);
        }
      }

      if (!exponentialRetryPolicy.shouldRetry(retryCount, -1)) {
        throw new InvalidAbfsRestOperationException(ex);
      }

      // once HttpException is thrown by AzureADAuthenticator,
      // it indicates the policy in AzureADAuthenticator determined
      // retry is not needed
      if (ex instanceof HttpException) {
        throw new AbfsRestOperationException((HttpException) ex);
      }

      return false;
    } finally {
      AbfsClientThrottlingIntercept.updateMetrics(operationType, httpOperation);
    }

    LOG.debug("HttpRequest: " + httpOperation.toString());

    if (exponentialRetryPolicy.shouldRetry(retryCount, httpOperation.getStatusCode())) {
      return false;
    }

    result = httpOperation;

    return true;
  }

  private AbfsHttpOperation getHttpOperationWithAuthInfoApplied(URL url, String method,
      List<AbfsHttpHeader> requestHeaders)
      throws IOException, URISyntaxException {
    AbfsHttpOperation httpOperation = null;

    switch (storeAuthenticator.getAuthType())
    {
    case OAuth:
    case Custom:
      httpOperation = new AbfsHttpOperation(url, method, requestHeaders);
      httpOperation.getConnection().setRequestProperty(HttpHeaderConfigurations.AUTHORIZATION,
          storeAuthenticator.getAccessToken());
      break;
    case SharedKey:
      httpOperation = new AbfsHttpOperation(url, method, requestHeaders);
      httpOperation.getConnection().setRequestProperty(HttpHeaderConfigurations.AUTHORIZATION,
          storeAuthenticator.getSharedKeyCredentials().getRequestSignature(
              httpOperation.getConnection(),
              hasRequestBody ? bufferLength : 0));
      break;
    case SAS:
      URL requestUrl = UriUtils
          .addSASToRequestUrl(this.url,
              this.abfsStoreAuthenticator.getSasToken());
      httpOperation = new AbfsHttpOperation(requestUrl, method, requestHeaders);
      break;
    }

    return httpOperation;
  }
}
