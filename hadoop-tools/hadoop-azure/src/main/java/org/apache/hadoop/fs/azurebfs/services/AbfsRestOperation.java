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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.authentication.AuthorizationStatus;
import org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsAuthorizationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.extensions.AbfsAuthorizer;
import org.apache.hadoop.fs.azurebfs.extensions.AuthorizationResource;
import org.apache.hadoop.fs.azurebfs.extensions.AuthorizationResult;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator.HttpException;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;

/**
 * The AbfsRestOperation for Rest AbfsClient.
 */
public class AbfsRestOperation {
  // The type of the REST operation (Append, ReadFile, etc)
  private final AbfsRestOperationType operationType;
  // Blob FS client, which has the credentials, retry policy, and logs.
  private final AbfsClient client;
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
  private AuthorizationStatus authorizationStatus;

  public AbfsHttpOperation getResult() {
    return result;
  }

  public AuthorizationStatus getAuthorizationStatus() {
    return authorizationStatus;
  }

  /**
   * Initializes a new REST operation.
   *
   * @param client The Blob FS client.
   * @param method The HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE).
   * @param url The full URL including query string parameters.
   * @param requestHeaders The HTTP request headers.
   */
  AbfsRestOperation(final AbfsRestOperationType operationType,
                    final AbfsClient client,
                    final String method,
                    final URL url,
                    final List<AbfsHttpHeader> requestHeaders) {
    this.operationType = operationType;
    this.client = client;
    this.method = method;
    this.url = url;
    this.requestHeaders = requestHeaders;
    this.hasRequestBody = (AbfsHttpConstants.HTTP_METHOD_PUT.equals(method)
            || AbfsHttpConstants.HTTP_METHOD_PATCH.equals(method));
    this.authorizer = client.getAuthorizer();
  }

  /**
   * Initializes a new REST operation.
   *
   * @param operationType The type of the REST operation (Append, ReadFile, etc).
   * @param client The Blob FS client.
   * @param method The HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE).
   * @param url The full URL including query string parameters.
   * @param requestHeaders The HTTP request headers.
   * @param buffer For uploads, this is the request entity body.  For downloads,
   *               this will hold the response entity body.
   * @param bufferOffset An offset into the buffer where the data beings.
   * @param bufferLength The length of the data in the buffer.
   */
  AbfsRestOperation(AbfsRestOperationType operationType,
                    AbfsClient client,
                    String method,
                    URL url,
                    List<AbfsHttpHeader> requestHeaders,
                    byte[] buffer,
                    int bufferOffset,
                    int bufferLength) {
    this(operationType, client, method, url, requestHeaders);
    this.buffer = buffer;
    this.bufferOffset = bufferOffset;
    this.bufferLength = bufferLength;
  }

  /**
   * Constructor that accepts cached authorization status
   * used by read and append APIs.
   * @param opType
   * @param abfsClient
   * @param httpMethod
   * @param url
   * @param requestHeaders
   * @param buffer
   * @param bufferOffset
   * @param bufferLength
   * @param authzStatus
   */
  public AbfsRestOperation(final AbfsRestOperationType opType,
      final AbfsClient abfsClient,
      final String httpMethod,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders,
      final byte[] buffer,
      final int bufferOffset,
      final int bufferLength,
      final AuthorizationStatus authzStatus) {
    this(opType, abfsClient, httpMethod, url, requestHeaders, buffer,
        bufferOffset, bufferLength);
    this.authorizationStatus = authzStatus;
  }

  /**
   * Constructor that accepts cached authorization status
   * used by flush API.
   * @param opType
   * @param abfsClient
   * @param httpMethod
   * @param url
   * @param requestHeaders
   * @param authzStatus
   */
  public AbfsRestOperation(final AbfsRestOperationType opType,
      final AbfsClient abfsClient,
      final String httpMethod,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders,
      final AuthorizationStatus authzStatus) {
    this(opType, abfsClient, httpMethod, url, requestHeaders);
    this.authorizationStatus = authzStatus;
  }

  /**
   * Executes the REST operation with retry, by issuing one or more
   * HTTP operations.
   */
  void execute() throws AzureBlobFileSystemException {

    checkAuthorizationStatus();

    // see if we have latency reports from the previous requests
    String latencyHeader = this.client.getAbfsPerfTracker().getClientLatency();
    if (latencyHeader != null && !latencyHeader.isEmpty()) {
      AbfsHttpHeader httpHeader =
              new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_ABFS_CLIENT_LATENCY, latencyHeader);
      requestHeaders.add(httpHeader);
    }

    int retryCount = 0;
    LOG.debug("First execution of REST operation - {}", operationType);
    while (!executeHttpOperation(retryCount++)) {
      try {
        LOG.debug("Retrying REST operation {}. RetryCount = {}",
            operationType, retryCount);
        Thread.sleep(client.getRetryPolicy().getRetryInterval(retryCount));
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }

    //reset IsAuthorizedStatus For NonSAS Authorizers
    // - I/O stream cache the authz Status, for non-SAS authorization,
    // reset authzstatus now so that each request leads to
    // authorization check (if SAS token was present, there would be a
    // timeout period where the status will be reset, for non-SAS case saving
    // isAuthorized will cause the status saved in irreversible way).
    // - APIs such as rename and liststatus perform authorization at AbfsClient
    // These APIs are the only current ones that will be passing already
    // fetched authorization status when non-SAS authorizer is configured.
    if ((authorizer != null) && (authorizer.getAuthType() != AuthType.SAS))
    {
      this.authorizationStatus.setAuthorized(false);
    }

    if (result.getStatusCode() >= HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new AbfsRestOperationException(result.getStatusCode(), result.getStorageErrorCode(),
          result.getStorageErrorMessage(), null, result);
    }

    LOG.trace("{} REST operation complete", operationType);
  }

  /**
   * Checks if the available AuthorizationStatus is valid.
   * @throws AzureBlobFileSystemException
   */
  private void checkAuthorizationStatus() throws AzureBlobFileSystemException {

    if ((this.authorizer == null)) {
      return; // no external authorizer or authorizer not providing Auth Token
    }

    URI qualifiedUri;

    try {
      qualifiedUri = UriUtils.getQualifiedPathURI(this.url);
    } catch (URISyntaxException ex) {
      throw new InvalidUriException("Invalid URL: " + this.url.toString());
    }

    // return if
    // a. authorizationStatus is not null (presence of
    // authorizationStatus means user is authorized) and isAuthorized +
    // authorizer authtype is anything but SAS
    // b. authorizer Authtype is SAS and SAS is valid
    if ((this.authorizationStatus != null) &&
        this.authorizationStatus.isAuthorized() &&
        (this.authorizer.getAuthType() != AuthType.SAS)) {
      LOG.debug("Provided authorization status is valid (Authorizer Authype "
          + "is not SAS");
      return;
    } else if ((this.authorizer.getAuthType() == AuthType.SAS)
        && this.authorizationStatus.isValidSas(qualifiedUri)) {
      LOG.debug("Provided authorization status and the SAS token is valid");
      return; // Current sasData is already valid.
    } else {
      this.authorizationStatus = null; // reset authzStatus
    }

    String authorizerAction = AbfsAuthorizerConstants.getAction(operationType);

    AuthorizationResource[] authResource = new AuthorizationResource[1];
    authResource[0] = new AuthorizationResource(authorizerAction, qualifiedUri);

    this.authorizationStatus = fetchAuthorizationStatus(this.authorizer,
        authResource);
  }

  /**
   * Fetch Authorization status for a store path on a action.
   * @param authorizer
   * @param authorizationResource
   * @return AuthorizationStatus
   * @throws AzureBlobFileSystemException
   */
  public static AuthorizationStatus fetchAuthorizationStatus(
      AbfsAuthorizer authorizer,
      AuthorizationResource[] authorizationResource)
      throws AzureBlobFileSystemException {

    for (AuthorizationResource authResource : authorizationResource) {
      LOG.debug("checkPrivileges: Action={} Path={}",
          authResource.getAuthorizerAction(),
          authResource.getStorePathUri().getPath());
    }

    AuthorizationResult authzResult = null;

    try {
      authzResult = authorizer.checkPrivileges(authorizationResource);
    } catch (Exception e) {
      LOG.debug("checkPrivileges failed with Exception:{} - {}", e.getClass()
          .getCanonicalName(), e.getMessage());
      e.printStackTrace();
      throw e;
    }

    AuthorizationStatus authzStatus = null;

    if ((authzResult != null) && authzResult.isAuthorized()) {
      authzStatus = new AuthorizationStatus();
      authzStatus.setAuthorized(true);
      LOG.debug("Authorization check passed.");

      // User is authorized. Check for SAS Token.
      if (authorizer.getAuthType() == AuthType.SAS) {
        if (authzResult.getAuthResourceResult().length == authorizationResource.length) {
          authzStatus.setSasToken(authorizationResource, authzResult);
        } else {
          throw new AbfsAuthorizationException(
              "Authorizer failed to provide SAS token.", new IOException());
        }
      }
    }

    return authzStatus;
  }

  /**
   * Updates Http Request with Auth Token.
   * @param url
   * @param method
   * @param requestHeaders
   * @return
   * @throws IOException
   */
  private AbfsHttpOperation getHttpOperationWithAuthInfoApplied(URL url,
      String method, List<AbfsHttpHeader> requestHeaders) throws IOException {
    AbfsHttpOperation httpOperation = null;

    switch (this.client.getAuthType()) {
    case OAuth:
    case Custom:
      LOG.debug("Authenticating request with OAuth2 access token");
      httpOperation = new AbfsHttpOperation(url, method, requestHeaders);
      httpOperation.getConnection()
          .setRequestProperty(HttpHeaderConfigurations.AUTHORIZATION,
              this.client.getAccessToken());
      break;
    case SharedKey:
      LOG.debug("Signing request with shared key");
      httpOperation = new AbfsHttpOperation(url, method, requestHeaders);
      // sign the HTTP request
      client.getSharedKeyCredentials()
          .signRequest(httpOperation.getConnection(),
              hasRequestBody ? bufferLength : 0);
      break;
    case SAS:
      LOG.debug("Adding SAS token");
      URL requestUrl = null;
      try {
        requestUrl = UriUtils.addSASToRequestUrl(this.url,
            this.authorizationStatus
                .getSasTokenQuery(UriUtils.getQualifiedPathURI(this.url)));
        httpOperation = new AbfsHttpOperation(requestUrl, method,
            requestHeaders);
        break;
      } catch (URISyntaxException | MalformedURLException e) {
        throw new InvalidUriException(e.getMessage());
      }
    }

    return httpOperation;
  }

  /**
   * Executes a single HTTP operation to complete the REST operation.  If it
   * fails, there may be a retry.  The retryCount is incremented with each
   * attempt.
   */
  private boolean executeHttpOperation(final int retryCount) throws AzureBlobFileSystemException {
    AbfsHttpOperation httpOperation = null;
    try {
      // initialize the HTTP request and open the connection
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

      if (!client.getRetryPolicy().shouldRetry(retryCount, -1)) {
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

    LOG.debug("HttpRequest: {}", httpOperation.toString());

    if (client.getRetryPolicy().shouldRetry(retryCount, httpOperation.getStatusCode())) {
      return false;
    }

    result = httpOperation;

    return true;
  }
}
