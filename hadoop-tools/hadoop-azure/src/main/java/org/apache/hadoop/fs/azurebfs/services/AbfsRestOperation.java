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
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.AbfsStatistic;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_CONTINUE;

/**
 * The AbfsRestOperation for Rest AbfsClient.
 */
public class AbfsRestOperation {
  // The type of the REST operation (Append, ReadFile, etc)
  private final AbfsRestOperationType operationType;
  // Blob FS client, which has the credentials, retry policy, and logs.
  private final AbfsClient client;
  // Return intercept instance
  private final AbfsThrottlingIntercept intercept;
  // the HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE)
  private final String method;
  // full URL including query parameters
  private final URL url;
  // all the custom HTTP request headers provided by the caller
  private final List<AbfsHttpHeader> requestHeaders;

  // This is a simple operation class, where all the upload methods have a
  // request body and all the download methods have a response body.
  private final boolean hasRequestBody;

  // Used only by AbfsInputStream/AbfsOutputStream to reuse SAS tokens.
  private final String sasToken;

  private static final Logger LOG = LoggerFactory.getLogger(AbfsClient.class);

  // For uploads, this is the request entity body.  For downloads,
  // this will hold the response entity body.
  private byte[] buffer;
  private int bufferOffset;
  private int bufferLength;
  private int retryCount = 0;

  private AbfsHttpOperation result;
  private AbfsCounters abfsCounters;

  /**
   * This variable contains the reason of last API call within the same
   * AbfsRestOperation object.
   */
  private String failureReason;

  /**
   * Checks if there is non-null HTTP response.
   * @return true if there is a non-null HTTP response from the ABFS call.
   */
  public boolean hasResult() {
    return result != null;
  }

  public AbfsHttpOperation getResult() {
    return result;
  }

  public void hardSetResult(int httpStatus) {
    result = AbfsHttpOperation.getAbfsHttpOperationWithFixedResult(this.url,
        this.method, httpStatus);
  }

  public URL getUrl() {
    return url;
  }

  public List<AbfsHttpHeader> getRequestHeaders() {
    return requestHeaders;
  }

  public boolean isARetriedRequest() {
    return (retryCount > 0);
  }

  String getSasToken() {
    return sasToken;
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
    this(operationType, client, method, url, requestHeaders, null);
  }

  /**
   * Initializes a new REST operation.
   *
   * @param client The Blob FS client.
   * @param method The HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE).
   * @param url The full URL including query string parameters.
   * @param requestHeaders The HTTP request headers.
   * @param sasToken A sasToken for optional re-use by AbfsInputStream/AbfsOutputStream.
   */
  AbfsRestOperation(final AbfsRestOperationType operationType,
                    final AbfsClient client,
                    final String method,
                    final URL url,
                    final List<AbfsHttpHeader> requestHeaders,
                    final String sasToken) {
    this.operationType = operationType;
    this.client = client;
    this.method = method;
    this.url = url;
    this.requestHeaders = requestHeaders;
    this.hasRequestBody = (AbfsHttpConstants.HTTP_METHOD_PUT.equals(method)
            || AbfsHttpConstants.HTTP_METHOD_POST.equals(method)
            || AbfsHttpConstants.HTTP_METHOD_PATCH.equals(method));
    this.sasToken = sasToken;
    this.abfsCounters = client.getAbfsCounters();
    this.intercept = client.getIntercept();
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
   * @param sasToken A sasToken for optional re-use by AbfsInputStream/AbfsOutputStream.
   */
  AbfsRestOperation(AbfsRestOperationType operationType,
                    AbfsClient client,
                    String method,
                    URL url,
                    List<AbfsHttpHeader> requestHeaders,
                    byte[] buffer,
                    int bufferOffset,
                    int bufferLength,
                    String sasToken) {
    this(operationType, client, method, url, requestHeaders, sasToken);
    this.buffer = buffer;
    this.bufferOffset = bufferOffset;
    this.bufferLength = bufferLength;
    this.abfsCounters = client.getAbfsCounters();
  }

  /**
   * Execute a AbfsRestOperation. Track the Duration of a request if
   * abfsCounters isn't null.
   * @param tracingContext TracingContext instance to track correlation IDs
   */
  public void execute(TracingContext tracingContext)
      throws AzureBlobFileSystemException {

    try {
      IOStatisticsBinding.trackDurationOfInvocation(abfsCounters,
          AbfsStatistic.getStatNameFromHttpCall(method),
          () -> completeExecute(tracingContext));
    } catch (AzureBlobFileSystemException aze) {
      throw aze;
    } catch (IOException e) {
      throw new UncheckedIOException("Error while tracking Duration of an "
          + "AbfsRestOperation call", e);
    }
  }

  /**
   * Executes the REST operation with retry, by issuing one or more
   * HTTP operations.
   * @param tracingContext TracingContext instance to track correlation IDs
   */
  private void completeExecute(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    // see if we have latency reports from the previous requests
    String latencyHeader = getClientLatency();
    if (latencyHeader != null && !latencyHeader.isEmpty()) {
      AbfsHttpHeader httpHeader =
              new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_ABFS_CLIENT_LATENCY, latencyHeader);
      requestHeaders.add(httpHeader);
    }

    retryCount = 0;
    LOG.debug("First execution of REST operation - {}", operationType);
    while (!executeHttpOperation(retryCount, tracingContext)) {
      try {
        ++retryCount;
        tracingContext.setRetryCount(retryCount);
        LOG.debug("Retrying REST operation {}. RetryCount = {}",
            operationType, retryCount);
        Thread.sleep(client.getRetryPolicy().getRetryInterval(retryCount));
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }

    int status = result.getStatusCode();
    /*
      If even after exhausting all retries, the http status code has an
      invalid value it qualifies for InvalidAbfsRestOperationException.
      All http status code less than 1xx range are considered as invalid
      status codes.
     */
    if (status < HTTP_CONTINUE) {
      throw new InvalidAbfsRestOperationException(null, retryCount);
    }

    if (status >= HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new AbfsRestOperationException(result.getStatusCode(), result.getStorageErrorCode(),
          result.getStorageErrorMessage(), null, result);
    }
    LOG.trace("{} REST operation complete", operationType);
  }

  @VisibleForTesting
  String getClientLatency() {
    return client.getAbfsPerfTracker().getClientLatency();
  }

  /**
   * Executes a single HTTP operation to complete the REST operation.  If it
   * fails, there may be a retry.  The retryCount is incremented with each
   * attempt.
   */
  private boolean executeHttpOperation(final int retryCount,
    TracingContext tracingContext) throws AzureBlobFileSystemException {
    AbfsHttpOperation httpOperation;

    try {
      // initialize the HTTP request and open the connection
      httpOperation = createHttpOperation();
      incrementCounter(AbfsStatistic.CONNECTIONS_MADE, 1);
      tracingContext.constructHeader(httpOperation, failureReason);

      signRequest(httpOperation, hasRequestBody ? bufferLength : 0);

    } catch (IOException e) {
      LOG.debug("Auth failure: {}, {}", method, url);
      throw new AbfsRestOperationException(-1, null,
          "Auth failure: " + e.getMessage(), e);
    }

    try {
      // dump the headers
      AbfsIoUtils.dumpHeadersToDebugLog("Request Headers",
          httpOperation.getConnection().getRequestProperties());
      intercept.sendingRequest(operationType, abfsCounters);
      if (hasRequestBody) {
        // HttpUrlConnection requires
        httpOperation.sendRequest(buffer, bufferOffset, bufferLength);
        incrementCounter(AbfsStatistic.SEND_REQUESTS, 1);
        incrementCounter(AbfsStatistic.BYTES_SENT, bufferLength);
      }

      httpOperation.processResponse(buffer, bufferOffset, bufferLength);
      incrementCounter(AbfsStatistic.GET_RESPONSES, 1);
      //Only increment bytesReceived counter when the status code is 2XX.
      if (httpOperation.getStatusCode() >= HttpURLConnection.HTTP_OK
          && httpOperation.getStatusCode() <= HttpURLConnection.HTTP_PARTIAL) {
        incrementCounter(AbfsStatistic.BYTES_RECEIVED,
            httpOperation.getBytesReceived());
      } else if (httpOperation.getStatusCode() == HttpURLConnection.HTTP_UNAVAILABLE) {
        incrementCounter(AbfsStatistic.SERVER_UNAVAILABLE, 1);
      }
    } catch (UnknownHostException ex) {
      String hostname = null;
      hostname = httpOperation.getHost();
      failureReason = RetryReason.getAbbreviation(ex, null, null);
      LOG.warn("Unknown host name: {}. Retrying to resolve the host name...",
          hostname);
      if (!client.getRetryPolicy().shouldRetry(retryCount, -1)) {
        throw new InvalidAbfsRestOperationException(ex, retryCount);
      }
      return false;
    } catch (IOException ex) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("HttpRequestFailure: {}, {}", httpOperation, ex);
      }

      failureReason = RetryReason.getAbbreviation(ex, -1, "");

      if (!client.getRetryPolicy().shouldRetry(retryCount, -1)) {
        throw new InvalidAbfsRestOperationException(ex, retryCount);
      }

      return false;
    } finally {
      int status = httpOperation.getStatusCode();
      /*
        A status less than 300 (2xx range) or greater than or equal
        to 500 (5xx range) should contribute to throttling metrics being updated.
        Less than 200 or greater than or equal to 500 show failed operations. 2xx
        range contributes to successful operations. 3xx range is for redirects
        and 4xx range is for user errors. These should not be a part of
        throttling backoff computation.
       */
      boolean updateMetricsResponseCode = (status < HttpURLConnection.HTTP_MULT_CHOICE
              || status >= HttpURLConnection.HTTP_INTERNAL_ERROR);
      if (updateMetricsResponseCode) {
        intercept.updateMetrics(operationType, httpOperation);
      }
    }

    LOG.debug("HttpRequest: {}: {}", operationType, httpOperation);

    if (client.getRetryPolicy().shouldRetry(retryCount, httpOperation.getStatusCode())) {
      int status = httpOperation.getStatusCode();
      failureReason = RetryReason.getAbbreviation(null, status, httpOperation.getStorageErrorMessage());
      return false;
    }

    result = httpOperation;

    return true;
  }

  /**
   * Sign an operation.
   * @param httpOperation operation to sign
   * @param bytesToSign how many bytes to sign for shared key auth.
   * @throws IOException failure
   */
  @VisibleForTesting
  public void signRequest(final AbfsHttpOperation httpOperation, int bytesToSign) throws IOException {
    switch(client.getAuthType()) {
      case Custom:
      case OAuth:
        LOG.debug("Authenticating request with OAuth2 access token");
        httpOperation.getConnection().setRequestProperty(HttpHeaderConfigurations.AUTHORIZATION,
            client.getAccessToken());
        break;
      case SAS:
        // do nothing; the SAS token should already be appended to the query string
        httpOperation.setMaskForSAS(); //mask sig/oid from url for logs
        break;
      case SharedKey:
      default:
        // sign the HTTP request
        LOG.debug("Signing request with shared key");
        // sign the HTTP request
        client.getSharedKeyCredentials().signRequest(
            httpOperation.getConnection(),
            bytesToSign);
        break;
    }
  }

  /**
   * Creates new object of {@link AbfsHttpOperation} with the url, method, and
   * requestHeaders fields of the AbfsRestOperation object.
   */
  @VisibleForTesting
  AbfsHttpOperation createHttpOperation() throws IOException {
    return new AbfsHttpOperation(url, method, requestHeaders);
  }

  /**
   * Incrementing Abfs counters with a long value.
   *
   * @param statistic the Abfs statistic that needs to be incremented.
   * @param value     the value to be incremented by.
   */
  private void incrementCounter(AbfsStatistic statistic, long value) {
    if (abfsCounters != null) {
      abfsCounters.incrementCounter(statistic, value);
    }
  }
}
