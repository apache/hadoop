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

import org.apache.hadoop.fs.azurebfs.AbfsStatistic;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;

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
   *
   */
  public void execute() throws AzureBlobFileSystemException {

    try {
      IOStatisticsBinding.trackDurationOfInvocation(abfsCounters,
          AbfsStatistic.getStatNameFromHttpCall(method),
          () -> completeExecute());
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
   */
  private void completeExecute() throws AzureBlobFileSystemException {
    // see if we have latency reports from the previous requests
    String latencyHeader = this.client.getAbfsPerfTracker().getClientLatency();
    if (latencyHeader != null && !latencyHeader.isEmpty()) {
      AbfsHttpHeader httpHeader =
              new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_ABFS_CLIENT_LATENCY, latencyHeader);
      requestHeaders.add(httpHeader);
    }

    retryCount = 0;
    LOG.debug("First execution of REST operation - {}", operationType);
    while (!executeHttpOperation(retryCount)) {
      try {
        ++retryCount;
        LOG.debug("Retrying REST operation {}. RetryCount = {}",
            operationType, retryCount);
        Thread.sleep(client.getRetryPolicy().getRetryInterval(retryCount));
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }

    if (result.getStatusCode() >= HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new AbfsRestOperationException(result.getStatusCode(), result.getStorageErrorCode(),
          result.getStorageErrorMessage(), null, result);
    }

    LOG.trace("{} REST operation complete", operationType);
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
      httpOperation = new AbfsHttpOperation(url, method, requestHeaders);
      incrementCounter(AbfsStatistic.CONNECTIONS_MADE, 1);

      switch(client.getAuthType()) {
        case Custom:
        case OAuth:
          LOG.debug("Authenticating request with OAuth2 access token");
          httpOperation.getConnection().setRequestProperty(HttpHeaderConfigurations.AUTHORIZATION,
              client.getAccessToken());
          break;
        case SAS:
          // do nothing; the SAS token should already be appended to the query string
          break;
        case SharedKey:
          // sign the HTTP request
          LOG.debug("Signing request with shared key");
          // sign the HTTP request
          client.getSharedKeyCredentials().signRequest(
              httpOperation.getConnection(),
              hasRequestBody ? bufferLength : 0);
          break;
      }
    } catch (IOException e) {
      LOG.debug("Auth failure: {}, {}", method, url);
      throw new AbfsRestOperationException(-1, null,
          "Auth failure: " + e.getMessage(), e);
    }

    try {
      // dump the headers
      AbfsIoUtils.dumpHeadersToDebugLog("Request Headers",
          httpOperation.getConnection().getRequestProperties());
      AbfsClientThrottlingIntercept.sendingRequest(operationType, abfsCounters);

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
      LOG.warn("Unknown host name: %s. Retrying to resolve the host name...",
          hostname);
      if (!client.getRetryPolicy().shouldRetry(retryCount, -1)) {
        throw new InvalidAbfsRestOperationException(ex);
      }
      return false;
    } catch (IOException ex) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("HttpRequestFailure: {}, {}", httpOperation.toString(), ex);
      }

      if (!client.getRetryPolicy().shouldRetry(retryCount, -1)) {
        throw new InvalidAbfsRestOperationException(ex);
      }

      return false;
    } finally {
      AbfsClientThrottlingIntercept.updateMetrics(operationType, httpOperation);
    }

    LOG.debug("HttpRequest: {}: {}", operationType, httpOperation.toString());

    if (client.getRetryPolicy().shouldRetry(retryCount, httpOperation.getStatusCode())) {
      return false;
    }

    result = httpOperation;

    return true;
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
