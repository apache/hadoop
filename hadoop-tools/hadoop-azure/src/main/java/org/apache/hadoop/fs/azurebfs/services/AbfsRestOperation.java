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
import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.ClosedIOException;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbfsStatistic;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.enums.RetryValue;
import org.apache.http.impl.execchain.RequestAbortedException;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.PUT_BLOCK_LIST;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ZERO;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.MAX_RETRY_COUNT;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.NUMBER_OF_REQUESTS_FAILED;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.NUMBER_OF_REQUESTS_SUCCEEDED;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.NUMBER_OF_REQUESTS_SUCCEEDED_WITHOUT_RETRYING;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.TOTAL_NUMBER_OF_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.NUMBER_OF_IOPS_THROTTLED_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.NUMBER_OF_OTHER_THROTTLED_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.NUMBER_OF_BANDWIDTH_THROTTLED_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.NUMBER_OF_NETWORK_FAILED_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.MIN_BACK_OFF;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.MAX_BACK_OFF;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.TOTAL_BACK_OFF;
import static org.apache.hadoop.fs.azurebfs.enums.AbfsBackoffMetricsEnum.TOTAL_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.enums.RetryValue.getRetryValue;
import static org.apache.hadoop.util.Time.now;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_CONTINUE;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.EGRESS_LIMIT_BREACH_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.INGRESS_LIMIT_BREACH_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.TPS_LIMIT_BREACH_ABBREVIATION;

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
  private static final Logger LOG1 = LoggerFactory.getLogger(AbfsRestOperation.class);
  // For uploads, this is the request entity body.  For downloads,
  // this will hold the response entity body.
  private byte[] buffer;
  private int bufferOffset;
  private int bufferLength;
  private int retryCount = 0;
  private boolean isThrottledRequest = false;
  private long maxRetryCount = 0L;
  private final int maxIoRetries;
  private AbfsHttpOperation result;
  private final AbfsCounters abfsCounters;
  private AbfsBackoffMetrics abfsBackoffMetrics;
  /**
   * This variable contains the reason of last API call within the same
   * AbfsRestOperation object.
   */
  private String failureReason;
  private AbfsRetryPolicy retryPolicy;

  private final AbfsConfiguration abfsConfiguration;

  /**
   * This variable stores the tracing context used for last Rest Operation.
   */
  private TracingContext lastUsedTracingContext;

  /**
   * Number of retries due to IOException.
   */
  private int apacheHttpClientIoExceptions = 0;

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

  /**
   * For setting dummy result of getFileStatus for implicit paths.
   * @param httpStatus http status code to be set.
   */
  public void hardSetGetFileStatusResult(int httpStatus) {
    result = new AbfsHttpOperation.AbfsHttpOperationWithFixedResultForGetFileStatus(this.url,
        this.method, httpStatus);
  }

  /**
   * For setting dummy result of listPathStatus for file paths.
   * @param httpStatus http status code to be set.
   * @param listResultSchema list result schema to be set.
   */
  public void hardSetGetListStatusResult(int httpStatus, final ListResultSchema listResultSchema) {
    result = new AbfsHttpOperation.AbfsHttpOperationWithFixedResultForGetListStatus(this.url,
        this.method, httpStatus, listResultSchema);
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

  private static final int MIN_FIRST_RANGE = 1;
  private static final int MAX_FIRST_RANGE = 5;
  private static final int MAX_SECOND_RANGE = 15;
  private static final int MAX_THIRD_RANGE = 25;

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
                    final List<AbfsHttpHeader> requestHeaders,
                    final AbfsConfiguration abfsConfiguration) {
    this(operationType, client, method, url, requestHeaders, null, abfsConfiguration
    );
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
                    final String sasToken,
                    final AbfsConfiguration abfsConfiguration) {
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
    if (abfsCounters != null) {
      this.abfsBackoffMetrics = abfsCounters.getAbfsBackoffMetrics();
    }
    this.maxIoRetries = abfsConfiguration.getMaxIoRetries();
    this.intercept = client.getIntercept();
    this.abfsConfiguration = abfsConfiguration;
    this.retryPolicy = client.getExponentialRetryPolicy();
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
   * this will hold the response entity body.
   * @param bufferOffset An offset into the buffer where the data begins.
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
                    String sasToken,
                    final AbfsConfiguration abfsConfiguration) {
    this(operationType, client, method, url, requestHeaders, sasToken, abfsConfiguration
    );
    this.buffer = buffer;
    this.bufferOffset = bufferOffset;
    this.bufferLength = bufferLength;
  }

  /**
   * Execute a AbfsRestOperation. Track the Duration of a request if
   * abfsCounters isn't null.
   * @param tracingContext TracingContext instance to track correlation IDs
   * @throws AzureBlobFileSystemException if the operation fails.
   */
  public void execute(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    // Since this might be a sub-sequential or parallel rest operation
    // triggered by a single file system call, using a new tracing context.
    lastUsedTracingContext = createNewTracingContext(tracingContext);
    try {
      if (abfsCounters != null) {
        abfsCounters.getLastExecutionTime().set(now());
      }
      client.timerOrchestrator(TimerFunctionality.RESUME, null);
      IOStatisticsBinding.trackDurationOfInvocation(abfsCounters,
          AbfsStatistic.getStatNameFromHttpCall(method),
          () -> completeExecute(lastUsedTracingContext));
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
  void completeExecute(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    // see if we have latency reports from the previous requests
    String latencyHeader = getClientLatency();
    if (latencyHeader != null && !latencyHeader.isEmpty()) {
      AbfsHttpHeader httpHeader =
              new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_ABFS_CLIENT_LATENCY, latencyHeader);
      requestHeaders.add(httpHeader);
    }

    // By Default Exponential Retry Policy Will be used
    retryCount = 0;
    retryPolicy = client.getExponentialRetryPolicy();
    LOG.debug("First execution of REST operation - {}", operationType);
    long sleepDuration = 0L;
    if (abfsBackoffMetrics != null) {
      synchronized (this) {
        abfsBackoffMetrics.incrementMetricValue(TOTAL_NUMBER_OF_REQUESTS);
      }
    }
    while (!executeHttpOperation(retryCount, tracingContext)) {
      try {
        ++retryCount;
        tracingContext.setRetryCount(retryCount);
        long retryInterval = retryPolicy.getRetryInterval(retryCount);
        LOG.debug("Rest operation {} failed with failureReason: {}. Retrying with retryCount = {}, retryPolicy: {} and sleepInterval: {}",
            operationType, failureReason, retryCount, retryPolicy.getAbbreviation(), retryInterval);
        if (abfsBackoffMetrics != null) {
          updateBackoffTimeMetrics(retryCount, sleepDuration);
        }
        Thread.sleep(retryInterval);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
    if (abfsBackoffMetrics != null) {
      updateBackoffMetrics(retryCount, result.getStatusCode());
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
  void updateBackoffMetrics(int retryCount, int statusCode) {
    if (abfsBackoffMetrics != null) {
      if (statusCode < HttpURLConnection.HTTP_OK
              || statusCode >= HttpURLConnection.HTTP_INTERNAL_ERROR) {
        synchronized (this) {
          if (retryCount >= maxIoRetries) {
            abfsBackoffMetrics.incrementMetricValue(NUMBER_OF_REQUESTS_FAILED);
          }
        }
      } else {
        synchronized (this) {
          if (retryCount > ZERO && retryCount <= maxIoRetries) {
            maxRetryCount = Math.max(abfsBackoffMetrics.getMetricValue(MAX_RETRY_COUNT), retryCount);
            abfsBackoffMetrics.setMetricValue(MAX_RETRY_COUNT, maxRetryCount);
            updateCount(retryCount);
          } else {
            abfsBackoffMetrics.incrementMetricValue(NUMBER_OF_REQUESTS_SUCCEEDED_WITHOUT_RETRYING);
          }
        }
      }
    }
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
    final AbfsHttpOperation httpOperation;
    // Used to avoid CST Metric Update in Case of UnknownHost/IO Exception.
    boolean wasKnownExceptionThrown = false;

    try {
      // initialize the HTTP request and open the connection
      httpOperation = createHttpOperation();
      incrementCounter(AbfsStatistic.CONNECTIONS_MADE, 1);
      tracingContext.constructHeader(httpOperation, failureReason, retryPolicy.getAbbreviation());

      signRequest(httpOperation, hasRequestBody ? bufferLength : 0);

    } catch (IOException e) {
      LOG.debug("Auth failure: {}, {}", method, url);
      throw new AbfsRestOperationException(-1, null,
          "Auth failure: " + e.getMessage(), e);
    }

    try {
      // dump the headers
      AbfsIoUtils.dumpHeadersToDebugLog("Request Headers",
          httpOperation.getRequestProperties());
      intercept.sendingRequest(operationType, abfsCounters);
      if (hasRequestBody) {
        httpOperation.sendPayload(buffer, bufferOffset, bufferLength);
        incrementCounter(AbfsStatistic.SEND_REQUESTS, 1);
        if (!(operationType.name().equals(PUT_BLOCK_LIST))) {
          incrementCounter(AbfsStatistic.BYTES_SENT, bufferLength);
        }
      }
      httpOperation.processResponse(buffer, bufferOffset, bufferLength);
      if (!isThrottledRequest && httpOperation.getStatusCode()
          >= HttpURLConnection.HTTP_INTERNAL_ERROR) {
        isThrottledRequest = true;
        AzureServiceErrorCode serviceErrorCode =
            AzureServiceErrorCode.getAzureServiceCode(
                httpOperation.getStatusCode(),
                httpOperation.getStorageErrorCode(),
                httpOperation.getStorageErrorMessage());
        LOG1.trace("Service code is " + serviceErrorCode + " status code is "
            + httpOperation.getStatusCode() + " error code is "
            + httpOperation.getStorageErrorCode()
            + " error message is " + httpOperation.getStorageErrorMessage());
        if (abfsBackoffMetrics != null) {
          synchronized (this) {
            if (serviceErrorCode.equals(
                    AzureServiceErrorCode.INGRESS_OVER_ACCOUNT_LIMIT)
                    || serviceErrorCode.equals(
                    AzureServiceErrorCode.EGRESS_OVER_ACCOUNT_LIMIT)) {
              abfsBackoffMetrics.incrementMetricValue(NUMBER_OF_BANDWIDTH_THROTTLED_REQUESTS);
            } else if (serviceErrorCode.equals(
                    AzureServiceErrorCode.TPS_OVER_ACCOUNT_LIMIT)) {
              abfsBackoffMetrics.incrementMetricValue(NUMBER_OF_IOPS_THROTTLED_REQUESTS);
            } else {
              abfsBackoffMetrics.incrementMetricValue(NUMBER_OF_OTHER_THROTTLED_REQUESTS);
            }
          }
        }
      }
        incrementCounter(AbfsStatistic.GET_RESPONSES, 1);
      //Only increment bytesReceived counter when the status code is 2XX.
      if (httpOperation.getStatusCode() >= HttpURLConnection.HTTP_OK
          && httpOperation.getStatusCode() <= HttpURLConnection.HTTP_PARTIAL) {
        incrementCounter(AbfsStatistic.BYTES_RECEIVED,
            httpOperation.getBytesReceived());
      } else if (httpOperation.getStatusCode() == HttpURLConnection.HTTP_UNAVAILABLE) {
        incrementCounter(AbfsStatistic.SERVER_UNAVAILABLE, 1);
      }

      // If no exception occurred till here it means http operation was successfully complete and
      // a response from server has been received which might be failure or success.
      // If any kind of exception has occurred it will be caught below.
      // If request failed to determine failure reason and retry policy here.
      // else simply return with success after saving the result.
      LOG.debug("HttpRequest: {}: {}", operationType, httpOperation);

      int status = httpOperation.getStatusCode();
      failureReason = RetryReason.getAbbreviation(null, status, httpOperation.getStorageErrorMessage());
      retryPolicy = client.getRetryPolicy(failureReason);

      if (retryPolicy.shouldRetry(retryCount, httpOperation.getStatusCode())) {
        return false;
      }

      // If the request has succeeded or failed with non-retrial error, save the operation and return.
      result = httpOperation;

    } catch (UnknownHostException ex) {
      wasKnownExceptionThrown = true;
      String hostname = null;
      hostname = httpOperation.getHost();
      failureReason = RetryReason.getAbbreviation(ex, null, null);
      retryPolicy = client.getRetryPolicy(failureReason);
      LOG.warn("Unknown host name: {}. Retrying to resolve the host name...",
          hostname);
      if (httpOperation instanceof AbfsAHCHttpOperation) {
        registerApacheHttpClientIoException();
      }
      if (abfsBackoffMetrics != null) {
        synchronized (this) {
          abfsBackoffMetrics.incrementMetricValue(NUMBER_OF_NETWORK_FAILED_REQUESTS);
        }
      }
      if (!retryPolicy.shouldRetry(retryCount, -1)) {
        updateBackoffMetrics(retryCount, httpOperation.getStatusCode());
        throw new InvalidAbfsRestOperationException(ex, retryCount);
      }
      return false;
    } catch (IOException ex) {
      wasKnownExceptionThrown = true;
      if (LOG.isDebugEnabled()) {
        LOG.debug("HttpRequestFailure: {}, {}", httpOperation, ex);
      }
      if (abfsBackoffMetrics != null) {
        synchronized (this) {
          abfsBackoffMetrics.incrementMetricValue(NUMBER_OF_NETWORK_FAILED_REQUESTS);
        }
      }
      failureReason = RetryReason.getAbbreviation(ex, -1, "");
      retryPolicy = client.getRetryPolicy(failureReason);
      if (httpOperation instanceof AbfsAHCHttpOperation) {
        registerApacheHttpClientIoException();
        if (ex instanceof RequestAbortedException
            && ex.getCause() instanceof ClosedIOException) {
          throw new AbfsDriverException((IOException) ex.getCause());
        }
      }
      if (!retryPolicy.shouldRetry(retryCount, -1)) {
        updateBackoffMetrics(retryCount, httpOperation.getStatusCode());
        throw new InvalidAbfsRestOperationException(ex, retryCount);
      }
      return false;
    } finally {
      int statusCode = httpOperation.getStatusCode();
      // Update Metrics only if Succeeded or Throttled due to account limits.
      // Also Update in case of any unhandled exception is thrown.
      if (shouldUpdateCSTMetrics(statusCode) && !wasKnownExceptionThrown) {
        intercept.updateMetrics(operationType, httpOperation);
      }
    }

    return true;
  }

  /**
   * Registers switch off of ApacheHttpClient in case of IOException retries increases
   * more than the threshold.
   */
  private void registerApacheHttpClientIoException() {
    apacheHttpClientIoExceptions++;
    if (apacheHttpClientIoExceptions
        >= abfsConfiguration.getMaxApacheHttpClientIoExceptionsRetries()) {
      AbfsApacheHttpClient.registerFallback();
    }
  }

  /**
   * Sign an operation.
   * @param httpOperation operation to sign
   * @param bytesToSign how many bytes to sign for shared key auth.
   * @throws IOException failure
   */
  @VisibleForTesting
  public void signRequest(final AbfsHttpOperation httpOperation, int bytesToSign) throws IOException {
    if (client.isSendMetricCall()) {
      client.getMetricSharedkeyCredentials().signRequest(httpOperation, bytesToSign);
    } else {
      switch (client.getAuthType()) {
      case Custom:
      case OAuth:
        LOG.debug("Authenticating request with OAuth2 access token");
        httpOperation.setRequestProperty(HttpHeaderConfigurations.AUTHORIZATION,
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
            httpOperation,
            bytesToSign);
        break;
      }
    }
  }

  /**
   * Creates new object of {@link AbfsHttpOperation} with the url, method, and
   * requestHeaders fields of the AbfsRestOperation object.
   */
  @VisibleForTesting
  AbfsHttpOperation createHttpOperation() throws IOException {
    HttpOperationType httpOperationType
        = abfsConfiguration.getPreferredHttpOperationType();
    if (httpOperationType == HttpOperationType.APACHE_HTTP_CLIENT
        && isApacheClientUsable()) {
      return createAbfsAHCHttpOperation();
    }
    return createAbfsHttpOperation();
  }

  private boolean isApacheClientUsable() {
    return AbfsApacheHttpClient.usable();
  }

  @VisibleForTesting
  AbfsJdkHttpOperation createAbfsHttpOperation() throws IOException {
    return new AbfsJdkHttpOperation(url, method, requestHeaders,
        Duration.ofMillis(client.getAbfsConfiguration().getHttpConnectionTimeout()),
        Duration.ofMillis(client.getAbfsConfiguration().getHttpReadTimeout()), client);
  }

  @VisibleForTesting
  AbfsAHCHttpOperation createAbfsAHCHttpOperation() throws IOException {
    return new AbfsAHCHttpOperation(url, method, requestHeaders,
        Duration.ofMillis(client.getAbfsConfiguration().getHttpConnectionTimeout()),
        Duration.ofMillis(client.getAbfsConfiguration().getHttpReadTimeout()),
        client.getAbfsApacheHttpClient(), client);
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

  /**
   * Updates the count metrics based on the provided retry count.
   * @param retryCount The retry count used to determine the metrics category.
   *
   * This method increments the number of succeeded requests for the specified retry count.
   */
  private void updateCount(int retryCount){
      abfsBackoffMetrics.incrementMetricValue(NUMBER_OF_REQUESTS_SUCCEEDED, getRetryValue(retryCount));
  }

  /**
   * Updates backoff time metrics based on the provided retry count and sleep duration.
   * @param retryCount    The retry count used to determine the metrics category.
   * @param sleepDuration The duration of sleep during backoff.
   *
   * This method calculates and updates various backoff time metrics, including minimum, maximum,
   * and total backoff time, as well as the total number of requests for the specified retry count.
   */
  private void updateBackoffTimeMetrics(int retryCount, long sleepDuration) {
    synchronized (this) {
      RetryValue retryCounter = getRetryValue(retryCount);
      long minBackoffTime = Math.min(abfsBackoffMetrics.getMetricValue(MIN_BACK_OFF, retryCounter), sleepDuration);
      long maxBackoffForTime = Math.max(abfsBackoffMetrics.getMetricValue(MAX_BACK_OFF, retryCounter), sleepDuration);
      long totalBackoffTime = abfsBackoffMetrics.getMetricValue(TOTAL_BACK_OFF, retryCounter) + sleepDuration;
      abfsBackoffMetrics.incrementMetricValue(TOTAL_REQUESTS, retryCounter);
      abfsBackoffMetrics.setMetricValue(MIN_BACK_OFF, minBackoffTime, retryCounter);
      abfsBackoffMetrics.setMetricValue(MAX_BACK_OFF, maxBackoffForTime, retryCounter);
      abfsBackoffMetrics.setMetricValue(TOTAL_BACK_OFF, totalBackoffTime, retryCounter);
    }
  }

  /**
   * Updating Client Side Throttling Metrics for relevant response status codes.
   * Following criteria is used to decide based on status code and failure reason.
   * <ol>
   *   <li>Case 1: Status code in 2xx range: Successful Operations should contribute</li>
   *   <li>Case 2: Status code in 3xx range: Redirection Operations should not contribute</li>
   *   <li>Case 3: Status code in 4xx range: User Errors should not contribute</li>
   *   <li>
   *     Case 4: Status code is 503: Throttling Error should contribute as following:
   *     <ol>
   *       <li>Case 4.a: Ingress Over Account Limit: Should Contribute</li>
   *       <li>Case 4.b: Egress Over Account Limit: Should Contribute</li>
   *       <li>Case 4.c: TPS Over Account Limit: Should Contribute</li>
   *       <li>Case 4.d: Other Server Throttling: Should not contribute</li>
   *     </ol>
   *   </li>
   *   <li>Case 5: Status code in 5xx range other than 503: Should not contribute</li>
   * </ol>
   * @param statusCode
   * @return
   */
  private boolean shouldUpdateCSTMetrics(final int statusCode) {
    return statusCode <  HttpURLConnection.HTTP_MULT_CHOICE // Case 1
        || INGRESS_LIMIT_BREACH_ABBREVIATION.equals(failureReason) // Case 4.a
        || EGRESS_LIMIT_BREACH_ABBREVIATION.equals(failureReason) // Case 4.b
        || TPS_LIMIT_BREACH_ABBREVIATION.equals(failureReason); // Case 4.c
  }

  /**
   * Creates a new Tracing context before entering the retry loop of a rest operation.
   * This will ensure all rest operations have unique
   * tracing context that will be used for all the retries.
   * @param tracingContext original tracingContext.
   * @return tracingContext new tracingContext object created from original one.
   */
  @VisibleForTesting
  public TracingContext createNewTracingContext(final TracingContext tracingContext) {
    return new TracingContext(tracingContext);
  }

  /**
   * Returns the tracing contest used for last rest operation made.
   * @return tracingContext lasUserTracingContext.
   */
  @VisibleForTesting
  public final TracingContext getLastTracingContext() {
    return lastUsedTracingContext;
  }

  int getRetryCount() {
    return retryCount;
  }
}
