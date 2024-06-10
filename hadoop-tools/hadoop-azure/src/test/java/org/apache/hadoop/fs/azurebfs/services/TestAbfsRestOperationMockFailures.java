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

import java.io.InterruptedIOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Stubber;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.EGRESS_OVER_ACCOUNT_LIMIT;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.INGRESS_OVER_ACCOUNT_LIMIT;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.OTHER_SERVER_THROTTLING;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.TPS_OVER_ACCOUNT_LIMIT;
import static org.apache.hadoop.fs.azurebfs.services.RetryPolicyConstants.EXPONENTIAL_RETRY_POLICY_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryPolicyConstants.STATIC_RETRY_POLICY_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.AbfsClientTestUtil.addGeneralMockBehaviourToAbfsClient;
import static org.apache.hadoop.fs.azurebfs.services.AbfsClientTestUtil.addGeneralMockBehaviourToRestOpAndHttpOp;

import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_RESET_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_RESET_MESSAGE;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_JDK_MESSAGE;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.EGRESS_LIMIT_BREACH_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.INGRESS_LIMIT_BREACH_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.IO_EXCEPTION_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.TPS_LIMIT_BREACH_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.OTHER_SERVER_THROTTLING_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.READ_TIMEOUT_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.READ_TIMEOUT_JDK_MESSAGE;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.SOCKET_EXCEPTION_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.UNKNOWN_HOST_EXCEPTION_ABBREVIATION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class TestAbfsRestOperationMockFailures {
  // In these tests a request first fails with given exceptions and then succeed on retry.
  // Client Side Throttling Metrics will be updated at least for retried request which succeeded.
  // For original requests it will be updated only for EGR, IGR, OPR throttling.

  @Test
  public void testClientRequestIdForConnectTimeoutRetry() throws Exception {
    Exception[] exceptions = new Exception[1];
    String[] abbreviations = new String[1];
    exceptions[0] = new SocketTimeoutException(CONNECTION_TIMEOUT_JDK_MESSAGE);
    abbreviations[0] = CONNECTION_TIMEOUT_ABBREVIATION;
    testClientRequestIdForTimeoutRetry(exceptions, abbreviations, 1, 1);
  }

  @Test
  public void testClientRequestIdForConnectAndReadTimeoutRetry()
      throws Exception {
    Exception[] exceptions = new Exception[2];
    String[] abbreviations = new String[2];
    exceptions[0] = new SocketTimeoutException(CONNECTION_TIMEOUT_JDK_MESSAGE);
    abbreviations[0] = CONNECTION_TIMEOUT_ABBREVIATION;
    exceptions[1] = new SocketTimeoutException(READ_TIMEOUT_JDK_MESSAGE);
    abbreviations[1] = READ_TIMEOUT_ABBREVIATION;
    testClientRequestIdForTimeoutRetry(exceptions, abbreviations, 1, 1);
  }

  @Test
  public void testClientRequestIdForReadTimeoutRetry() throws Exception {
    Exception[] exceptions = new Exception[1];
    String[] abbreviations = new String[1];
    exceptions[0] = new SocketTimeoutException(READ_TIMEOUT_JDK_MESSAGE);
    abbreviations[0] = READ_TIMEOUT_ABBREVIATION;
    testClientRequestIdForTimeoutRetry(exceptions, abbreviations, 1, 0);
  }

  @Test
  public void testClientRequestIdForUnknownHostRetry() throws Exception {
    Exception[] exceptions = new Exception[1];
    String[] abbreviations = new String[1];
    exceptions[0] = new UnknownHostException();
    abbreviations[0] = UNKNOWN_HOST_EXCEPTION_ABBREVIATION;
    testClientRequestIdForTimeoutRetry(exceptions, abbreviations, 1, 0);
  }

  @Test
  public void testClientRequestIdForConnectionResetRetry() throws Exception {
    Exception[] exceptions = new Exception[1];
    String[] abbreviations = new String[1];
    exceptions[0] = new SocketTimeoutException(CONNECTION_RESET_MESSAGE + " by peer");
    abbreviations[0] = CONNECTION_RESET_ABBREVIATION;
    testClientRequestIdForTimeoutRetry(exceptions, abbreviations, 1, 0);
  }

  @Test
  public void testClientRequestIdForUnknownSocketExRetry() throws Exception {
    Exception[] exceptions = new Exception[1];
    String[] abbreviations = new String[1];
    exceptions[0] = new SocketException("unknown");
    abbreviations[0] = SOCKET_EXCEPTION_ABBREVIATION;
    testClientRequestIdForTimeoutRetry(exceptions, abbreviations, 1, 0);
  }

  @Test
  public void testClientRequestIdForIOERetry() throws Exception {
    Exception[] exceptions = new Exception[1];
    String[] abbreviations = new String[1];
    exceptions[0] = new InterruptedIOException();
    abbreviations[0] = IO_EXCEPTION_ABBREVIATION;
    testClientRequestIdForTimeoutRetry(exceptions, abbreviations, 1, 0);
  }

  @Test
  public void testClientRequestIdFor400Retry() throws Exception {
    testClientRequestIdForStatusRetry(HTTP_BAD_REQUEST, "", "400", 1);
  }

  @Test
  public void testClientRequestIdFor500Retry() throws Exception {
    testClientRequestIdForStatusRetry(HTTP_INTERNAL_ERROR, "", "500", 1);
  }

  @Test
  public void testClientRequestIdFor503INGRetry() throws Exception {
    testClientRequestIdForStatusRetry(
            HTTP_UNAVAILABLE,
            INGRESS_OVER_ACCOUNT_LIMIT.getErrorMessage(),
            INGRESS_LIMIT_BREACH_ABBREVIATION,
            2);
  }

  @Test
  public void testClientRequestIdFor503EGRRetry() throws Exception {
    testClientRequestIdForStatusRetry(
            HTTP_UNAVAILABLE,
            EGRESS_OVER_ACCOUNT_LIMIT.getErrorMessage(),
            EGRESS_LIMIT_BREACH_ABBREVIATION,
            2);
  }

  @Test
  public void testClientRequestIdFor503OPRRetry() throws Exception {
    testClientRequestIdForStatusRetry(
            HTTP_UNAVAILABLE,
            TPS_OVER_ACCOUNT_LIMIT.getErrorMessage(),
        TPS_LIMIT_BREACH_ABBREVIATION,
            2);
  }

  @Test
  public void testClientRequestIdFor503OtherRetry() throws Exception {
    testClientRequestIdForStatusRetry(
            HTTP_UNAVAILABLE,
            OTHER_SERVER_THROTTLING.getErrorMessage(),
            OTHER_SERVER_THROTTLING_ABBREVIATION,
            1);
  }

  /**
   * Test for mocking the failure scenario with retry policy assertions.
   * Here we will try to create a request with following life cycle:
   * 1. Primary Request made fails with Connection Timeout and fall into retry loop
   * 2. Retried request fails with 503 and again go for retry
   * 3. Retried request fails with 503 and do not go for retry.
   *
   * We will try to assert that:
   * 1. Correct retry policy is used to get the retry interval for each failed request
   * 2. Tracing header construction takes place with proper arguments based on the failure reason and retry policy used
   * @throws Exception
   */
  @Test
  public void testRetryPolicyWithDifferentFailureReasons() throws Exception {

    AbfsClient abfsClient = Mockito.mock(AbfsClient.class);
    ExponentialRetryPolicy exponentialRetryPolicy = Mockito.mock(
        ExponentialRetryPolicy.class);
    StaticRetryPolicy staticRetryPolicy = Mockito.mock(StaticRetryPolicy.class);
    AbfsThrottlingIntercept intercept = Mockito.mock(
        AbfsThrottlingIntercept.class);
    addGeneralMockBehaviourToAbfsClient(abfsClient, exponentialRetryPolicy, staticRetryPolicy, intercept);

    AbfsRestOperation abfsRestOperation = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.ReadFile,
        abfsClient,
        "PUT",
        null,
        new ArrayList<>(),
        Mockito.mock(AbfsConfiguration.class)
    ));

    AbfsHttpOperation httpOperation = Mockito.mock(AbfsHttpOperation.class);
    addGeneralMockBehaviourToRestOpAndHttpOp(abfsRestOperation, httpOperation);

    Stubber stubber = Mockito.doThrow(new SocketTimeoutException(CONNECTION_TIMEOUT_JDK_MESSAGE));
    stubber.doNothing().when(httpOperation).processResponse(
        nullable(byte[].class), nullable(int.class), nullable(int.class));

    when(httpOperation.getStatusCode()).thenReturn(-1).thenReturn(HTTP_UNAVAILABLE);

    TracingContext tracingContext = Mockito.mock(TracingContext.class);
    Mockito.doNothing().when(tracingContext).setRetryCount(nullable(int.class));
    Mockito.doReturn("").when(httpOperation).getStorageErrorMessage();
    Mockito.doReturn("").when(httpOperation).getStorageErrorCode();
    Mockito.doReturn("HEAD").when(httpOperation).getMethod();
    Mockito.doReturn("").when(httpOperation).getMaskedUrl();
    Mockito.doReturn("").when(httpOperation).getRequestId();
    Mockito.doReturn(EGRESS_OVER_ACCOUNT_LIMIT.getErrorMessage()).when(httpOperation).getStorageErrorMessage();
    Mockito.doReturn(tracingContext).when(abfsRestOperation).createNewTracingContext(any());

    try {
      // Operation will fail with CT first and then 503 thereafter.
      abfsRestOperation.execute(tracingContext);
    } catch(AbfsRestOperationException ex) {
      Assertions.assertThat(ex.getStatusCode())
          .describedAs("Status Code must be HTTP_UNAVAILABLE(503)")
          .isEqualTo(HTTP_UNAVAILABLE);
    }

    // Assert that httpOperation.processResponse was called 3 times.
    // One for retry count 0
    // One for retry count 1 after failing with CT
    // One for retry count 2 after failing with 503
    Mockito.verify(httpOperation, times(3)).processResponse(
        nullable(byte[].class), nullable(int.class), nullable(int.class));

    // Primary Request Failed with CT. Static Retry Policy should be used.
    Mockito.verify(abfsClient, Mockito.times(1))
        .getRetryPolicy(CONNECTION_TIMEOUT_ABBREVIATION);
    Mockito.verify(staticRetryPolicy, Mockito.times(1))
        .shouldRetry(0, -1);
    Mockito.verify(staticRetryPolicy, Mockito.times(1))
        .getRetryInterval(1);
    Mockito.verify(tracingContext, Mockito.times(1))
        .constructHeader(httpOperation, CONNECTION_TIMEOUT_ABBREVIATION, STATIC_RETRY_POLICY_ABBREVIATION);

    // Assert that exponential Retry Policy was used during second and third Iteration.
    // Iteration 2 and 3 failed with 503 and should retry was called with retry count 1 and 2
    // Before iteration 3 sleep will be computed using exponential retry policy and retry count 2
    // Should retry with retry count 2 will return false and no further requests will be made.
    Mockito.verify(abfsClient, Mockito.times(2))
        .getRetryPolicy(EGRESS_LIMIT_BREACH_ABBREVIATION);
    Mockito.verify(exponentialRetryPolicy, Mockito.times(1))
        .shouldRetry(1, HTTP_UNAVAILABLE);
    Mockito.verify(exponentialRetryPolicy, Mockito.times(1))
        .shouldRetry(2, HTTP_UNAVAILABLE);
    Mockito.verify(exponentialRetryPolicy, Mockito.times(1))
        .getRetryInterval(2);
    Mockito.verify(tracingContext, Mockito.times(1))
        .constructHeader(httpOperation, EGRESS_LIMIT_BREACH_ABBREVIATION, EXPONENTIAL_RETRY_POLICY_ABBREVIATION);

    // Assert that intercept.updateMetrics was called 2 times. Both the retried request fails with EGR.
    Mockito.verify(intercept, Mockito.times(2))
        .updateMetrics(nullable(AbfsRestOperationType.class), nullable(
            AbfsHttpOperation.class));
  }

  private void testClientRequestIdForStatusRetry(int status,
                                                 String serverErrorMessage,
                                                 String keyExpected,
                                                 int numOfTimesCSTMetricsUpdated) throws Exception {

    AbfsClient abfsClient = Mockito.mock(AbfsClient.class);
    ExponentialRetryPolicy exponentialRetryPolicy = Mockito.mock(
        ExponentialRetryPolicy.class);
    StaticRetryPolicy staticRetryPolicy = Mockito.mock(StaticRetryPolicy.class);
    AbfsThrottlingIntercept intercept = Mockito.mock(
        AbfsThrottlingIntercept.class);
    addGeneralMockBehaviourToAbfsClient(abfsClient, exponentialRetryPolicy, staticRetryPolicy, intercept);

    // Create a readfile operation that will fail
    AbfsRestOperation abfsRestOperation = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.ReadFile,
        abfsClient,
        "PUT",
        null,
        new ArrayList<>(),
        Mockito.mock(AbfsConfiguration.class)
    ));

    AbfsHttpOperation httpOperation = Mockito.mock(AbfsHttpOperation.class);
    addGeneralMockBehaviourToRestOpAndHttpOp(abfsRestOperation, httpOperation);

    Mockito.doNothing()
        .doNothing()
        .when(httpOperation)
        .processResponse(nullable(byte[].class), nullable(int.class),
            nullable(int.class));

    int[] statusCount = new int[1];
    statusCount[0] = 0;
    Mockito.doAnswer(answer -> {
      if (statusCount[0] <= 10) {
        statusCount[0]++;
        return status;
      }
      return HTTP_OK;
    }).when(httpOperation).getStatusCode();

    Mockito.doReturn(serverErrorMessage)
        .when(httpOperation)
        .getStorageErrorMessage();

    TracingContext tracingContext = Mockito.mock(TracingContext.class);
    Mockito.doNothing().when(tracingContext).setRetryCount(nullable(int.class));
    Mockito.doReturn(tracingContext).when(abfsRestOperation).createNewTracingContext(any());

    int[] count = new int[1];
    count[0] = 0;
    Mockito.doAnswer(invocationOnMock -> {
      if (count[0] == 1) {
        Assertions.assertThat((String) invocationOnMock.getArgument(1))
            .isEqualTo(keyExpected);
      }
      count[0]++;
      return null;
    }).when(tracingContext).constructHeader(any(), any(), any());

    abfsRestOperation.execute(tracingContext);
    Assertions.assertThat(count[0]).isEqualTo(2);

    Mockito.verify(intercept, Mockito.times(numOfTimesCSTMetricsUpdated)).updateMetrics(any(), any());

  }

  private void testClientRequestIdForTimeoutRetry(Exception[] exceptions,
                                                  String[] abbreviationsExpected,
                                                  int len,
                                                  int numOfCTExceptions) throws Exception {
    AbfsClient abfsClient = Mockito.mock(AbfsClient.class);
    ExponentialRetryPolicy exponentialRetryPolicy = Mockito.mock(
        ExponentialRetryPolicy.class);
    StaticRetryPolicy staticRetryPolicy = Mockito.mock(StaticRetryPolicy.class);
    AbfsThrottlingIntercept intercept = Mockito.mock(
        AbfsThrottlingIntercept.class);
    addGeneralMockBehaviourToAbfsClient(abfsClient, exponentialRetryPolicy, staticRetryPolicy, intercept);

    AbfsRestOperation abfsRestOperation = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.ReadFile,
        abfsClient,
        "PUT",
        null,
        new ArrayList<>(),
        Mockito.mock(AbfsConfiguration.class)
    ));

    AbfsHttpOperation httpOperation = Mockito.mock(AbfsHttpOperation.class);
    addGeneralMockBehaviourToRestOpAndHttpOp(abfsRestOperation, httpOperation);

    Stubber stubber = Mockito.doThrow(exceptions[0]);
    for (int iteration = 1; iteration < len; iteration++) {
      stubber.doThrow(exceptions[iteration]);
    }
    stubber
        .doNothing()
        .when(httpOperation)
        .processResponse(nullable(byte[].class), nullable(int.class),
            nullable(int.class));

    Mockito.doReturn(HTTP_OK).when(httpOperation).getStatusCode();

    TracingContext tracingContext = Mockito.mock(TracingContext.class);
    Mockito.doNothing().when(tracingContext).setRetryCount(nullable(int.class));
    Mockito.doReturn(tracingContext).when(abfsRestOperation).createNewTracingContext(any());

    int[] count = new int[1];
    count[0] = 0;
    Mockito.doAnswer(invocationOnMock -> {
      if (count[0] > 0 && count[0] <= len) {
        Assertions.assertThat((String) invocationOnMock.getArgument(1))
            .isEqualTo(abbreviationsExpected[count[0] - 1]);
      }
      count[0]++;
      return null;
    }).when(tracingContext).constructHeader(any(), any(), any());

    abfsRestOperation.execute(tracingContext);
    Assertions.assertThat(count[0]).isEqualTo(len + 1);

    /**
     * Assert that getRetryPolicy was called with
     * failureReason CT only for Connection Timeout Cases.
     * For every failed request getRetryPolicy will be called three times
     * It will be called with failureReason CT for every request failing with CT
     */
    Mockito.verify(abfsClient, Mockito.times(
        numOfCTExceptions))
        .getRetryPolicy(CONNECTION_TIMEOUT_ABBREVIATION);
  }
}
