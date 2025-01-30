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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbfsCountersImpl;
import org.assertj.core.api.Assertions;
import org.mockito.AdditionalMatchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.util.functional.FunctionRaisingIOE;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPLICATION_XML;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCKLIST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_GET;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_LENGTH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.IF_MATCH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_BLOB_CONTENT_MD5;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_CLOSE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_COMP;
import static org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.PutBlockList;
import static org.apache.hadoop.fs.azurebfs.services.AuthType.OAuth;
import static org.apache.hadoop.fs.azurebfs.services.RetryPolicyConstants.EXPONENTIAL_RETRY_POLICY_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryPolicyConstants.STATIC_RETRY_POLICY_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_ABBREVIATION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;

/**
 * Utility class to help defining mock behavior on AbfsClient and AbfsRestOperation
 * objects which are protected inside services package.
 */
public final class AbfsClientTestUtil {
  private static final long ONE_SEC = 1000;

  private AbfsClientTestUtil() {

  }

  public static void setMockAbfsRestOperationForListPathOperation(
      final AbfsClient spiedClient,
      FunctionRaisingIOE<AbfsJdkHttpOperation, AbfsJdkHttpOperation> functionRaisingIOE)
      throws Exception {
    ExponentialRetryPolicy exponentialRetryPolicy = Mockito.mock(ExponentialRetryPolicy.class);
    StaticRetryPolicy staticRetryPolicy = Mockito.mock(StaticRetryPolicy.class);
    AbfsThrottlingIntercept intercept = Mockito.mock(AbfsThrottlingIntercept.class);
    AbfsJdkHttpOperation httpOperation = Mockito.mock(AbfsJdkHttpOperation.class);
    AbfsRestOperation abfsRestOperation = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.ListPaths,
        spiedClient,
        HTTP_METHOD_GET,
        null,
        new ArrayList<>(),
        spiedClient.getAbfsConfiguration()
    ));

    Mockito.doReturn(abfsRestOperation).when(spiedClient).getAbfsRestOperation(
        eq(AbfsRestOperationType.ListPaths), any(), any(), any());

    addGeneralMockBehaviourToAbfsClient(spiedClient, exponentialRetryPolicy, staticRetryPolicy, intercept);
    addGeneralMockBehaviourToRestOpAndHttpOp(abfsRestOperation, httpOperation);

    functionRaisingIOE.apply(httpOperation);
  }

  /**
   * Sets up a mocked AbfsRestOperation for a flush operation in the Azure Blob File System (ABFS).
   * This method is primarily used in testing scenarios where specific behavior needs to be simulated
   * during a flush operation, such as returning a particular status code or triggering an exception.
   *
   * The method creates a mock AbfsRestOperation configured with the appropriate request headers
   * and parameters for a "PutBlockList" operation. It then uses the provided
   * {@code functionRaisingIOE} to modify the behavior of the mock AbfsRestOperation during execution.
   *
   * @param spiedClient          The spied instance of the AbfsClient used for making HTTP requests.
   * @param eTag                 The ETag of the blob, used in the If-Match header to ensure
   *                             conditional requests.
   * @param blockListXml         The XML string representing the block list to be uploaded.
   * @param functionRaisingIOE   A function that modifies the behavior of the AbfsRestOperation,
   *                             allowing the simulation of specific outcomes, such as HTTP errors
   *                             or connection resets.
   * @throws Exception           If an error occurs while setting up the mock operation.
   */
  public static void setMockAbfsRestOperationForFlushOperation(
      final AbfsClient spiedClient, String eTag, String blockListXml, FunctionRaisingIOE<AbfsHttpOperation, AbfsHttpOperation> functionRaisingIOE)
      throws Exception {
    List<AbfsHttpHeader> requestHeaders = ITestAbfsClient.getTestRequestHeaders(spiedClient);
    byte[] buffer = blockListXml.getBytes(StandardCharsets.UTF_8);
    requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(buffer.length)));
    requestHeaders.add(new AbfsHttpHeader(CONTENT_TYPE, APPLICATION_XML));
    requestHeaders.add(new AbfsHttpHeader(IF_MATCH, eTag));
    requestHeaders.add(new AbfsHttpHeader(X_MS_BLOB_CONTENT_MD5, spiedClient.computeMD5Hash(buffer, 0, buffer.length)));
    final AbfsUriQueryBuilder abfsUriQueryBuilder = spiedClient.createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, BLOCKLIST);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CLOSE, String.valueOf(false));
    final URL url = spiedClient.createRequestUrl("/test/file", abfsUriQueryBuilder.toString());
    AbfsRestOperation abfsRestOperation = Mockito.spy(new AbfsRestOperation(
        PutBlockList, spiedClient, HTTP_METHOD_PUT,
        url,
        requestHeaders, buffer, 0, buffer.length, null,
        spiedClient.getAbfsConfiguration()));

    Mockito.doReturn(abfsRestOperation)
        .when(spiedClient)
        .getAbfsRestOperation(eq(AbfsRestOperationType.PutBlockList),
            Mockito.anyString(), Mockito.any(URL.class), Mockito.anyList(),
            Mockito.nullable(byte[].class), Mockito.anyInt(), Mockito.anyInt(),
            Mockito.nullable(String.class));

    addMockBehaviourToRestOpAndHttpOp(abfsRestOperation, functionRaisingIOE);
  }

  /**
   * Adding general mock behaviour to AbfsRestOperation and AbfsHttpOperation
   * to avoid any NPE occurring. These will avoid any network call made and
   * will return the relevant exception or return value directly.
   * @param abfsRestOperation to be mocked
   * @param httpOperation to be mocked
   * @throws IOException
   */
  public static void addGeneralMockBehaviourToRestOpAndHttpOp(final AbfsRestOperation abfsRestOperation,
                                                              final AbfsHttpOperation httpOperation) throws IOException {
    HttpURLConnection httpURLConnection = Mockito.mock(HttpURLConnection.class);
    Mockito.doNothing().when(httpURLConnection)
        .setRequestProperty(nullable(String.class), nullable(String.class));
    Mockito.doReturn("").when(abfsRestOperation).getClientLatency();
    Mockito.doReturn(httpOperation).when(abfsRestOperation).createHttpOperation();
  }

  /**
   * Adds custom mock behavior to an {@link AbfsRestOperation} and its associated {@link AbfsHttpOperation}.
   * This method is primarily used in testing scenarios where specific behavior needs to be simulated
   * during an HTTP operation, such as triggering an exception or modifying the HTTP response.
   *
   * The method intercepts the creation of an {@link AbfsHttpOperation} within the provided
   * {@link AbfsRestOperation} and applies a given function to modify the behavior of the
   * {@link AbfsHttpOperation}.
   *
   * @param abfsRestOperation    The spied instance of {@link AbfsRestOperation} to which the mock
   *                             behavior is added.
   * @param functionRaisingIOE   A function that modifies the behavior of the created
   *                             {@link AbfsHttpOperation}, allowing the simulation of specific
   *                             outcomes, such as HTTP errors or connection resets.
   * @throws IOException         If an I/O error occurs while applying the function to the
   *                             {@link AbfsHttpOperation}.
   */
  public static void addMockBehaviourToRestOpAndHttpOp(final AbfsRestOperation abfsRestOperation,
      FunctionRaisingIOE<AbfsHttpOperation, AbfsHttpOperation> functionRaisingIOE)
      throws IOException {
    Mockito.doAnswer(answer -> {
      AbfsHttpOperation httpOp = (AbfsHttpOperation) Mockito.spy(
          answer.callRealMethod());
      functionRaisingIOE.apply(httpOp);
      return httpOp;
    }).when(abfsRestOperation).createHttpOperation();
  }

  /**
   * Adding general mock behaviour to AbfsClient to avoid any NPE occurring.
   * These will avoid any network call made and will return the relevant exception or return value directly.
   * @param abfsClient to be mocked
   * @param exponentialRetryPolicy
   * @param staticRetryPolicy
   * @throws IOException
   */
  public static void addGeneralMockBehaviourToAbfsClient(final AbfsClient abfsClient,
                                                         final ExponentialRetryPolicy exponentialRetryPolicy,
                                                         final StaticRetryPolicy staticRetryPolicy,
                                                         final AbfsThrottlingIntercept intercept) throws IOException, URISyntaxException {
    Mockito.doReturn(OAuth).when(abfsClient).getAuthType();
    Mockito.doReturn("").when(abfsClient).getAccessToken();
    AbfsConfiguration abfsConfiguration = Mockito.mock(AbfsConfiguration.class);
    Mockito.doReturn(abfsConfiguration).when(abfsClient).getAbfsConfiguration();
    AbfsCounters abfsCounters = Mockito.spy(new AbfsCountersImpl(new URI("abcd")));
    Mockito.doReturn(abfsCounters).when(abfsClient).getAbfsCounters();

    Mockito.doReturn(intercept).when(abfsClient).getIntercept();
    Mockito.doNothing()
        .when(intercept)
        .sendingRequest(any(), nullable(AbfsCounters.class));
    Mockito.doNothing().when(intercept).updateMetrics(any(), any());

    // Returning correct retry policy based on failure reason
    Mockito.doReturn(exponentialRetryPolicy).when(abfsClient).getExponentialRetryPolicy();
    Mockito.doReturn(staticRetryPolicy).when(abfsClient).getRetryPolicy(CONNECTION_TIMEOUT_ABBREVIATION);
    Mockito.doReturn(exponentialRetryPolicy).when(abfsClient).getRetryPolicy(
            AdditionalMatchers.not(eq(CONNECTION_TIMEOUT_ABBREVIATION)));

    // Defining behavior of static retry policy
    Mockito.doReturn(true).when(staticRetryPolicy)
            .shouldRetry(nullable(Integer.class), nullable(Integer.class));
    Mockito.doReturn(false).when(staticRetryPolicy).shouldRetry(1, HTTP_OK);
    Mockito.doReturn(false).when(staticRetryPolicy).shouldRetry(2, HTTP_OK);
    Mockito.doReturn(true).when(staticRetryPolicy).shouldRetry(1, HTTP_UNAVAILABLE);
    // We want only two retries to occcur
    Mockito.doReturn(false).when(staticRetryPolicy).shouldRetry(2, HTTP_UNAVAILABLE);
    Mockito.doReturn(STATIC_RETRY_POLICY_ABBREVIATION).when(staticRetryPolicy).getAbbreviation();
    Mockito.doReturn(ONE_SEC).when(staticRetryPolicy).getRetryInterval(nullable(Integer.class));

    // Defining behavior of exponential retry policy
    Mockito.doReturn(true).when(exponentialRetryPolicy)
            .shouldRetry(nullable(Integer.class), nullable(Integer.class));
    Mockito.doReturn(false).when(exponentialRetryPolicy).shouldRetry(1, HTTP_OK);
    Mockito.doReturn(false).when(exponentialRetryPolicy).shouldRetry(2, HTTP_OK);
    Mockito.doReturn(true).when(exponentialRetryPolicy).shouldRetry(1, HTTP_UNAVAILABLE);
    // We want only two retries to occcur
    Mockito.doReturn(false).when(exponentialRetryPolicy).shouldRetry(2, HTTP_UNAVAILABLE);
    Mockito.doReturn(EXPONENTIAL_RETRY_POLICY_ABBREVIATION).when(exponentialRetryPolicy).getAbbreviation();
    Mockito.doReturn(2 * ONE_SEC).when(exponentialRetryPolicy).getRetryInterval(nullable(Integer.class));

    AbfsConfiguration configurations = Mockito.mock(AbfsConfiguration.class);
    Mockito.doReturn(configurations).when(abfsClient).getAbfsConfiguration();
    Mockito.doReturn(true).when(configurations).getStaticRetryForConnectionTimeoutEnabled();
  }

  public static void hookOnRestOpsForTracingContextSingularity(AbfsClient client) {
    Set<TracingContext> tracingContextSet = new HashSet<>();
    ReentrantLock lock = new ReentrantLock();
    Answer answer = new Answer() {
      @Override
      public Object answer(final InvocationOnMock invocationOnMock)
          throws Throwable {
        AbfsRestOperation op = Mockito.spy((AbfsRestOperation) invocationOnMock.callRealMethod());
        Mockito.doAnswer(completeExecuteInvocation -> {
          lock.lock();
          try {
            TracingContext context = completeExecuteInvocation.getArgument(0);
            Assertions.assertThat(tracingContextSet).doesNotContain(context);
            tracingContextSet.add(context);
          } finally {
            lock.unlock();
          }
          return completeExecuteInvocation.callRealMethod();
        }).when(op).completeExecute(Mockito.any(TracingContext.class));
        return op;
      }
    };

    Mockito.doAnswer(answer)
        .when(client)
        .getAbfsRestOperation(Mockito.any(AbfsRestOperationType.class),
            Mockito.anyString(), Mockito.any(URL.class), Mockito.anyList(),
            Mockito.nullable(byte[].class), Mockito.anyInt(), Mockito.anyInt(),
            Mockito.nullable(String.class));
    Mockito.doAnswer(answer)
        .when(client)
        .getAbfsRestOperation(Mockito.any(AbfsRestOperationType.class),
            Mockito.anyString(), Mockito.any(URL.class), Mockito.anyList());
    Mockito.doAnswer(answer)
        .when(client)
        .getAbfsRestOperation(Mockito.any(AbfsRestOperationType.class),
            Mockito.anyString(), Mockito.any(URL.class), Mockito.anyList(),
            Mockito.nullable(String.class));
  }
}
