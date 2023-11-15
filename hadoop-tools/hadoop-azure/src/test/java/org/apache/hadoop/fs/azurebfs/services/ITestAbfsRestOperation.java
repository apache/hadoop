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
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.TestAbfsConfigurationFieldsValidation;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APPEND_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PATCH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HUNDRED_CONTINUE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_HTTP_METHOD_OVERRIDE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_POSITION;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

@RunWith(Parameterized.class)
public class ITestAbfsRestOperation extends AbstractAbfsIntegrationTest {

  // Specifies whether getOutputStream() or write() throws IOException.
  public enum ErrorType {OUTPUTSTREAM, WRITE};

  private static final int HTTP_EXPECTATION_FAILED = 417;
  private static final int HTTP_ERROR = 0;
  private static final int ZERO = 0;
  private static final int REDUCED_RETRY_COUNT = 2;
  private static final int REDUCED_BACKOFF_INTERVAL = 100;
  private static final int BUFFER_LENGTH = 5;
  private static final int BUFFER_OFFSET = 0;
  private static final String TEST_PATH = "/testfile";

  // Specifies whether the expect header is enabled or not.
  @Parameterized.Parameter
  public boolean expectHeaderEnabled;

  // Gives the http response code.
  @Parameterized.Parameter(1)
  public int responseCode;

  // Gives the http response message.
  @Parameterized.Parameter(2)
  public String responseMessage;

  // Gives the errorType based on the enum.
  @Parameterized.Parameter(3)
  public ErrorType errorType;

  // The intercept.
  private AbfsThrottlingIntercept intercept;

  /*
    HTTP_OK = 200,
    HTTP_UNAVAILABLE = 503,
    HTTP_NOT_FOUND = 404,
    HTTP_EXPECTATION_FAILED = 417,
    HTTP_ERROR = 0.
   */
  @Parameterized.Parameters(name = "expect={0}-code={1}-ErrorType={3}")
  public static Iterable<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {true, HTTP_OK, "OK", ErrorType.WRITE},
        {false, HTTP_OK, "OK", ErrorType.WRITE},
        {true, HTTP_UNAVAILABLE, "ServerBusy", ErrorType.OUTPUTSTREAM},
        {true, HTTP_NOT_FOUND, "Resource Not Found", ErrorType.OUTPUTSTREAM},
        {true, HTTP_EXPECTATION_FAILED, "Expectation Failed", ErrorType.OUTPUTSTREAM},
        {true, HTTP_ERROR, "Error", ErrorType.OUTPUTSTREAM}
    });
  }

  public ITestAbfsRestOperation() throws Exception {
    super();
  }

  /**
   * Test helper method to get random bytes array.
   * @param length The length of byte buffer
   * @return byte buffer
   */
  private byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
  }

  /**
   * Gives the AbfsRestOperation.
   * @return abfsRestOperation.
   */
  private AbfsRestOperation getRestOperation() throws Exception {
    // Get the filesystem.
    final AzureBlobFileSystem fs = getFileSystem();

    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    AbfsClient abfsClient = fs.getAbfsStore().getClient();

    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        configuration.get(FS_AZURE_ABFS_ACCOUNT_NAME));

    // Update the configuration with reduced retry count and reduced backoff interval.
    AbfsConfiguration abfsConfig
        = TestAbfsConfigurationFieldsValidation.updateRetryConfigs(
        abfsConfiguration,
        REDUCED_RETRY_COUNT, REDUCED_BACKOFF_INTERVAL);

    intercept = Mockito.mock(AbfsThrottlingIntercept.class);
    Mockito.doNothing().when(intercept).updateMetrics(Mockito.any(), Mockito.any());

    // Gets the client.
    AbfsClient testClient = Mockito.spy(ITestAbfsClient.createTestClientFromCurrentContext(
        abfsClient,
        abfsConfig));

    Mockito.doReturn(intercept).when(testClient).getIntercept();

    // Expect header is enabled or not based on the parameter.
    AppendRequestParameters appendRequestParameters
        = new AppendRequestParameters(
        BUFFER_OFFSET, BUFFER_OFFSET, BUFFER_LENGTH,
        AppendRequestParameters.Mode.APPEND_MODE, false, null,
        expectHeaderEnabled);

    byte[] buffer = getRandomBytesArray(5);

    // Create a test container to upload the data.
    Path testPath = path(TEST_PATH);
    fs.create(testPath);
    String finalTestPath = testPath.toString().substring(testPath.toString().lastIndexOf("/"));

    // Creates a list of request headers.
    final List<AbfsHttpHeader> requestHeaders = ITestAbfsClient.getTestRequestHeaders(testClient);
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE, HTTP_METHOD_PATCH));
    if (appendRequestParameters.isExpectHeaderEnabled()) {
      requestHeaders.add(new AbfsHttpHeader(EXPECT, HUNDRED_CONTINUE));
    }

    // Updates the query parameters.
    final AbfsUriQueryBuilder abfsUriQueryBuilder = testClient.createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, APPEND_ACTION);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_POSITION, Long.toString(appendRequestParameters.getPosition()));

    // Creates the url for the specified path.
    URL url =  testClient.createRequestUrl(finalTestPath, abfsUriQueryBuilder.toString());

    // Create a mock of the AbfsRestOperation to set the urlConnection in the corresponding httpOperation.
    AbfsRestOperation op = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.Append,
        testClient,
        HTTP_METHOD_PUT,
        url,
        requestHeaders, buffer,
        appendRequestParameters.getoffset(),
        appendRequestParameters.getLength(), null));

    AbfsHttpOperation abfsHttpOperation = Mockito.spy(new AbfsHttpOperation(url, HTTP_METHOD_PUT, requestHeaders));

    // Sets the expect request property if expect header is enabled.
    if (expectHeaderEnabled) {
      Mockito.doReturn(HUNDRED_CONTINUE)
          .when(abfsHttpOperation)
          .getConnProperty(EXPECT);
    }

    HttpURLConnection urlConnection = mock(HttpURLConnection.class);
    Mockito.doNothing().when(urlConnection).setRequestProperty(Mockito
        .any(), Mockito.any());
    Mockito.doReturn(HTTP_METHOD_PUT).when(urlConnection).getRequestMethod();
    Mockito.doReturn(url).when(urlConnection).getURL();
    Mockito.doReturn(urlConnection).when(abfsHttpOperation).getConnection();

    Mockito.doNothing().when(abfsHttpOperation).setRequestProperty(Mockito
        .any(), Mockito.any());
    Mockito.doReturn(url).when(abfsHttpOperation).getConnUrl();
    Mockito.doReturn(HTTP_METHOD_PUT).when(abfsHttpOperation).getConnRequestMethod();

    switch (errorType) {
    case OUTPUTSTREAM:
      // If the getOutputStream() throws IOException and Expect Header is
      // enabled, it returns back to processResponse and hence we have
      // mocked the response code and the response message to check different
      // behaviour based on response code.
      Mockito.doReturn(responseCode).when(abfsHttpOperation).getConnResponseCode();
      Mockito.doReturn(responseMessage)
          .when(abfsHttpOperation)
          .getConnResponseMessage();
      Mockito.doThrow(new ProtocolException("Server rejected Operation"))
          .when(abfsHttpOperation)
          .getConnOutputStream();
      break;
    case WRITE:
      // If write() throws IOException and Expect Header is
      // enabled or not, it should throw back the exception.
      OutputStream outputStream = Mockito.spy(new OutputStream() {
        @Override
        public void write(final int i) throws IOException {
        }
      });
      Mockito.doReturn(outputStream).when(abfsHttpOperation).getConnOutputStream();
      Mockito.doThrow(new IOException())
          .when(outputStream)
          .write(buffer, appendRequestParameters.getoffset(),
              appendRequestParameters.getLength());
      break;
    default:
      break;
    }

    // Sets the httpOperation for the rest operation.
    Mockito.doReturn(abfsHttpOperation)
        .when(op)
        .createHttpOperation();
    return op;
  }

  void assertTraceContextState(int retryCount, int assertRetryCount, int bytesSent, int assertBytesSent,
                               int expectedBytesSent, int assertExpectedBytesSent) {
    // Assert that the request is retried or not.
    Assertions.assertThat(retryCount)
            .describedAs("The retry count is incorrect")
            .isEqualTo(assertRetryCount);

    // Assert that metrics will be updated correctly.
    Assertions.assertThat(bytesSent)
            .describedAs("The bytes sent is incorrect")
            .isEqualTo(assertBytesSent);
    Assertions.assertThat(expectedBytesSent)
            .describedAs("The expected bytes sent is incorrect")
            .isEqualTo(assertExpectedBytesSent);
  }

  /**
   * Test the functionalities based on whether getOutputStream() or write()
   * throws exception and what is the corresponding response code.
   */
  @Test
  public void testExpectHundredContinue() throws Exception {
    // Gets the AbfsRestOperation.
    AbfsRestOperation op = getRestOperation();
    AbfsHttpOperation httpOperation = op.createHttpOperation();

    TracingContext tracingContext = Mockito.spy(new TracingContext("abcd",
        "abcde", FSOperationType.APPEND,
        TracingHeaderFormat.ALL_ID_FORMAT, null));
    Mockito.doReturn(tracingContext).when(op).createNewTracingContext(Mockito.any());

    switch (errorType) {
    case WRITE:
      // If write() throws IOException and Expect Header is
      // enabled or not, it should throw back the exception
      // which is caught and exponential retry logic comes into place.
      intercept(IOException.class,
          () -> op.execute(tracingContext));

      // Asserting update of metrics and retries.
      assertTraceContextState(tracingContext.getRetryCount(), REDUCED_RETRY_COUNT, httpOperation.getBytesSent(), BUFFER_LENGTH,
              0, 0);
      break;
    case OUTPUTSTREAM:
      switch (responseCode) {
      case HTTP_UNAVAILABLE:
        // In the case of 503 i.e. throttled case, we should retry.
        intercept(IOException.class,
            () -> op.execute(tracingContext));

        // Asserting update of metrics and retries.
        assertTraceContextState(tracingContext.getRetryCount(), REDUCED_RETRY_COUNT, httpOperation.getBytesSent(), ZERO,
                httpOperation.getExpectedBytesToBeSent(), BUFFER_LENGTH);

        // Verifies that update Metrics call is made for throttle case and for the first without retry +
        // for the retried cases as well.
        Mockito.verify(intercept, times(REDUCED_RETRY_COUNT + 1))
            .updateMetrics(Mockito.any(), Mockito.any());
        break;
      case HTTP_ERROR:
        // In the case of http status code 0 i.e. ErrorType case, we should retry.
        intercept(IOException.class,
            () -> op.execute(tracingContext));

        // Asserting update of metrics and retries.
        assertTraceContextState(tracingContext.getRetryCount(), REDUCED_RETRY_COUNT, httpOperation.getBytesSent(),
                ZERO, 0, 0);

        // Verifies that update Metrics call is made for ErrorType case and for the first without retry +
        // for the retried cases as well.
        Mockito.verify(intercept, times(REDUCED_RETRY_COUNT + 1))
            .updateMetrics(Mockito.any(), Mockito.any());
        break;
      case HTTP_NOT_FOUND:
      case HTTP_EXPECTATION_FAILED:
        // In the case of 4xx ErrorType. i.e. user ErrorType, retry should not happen.
        intercept(AzureBlobFileSystemException.class,
            () -> op.execute(tracingContext));

        // Asserting update of metrics and retries.
        assertTraceContextState(tracingContext.getRetryCount(), ZERO, 0,
                0, 0, 0);

        // Verifies that update Metrics call is not made for user ErrorType case.
        Mockito.verify(intercept, never())
            .updateMetrics(Mockito.any(), Mockito.any());
        break;
      default:
        break;
      }
      break;
    default:
      break;
    }
  }
}
