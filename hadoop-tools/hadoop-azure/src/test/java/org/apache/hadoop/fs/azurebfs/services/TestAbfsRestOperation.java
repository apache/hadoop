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

@RunWith(Parameterized.class)
public class TestAbfsRestOperation extends AbstractAbfsIntegrationTest {

  public enum error {OUTPUTSTREAM, WRITE};

  @Parameterized.Parameter
  public boolean expectHeaderEnabled;

  @Parameterized.Parameter(1)
  public int responseCode;

  @Parameterized.Parameter(2)
  public String responseMessage;

  @Parameterized.Parameter(3)
  public error errorType;

  @Parameterized.Parameters(name = "expect={0}-code={1}-error={3}")
  public static Iterable<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {true, 200, "OK", error.WRITE},
        {false, 200, "OK", error.WRITE},
        {true, 503, "ServerBusy", error.OUTPUTSTREAM},
        {false, 503, "ServerBusy", error.OUTPUTSTREAM},
        {true, 404, "Resource Not Found", error.OUTPUTSTREAM},
        {true, 417, "Expectation Failed", error.OUTPUTSTREAM}
    });
  }

  public TestAbfsRestOperation() throws Exception {
    super();
  }

  private byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
  }

  private AbfsRestOperation getRestOperation() throws Exception{
    final AzureBlobFileSystem fs = getFileSystem();
    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    AbfsClient abfsClient = fs.getAbfsStore().getClient();

    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        configuration.get(FS_AZURE_ABFS_ACCOUNT_NAME));

    AbfsConfiguration abfsConfig
        = TestAbfsConfigurationFieldsValidation.updateRetryConfigs(
        abfsConfiguration,
        2, 100);

    AbfsClient testClient = TestAbfsClient.createTestClientFromCurrentContext(
        abfsClient,
        abfsConfig);
    AppendRequestParameters appendRequestParameters = new AppendRequestParameters(
        0, 0, 5, AppendRequestParameters.Mode.APPEND_MODE, false, null, expectHeaderEnabled);
    byte[] buffer = getRandomBytesArray(5);

    final String TEST_PATH = "/testfile";
    Path testPath = path(TEST_PATH);
    fs.create(testPath);
    String newString = testPath.toString().substring(testPath.toString().lastIndexOf("/"));
    final List<AbfsHttpHeader> requestHeaders = TestAbfsClient.getTestRequestHeaders(testClient);
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE, HTTP_METHOD_PATCH));
    if (appendRequestParameters.isExpectHeaderEnabled()) {
      requestHeaders.add(new AbfsHttpHeader(EXPECT, HUNDRED_CONTINUE));
    }
    final AbfsUriQueryBuilder abfsUriQueryBuilder = testClient.createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, APPEND_ACTION);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_POSITION, Long.toString(appendRequestParameters.getPosition()));
    URL url =  testClient.createRequestUrl(newString, abfsUriQueryBuilder.toString());
    AbfsRestOperation op = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.Append,
        testClient,
        HTTP_METHOD_PUT,
        url,
        requestHeaders, buffer,
        appendRequestParameters.getoffset(),
        appendRequestParameters.getLength(), null));

    AbfsHttpOperation abfsHttpOperation = new AbfsHttpOperation(url, HTTP_METHOD_PUT, requestHeaders);
    HttpURLConnection urlConnection = mock(HttpURLConnection.class);

    if (expectHeaderEnabled) {
      Mockito.doReturn(HUNDRED_CONTINUE)
          .when(urlConnection)
          .getRequestProperty(EXPECT);
    }
    Mockito.doNothing().when(urlConnection).setRequestProperty(Mockito
        .any(), Mockito.any());
    Mockito.doReturn(url).when(urlConnection).getURL();
    Mockito.doReturn(HTTP_METHOD_PUT).when(urlConnection).getRequestMethod();

    switch (errorType) {
    case OUTPUTSTREAM:
      Mockito.doReturn(responseCode).when(urlConnection).getResponseCode();
      Mockito.doReturn(responseMessage)
          .when(urlConnection)
          .getResponseMessage();
      Mockito.doThrow(new ProtocolException("Server rejected Operation"))
          .when(urlConnection)
          .getOutputStream();
      break;
    case WRITE:
      OutputStream outputStream = Mockito.spy(new OutputStream() {
        @Override
        public void write(final int i) throws IOException {
        }
      });
      Mockito.doReturn(outputStream).when(urlConnection).getOutputStream();
      Mockito.doThrow(new IOException())
          .when(outputStream)
          .write(buffer, appendRequestParameters.getoffset(),
              appendRequestParameters.getLength());
      break;
    default:
      break;
    }
    abfsHttpOperation.setConnection(urlConnection);
    Mockito.doReturn(abfsHttpOperation)
        .when(op)
        .getHttpOperation(Mockito.any(), Mockito.any(), Mockito.any());
    return op;
  }

  @Test
  public void testExpectHundredContinue() throws Exception {
    AbfsRestOperation op = getRestOperation();
    TracingContext tracingContext = Mockito.spy(new TracingContext("abcd",
        "abcde", FSOperationType.APPEND,
        TracingHeaderFormat.ALL_ID_FORMAT, null));
    switch (errorType) {
    case WRITE:
      intercept(IOException.class,
          () -> op.execute(tracingContext));
      Assertions.assertThat(tracingContext.getRetryCount())
          .describedAs("The retry count is incorrect")
          .isEqualTo(2);
      break;
    case OUTPUTSTREAM:
      switch (responseCode) {
        case 503:
          intercept(IOException.class,
              () -> op.execute(tracingContext));
          Assertions.assertThat(tracingContext.getRetryCount())
              .describedAs("The retry count is incorrect")
              .isEqualTo(2);
          break;
      case 404:
        intercept(AzureBlobFileSystemException.class,
            () -> op.execute(tracingContext));
        Assertions.assertThat(tracingContext.getRetryCount())
            .describedAs("The retry count is incorrect")
            .isEqualTo(0);
        break;
      case 417:
        intercept(AzureBlobFileSystemException.class,
            () -> op.execute(tracingContext));
        Assertions.assertThat(tracingContext.getRetryCount())
            .describedAs("The retry count is incorrect")
            .isEqualTo(0);
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


