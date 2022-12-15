package org.apache.hadoop.fs.azurebfs.services;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.util.List;
import java.util.Random;

import org.junit.Test;
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
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_IS_EXPECT_HEADER_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_HTTP_METHOD_OVERRIDE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_ACTION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_POSITION;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.Mockito.mock;

public class TestAbfsRestOperation extends AbstractAbfsIntegrationTest {

  public TestAbfsRestOperation() throws Exception {
    super();
  }

  private byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
  }

  @Test
  public void testExpectHundredContinue() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    AbfsClient abfsClient = fs.getAbfsStore().getClient();

    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        configuration.get(FS_AZURE_ABFS_ACCOUNT_NAME));

    AbfsConfiguration abfsConfig
        = TestAbfsConfigurationFieldsValidation.updateRetryConfigs(
        abfsConfiguration,
        5, 100);

    AbfsClient testClient = TestAbfsClient.createTestClientFromCurrentContext(
        abfsClient,
        abfsConfig);

    AppendRequestParameters appendRequestParameters = new AppendRequestParameters(
        0, 0, 5, AppendRequestParameters.Mode.APPEND_MODE, false, null, true);
    byte[] buffer = getRandomBytesArray(5);

    // Mock instance of AbfsRestOperation

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
    HttpURLConnection urlConnection = Mockito.spy((HttpURLConnection) url.openConnection());
    final int CONNECT_TIMEOUT = 1 * 1000;
    final int READ_TIMEOUT = 1 * 1000;

    urlConnection.setConnectTimeout(CONNECT_TIMEOUT);
    urlConnection.setReadTimeout(READ_TIMEOUT);
    urlConnection.setRequestMethod(HTTP_METHOD_PUT);

    for (AbfsHttpHeader header : requestHeaders) {
      urlConnection.setRequestProperty(header.getName(), header.getValue());
    }
    abfsHttpOperation.setConnection(urlConnection);

    Mockito.doThrow(new ProtocolException("Server rejected Operation")).when(urlConnection).getOutputStream();
    Mockito.doReturn(abfsHttpOperation).when(op).getHttpOperation(Mockito.any(), Mockito.any(), Mockito.any());
    TracingContext tracingContext = new TracingContext("abcd",
        "abcde", FSOperationType.APPEND,
        TracingHeaderFormat.ALL_ID_FORMAT, null);

    Mockito.doReturn(503).when(urlConnection).getResponseCode();
    Mockito.doReturn("Server busy").when(urlConnection).getResponseMessage();

    op.execute(tracingContext);
  }

  @Test
  public void testExpectHundredContinueWriteException() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Configuration configuration = new Configuration();
    configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
    AbfsClient abfsClient = fs.getAbfsStore().getClient();

    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        configuration.get(FS_AZURE_ABFS_ACCOUNT_NAME));

    AbfsConfiguration abfsConfig
        = TestAbfsConfigurationFieldsValidation.updateRetryConfigs(
        abfsConfiguration,
        0, 100);

    AbfsClient testClient = TestAbfsClient.createTestClientFromCurrentContext(
        abfsClient,
        abfsConfig);

    AppendRequestParameters appendRequestParameters = new AppendRequestParameters(
        0, 0, 5, AppendRequestParameters.Mode.APPEND_MODE, false, null, true);
    byte[] buffer = getRandomBytesArray(5);

    // Mock instance of AbfsRestOperation

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
    HttpURLConnection urlConnection = Mockito.spy((HttpURLConnection) url.openConnection());
    final int CONNECT_TIMEOUT = 1 * 1000;
    final int READ_TIMEOUT = 1 * 1000;

    urlConnection.setConnectTimeout(CONNECT_TIMEOUT);
    urlConnection.setReadTimeout(READ_TIMEOUT);
    urlConnection.setRequestMethod(HTTP_METHOD_PUT);

    for (AbfsHttpHeader header : requestHeaders) {
      urlConnection.setRequestProperty(header.getName(), header.getValue());
    }
    OutputStream outputStream = Mockito.spy(new OutputStream() {
      @Override
      public void write(final int i) throws IOException {
      }
    });
    Mockito.doReturn(outputStream).when(urlConnection).getOutputStream();
    abfsHttpOperation.setConnection(urlConnection);
    Mockito.doThrow(new IOException()).when(outputStream).write(buffer, appendRequestParameters.getoffset(), appendRequestParameters.getLength());
    Mockito.doReturn(abfsHttpOperation).when(op).getHttpOperation(Mockito.any(), Mockito.any(), Mockito.any());
    TracingContext tracingContext = new TracingContext("abcd",
        "abcde", FSOperationType.APPEND,
        TracingHeaderFormat.ALL_ID_FORMAT, null);
    intercept(IOException.class,
        () -> op.execute(tracingContext));
  }
}


