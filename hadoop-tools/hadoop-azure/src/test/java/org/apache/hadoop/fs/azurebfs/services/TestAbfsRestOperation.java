package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.HttpURLConnection;
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
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HUNDRED_CONTINUE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_IS_EXPECT_HEADER_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;

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
    configuration.setBoolean(FS_AZURE_ACCOUNT_IS_EXPECT_HEADER_ENABLED, true);
    AbfsClient abfsClient = fs.getAbfsStore().getClient();

    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        "dummy.dfs.core.windows.net");

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
    String newString = testPath.toString().substring(testPath.toString().lastIndexOf("/"),
        testPath.toString().length());

    final List<AbfsHttpHeader> requestHeaders = TestAbfsClient.getTestRequestHeaders(testClient);
//    if (appendRequestParameters.isExpectHeaderEnabled()) {
//      requestHeaders.add(new AbfsHttpHeader(EXPECT, HUNDRED_CONTINUE));
//    }
    AbfsRestOperation op = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.Append,
        testClient,
        HTTP_METHOD_PUT,
        TestAbfsClient.getTestUrl(testClient, newString),
        requestHeaders, buffer,
        appendRequestParameters.getoffset(),
        appendRequestParameters.getLength(), null));

    HttpURLConnection urlConnection = Mockito.spy((HttpURLConnection) TestAbfsClient.getTestUrl(testClient, newString).openConnection());
    final int CONNECT_TIMEOUT = 30 * 1000;
    final int READ_TIMEOUT = 30 * 1000;

    urlConnection.setConnectTimeout(CONNECT_TIMEOUT);
    urlConnection.setReadTimeout(READ_TIMEOUT);
    urlConnection.setRequestMethod(HTTP_METHOD_PUT);

    for (AbfsHttpHeader header : requestHeaders) {
      urlConnection.setRequestProperty(header.getName(), header.getValue());
    }
    AbfsHttpOperation abfsHttpOperation = new AbfsHttpOperation(TestAbfsClient.getTestUrl(testClient, newString), HTTP_METHOD_PUT,
        requestHeaders);
    abfsHttpOperation.setConnection(urlConnection);
    //Mockito.doThrow(new IOException()).when(urlConnection).getOutputStream();
    Mockito.doReturn(abfsHttpOperation).when(op).getHttpOperation(Mockito.any(), Mockito.any(), Mockito.any());
    TracingContext tracingContext = new TracingContext("abcd",
        "abcde", FSOperationType.APPEND,
        TracingHeaderFormat.ALL_ID_FORMAT, null);
    op.execute(tracingContext);
  }

  private class HttpsURLConnection {}
}
