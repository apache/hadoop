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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.constants.HttpOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsApacheHttpExpect100Exception;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.http.HttpResponse;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EXPECT_100_JDK_ERROR;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_IS_EXPECT_HEADER_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test create operation.
 */
@RunWith(Parameterized.class)
public class ITestAbfsOutputStream extends AbstractAbfsIntegrationTest {

  private static final int TEST_EXECUTION_TIMEOUT = 2 * 60 * 1000;
  private static final String TEST_FILE_PATH = "testfile";
  private static final int TEN = 10;

  @Parameterized.Parameter
  public HttpOperationType httpOperationType;

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {HttpOperationType.JDK_HTTP_URL_CONNECTION},
        {HttpOperationType.APACHE_HTTP_CLIENT}
    });
  }


  public ITestAbfsOutputStream() throws Exception {
    super();
  }

  @Override
  public AzureBlobFileSystem getFileSystem(final Configuration configuration)
      throws Exception {
    Configuration conf = new Configuration(configuration);
    conf.set(ConfigurationKeys.FS_AZURE_NETWORKING_LIBRARY, httpOperationType.toString());
    return (AzureBlobFileSystem) FileSystem.newInstance(conf);
  }

  @Test
  public void testMaxRequestsAndQueueCapacityDefaults() throws Exception {
    Configuration conf = getRawConfiguration();
    final AzureBlobFileSystem fs = getFileSystem(conf);
    try (FSDataOutputStream out = fs.create(path(TEST_FILE_PATH))) {
    AbfsOutputStream stream = (AbfsOutputStream) out.getWrappedStream();

      int maxConcurrentRequests
          = getConfiguration().getWriteMaxConcurrentRequestCount();
      if (stream.isAppendBlobStream()) {
        maxConcurrentRequests = 1;
      }

    Assertions.assertThat(stream.getMaxConcurrentRequestCount()).describedAs(
        "maxConcurrentRequests should be " + maxConcurrentRequests)
        .isEqualTo(maxConcurrentRequests);
    Assertions.assertThat(stream.getMaxRequestsThatCanBeQueued()).describedAs(
        "maxRequestsToQueue should be " + getConfiguration()
            .getMaxWriteRequestsToQueue())
        .isEqualTo(getConfiguration().getMaxWriteRequestsToQueue());
    }
  }

  @Test
  public void testMaxRequestsAndQueueCapacity() throws Exception {
    Configuration conf = getRawConfiguration();
    int maxConcurrentRequests = 6;
    int maxRequestsToQueue = 10;
    conf.set(ConfigurationKeys.AZURE_WRITE_MAX_CONCURRENT_REQUESTS,
        "" + maxConcurrentRequests);
    conf.set(ConfigurationKeys.AZURE_WRITE_MAX_REQUESTS_TO_QUEUE,
        "" + maxRequestsToQueue);
    final AzureBlobFileSystem fs = getFileSystem(conf);
    try (FSDataOutputStream out = fs.create(path(TEST_FILE_PATH))) {
      AbfsOutputStream stream = (AbfsOutputStream) out.getWrappedStream();

      if (stream.isAppendBlobStream()) {
        maxConcurrentRequests = 1;
      }

      Assertions.assertThat(stream.getMaxConcurrentRequestCount()).describedAs(
          "maxConcurrentRequests should be " + maxConcurrentRequests).isEqualTo(maxConcurrentRequests);
      Assertions.assertThat(stream.getMaxRequestsThatCanBeQueued()).describedAs("maxRequestsToQueue should be " + maxRequestsToQueue)
          .isEqualTo(maxRequestsToQueue);
    }
  }

  /**
   * Verify the passing of AzureBlobFileSystem reference to AbfsOutputStream
   * to make sure that the FS instance is not eligible for GC while writing.
   */
  @Test(timeout = TEST_EXECUTION_TIMEOUT)
  public void testAzureBlobFileSystemBackReferenceInOutputStream()
      throws Exception {
    byte[] testBytes = new byte[5 * 1024];
    // Creating an output stream using a FS in a separate method to make the
    // FS instance used eligible for GC. Since when a method is popped from
    // the stack frame, it's variables become anonymous, this creates higher
    // chance of getting Garbage collected.
    try (AbfsOutputStream out = getStream()) {
      // Every 5KB block written is flushed and a GC is hinted, if the
      // executor service is shut down in between, the test should fail
      // indicating premature shutdown while writing.
      for (int i = 0; i < 5; i++) {
        out.write(testBytes);
        out.flush();
        System.gc();
        Assertions.assertThat(
            out.getExecutorService().isShutdown() || out.getExecutorService()
                .isTerminated())
            .describedAs("Executor Service should not be closed before "
                + "OutputStream while writing")
            .isFalse();
        Assertions.assertThat(out.getFsBackRef().isNull())
            .describedAs("BackReference in output stream should not be null")
            .isFalse();
      }
    }
  }

  /**
   * Verify AbfsOutputStream close() behaviour of throwing a PathIOE when the
   * FS instance is closed before the stream.
   */
  @Test
  public void testAbfsOutputStreamClosingFsBeforeStream()
      throws Exception {
    AzureBlobFileSystem fs = new AzureBlobFileSystem();
    fs.initialize(new URI(getTestUrl()), new Configuration());
    Path pathFs = path(getMethodName());
    byte[] inputBytes = new byte[5 * 1024];
    try (AbfsOutputStream out = createAbfsOutputStreamWithFlushEnabled(fs,
        pathFs)) {
      out.write(inputBytes);
      fs.close();
      // verify that output stream close after fs.close() would raise a
      // pathIOE containing the path being written to.
      intercept(PathIOException.class, getMethodName(), out::close);
    }
  }

  @Test
  public void testExpect100ContinueFailureInAppend() throws Exception {
    if (!getIsNamespaceEnabled(getFileSystem())) {
      Assume.assumeFalse("Not valid for APPEND BLOB", isAppendBlobEnabled());
    }
    Configuration configuration = new Configuration(getRawConfiguration());
    configuration.set(FS_AZURE_ACCOUNT_IS_EXPECT_HEADER_ENABLED, "true");
    AzureBlobFileSystem fs = getFileSystem(configuration);
    Path path = new Path("/testFile");
    AbfsOutputStream os = Mockito.spy(
        (AbfsOutputStream) fs.create(path).getWrappedStream());
    AzureIngressHandler ingressHandler = Mockito.spy(
        os.getIngressHandler());
    Mockito.doReturn(ingressHandler).when(os).getIngressHandler();

    AbfsClient spiedClient = Mockito.spy(ingressHandler.getClient());
    Mockito.doReturn(spiedClient).when(ingressHandler).getClient();
    AbfsHttpOperation[] httpOpForAppendTest = new AbfsHttpOperation[2];
    mockSetupForAppend(httpOpForAppendTest, spiedClient);
    Mockito.doReturn(spiedClient).when(os).getClient();
    fs.delete(path, true);
    os.write(1);
    if (spiedClient instanceof AbfsDfsClient) {
      intercept(FileNotFoundException.class, os::close);
    } else {
      IOException ex = intercept(IOException.class, os::close);
      Assertions.assertThat(ex.getCause().getCause()).isInstanceOf(
          AbfsRestOperationException.class);
    }
    Assertions.assertThat(httpOpForAppendTest[0].getConnectionDisconnectedOnError())
        .describedAs("First try from AbfsClient will have expect-100 "
            + "header and should fail with expect-100 error.").isTrue();
    if (httpOpForAppendTest[0] instanceof AbfsJdkHttpOperation) {
      Mockito.verify((AbfsJdkHttpOperation) httpOpForAppendTest[0],
              Mockito.times(0))
          .processConnHeadersAndInputStreams(Mockito.any(byte[].class),
              Mockito.anyInt(), Mockito.anyInt());
    }

    Assertions.assertThat(httpOpForAppendTest[1].getConnectionDisconnectedOnError())
        .describedAs("The retried operation from AbfsClient should not "
            + "fail with expect-100 error. The retried operation does not have"
            + "expect-100 header.").isFalse();
    if (httpOpForAppendTest[1] instanceof AbfsJdkHttpOperation) {
      Mockito.verify((AbfsJdkHttpOperation) httpOpForAppendTest[1],
              Mockito.times(1))
          .processConnHeadersAndInputStreams(Mockito.any(byte[].class),
              Mockito.anyInt(), Mockito.anyInt());
    }
  }

  private void mockSetupForAppend(final AbfsHttpOperation[] httpOpForAppendTest,
      final AbfsClient spiedClient) {
    int[] index = new int[1];
    index[0] = 0;
    Mockito.doAnswer(abfsRestOpAppendGetInvocation -> {
          AbfsRestOperation op = Mockito.spy(
              (AbfsRestOperation) abfsRestOpAppendGetInvocation.callRealMethod());
          boolean[] isExpectCall = new boolean[1];
          for (AbfsHttpHeader header : op.getRequestHeaders()) {
            if (header.getName().equals(EXPECT)) {
              isExpectCall[0] = true;
            }
          }
          Mockito.doAnswer(createHttpOpInvocation -> {
            httpOpForAppendTest[index[0]] = Mockito.spy(
                (AbfsHttpOperation) createHttpOpInvocation.callRealMethod());
            if (isExpectCall[0]) {
              if (httpOpForAppendTest[index[0]] instanceof AbfsJdkHttpOperation) {
                Mockito.doAnswer(invocation -> {
                      OutputStream os = (OutputStream) invocation.callRealMethod();
                      os.write(1);
                      os.close();
                      throw new ProtocolException(EXPECT_100_JDK_ERROR);
                    })
                    .when((AbfsJdkHttpOperation) httpOpForAppendTest[index[0]])
                    .getConnOutputStream();
              } else {
                Mockito.doAnswer(invocation -> {
                      throw new AbfsApacheHttpExpect100Exception(
                          (HttpResponse) invocation.callRealMethod());
                    })
                    .when((AbfsAHCHttpOperation) httpOpForAppendTest[index[0]])
                    .executeRequest();
              }
            }
            return httpOpForAppendTest[index[0]++];
          }).when(op).createHttpOperation();
          return op;
        })
        .when(spiedClient)
        .getAbfsRestOperation(Mockito.any(AbfsRestOperationType.class),
            Mockito.anyString(), Mockito.any(
                URL.class), Mockito.anyList(), Mockito.any(byte[].class),
            Mockito.anyInt(), Mockito.anyInt(), Mockito.nullable(String.class));
  }

  /**
   * Separate method to create an outputStream using a local FS instance so
   * that once this method has returned, the FS instance can be eligible for GC.
   *
   * @return AbfsOutputStream used for writing.
   */
  private AbfsOutputStream getStream() throws URISyntaxException, IOException {
    AzureBlobFileSystem fs1 = new AzureBlobFileSystem();
    fs1.initialize(new URI(getTestUrl()), new Configuration());
    Path pathFs1 = path(getMethodName() + "1");

    return createAbfsOutputStreamWithFlushEnabled(fs1, pathFs1);
  }

  /**
   * Verify that if getBlockList throws exception append should fail.
   */
  @Test
  public void testValidateGetBlockList() throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    Assume.assumeTrue(!getIsNamespaceEnabled(fs));
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    assumeBlobServiceType();

    // Mock the clientHandler to return the blobClient when getBlobClient is called
    AbfsClientHandler clientHandler = Mockito.spy(store.getClientHandler());
    AbfsBlobClient blobClient = Mockito.spy(clientHandler.getBlobClient());

    Mockito.doReturn(clientHandler).when(store).getClientHandler();
    Mockito.doReturn(blobClient).when(clientHandler).getBlobClient();
    Mockito.doReturn(blobClient).when(clientHandler).getIngressClient();

    Mockito.doReturn(store).when(fs).getAbfsStore();
    Path testFilePath = new Path("/testFile");
    AbfsOutputStream os = Mockito.spy((AbfsOutputStream) fs.create(testFilePath).getWrappedStream());

    Mockito.doReturn(clientHandler).when(os).getClientHandler();
    Mockito.doReturn(blobClient).when(clientHandler).getBlobClient();

    AbfsRestOperationException exception = getMockAbfsRestOperationException(HTTP_CONFLICT);
    // Throw exception when getBlockList is called
    Mockito.doThrow(exception).when(blobClient).getBlockList(Mockito.anyString(), Mockito.any(TracingContext.class));

    // Create a non-empty file
    os.write(TEN);
    os.hsync();
    os.close();

    Mockito.doCallRealMethod().when(store).openFileForWrite(Mockito.any(Path.class), Mockito.any(), Mockito.anyBoolean(), Mockito.any(TracingContext.class));
    intercept(AzureBlobFileSystemException.class, () -> store
        .openFileForWrite(testFilePath, null, false, getTestTracingContext(fs, true)));
  }

  /**
   * Verify that for flush without append no network calls are made for blob endpoint.
   **/
  @Test
  public void testNoNetworkCallsForFlush() throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    Assume.assumeTrue(!getIsNamespaceEnabled(fs));
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Assume.assumeTrue(store.getClient() instanceof AbfsBlobClient);

    // Mock the clientHandler to return the blobClient when getBlobClient is called
    AbfsClientHandler clientHandler = Mockito.spy(store.getClientHandler());
    AbfsBlobClient blobClient = Mockito.spy(clientHandler.getBlobClient());

    Mockito.doReturn(clientHandler).when(store).getClientHandler();
    Mockito.doReturn(blobClient).when(clientHandler).getBlobClient();
    Mockito.doReturn(blobClient).when(clientHandler).getIngressClient();

    Mockito.doReturn(store).when(fs).getAbfsStore();
    Path testFilePath = new Path("/testFile");
    AbfsOutputStream os = Mockito.spy((AbfsOutputStream) fs.create(testFilePath).getWrappedStream());
    AzureIngressHandler ingressHandler = Mockito.spy(os.getIngressHandler());
    Mockito.doReturn(ingressHandler).when(os).getIngressHandler();
    Mockito.doReturn(blobClient).when(ingressHandler).getClient();

    Mockito.doReturn(clientHandler).when(os).getClientHandler();
    Mockito.doReturn(blobClient).when(clientHandler).getBlobClient();

    os.hsync();

    Mockito.verify(blobClient, Mockito.times(0))
        .append(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
            Mockito.any(TracingContext.class));
    Mockito.verify(blobClient, Mockito.times(0)).
        flush(Mockito.any(byte[].class), Mockito.anyString(), Mockito.anyBoolean(),
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(),
            Mockito.any(TracingContext.class));
  }

  private AbfsRestOperationException getMockAbfsRestOperationException(int status) {
    return new AbfsRestOperationException(status, "", "", new Exception());
  }

  /**
   * Verify that for flush without append no network calls are made for blob endpoint.
   **/
  @Test
  public void testNoNetworkCallsForSecondFlush() throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    Assume.assumeTrue(!getIsNamespaceEnabled(fs));
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Assume.assumeTrue(store.getClient() instanceof AbfsBlobClient);
    Assume.assumeFalse("Not valid for APPEND BLOB", isAppendBlobEnabled());

    // Step 2: Mock the clientHandler to return the blobClient when getBlobClient is called
    AbfsClientHandler clientHandler = Mockito.spy(store.getClientHandler());
    AbfsBlobClient blobClient = Mockito.spy(clientHandler.getBlobClient());

    Mockito.doReturn(clientHandler).when(store).getClientHandler();
    Mockito.doReturn(blobClient).when(clientHandler).getBlobClient();
    Mockito.doReturn(blobClient).when(clientHandler).getIngressClient();

    Mockito.doReturn(store).when(fs).getAbfsStore();
    Path testFilePath = new Path("/testFile");
    AbfsOutputStream os = Mockito.spy((AbfsOutputStream) fs.create(testFilePath).getWrappedStream());
    AzureIngressHandler ingressHandler = Mockito.spy(os.getIngressHandler());
    Mockito.doReturn(ingressHandler).when(os).getIngressHandler();
    Mockito.doReturn(blobClient).when(ingressHandler).getClient();

    Mockito.doReturn(clientHandler).when(os).getClientHandler();
    Mockito.doReturn(blobClient).when(clientHandler).getBlobClient();

    os.write(10);
    os.hsync();
    os.close();

    Mockito.verify(blobClient, Mockito.times(1))
        .append(Mockito.anyString(), Mockito.any(byte[].class), Mockito.any(
                AppendRequestParameters.class), Mockito.any(), Mockito.any(),
            Mockito.any(TracingContext.class));
    Mockito.verify(blobClient, Mockito.times(1)).
        flush(Mockito.any(byte[].class), Mockito.anyString(), Mockito.anyBoolean(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(),
            Mockito.any(TracingContext.class));
  }
}
