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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;
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
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.http.HttpResponse;
import org.apache.hadoop.fs.store.DataBlocks;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EXPECT_100_JDK_ERROR;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_IS_EXPECT_HEADER_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Test create operation.
 */
@ParameterizedClass(name="{0}")
@MethodSource("params")
public class ITestAbfsOutputStream extends AbstractAbfsIntegrationTest {

  private static final int TEST_EXECUTION_TIMEOUT = 2 * 60 * 1000;
  private static final String TEST_FILE_PATH = "testfile";
  private static final int TEN = 10;

  public HttpOperationType httpOperationType;

  public static Iterable<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {HttpOperationType.JDK_HTTP_URL_CONNECTION},
        {HttpOperationType.APACHE_HTTP_CLIENT}
    });
  }

  public ITestAbfsOutputStream(final HttpOperationType pHttpOperationType) throws Exception {
    super();
    this.httpOperationType = pHttpOperationType;
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
  @Test
  @Timeout(value = TEST_EXECUTION_TIMEOUT, unit = TimeUnit.MILLISECONDS)
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
      assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
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
    assumeThat(getIsNamespaceEnabled(fs)).isFalse();
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
    assumeThat(getIsNamespaceEnabled(fs)).isFalse();
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
            Mockito.any(TracingContext.class), Mockito.anyString());
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
    assumeThat(getIsNamespaceEnabled(fs)).isFalse();
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    assumeBlobServiceType();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();

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
            Mockito.any(TracingContext.class), Mockito.nullable(String.class));
  }

  /**
   * Tests that the message digest is reset when an exception occurs during remote flush.
   * Simulates a failure in the flush operation and verifies reset is called on MessageDigest.
   */
  @Test
  public void testResetCalledOnExceptionInRemoteFlush() throws Exception {
    assumeHnsDisabled();
    assumeBlobServiceType();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());

    // Create a file and spy on AbfsOutputStream
    Path path = new Path("/testFile");
    AbfsOutputStream realOs = (AbfsOutputStream) fs.create(path).getWrappedStream();
    AbfsOutputStream os = Mockito.spy(realOs);
    AzureIngressHandler ingressHandler = Mockito.spy(os.getIngressHandler());
    Mockito.doReturn(ingressHandler).when(os).getIngressHandler();
    AbfsClient spiedClient = Mockito.spy(ingressHandler.getClient());
    Mockito.doReturn(spiedClient).when(ingressHandler).getClient();
    AzureBlobBlockManager blockManager = Mockito.spy((AzureBlobBlockManager) os.getBlockManager());
    Mockito.doReturn(blockManager).when(ingressHandler).getBlockManager();
    Mockito.doReturn(true).when(blockManager).hasBlocksToCommit();
    Mockito.doReturn("dummy-block-id").when(blockManager).getBlockIdToCommit();

    MessageDigest mockMessageDigest = Mockito.mock(MessageDigest.class);
    Mockito.doReturn(mockMessageDigest).when(os).getFullBlobContentMd5();
    Mockito.doReturn(os).when(ingressHandler).getAbfsOutputStream();
    Mockito.doReturn("dummyMd5").when(ingressHandler).computeFullBlobMd5();

    // Simulating the exception in client flush call
    Mockito.doThrow(
            new AbfsRestOperationException(HTTP_UNAVAILABLE, "", "", new Exception()))
        .when(spiedClient).flush(
            Mockito.any(byte[].class),
            Mockito.anyString(),
            Mockito.anyBoolean(),
            Mockito.nullable(String.class),
            Mockito.nullable(String.class),
            Mockito.anyString(),
            Mockito.nullable(ContextEncryptionAdapter.class),
            Mockito.any(TracingContext.class), Mockito.nullable(String.class));

    // Triggering the flush to simulate exception
    try {
      ingressHandler.remoteFlush(0, false, false, null,
          getTestTracingContext(fs, true));
    } catch (AzureBlobFileSystemException e) {
      //expected exception
    }
    // Verify that reset was called on the message digest
    if (spiedClient.isFullBlobChecksumValidationEnabled()) {
      Assertions.assertThat(Mockito.mockingDetails(mockMessageDigest).getInvocations()
          .stream()
          .filter(i -> i.getMethod().getName().equals("reset"))
          .count())
          .as("Expected MessageDigest.reset() to be called exactly once when checksum validation is enabled")
          .isEqualTo(1);
    }
  }

  /**
   * Tests that the message digest is reset when an exception occurs during remote flush.
   * Simulates a failure in the flush operation and verifies reset is called on MessageDigest.
   */
  @Test
  public void testNoChecksumComputedWhenConfigFalse()  throws Exception {
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB").isFalse();
    assumeBlobServiceType();
    assumeHnsDisabled();
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, false);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) fileSystem) {
      AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());

      // Create spies for the client handler and blob client
      AbfsClientHandler clientHandler = Mockito.spy(store.getClientHandler());
      AbfsBlobClient blobClient = Mockito.spy(clientHandler.getBlobClient());

      // Set up the spies to return the mocked objects
      Mockito.doReturn(clientHandler).when(store).getClientHandler();
      Mockito.doReturn(blobClient).when(clientHandler).getBlobClient();
      Mockito.doReturn(blobClient).when(clientHandler).getIngressClient();
      AbfsOutputStream abfsOutputStream = Mockito.spy(
          (AbfsOutputStream) fs.create(new Path("/test/file"))
              .getWrappedStream());
      AzureIngressHandler ingressHandler = Mockito.spy(
          abfsOutputStream.getIngressHandler());
      Mockito.doReturn(ingressHandler)
          .when(abfsOutputStream)
          .getIngressHandler();
      Mockito.doReturn(blobClient).when(ingressHandler).getClient();
      FSDataOutputStream os = Mockito.spy(
          new FSDataOutputStream(abfsOutputStream, null));
      AbfsOutputStream out = (AbfsOutputStream) os.getWrappedStream();
      byte[] bytes = new byte[1024 * 1024 * 4];
      new Random().nextBytes(bytes);
      // Write some bytes and attempt to flush, which should retry
      out.write(bytes);
      out.hsync();
      Assertions.assertThat(Mockito.mockingDetails(blobClient).getInvocations()
              .stream()
              .filter(
                  i -> i.getMethod().getName().equals("addCheckSumHeaderForWrite"))
              .count())
          .as("Expected addCheckSumHeaderForWrite() to be called exactly 0 times")
          .isZero();
    }
  }

  /**
   * Tests that the message digest is reset when an exception occurs during remote flush.
   * Simulates a failure in the flush operation and verifies reset is called on MessageDigest.
   */
  @Test
  public void testChecksumComputedWhenConfigTrue()  throws Exception {
    assumeHnsDisabled();
    assumeBlobServiceType();
    assumeThat(isAppendBlobEnabled()).as("Not valid for APPEND BLOB")
        .isFalse();
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, true);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) fileSystem) {
      AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
      // Create spies for the client handler and blob client
      AbfsClientHandler clientHandler = Mockito.spy(store.getClientHandler());
      AbfsBlobClient blobClient = Mockito.spy(clientHandler.getBlobClient());

      // Set up the spies to return the mocked objects
      Mockito.doReturn(clientHandler).when(store).getClientHandler();
      Mockito.doReturn(blobClient).when(clientHandler).getBlobClient();
      Mockito.doReturn(blobClient).when(clientHandler).getIngressClient();
      AbfsOutputStream abfsOutputStream = Mockito.spy(
          (AbfsOutputStream) fs.create(new Path("/test/file"))
              .getWrappedStream());
      AzureIngressHandler ingressHandler = Mockito.spy(
          abfsOutputStream.getIngressHandler());
      Mockito.doReturn(ingressHandler)
          .when(abfsOutputStream)
          .getIngressHandler();
      Mockito.doReturn(blobClient).when(ingressHandler).getClient();
      FSDataOutputStream os = Mockito.spy(
          new FSDataOutputStream(abfsOutputStream, null));
      AbfsOutputStream out = (AbfsOutputStream) os.getWrappedStream();
      byte[] bytes = new byte[1024 * 1024 * 4];
      new Random().nextBytes(bytes);
      // Write some bytes and attempt to flush, which should retry
      out.write(bytes);
      out.hsync();
      Assertions.assertThat(Mockito.mockingDetails(blobClient).getInvocations()
              .stream()
              .filter(
                  i -> i.getMethod().getName().equals("addCheckSumHeaderForWrite"))
              .count())
          .as("Expected addCheckSumHeaderForWrite() to be called exactly once")
          .isEqualTo(1);
    }
  }

  /**
   * Tests the selection logic for the DFS-to-Blob fallback handler in AbfsOutputStream.
   * Verifies:
   *   For FNS, fallback succeeds regardless of ingress service type.
   *   For HNS with BLOB ingress, fallback fails with InvalidConfigurationValueException.
   *   For HNS with DFS ingress, fallback succeeds.
   */
  @Test
  public void testDFSToBlobFallbackHandlerSelection() throws Exception {
    // Common mocks
    DataBlocks.BlockFactory blockFactory = Mockito.mock(DataBlocks.BlockFactory.class);
    AzureBlockManager blockManager = Mockito.mock(AzureBlockManager.class);
    AbfsClientHandler clientHandler = Mockito.mock(AbfsClientHandler.class);
    AbfsClient client = Mockito.mock(AbfsClient.class);

    Mockito.when(clientHandler.getClient(Mockito.any())).thenReturn(client);

    Method createNewHandler =
            AbfsOutputStream.class.getDeclaredMethod(
                    "createNewHandler",
                    AbfsServiceType.class,
                    DataBlocks.BlockFactory.class,
                    int.class,
                    boolean.class,
                    AzureBlockManager.class);
    createNewHandler.setAccessible(true);

    Field fallbackField =
            AbfsOutputStream.class.getDeclaredField("isDFSToBlobFallbackEnabled");
    fallbackField.setAccessible(true);

    Field serviceTypeField =
            AbfsOutputStream.class.getDeclaredField("serviceTypeAtInit");
    serviceTypeField.setAccessible(true);

    Field clientHandlerField =
            AbfsOutputStream.class.getDeclaredField("clientHandler");
    clientHandlerField.setAccessible(true);

    // FNS case: fallback should succeed regardless of ingress service type
    // Only setting isDFSToBlobFallbackEnabled config is enough
    Mockito.when(client.getIsNamespaceEnabled()).thenReturn(false);

    AbfsOutputStream fnsStream =
            Mockito.mock(AbfsOutputStream.class, Mockito.CALLS_REAL_METHODS);

    fallbackField.set(fnsStream, true);
    clientHandlerField.set(fnsStream, clientHandler);

    Object fnsHandler =
            createNewHandler.invoke(
                    fnsStream,
                    AbfsServiceType.BLOB,
                    blockFactory,
                    1024,
                    false,
                    blockManager);

    Assertions.assertThat(fnsHandler)
            .as("FNS: fallback should succeed regardless of ingress service type")
            .isInstanceOf(AzureDfsToBlobIngressFallbackHandler.class);

    // HNS case: if ingress service type is BLOB, fallback should fail
    Mockito.when(client.getIsNamespaceEnabled()).thenReturn(true);

    AbfsOutputStream hnsBlobStream =
            Mockito.mock(AbfsOutputStream.class, Mockito.CALLS_REAL_METHODS);

    fallbackField.set(hnsBlobStream, true);
    serviceTypeField.set(hnsBlobStream, AbfsServiceType.BLOB);
    clientHandlerField.set(hnsBlobStream, clientHandler);

    Assertions.assertThatThrownBy(() ->
                    createNewHandler.invoke(
                            hnsBlobStream,
                            AbfsServiceType.BLOB,
                            blockFactory,
                            1024,
                            false,
                            blockManager))
            .as("HNS with BLOB ingress should not allow fallback")
            .hasCauseInstanceOf(InvalidConfigurationValueException.class);

    // HNS case: if ingress service type is DFS, fallback should succeed
    AbfsOutputStream hnsDfsStream =
            Mockito.mock(AbfsOutputStream.class, Mockito.CALLS_REAL_METHODS);

    fallbackField.set(hnsDfsStream, true);
    serviceTypeField.set(hnsDfsStream, AbfsServiceType.DFS);
    clientHandlerField.set(hnsDfsStream, clientHandler);

    Object hnsHandler =
            createNewHandler.invoke(
                    hnsDfsStream,
                    AbfsServiceType.DFS,
                    blockFactory,
                    1024,
                    false,
                    blockManager);

    Assertions.assertThat(hnsHandler)
            .as("HNS with DFS ingress should allow fallback")
            .isInstanceOf(AzureDfsToBlobIngressFallbackHandler.class);
  }
}
