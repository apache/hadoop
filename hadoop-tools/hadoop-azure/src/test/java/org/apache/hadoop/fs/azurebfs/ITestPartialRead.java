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

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingIntercept;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingInterceptTestUtil;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsClient;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsClientThrottlingAnalyzer;
import org.apache.hadoop.fs.azurebfs.services.MockHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.MockHttpOperationTestIntercept;
import org.apache.hadoop.fs.azurebfs.services.MockHttpOperationTestInterceptResult;

import static java.net.HttpURLConnection.HTTP_PARTIAL;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestPartialRead extends AbstractAbfsIntegrationTest {

  private static final String TEST_PATH = "/testfile";

  private Logger LOG =
      LoggerFactory.getLogger(ITestPartialRead.class);

  public ITestPartialRead() throws Exception {
  }


  /**
   * Test1: Execute read for 4 MB, but httpOperation will read for only 1MB.:
   * retry with the remaining data, add data in throttlingIntercept.
   * Test2: Execute read for 4 MB, but httpOperation will throw connection-rest exception + read 1 MB:
   * retry with remaining data + add data in throttlingIntercept.
   * */


  private byte[] setup(final Path testPath, final int fileSize)
      throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore()
        .getAbfsConfiguration();
    final int bufferSize = 4 * ONE_MB;
    abfsConfiguration.setWriteBufferSize(bufferSize);
    abfsConfiguration.setReadBufferSize(bufferSize);
    abfsConfiguration.setReadAheadQueueDepth(0);

    final byte[] b = new byte[fileSize];
    new Random().nextBytes(b);


    FSDataOutputStream stream = fs.create(testPath);
    try {
      stream.write(b);
    } finally {
      stream.close();
    }
    return b;
  }

  @Test
  public void testRecoverPartialRead() throws Exception {
    int fileSize = 4 * ONE_MB;
    Path testPath = path(TEST_PATH);
    byte[] originalFile = setup(testPath, fileSize);

    final AzureBlobFileSystem fs = getFileSystem();
    MockAbfsClient abfsClient = new MockAbfsClient(fs.getAbfsClient());

    ActualServerReadByte actualServerReadByte = new ActualServerReadByte(
        fileSize, originalFile);
    MockHttpOperationTestIntercept mockHttpOperationTestIntercept
        = new MockHttpOperationTestIntercept() {
      private int callCount = 0;

      @Override
      public MockHttpOperationTestInterceptResult intercept(final MockHttpOperation mockHttpOperation,
          final byte[] buffer,
          final int offset,
          final int length) throws IOException {
        /*
         * 1. Check if server can handle the request parameters.
         * 2. return 1MB data to test-client.
         * */
        callActualServerAndAssertBehaviour(mockHttpOperation, buffer, offset,
            length, actualServerReadByte, ONE_MB);

        MockHttpOperationTestInterceptResult
            mockHttpOperationTestInterceptResult
            = new MockHttpOperationTestInterceptResult();
        mockHttpOperationTestInterceptResult.setStatus(HTTP_PARTIAL);
        mockHttpOperationTestInterceptResult.setBytesRead(ONE_MB);
        callCount++;
        return mockHttpOperationTestInterceptResult;
      }

      public int getCallCount() {
        return callCount;
      }
    };
    abfsClient.setMockHttpOperationTestIntercept(
        mockHttpOperationTestIntercept);
    fs.getAbfsStore().setClient(abfsClient);

    AbfsClientThrottlingIntercept intercept
        = AbfsClientThrottlingInterceptTestUtil.get();
    MockAbfsClientThrottlingAnalyzer readAnalyzer
        = new MockAbfsClientThrottlingAnalyzer("read");
    MockAbfsClientThrottlingAnalyzer analyzerToBeAsserted
        = (MockAbfsClientThrottlingAnalyzer) AbfsClientThrottlingInterceptTestUtil.setReadAnalyzer(
        intercept, readAnalyzer);

    FSDataInputStream inputStream = fs.open(testPath);
    byte[] buffer = new byte[fileSize];
    inputStream.read(0, buffer, 0, fileSize);

    Assert.assertEquals(4, mockHttpOperationTestIntercept.getCallCount());
    Assert.assertEquals(4,
        analyzerToBeAsserted.getFailedInstances().intValue());
  }

  private void callActualServerAndAssertBehaviour(final MockHttpOperation mockHttpOperation,
      final byte[] buffer,
      final int offset,
      final int length,
      final ActualServerReadByte actualServerReadByte,
      final int byteLenMockServerReturn) throws IOException {
    LOG.info("length: " + length + "; offset: " + offset);
    mockHttpOperation.processResponseSuperCall(buffer, offset, length);
    Assert.assertTrue(
        mockHttpOperation.getStatusCode() == HTTP_PARTIAL
            || mockHttpOperation.getStatusCode() == HttpURLConnection.HTTP_OK);
    int iterator = 0;
    int currPointer = actualServerReadByte.currPointer;
    while (actualServerReadByte.currPointer < actualServerReadByte.size
        && iterator < buffer.length) {
      actualServerReadByte.bytes[actualServerReadByte.currPointer++]
          = buffer[iterator++];
    }
    actualServerReadByte.currPointer = currPointer + byteLenMockServerReturn;
    if (actualServerReadByte.currPointer == actualServerReadByte.size) {
      for (int i = 0; i < actualServerReadByte.size; i++) {
        if (actualServerReadByte.bytes[i]
            != actualServerReadByte.originalFile[i]) {
          Assert.assertTrue("Parsed data is not equal to original file", false);
        }
      }
    }
  }

  @Test
  public void testPartialReadWithConnectionReset() throws IOException {
    int fileSize = 4 * ONE_MB;
    Path testPath = path(TEST_PATH);
    byte[] originalFile = setup(testPath, fileSize);

    final AzureBlobFileSystem fs = getFileSystem();
    MockAbfsClient abfsClient = new MockAbfsClient(fs.getAbfsClient());

    ActualServerReadByte actualServerReadByte = new ActualServerReadByte(
        fileSize, originalFile);

    MockHttpOperationTestIntercept mockHttpOperationTestIntercept
        = new MockHttpOperationTestIntercept() {
      private int callCount = 0;

      @Override
      public MockHttpOperationTestInterceptResult intercept(final MockHttpOperation mockHttpOperation,
          final byte[] buffer,
          final int offset,
          final int length) throws IOException {
        /*
         * 1. Check if server can handle the request parameters.
         * 2. return 1MB data with connection-reset exception to test-client.
         * */

        callActualServerAndAssertBehaviour(mockHttpOperation, buffer, offset,
            length, actualServerReadByte, ONE_MB);

        MockHttpOperationTestInterceptResult
            mockHttpOperationTestInterceptResult
            = new MockHttpOperationTestInterceptResult();
        mockHttpOperationTestInterceptResult.setStatus(HTTP_PARTIAL);
        mockHttpOperationTestInterceptResult.setBytesRead(ONE_MB);
        mockHttpOperationTestInterceptResult.setException(new SocketException(
            "Connection reset"));
        callCount++;
        return mockHttpOperationTestInterceptResult;
      }

      public int getCallCount() {
        return callCount;
      }
    };
    abfsClient.setMockHttpOperationTestIntercept(
        mockHttpOperationTestIntercept);
    fs.getAbfsStore().setClient(abfsClient);

    AbfsClientThrottlingIntercept intercept
        = AbfsClientThrottlingInterceptTestUtil.get();
    MockAbfsClientThrottlingAnalyzer readAnalyzer
        = new MockAbfsClientThrottlingAnalyzer("read");
    MockAbfsClientThrottlingAnalyzer analyzerToBeAsserted
        = (MockAbfsClientThrottlingAnalyzer) AbfsClientThrottlingInterceptTestUtil.setReadAnalyzer(
        intercept, readAnalyzer);

    FSDataInputStream inputStream = fs.open(testPath);
    byte[] buffer = new byte[fileSize];
    inputStream.read(0, buffer, 0, fileSize);

    Assert.assertEquals(4, mockHttpOperationTestIntercept.getCallCount());
    Assert.assertEquals(4,
        analyzerToBeAsserted.getFailedInstances().intValue());
  }

  private class ActualServerReadByte {

    byte[] bytes;

    int size;

    int currPointer = 0;

    byte[] originalFile;

    ActualServerReadByte(int size, byte[] originalFile) {
      bytes = new byte[size];
      this.size = size;
      this.originalFile = originalFile;
    }
  }

}
