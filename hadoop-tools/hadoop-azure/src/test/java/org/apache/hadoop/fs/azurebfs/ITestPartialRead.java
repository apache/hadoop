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
import java.net.SocketException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingIntercept;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingInterceptTestUtil;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsClientThrottlingAnalyzer;
import org.apache.hadoop.fs.azurebfs.services.MockClassUtils;
import org.apache.hadoop.fs.azurebfs.services.MockHttpOperationTestIntercept;
import org.apache.hadoop.fs.azurebfs.services.MockHttpOperationTestInterceptResult;

import static java.net.HttpURLConnection.HTTP_PARTIAL;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CONNECTION_RESET;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestPartialRead extends AbstractAbfsIntegrationTest {

  private static final String TEST_PATH = "/testfile";

  private Logger LOG =
      LoggerFactory.getLogger(ITestPartialRead.class);

  public ITestPartialRead() throws Exception {
  }

  /*
   * Test1: Execute read for 4 MB, but httpOperation will read for only 1MB for
   * 50% occurrence and 0B for 50% occurrence: retry with the remaining data,
   *  add data in throttlingIntercept.
   * Test2: Execute read for 4 MB, but httpOperation will read for only 1MB.:
   * retry with the remaining data, add data in throttlingIntercept.
   * Test3: Execute read for 4 MB, but httpOperation will throw connection-reset
   * exception + read 1 MB: retry with remaining data + add data in throttlingIntercept.
   */

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
  public void testRecoverZeroBytePartialRead() throws Exception {
    int fileSize = 4 * ONE_MB;
    Path testPath = path(TEST_PATH);
    byte[] originalFile = setup(testPath, fileSize);

    final AzureBlobFileSystem fs = getFileSystem();

    final Boolean[] oneMBSupplier = new Boolean[1];
    oneMBSupplier[0] = false;

    ActualServerReadByte actualServerReadByte = new ActualServerReadByte(
        fileSize, originalFile);
    MockHttpOperationTestIntercept mockHttpOperationTestIntercept
        = new MockHttpOperationTestIntercept() {
      private int callCount = 0;

      @Override
      public MockHttpOperationTestInterceptResult intercept(final AbfsHttpOperation abfsHttpOperation,
          final byte[] buffer,
          final int offset,
          final int length) throws IOException {
        /*
         * 1. Check if server can handle the request parameters.
         * 2. return 1MB data for 50% occurrence and 0B for 50% occurrence to test-client.
         */
        int size;
        if (oneMBSupplier[0]) {
          size = ONE_MB;
          oneMBSupplier[0] = false;
        } else {
          size = 0;
          oneMBSupplier[0] = true;
        }
        callActualServerAndAssertBehaviour(abfsHttpOperation, buffer, offset,
            length, actualServerReadByte, size, fs.open(testPath));

        MockHttpOperationTestInterceptResult
            mockHttpOperationTestInterceptResult
            = new MockHttpOperationTestInterceptResult();
        mockHttpOperationTestInterceptResult.setStatus(HTTP_PARTIAL);
        mockHttpOperationTestInterceptResult.setBytesRead(size);
        callCount++;
        return mockHttpOperationTestInterceptResult;
      }

      public int getCallCount() {
        return callCount;
      }
    };

    setMocks(fs, fs.getAbfsClient(), mockHttpOperationTestIntercept);

    MockAbfsClientThrottlingAnalyzer
        analyzerToBeAsserted = setReadAnalyzer();

    FSDataInputStream inputStream = fs.open(testPath);
    byte[] buffer = new byte[fileSize];
    inputStream.read(0, buffer, 0, fileSize);
    Assertions.assertThat(mockHttpOperationTestIntercept.getCallCount())
        .describedAs("Number of server calls is wrong")
        .isEqualTo(8);
    Assertions.assertThat(analyzerToBeAsserted.getFailedInstances().intValue())
        .describedAs(
            "Number of server calls counted as throttling case is incorrect")
        .isEqualTo(7);
  }

  private MockAbfsClientThrottlingAnalyzer setReadAnalyzer() {
    AbfsClientThrottlingIntercept intercept
        = AbfsClientThrottlingInterceptTestUtil.get();
    MockAbfsClientThrottlingAnalyzer readAnalyzer
        = new MockAbfsClientThrottlingAnalyzer("read");
    MockAbfsClientThrottlingAnalyzer analyzerToBeAsserted
        = (MockAbfsClientThrottlingAnalyzer) AbfsClientThrottlingInterceptTestUtil.setReadAnalyzer(
        intercept, readAnalyzer);
    return analyzerToBeAsserted;
  }

  @Test
  public void testRecoverPartialRead() throws Exception {
    int fileSize = 4 * ONE_MB;
    Path testPath = path(TEST_PATH);
    byte[] originalFile = setup(testPath, fileSize);

    final AzureBlobFileSystem fs = getFileSystem();

    ActualServerReadByte actualServerReadByte = new ActualServerReadByte(
        fileSize, originalFile);

    final AbfsClient originalClient = fs.getAbfsClient();

    FSDataInputStream inputStreamOriginal = fs.open(testPath);

    MockHttpOperationTestIntercept mockHttpOperationTestIntercept
        = new MockHttpOperationTestIntercept() {
      private int callCount = 0;

      @Override
      public MockHttpOperationTestInterceptResult intercept(final AbfsHttpOperation abfsHttpOperation,
          final byte[] buffer,
          final int offset,
          final int length) throws IOException {
        /*
         * 1. Check if server can handle the request parameters.
         * 2. return 1MB data to test-client.
         */
        callActualServerAndAssertBehaviour(abfsHttpOperation, buffer, offset,
            length, actualServerReadByte, ONE_MB, inputStreamOriginal);

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

    setMocks(fs, originalClient, mockHttpOperationTestIntercept);

    MockAbfsClientThrottlingAnalyzer analyzerToBeAsserted = setReadAnalyzer();

    FSDataInputStream inputStream = fs.open(testPath);
    byte[] buffer = new byte[fileSize];
    inputStream.read(0, buffer, 0, fileSize);
    Assertions.assertThat(mockHttpOperationTestIntercept.getCallCount())
        .describedAs("Number of server calls is wrong")
        .isEqualTo(4);
    Assertions.assertThat(analyzerToBeAsserted.getFailedInstances().intValue())
        .describedAs(
            "Number of server calls counted as throttling case is incorrect")
        .isEqualTo(3);
  }

  @Test
  public void testPartialReadWithConnectionReset() throws IOException {
    int fileSize = 4 * ONE_MB;
    Path testPath = path(TEST_PATH);
    byte[] originalFile = setup(testPath, fileSize);

    final AzureBlobFileSystem fs = getFileSystem();

    ActualServerReadByte actualServerReadByte = new ActualServerReadByte(
        fileSize, originalFile);

    MockHttpOperationTestIntercept mockHttpOperationTestIntercept
        = new MockHttpOperationTestIntercept() {
      private int callCount = 0;

      @Override
      public MockHttpOperationTestInterceptResult intercept(final AbfsHttpOperation abfsHttpOperation,
          final byte[] buffer,
          final int offset,
          final int length) throws IOException {
        /*
         * 1. Check if server can handle the request parameters.
         * 2. return 1MB data with connection-reset exception to test-client.
         */

        callActualServerAndAssertBehaviour(abfsHttpOperation, buffer, offset,
            length, actualServerReadByte, ONE_MB, fs.open(testPath));

        MockHttpOperationTestInterceptResult
            mockHttpOperationTestInterceptResult
            = new MockHttpOperationTestInterceptResult();
        mockHttpOperationTestInterceptResult.setStatus(HTTP_PARTIAL);
        mockHttpOperationTestInterceptResult.setBytesRead(ONE_MB);
        mockHttpOperationTestInterceptResult.setException(new SocketException(
            CONNECTION_RESET));
        callCount++;
        return mockHttpOperationTestInterceptResult;
      }

      public int getCallCount() {
        return callCount;
      }
    };

    setMocks(fs, fs.getAbfsClient(), mockHttpOperationTestIntercept);

    MockAbfsClientThrottlingAnalyzer analyzerToBeAsserted = setReadAnalyzer();

    FSDataInputStream inputStream = fs.open(testPath);
    byte[] buffer = new byte[fileSize];
    inputStream.read(0, buffer, 0, fileSize);

    Assertions.assertThat(mockHttpOperationTestIntercept.getCallCount())
        .describedAs("Number of server calls is wrong")
        .isEqualTo(4);
    Assertions.assertThat(analyzerToBeAsserted.getFailedInstances().intValue())
        .describedAs(
            "Number of server calls counted as throttling case is incorrect")
        .isEqualTo(3);
  }

  @SuppressWarnings("unchecked") // suppressing unchecked since, className of List<AbfsHttpHeader> not possible and need to supply List.class
  private void setMocks(final AzureBlobFileSystem fs,
      final AbfsClient originalClient,
      final MockHttpOperationTestIntercept mockHttpOperationTestIntercept) {
    AbfsClient abfsClient = Mockito.spy(originalClient);
    MockClassUtils.mockAbfsClientGetAbfsRestOperation(
        (getRestOpMockInvocation, objects) -> {
          AbfsRestOperation abfsRestOperation
              = MockClassUtils.createAbfsRestOperation(
              getRestOpMockInvocation.getArgument(0,
                  AbfsRestOperationType.class),
              (AbfsClient) objects[0],
              getRestOpMockInvocation.getArgument(1, String.class),
              getRestOpMockInvocation.getArgument(2, URL.class),
              getRestOpMockInvocation.getArgument(3,List.class),
              getRestOpMockInvocation.getArgument(4, byte[].class),
              getRestOpMockInvocation.getArgument(5, Integer.class),
              getRestOpMockInvocation.getArgument(6, Integer.class),
              getRestOpMockInvocation.getArgument(7, String.class)
          );
          if (AbfsRestOperationType.ReadFile
              == getRestOpMockInvocation.getArgument(0,
              AbfsRestOperationType.class)) {
            AbfsRestOperation mockRestOp = Mockito.spy(abfsRestOperation);
            setMockAbfsRestOperation(mockHttpOperationTestIntercept,
                mockRestOp);
            return mockRestOp;
          }
          return abfsRestOperation;
        }, abfsClient);
    fs.getAbfsStore().setClient(abfsClient);
  }

  private void setMockAbfsRestOperation(final MockHttpOperationTestIntercept mockHttpOperationTestIntercept,
      final AbfsRestOperation mockRestOp) throws IOException {
    MockClassUtils.mockAbfsRestOperationGetHttpOperation(
        (getHttpOpInvocationMock, getHttpOpObjects) -> {
          AbfsRestOperation op
              = (AbfsRestOperation) getHttpOpObjects[0];
          AbfsHttpOperation httpOperation = new AbfsHttpOperation(
              op.getUrl(), op.getMethod(), op.getRequestHeaders());
          AbfsHttpOperation spiedOp = Mockito.spy(httpOperation);
          setMockAbfsRestOperation(mockHttpOperationTestIntercept, spiedOp);
          return spiedOp;
        }, mockRestOp);
  }

  private void setMockAbfsRestOperation(final MockHttpOperationTestIntercept mockHttpOperationTestIntercept,
      final AbfsHttpOperation spiedOp) throws IOException {
    MockClassUtils.mockAbfsHttpOperationProcessResponse(
        (processResponseInvokation, processResponseObjs) -> {
          byte[] buffer = processResponseInvokation.getArgument(0,
              byte[].class);
          int offset = processResponseInvokation.getArgument(1,
              Integer.class);
          int length = processResponseInvokation.getArgument(2,
              Integer.class);

          AbfsHttpOperation abfsHttpOperation
              = (AbfsHttpOperation) processResponseObjs[0];

          MockHttpOperationTestInterceptResult result
              = mockHttpOperationTestIntercept.intercept(
              abfsHttpOperation, buffer, offset, length);
          MockClassUtils.setHttpOpStatus(result.getStatus(),
              abfsHttpOperation);
          MockClassUtils.setHttpOpBytesReceived(
              result.getBytesRead(), abfsHttpOperation);
          if (result.getException() != null) {
            throw result.getException();
          }
          return null;
        }, spiedOp);
  }

  private void callActualServerAndAssertBehaviour(final AbfsHttpOperation mockHttpOperation,
      final byte[] buffer,
      final int offset,
      final int length,
      final ActualServerReadByte actualServerReadByte,
      final int byteLenMockServerReturn, FSDataInputStream inputStream)
      throws IOException {
    LOG.info("length: " + length + "; offset: " + offset);
    Mockito.doCallRealMethod()
        .when(mockHttpOperation)
        .processResponse(Mockito.nullable(byte[].class),
            Mockito.nullable(Integer.class), Mockito.nullable(Integer.class));
    mockHttpOperation.processResponse(buffer, offset, length);
    int iterator = 0;
    int currPointer = actualServerReadByte.currPointer;
    while (actualServerReadByte.currPointer < actualServerReadByte.size
        && iterator < buffer.length) {
      actualServerReadByte.bytes[actualServerReadByte.currPointer++]
          = buffer[iterator++];
    }
    actualServerReadByte.currPointer = currPointer + byteLenMockServerReturn;
    Boolean isOriginalAndReceivedFileEqual = true;
    if (actualServerReadByte.currPointer == actualServerReadByte.size) {
      for (int i = 0; i < actualServerReadByte.size; i++) {
        if (actualServerReadByte.bytes[i]
            != actualServerReadByte.originalFile[i]) {
          isOriginalAndReceivedFileEqual = false;
          break;
        }
      }
      Assertions.assertThat(isOriginalAndReceivedFileEqual)
          .describedAs("Parsed data is not equal to original file")
          .isTrue();
    }
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
