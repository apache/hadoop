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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbfsCountersImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.ReadType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TimeoutException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.utils.TestCachedSASToken;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderVersion;
import org.apache.hadoop.fs.impl.OpenFileParameters;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COLON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SPLIT_NO_LIMIT;
import static org.apache.hadoop.fs.azurebfs.constants.ReadType.DIRECT_READ;
import static org.apache.hadoop.fs.azurebfs.constants.ReadType.FOOTER_READ;
import static org.apache.hadoop.fs.azurebfs.constants.ReadType.MISSEDCACHE_READ;
import static org.apache.hadoop.fs.azurebfs.constants.ReadType.NORMAL_READ;
import static org.apache.hadoop.fs.azurebfs.constants.ReadType.PREFETCH_READ;
import static org.apache.hadoop.fs.azurebfs.constants.ReadType.SMALLFILE_READ;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READ_AHEAD_QUEUE_DEPTH;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test AbfsInputStream.
 */
public class TestAbfsInputStream extends
    AbstractAbfsIntegrationTest {

  private static final int ONE_KB = 1 * 1024;
  private static final int TWO_KB = 2 * 1024;
  private static final int THREE_KB = 3 * 1024;
  private static final int SIXTEEN_KB = 16 * ONE_KB;
  private static final int FORTY_EIGHT_KB = 48 * ONE_KB;
  private static final int ONE_MB = 1 * 1024 * 1024;
  private static final int FOUR_MB = 4 * ONE_MB;
  private static final int EIGHT_MB = 8 * ONE_MB;
  private static final int TEST_READAHEAD_DEPTH_2 = 2;
  private static final int TEST_READAHEAD_DEPTH_4 = 4;
  private static final int REDUCED_READ_BUFFER_AGE_THRESHOLD = 3000; // 3 sec
  private static final int INCREASED_READ_BUFFER_AGE_THRESHOLD =
      REDUCED_READ_BUFFER_AGE_THRESHOLD * 10; // 30 sec
  private static final int ALWAYS_READ_BUFFER_SIZE_TEST_FILE_SIZE = 16 * ONE_MB;
  private static final int POSITION_INDEX = 9;
  private static final int OPERATION_INDEX = 6;
  private static final int READTYPE_INDEX = 11;

  @AfterEach
  @Override
  public void teardown() throws Exception {
    super.teardown();
    getBufferManager().testResetReadBufferManager();
  }

  private AbfsRestOperation getMockRestOp() {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsHttpOperation httpOp = mock(AbfsHttpOperation.class);
    when(httpOp.getBytesReceived()).thenReturn(1024L);
    when(op.getResult()).thenReturn(httpOp);
    when(op.getSasToken()).thenReturn(TestCachedSASToken.getTestCachedSASTokenInstance().get());
    return op;
  }

  private AbfsClient getMockAbfsClient() throws URISyntaxException {
    // Mock failure for client.read()
    AbfsClient client = mock(AbfsClient.class);
    AbfsCounters abfsCounters = Mockito.spy(new AbfsCountersImpl(new URI("abcd")));
    Mockito.doReturn(abfsCounters).when(client).getAbfsCounters();
    AbfsPerfTracker tracker = new AbfsPerfTracker(
        "test",
        this.getAccountName(),
        this.getConfiguration());
    when(client.getAbfsPerfTracker()).thenReturn(tracker);

    return client;
  }

  private AbfsInputStream getAbfsInputStream(AbfsClient mockAbfsClient,
      String fileName) throws IOException {
    AbfsInputStreamContext inputStreamContext = new AbfsInputStreamContext(-1);
    // Create AbfsInputStream with the client instance
    AbfsInputStream inputStream = new AbfsInputStream(
        mockAbfsClient,
        null,
        FORWARD_SLASH + fileName,
        THREE_KB,
        inputStreamContext.withReadBufferSize(ONE_KB).withReadAheadQueueDepth(10).withReadAheadBlockSize(ONE_KB),
        "eTag",
        getTestTracingContext(null, false));

    inputStream.setCachedSasToken(
        TestCachedSASToken.getTestCachedSASTokenInstance());

    return inputStream;
  }

  public AbfsInputStream getAbfsInputStream(AbfsClient abfsClient,
      String fileName,
      int fileSize,
      String eTag,
      int readAheadQueueDepth,
      int readBufferSize,
      boolean alwaysReadBufferSize,
      int readAheadBlockSize) throws IOException {
    AbfsInputStreamContext inputStreamContext = new AbfsInputStreamContext(-1);
    // Create AbfsInputStream with the client instance
    AbfsInputStream inputStream = new AbfsInputStream(
        abfsClient,
        null,
        FORWARD_SLASH + fileName,
        fileSize,
        inputStreamContext.withReadBufferSize(readBufferSize)
            .withReadAheadQueueDepth(readAheadQueueDepth)
            .withShouldReadBufferSizeAlways(alwaysReadBufferSize)
            .withReadAheadBlockSize(readAheadBlockSize),
        eTag,
        getTestTracingContext(getFileSystem(), false));

    inputStream.setCachedSasToken(
        TestCachedSASToken.getTestCachedSASTokenInstance());

    return inputStream;
  }

  private void queueReadAheads(AbfsInputStream inputStream) {
    // Mimic AbfsInputStream readAhead queue requests
    getBufferManager()
        .queueReadAhead(inputStream, 0, ONE_KB, inputStream.getTracingContext());
    getBufferManager()
        .queueReadAhead(inputStream, ONE_KB, ONE_KB,
            inputStream.getTracingContext());
    getBufferManager()
        .queueReadAhead(inputStream, TWO_KB, TWO_KB,
            inputStream.getTracingContext());
  }

  private void verifyReadCallCount(AbfsClient client, int count)
      throws IOException, InterruptedException {
    // ReadAhead threads are triggered asynchronously.
    // Wait a second before verifying the number of total calls.
    Thread.sleep(1000);
    verify(client, times(count)).read(any(String.class), any(Long.class),
        any(byte[].class), any(Integer.class), any(Integer.class),
        any(String.class), any(String.class), any(), any(TracingContext.class));
  }

  private void checkEvictedStatus(AbfsInputStream inputStream, int position, boolean expectedToThrowException)
      throws Exception {
    // Sleep for the eviction threshold time
    Thread.sleep(getBufferManager().getThresholdAgeMilliseconds() + 1000);

    // Eviction is done only when AbfsInputStream tries to queue new items.
    // 1 tryEvict will remove 1 eligible item. To ensure that the current test buffer
    // will get evicted (considering there could be other tests running in parallel),
    // call tryEvict for the number of items that are there in completedReadList.
    int numOfCompletedReadListItems = getBufferManager().getCompletedReadListSize();
    while (numOfCompletedReadListItems > 0) {
      getBufferManager().callTryEvict();
      numOfCompletedReadListItems--;
    }

    if (expectedToThrowException) {
      intercept(IOException.class,
          () -> inputStream.read(position, new byte[ONE_KB], 0, ONE_KB));
    } else {
      inputStream.read(position, new byte[ONE_KB], 0, ONE_KB);
    }
  }

  public TestAbfsInputStream() throws Exception {
    super();
    // Reduce thresholdAgeMilliseconds to 3 sec for the tests
    getBufferManager().setThresholdAgeMilliseconds(REDUCED_READ_BUFFER_AGE_THRESHOLD);
  }

  private void writeBufferToNewFile(Path testFile, byte[] buffer) throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    fs.create(testFile);
    FSDataOutputStream out = fs.append(testFile);
    out.write(buffer);
    out.close();
  }

  private void verifyOpenWithProvidedStatus(Path path, FileStatus fileStatus,
      byte[] buf, AbfsRestOperationType source)
      throws IOException, ExecutionException, InterruptedException {
    byte[] readBuf = new byte[buf.length];
    AzureBlobFileSystem fs = getFileSystem();
    FutureDataInputStreamBuilder builder = fs.openFile(path);
    builder.withFileStatus(fileStatus);
    FSDataInputStream in = builder.build().get();
    assertEquals(buf.length, in.read(readBuf),
        String.format("Open with fileStatus [from %s result]: Incorrect number of bytes read", source));
    assertArrayEquals(readBuf, buf,
        String.format("Open with fileStatus [from %s result]: Incorrect read data", source));
  }

  private void checkGetPathStatusCalls(Path testFile, FileStatus fileStatus,
      AzureBlobFileSystemStore abfsStore, AbfsClient mockClient,
      AbfsRestOperationType source, TracingContext tracingContext)
      throws IOException {

    // verify GetPathStatus not invoked when FileStatus is provided
    abfsStore.openFileForRead(testFile, Optional
        .ofNullable(new OpenFileParameters().withStatus(fileStatus)), null, tracingContext);
    verify(mockClient, times(0).description((String.format(
        "FileStatus [from %s result] provided, GetFileStatus should not be invoked",
        source)))).getPathStatus(anyString(), anyBoolean(), any(TracingContext.class), any(
        ContextEncryptionAdapter.class));

    // verify GetPathStatus invoked when FileStatus not provided
    abfsStore.openFileForRead(testFile,
        Optional.empty(), null,
        tracingContext);
    verify(mockClient, times(1).description(
        "GetPathStatus should be invoked when FileStatus not provided"))
        .getPathStatus(anyString(), anyBoolean(), any(TracingContext.class), nullable(
            ContextEncryptionAdapter.class));

    Mockito.reset(mockClient); //clears invocation count for next test case
  }

  @Test
  public void testOpenFileWithOptions() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    String testFolder = "/testFolder";
    Path smallTestFile = new Path(testFolder + "/testFile0");
    Path largeTestFile = new Path(testFolder + "/testFile1");
    fs.mkdirs(new Path(testFolder));
    int readBufferSize = getConfiguration().getReadBufferSize();
    byte[] smallBuffer = new byte[5];
    byte[] largeBuffer = new byte[readBufferSize + 5];
    new Random().nextBytes(smallBuffer);
    new Random().nextBytes(largeBuffer);
    writeBufferToNewFile(smallTestFile, smallBuffer);
    writeBufferToNewFile(largeTestFile, largeBuffer);

    FileStatus[] getFileStatusResults = {fs.getFileStatus(smallTestFile),
        fs.getFileStatus(largeTestFile)};
    FileStatus[] listStatusResults = fs.listStatus(new Path(testFolder));

    // open with fileStatus from GetPathStatus
    verifyOpenWithProvidedStatus(smallTestFile, getFileStatusResults[0],
        smallBuffer, AbfsRestOperationType.GetPathStatus);
    verifyOpenWithProvidedStatus(largeTestFile, getFileStatusResults[1],
        largeBuffer, AbfsRestOperationType.GetPathStatus);

    // open with fileStatus from ListStatus
    verifyOpenWithProvidedStatus(smallTestFile, listStatusResults[0], smallBuffer,
        AbfsRestOperationType.ListPaths);
    verifyOpenWithProvidedStatus(largeTestFile, listStatusResults[1], largeBuffer,
        AbfsRestOperationType.ListPaths);

    // verify number of GetPathStatus invocations
    AzureBlobFileSystemStore abfsStore = getAbfsStore(fs);
    AbfsClient mockClient = spy(getAbfsClient(abfsStore));
    setAbfsClient(abfsStore, mockClient);
    TracingContext tracingContext = getTestTracingContext(fs, false);
    checkGetPathStatusCalls(smallTestFile, getFileStatusResults[0],
        abfsStore, mockClient, AbfsRestOperationType.GetPathStatus, tracingContext);
    checkGetPathStatusCalls(largeTestFile, getFileStatusResults[1],
        abfsStore, mockClient, AbfsRestOperationType.GetPathStatus, tracingContext);
    checkGetPathStatusCalls(smallTestFile, listStatusResults[0],
        abfsStore, mockClient, AbfsRestOperationType.ListPaths, tracingContext);
    checkGetPathStatusCalls(largeTestFile, listStatusResults[1],
        abfsStore, mockClient, AbfsRestOperationType.ListPaths, tracingContext);

    // Verify with incorrect filestatus
    getFileStatusResults[0].setPath(new Path("wrongPath"));
    intercept(ExecutionException.class,
        () -> verifyOpenWithProvidedStatus(smallTestFile,
            getFileStatusResults[0], smallBuffer,
            AbfsRestOperationType.GetPathStatus));
  }

  /**
   * This test expects AbfsInputStream to throw the exception that readAhead
   * thread received on read. The readAhead thread must be initiated from the
   * active read request itself.
   * Also checks that the ReadBuffers are evicted as per the ReadBufferManager
   * threshold criteria.
   * @throws Exception
   */
  @Test
  public void testFailedReadAhead() throws Exception {
    AbfsClient client = getMockAbfsClient();
    AbfsRestOperation successOp = getMockRestOp();

    // Stub :
    // Read request leads to 3 readahead calls: Fail all 3 readahead-client.read()
    // Actual read request fails with the failure in readahead thread
    doThrow(new TimeoutException("Internal Server error for RAH-Thread-X"))
        .doThrow(new TimeoutException("Internal Server error for RAH-Thread-Y"))
        .doThrow(new TimeoutException("Internal Server error RAH-Thread-Z"))
        .doReturn(successOp) // Any extra calls to read, pass it.
        .when(client)
        .read(any(String.class), any(Long.class), any(byte[].class),
            any(Integer.class), any(Integer.class), any(String.class),
            any(String.class), any(), any(TracingContext.class));

    AbfsInputStream inputStream = getAbfsInputStream(client, "testFailedReadAhead.txt");

    // Scenario: ReadAhead triggered from current active read call failed
    // Before the change to return exception from readahead buffer,
    // AbfsInputStream would have triggered an extra readremote on noticing
    // data absent in readahead buffers
    // In this test, a read should trigger 3 client.read() calls as file is 3 KB
    // and readahead buffer size set in AbfsInputStream is 1 KB
    // There should only be a total of 3 client.read() in this test.
    intercept(IOException.class,
        () -> inputStream.read(new byte[ONE_KB]));

    // Only the 3 readAhead threads should have triggered client.read
    verifyReadCallCount(client, 3);

    // Stub returns success for the 4th read request, if ReadBuffers still
    // persisted, ReadAheadManager getBlock would have returned exception.
    checkEvictedStatus(inputStream, 0, false);
  }

  @Test
  public void testFailedReadAheadEviction() throws Exception {
    AbfsClient client = getMockAbfsClient();
    AbfsRestOperation successOp = getMockRestOp();
    getBufferManager().setThresholdAgeMilliseconds(INCREASED_READ_BUFFER_AGE_THRESHOLD);
    // Stub :
    // Read request leads to 3 readahead calls: Fail all 3 readahead-client.read()
    // Actual read request fails with the failure in readahead thread
    doThrow(new TimeoutException("Internal Server error"))
        .when(client)
        .read(any(String.class), any(Long.class), any(byte[].class),
            any(Integer.class), any(Integer.class), any(String.class),
            any(String.class), any(), any(TracingContext.class));

    AbfsInputStream inputStream = getAbfsInputStream(client, "testFailedReadAheadEviction.txt");

    // Add a failed buffer to completed queue and set to no free buffers to read ahead.
    ReadBuffer buff = new ReadBuffer();
    buff.setStatus(ReadBufferStatus.READ_FAILED);
    buff.setStream(inputStream);
    getBufferManager().testMimicFullUseAndAddFailedBuffer(buff);

    // if read failed buffer eviction is tagged as a valid eviction, it will lead to
    // wrong assumption of queue logic that a buffer is freed up and can lead to :
    // java.util.EmptyStackException
    // at java.util.Stack.peek(Stack.java:102)
    // at java.util.Stack.pop(Stack.java:84)
    // at org.apache.hadoop.fs.azurebfs.services.ReadBufferManager.queueReadAhead
    getBufferManager().queueReadAhead(inputStream, 0, ONE_KB,
        getTestTracingContext(getFileSystem(), true));
  }

  /**
   *
   * The test expects AbfsInputStream to initiate a remote read request for
   * the request offset and length when previous read ahead on the offset had failed.
   * Also checks that the ReadBuffers are evicted as per the ReadBufferManager
   * threshold criteria.
   * @throws Exception
   */
  @Test
  public void testOlderReadAheadFailure() throws Exception {
    AbfsClient client = getMockAbfsClient();
    AbfsRestOperation successOp = getMockRestOp();

    // Stub :
    // First Read request leads to 3 readahead calls: Fail all 3 readahead-client.read()
    // A second read request will see that readahead had failed for data in
    // the requested offset range and also that its is an older readahead request.
    // So attempt a new read only for the requested range.
    doThrow(new TimeoutException("Internal Server error for RAH-X"))
        .doThrow(new TimeoutException("Internal Server error for RAH-Y"))
        .doThrow(new TimeoutException("Internal Server error for RAH-Z"))
        .doReturn(successOp) // pass the read for second read request
        .doReturn(successOp) // pass success for post eviction test
        .when(client)
        .read(any(String.class), any(Long.class), any(byte[].class),
            any(Integer.class), any(Integer.class), any(String.class),
            any(String.class), any(), any(TracingContext.class));

    AbfsInputStream inputStream = getAbfsInputStream(client, "testOlderReadAheadFailure.txt");

    // First read request that fails as the readahead triggered from this request failed.
    intercept(IOException.class,
        () -> inputStream.read(new byte[ONE_KB]));

    // Only the 3 readAhead threads should have triggered client.read
    verifyReadCallCount(client, 3);

    // Sleep for thresholdAgeMs so that the read ahead buffer qualifies for being old.
    Thread.sleep(getBufferManager().getThresholdAgeMilliseconds());

    // Second read request should retry the read (and not issue any new readaheads)
    inputStream.read(ONE_KB, new byte[ONE_KB], 0, ONE_KB);

    // Once created, mock will remember all interactions. So total number of read
    // calls will be one more from earlier (there is a reset mock which will reset the
    // count, but the mock stub is erased as well which needs AbsInputStream to be recreated,
    // which beats the purpose)
    verifyReadCallCount(client, 4);

    // Stub returns success for the 5th read request, if ReadBuffers still
    // persisted request would have failed for position 0.
    checkEvictedStatus(inputStream, 0, false);
  }

  /**
   * The test expects AbfsInputStream to utilize any data read ahead for
   * requested offset and length.
   * @throws Exception
   */
  @Test
  public void testSuccessfulReadAhead() throws Exception {
    // Mock failure for client.read()
    AbfsClient client = getMockAbfsClient();

    // Success operation mock
    AbfsRestOperation op = getMockRestOp();

    // Stub :
    // Pass all readAheads and fail the post eviction request to
    // prove ReadAhead buffer is used
    // for post eviction check, fail all read aheads
    doReturn(op)
        .doReturn(op)
        .doReturn(op)
        .doThrow(new TimeoutException("Internal Server error for RAH-X"))
        .doThrow(new TimeoutException("Internal Server error for RAH-Y"))
        .doThrow(new TimeoutException("Internal Server error for RAH-Z"))
        .when(client)
        .read(any(String.class), any(Long.class), any(byte[].class),
            any(Integer.class), any(Integer.class), any(String.class),
            any(String.class), any(), any(TracingContext.class));

    AbfsInputStream inputStream = getAbfsInputStream(client, "testSuccessfulReadAhead.txt");
    int beforeReadCompletedListSize = getBufferManager().getCompletedReadListSize();

    // First read request that triggers readAheads.
    inputStream.read(new byte[ONE_KB]);

    // Only the 3 readAhead threads should have triggered client.read
    verifyReadCallCount(client, 3);
    int newAdditionsToCompletedRead =
        getBufferManager().getCompletedReadListSize()
            - beforeReadCompletedListSize;
    // read buffer might be dumped if the ReadBufferManager getblock preceded
    // the action of buffer being picked for reading from readaheadqueue, so that
    // inputstream can proceed with read and not be blocked on readahead thread
    // availability. So the count of buffers in completedReadQueue for the stream
    // can be same or lesser than the requests triggered to queue readahead.
    assertThat(newAdditionsToCompletedRead)
        .describedAs(
            "New additions to completed reads should be same or less than as number of readaheads")
        .isLessThanOrEqualTo(3);

    // Another read request whose requested data is already read ahead.
    inputStream.read(ONE_KB, new byte[ONE_KB], 0, ONE_KB);

    // Once created, mock will remember all interactions.
    // As the above read should not have triggered any server calls, total
    // number of read calls made at this point will be same as last.
    verifyReadCallCount(client, 3);

    // Stub will throw exception for client.read() for 4th and later calls
    // if not using the read-ahead buffer exception will be thrown on read
    checkEvictedStatus(inputStream, 0, true);
  }

  /**
   * This test expects InProgressList is not purged by the inputStream close.
   */
  @Test
  public void testStreamPurgeDuringReadAheadCallExecuting() throws Exception {
    AbfsClient client = getMockAbfsClient();
    AbfsRestOperation successOp = getMockRestOp();
    final Long serverCommunicationMockLatency = 3_000L;
    final Long readBufferTransferToInProgressProbableTime = 1_000L;
    final Integer readBufferQueuedCount = 3;

    Mockito.doAnswer(invocationOnMock -> {
          //sleeping thread to mock the network latency from client to backend.
          Thread.sleep(serverCommunicationMockLatency);
          return successOp;
        })
        .when(client)
        .read(any(String.class), any(Long.class), any(byte[].class),
            any(Integer.class), any(Integer.class), any(String.class),
            any(String.class), nullable(ContextEncryptionAdapter.class),
            any(TracingContext.class));

    final ReadBufferManager readBufferManager
        = getBufferManager();

    final int readBufferTotal = readBufferManager.getNumBuffers();
    final int expectedFreeListBufferCount = readBufferTotal
        - readBufferQueuedCount;

    try (AbfsInputStream inputStream = getAbfsInputStream(client,
        "testSuccessfulReadAhead.txt")) {
      // As this is try-with-resources block, the close() method of the created
      // abfsInputStream object shall be called on the end of the block.
      queueReadAheads(inputStream);

      //Sleeping to give ReadBufferWorker to pick the readBuffers for processing.
      Thread.sleep(readBufferTransferToInProgressProbableTime);

      assertThat(readBufferManager.getInProgressCopiedList())
          .describedAs(String.format("InProgressList should have %d elements",
              readBufferQueuedCount))
          .hasSize(readBufferQueuedCount);
      assertThat(readBufferManager.getFreeListCopy())
          .describedAs(String.format("FreeList should have %d elements",
              expectedFreeListBufferCount))
          .hasSize(expectedFreeListBufferCount);
      assertThat(readBufferManager.getCompletedReadListCopy())
          .describedAs("CompletedList should have 0 elements")
          .hasSize(0);
    }

    assertThat(readBufferManager.getInProgressCopiedList())
        .describedAs(String.format("InProgressList should have %d elements",
            readBufferQueuedCount))
        .hasSize(readBufferQueuedCount);
    assertThat(readBufferManager.getFreeListCopy())
        .describedAs(String.format("FreeList should have %d elements",
            expectedFreeListBufferCount))
        .hasSize(expectedFreeListBufferCount);
    assertThat(readBufferManager.getCompletedReadListCopy())
        .describedAs("CompletedList should have 0 elements")
        .hasSize(0);
  }

  /**
   * This test expects ReadAheadManager to throw exception if the read ahead
   * thread had failed within the last thresholdAgeMilliseconds.
   * Also checks that the ReadBuffers are evicted as per the ReadBufferManager
   * threshold criteria.
   * @throws Exception
   */
  @Test
  public void testReadAheadManagerForFailedReadAhead() throws Exception {
    AbfsClient client = getMockAbfsClient();
    AbfsRestOperation successOp = getMockRestOp();

    // Stub :
    // Read request leads to 3 readahead calls: Fail all 3 readahead-client.read()
    // Actual read request fails with the failure in readahead thread
    doThrow(new TimeoutException("Internal Server error for RAH-Thread-X"))
        .doThrow(new TimeoutException("Internal Server error for RAH-Thread-Y"))
        .doThrow(new TimeoutException("Internal Server error RAH-Thread-Z"))
        .doReturn(successOp) // Any extra calls to read, pass it.
        .when(client)
        .read(any(String.class), any(Long.class), any(byte[].class),
            any(Integer.class), any(Integer.class), any(String.class),
            any(String.class), any(), any(TracingContext.class));

    AbfsInputStream inputStream = getAbfsInputStream(client, "testReadAheadManagerForFailedReadAhead.txt");

    queueReadAheads(inputStream);

    // AbfsInputStream Read would have waited for the read-ahead for the requested offset
    // as we are testing from ReadAheadManager directly, sleep for a sec to
    // get the read ahead threads to complete
    Thread.sleep(1000);

    // if readAhead failed for specific offset, getBlock should
    // throw exception from the ReadBuffer that failed within last thresholdAgeMilliseconds sec
    intercept(IOException.class,
        () -> getBufferManager().getBlock(
            inputStream,
            0,
            ONE_KB,
            new byte[ONE_KB]));

    // Only the 3 readAhead threads should have triggered client.read
    verifyReadCallCount(client, 3);

    // Stub returns success for the 4th read request, if ReadBuffers still
    // persisted, ReadAheadManager getBlock would have returned exception.
    checkEvictedStatus(inputStream, 0, false);
  }

  /**
   * The test expects ReadAheadManager to return 0 receivedBytes when previous
   * read ahead on the offset had failed and not throw exception received then.
   * Also checks that the ReadBuffers are evicted as per the ReadBufferManager
   * threshold criteria.
   * @throws Exception
   */
  @Test
  public void testReadAheadManagerForOlderReadAheadFailure() throws Exception {
    AbfsClient client = getMockAbfsClient();
    AbfsRestOperation successOp = getMockRestOp();

    // Stub :
    // First Read request leads to 3 readahead calls: Fail all 3 readahead-client.read()
    // A second read request will see that readahead had failed for data in
    // the requested offset range but also that its is an older readahead request.
    // System issue could have resolved by now, so attempt a new read only for the requested range.
    doThrow(new TimeoutException("Internal Server error for RAH-X"))
        .doThrow(new TimeoutException("Internal Server error for RAH-X"))
        .doThrow(new TimeoutException("Internal Server error for RAH-X"))
        .doReturn(successOp) // pass the read for second read request
        .doReturn(successOp) // pass success for post eviction test
        .when(client)
        .read(any(String.class), any(Long.class), any(byte[].class),
            any(Integer.class), any(Integer.class), any(String.class),
            any(String.class), any(), any(TracingContext.class));

    AbfsInputStream inputStream = getAbfsInputStream(client, "testReadAheadManagerForOlderReadAheadFailure.txt");

    queueReadAheads(inputStream);

    // AbfsInputStream Read would have waited for the read-ahead for the requested offset
    // as we are testing from ReadAheadManager directly, sleep for thresholdAgeMilliseconds so that
    // read buffer qualifies for to be an old buffer
    Thread.sleep(getBufferManager().getThresholdAgeMilliseconds());

    // Only the 3 readAhead threads should have triggered client.read
    verifyReadCallCount(client, 3);

    // getBlock from a new read request should return 0 if there is a failure
    // 30 sec before in read ahead buffer for respective offset.
    int bytesRead = getBufferManager().getBlock(
        inputStream,
        ONE_KB,
        ONE_KB,
        new byte[ONE_KB]);
    Assertions.assertEquals(0, bytesRead,
        "bytesRead should be zero when previously read "+ "ahead buffer had failed");

    // Stub returns success for the 5th read request, if ReadBuffers still
    // persisted request would have failed for position 0.
    checkEvictedStatus(inputStream, 0, false);
  }

  /**
   * The test expects ReadAheadManager to return data from previously read
   * ahead data of same offset.
   * @throws Exception
   */
  @Test
  public void testReadAheadManagerForSuccessfulReadAhead() throws Exception {
    // Mock failure for client.read()
    AbfsClient client = getMockAbfsClient();

    // Success operation mock
    AbfsRestOperation op = getMockRestOp();

    // Stub :
    // Pass all readAheads and fail the post eviction request to
    // prove ReadAhead buffer is used
    doReturn(op)
        .doReturn(op)
        .doReturn(op)
        .doThrow(new TimeoutException("Internal Server error for RAH-X")) // for post eviction request
        .doThrow(new TimeoutException("Internal Server error for RAH-Y"))
        .doThrow(new TimeoutException("Internal Server error for RAH-Z"))
        .when(client)
        .read(any(String.class), any(Long.class), any(byte[].class),
            any(Integer.class), any(Integer.class), any(String.class),
            any(String.class), any(), any(TracingContext.class));

    AbfsInputStream inputStream = getAbfsInputStream(client, "testSuccessfulReadAhead.txt");

    queueReadAheads(inputStream);

    // AbfsInputStream Read would have waited for the read-ahead for the requested offset
    // as we are testing from ReadAheadManager directly, sleep for a sec to
    // get the read ahead threads to complete
    Thread.sleep(1000);

    // Only the 3 readAhead threads should have triggered client.read
    verifyReadCallCount(client, 3);

    // getBlock for a new read should return the buffer read-ahead
    int bytesRead = getBufferManager().getBlock(
        inputStream,
        ONE_KB,
        ONE_KB,
        new byte[ONE_KB]);

    Assertions.assertTrue(bytesRead > 0, "bytesRead should be non-zero from the "
        + "buffer that was read-ahead");

    // Once created, mock will remember all interactions.
    // As the above read should not have triggered any server calls, total
    // number of read calls made at this point will be same as last.
    verifyReadCallCount(client, 3);

    // Stub will throw exception for client.read() for 4th and later calls
    // if not using the read-ahead buffer exception will be thrown on read
    checkEvictedStatus(inputStream, 0, true);
  }

  /**
   * Test readahead with different config settings for request request size and
   * readAhead block size
   * @throws Exception
   */
  @Test
  public void testDiffReadRequestSizeAndRAHBlockSize() throws Exception {
    // Set requestRequestSize = 4MB and readAheadBufferSize=8MB
    resetReadBufferManager(FOUR_MB, INCREASED_READ_BUFFER_AGE_THRESHOLD);
    testReadAheadConfigs(FOUR_MB, TEST_READAHEAD_DEPTH_4, false, EIGHT_MB);

    // Test for requestRequestSize =16KB and readAheadBufferSize=16KB
    resetReadBufferManager(SIXTEEN_KB, INCREASED_READ_BUFFER_AGE_THRESHOLD);
    AbfsInputStream inputStream = testReadAheadConfigs(SIXTEEN_KB,
        TEST_READAHEAD_DEPTH_2, true, SIXTEEN_KB);
    testReadAheads(inputStream, SIXTEEN_KB, SIXTEEN_KB);

    // Test for requestRequestSize =16KB and readAheadBufferSize=48KB
    resetReadBufferManager(FORTY_EIGHT_KB, INCREASED_READ_BUFFER_AGE_THRESHOLD);
    inputStream = testReadAheadConfigs(SIXTEEN_KB, TEST_READAHEAD_DEPTH_2, true,
        FORTY_EIGHT_KB);
    testReadAheads(inputStream, SIXTEEN_KB, FORTY_EIGHT_KB);

    // Test for requestRequestSize =48KB and readAheadBufferSize=16KB
    resetReadBufferManager(FORTY_EIGHT_KB, INCREASED_READ_BUFFER_AGE_THRESHOLD);
    inputStream = testReadAheadConfigs(FORTY_EIGHT_KB, TEST_READAHEAD_DEPTH_2,
        true,
        SIXTEEN_KB);
    testReadAheads(inputStream, FORTY_EIGHT_KB, SIXTEEN_KB);
  }

  @Test
  public void testDefaultReadaheadQueueDepth() throws Exception {
    Configuration config = getRawConfiguration();
    config.unset(FS_AZURE_READ_AHEAD_QUEUE_DEPTH);
    AzureBlobFileSystem fs = getFileSystem(config);
    Path testFile = path("/testFile");
    fs.create(testFile).close();
    FSDataInputStream in = fs.open(testFile);
    assertThat(
        ((AbfsInputStream) in.getWrappedStream()).getReadAheadQueueDepth())
        .describedAs("readahead queue depth should be set to default value 2")
        .isEqualTo(2);
    in.close();
  }

  /**
   * Test to verify that the read type and position are correctly set in the
   * client request id header for various type of read operations performed.
   * @throws Exception if any error occurs during the test
   */
  @Test
  public void testReadTypeInTracingContextHeader() throws Exception {
    AzureBlobFileSystem spiedFs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore spiedStore = Mockito.spy(spiedFs.getAbfsStore());
    AbfsConfiguration spiedConfig = Mockito.spy(spiedStore.getAbfsConfiguration());
    AbfsClient spiedClient = Mockito.spy(spiedStore.getClient());
    Mockito.doReturn(ONE_MB).when(spiedConfig).getReadBufferSize();
    Mockito.doReturn(ONE_MB).when(spiedConfig).getReadAheadBlockSize();
    Mockito.doReturn(spiedClient).when(spiedStore).getClient();
    Mockito.doReturn(spiedStore).when(spiedFs).getAbfsStore();
    Mockito.doReturn(spiedConfig).when(spiedStore).getAbfsConfiguration();
    int totalReadCalls = 0;
    int fileSize;

    /*
     * Test to verify Normal Read Type.
     * Disabling read ahead ensures that read type is normal read.
     */
    fileSize = 3 * ONE_MB; // To make sure multiple blocks are read.
    totalReadCalls += 3; // 3 blocks of 1MB each.
    doReturn(false).when(spiedConfig).isReadAheadV2Enabled();
    doReturn(false).when(spiedConfig).isReadAheadEnabled();
    testReadTypeInTracingContextHeaderInternal(spiedFs, fileSize, NORMAL_READ, 3, totalReadCalls);

    /*
     * Test to verify Missed Cache Read Type.
     * Setting read ahead depth to 0 ensure that nothing can be got from prefetch.
     * In such a case Input Stream will do a sequential read with missed cache read type.
     */
    fileSize = 3 * ONE_MB; // To make sure multiple blocks are read with MR
    totalReadCalls += 3; // 3 block of 1MB.
    Mockito.doReturn(0).when(spiedConfig).getReadAheadQueueDepth();
    doReturn(true).when(spiedConfig).isReadAheadEnabled();
    testReadTypeInTracingContextHeaderInternal(spiedFs, fileSize, MISSEDCACHE_READ, 3, totalReadCalls);

    /*
     * Test to verify Prefetch Read Type.
     * Setting read ahead depth to 2 with prefetch enabled ensures that prefetch is done.
     * First read here might be Normal or Missed Cache but the rest 2 should be Prefetched Read.
     */
    fileSize = 3 * ONE_MB; // To make sure multiple blocks are read.
    totalReadCalls += 3;
    doReturn(true).when(spiedConfig).isReadAheadEnabled();
    Mockito.doReturn(3).when(spiedConfig).getReadAheadQueueDepth();
    testReadTypeInTracingContextHeaderInternal(spiedFs, fileSize, PREFETCH_READ, 3, totalReadCalls);

    /*
     * Test to verify Footer Read Type.
     * Having file size less than footer read size and disabling small file opt
     */
    fileSize = 8 * ONE_KB;
    totalReadCalls += 1; // Full file will be read along with footer.
    doReturn(false).when(spiedConfig).readSmallFilesCompletely();
    doReturn(true).when(spiedConfig).optimizeFooterRead();
    testReadTypeInTracingContextHeaderInternal(spiedFs, fileSize, FOOTER_READ, 1, totalReadCalls);

    /*
     * Test to verify Small File Read Type.
     * Having file size less than block size and disabling footer read opt
     */
    totalReadCalls += 1; // Full file will be read along with footer.
    doReturn(true).when(spiedConfig).readSmallFilesCompletely();
    doReturn(false).when(spiedConfig).optimizeFooterRead();
    testReadTypeInTracingContextHeaderInternal(spiedFs, fileSize, SMALLFILE_READ, 1, totalReadCalls);

    /*
     * Test to verify Direct Read Type and a read from random position.
     * Separate AbfsInputStream method needs to be called.
     */
    fileSize = ONE_MB;
    totalReadCalls += 1;
    doReturn(false).when(spiedConfig).readSmallFilesCompletely();
    doReturn(true).when(spiedConfig).isBufferedPReadDisabled();
    Path testPath = createTestFile(spiedFs, fileSize);
    try (FSDataInputStream iStream = spiedFs.open(testPath)) {
      AbfsInputStream stream = (AbfsInputStream) iStream.getWrappedStream();
      int bytesRead = stream.read(ONE_MB/3, new byte[fileSize], 0,
          fileSize);
      assertThat(fileSize - ONE_MB/3)
          .describedAs("Read size should match file size")
          .isEqualTo(bytesRead);
    }
    assertReadTypeInClientRequestId(spiedFs, 1, totalReadCalls, DIRECT_READ);
  }

  private void testReadTypeInTracingContextHeaderInternal(AzureBlobFileSystem fs,
      int fileSize, ReadType readType, int numOfReadCalls, int totalReadCalls) throws Exception {
    Path testPath = createTestFile(fs, fileSize);
    readFile(fs, testPath, fileSize);
    assertReadTypeInClientRequestId(fs, numOfReadCalls, totalReadCalls, readType);
  }

  private Path createTestFile(AzureBlobFileSystem fs, int fileSize) throws Exception {
    Path testPath = new Path("testFile");
    byte[] fileContent = getRandomBytesArray(fileSize);
    try (FSDataOutputStream oStream = fs.create(testPath)) {
      oStream.write(fileContent);
      oStream.flush();
    }
    return testPath;
  }

  private void readFile(AzureBlobFileSystem fs, Path testPath, int fileSize) throws Exception {
    try (FSDataInputStream iStream = fs.open(testPath)) {
      int bytesRead = iStream.read(new byte[fileSize], 0,
          fileSize);
      assertThat(fileSize)
          .describedAs("Read size should match file size")
          .isEqualTo(bytesRead);
    }
  }

  private void assertReadTypeInClientRequestId(AzureBlobFileSystem fs, int numOfReadCalls,
      int totalReadCalls, ReadType readType) throws Exception {
    ArgumentCaptor<String> captor1 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> captor2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<byte[]> captor3 = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> captor4 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> captor5 = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<String> captor6 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> captor7 = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<ContextEncryptionAdapter> captor8 = ArgumentCaptor.forClass(ContextEncryptionAdapter.class);
    ArgumentCaptor<TracingContext> captor9 = ArgumentCaptor.forClass(TracingContext.class);

    verify(fs.getAbfsStore().getClient(), times(totalReadCalls)).read(
        captor1.capture(), captor2.capture(), captor3.capture(),
        captor4.capture(), captor5.capture(), captor6.capture(),
        captor7.capture(), captor8.capture(), captor9.capture());
    List<TracingContext> tracingContextList = captor9.getAllValues();
    if (readType == PREFETCH_READ) {
      /*
       * For Prefetch Enabled, first read can be Normal or Missed Cache Read.
       * So we will assert only for last 2 calls which should be Prefetched Read.
       * Since calls are asynchronous, we can not guarantee the order of calls.
       * Therefore, we cannot assert on exact position here.
       */
      for (int i = tracingContextList.size() - (numOfReadCalls - 1); i < tracingContextList.size(); i++) {
        verifyHeaderForReadTypeInTracingContextHeader(tracingContextList.get(i), readType, -1);
      }
    } else if (readType == DIRECT_READ) {
      int expectedReadPos = ONE_MB/3;
      for (int i = tracingContextList.size() - numOfReadCalls; i < tracingContextList.size(); i++) {
        verifyHeaderForReadTypeInTracingContextHeader(tracingContextList.get(i), readType, expectedReadPos);
        expectedReadPos += ONE_MB;
      }
    } else {
      int expectedReadPos = 0;
      for (int i = tracingContextList.size() - numOfReadCalls; i < tracingContextList.size(); i++) {
        verifyHeaderForReadTypeInTracingContextHeader(tracingContextList.get(i), readType, expectedReadPos);
        expectedReadPos += ONE_MB;
      }
    }
  }

  private void verifyHeaderForReadTypeInTracingContextHeader(TracingContext tracingContext, ReadType readType, int expectedReadPos) {
    AbfsHttpOperation mockOp = Mockito.mock(AbfsHttpOperation.class);
    doReturn(EMPTY_STRING).when(mockOp).getTracingContextSuffix();
    tracingContext.constructHeader(mockOp, null, null);
    String[] idList = tracingContext.getHeader().split(COLON, SPLIT_NO_LIMIT);
    assertThat(idList).describedAs("Client Request Id should have all fields").hasSize(
        TracingHeaderVersion.getCurrentVersion().getFieldCount());
    if (expectedReadPos > 0) {
      assertThat(idList[POSITION_INDEX])
          .describedAs("Read Position should match")
          .isEqualTo(Integer.toString(expectedReadPos));
    }
    assertThat(idList[OPERATION_INDEX]).describedAs("Operation Type Should Be Read")
        .isEqualTo(FSOperationType.READ.toString());
    assertThat(idList[READTYPE_INDEX]).describedAs("Read type in tracing context header should match")
        .isEqualTo(readType.toString());
  }


  private void testReadAheads(AbfsInputStream inputStream,
      int readRequestSize,
      int readAheadRequestSize)
      throws Exception {
    if (readRequestSize > readAheadRequestSize) {
      readAheadRequestSize = readRequestSize;
    }

    byte[] firstReadBuffer = new byte[readRequestSize];
    byte[] secondReadBuffer = new byte[readAheadRequestSize];

    // get the expected bytes to compare
    byte[] expectedFirstReadAheadBufferContents = new byte[readRequestSize];
    byte[] expectedSecondReadAheadBufferContents = new byte[readAheadRequestSize];
    getExpectedBufferData(0, readRequestSize, expectedFirstReadAheadBufferContents);
    getExpectedBufferData(readRequestSize, readAheadRequestSize,
        expectedSecondReadAheadBufferContents);

    assertThat(inputStream.read(firstReadBuffer, 0, readRequestSize))
        .describedAs("Read should be of exact requested size")
        .isEqualTo(readRequestSize);

    assertTrue(
       Arrays.equals(firstReadBuffer,
            expectedFirstReadAheadBufferContents), "Data mismatch found in RAH1");

    assertThat(inputStream.read(secondReadBuffer, 0, readAheadRequestSize))
        .describedAs("Read should be of exact requested size")
        .isEqualTo(readAheadRequestSize);

    assertTrue(
       Arrays.equals(secondReadBuffer,
            expectedSecondReadAheadBufferContents), "Data mismatch found in RAH2");
  }

  public AbfsInputStream testReadAheadConfigs(int readRequestSize,
      int readAheadQueueDepth,
      boolean alwaysReadBufferSizeEnabled,
      int readAheadBlockSize) throws Exception {
    Configuration
        config = new Configuration(
        this.getRawConfiguration());
    config.set("fs.azure.read.request.size", Integer.toString(readRequestSize));
    config.set("fs.azure.readaheadqueue.depth",
        Integer.toString(readAheadQueueDepth));
    config.set("fs.azure.read.alwaysReadBufferSize",
        Boolean.toString(alwaysReadBufferSizeEnabled));
    config.set("fs.azure.read.readahead.blocksize",
        Integer.toString(readAheadBlockSize));
    if (readRequestSize > readAheadBlockSize) {
      readAheadBlockSize = readRequestSize;
    }

    Path testPath = path("/testReadAheadConfigs");
    final AzureBlobFileSystem fs = createTestFile(testPath,
        ALWAYS_READ_BUFFER_SIZE_TEST_FILE_SIZE, config);
    byte[] byteBuffer = new byte[ONE_MB];
    AbfsInputStream inputStream = this.getAbfsStore(fs)
        .openFileForRead(testPath, null, getTestTracingContext(fs, false));

    assertThat(inputStream.getBufferSize())
        .describedAs("Unexpected AbfsInputStream buffer size")
        .isEqualTo(readRequestSize);

    assertThat(inputStream.getReadAheadQueueDepth())
        .describedAs("Unexpected ReadAhead queue depth")
        .isEqualTo(readAheadQueueDepth);

    assertThat(inputStream.shouldAlwaysReadBufferSize())
        .describedAs("Unexpected AlwaysReadBufferSize settings")
        .isEqualTo(alwaysReadBufferSizeEnabled);

    assertThat(getBufferManager().getReadAheadBlockSize())
        .describedAs("Unexpected readAhead block size")
        .isEqualTo(readAheadBlockSize);

    return inputStream;
  }

  private void getExpectedBufferData(int offset, int length, byte[] b) {
    boolean startFillingIn = false;
    int indexIntoBuffer = 0;
    char character = 'a';

    for (int i = 0; i < (offset + length); i++) {
      if (i == offset) {
        startFillingIn = true;
      }

      if ((startFillingIn) && (indexIntoBuffer < length)) {
        b[indexIntoBuffer] = (byte) character;
        indexIntoBuffer++;
      }

      character = (character == 'z') ? 'a' : (char) ((int) character + 1);
    }
  }

  private AzureBlobFileSystem createTestFile(Path testFilePath, long testFileSize,
      Configuration config) throws Exception {
    AzureBlobFileSystem fs;

    if (config == null) {
      fs = this.getFileSystem();
    } else {
      final AzureBlobFileSystem currentFs = getFileSystem();
      fs = (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(),
          config);
    }

    if (fs.exists(testFilePath)) {
      FileStatus status = fs.getFileStatus(testFilePath);
      if (status.getLen() >= testFileSize) {
        return fs;
      }
    }

    byte[] buffer = new byte[EIGHT_MB];
    char character = 'a';
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = (byte) character;
      character = (character == 'z') ? 'a' : (char) ((int) character + 1);
    }

    try (FSDataOutputStream outputStream = fs.create(testFilePath)) {
      int bytesWritten = 0;
      while (bytesWritten < testFileSize) {
        outputStream.write(buffer);
        bytesWritten += buffer.length;
      }
    }

    assertThat(fs.getFileStatus(testFilePath).getLen())
        .describedAs("File not created of expected size")
        .isEqualTo(testFileSize);

    return fs;
  }

  private void resetReadBufferManager(int bufferSize, int threshold) {
    getBufferManager()
        .testResetReadBufferManager(bufferSize, threshold);
    // Trigger GC as aggressive recreation of ReadBufferManager buffers
    // by successive tests can lead to OOM based on the dev VM/machine capacity.
    System.gc();
  }

  private ReadBufferManager getBufferManager() {
    return ReadBufferManagerV1.getBufferManager();
  }
}
