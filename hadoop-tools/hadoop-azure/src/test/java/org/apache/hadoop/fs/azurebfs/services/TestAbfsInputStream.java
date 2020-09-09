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

import org.junit.Assert;
import org.junit.Test;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TimeoutException;
import org.apache.hadoop.fs.azurebfs.utils.TestCachedSASToken;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;

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
  private static final int READAHEAD_BUFFER_COUNT = 16;
  private static final int TEST_READAHEAD_BUFFER_COUNT_4 = 4;
  private static final int READAHEAD_DEPTH = 10;
  private static final int TEST_READAHEAD_DEPTH_2 = 2;
  private static final int TEST_READAHEAD_DEPTH_4 = 4;
  private static final int REDUCED_READ_BUFFER_AGE_THRESHOLD = 3000; // 3 sec
  private static final int INCREASED_READ_BUFFER_AGE_THRESHOLD = 30000; // 30 sec
  private static final int ALWAYS_READ_BUFFER_SIZE_TEST_FILE_SIZE = 16 * ONE_MB;

  private AbfsRestOperation getMockRestOp() {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsHttpOperation httpOp = mock(AbfsHttpOperation.class);
    when(httpOp.getBytesReceived()).thenReturn(1024L);
    when(op.getResult()).thenReturn(httpOp);
    when(op.getSasToken()).thenReturn(TestCachedSASToken.getTestCachedSASTokenInstance().get());
    return op;
  }

  private AbfsClient getMockAbfsClient() {
    // Mock failure for client.read()
    AbfsClient client = mock(AbfsClient.class);
    AbfsPerfTracker tracker = new AbfsPerfTracker(
        "test",
        this.getAccountName(),
        this.getConfiguration());
    when(client.getAbfsPerfTracker()).thenReturn(tracker);

    return client;
  }

  public AbfsInputStream getAbfsInputStream(AbfsClient abfsClient,
      String fileName,
      int fileSize,
      String eTag,
      int readAheadQueueDepth,
      int readBufferSize,
      boolean alwaysReadBufferSize,
      int readAheadBlockSize,
      int readAheadBufferCount) {
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
            .withReadAheadBlockSize(readAheadBlockSize)
            .withReadAheadBufferCount(readAheadBufferCount),
        eTag);

    inputStream.setCachedSasToken(
        TestCachedSASToken.getTestCachedSASTokenInstance());

    return inputStream;
  }

  private void queueReadAheads(AbfsInputStream inputStream) {
    // Mimic AbfsInputStream readAhead queue requests
    ReadBufferManager.getBufferManager()
        .queueReadAhead(inputStream, 0, ONE_KB);
    ReadBufferManager.getBufferManager()
        .queueReadAhead(inputStream, ONE_KB, ONE_KB);
    ReadBufferManager.getBufferManager()
        .queueReadAhead(inputStream, TWO_KB, TWO_KB);
  }

  private void verifyReadCallCount(AbfsClient client, int count) throws
      AzureBlobFileSystemException, InterruptedException {
    // ReadAhead threads are triggered asynchronously.
    // Wait a second before verifying the number of total calls.
    Thread.sleep(1000);
    verify(client, times(count)).read(any(String.class), any(Long.class),
        any(byte[].class), any(Integer.class), any(Integer.class),
        any(String.class), any(String.class));
  }

  private void checkEvictedStatus(AbfsInputStream inputStream, int position, boolean expectedToThrowException)
      throws Exception {
    // Sleep for the eviction threshold time
    Thread.sleep(ReadBufferManager.getBufferManager().getThresholdAgeMilliseconds() + 1000);

    // Eviction is done only when AbfsInputStream tries to queue new items.
    // 1 tryEvict will remove 1 eligible item. To ensure that the current test buffer
    // will get evicted (considering there could be other tests running in parallel),
    // call tryEvict for the number of items that are there in completedReadList.
    int numOfCompletedReadListItems = ReadBufferManager.getBufferManager().getCompletedReadListSize();
    while (numOfCompletedReadListItems > 0) {
      ReadBufferManager.getBufferManager().callTryEvict();
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
    ReadBufferManager.getBufferManager().setThresholdAgeMilliseconds(REDUCED_READ_BUFFER_AGE_THRESHOLD);
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
            any(String.class));

    AbfsInputStream inputStream = getAbfsInputStream(client,
        "testFailedReadAhead.txt", THREE_KB, "eTag", READAHEAD_DEPTH, ONE_KB,
        false, ONE_KB, READAHEAD_BUFFER_COUNT);

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

  /**
   * The test expects AbfsInputStream to initiate a remote read request for
   * the request offset and length when previous read ahead on the offset had failed.
   * Also checks that the ReadBuffers are evicted as per the ReadBufferManager
   * threshold criteria.
   * @throws Exception
   */
  @Test
  public void testOlderReadAheadFailure() throws Exception {
    ReadBufferManager.getBufferManager().setThresholdAgeMilliseconds(REDUCED_READ_BUFFER_AGE_THRESHOLD);
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
            any(String.class));

    AbfsInputStream inputStream = getAbfsInputStream(client,
        "testOlderReadAheadFailure.txt", THREE_KB, "eTag", READAHEAD_DEPTH, ONE_KB,
        false, ONE_KB, READAHEAD_BUFFER_COUNT);

    // First read request that fails as the readahead triggered from this request failed.
    intercept(IOException.class,
        () -> inputStream.read(new byte[ONE_KB]));

    // Only the 3 readAhead threads should have triggered client.read
    verifyReadCallCount(client, 3);

    // Sleep for thresholdAgeMs so that the read ahead buffer qualifies for being old.
    Thread.sleep(ReadBufferManager.getBufferManager().getThresholdAgeMilliseconds());

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
    ReadBufferManager.getBufferManager().setThresholdAgeMilliseconds(REDUCED_READ_BUFFER_AGE_THRESHOLD);
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
            any(String.class));

    AbfsInputStream inputStream = getAbfsInputStream(client,
        "testSuccessfulReadAhead.txt", THREE_KB, "eTag", READAHEAD_DEPTH, ONE_KB,
        false, ONE_KB, READAHEAD_BUFFER_COUNT);

    // First read request that triggers readAheads.
    inputStream.read(new byte[ONE_KB]);

    // Only the 3 readAhead threads should have triggered client.read
    verifyReadCallCount(client, 3);

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
   * This test expects ReadAheadManager to throw exception if the read ahead
   * thread had failed within the last thresholdAgeMilliseconds.
   * Also checks that the ReadBuffers are evicted as per the ReadBufferManager
   * threshold criteria.
   * @throws Exception
   */
  @Test
  public void testReadAheadManagerForFailedReadAhead() throws Exception {
    ReadBufferManager.getBufferManager().setThresholdAgeMilliseconds(REDUCED_READ_BUFFER_AGE_THRESHOLD);
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
            any(String.class));

    AbfsInputStream inputStream = getAbfsInputStream(client,
        "testReadAheadManagerForFailedReadAhead.txt", THREE_KB, "eTag", READAHEAD_DEPTH, ONE_KB,
        false, FOUR_MB, READAHEAD_BUFFER_COUNT);

    queueReadAheads(inputStream);

    // AbfsInputStream Read would have waited for the read-ahead for the requested offset
    // as we are testing from ReadAheadManager directly, sleep for a sec to
    // get the read ahead threads to complete
    Thread.sleep(1000);

    // if readAhead failed for specific offset, getBlock should
    // throw exception from the ReadBuffer that failed within last thresholdAgeMilliseconds sec
    intercept(IOException.class,
        () -> ReadBufferManager.getBufferManager().getBlock(
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
    ReadBufferManager.getBufferManager().setThresholdAgeMilliseconds(REDUCED_READ_BUFFER_AGE_THRESHOLD);
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
            any(String.class));

    AbfsInputStream inputStream = getAbfsInputStream(client,
        "testReadAheadManagerForOlderReadAheadFailure.txt", THREE_KB, "eTag", READAHEAD_DEPTH, ONE_KB,
        false, FOUR_MB, READAHEAD_BUFFER_COUNT);

    queueReadAheads(inputStream);

    // AbfsInputStream Read would have waited for the read-ahead for the requested offset
    // as we are testing from ReadAheadManager directly, sleep for thresholdAgeMilliseconds so that
    // read buffer qualifies for to be an old buffer
    Thread.sleep(ReadBufferManager.getBufferManager().getThresholdAgeMilliseconds());

    // Only the 3 readAhead threads should have triggered client.read
    verifyReadCallCount(client, 3);

    // getBlock from a new read request should return 0 if there is a failure
    // 30 sec before in read ahead buffer for respective offset.
    int bytesRead = ReadBufferManager.getBufferManager().getBlock(
        inputStream,
        ONE_KB,
        ONE_KB,
        new byte[ONE_KB]);
    Assert.assertEquals("bytesRead should be zero when previously read "
        + "ahead buffer had failed", 0, bytesRead);

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
    ReadBufferManager.getBufferManager().setThresholdAgeMilliseconds(REDUCED_READ_BUFFER_AGE_THRESHOLD);
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
            any(String.class));

    AbfsInputStream inputStream = getAbfsInputStream(client,
        "testSuccessfulReadAhead.txt", THREE_KB, "eTag", READAHEAD_DEPTH, ONE_KB,
        false, FOUR_MB, READAHEAD_BUFFER_COUNT);

    queueReadAheads(inputStream);

    // AbfsInputStream Read would have waited for the read-ahead for the requested offset
    // as we are testing from ReadAheadManager directly, sleep for a sec to
    // get the read ahead threads to complete
    Thread.sleep(1000);

    // Only the 3 readAhead threads should have triggered client.read
    verifyReadCallCount(client, 3);

    // getBlock for a new read should return the buffer read-ahead
    int bytesRead = ReadBufferManager.getBufferManager().getBlock(
        inputStream,
        ONE_KB,
        ONE_KB,
        new byte[ONE_KB]);

    Assert.assertTrue("bytesRead should be non-zero from the "
        + "buffer that was read-ahead", bytesRead > 0);

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
    ReadBufferManager.getBufferManager()
        .testResetReadBufferManager(FOUR_MB, EIGHT_MB, INCREASED_READ_BUFFER_AGE_THRESHOLD);
    testReadAheadConfigs(FOUR_MB, TEST_READAHEAD_DEPTH_4, false, EIGHT_MB,
        READAHEAD_BUFFER_COUNT);

    // Test for requestRequestSize =16KB and readAheadBufferSize=16KB
    ReadBufferManager.getBufferManager()
        .testResetReadBufferManager(SIXTEEN_KB, TEST_READAHEAD_BUFFER_COUNT_4, INCREASED_READ_BUFFER_AGE_THRESHOLD);
    AbfsInputStream inputStream = testReadAheadConfigs(SIXTEEN_KB,
        TEST_READAHEAD_DEPTH_2, true, SIXTEEN_KB,
        TEST_READAHEAD_BUFFER_COUNT_4);
    testReadAheads(inputStream, SIXTEEN_KB, SIXTEEN_KB);

    // Test for requestRequestSize =16KB and readAheadBufferSize=48KB
    ReadBufferManager.getBufferManager()
        .testResetReadBufferManager(FORTY_EIGHT_KB,
            TEST_READAHEAD_BUFFER_COUNT_4, INCREASED_READ_BUFFER_AGE_THRESHOLD);
    inputStream = testReadAheadConfigs(SIXTEEN_KB, TEST_READAHEAD_DEPTH_2, true,
        FORTY_EIGHT_KB, TEST_READAHEAD_BUFFER_COUNT_4);
    testReadAheads(inputStream, SIXTEEN_KB, FORTY_EIGHT_KB);

    // Test for requestRequestSize =48KB and readAheadBufferSize=16KB
    ReadBufferManager.getBufferManager()
        .testResetReadBufferManager(SIXTEEN_KB, TEST_READAHEAD_BUFFER_COUNT_4, INCREASED_READ_BUFFER_AGE_THRESHOLD);
    inputStream = testReadAheadConfigs(FORTY_EIGHT_KB, TEST_READAHEAD_DEPTH_2,
        true,
        SIXTEEN_KB, TEST_READAHEAD_BUFFER_COUNT_4);
    testReadAheads(inputStream, FORTY_EIGHT_KB, SIXTEEN_KB);
  }

  private void testReadAheads(AbfsInputStream inputStream,
      int readRequestSize,
      int readAheadRequestSize)
      throws Exception {
    // trigger a read for 1 KB on 2 KB buffer
    byte[] twoKBBuffer = new byte[TWO_KB];
    assertTrue("Read should be of exact requested size",
        inputStream.read(twoKBBuffer, 0, ONE_KB) == ONE_KB);
    if (readRequestSize > readAheadRequestSize) {
      readRequestSize = readAheadRequestSize;
    }

    // get the expected bytes to compare
    byte[] expected_RAH1_contents = new byte[readRequestSize];
    byte[] expected_RAH2_contents = new byte[readAheadRequestSize];
    getExpectedBufferData(0, readRequestSize, expected_RAH1_contents);
    getExpectedBufferData(readRequestSize, readAheadRequestSize,
        expected_RAH2_contents);

    // Sleep for the readaheads to be complete
    Thread.sleep(5000);

    // Fetch RAH buffer for first read.
    ReadBuffer firstReadBuffer = ReadBufferManager.getBufferManager()
        .getBuffer(inputStream, 0);
    assertTrue("ReadBuffer for first issued read not found",
        (firstReadBuffer != null));
    assertTrue("First readAhead should be of read request size",
        firstReadBuffer.getRequestedLength() == readRequestSize);
    byte[] dataFromReadAheadCompletedQueue_RAH1 = new byte[readRequestSize];
    ReadBufferManager.getBufferManager()
        .testGetBlockFromCompletedQueue(inputStream, 0, readRequestSize,
            dataFromReadAheadCompletedQueue_RAH1);
    assertTrue("Data mismatch found in RAH1",
        Arrays.equals(dataFromReadAheadCompletedQueue_RAH1,
            expected_RAH1_contents));

    // Fetch RAH buffer for second read
    ReadBuffer secondReadBuffer = ReadBufferManager.getBufferManager()
        .getBuffer(inputStream, readRequestSize);
    secondReadBuffer.getLatch().await();
    assertTrue("ReadBuffer for second issued read not found",
        (secondReadBuffer != null));
    assertTrue("Second readAhead should be of readAhead block size",
        secondReadBuffer.getRequestedLength() == readAheadRequestSize);
    byte[] dataFromReadAheadCompletedQueue_RAH2
        = new byte[readAheadRequestSize];
    ReadBufferManager.getBufferManager()
        .testGetBlockFromCompletedQueue(inputStream, readRequestSize,
            readAheadRequestSize, dataFromReadAheadCompletedQueue_RAH2);
    assertTrue("Data mismatch found in RAH2",
        Arrays.equals(dataFromReadAheadCompletedQueue_RAH2,
            expected_RAH2_contents));
  }

  public AbfsInputStream testReadAheadConfigs(int readRequestSize,
      int readAheadQueueDepth,
      boolean alwaysReadBufferSizeEnabled,
      int readAheadBlockSize,
      int readAheadBufferCount) throws Exception {
    Configuration
        config = new Configuration(
        this.getRawConfiguration());
    config.set("fs.azure.read.request.size", Integer.toString(readRequestSize));
    config.set("fs.azure.readaheadqueue.depth",
        Integer.toString(readAheadQueueDepth));
    config.set("fs.azure.alwaysReadBufferSize",
        Boolean.toString(alwaysReadBufferSizeEnabled));
    config.set("fs.azure.read.ahead.block.size",
        Integer.toString(readAheadBlockSize));
    config.set("fs.azure.read.ahead.buffer.count",
        Integer.toString(readAheadBufferCount));

    Path testPath = new Path(
        "/testReadAheadConfigs");
    final AzureBlobFileSystem fs = createTestFile(testPath,
        ALWAYS_READ_BUFFER_SIZE_TEST_FILE_SIZE, config);
    byte[] byteBuffer = new byte[ONE_MB];
    AbfsInputStream inputStream = this.getAbfsStore(fs)
        .openFileForRead(testPath, null);

    assertEquals("Unexpected AbfsInputStream buffer size", readRequestSize,
        inputStream.getBufferSize());
    assertEquals("Unexpected ReadAhead queue depth", readAheadQueueDepth,
        inputStream.getReadAheadQueueDepth());
    assertEquals("Unexpected AlwaysReadBufferSize settings",
        alwaysReadBufferSizeEnabled,
        inputStream.shouldAlwaysReadBufferSize());
    assertEquals("Unexpected readAhead block size", readAheadBlockSize,
        ReadBufferManager.getBufferManager().getReadAheadBlockSize());
    assertEquals("Unexpected readAhead buffer count", readAheadBufferCount,
        ReadBufferManager.getBufferManager().getReadAheadBufferCount());

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

    assertEquals("File not created of expected size", testFileSize,
        fs.getFileStatus(testFilePath).getLen());
    return fs;
  }
}