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

package org.apache.hadoop.fs.azurebfs;

import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.Map;
import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.runners.Parameterized;
import org.junit.runner.RunWith;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.BYTES_SENT;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.SEND_REQUESTS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_AZURE_ENABLE_SMALL_WRITE_OPTIMIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_APPENDBLOB_ENABLED;

/**
 * Test combination for small writes with flush and close operations.
 * This test class formulates an append test flow to assert on various scenarios.
 * Test stages:
 * 1. Pre-create test file of required size. This is determined by
 * startingFileSize parameter. If it is 0, then pre-creation is skipped.
 *
 * 2. Formulate an append loop or iteration. An iteration, will do N writes
 * (determined by numOfClientWrites parameter) with each writing X bytes
 * (determined by recurringClientWriteSize parameter).
 *
 * 3. Determine total number of append iterations needed by a test.
 * If intention is to close the outputStream right after append, setting
 * directCloseTest parameter will determine 1 append test iteration with an
 * ending close.
 * Else, it will execute TEST_FLUSH_ITERATION number of test iterations, with
 * each doing appends, hflush/hsync and then close.
 *
 * 4. Execute test iterations with asserts on number of store requests made and
 * validating file content.
 */
@RunWith(Parameterized.class)
public class ITestSmallWriteOptimization extends AbstractAbfsScaleTest {
  private static final int ONE_MB = 1024 * 1024;
  private static final int TWO_MB = 2 * ONE_MB;
  private static final int TEST_BUFFER_SIZE = TWO_MB;
  private static final int HALF_TEST_BUFFER_SIZE = TWO_MB / 2;
  private static final int QUARTER_TEST_BUFFER_SIZE = TWO_MB / 4;
  private static final int TEST_FLUSH_ITERATION = 2;

  @Parameterized.Parameter
  public String testScenario;

  @Parameterized.Parameter(1)
  public boolean enableSmallWriteOptimization;

  /**
   * If true, will initiate close after appends. (That is, no explicit hflush or
   * hsync calls will be made from client app.)
   */
  @Parameterized.Parameter(2)
  public boolean directCloseTest;

  /**
   * If non-zero, test file should be created as pre-requisite with this size.
   */
  @Parameterized.Parameter(3)
  public Integer startingFileSize;

  /**
   * Determines the write sizes to be issued by client app.
   */
  @Parameterized.Parameter(4)
  public Integer recurringClientWriteSize;

  /**
   * Determines the number of Client writes to make.
   */
  @Parameterized.Parameter(5)
  public Integer numOfClientWrites;

  /**
   * True, if the small write optimization is supposed to be effective in
   * the scenario.
   */
  @Parameterized.Parameter(6)
  public boolean flushExpectedToBeMergedWithAppend;

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> params() {
    return Arrays.asList(
        // Parameter Order :
        // testScenario,
        // enableSmallWriteOptimization, directCloseTest, startingFileSize,
        // recurringClientWriteSize, numOfClientWrites, flushExpectedToBeMergedWithAppend
        new Object[][]{
            // Buffer Size Write tests
            { "OptmON_FlushCloseTest_EmptyFile_BufferSizeWrite",
                true, false, 0, TEST_BUFFER_SIZE, 1, false
            },
            {   "OptmON_FlushCloseTest_NonEmptyFile_BufferSizeWrite",
                true, false, 2 * TEST_BUFFER_SIZE, TEST_BUFFER_SIZE, 1, false
            },
            {   "OptmON_CloseTest_EmptyFile_BufferSizeWrite",
                true, true, 0, TEST_BUFFER_SIZE, 1, false
            },
            {   "OptmON_CloseTest_NonEmptyFile_BufferSizeWrite",
                true, true, 2 * TEST_BUFFER_SIZE, TEST_BUFFER_SIZE, 1, false
            },
            {   "OptmOFF_FlushCloseTest_EmptyFile_BufferSizeWrite",
                false, false, 0, TEST_BUFFER_SIZE, 1, false
            },
            {   "OptmOFF_FlushCloseTest_NonEmptyFile_BufferSizeWrite",
                false, false, 2 * TEST_BUFFER_SIZE, TEST_BUFFER_SIZE, 1, false
            },
            {   "OptmOFF_CloseTest_EmptyFile_BufferSizeWrite",
                false, true, 0, TEST_BUFFER_SIZE, 1, false
            },
            {   "OptmOFF_CloseTest_NonEmptyFile_BufferSizeWrite",
                false, true, 2 * TEST_BUFFER_SIZE, TEST_BUFFER_SIZE, 1, false
            },
            // Less than buffer size write tests
            {   "OptmON_FlushCloseTest_EmptyFile_LessThanBufferSizeWrite",
                true, false, 0, Math.abs(HALF_TEST_BUFFER_SIZE), 1, true
            },
            {   "OptmON_FlushCloseTest_NonEmptyFile_LessThanBufferSizeWrite",
                true, false, 2 * TEST_BUFFER_SIZE,
                Math.abs(HALF_TEST_BUFFER_SIZE), 1, true
            },
            {   "OptmON_CloseTest_EmptyFile_LessThanBufferSizeWrite",
                true, true, 0, Math.abs(HALF_TEST_BUFFER_SIZE), 1, true
            },
            {   "OptmON_CloseTest_NonEmptyFile_LessThanBufferSizeWrite",
                true, true, 2 * TEST_BUFFER_SIZE,
                Math.abs(HALF_TEST_BUFFER_SIZE), 1, true
            },
            {   "OptmOFF_FlushCloseTest_EmptyFile_LessThanBufferSizeWrite",
                false, false, 0, Math.abs(HALF_TEST_BUFFER_SIZE), 1, false
            },
            {   "OptmOFF_FlushCloseTest_NonEmptyFile_LessThanBufferSizeWrite",
                false, false, 2 * TEST_BUFFER_SIZE,
                Math.abs(HALF_TEST_BUFFER_SIZE), 1, false
            },
            {   "OptmOFF_CloseTest_EmptyFile_LessThanBufferSizeWrite",
                false, true, 0, Math.abs(HALF_TEST_BUFFER_SIZE), 1, false
            },
            {   "OptmOFF_CloseTest_NonEmptyFile_LessThanBufferSizeWrite",
                false, true, 2 * TEST_BUFFER_SIZE,
                Math.abs(HALF_TEST_BUFFER_SIZE), 1, false
            },
            // Multiple small writes still less than buffer size
            {   "OptmON_FlushCloseTest_EmptyFile_MultiSmallWritesStillLessThanBufferSize",
                true, false, 0, Math.abs(QUARTER_TEST_BUFFER_SIZE), 3, true
            },
            {   "OptmON_FlushCloseTest_NonEmptyFile_MultiSmallWritesStillLessThanBufferSize",
                true, false, 2 * TEST_BUFFER_SIZE,
                Math.abs(QUARTER_TEST_BUFFER_SIZE), 3, true
            },
            {   "OptmON_CloseTest_EmptyFile_MultiSmallWritesStillLessThanBufferSize",
                true, true, 0, Math.abs(QUARTER_TEST_BUFFER_SIZE), 3, true
            },
            {   "OptmON_CloseTest_NonEmptyFile_MultiSmallWritesStillLessThanBufferSize",
                true, true, 2 * TEST_BUFFER_SIZE,
                Math.abs(QUARTER_TEST_BUFFER_SIZE), 3, true
            },
            {   "OptmOFF_FlushCloseTest_EmptyFile_MultiSmallWritesStillLessThanBufferSize",
                false, false, 0, Math.abs(QUARTER_TEST_BUFFER_SIZE), 3, false
            },
            {   "OptmOFF_FlushCloseTest_NonEmptyFile_MultiSmallWritesStillLessThanBufferSize",
                false, false, 2 * TEST_BUFFER_SIZE,
                Math.abs(QUARTER_TEST_BUFFER_SIZE), 3, false
            },
            {   "OptmOFF_CloseTest_EmptyFile_MultiSmallWritesStillLessThanBufferSize",
                false, true, 0, Math.abs(QUARTER_TEST_BUFFER_SIZE), 3, false
            },
            {   "OptmOFF_CloseTest_NonEmptyFile_MultiSmallWritesStillLessThanBufferSize",
                false, true, 2 * TEST_BUFFER_SIZE,
                Math.abs(QUARTER_TEST_BUFFER_SIZE), 3, false
            },
            // Multiple full buffer writes
            {   "OptmON_FlushCloseTest_EmptyFile_MultiBufferSizeWrite",
                true, false, 0, TEST_BUFFER_SIZE, 3, false
            },
            {   "OptmON_FlushCloseTest_NonEmptyFile_MultiBufferSizeWrite",
                true, false, 2 * TEST_BUFFER_SIZE, TEST_BUFFER_SIZE, 3, false
            },
            {   "OptmON_CloseTest_EmptyFile_MultiBufferSizeWrite",
                true, true, 0, TEST_BUFFER_SIZE, 3, false
            },
            {   "OptmON_CloseTest_NonEmptyFile_MultiBufferSizeWrite",
                true, true, 2 * TEST_BUFFER_SIZE, TEST_BUFFER_SIZE, 3, false
            },
            {   "OptmOFF_FlushCloseTest_EmptyFile_MultiBufferSizeWrite",
                false, false, 0, TEST_BUFFER_SIZE, 3, false
            },
            {   "OptmOFF_FlushCloseTest_NonEmptyFile_MultiBufferSizeWrite",
                false, false, 2 * TEST_BUFFER_SIZE, TEST_BUFFER_SIZE, 3, false
            },
            {   "OptmOFF_CloseTest_EmptyFile_MultiBufferSizeWrite",
                false, true, 0, TEST_BUFFER_SIZE, 3, false
            },
            {   "OptmOFF_CloseTest_NonEmptyFile_MultiBufferSizeWrite",
                false, true, 2 * TEST_BUFFER_SIZE, TEST_BUFFER_SIZE, 3, false
            },
            // Multiple full buffers triggered and data less than buffer size pending
            {   "OptmON_FlushCloseTest_EmptyFile_BufferAndExtraWrite",
                true, false, 0,
                TEST_BUFFER_SIZE + Math.abs(QUARTER_TEST_BUFFER_SIZE),
                3, false
            },
            {   "OptmON_FlushCloseTest_NonEmptyFile_BufferAndExtraWrite",
                true, false, 2 * TEST_BUFFER_SIZE,
                TEST_BUFFER_SIZE + Math.abs(QUARTER_TEST_BUFFER_SIZE),
                3, false
            },
            {   "OptmON_CloseTest_EmptyFile__BufferAndExtraWrite",
                true, true, 0,
                TEST_BUFFER_SIZE + Math.abs(QUARTER_TEST_BUFFER_SIZE),
                3, false
            },
            {   "OptmON_CloseTest_NonEmptyFile_BufferAndExtraWrite",
                true, true, 2 * TEST_BUFFER_SIZE,
                TEST_BUFFER_SIZE + Math.abs(QUARTER_TEST_BUFFER_SIZE),
                3, false
            },
            {   "OptmOFF_FlushCloseTest_EmptyFile_BufferAndExtraWrite",
                false, false, 0,
                TEST_BUFFER_SIZE + Math.abs(QUARTER_TEST_BUFFER_SIZE),
                3, false
            },
            {   "OptmOFF_FlushCloseTest_NonEmptyFile_BufferAndExtraWrite",
                false, false, 2 * TEST_BUFFER_SIZE,
                TEST_BUFFER_SIZE + Math.abs(QUARTER_TEST_BUFFER_SIZE),
                3, false
            },
            {   "OptmOFF_CloseTest_EmptyFile_BufferAndExtraWrite",
                false, true, 0,
                TEST_BUFFER_SIZE + Math.abs(QUARTER_TEST_BUFFER_SIZE),
                3, false
            },
            {   "OptmOFF_CloseTest_NonEmptyFile_BufferAndExtraWrite",
                false, true, 2 * TEST_BUFFER_SIZE,
                TEST_BUFFER_SIZE + Math.abs(QUARTER_TEST_BUFFER_SIZE),
                3, false
            },
            // 0 byte tests
            {   "OptmON_FlushCloseTest_EmptyFile_0ByteWrite",
                true, false, 0, 0, 1, false
            },
            {   "OptmON_FlushCloseTest_NonEmptyFile_0ByteWrite",
                true, false, 2 * TEST_BUFFER_SIZE, 0, 1, false
            },
            {   "OptmON_CloseTest_EmptyFile_0ByteWrite",
                true, true, 0, 0, 1, false
            },
            {   "OptmON_CloseTest_NonEmptyFile_0ByteWrite",
                true, true, 2 * TEST_BUFFER_SIZE, 0, 1, false
            },
            {   "OptmOFF_FlushCloseTest_EmptyFile_0ByteWrite",
                false, false, 0, 0, 1, false
            },
            {   "OptmOFF_FlushCloseTest_NonEmptyFile_0ByteWrite",
                false, false, 2 * TEST_BUFFER_SIZE, 0, 1, false
            },
            {   "OptmOFF_CloseTest_EmptyFile_0ByteWrite",
                false, true, 0, 0, 1, false
            },
            {   "OptmOFF_CloseTest_NonEmptyFile_0ByteWrite",
                false, true, 2 * TEST_BUFFER_SIZE, 0, 1, false
            },
        });
  }
  public ITestSmallWriteOptimization() throws Exception {
    super();
  }

  @Test
  public void testSmallWriteOptimization()
      throws IOException {
    boolean serviceDefaultOptmSettings = DEFAULT_AZURE_ENABLE_SMALL_WRITE_OPTIMIZATION;
    // Tests with Optimization should only run if service has the feature on by
    // default. Default settings will be turned on when server support is
    // available on all store prod regions.
    if (enableSmallWriteOptimization) {
      Assume.assumeTrue(serviceDefaultOptmSettings);
    }

    final AzureBlobFileSystem currentfs = this.getFileSystem();
    Configuration config = currentfs.getConf();
    boolean isAppendBlobTestSettingEnabled = (config.get(FS_AZURE_TEST_APPENDBLOB_ENABLED) == "true");

    // This optimization doesnt take effect when append blob is on.
    Assume.assumeFalse(isAppendBlobTestSettingEnabled);

    config.set(ConfigurationKeys.AZURE_WRITE_BUFFER_SIZE, Integer.toString(TEST_BUFFER_SIZE));
    config.set(ConfigurationKeys.AZURE_ENABLE_SMALL_WRITE_OPTIMIZATION, Boolean.toString(enableSmallWriteOptimization));
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.get(
        currentfs.getUri(), config);

    formulateSmallWriteTestAppendPattern(fs, startingFileSize,
        recurringClientWriteSize, numOfClientWrites,
        directCloseTest, flushExpectedToBeMergedWithAppend);
  }

  /**
   * if isDirectCloseTest == true, append + close is triggered
   * if isDirectCloseTest == false, append + flush runs are repeated over
   * iterations followed by close
   * @param fs
   * @param startingFileSize
   * @param recurringWriteSize
   * @param numOfWrites
   * @param isDirectCloseTest
   * @throws IOException
   */
  private void formulateSmallWriteTestAppendPattern(final AzureBlobFileSystem fs,
      int startingFileSize,
      int recurringWriteSize,
      int numOfWrites,
      boolean isDirectCloseTest,
      boolean flushExpectedToBeMergedWithAppend) throws IOException {

    int totalDataToBeAppended = 0;
    int testIteration = 0;
    int dataWrittenPerIteration = (numOfWrites * recurringWriteSize);

    if (isDirectCloseTest) {
      totalDataToBeAppended = dataWrittenPerIteration;
      testIteration = 1;
    } else {
      testIteration = TEST_FLUSH_ITERATION;
      totalDataToBeAppended = testIteration * dataWrittenPerIteration;
    }

    int totalFileSize = totalDataToBeAppended + startingFileSize;
    // write buffer of file size created. This will be used as write
    // source and for file content validation
    final byte[] writeBuffer = new byte[totalFileSize];
    new Random().nextBytes(writeBuffer);
    int writeBufferCursor = 0;

    Path testPath = new Path(getMethodName() + UUID.randomUUID().toString());
    FSDataOutputStream opStream;

    if (startingFileSize > 0) {
      writeBufferCursor += createFileWithStartingTestSize(fs, writeBuffer, writeBufferCursor, testPath,
          startingFileSize);
      opStream = fs.append(testPath);
    } else {
      opStream = fs.create(testPath);
    }

    final int writeBufferSize = fs.getAbfsStore()
        .getAbfsConfiguration()
        .getWriteBufferSize();
    long expectedTotalRequestsMade = fs.getInstrumentationMap()
        .get(CONNECTIONS_MADE.getStatName());
    long expectedRequestsMadeWithData = fs.getInstrumentationMap()
        .get(SEND_REQUESTS.getStatName());
    long expectedBytesSent = fs.getInstrumentationMap()
        .get(BYTES_SENT.getStatName());

    while (testIteration > 0) {
      // trigger recurringWriteSize appends over numOfWrites
      writeBufferCursor += executeWritePattern(opStream, writeBuffer,
          writeBufferCursor, numOfWrites, recurringWriteSize);

      int numOfBuffersWrittenToStore = (int) Math.floor(
          dataWrittenPerIteration / writeBufferSize);
      int dataSizeWrittenToStore = numOfBuffersWrittenToStore * writeBufferSize;
      int pendingDataToStore = dataWrittenPerIteration - dataSizeWrittenToStore;

      expectedTotalRequestsMade += numOfBuffersWrittenToStore;
      expectedRequestsMadeWithData += numOfBuffersWrittenToStore;
      expectedBytesSent += dataSizeWrittenToStore;

      if (isDirectCloseTest) {
        opStream.close();
      } else {
        opStream.hflush();
      }

      boolean wasDataPendingToBeWrittenToServer = (pendingDataToStore > 0);
      // Small write optimization will only work if
      // a. config for small write optimization is on
      // b. no buffer writes have been triggered since last flush
      // c. there is some pending data in buffer to write to store
      final boolean smallWriteOptimizationEnabled = fs.getAbfsStore()
          .getAbfsConfiguration()
          .isSmallWriteOptimizationEnabled();
      boolean flushWillBeMergedWithAppend = smallWriteOptimizationEnabled
          && (numOfBuffersWrittenToStore == 0)
          && (wasDataPendingToBeWrittenToServer);

      Assertions.assertThat(flushWillBeMergedWithAppend)
          .describedAs(flushExpectedToBeMergedWithAppend
              ? "Flush was to be merged with Append"
              : "Flush should not have been merged with Append")
          .isEqualTo(flushExpectedToBeMergedWithAppend);

      int totalAppendFlushCalls = (flushWillBeMergedWithAppend
          ? 1 // 1 append (with flush and close param)
          : (wasDataPendingToBeWrittenToServer)
              ? 2 // 1 append + 1 flush (with close)
              : 1); // 1 flush (with close)

      expectedTotalRequestsMade += totalAppendFlushCalls;
      expectedRequestsMadeWithData += totalAppendFlushCalls;
      expectedBytesSent += wasDataPendingToBeWrittenToServer
          ? pendingDataToStore
          : 0;

      assertOpStats(fs.getInstrumentationMap(), expectedTotalRequestsMade,
          expectedRequestsMadeWithData, expectedBytesSent);

      if (isDirectCloseTest) {
        // stream already closed
        validateStoreAppends(fs, testPath, totalFileSize, writeBuffer);
        return;
      }

      testIteration--;
    }

    opStream.close();
    expectedTotalRequestsMade += 1;
    expectedRequestsMadeWithData += 1;
    // no change in expectedBytesSent
    assertOpStats(fs.getInstrumentationMap(), expectedTotalRequestsMade, expectedRequestsMadeWithData, expectedBytesSent);

    validateStoreAppends(fs, testPath, totalFileSize, writeBuffer);
  }

  private int createFileWithStartingTestSize(AzureBlobFileSystem fs, byte[] writeBuffer,
      int writeBufferCursor, Path testPath, int startingFileSize)
      throws IOException {
    FSDataOutputStream opStream = fs.create(testPath);
    writeBufferCursor += executeWritePattern(opStream,
        writeBuffer,
        writeBufferCursor,
        1,
        startingFileSize);

    opStream.close();
    Assertions.assertThat(fs.getFileStatus(testPath).getLen())
        .describedAs("File should be of size %d at the start of test.",
            startingFileSize)
        .isEqualTo(startingFileSize);

    return writeBufferCursor;
  }

  private void validateStoreAppends(AzureBlobFileSystem fs,
      Path testPath,
      int totalFileSize,
      byte[] bufferWritten)
      throws IOException {
    // Final validation
    Assertions.assertThat(fs.getFileStatus(testPath).getLen())
        .describedAs("File should be of size %d at the end of test.",
            totalFileSize)
        .isEqualTo(totalFileSize);

    byte[] fileReadFromStore = new byte[totalFileSize];
    fs.open(testPath).read(fileReadFromStore, 0, totalFileSize);

    assertArrayEquals("Test file content incorrect", bufferWritten,
        fileReadFromStore);
  }

  private void assertOpStats(Map<String, Long> metricMap,
      long expectedTotalRequestsMade,
      long expectedRequestsMadeWithData,
      long expectedBytesSent) {
    assertAbfsStatistics(CONNECTIONS_MADE, expectedTotalRequestsMade,
        metricMap);
    assertAbfsStatistics(SEND_REQUESTS, expectedRequestsMadeWithData,
        metricMap);
    assertAbfsStatistics(BYTES_SENT, expectedBytesSent, metricMap);
  }

  private int executeWritePattern(FSDataOutputStream opStream,
      byte[] buffer,
      int startOffset,
      int writeLoopCount,
      int writeSize)
      throws IOException {
    int dataSizeWritten = startOffset;

    while (writeLoopCount > 0) {
      opStream.write(buffer, startOffset, writeSize);
      startOffset += writeSize;
      writeLoopCount--;
    }

    dataSizeWritten = startOffset - dataSizeWritten;
    return dataSizeWritten;
  }
}
