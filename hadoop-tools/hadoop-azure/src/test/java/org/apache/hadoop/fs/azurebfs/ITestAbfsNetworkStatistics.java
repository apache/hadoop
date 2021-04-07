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

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.BYTES_RECEIVED;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.GET_RESPONSES;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.SEND_REQUESTS;

public class ITestAbfsNetworkStatistics extends AbstractAbfsIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAbfsNetworkStatistics.class);
  private static final int WRITE_OPERATION_LOOP_COUNT = 10;

  public ITestAbfsNetworkStatistics() throws Exception {
  }

  /**
   * Testing connections_made, send_request and bytes_send statistics in
   * {@link AbfsRestOperation}.
   */
  @Test
  public void testAbfsHttpSendStatistics() throws IOException {
    describe("Test to check correct values of statistics after Abfs http send "
        + "request is done.");

    AzureBlobFileSystem fs = getFileSystem();
    Map<String, Long> metricMap;
    Path sendRequestPath = path(getMethodName());
    String testNetworkStatsString = "http_send";

    metricMap = fs.getInstrumentationMap();
    long expectedConnectionsMade = metricMap.get(CONNECTIONS_MADE.getStatName());
    long expectedRequestsSent = metricMap.get(SEND_REQUESTS.getStatName());
    long expectedBytesSent = 0;

    // --------------------------------------------------------------------
     // Operation: Creating AbfsOutputStream
    try (AbfsOutputStream out = createAbfsOutputStreamWithFlushEnabled(fs,
        sendRequestPath)) {
       // Network stats calculation: For Creating AbfsOutputStream:
       // 1 create request = 1 connection made and 1 send request
      expectedConnectionsMade++;
      expectedRequestsSent++;
      // --------------------------------------------------------------------

      // Operation: Write small data
      // Network stats calculation: No additions.
      // Data written is less than the buffer size and hence will not
      // trigger any append request to store
      out.write(testNetworkStatsString.getBytes());
      // --------------------------------------------------------------------

       // Operation: HFlush
       // Flushes all outstanding data (i.e. the current unfinished packet)
       // from the client into the service on all DataNode replicas.
      out.hflush();
      /*
       * Network stats calculation:
       * 3 possibilities here:
       * A. As there is pending data to be written to store, this will result in:
       *    1 append + 1 flush = 2 connections and 2 send requests
       *
       * B. If config "fs.azure.enable.small.write.optimization" is enabled, append
       *   and flush call will be merged for small data in buffer in this test.
       *   In which case it will be:
       *   1 append+flush request = 1 connection and 1 send request
       *
       * C. If the path is configured for append Blob files to be used, hflush
       *   is a no-op. So in this case:
       *   1 append = 1 connection and 1 send request
       */
      if (fs.getAbfsStore().isAppendBlobKey(fs.makeQualified(sendRequestPath).toString())
          || (this.getConfiguration().isSmallWriteOptimizationEnabled())) {
        expectedConnectionsMade++;
        expectedRequestsSent++;
      } else {
        expectedConnectionsMade += 2;
        expectedRequestsSent += 2;
      }
      expectedBytesSent += testNetworkStatsString.getBytes().length;
      // --------------------------------------------------------------------

      // Assertions
      metricMap = fs.getInstrumentationMap();
      assertAbfsStatistics(CONNECTIONS_MADE,
          expectedConnectionsMade, metricMap);
      assertAbfsStatistics(SEND_REQUESTS, expectedRequestsSent,
          metricMap);
      assertAbfsStatistics(AbfsStatistic.BYTES_SENT,
          expectedBytesSent, metricMap);
    }

    // --------------------------------------------------------------------
    // Operation: AbfsOutputStream close.
    // Network Stats calculation: 1 flush (with close) is send.
    // 1 flush request = 1 connection and 1 send request
    expectedConnectionsMade++;
    expectedRequestsSent++;
    // --------------------------------------------------------------------

    // Operation: Re-create the file / create overwrite scenario
    try (AbfsOutputStream out = createAbfsOutputStreamWithFlushEnabled(fs,
        sendRequestPath)) {
      /*
       * Network Stats calculation: create overwrite
       * There are 2 possibilities here.
       * A. create overwrite results in 1 server call
       *    create with overwrite=true = 1 connection and 1 send request
       *
       * B. If config "fs.azure.enable.conditional.create.overwrite" is enabled,
       *    create overwrite=false (will fail in this case as file is indeed present)
       *    + getFileStatus to fetch the file ETag
       *    + create overwrite=true
       *    = 3 connections and 2 send requests
       */
      if (this.getConfiguration().isConditionalCreateOverwriteEnabled()) {
        expectedConnectionsMade += 3;
        expectedRequestsSent += 2;
      } else {
        expectedConnectionsMade += 1;
        expectedRequestsSent += 1;
      }
      // --------------------------------------------------------------------

      // Operation: Multiple small appends + hflush
      for (int i = 0; i < WRITE_OPERATION_LOOP_COUNT; i++) {
        out.write(testNetworkStatsString.getBytes());
        // Network stats calculation: no-op. Small write
        out.hflush();
        // Network stats calculation: Hflush
        // refer to previous comments for hFlush network stats calcualtion
        // possibilities
        if (fs.getAbfsStore().isAppendBlobKey(fs.makeQualified(sendRequestPath).toString())
            || (this.getConfiguration().isSmallWriteOptimizationEnabled())) {
          expectedConnectionsMade++;
          expectedRequestsSent++;
        } else {
          expectedConnectionsMade += 2;
          expectedRequestsSent += 2;
        }
        expectedBytesSent += testNetworkStatsString.getBytes().length;
      }
      // --------------------------------------------------------------------

      // Assertions
      metricMap = fs.getInstrumentationMap();
      assertAbfsStatistics(CONNECTIONS_MADE, expectedConnectionsMade, metricMap);
      assertAbfsStatistics(SEND_REQUESTS, expectedRequestsSent, metricMap);
      assertAbfsStatistics(AbfsStatistic.BYTES_SENT, expectedBytesSent, metricMap);
    }

  }

  /**
   * Testing get_response and bytes_received in {@link AbfsRestOperation}.
   */
  @Test
  public void testAbfsHttpResponseStatistics() throws IOException {
    describe("Test to check correct values of statistics after Http "
        + "Response is processed.");

    AzureBlobFileSystem fs = getFileSystem();
    Path getResponsePath = path(getMethodName());
    Map<String, Long> metricMap;
    String testResponseString = "some response";

    FSDataOutputStream out = null;
    FSDataInputStream in = null;
    long expectedConnectionsMade;
    long expectedGetResponses;
    long expectedBytesReceived;

    try {
      // Creating a File and writing some bytes in it.
      out = fs.create(getResponsePath);
      out.write(testResponseString.getBytes());
      out.hflush();

      // Set metric baseline
      metricMap = fs.getInstrumentationMap();
      long bytesWrittenToFile = testResponseString.getBytes().length;
      expectedConnectionsMade = metricMap.get(CONNECTIONS_MADE.getStatName());
      expectedGetResponses = metricMap.get(CONNECTIONS_MADE.getStatName());
      expectedBytesReceived = metricMap.get(BYTES_RECEIVED.getStatName());

      // --------------------------------------------------------------------
      // Operation: Create AbfsInputStream
      in = fs.open(getResponsePath);
      // Network stats calculation: For Creating AbfsInputStream:
      // 1 GetFileStatus request to fetch file size = 1 connection and 1 get response
      expectedConnectionsMade++;
      expectedGetResponses++;
      // --------------------------------------------------------------------

      // Operation: Read
      int result = in.read();
      // Network stats calculation: For read:
      // 1 read request = 1 connection and 1 get response
      expectedConnectionsMade++;
      expectedGetResponses++;
      expectedBytesReceived += bytesWrittenToFile;
      // --------------------------------------------------------------------

      // Assertions
      metricMap = fs.getInstrumentationMap();
      assertAbfsStatistics(CONNECTIONS_MADE, expectedConnectionsMade, metricMap);
      assertAbfsStatistics(GET_RESPONSES, expectedGetResponses, metricMap);
      assertAbfsStatistics(AbfsStatistic.BYTES_RECEIVED, expectedBytesReceived, metricMap);
    } finally {
      IOUtils.cleanupWithLogger(LOG, out, in);
    }

    // --------------------------------------------------------------------
    // Operation: AbfsOutputStream close.
    // Network Stats calculation: no op.
    // --------------------------------------------------------------------

    try {

      // Recreate file with different file size
      // [Create and append related network stats checks are done in
      // test method testAbfsHttpSendStatistics]
      StringBuilder largeBuffer = new StringBuilder();
      out = fs.create(getResponsePath);

      for (int i = 0; i < WRITE_OPERATION_LOOP_COUNT; i++) {
        out.write(testResponseString.getBytes());
        out.hflush();
        largeBuffer.append(testResponseString);
      }

      // sync back to metric baseline
      metricMap = fs.getInstrumentationMap();
      expectedConnectionsMade = metricMap.get(CONNECTIONS_MADE.getStatName());
      expectedGetResponses = metricMap.get(GET_RESPONSES.getStatName());
      // --------------------------------------------------------------------
      // Operation: Create AbfsInputStream
      in = fs.open(getResponsePath);
      // Network stats calculation: For Creating AbfsInputStream:
      // 1 GetFileStatus for file size = 1 connection and 1 get response
      expectedConnectionsMade++;
      expectedGetResponses++;
      // --------------------------------------------------------------------

      // Operation: Read
      in.read(0, largeBuffer.toString().getBytes(), 0, largeBuffer.toString().getBytes().length);
      // Network stats calculation: Total data written is still lesser than
      // a buffer size. Hence will trigger only one read to store. So result is:
      // 1 read request = 1 connection and 1 get response
      expectedConnectionsMade++;
      expectedGetResponses++;
      expectedBytesReceived += (WRITE_OPERATION_LOOP_COUNT * testResponseString.getBytes().length);
      // --------------------------------------------------------------------

      // Assertions
      metricMap = fs.getInstrumentationMap();
      assertAbfsStatistics(CONNECTIONS_MADE, expectedConnectionsMade, metricMap);
      assertAbfsStatistics(GET_RESPONSES, expectedGetResponses, metricMap);
      assertAbfsStatistics(AbfsStatistic.BYTES_RECEIVED, expectedBytesReceived, metricMap);
    } finally {
      IOUtils.cleanupWithLogger(LOG, out, in);
    }
  }

  /**
   * Testing bytes_received counter value when a response failure occurs.
   */
  @Test
  public void testAbfsHttpResponseFailure() throws IOException {
    describe("Test to check the values of bytes received counter when a "
        + "response is failed");

    AzureBlobFileSystem fs = getFileSystem();
    Path responseFailurePath = path(getMethodName());
    Map<String, Long> metricMap;
    FSDataOutputStream out = null;

    try {
      //create an empty file
      out = fs.create(responseFailurePath);
      //Re-creating the file again on same path with false overwrite, this
      // would cause a response failure with status code 409.
      out = fs.create(responseFailurePath, false);
    } catch (FileAlreadyExistsException faee) {
      metricMap = fs.getInstrumentationMap();
      // Assert after catching the 409 error to check the counter values.
      assertAbfsStatistics(AbfsStatistic.BYTES_RECEIVED, 0, metricMap);
    } finally {
      IOUtils.cleanupWithLogger(LOG, out);
    }
  }
}