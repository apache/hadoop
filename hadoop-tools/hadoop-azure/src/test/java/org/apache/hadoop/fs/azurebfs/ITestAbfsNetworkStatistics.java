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
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.SEND_REQUESTS;

public class ITestAbfsNetworkStatistics extends AbstractAbfsIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAbfsNetworkStatistics.class);
  private static final int LARGE_OPERATIONS = 10;

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
    long connectionsMade, requestsSent, bytesSent;

    metricMap = fs.getInstrumentationMap();
    long connectionsMadeBeforeTest = metricMap
        .get(CONNECTIONS_MADE.getStatName());
    long requestsMadeBeforeTest = metricMap.get(SEND_REQUESTS.getStatName());

    /*
     * Creating AbfsOutputStream will result in 1 connection made and 1 send
     * request.
     */
    try (AbfsOutputStream out = createAbfsOutputStreamWithFlushEnabled(fs,
        sendRequestPath)) {
      out.write(testNetworkStatsString.getBytes());

      /*
       * Flushes all outstanding data (i.e. the current unfinished packet)
       * from the client into the service on all DataNode replicas.
       */
      out.hflush();

      metricMap = fs.getInstrumentationMap();

      /*
       * Testing the network stats with 1 write operation.
       *
       * connections_made : (connections made above) + 2(flush).
       *
       * send_requests : (requests sent above) + 2(flush).
       *
       * bytes_sent : bytes wrote in AbfsOutputStream.
       */
      long extraCalls = 0;
      if (!fs.getAbfsStore()
          .isAppendBlobKey(fs.makeQualified(sendRequestPath).toString())) {
        // no network calls are made for hflush in case of appendblob
        extraCalls++;
      }
      long expectedConnectionsMade = connectionsMadeBeforeTest + extraCalls + 2;
      long expectedRequestsSent = requestsMadeBeforeTest + extraCalls + 2;
      connectionsMade = assertAbfsStatistics(CONNECTIONS_MADE,
          expectedConnectionsMade, metricMap);
      requestsSent = assertAbfsStatistics(SEND_REQUESTS, expectedRequestsSent,
          metricMap);
      bytesSent = assertAbfsStatistics(AbfsStatistic.BYTES_SENT,
          testNetworkStatsString.getBytes().length, metricMap);
    }

    // To close the AbfsOutputStream 1 connection is made and 1 request is sent.
    connectionsMade++;
    requestsSent++;

    try (AbfsOutputStream out = createAbfsOutputStreamWithFlushEnabled(fs,
        sendRequestPath)) {

      for (int i = 0; i < LARGE_OPERATIONS; i++) {
        out.write(testNetworkStatsString.getBytes());

        /*
         * 1 flush call would create 2 connections and 2 send requests.
         * when hflush() is called it will essentially trigger append() and
         * flush() inside AbfsRestOperation. Both of which calls
         * executeHttpOperation() method which creates a connection and sends
         * requests.
         */
        out.hflush();
      }

      metricMap = fs.getInstrumentationMap();

      /*
       * Testing the network stats with Large amount of bytes sent.
       *
       * connections made : connections_made(Last assertion) + 1
       * (AbfsOutputStream) + LARGE_OPERATIONS * 2(flush).
       *
       * send requests : requests_sent(Last assertion) + 1(AbfsOutputStream) +
       * LARGE_OPERATIONS * 2(flush).
       *
       * bytes sent : bytes_sent(Last assertion) + LARGE_OPERATIONS * (bytes
       * wrote each time).
       *
       */
      if (fs.getAbfsStore().isAppendBlobKey(fs.makeQualified(sendRequestPath).toString())) {
        // no network calls are made for hflush in case of appendblob
        assertAbfsStatistics(CONNECTIONS_MADE,
            connectionsMade + 1 + LARGE_OPERATIONS, metricMap);
        assertAbfsStatistics(SEND_REQUESTS,
            requestsSent + 1 + LARGE_OPERATIONS, metricMap);
      } else {
        assertAbfsStatistics(CONNECTIONS_MADE,
            connectionsMade + 1 + LARGE_OPERATIONS * 2, metricMap);
        assertAbfsStatistics(SEND_REQUESTS,
            requestsSent + 1 + LARGE_OPERATIONS * 2, metricMap);
      }
      assertAbfsStatistics(AbfsStatistic.BYTES_SENT,
          bytesSent + LARGE_OPERATIONS * (testNetworkStatsString.getBytes().length),
          metricMap);

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
    long getResponses, bytesReceived;

    FSDataOutputStream out = null;
    FSDataInputStream in = null;
    try {

      /*
       * Creating a File and writing some bytes in it.
       *
       * get_response : 3(getFileSystem) + 1(OutputStream creation) + 2
       * (Writing data in Data store).
       *
       */
      out = fs.create(getResponsePath);
      out.write(testResponseString.getBytes());
      out.hflush();

      metricMap = fs.getInstrumentationMap();
      long getResponsesBeforeTest = metricMap
          .get(CONNECTIONS_MADE.getStatName());

      // open would require 1 get response.
      in = fs.open(getResponsePath);
      // read would require 1 get response and also get the bytes received.
      int result = in.read();

      // Confirming read isn't -1.
      LOG.info("Result of read operation : {}", result);

      metricMap = fs.getInstrumentationMap();

      /*
       * Testing values of statistics after writing and reading a buffer.
       *
       * get_responses - (above operations) + 1(open()) + 1 (read()).;
       *
       * bytes_received - This should be equal to bytes sent earlier.
       */
      long extraCalls = 0;
      if (!fs.getAbfsStore()
          .isAppendBlobKey(fs.makeQualified(getResponsePath).toString())) {
        // no network calls are made for hflush in case of appendblob
        extraCalls++;
      }
      long expectedGetResponses = getResponsesBeforeTest + extraCalls + 1;
      getResponses = assertAbfsStatistics(AbfsStatistic.GET_RESPONSES,
          expectedGetResponses, metricMap);

      // Testing that bytes received is equal to bytes sent.
      long bytesSend = metricMap.get(AbfsStatistic.BYTES_SENT.getStatName());
      bytesReceived = assertAbfsStatistics(AbfsStatistic.BYTES_RECEIVED,
          bytesSend,
          metricMap);

    } finally {
      IOUtils.cleanupWithLogger(LOG, out, in);
    }

    // To close the streams 1 response is received.
    getResponses++;

    try {

      /*
       * Creating a file and writing buffer into it. Also recording the
       * buffer for future read() call.
       * This creating outputStream and writing requires 2 *
       * (LARGE_OPERATIONS) get requests.
       */
      StringBuilder largeBuffer = new StringBuilder();
      out = fs.create(getResponsePath);
      for (int i = 0; i < LARGE_OPERATIONS; i++) {
        out.write(testResponseString.getBytes());
        out.hflush();
        largeBuffer.append(testResponseString);
      }

      // Open requires 1 get_response.
      in = fs.open(getResponsePath);

      /*
       * Reading the file which was written above. This read() call would
       * read bytes equal to the bytes that was written above.
       * Get response would be 1 only.
       */
      in.read(0, largeBuffer.toString().getBytes(), 0,
          largeBuffer.toString().getBytes().length);

      metricMap = fs.getInstrumentationMap();

      /*
       * Testing the statistics values after writing and reading a large buffer.
       *
       * get_response : get_responses(Last assertion) + 1
       * (OutputStream) + 2 * LARGE_OPERATIONS(Writing and flushing
       * LARGE_OPERATIONS times) + 1(open()) + 1(read()).
       *
       * bytes_received : bytes_received(Last assertion) + LARGE_OPERATIONS *
       * bytes wrote each time (bytes_received is equal to bytes wrote in the
       * File).
       *
       */
      assertAbfsStatistics(AbfsStatistic.BYTES_RECEIVED,
          bytesReceived + LARGE_OPERATIONS * (testResponseString.getBytes().length),
          metricMap);
      if (fs.getAbfsStore().isAppendBlobKey(fs.makeQualified(getResponsePath).toString())) {
        // no network calls are made for hflush in case of appendblob
        assertAbfsStatistics(AbfsStatistic.GET_RESPONSES,
            getResponses + 3 + LARGE_OPERATIONS, metricMap);
      } else {
        assertAbfsStatistics(AbfsStatistic.GET_RESPONSES,
            getResponses + 3 + 2 * LARGE_OPERATIONS, metricMap);
      }

    } finally {
      IOUtils.cleanupWithLogger(LOG, out, in);
    }
  }

}
