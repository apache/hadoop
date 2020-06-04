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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsPerfLoggable;

/**
 * {@code AbfsPerfTracker} keeps track of service latencies observed by {@code AbfsClient}. Every request hands over
 * its perf-related information as a {@code AbfsPerfInfo} object (contains success/failure, latency etc) to the
 * {@code AbfsPerfTracker}'s queue. When a request is made, we check {@code AbfsPerfTracker} to see if there are
 * any latency numbers to be reported. If there are any, the stats are added to an HTTP header
 * ({@code x-ms-abfs-client-latency}) on the next request.
 *
 * A typical perf log line appears like:
 *
 * h=KARMA t=2019-10-25T20:21:14.518Z a=abfstest01.dfs.core.windows.net
 * c=abfs-testcontainer-84828169-6488-4a62-a875-1e674275a29f cr=delete ce=deletePath r=Succeeded l=32 ls=32 lc=1 s=200
 * e= ci=95121dae-70a8-4187-b067-614091034558 ri=97effdcf-201f-0097-2d71-8bae00000000 ct=0 st=0 rt=0 bs=0 br=0 m=DELETE
 * u=https%3A%2F%2Fabfstest01.dfs.core.windows.net%2Fabfs-testcontainer%2Ftest%3Ftimeout%3D90%26recursive%3Dtrue
 *
 * The fields have the following definitions:
 *
 * h: host name
 * t: time when this request was logged
 * a: Azure storage account name
 * c: container name
 * cr: name of the caller method
 * ce: name of the callee method
 * r: result (Succeeded/Failed)
 * l: latency (time spent in callee)
 * ls: latency sum (aggregate time spent in caller; logged when there are multiple callees;
 *     logged with the last callee)
 * lc: latency count (number of callees; logged when there are multiple callees;
 *     logged with the last callee)
 * s: HTTP Status code
 * e: Error code
 * ci: client request ID
 * ri: server request ID
 * ct: connection time in milliseconds
 * st: sending time in milliseconds
 * rt: receiving time in milliseconds
 * bs: bytes sent
 * br: bytes received
 * m: HTTP method (GET, PUT etc)
 * u: Encoded HTTP URL
 *
 */
public final class AbfsPerfTracker {

  // the logger.
  private static final Logger LOG = LoggerFactory.getLogger(AbfsPerfTracker.class);

  // the field names of perf log lines.
  private static final String HOST_NAME_KEY = "h";
  private static final String TIMESTAMP_KEY = "t";
  private static final String STORAGE_ACCOUNT_NAME_KEY = "a";
  private static final String CONTAINER_NAME_KEY = "c";
  private static final String CALLER_METHOD_NAME_KEY = "cr";
  private static final String CALLEE_METHOD_NAME_KEY = "ce";
  private static final String RESULT_KEY = "r";
  private static final String LATENCY_KEY = "l";
  private static final String LATENCY_SUM_KEY = "ls";
  private static final String LATENCY_COUNT_KEY = "lc";
  private static final String HTTP_STATUS_CODE_KEY = "s";
  private static final String ERROR_CODE_KEY = "e";
  private static final String CLIENT_REQUEST_ID_KEY = "ci";
  private static final String SERVER_REQUEST_ID_KEY = "ri";
  private static final String CONNECTION_TIME_KEY = "ct";
  private static final String SENDING_TIME_KEY = "st";
  private static final String RECEIVING_TIME_KEY = "rt";
  private static final String BYTES_SENT_KEY = "bs";
  private static final String BYTES_RECEIVED_KEY = "br";
  private static final String HTTP_METHOD_KEY = "m";
  private static final String HTTP_URL_KEY = "u";
  private static final String STRING_PLACEHOLDER = "%s";

  // the queue to hold latency information.
  private final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();

  // whether the latency tracker has been enabled.
  private boolean enabled = false;

  // the host name.
  private String hostName;

  // singleton latency reporting format.
  private String singletonLatencyReportingFormat;

  // aggregate latency reporting format.
  private String aggregateLatencyReportingFormat;

  public AbfsPerfTracker(String filesystemName, String accountName, AbfsConfiguration configuration) {
    this(filesystemName, accountName, configuration.shouldTrackLatency());
  }

  protected AbfsPerfTracker(String filesystemName, String accountName, boolean enabled) {
    this.enabled = enabled;

    LOG.debug("AbfsPerfTracker configuration: {}", enabled);

    if (enabled) {
      try {
        hostName = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        hostName = "UnknownHost";
      }

      String commonReportingFormat = new StringBuilder()
              .append(HOST_NAME_KEY)
              .append(AbfsHttpConstants.EQUAL)
              .append(hostName)
              .append(AbfsHttpConstants.SINGLE_WHITE_SPACE)
              .append(TIMESTAMP_KEY)
              .append(AbfsHttpConstants.EQUAL)
              .append(STRING_PLACEHOLDER)
              .append(AbfsHttpConstants.SINGLE_WHITE_SPACE)
              .append(STORAGE_ACCOUNT_NAME_KEY)
              .append(AbfsHttpConstants.EQUAL)
              .append(accountName)
              .append(AbfsHttpConstants.SINGLE_WHITE_SPACE)
              .append(CONTAINER_NAME_KEY)
              .append(AbfsHttpConstants.EQUAL)
              .append(filesystemName)
              .append(AbfsHttpConstants.SINGLE_WHITE_SPACE)
              .append(CALLER_METHOD_NAME_KEY)
              .append(AbfsHttpConstants.EQUAL)
              .append(STRING_PLACEHOLDER)
              .append(AbfsHttpConstants.SINGLE_WHITE_SPACE)
              .append(CALLEE_METHOD_NAME_KEY)
              .append(AbfsHttpConstants.EQUAL)
              .append(STRING_PLACEHOLDER)
              .append(AbfsHttpConstants.SINGLE_WHITE_SPACE)
              .append(RESULT_KEY)
              .append(AbfsHttpConstants.EQUAL)
              .append(STRING_PLACEHOLDER)
              .append(AbfsHttpConstants.SINGLE_WHITE_SPACE)
              .append(LATENCY_KEY)
              .append(AbfsHttpConstants.EQUAL)
              .append(STRING_PLACEHOLDER)
              .toString();

      /**
        * Example singleton log (no ls or lc field)
        * h=KARMA t=2019-10-25T20:21:14.518Z a=abfstest01.dfs.core.windows.net
        * c=abfs-testcontainer-84828169-6488-4a62-a875-1e674275a29f cr=delete ce=deletePath r=Succeeded l=32 s=200
        * e= ci=95121dae-70a8-4187-b067-614091034558 ri=97effdcf-201f-0097-2d71-8bae00000000 ct=0 st=0 rt=0 bs=0 br=0 m=DELETE
        * u=https%3A%2F%2Fabfstest01.dfs.core.windows.net%2Fabfs-testcontainer%2Ftest%3Ftimeout%3D90%26recursive%3Dtrue
      */
      singletonLatencyReportingFormat = new StringBuilder()
              .append(commonReportingFormat)
              .append(STRING_PLACEHOLDER)
              .toString();

      /**
       * Example aggregate log
       * h=KARMA t=2019-10-25T20:21:14.518Z a=abfstest01.dfs.core.windows.net
       * c=abfs-testcontainer-84828169-6488-4a62-a875-1e674275a29f cr=delete ce=deletePath r=Succeeded l=32 ls=32 lc=1 s=200
       * e= ci=95121dae-70a8-4187-b067-614091034558 ri=97effdcf-201f-0097-2d71-8bae00000000 ct=0 st=0 rt=0 bs=0 br=0 m=DELETE
       * u=https%3A%2F%2Fabfstest01.dfs.core.windows.net%2Fabfs-testcontainer%2Ftest%3Ftimeout%3D90%26recursive%3Dtrue
       */
      aggregateLatencyReportingFormat = new StringBuilder()
              .append(commonReportingFormat)
              .append(AbfsHttpConstants.SINGLE_WHITE_SPACE)
              .append(LATENCY_SUM_KEY)
              .append(AbfsHttpConstants.EQUAL)
              .append(STRING_PLACEHOLDER)
              .append(AbfsHttpConstants.SINGLE_WHITE_SPACE)
              .append(LATENCY_COUNT_KEY)
              .append(AbfsHttpConstants.EQUAL)
              .append(STRING_PLACEHOLDER)
              .append(STRING_PLACEHOLDER)
              .toString();
    }
  }

  public void trackInfo(AbfsPerfInfo perfInfo) {
    if (!enabled) {
      return;
    }

    if (isValidInstant(perfInfo.getAggregateStart()) && perfInfo.getAggregateCount() > 0) {
      recordClientLatency(
              perfInfo.getTrackingStart(),
              perfInfo.getTrackingEnd(),
              perfInfo.getCallerName(),
              perfInfo.getCalleeName(),
              perfInfo.getSuccess(),
              perfInfo.getAggregateStart(),
              perfInfo.getAggregateCount(),
              perfInfo.getResult());
    } else {
      recordClientLatency(
              perfInfo.getTrackingStart(),
              perfInfo.getTrackingEnd(),
              perfInfo.getCallerName(),
              perfInfo.getCalleeName(),
              perfInfo.getSuccess(),
              perfInfo.getResult());
    }
  }

  public Instant getLatencyInstant() {
    if (!enabled) {
      return null;
    }

    return Instant.now();
  }

  private void recordClientLatency(
          Instant operationStart,
          Instant operationStop,
          String callerName,
          String calleeName,
          boolean success,
          AbfsPerfLoggable res) {

    Instant trackerStart = Instant.now();
    long latency = isValidInstant(operationStart) && isValidInstant(operationStop)
            ? Duration.between(operationStart, operationStop).toMillis() : -1;

    String latencyDetails = String.format(singletonLatencyReportingFormat,
            Instant.now(),
            callerName,
            calleeName,
            success ? "Succeeded" : "Failed",
            latency,
            res == null ? "" : (" " + res.getLogString()));

    this.offerToQueue(trackerStart, latencyDetails);
  }

  private void recordClientLatency(
          Instant operationStart,
          Instant operationStop,
          String callerName,
          String calleeName,
          boolean success,
          Instant aggregateStart,
          long aggregateCount,
          AbfsPerfLoggable res){

    Instant trackerStart = Instant.now();
    long latency = isValidInstant(operationStart) && isValidInstant(operationStop)
            ? Duration.between(operationStart, operationStop).toMillis() : -1;
    long aggregateLatency = isValidInstant(aggregateStart) && isValidInstant(operationStop)
            ? Duration.between(aggregateStart, operationStop).toMillis() : -1;

    String latencyDetails = String.format(aggregateLatencyReportingFormat,
            Instant.now(),
            callerName,
            calleeName,
            success ? "Succeeded" : "Failed",
            latency,
            aggregateLatency,
            aggregateCount,
            res == null ? "" : (" " + res.getLogString()));

    offerToQueue(trackerStart, latencyDetails);
  }

  public String getClientLatency() {
    if (!enabled) {
      return null;
    }

    Instant trackerStart = Instant.now();
    String latencyDetails = queue.poll(); // non-blocking pop

    if (LOG.isDebugEnabled()) {
      Instant stop = Instant.now();
      long elapsed = Duration.between(trackerStart, stop).toMillis();
      LOG.debug("Dequeued latency info [{} ms]: {}", elapsed, latencyDetails);
    }

    return latencyDetails;
  }

  private void offerToQueue(Instant trackerStart, String latencyDetails) {
    queue.offer(latencyDetails); // non-blocking append

    if (LOG.isDebugEnabled()) {
      Instant trackerStop = Instant.now();
      long elapsed = Duration.between(trackerStart, trackerStop).toMillis();
      LOG.debug("Queued latency info [{} ms]: {}", elapsed, latencyDetails);
    }
  }

  private boolean isValidInstant(Instant testInstant) {
    return testInstant != null && testInstant != Instant.MIN && testInstant != Instant.MAX;
  }
}