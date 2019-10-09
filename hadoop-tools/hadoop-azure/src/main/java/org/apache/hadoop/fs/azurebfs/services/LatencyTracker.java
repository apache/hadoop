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

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * {@code LatencyTracker} keeps track of service latencies observed by {@code AbfsClient}. Every request adds
 * its information (success/failure, latency etc) to the {@code LatencyTracker}'s queue.
 * When a request is made, we check {@code LatencyTracker} to see if there are any latency numbers to be reported.
 * If there are any, the stats are added to an HTTP header ({@code x-ms-abfs-client-latency}) on the next request. *
 */
public class LatencyTracker {

  // the logger
  private static final Logger LOG = LoggerFactory.getLogger(LatencyTracker.class);

  // the queue to hold latency information
  private final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();

  // whether the latency tracker has been enabled
  private boolean enabled = false;

  // the host name
  private String hostName;

  // the file system name
  private String filesystemName;

  // the account name
  private String accountName;

  // singleton latency reporting format
  private String singletonLatencyReportingFormat;

  // aggregate latency reporting format
  private String aggregateLatencyReportingFormat;

  public LatencyTracker(String filesystemName, String accountName, AbfsConfiguration configuration) {
    this(filesystemName, accountName, configuration.shouldTrackLatency());
  }

  protected LatencyTracker(String filesystemName, String accountName, boolean enabled) {
    this.enabled = enabled;
    this.filesystemName = filesystemName;
    this.accountName = accountName;

    LOG.debug("LatencyTracker configuration: {}", enabled);

    if (enabled) {
      try {
        hostName = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        hostName = "UnknownHost";
      }

      singletonLatencyReportingFormat = "h=" + hostName + " t=%s a=" + accountName + " c=" + filesystemName + " cr=%s ce=%s r=%s l=%s%s";
      aggregateLatencyReportingFormat = "h=" + hostName + " t=%s a=" + accountName + " c=" + filesystemName + " cr=%s ce=%s r=%s l=%s ls=%s lc=%s%s";
    }
  }

  public void recordClientLatency(
          Instant operationStart,
          String callerName,
          String calleeName,
          boolean success,
          AbfsHttpOperation res) {
    if (!enabled) {
      return;
    }

    Instant operationStop = getLatencyInstant();

    recordClientLatency(operationStart, operationStop, callerName, calleeName, success, res);
  }

  public void recordClientLatency(
          Instant operationStart,
          Instant operationStop,
          String callerName,
          String calleeName,
          boolean success,
          AbfsHttpOperation res) {
    if (!enabled) {
      return;
    }

    Instant trackerStart = Instant.now();
    long latency = isValidInstant(operationStart) && isValidInstant(operationStop)
            ? Duration.between(operationStart, operationStop).toMillis() : -1;

    String latencyDetails = String.format(singletonLatencyReportingFormat,
            Instant.now(),
            callerName,
            calleeName,
            success ? "Succeeded" : "Failed",
            latency,
            res == null ? "" : (" " + res.toKvpString()));

    this.offerToQueue(trackerStart, latencyDetails);
  }

  public void recordClientLatency(
          Instant operationStart,
          String callerName,
          String calleeName,
          boolean success,
          Instant aggregateStart,
          long aggregateCount,
          AbfsHttpOperation res) {
    if (!enabled) {
      return;
    }

    Instant operationStop = getLatencyInstant();

    recordClientLatency(operationStart, operationStop, callerName, calleeName, success, aggregateStart, aggregateCount, res);
  }

  public void recordClientLatency(
          Instant operationStart,
          Instant operationStop,
          String callerName,
          String calleeName,
          boolean success,
          Instant aggregateStart,
          long aggregateCount,
          AbfsHttpOperation res){
    if (!enabled) {
      return;
    }

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
            res == null ? "" : (" " + res.toKvpString()));

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
      LOG.debug(String.format("Dequeued latency info [%s ms]: %s", elapsed, latencyDetails));
    }

    return latencyDetails;
  }

  public Instant getLatencyInstant() {
    if (!enabled) {
      return null;
    }

    return Instant.now();
  }

  private void offerToQueue(Instant trackerStart, String latencyDetails) {
    queue.offer(latencyDetails); // non-blocking append

    if (LOG.isDebugEnabled()) {
      Instant trackerStop = Instant.now();
      long elapsed = Duration.between(trackerStart, trackerStop).toMillis();
      LOG.debug(String.format("Queued latency info [%s ms]: %s", elapsed, latencyDetails));
    }
  }

  private boolean isValidInstant(Instant testInstant) {
    return testInstant != null && testInstant != Instant.MIN && testInstant != Instant.MAX;
  }
}