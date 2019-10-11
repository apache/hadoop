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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

/**
 * Test the latency tracker for abfs
 *
 */
public final class TestLatencyTracker {
  private static final Logger LOG = LoggerFactory.getLogger(TestLatencyTracker.class);
  private static final int TEST_AGGREGATE_LATENCY = 42;
  private final String filesystemName = "bogusFilesystemName";
  private final String accountName = "bogusAccountName";
  private final URL url;

  public TestLatencyTracker() throws Exception {
    this.url = new URL("http", "www.microsoft.com", "/bogusFile");
  }

  @Test
  public void verifyDisablingOfTracker() throws Exception {
    // verify that disabling of the tracker works
    LatencyTracker latencyTracker = new LatencyTracker(accountName, filesystemName, false);

    String latencyDetails = latencyTracker.getClientLatency();
    Assert.assertNull("LatencyTracker should be empty", latencyDetails);

    latencyTracker.recordClientLatency(Instant.now(), "disablingCaller", "disablingCallee", true,
            new AbfsHttpOperation(url, "GET", new ArrayList<AbfsHttpHeader>()));

    latencyDetails = latencyTracker.getClientLatency();
    Assert.assertNull("LatencyTracker should return no record", latencyDetails);
  }

  @Test
  public void verifyTrackingForSingletonLatencyRecords() throws Exception {
    // verify that tracking for singleton latency records works as expected
    final int numTasks = 100;
    LatencyTracker latencyTracker = new LatencyTracker(accountName, filesystemName, true);

    String latencyDetails = latencyTracker.getClientLatency();
    Assert.assertNull("LatencyTracker should be empty", latencyDetails);

    ExecutorService executorService = Executors.newCachedThreadPool();
    List<Callable<Integer>> tasks = new ArrayList<Callable<Integer>>();
    AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<AbfsHttpHeader>());

    for (int i=0; i < numTasks; i++) {
      Callable<Integer> c = new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          latencyTracker.recordClientLatency(Instant.now(), "oneOperationCaller", "oneOperationCallee", true, httpOperation);
          return 0;
        }
      };
      tasks.add(c);
    }

    for (Future<Integer> fr: executorService.invokeAll(tasks)) {
      fr.get();
    }

    for (int i=0; i < numTasks; i++) {
      latencyDetails = latencyTracker.getClientLatency();
      Assert.assertNotNull("LatencyTracker should return non-null record", latencyDetails);
      Assert.assertTrue("Latency record should be in the correct format", Pattern.matches(
              "h=[^ ]* t=[^ ]* a=bogusFilesystemName c=bogusAccountName cr=oneOperationCaller ce=oneOperationCallee r=Succeeded l=[0-9]+"
                      + " s=0 e= ci=[^ ]* ri=[^ ]* bs=0 br=0 m=GET u=http%3A%2F%2Fwww.microsoft.com%2FbogusFile", latencyDetails));
    }
  }

  @Test
  public void verifyTrackingForAggregateLatencyRecords() throws Exception {
    // verify that tracking of aggregate latency records works as expected
    final int numTasks = 100;
    LatencyTracker latencyTracker = new LatencyTracker(accountName, filesystemName, true);

    String latencyDetails = latencyTracker.getClientLatency();
    Assert.assertNull("LatencyTracker should be empty", latencyDetails);

    ExecutorService executorService = Executors.newCachedThreadPool();
    List<Callable<Integer>> tasks = new ArrayList<Callable<Integer>>();
    AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<AbfsHttpHeader>());

    for (int i=0; i < numTasks; i++) {
      Callable<Integer> c = new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          // test latency tracking when aggregate latency numbers are also passed
          latencyTracker.recordClientLatency(Instant.now(), "oneOperationCaller", "oneOperationCallee", true, Instant.now(), TEST_AGGREGATE_LATENCY, httpOperation);
          return 0;
        }
      };
      tasks.add(c);
    }

    for (Future<Integer> fr: executorService.invokeAll(tasks)) {
      fr.get();
    }

    for (int i=0; i < numTasks; i++) {
      latencyDetails = latencyTracker.getClientLatency();
      Assert.assertNotNull("LatencyTracker should return non-null record", latencyDetails);
      Assert.assertTrue("Latency record should be in the correct format", Pattern.matches(
              "h=[^ ]* t=[^ ]* a=bogusFilesystemName c=bogusAccountName cr=oneOperationCaller ce=oneOperationCallee r=Succeeded l=[0-9]+"
                      + " ls=[0-9]+ lc=" + TEST_AGGREGATE_LATENCY + " s=0 e= ci=[^ ]* ri=[^ ]* bs=0 br=0 m=GET u=http%3A%2F%2Fwww.microsoft.com%2FbogusFile", latencyDetails));
    }
  }

  @Test
  public void verifyRecordingSingletonLatencyIsCheapWhenDisabled() throws Exception {
    // when latency tracker is disabled, we expect it to take time equivalent to checking a boolean value
    final long maxLatencyWhenDisabledMs = 1;
    final long minLatencyWhenDisabledMs = 0;
    final long numTasks = 1000;
    long aggregateLatency = 0;
    LatencyTracker latencyTracker = new LatencyTracker(accountName, filesystemName, false);

    ExecutorService executorService = Executors.newCachedThreadPool();
    List<Callable<Long>> tasks = new ArrayList<Callable<Long>>();
    final AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<AbfsHttpHeader>());

    for (int i=0; i < numTasks; i++) {
      Callable<Long> c = new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          Instant startRecord = Instant.now();

          try{
          } finally {
            latencyTracker.recordClientLatency(startRecord, "oneOperationCaller", "oneOperationCallee", true, httpOperation);
          }

          long latencyRecord = Duration.between(startRecord, Instant.now()).toMillis();
          LOG.debug("Spent {} ms in recording latency.", latencyRecord);
          return latencyRecord;
        }
      };
      tasks.add(c);
    }

    for (Future<Long> fr: executorService.invokeAll(tasks)) {
      aggregateLatency += fr.get();
    }

    double averageRecordLatency = aggregateLatency/numTasks;
    Assert.assertTrue(String.format("Average time for recording singleton latencies, %s ms should be in the range [%s, %s).",
            averageRecordLatency, minLatencyWhenDisabledMs, maxLatencyWhenDisabledMs),
            averageRecordLatency < maxLatencyWhenDisabledMs && averageRecordLatency >= minLatencyWhenDisabledMs);
  }

  @Test
  public void verifyRecordingAggregateLatencyIsCheapWhenDisabled() throws Exception {
    // when latency tracker is disabled, we expect it to take time equivalent to checking a boolean value
    final long maxLatencyWhenDisabledMs = 1;
    final long minLatencyWhenDisabledMs = 0;
    final long numTasks = 1000;
    long aggregateLatency = 0;
    LatencyTracker latencyTracker = new LatencyTracker(accountName, filesystemName, false);

    ExecutorService executorService = Executors.newCachedThreadPool();
    List<Callable<Long>> tasks = new ArrayList<Callable<Long>>();
    final AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<AbfsHttpHeader>());

    for (int i=0; i < numTasks; i++) {
      Callable<Long> c = new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          Instant startRecord = Instant.now();

          try {
            // placeholder try block
          } finally {
            latencyTracker.recordClientLatency(startRecord, "oneOperationCaller", "oneOperationCallee", true, startRecord, TEST_AGGREGATE_LATENCY, httpOperation);
          }

          long latencyRecord = Duration.between(startRecord, Instant.now()).toMillis();
          LOG.debug("Spent {} ms in recording latency.", latencyRecord);
          return latencyRecord;
        }
      };
      tasks.add(c);
    }

    for (Future<Long> fr: executorService.invokeAll(tasks)) {
      aggregateLatency += fr.get();
    }

    double averageRecordLatency = aggregateLatency/numTasks;
    Assert.assertTrue(String.format("Average time for recording singleton latencies, %s ms should be in the range [%s, %s).",
            averageRecordLatency, minLatencyWhenDisabledMs, maxLatencyWhenDisabledMs),
            averageRecordLatency < maxLatencyWhenDisabledMs && averageRecordLatency >= minLatencyWhenDisabledMs);
  }

  @Test
  public void verifyGettingLatencyRecordsIsCheapWhenDisabled() throws Exception {
    // when latency tracker is disabled, we expect it to take time equivalent to checking a boolean value
    final long maxLatencyWhenDisabledMs = 1;
    final long minLatencyWhenDisabledMs = 0;
    final long numTasks = 1000;
    long aggregateLatency = 0;
    LatencyTracker latencyTracker = new LatencyTracker(accountName, filesystemName, false);

    ExecutorService executorService = Executors.newCachedThreadPool();
    List<Callable<Long>> tasks = new ArrayList<Callable<Long>>();
    final AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<AbfsHttpHeader>());

    for (int i=0; i < numTasks; i++) {
      Callable<Long> c = new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          Instant startGet = Instant.now();
          latencyTracker.getClientLatency();
          long latencyGet = Duration.between(startGet, Instant.now()).toMillis();
          LOG.debug("Spent {} ms in retrieving latency record.", latencyGet);
          return latencyGet;
        }
      };
      tasks.add(c);
    }

    for (Future<Long> fr: executorService.invokeAll(tasks)) {
      aggregateLatency += fr.get();
    }

    double averageRecordLatency = aggregateLatency/numTasks;
    Assert.assertTrue(String.format("Average time for getting latency records, %s ms should be in the range [%s, %s).",
            averageRecordLatency, minLatencyWhenDisabledMs, maxLatencyWhenDisabledMs),
            averageRecordLatency < maxLatencyWhenDisabledMs && averageRecordLatency >= minLatencyWhenDisabledMs);
  }

  @Test
  public void verifyRecordingSingletonLatencyIsCheapWhenEnabled() throws Exception {
    final long maxLatencyWhenDisabledMs = 50;
    final long minLatencyWhenDisabledMs = 0;
    final long numTasks = 1000;
    long aggregateLatency = 0;
    LatencyTracker latencyTracker = new LatencyTracker(accountName, filesystemName, true);

    ExecutorService executorService = Executors.newCachedThreadPool();
    List<Callable<Long>> tasks = new ArrayList<Callable<Long>>();
    final AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<AbfsHttpHeader>());

    for (int i=0; i < numTasks; i++) {
      Callable<Long> c = new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          Instant startRecord = Instant.now();

          try {
            // placeholder try block
          } finally {
            latencyTracker.recordClientLatency(startRecord, "oneOperationCaller", "oneOperationCallee", true, httpOperation);
          }

          long latencyRecord = Duration.between(startRecord, Instant.now()).toMillis();
          LOG.debug("Spent {} ms in recording latency.", latencyRecord);
          return latencyRecord;
        }
      };
      tasks.add(c);
    }

    for (Future<Long> fr: executorService.invokeAll(tasks)) {
      aggregateLatency += fr.get();
    }

    double averageRecordLatency = aggregateLatency/numTasks;
    Assert.assertTrue(String.format("Average time for recording singleton latencies, %s ms should be in the range [%s, %s).",
            averageRecordLatency, minLatencyWhenDisabledMs, maxLatencyWhenDisabledMs),
            averageRecordLatency < maxLatencyWhenDisabledMs && averageRecordLatency >= minLatencyWhenDisabledMs);
  }

  @Test
  public void verifyRecordingAggregateLatencyIsCheapWhenEnabled() throws Exception {
    final long maxLatencyWhenDisabledMs = 50;
    final long minLatencyWhenDisabledMs = 0;
    final long numTasks = 1000;
    long aggregateLatency = 0;
    LatencyTracker latencyTracker = new LatencyTracker(accountName, filesystemName, true);

    ExecutorService executorService = Executors.newCachedThreadPool();
    List<Callable<Long>> tasks = new ArrayList<Callable<Long>>();
    final AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<AbfsHttpHeader>());

    for (int i=0; i < numTasks; i++) {
      Callable<Long> c = new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          Instant startRecord = Instant.now();

          try {
            // placeholder try block
          } finally {
            latencyTracker.recordClientLatency(startRecord, "oneOperationCaller", "oneOperationCallee", true, startRecord, TEST_AGGREGATE_LATENCY, httpOperation);
          }

          long latencyRecord = Duration.between(startRecord, Instant.now()).toMillis();
          LOG.debug("Spent {} ms in recording latency.", latencyRecord);
          return latencyRecord;
        }
      };
      tasks.add(c);
    }

    for (Future<Long> fr: executorService.invokeAll(tasks)) {
      aggregateLatency += fr.get();
    }

    double averageRecordLatency = aggregateLatency/numTasks;
    Assert.assertTrue(String.format("Average time for recording singleton latencies, %s ms should be in the range [%s, %s).",
            averageRecordLatency, minLatencyWhenDisabledMs, maxLatencyWhenDisabledMs),
            averageRecordLatency < maxLatencyWhenDisabledMs && averageRecordLatency >= minLatencyWhenDisabledMs);
  }

  @Test
  public void verifyGettingLatencyRecordsIsCheapWhenEnabled() throws Exception {
    final long maxLatencyWhenDisabledMs = 50;
    final long minLatencyWhenDisabledMs = 0;
    final long numTasks = 1000;
    long aggregateLatency = 0;
    LatencyTracker latencyTracker = new LatencyTracker(accountName, filesystemName, true);

    ExecutorService executorService = Executors.newCachedThreadPool();
    List<Callable<Long>> tasks = new ArrayList<Callable<Long>>();
    final AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<AbfsHttpHeader>());

    for (int i=0; i < numTasks; i++) {
      Callable<Long> c = new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          Instant startRecord = Instant.now();
          latencyTracker.getClientLatency();
          long latencyRecord = Duration.between(startRecord, Instant.now()).toMillis();
          LOG.debug("Spent {} ms in recording latency.", latencyRecord);
          return latencyRecord;
        }
      };
      tasks.add(c);
    }

    for (Future<Long> fr: executorService.invokeAll(tasks)) {
      aggregateLatency += fr.get();
    }

    double averageRecordLatency = aggregateLatency/numTasks;
    Assert.assertTrue(String.format("Average time for recording singleton latencies, %s ms should be in the range [%s, %s).",
            averageRecordLatency, minLatencyWhenDisabledMs, maxLatencyWhenDisabledMs),
            averageRecordLatency < maxLatencyWhenDisabledMs && averageRecordLatency >= minLatencyWhenDisabledMs);
  }

  @Test
  public void verifyNoExceptionOnInvalidInputWhenDisabled() throws Exception {
    Instant testInstant = Instant.now();
    LatencyTracker latencyTracker = new LatencyTracker(accountName, filesystemName, false);
    final AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<AbfsHttpHeader>());

    try {
      latencyTracker.recordClientLatency(null, null, null, false, null);
      latencyTracker.recordClientLatency(Instant.now(), null, null, false, null);
      latencyTracker.recordClientLatency(Instant.now(), "test", null, false, null);
      latencyTracker.recordClientLatency(Instant.now(), "test", "test", false, null);
      latencyTracker.recordClientLatency(Instant.now(), "test", "test", false, httpOperation);

      latencyTracker.recordClientLatency(null, null, null, null, false, null);
      latencyTracker.recordClientLatency(Instant.now(), null, null, null, false, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), null, null, false, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), "test", null, false, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), "test", "test", false, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), "test", "test", false, httpOperation);

      latencyTracker.recordClientLatency(testInstant, Instant.now(), null, null, false, null);
      latencyTracker.recordClientLatency(Instant.MAX, Instant.now(), null, null, false, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.MIN, null, null, false, null);

      latencyTracker.recordClientLatency(null, null, null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), null, null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), "test", null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), "test", "test", false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), "test", "test", false, Instant.now(), 0, null);
      latencyTracker.recordClientLatency(Instant.now(), "test", "test", false, Instant.now(), TEST_AGGREGATE_LATENCY, httpOperation);

      latencyTracker.recordClientLatency(null, null, null, null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), null, null, null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), null, null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), "test", null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), "test", "test", false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), "test", "test", false, Instant.now(), 0, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), "test", "test", false, Instant.now(), TEST_AGGREGATE_LATENCY, httpOperation);

      latencyTracker.recordClientLatency(testInstant, Instant.now(), null, null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.MAX, Instant.now(), null, null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.MIN, null, null, false, null, 0, null);


    } catch (Exception e) {
      Assert.assertTrue("There should be no exception", false);
    }
  }

  @Test
  public void verifyNoExceptionOnInvalidInputWhenEnabled() throws Exception {
    Instant testInstant = Instant.now();
    LatencyTracker latencyTracker = new LatencyTracker(accountName, filesystemName, true);
    final AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<AbfsHttpHeader>());

    try {
      latencyTracker.recordClientLatency(null, null, null, false, null);
      latencyTracker.recordClientLatency(Instant.now(), null, null, false, null);
      latencyTracker.recordClientLatency(Instant.now(), "test", null, false, null);
      latencyTracker.recordClientLatency(Instant.now(), "test", "test", false, null);
      latencyTracker.recordClientLatency(Instant.now(), "test", "test", false, httpOperation);

      latencyTracker.recordClientLatency(null, null, null, null, false, null);
      latencyTracker.recordClientLatency(Instant.now(), null, null, null, false, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), null, null, false, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), "test", null, false, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), "test", "test", false, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), "test", "test", false, httpOperation);

      latencyTracker.recordClientLatency(testInstant, Instant.now(), null, null, false, null);
      latencyTracker.recordClientLatency(Instant.MAX, Instant.now(), null, null, false, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.MIN, null, null, false, null);

      latencyTracker.recordClientLatency(null, null, null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), null, null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), "test", null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), "test", "test", false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), "test", "test", false, Instant.now(), 0, null);
      latencyTracker.recordClientLatency(Instant.now(), "test", "test", false, Instant.now(), TEST_AGGREGATE_LATENCY, httpOperation);

      latencyTracker.recordClientLatency(null, null, null, null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), null, null, null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), null, null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), "test", null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), "test", "test", false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), "test", "test", false, Instant.now(), 0, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.now(), "test", "test", false, Instant.now(), TEST_AGGREGATE_LATENCY, httpOperation);

      latencyTracker.recordClientLatency(testInstant, Instant.now(), null, null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.MAX, Instant.now(), null, null, false, null, 0, null);
      latencyTracker.recordClientLatency(Instant.now(), Instant.MIN, null, null, false, null, 0, null);
    } catch (Exception e){
      Assert.assertTrue("There should be no exception", false);
    }
  }
}