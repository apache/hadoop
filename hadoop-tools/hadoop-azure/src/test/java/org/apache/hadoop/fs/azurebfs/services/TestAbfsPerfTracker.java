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

import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the latency tracker for ABFS.
 *
 */
public final class TestAbfsPerfTracker {
  private static final Logger LOG = LoggerFactory.getLogger(TestAbfsPerfTracker.class);
  private static ExecutorService executorService = null;
  private static final int TEST_AGGREGATE_COUNT = 42;
  private final String filesystemName = "bogusFilesystemName";
  private final String accountName = "bogusAccountName";
  private final URL url;

  public TestAbfsPerfTracker() throws Exception {
    this.url = new URL("http", "www.microsoft.com", "/bogusFile");
  }

  @Before
  public void setUp() throws Exception {
    executorService = Executors.newCachedThreadPool();
  }

  @After
  public void tearDown() throws Exception {
    executorService.shutdown();
  }

  @Test
  public void verifyDisablingOfTracker() throws Exception {
    // verify that disabling of the tracker works
    AbfsPerfTracker abfsPerfTracker = new AbfsPerfTracker(accountName, filesystemName, false);

    String latencyDetails = abfsPerfTracker.getClientLatency();
    assertThat(latencyDetails).describedAs("AbfsPerfTracker should be empty").isNull();

    try (AbfsPerfInfo tracker = new AbfsPerfInfo(abfsPerfTracker, "disablingCaller",
            "disablingCallee")) {
      AbfsHttpOperation op = new AbfsHttpOperation(url, "GET", new ArrayList<>());
      tracker.registerResult(op).registerSuccess(true);
    }

    latencyDetails = abfsPerfTracker.getClientLatency();
    assertThat(latencyDetails).describedAs("AbfsPerfTracker should return no record").isNull();
  }

  @Test
  public void verifyTrackingForSingletonLatencyRecords() throws Exception {
    // verify that tracking for singleton latency records works as expected
    final int numTasks = 100;
    AbfsPerfTracker abfsPerfTracker = new AbfsPerfTracker(accountName, filesystemName, true);

    String latencyDetails = abfsPerfTracker.getClientLatency();
    assertThat(latencyDetails).describedAs("AbfsPerfTracker should be empty").isNull();

    List<Callable<Integer>> tasks = new ArrayList<>();
    AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<>());

    for (int i = 0; i < numTasks; i++) {
      tasks.add(() -> {
        try (AbfsPerfInfo tracker = new AbfsPerfInfo(abfsPerfTracker, "oneOperationCaller",
                "oneOperationCallee")) {
          tracker.registerResult(httpOperation).registerSuccess(true);
          return 0;
        }
      });
    }

    for (Future<Integer> fr: executorService.invokeAll(tasks)) {
      fr.get();
    }

    for (int i = 0; i < numTasks; i++) {
      latencyDetails = abfsPerfTracker.getClientLatency();
      assertThat(latencyDetails).describedAs("AbfsPerfTracker should return non-null record").isNotNull();
      assertThat(latencyDetails).describedAs("Latency record should be in the correct format")
        .containsPattern("h=[^ ]* t=[^ ]* a=bogusFilesystemName c=bogusAccountName cr=oneOperationCaller"
          + " ce=oneOperationCallee r=Succeeded l=[0-9]+ s=0 e= ci=[^ ]* ri=[^ ]* bs=0 br=0 m=GET"
          + " u=http%3A%2F%2Fwww.microsoft.com%2FbogusFile");
    }

    latencyDetails = abfsPerfTracker.getClientLatency();
    assertThat(latencyDetails).describedAs("AbfsPerfTracker should return no record").isNull();
  }

  @Test
  public void verifyTrackingForAggregateLatencyRecords() throws Exception {
    // verify that tracking of aggregate latency records works as expected
    final int numTasks = 100;
    AbfsPerfTracker abfsPerfTracker = new AbfsPerfTracker(accountName, filesystemName, true);

    String latencyDetails = abfsPerfTracker.getClientLatency();
    assertThat(latencyDetails).describedAs("AbfsPerfTracker should be empty").isNull();

    List<Callable<Integer>> tasks = new ArrayList<>();
    AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<>());

    for (int i = 0; i < numTasks; i++) {
      tasks.add(() -> {
        try (AbfsPerfInfo tracker = new AbfsPerfInfo(abfsPerfTracker, "oneOperationCaller",
                "oneOperationCallee")) {
          tracker.registerResult(httpOperation).registerSuccess(true)
                  .registerAggregates(Instant.now(), TEST_AGGREGATE_COUNT);
          return 0;
        }
      });
    }

    for (Future<Integer> fr: executorService.invokeAll(tasks)) {
      fr.get();
    }

    for (int i = 0; i < numTasks; i++) {
      latencyDetails = abfsPerfTracker.getClientLatency();
      assertThat(latencyDetails).describedAs("AbfsPerfTracker should return non-null record").isNotNull();
      assertThat(latencyDetails).describedAs("Latency record should be in the correct format")
        .containsPattern("h=[^ ]* t=[^ ]* a=bogusFilesystemName c=bogusAccountName cr=oneOperationCaller"
                + " ce=oneOperationCallee r=Succeeded l=[0-9]+ ls=[0-9]+ lc=" + TEST_AGGREGATE_COUNT
                + " s=0 e= ci=[^ ]* ri=[^ ]* bs=0 br=0 m=GET u=http%3A%2F%2Fwww.microsoft.com%2FbogusFile");
    }

    latencyDetails = abfsPerfTracker.getClientLatency();
    assertThat(latencyDetails).describedAs("AbfsPerfTracker should return no record").isNull();
  }

  @Test
  public void verifyRecordingSingletonLatencyIsCheapWhenDisabled() throws Exception {
    // when latency tracker is disabled, we expect it to take time equivalent to checking a boolean value
    final double maxLatencyWhenDisabledMs = 1000;
    final double minLatencyWhenDisabledMs = 0;
    final long numTasks = 1000;
    long aggregateLatency = 0;
    AbfsPerfTracker abfsPerfTracker = new AbfsPerfTracker(accountName, filesystemName, false);
    List<Callable<Long>> tasks = new ArrayList<>();
    final AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<>());

    for (int i = 0; i < numTasks; i++) {
      tasks.add(() -> {
        Instant startRecord = Instant.now();

        try (AbfsPerfInfo tracker = new AbfsPerfInfo(abfsPerfTracker, "oneOperationCaller",
                "oneOperationCallee")) {
          tracker.registerResult(httpOperation).registerSuccess(true);
        }

        long latencyRecord = Duration.between(startRecord, Instant.now()).toMillis();
        LOG.debug("Spent {} ms in recording latency.", latencyRecord);
        return latencyRecord;
      });
    }

    for (Future<Long> fr: executorService.invokeAll(tasks)) {
      aggregateLatency += fr.get();
    }

    double averageRecordLatency = aggregateLatency / numTasks;
    assertThat(averageRecordLatency).describedAs("Average time for recording singleton latencies should be bounded")
      .isBetween(minLatencyWhenDisabledMs, maxLatencyWhenDisabledMs);
  }

  @Test
  public void verifyRecordingAggregateLatencyIsCheapWhenDisabled() throws Exception {
    // when latency tracker is disabled, we expect it to take time equivalent to checking a boolean value
    final double maxLatencyWhenDisabledMs = 1000;
    final double minLatencyWhenDisabledMs = 0;
    final long numTasks = 1000;
    long aggregateLatency = 0;
    AbfsPerfTracker abfsPerfTracker = new AbfsPerfTracker(accountName, filesystemName, false);
    List<Callable<Long>> tasks = new ArrayList<>();
    final AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<>());

    for (int i = 0; i < numTasks; i++) {
      tasks.add(() -> {
        Instant startRecord = Instant.now();

        try (AbfsPerfInfo tracker = new AbfsPerfInfo(abfsPerfTracker, "oneOperationCaller",
                "oneOperationCallee")) {
          tracker.registerResult(httpOperation).registerSuccess(true)
                  .registerAggregates(startRecord, TEST_AGGREGATE_COUNT);
        }

        long latencyRecord = Duration.between(startRecord, Instant.now()).toMillis();
        LOG.debug("Spent {} ms in recording latency.", latencyRecord);
        return latencyRecord;
      });
    }

    for (Future<Long> fr: executorService.invokeAll(tasks)) {
      aggregateLatency += fr.get();
    }

    double averageRecordLatency = aggregateLatency / numTasks;
    assertThat(averageRecordLatency).describedAs("Average time for recording aggregate latencies should be bounded")
      .isBetween(minLatencyWhenDisabledMs, maxLatencyWhenDisabledMs);
  }

  @Test
  public void verifyGettingLatencyRecordsIsCheapWhenDisabled() throws Exception {
    // when latency tracker is disabled, we expect it to take time equivalent to checking a boolean value
    final double maxLatencyWhenDisabledMs = 1000;
    final double minLatencyWhenDisabledMs = 0;
    final long numTasks = 1000;
    long aggregateLatency = 0;
    AbfsPerfTracker abfsPerfTracker = new AbfsPerfTracker(accountName, filesystemName, false);
    List<Callable<Long>> tasks = new ArrayList<>();

    for (int i = 0; i < numTasks; i++) {
      tasks.add(() -> {
        Instant startGet = Instant.now();
        abfsPerfTracker.getClientLatency();
        long latencyGet = Duration.between(startGet, Instant.now()).toMillis();
        LOG.debug("Spent {} ms in retrieving latency record.", latencyGet);
        return latencyGet;
      });
    }

    for (Future<Long> fr: executorService.invokeAll(tasks)) {
      aggregateLatency += fr.get();
    }

    double averageRecordLatency = aggregateLatency / numTasks;
    assertThat(averageRecordLatency).describedAs("Average time for getting latency records should be bounded")
      .isBetween(minLatencyWhenDisabledMs, maxLatencyWhenDisabledMs);
  }

  @Test
  public void verifyRecordingSingletonLatencyIsCheapWhenEnabled() throws Exception {
    final double maxLatencyWhenDisabledMs = 5000;
    final double minLatencyWhenDisabledMs = 0;
    final long numTasks = 1000;
    long aggregateLatency = 0;
    AbfsPerfTracker abfsPerfTracker = new AbfsPerfTracker(accountName, filesystemName, true);
    List<Callable<Long>> tasks = new ArrayList<>();
    final AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<>());

    for (int i = 0; i < numTasks; i++) {
      tasks.add(() -> {
        Instant startRecord = Instant.now();

        try (AbfsPerfInfo tracker = new AbfsPerfInfo(abfsPerfTracker, "oneOperationCaller",
                "oneOperationCallee")) {
          tracker.registerResult(httpOperation).registerSuccess(true);
        }

        long latencyRecord = Duration.between(startRecord, Instant.now()).toMillis();
        LOG.debug("Spent {} ms in recording latency.", latencyRecord);
        return latencyRecord;
      });
    }

    for (Future<Long> fr: executorService.invokeAll(tasks)) {
      aggregateLatency += fr.get();
    }

    double averageRecordLatency = aggregateLatency / numTasks;
    assertThat(averageRecordLatency).describedAs("Average time for recording singleton latencies should be bounded")
      .isBetween(minLatencyWhenDisabledMs, maxLatencyWhenDisabledMs);
  }

  @Test
  public void verifyRecordingAggregateLatencyIsCheapWhenEnabled() throws Exception {
    final double maxLatencyWhenDisabledMs = 5000;
    final double minLatencyWhenDisabledMs = 0;
    final long numTasks = 1000;
    long aggregateLatency = 0;
    AbfsPerfTracker abfsPerfTracker = new AbfsPerfTracker(accountName, filesystemName, true);
    List<Callable<Long>> tasks = new ArrayList<>();
    final AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<>());

    for (int i = 0; i < numTasks; i++) {
      tasks.add(() -> {
        Instant startRecord = Instant.now();

        try (AbfsPerfInfo tracker = new AbfsPerfInfo(abfsPerfTracker, "oneOperationCaller",
                "oneOperationCallee")) {
          tracker.registerResult(httpOperation).registerSuccess(true).
                  registerAggregates(startRecord, TEST_AGGREGATE_COUNT);
        }

        long latencyRecord = Duration.between(startRecord, Instant.now()).toMillis();
        LOG.debug("Spent {} ms in recording latency.", latencyRecord);
        return latencyRecord;
      });
    }

    for (Future<Long> fr: executorService.invokeAll(tasks)) {
      aggregateLatency += fr.get();
    }

    double averageRecordLatency = aggregateLatency / numTasks;
    assertThat(averageRecordLatency).describedAs("Average time for recording aggregate latencies is bounded")
      .isBetween(minLatencyWhenDisabledMs, maxLatencyWhenDisabledMs);
  }

  @Test
  public void verifyGettingLatencyRecordsIsCheapWhenEnabled() throws Exception {
    final double maxLatencyWhenDisabledMs = 5000;
    final double minLatencyWhenDisabledMs = 0;
    final long numTasks = 1000;
    long aggregateLatency = 0;
    AbfsPerfTracker abfsPerfTracker = new AbfsPerfTracker(accountName, filesystemName, true);
    List<Callable<Long>> tasks = new ArrayList<>();

    for (int i = 0; i < numTasks; i++) {
      tasks.add(() -> {
        Instant startRecord = Instant.now();
        abfsPerfTracker.getClientLatency();
        long latencyRecord = Duration.between(startRecord, Instant.now()).toMillis();
        LOG.debug("Spent {} ms in recording latency.", latencyRecord);
        return latencyRecord;
      });
    }

    for (Future<Long> fr: executorService.invokeAll(tasks)) {
      aggregateLatency += fr.get();
    }

    double averageRecordLatency = aggregateLatency / numTasks;
    assertThat(averageRecordLatency).describedAs("Average time for getting latency records should be bounded")
      .isBetween(minLatencyWhenDisabledMs, maxLatencyWhenDisabledMs);
  }

  @Test
  public void verifyNoExceptionOnInvalidInput() throws Exception {
    Instant testInstant = Instant.now();
    AbfsPerfTracker abfsPerfTrackerDisabled = new AbfsPerfTracker(accountName, filesystemName, false);
    AbfsPerfTracker abfsPerfTrackerEnabled = new AbfsPerfTracker(accountName, filesystemName, true);
    final AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<AbfsHttpHeader>());

    verifyNoException(abfsPerfTrackerDisabled);
    verifyNoException(abfsPerfTrackerEnabled);
  }

  private void verifyNoException(AbfsPerfTracker abfsPerfTracker) throws Exception {
    Instant testInstant = Instant.now();
    final AbfsHttpOperation httpOperation = new AbfsHttpOperation(url, "GET", new ArrayList<AbfsHttpHeader>());

    try (
            AbfsPerfInfo tracker01 = new AbfsPerfInfo(abfsPerfTracker, null, null);
            AbfsPerfInfo tracker02 = new AbfsPerfInfo(abfsPerfTracker, "test", null);
            AbfsPerfInfo tracker03 = new AbfsPerfInfo(abfsPerfTracker, "test", "test");
            AbfsPerfInfo tracker04 = new AbfsPerfInfo(abfsPerfTracker, "test", "test");

            AbfsPerfInfo tracker05 = new AbfsPerfInfo(abfsPerfTracker, null, null);
            AbfsPerfInfo tracker06 = new AbfsPerfInfo(abfsPerfTracker, "test", null);
            AbfsPerfInfo tracker07 = new AbfsPerfInfo(abfsPerfTracker, "test", "test");
            AbfsPerfInfo tracker08 = new AbfsPerfInfo(abfsPerfTracker, "test", "test");
            AbfsPerfInfo tracker09 = new AbfsPerfInfo(abfsPerfTracker, "test", "test");
            AbfsPerfInfo tracker10 = new AbfsPerfInfo(abfsPerfTracker, "test", "test");

            AbfsPerfInfo tracker11 = new AbfsPerfInfo(abfsPerfTracker, "test", "test");
            AbfsPerfInfo tracker12 = new AbfsPerfInfo(abfsPerfTracker, "test", "test");
            AbfsPerfInfo tracker13 = new AbfsPerfInfo(abfsPerfTracker, "test", "test");
    ) {
      tracker01.registerResult(null).registerSuccess(false);
      tracker02.registerResult(null).registerSuccess(false);
      tracker03.registerResult(null).registerSuccess(false);
      tracker04.registerResult(httpOperation).registerSuccess(false);

      tracker05.registerResult(null).registerSuccess(false).registerAggregates(null, 0);
      tracker06.registerResult(null).registerSuccess(false).registerAggregates(null, 0);
      tracker07.registerResult(null).registerSuccess(false).registerAggregates(null, 0);
      tracker08.registerResult(httpOperation).registerSuccess(false).registerAggregates(null, 0);
      tracker09.registerResult(httpOperation).registerSuccess(false).registerAggregates(Instant.now(), 0);
      tracker10.registerResult(httpOperation).registerSuccess(false).registerAggregates(Instant.now(), TEST_AGGREGATE_COUNT);

      tracker11.registerResult(httpOperation).registerSuccess(false).registerAggregates(testInstant, TEST_AGGREGATE_COUNT);
      tracker12.registerResult(httpOperation).registerSuccess(false).registerAggregates(Instant.MAX, TEST_AGGREGATE_COUNT);
      tracker13.registerResult(httpOperation).registerSuccess(false).registerAggregates(Instant.MIN, TEST_AGGREGATE_COUNT);
    }
  }

  /**
   * Test helper method to create an AbfsPerfTracker instance.
   * @param abfsConfig active test abfs config
   * @return instance of AbfsPerfTracker
   */
  public static AbfsPerfTracker getAPerfTrackerInstance(AbfsConfiguration abfsConfig) {
    AbfsPerfTracker tracker = new AbfsPerfTracker("test",
        abfsConfig.getAccountName(), abfsConfig);
    return tracker;
  }
}