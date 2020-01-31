/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a.scale;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.Assume;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.impl.FunctionsRaisingIOE;
import org.apache.hadoop.fs.impl.WrappedIOException;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.auth.delegation.Csvout;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static org.apache.hadoop.fs.s3a.Constants.ENABLE_MULTI_DELETE;

/**
 * Test some scalable operations related to file renaming and deletion.
 * Much of the setup code is lifted from ILoadTestSessionCredentials;
 * whereas that was designed to overload an STS endpoint, this just
 * tries to overload a single S3 shard with too many bulk IO requests
 * -and so see what happens.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ILoadTestS3ABulkDeleteThrottling extends S3AScaleTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ILoadTestS3ABulkDeleteThrottling.class);

  protected static final int THREADS = 20;

  private static boolean previousTestThrottled;

  private final ExecutorService executor =
      HadoopExecutors.newFixedThreadPool(
          THREADS,
          new ThreadFactoryBuilder()
              .setNameFormat(" #%d")
              .build());

  private final CompletionService<Outcome>
      completionService = new ExecutorCompletionService<>(executor);

  private File dataDir;

  @Override
  public void setup() throws Exception {
    super.setup();
    Assume.assumeTrue("multipart delete disabled",
        getConf().getBoolean(ENABLE_MULTI_DELETE, true));
    dataDir = GenericTestUtils.getTestDir("throttling");
    dataDir.mkdirs();
  }

  @Test
  public void test_010_DeleteThrottlingSmall() throws Throwable {
    describe("test how S3 reacts to massive multipart deletion requests");
    maybeSleep();
    final File results = deleteFiles(100, 200);
    LOG.info("Test run completed against {}:\n see {}", getFileSystem(),
        results);
  }

  @Test
  public void test_011_sleep() throws Throwable {
    maybeSleep();
  }

  @Test
  public void test_020_DeleteThrottlingBigFiles() throws Throwable {
    describe("test how S3 reacts to massive multipart deletion requests");
    final File results = deleteFiles(20, 1000);
    LOG.info("Test run completed against {}:\n see {}", getFileSystem(),
        results);
    previousTestThrottled = true;
  }

  private void maybeSleep() throws InterruptedException {
    if (previousTestThrottled) {
      LOG.info("Sleeping briefly to let store recover");
      Thread.sleep(30_000);
      previousTestThrottled = false;
    }
  }
  /**
   * Fetch requests.
   * @param requests number of requests.
   * @throws Exception failure
   * @return
   */
  private File deleteFiles(final int requests, final int entries)
      throws Exception {
    File csvFile = new File(dataDir,
        String.format("delete-%03d-%04d.csv", requests, entries));
    describe("Issuing %d requests of size %d, saving log to %s",
        requests, entries, csvFile);
    Path basePath = path("testDeleteObjectThrottling");
    final S3AFileSystem fs = getFileSystem();
    final String base = fs.pathToKey(basePath);
    final List<DeleteObjectsRequest.KeyVersion> fileList
        = buildDeleteRequest(base, entries);
    final FileWriter out = new FileWriter(csvFile);
    Csvout csvout = new Csvout(out, "\t", "\n");
    Outcome.writeSchema(csvout);


    final ContractTestUtils.NanoTimer jobTimer =
        new ContractTestUtils.NanoTimer();


    for (int i = 0; i < requests; i++) {
      final int id = i;
      completionService.submit(() -> {
        final long startTime = System.currentTimeMillis();
        Thread.currentThread().setName("#" + id);
        LOG.info("Issuing request {}", id);
        final ContractTestUtils.NanoTimer timer =
            new ContractTestUtils.NanoTimer();
        Exception ex = null;
        try {
          fs.removeKeys(fileList, false, null);
        } catch (IOException e) {
          ex = e;
        }
        timer.end("Request " + id);
        return new Outcome(id, startTime, timer,
            ex);
      });
    }

    NanoTimerStats stats = new NanoTimerStats("Overall");
    NanoTimerStats success = new NanoTimerStats("Successful");
    NanoTimerStats throttled = new NanoTimerStats("Throttled");
    List<Outcome> throttledEvents = new ArrayList<>();
    for (int i = 0; i < requests; i++) {
      Outcome outcome = completionService.take().get();
      ContractTestUtils.NanoTimer timer = outcome.timer;
      Exception ex = outcome.exception;
      outcome.writeln(csvout);
      stats.add(timer);
      if (ex != null) {
        // throttling event occurred.
        LOG.info("Throttled at event {}", i, ex);
        throttled.add(timer);
        throttledEvents.add(outcome);
      } else {
        success.add(timer);
      }
    }

    csvout.close();

    jobTimer.end("Execution of operations");
    // now print the stats
    LOG.info("Summary file is " + csvFile);
    LOG.info("Made {} requests with {} throttle events\n: {}\n{}\n{}",
        requests,
        throttled.getCount(),
        stats,
        throttled,
        success);

    double duration = jobTimer.duration();
    double iops = requests * entries * 1.0e9 / duration;
    LOG.info(String.format("TPS %3f operations/second",
        iops));
    // log at debug
    if (LOG.isDebugEnabled()) {
      throttledEvents.stream().forEach((outcome -> {
        LOG.debug("{}: duration: {}",
            outcome.id, outcome.timer.elapsedTimeMs());
      }));
    }
    return csvFile;
  }


  private List<DeleteObjectsRequest.KeyVersion> buildDeleteRequest(
      String base, int count) {
    List<DeleteObjectsRequest.KeyVersion> request = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      request.add(new DeleteObjectsRequest.KeyVersion(
          String.format("%s/file-%04d", base, i)));
    }
    return request;
  }


  private <R> R wrap(FunctionsRaisingIOE.CallableRaisingIOE<R> callable) {
    try {
      return callable.apply();
    } catch (IOException e) {
      throw new WrappedIOException(e);
    }
  }

  /**
   * Outcome of one of the load operations.
   */
  private static class Outcome {

    private final int id;

    private final long startTime;

    private final ContractTestUtils.NanoTimer timer;

    private final Exception exception;

    Outcome(final int id,
        final long startTime,
        final ContractTestUtils.NanoTimer timer,
        final Exception exception) {
      this.id = id;
      this.startTime = startTime;
      this.timer = timer;
      this.exception = exception;
    }

    /**
     * Write this record.
     * @param out the csvout to write through.
     * @return the csvout instance
     * @throws IOException IO failure.
     */
    public Csvout writeln(Csvout out) throws IOException {
      return out.write(
          id,
          startTime,
          exception == null ? 1 : 0,
          timer.getStartTime(),
          timer.getEndTime(),
          timer.duration(),
          '"' + (exception == null ? "" : exception.getMessage()) + '"')
          .newline();
    }

    /**
     * Write the schema of the outcome records.
     * @param out CSV destinatin
     * @throws IOException IO failure.
     */
    public static void writeSchema(Csvout out) throws IOException {
      out.write("id", "starttime", "success", "started", "ended",
          "duration", "error").newline();
    }
  }

}
