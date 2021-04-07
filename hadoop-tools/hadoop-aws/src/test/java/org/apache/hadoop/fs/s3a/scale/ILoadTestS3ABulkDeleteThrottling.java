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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.auth.delegation.Csvout;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static org.apache.hadoop.fs.s3a.Constants.EXPERIMENTAL_AWS_INTERNAL_THROTTLING;
import static org.apache.hadoop.fs.s3a.Constants.BULK_DELETE_PAGE_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.BULK_DELETE_PAGE_SIZE_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.ENABLE_MULTI_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.USER_AGENT_PREFIX;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.MAX_ENTRIES_TO_DELETE;

/**
 * Test some scalable operations related to file renaming and deletion.
 * Much of the setup code is lifted from ILoadTestSessionCredentials;
 * whereas that was designed to overload an STS endpoint, this just
 * tries to overload a single S3 shard with too many bulk IO requests
 * -and so see what happens.
 * Note: UA field includes the configuration tested for the benefit
 * of anyone looking through the server logs.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class ILoadTestS3ABulkDeleteThrottling extends S3AScaleTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ILoadTestS3ABulkDeleteThrottling.class);

  protected static final int THREADS = 20;
  public static final int TOTAL_KEYS = 25000;

  public static final int SMALL = BULK_DELETE_PAGE_SIZE_DEFAULT;
  public static final int SMALL_REQS = TOTAL_KEYS / SMALL;

  public static final int MAXIMUM = MAX_ENTRIES_TO_DELETE;
  public static final int MAXIMUM_REQS = TOTAL_KEYS / MAXIMUM;

  // shared across test cases.
  @SuppressWarnings("StaticNonFinalField")
  private static boolean testWasThrottled;

  private final ExecutorService executor =
      HadoopExecutors.newFixedThreadPool(
          THREADS,
          new ThreadFactoryBuilder()
              .setNameFormat("#%d")
              .build());

  private final CompletionService<Outcome>
      completionService = new ExecutorCompletionService<>(executor);

  private File dataDir;

  private final boolean throttle;
  private final int pageSize;
  private final int requests;

  /**
   * Test array for parameterized test runs.
   * <ul>
   *   <li>AWS client throttle on/off</li>
   *   <li>Page size</li>
   * </ul>
   *
   * @return a list of parameter tuples.
   */
  @Parameterized.Parameters(
      name = "bulk-delete-aws-retry={0}-requests={2}-size={1}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {false, SMALL, SMALL_REQS},
        {false, MAXIMUM, MAXIMUM_REQS},
        {true, SMALL, SMALL_REQS},
        {true, MAXIMUM, MAXIMUM_REQS},
    });
  }

  /**
   * Parameterized constructor.
   * @param throttle AWS client throttle on/off
   * @param pageSize Page size
   * @param requests request count;
   */
  public ILoadTestS3ABulkDeleteThrottling(
      final boolean throttle,
      final int pageSize,
      final int requests) {
    this.throttle = throttle;
    Preconditions.checkArgument(pageSize > 0,
        "page size too low %s", pageSize);

    this.pageSize = pageSize;
    this.requests = requests;
  }

  @Override
  protected Configuration createScaleConfiguration() {
    Configuration conf = super.createScaleConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    final Configuration conf = getConf();
    S3ATestUtils.removeBaseAndBucketOverrides(conf,
        EXPERIMENTAL_AWS_INTERNAL_THROTTLING,
        BULK_DELETE_PAGE_SIZE,
        USER_AGENT_PREFIX);
    conf.setBoolean(EXPERIMENTAL_AWS_INTERNAL_THROTTLING, throttle);
    Assertions.assertThat(pageSize)
        .describedAs("page size")
        .isGreaterThan(0);
    conf.setInt(BULK_DELETE_PAGE_SIZE, pageSize);
    conf.set(USER_AGENT_PREFIX,
        String.format("ILoadTestS3ABulkDeleteThrottling-%s-%04d",
            throttle, pageSize));

    super.setup();
    Assume.assumeTrue("multipart delete disabled",
        conf.getBoolean(ENABLE_MULTI_DELETE, true));
    dataDir = GenericTestUtils.getTestDir("throttling");
    dataDir.mkdirs();
    final String size = getFileSystem().getConf().get(BULK_DELETE_PAGE_SIZE);
    Assertions.assertThat(size)
        .describedAs("page size")
        .isNotEmpty();
    Assertions.assertThat(getFileSystem().getConf()
        .getInt(BULK_DELETE_PAGE_SIZE, -1))
        .isEqualTo(pageSize);

  }

  @Test
  public void test_010_Reset() throws Throwable {
    testWasThrottled = false;
  }

  @Test
  public void test_020_DeleteThrottling() throws Throwable {
    describe("test how S3 reacts to massive multipart deletion requests");
    final File results = deleteFiles(requests, pageSize);
    LOG.info("Test run completed against {}:\n see {}", getFileSystem(),
        results);
    if (testWasThrottled) {
      LOG.warn("Test was throttled");
    } else {
      LOG.info("No throttling recorded in filesystem");
    }
  }

  @Test
  public void test_030_Sleep() throws Throwable {
    maybeSleep();
  }

  private void maybeSleep() throws InterruptedException, IOException {
    if (testWasThrottled) {
      LOG.info("Sleeping briefly to let store recover");
      Thread.sleep(30_000);
      getFileSystem().delete(path("recovery"), true);
      testWasThrottled = false;
    }
  }

  /**
   * delete files.
   * @param requestCount number of requests.
   * @throws Exception failure
   * @return CSV filename
   */
  private File deleteFiles(final int requestCount,
      final int entries)
      throws Exception {
    File csvFile = new File(dataDir,
        String.format("delete-%03d-%04d-%s.csv",
            requestCount, entries, throttle));
    describe("Issuing %d requests of size %d, saving log to %s",
        requestCount, entries, csvFile);
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

    for (int i = 0; i < requestCount; i++) {
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
    for (int i = 0; i < requestCount; i++) {
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
        requestCount,
        throttled.getCount(),
        stats,
        throttled,
        success);

    double duration = jobTimer.duration();
    double iops = requestCount * entries * 1.0e9 / duration;
    LOG.info(String.format("TPS %3f operations/second",
        iops));
    // log at debug
    if (LOG.isDebugEnabled()) {
      throttledEvents.forEach((outcome -> {
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
