/*
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

package org.apache.hadoop.fs.s3a.auth.delegation;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.scale.NanoTimerStats;
import org.apache.hadoop.fs.s3a.scale.S3AScaleTestBase;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.assumeSessionTestsEnabled;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_BINDING;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_SESSION_BINDING;

/**
 * This test has a unique name as it is designed to do something special:
 * generate enough load on the AWS STS service to get some
 * statistics on its throttling.
 * This isn't documented anywhere, and for DT support it's
 * important to know how much effort it takes to overload the service.
 *
 * <b>Important</b>
 *
 * If this test does trigger STS throttling, then all users in the same
 * AWS account will experience throttling. This may be observable,
 * in delays and, if the applications in use are not resilient to
 * throttling events in STS, from application failures.
 *
 * Use with caution.
 * <ol>
 *   <li>Don't run it on an AWS endpoint which other users in a
 *   shared AWS account are actively using. </li>
 *   <li>Don't run it on the same AWS account which is being used for
 *   any production service.</li>
 *   <li>And choose a time (weekend, etc) where the account is under-used.</li>
 *   <li>Warn your fellow users.</li>
 * </ol>
 *
 * In experiments, the throttling recovers fast and appears restricted
 * to the single STS service which the test overloads.
 *
 * @see <a href="https://github.com/steveloughran/datasets/releases/tag/tag_2018-09-17-aws">
 *   AWS STS login throttling statistics</a>
 */
public class ILoadTestSessionCredentials extends S3AScaleTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ILoadTestSessionCredentials.class);

  protected static final int THREADS = 100;

  private final ExecutorService executor =
      HadoopExecutors.newFixedThreadPool(
          THREADS,
          new ThreadFactoryBuilder()
              .setNameFormat("DelegationTokenFetcher #%d")
              .build());

  private final CompletionService<Outcome>
      completionService =
      new ExecutorCompletionService<>(executor);

  private File dataDir;

  @Override
  protected Configuration createScaleConfiguration() {
    Configuration conf = super.createScaleConfiguration();
    conf.set(DELEGATION_TOKEN_BINDING,
        getDelegationBinding());
    conf.setInt(Constants.MAXIMUM_CONNECTIONS,
        Math.max(THREADS, Constants.DEFAULT_MAXIMUM_CONNECTIONS));
    conf.setInt(Constants.MAX_ERROR_RETRIES, 0);
    return conf;
  }

  /**
   * Which DT binding class to use.
   * @return the binding config option.
   */
  protected String getDelegationBinding() {
    return DELEGATION_TOKEN_SESSION_BINDING;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    assumeSessionTestsEnabled(getConfiguration());
    S3AFileSystem fileSystem = getFileSystem();
    assertNotNull(
        "No delegation tokens in FS",
        fileSystem.getCanonicalServiceName());
    dataDir = GenericTestUtils.getTestDir("kerberos");
    dataDir.mkdirs();
  }

  protected String getFilePrefix() {
    return "session";
  }

  @Test
  public void testCreate10Tokens() throws Throwable {
    File file = fetchTokens(10);
    String csv = FileUtils.readFileToString(file, "UTF-8");
    LOG.info("CSV data\n{}", csv);
  }

  @Test
  public void testCreateManyTokens() throws Throwable {
    fetchTokens(50000);
  }

  /**
   * Fetch tokens.
   * @param tokens number of tokens.
   * @return file the timings were
   * @throws Exception failure
   */
  private File fetchTokens(final int tokens)
      throws Exception {

    File filename = new File(dataDir, getFilePrefix() + "-" + tokens + ".csv");
    fetchTokens(tokens, filename);
    return filename;
  }

  /**
   * Fetch tokens.
   * @param tokens number of tokens.
   * @param csvFile file to save this to.
   * @throws Exception failure
   */
  private void fetchTokens(final int tokens, final File csvFile)
      throws Exception {
    describe("Fetching %d tokens, saving log to %s", tokens, csvFile);

    final FileWriter out = new FileWriter(csvFile);
    Csvout csvout = new Csvout(out, "\t", "\n");
    Outcome.writeSchema(csvout);


    final S3AFileSystem fileSystem = getFileSystem();
    final ContractTestUtils.NanoTimer jobTimer =
        new ContractTestUtils.NanoTimer();


    for (int i = 0; i < tokens; i++) {
      final int id = i;
      completionService.submit(() -> {
        final long startTime = System.currentTimeMillis();
        final ContractTestUtils.NanoTimer timer =
            new ContractTestUtils.NanoTimer();
        Exception ex = null;
        try {
          fileSystem.getDelegationToken("Count ");
        } catch (IOException e) {
          ex = e;
        }
        timer.end("Request");
        return new Outcome(id, startTime, timer, ex);
      });
    }

    NanoTimerStats stats = new NanoTimerStats("Overall");
    NanoTimerStats success = new NanoTimerStats("Successful");
    NanoTimerStats throttled = new NanoTimerStats("Throttled");
    List<Outcome> throttledEvents = new ArrayList<>();
    for (int i = 0; i < tokens; i++) {
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

    jobTimer.end("Execution of fetch calls");
    // now print the stats
    LOG.info("Summary file is " + csvFile);
    LOG.info("Fetched {} tokens with {} throttle events\n: {}\n{}\n{}",
        tokens,
        throttled.getCount(),
        stats,
        throttled,
        success);

    double duration = jobTimer.duration();
    double iops = tokens * 1.0e9 / duration;
    LOG.info(
        String.format("Effective IO rate is %3f operations/second", iops));
    // log at debug
    if (LOG.isDebugEnabled()) {
      throttledEvents.stream().forEach((outcome -> {
        LOG.debug("{}: duration: {}",
            outcome.id, outcome.timer.elapsedTimeMs());
      }));
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
          exception == null ? 1: 0,
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
          "duration", "error");
    }
  }

}
