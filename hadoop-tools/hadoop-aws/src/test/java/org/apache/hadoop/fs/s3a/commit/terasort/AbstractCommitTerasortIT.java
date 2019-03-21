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

package org.apache.hadoop.fs.s3a.commit.terasort;

import java.util.Optional;
import java.util.function.BiConsumer;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.terasort.TeraGen;
import org.apache.hadoop.examples.terasort.TeraSort;
import org.apache.hadoop.examples.terasort.TeraSortConfigKeys;
import org.apache.hadoop.examples.terasort.TeraValidate;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.commit.AbstractYarnClusterITest;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static java.util.Optional.empty;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.CONFLICT_MODE_APPEND;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.FS_S3A_COMMITTER_STAGING_CONFLICT_MODE;

/**
 * Runs Terasort against S3A.
 *
 * This is all done on a shared mini YARN and HDFS clusters, set up before
 * any of the tests methods run.
 *
 * The tests run in sequence, so each operation is isolated.
 * This also means that the test paths deleted in test
 * teardown; shared variables must all be static.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@SuppressWarnings("StaticNonFinalField")
public abstract class AbstractCommitTerasortIT extends
    AbstractYarnClusterITest {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractCommitTerasortIT.class);

  // all the durations are optional as they only get filled in when
  // a test run successfully completes. Failed tests don't have numbers.
  private static Optional<DurationInfo> terasortDuration = empty();

  private static Optional<DurationInfo> teragenStageDuration = empty();

  private static Optional<DurationInfo> terasortStageDuration = empty();

  private static Optional<DurationInfo> teravalidateStageDuration = empty();

  private Path terasortPath;

  private Path sortInput;

  private Path sortOutput;

  private Path sortValidate;

  /**
   * Not using special paths here.
   * @return false
   */
  @Override
  public boolean useInconsistentClient() {
    return false;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    requireScaleTestsEnabled();
    prepareToTerasort();
  }

  /**
   * Set up for terasorting by initializing paths.
   * The paths used must be unique across parallel runs.
   */
  private void prepareToTerasort() {
    // small sample size for faster runs
    Configuration yarnConfig = getYarn().getConfig();
    yarnConfig.setInt(TeraSortConfigKeys.SAMPLE_SIZE.key(), 1000);
    yarnConfig.setBoolean(
        TeraSortConfigKeys.USE_SIMPLE_PARTITIONER.key(),
        true);
    terasortPath = new Path("/terasort-" + getClass().getSimpleName())
        .makeQualified(getFileSystem());
    sortInput = new Path(terasortPath, "sortin");
    sortOutput = new Path(terasortPath, "sortout");
    sortValidate = new Path(terasortPath, "validate");
    if (!terasortDuration.isPresent()) {
      terasortDuration = Optional.of(new DurationInfo(LOG, "Terasort"));
    }
  }

  /**
   * Execute a single stage in the terasort,
   * @param stage Stage name for messages/assertions.
   * @param jobConf job conf
   * @param dest destination directory -the _SUCCESS File will be expected here.
   * @param tool tool to run.
   * @param args args for the tool.
   * @throws Exception any failure
   */
  private Optional<DurationInfo> executeStage(
      final String stage,
      final JobConf jobConf,
      final Path dest,
      final Tool tool,
      final String[] args) throws Exception {
    int result;
    DurationInfo d = new DurationInfo(LOG, stage);
    try {
      result = ToolRunner.run(jobConf, tool, args);
    } finally {
      d.close();
    }
    assertEquals(stage
        + "(" + StringUtils.join(", ", args) + ")"
        + " failed", 0, result);
    validateSuccessFile(getFileSystem(), dest, committerName());
    return Optional.of(d);
  }

  /**
   * Set up terasort by cleaning out the destination, and note the initial
   * time before any of the jobs are executed.
   */
  @Test
  public void test_100_terasort_setup() throws Throwable {
    describe("Setting up for a terasort");

    getFileSystem().delete(terasortPath, true);
  }

  @Test
  public void test_110_teragen() throws Throwable {
    describe("Teragen to %s", sortInput);

    JobConf jobConf = newJobConf();
    patchConfigurationForCommitter(jobConf);
    teragenStageDuration = executeStage("Teragen",
        jobConf,
        sortInput,
        new TeraGen(),
        new String[]{Integer.toString(SCALE_TEST_KEYS), sortInput.toString()});
  }

  @Test
  public void test_120_terasort() throws Throwable {
    describe("Terasort from %s to %s", sortInput, sortOutput);
    loadSuccessFile(getFileSystem(), sortInput);
    JobConf jobConf = newJobConf();
    patchConfigurationForCommitter(jobConf);
    // this job adds some data, so skip it.
    jobConf.set(FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CONFLICT_MODE_APPEND);
    terasortStageDuration = executeStage("TeraSort",
        jobConf,
        sortOutput,
        new TeraSort(),
        new String[]{sortInput.toString(), sortOutput.toString()});
  }

  @Test
  public void test_130_teravalidate() throws Throwable {
    describe("TeraValidate from %s to %s", sortOutput, sortValidate);
    loadSuccessFile(getFileSystem(), sortOutput);
    JobConf jobConf = newJobConf();
    patchConfigurationForCommitter(jobConf);
    teravalidateStageDuration = executeStage("TeraValidate",
        jobConf,
        sortValidate,
        new TeraValidate(),
        new String[]{sortOutput.toString(), sortValidate.toString()});
  }

  /**
   * Print the results, and save to the base dir as a CSV file.
   * Why there? Makes it easy to list and compare.
   */
  @Test
  public void test_140_teracomplete() throws Throwable {
    terasortDuration.get().close();

    final StringBuilder results = new StringBuilder();
    results.append("\"Operation\"\t\"Duration\"\n");

    // this is how you dynamically create a function in a method
    // for use afterwards.
    // Works because there's no IOEs being raised in this sequence.
    BiConsumer<String, Optional<DurationInfo>> stage =
        (s, od) ->
            results.append(String.format("\"%s\"\t\"%s\"\n",
                s,
                od.map(DurationInfo::getDurationString).orElse("")));

    stage.accept("Generate", teragenStageDuration);
    stage.accept("Terasort", terasortStageDuration);
    stage.accept("Validate", teravalidateStageDuration);
    stage.accept("Completed", terasortDuration);
    String text = results.toString();
    Path path = new Path(terasortPath, "results.csv");
    LOG.info("Results are in {}\n{}", path, text);
    ContractTestUtils.writeTextFile(getFileSystem(), path, text, true);
  }

  /**
   * Reset the duration so if two committer tests are run sequentially.
   * Without this the total execution time is reported as from the start of
   * the first test suite to the end of the second.
   */
  @Test
  public void test_150_teracleanup() throws Throwable {
    terasortDuration = Optional.empty();
  }
}
