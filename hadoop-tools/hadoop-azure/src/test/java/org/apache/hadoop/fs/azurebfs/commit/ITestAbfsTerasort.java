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

package org.apache.hadoop.fs.azurebfs.commit;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.junit.Assume;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.examples.terasort.TeraGen;
import org.apache.hadoop.examples.terasort.TeraSort;
import org.apache.hadoop.examples.terasort.TeraSortConfigKeys;
import org.apache.hadoop.examples.terasort.TeraValidate;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticsLogging;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.functional.RemoteIterators;

import static java.util.Optional.empty;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IOSTATISTICS_LOGGING_LEVEL_INFO;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterTestSupport.loadSuccessFile;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterTestSupport.validateSuccessFile;

/**
 * Runs Terasort against ABFS using the manifest committer.
 * The tests run in sequence, so each operation is isolated.
 * Scale test only (it is big and slow)
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@SuppressWarnings({"StaticNonFinalField", "OptionalUsedAsFieldOrParameterType"})
public class ITestAbfsTerasort extends AbstractAbfsClusterITest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAbfsTerasort.class);

  public static final int EXPECTED_PARTITION_COUNT = 10;

  public static final int PARTITION_SAMPLE_SIZE = 1000;

  public static final int ROW_COUNT = 1000;

  /**
   * This has to be common across all test methods.
   */
  private static final Path TERASORT_PATH = new Path("/ITestAbfsTerasort");

  /**
   * Duration tracker created in the first of the test cases and closed
   * in {@link #test_140_teracomplete()}.
   */
  private static Optional<DurationInfo> terasortDuration = empty();

  /**
   * Tracker of which stages are completed and how long they took.
   */
  private static final Map<String, DurationInfo> COMPLETED_STAGES = new HashMap<>();

  /**
   * FileSystem statistics are collected from the _SUCCESS markers.
   */
  protected static final IOStatisticsSnapshot JOB_IOSTATS =
      snapshotIOStatistics();

  /** Base path for all the terasort input and output paths. */
  private Path terasortPath;

  /** Input (teragen) path. */
  private Path sortInput;

  /** Path where sorted data goes. */
  private Path sortOutput;

  /** Path for validated job's output. */
  private Path sortValidate;

  public ITestAbfsTerasort() throws Exception {
  }


  @Override
  public void setup() throws Exception {
    // superclass calls requireScaleTestsEnabled();
    super.setup();
    prepareToTerasort();
  }

  /**
   * Set up the job conf with the options for terasort chosen by the scale
   * options.
   * @param conf configuration
   */
  @Override
  protected void applyCustomConfigOptions(JobConf conf) {
    // small sample size for faster runs
    conf.setInt(TeraSortConfigKeys.SAMPLE_SIZE.key(),
        getSampleSizeForEachPartition());
    conf.setInt(TeraSortConfigKeys.NUM_PARTITIONS.key(),
        getExpectedPartitionCount());
    conf.setBoolean(
        TeraSortConfigKeys.USE_SIMPLE_PARTITIONER.key(),
        false);
  }

  private int getExpectedPartitionCount() {
    return EXPECTED_PARTITION_COUNT;
  }

  private int getSampleSizeForEachPartition() {
    return PARTITION_SAMPLE_SIZE;
  }

  protected int getRowCount() {
    return ROW_COUNT;
  }

  /**
   * Set up the terasort by initializing paths variables
   * The paths used must be unique across parameterized runs but
   * common across all test cases in a single parameterized run.
   */
  private void prepareToTerasort() {
    terasortPath = getFileSystem().makeQualified(TERASORT_PATH);
    sortInput = new Path(terasortPath, "sortin");
    sortOutput = new Path(terasortPath, "sortout");
    sortValidate = new Path(terasortPath, "validate");
  }

  /**
   * Declare that a stage has completed.
   * @param stage stage name/key in the map
   * @param d duration.
   */
  private static void completedStage(final String stage,
      final DurationInfo d) {
    COMPLETED_STAGES.put(stage, d);
  }

  /**
   * Declare a stage which is required for this test case.
   * @param stage stage name
   */
  private static void requireStage(final String stage) {
    Assume.assumeTrue(
        "Required stage was not completed: " + stage,
        COMPLETED_STAGES.get(stage) != null);
  }

  /**
   * Execute a single stage in the terasort.
   * Updates the completed stages map with the stage duration -if successful.
   * @param stage Stage name for the stages map.
   * @param jobConf job conf
   * @param dest destination directory -the _SUCCESS file will be expected here.
   * @param tool tool to run.
   * @param args args for the tool.
   * @param minimumFileCount minimum number of files to have been created
   * @throws Exception any failure
   */
  private void executeStage(
      final String stage,
      final JobConf jobConf,
      final Path dest,
      final Tool tool,
      final String[] args,
      final int minimumFileCount) throws Exception {
    int result;

    // the duration info is created outside a try-with-resources
    // clause as it is used later.
    DurationInfo d = new DurationInfo(LOG, stage);
    try {
      result = ToolRunner.run(jobConf, tool, args);
    } finally {
      d.close();
    }
    dumpOutputTree(dest);
    assertEquals(stage
        + "(" + StringUtils.join(", ", args) + ")"
        + " failed", 0, result);
    final ManifestSuccessData successFile = validateSuccessFile(getFileSystem(), dest,
        minimumFileCount, "");
    JOB_IOSTATS.aggregate(successFile.getIOStatistics());

    completedStage(stage, d);
  }

  /**
   * Set up terasort by cleaning out the destination, and note the initial
   * time before any of the jobs are executed.
   *
   * This is executed first <i>for each parameterized run</i>.
   * It is where all variables which need to be reset for each run need
   * to be reset.
   */
  @Test
  public void test_100_terasort_setup() throws Throwable {
    describe("Setting up for a terasort");

    getFileSystem().delete(terasortPath, true);
    terasortDuration = Optional.of(new DurationInfo(LOG, false, "Terasort"));
  }

  @Test
  public void test_110_teragen() throws Throwable {
    describe("Teragen to %s", sortInput);
    getFileSystem().delete(sortInput, true);

    JobConf jobConf = newJobConf();
    patchConfigurationForCommitter(jobConf);
    executeStage("teragen",
        jobConf,
        sortInput,
        new TeraGen(),
        new String[]{Integer.toString(getRowCount()), sortInput.toString()},
        1);
  }


  @Test
  public void test_120_terasort() throws Throwable {
    describe("Terasort from %s to %s", sortInput, sortOutput);
    requireStage("teragen");
    getFileSystem().delete(sortOutput, true);

    loadSuccessFile(getFileSystem(), sortInput);
    JobConf jobConf = newJobConf();
    patchConfigurationForCommitter(jobConf);
    executeStage("terasort",
        jobConf,
        sortOutput,
        new TeraSort(),
        new String[]{sortInput.toString(), sortOutput.toString()},
        1);
  }

  @Test
  public void test_130_teravalidate() throws Throwable {
    describe("TeraValidate from %s to %s", sortOutput, sortValidate);
    requireStage("terasort");
    getFileSystem().delete(sortValidate, true);
    loadSuccessFile(getFileSystem(), sortOutput);
    JobConf jobConf = newJobConf();
    patchConfigurationForCommitter(jobConf);
    executeStage("teravalidate",
        jobConf,
        sortValidate,
        new TeraValidate(),
        new String[]{sortOutput.toString(), sortValidate.toString()},
        1);
  }

  /**
   * Print the results, and save to the base dir as a CSV file.
   * Why there? Makes it easy to list and compare.
   */
  @Test
  public void test_140_teracomplete() throws Throwable {
    terasortDuration.ifPresent(d -> {
      d.close();
      completedStage("overall", d);
    });

    // IO Statistics
    IOStatisticsLogging.logIOStatisticsAtLevel(LOG, IOSTATISTICS_LOGGING_LEVEL_INFO, JOB_IOSTATS);

    // and the summary
    final StringBuilder results = new StringBuilder();
    results.append("\"Operation\"\t\"Duration\"\n");

    // this is how you dynamically create a function in a method
    // for use afterwards.
    // Works because there's no IOEs being raised in this sequence.
    Consumer<String> stage = (s) -> {
      DurationInfo duration = COMPLETED_STAGES.get(s);
      results.append(String.format("\"%s\"\t\"%s\"\n",
          s,
          duration == null ? "" : duration));
    };

    stage.accept("teragen");
    stage.accept("terasort");
    stage.accept("teravalidate");
    stage.accept("overall");
    String text = results.toString();
    File resultsFile = File.createTempFile("results", ".csv");
    FileUtils.write(resultsFile, text, StandardCharsets.UTF_8);
    LOG.info("Results are in {}\n{}", resultsFile, text);
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

  @Test
  public void test_200_directory_deletion() throws Throwable {
    getFileSystem().delete(terasortPath, true);
  }

  /**
   * Dump the files under a path -but not fail if the path is not present.,
   * @param path path to dump
   * @throws Exception any failure.
   */
  protected void dumpOutputTree(Path path) throws Exception {
    LOG.info("Files under output directory {}", path);
    try {
      RemoteIterators.foreach(getFileSystem().listFiles(path, true),
          (status) -> LOG.info("{}", status));
    } catch (FileNotFoundException e) {
      LOG.info("Output directory {} not found", path);
    }
  }
}
