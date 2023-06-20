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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.io.File;
import java.util.List;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestPrinter;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CleanupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CreateOutputDirectoriesStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.LoadManifestsStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.SetupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageConfig;

import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.DEFAULT_WRITER_QUEUE_CAPACITY;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_SUMMARY_REPORT_DIR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_COMMIT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.addHeapInformation;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.createJobSummaryFilename;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.createManifestOutcome;

/**
 * Test loading manifests from a store.
 * By not creating files we can simulate a large job just by
 * creating the manifests.
 * The SaveTaskManifestStage stage is used for the save operation;
 * this does a save + rename.
 * For better test performance against a remote store, a thread
 * pool is used to save the manifests in parallel.
 */
public class TestLoadManifestsStage extends AbstractManifestCommitterTest {

  public static final int FILES_PER_TASK_ATTEMPT = 100;

  private int taskAttemptCount;

  private File entryFile;

  /**
   * How many task attempts to make?
   * Override point.
   * @return a number greater than 0.
   */
  protected int numberOfTaskAttempts() {
    return ManifestCommitterTestSupport.NUMBER_OF_TASK_ATTEMPTS;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    taskAttemptCount = numberOfTaskAttempts();
    Assertions.assertThat(taskAttemptCount)
        .describedAs("Task attempt count")
        .isGreaterThan(0);
  }

  @Override
  public void teardown() throws Exception {
    if (entryFile != null) {
      entryFile.delete();
    }
    super.teardown();
  }

  public long heapSize() {
    return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
  }

  /**
   * Build a large number of manifests, but without the real files
   * and directories.
   * Save the manifests under the job attempt dir, then load
   * them via the {@link LoadManifestsStage}.
   * The directory preparation process is then executed after this.
   * Because we know each task attempt creates the same number of directories,
   * they will all be merged and so only a limited number of output dirs
   * will be created.
   */
  @Test
  public void testSaveThenLoadManyManifests() throws Throwable {

    describe("Creating many manifests with fake file/dir entries,"
        + " load them and prepare the output dirs.");

    int filesPerTaskAttempt = FILES_PER_TASK_ATTEMPT;
    LOG.info("Number of task attempts: {}, files per task attempt {}",
        taskAttemptCount, filesPerTaskAttempt);

    final StageConfig stageConfig = createStageConfigForJob(JOB1, getDestDir());
    setJobStageConfig(stageConfig);

    // set up the job.
    new SetupJobStage(stageConfig).apply(false);

    LOG.info("Creating manifest files for {}", taskAttemptCount);

    executeTaskAttempts(taskAttemptCount, filesPerTaskAttempt);

    IOStatisticsSnapshot heapInfo = new IOStatisticsSnapshot();

    heapinfo(heapInfo, "initial");

    LOG.info("Loading in the manifests");

    // Load in the manifests
    LoadManifestsStage stage = new LoadManifestsStage(
        stageConfig);
    entryFile = File.createTempFile("entry", ".seq");
    LoadManifestsStage.Arguments args = new LoadManifestsStage.Arguments(
        entryFile, DEFAULT_WRITER_QUEUE_CAPACITY);

    LoadManifestsStage.Result loadManifestsResult = stage.apply(args);
    LoadManifestsStage.SummaryInfo summary = loadManifestsResult.getSummary();

    LOG.info("\nJob statistics after loading {}",
        ioStatisticsToPrettyString(getStageStatistics()));
    LOG.info("Heap size = {}", heapSize());
    heapinfo(heapInfo, "load.manifests");

    Assertions.assertThat(summary.getManifestCount())
        .describedAs("Manifest count of  %s", summary)
        .isEqualTo(taskAttemptCount);
    Assertions.assertThat(summary.getFileCount())
        .describedAs("File count of  %s", summary)
        .isEqualTo(taskAttemptCount * (long) filesPerTaskAttempt);
    Assertions.assertThat(summary.getTotalFileSize())
        .describedAs("File Size of  %s", summary)
        .isEqualTo(getTotalDataSize());


    // now that manifest list.
    List<String> manifestTaskIds = summary.getTaskIDs();
    Assertions.assertThat(getTaskIds())
        .describedAs("Task IDs of all tasks")
        .containsExactlyInAnyOrderElementsOf(manifestTaskIds);

    // now let's see about aggregating a large set of directories
    Set<Path> createdDirectories = new CreateOutputDirectoriesStage(
        stageConfig)
        .apply(loadManifestsResult.getLoadedManifestData().getDirectories())
        .getCreatedDirectories();
    heapinfo(heapInfo, "create.directories");

    // but after the merge process, only one per generated file output
    // dir exists
    Assertions.assertThat(createdDirectories)
        .describedAs("Directories created")
        .hasSize(filesPerTaskAttempt);

    // and skipping the rename stage (which is going to fail),
    // go straight to cleanup
    new CleanupJobStage(stageConfig).apply(
        new CleanupJobStage.Arguments("", true, true, false));
    heapinfo(heapInfo, "cleanup");

    ManifestSuccessData success = createManifestOutcome(stageConfig, OP_STAGE_JOB_COMMIT);
    success.snapshotIOStatistics(getStageStatistics());
    success.getIOStatistics().aggregate(heapInfo);

    Configuration conf = getConfiguration();
    enableManifestCommitter(conf);
    String reportDir = conf.getTrimmed(OPT_SUMMARY_REPORT_DIR, "");
    Path reportDirPath = new Path(reportDir);
    Path path = new Path(reportDirPath,
        createJobSummaryFilename("TestLoadManifestsStage"));
    final FileSystem summaryFS = path.getFileSystem(conf);
    success.save(summaryFS, path, true);
    LOG.info("Saved summary to {}", path);
    new ManifestPrinter().loadAndPrintManifest(summaryFS, path);
  }

  /**
   * Force a GC then add heap info.
   * @param stats stats to update
   * @param stage stage name
   */
  private static void heapinfo(final IOStatisticsSnapshot stats, final String stage) {
    System.gc();
    addHeapInformation(stats, stage);
  }

}
