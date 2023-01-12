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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CleanupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CreateOutputDirectoriesStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.LoadManifestsStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.SetupJobStage;

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

  private int taskAttemptCount;

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

    int filesPerTaskAttempt = 10;
    LOG.info("Number of task attempts: {}, files per task attempt {}",
        taskAttemptCount, filesPerTaskAttempt);

    setJobStageConfig(createStageConfigForJob(JOB1, getDestDir()));

    // set up the job.
    new SetupJobStage(getJobStageConfig()).apply(false);

    LOG.info("Creating manifest files for {}", taskAttemptCount);

    executeTaskAttempts(taskAttemptCount, filesPerTaskAttempt);

    LOG.info("Loading in the manifests");

    // Load in the manifests
    LoadManifestsStage stage = new LoadManifestsStage(
        getJobStageConfig());

    LoadManifestsStage.Result result = stage.apply(true);
    LoadManifestsStage.SummaryInfo summary = result.getSummary();
    List<TaskManifest> loadedManifests = result.getManifests();

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
    List<String> manifestTaskIds = loadedManifests.stream()
        .map(TaskManifest::getTaskID)
        .collect(Collectors.toList());
    Assertions.assertThat(getTaskIds())
        .describedAs("Task IDs of all tasks")
        .containsExactlyInAnyOrderElementsOf(manifestTaskIds);

    // now let's see about aggregating a large set of directories
    Set<Path> createdDirectories = new CreateOutputDirectoriesStage(
        getJobStageConfig())
        .apply(loadedManifests)
        .getCreatedDirectories();

    // but after the merge process, only one per generated file output
    // dir exists
    Assertions.assertThat(createdDirectories)
        .describedAs("Directories created")
        .hasSize(filesPerTaskAttempt);

    // and skipping the rename stage (which is going to fail),
    // go straight to cleanup
    new CleanupJobStage(getJobStageConfig()).apply(
        new CleanupJobStage.Arguments("", true, true, false));
  }

}
