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

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CleanupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CommitJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CommitTaskStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.SetupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageConfig;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CLEANUP;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CleanupJobStage.cleanupStageOptionsFromConfig;

/**
 * Test committing a task
 */
public class TestCommitTaskStage extends AbstractManifestCommitterTest {

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
    Path destDir = methodPath();
    StageConfig stageConfig = createStageConfigForJob(JOB1, destDir);
    setJobStageConfig(stageConfig);
    new SetupJobStage(stageConfig).apply(true);
  }

  @Test
  public void testCommitEmptyDirectory() throws Throwable {

    String tid = String.format("task_%03d", 1);
    String taskAttemptId = String.format("%s_%02d",
        tid, 1);
    StageConfig taskStageConfig = createTaskStageConfig(JOB1, tid,
        taskAttemptId);

    // the task attempt dir does not exist
    Path taDir = taskStageConfig.getTaskAttemptDir();
    assertPathDoesNotExist("task attempt path", taDir);

    // but the task commit succeeds

    CommitTaskStage.Result result = new CommitTaskStage(taskStageConfig)
        .apply(null);

    final TaskManifest manifest = result.getTaskManifest();
    Assertions.assertThat(manifest.getDirectoriesToCreate())
        .as("directories to create")
        .isEmpty();
    Assertions.assertThat(manifest.getFilesToCommit())
        .as("files to commit")
        .isEmpty();

    // now commit
    new CommitJobStage(getJobStageConfig())
        .apply(new CommitJobStage.Arguments(
            true, true, null,
            new CleanupJobStage.Arguments(
                OP_STAGE_JOB_CLEANUP,
            true,
                true,
            false,
                false)));

  }

}
