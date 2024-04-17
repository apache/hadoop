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

import java.io.FileNotFoundException;
import java.net.SocketTimeoutException;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperations;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.UnreliableManifestStoreOperations;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CleanupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CommitJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CommitTaskStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.SetupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.SetupTaskStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageConfig;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CLEANUP;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.manifestPathForTask;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.manifestTempPathForTaskAttempt;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.UnreliableManifestStoreOperations.E_TIMEOUT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.UnreliableManifestStoreOperations.generatedErrorMessage;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test committing a task.
 */
public class TestCommitTaskStage extends AbstractManifestCommitterTest {

  public static final String TASK1 = String.format("task_%03d", 1);

  public static final String TASK1_ATTEMPT1 = String.format("%s_%02d",
      TASK1, 1);

  @Override
  public void setup() throws Exception {
    super.setup();

    Path destDir = methodPath();
    StageConfig stageConfig = createStageConfigForJob(JOB1, destDir);
    setJobStageConfig(stageConfig);
    new SetupJobStage(stageConfig).apply(true);
  }

  @Test
  public void testCommitMissingDirectory() throws Throwable {

    String tid = String.format("task_%03d", 1);
    String taskAttemptId = String.format("%s_%02d",
        tid, 1);
    StageConfig taskStageConfig = createTaskStageConfig(JOB1, tid,
        taskAttemptId);

    // the task attempt dir does not exist
    Path taDir = taskStageConfig.getTaskAttemptDir();
    assertPathDoesNotExist("task attempt path", taDir);

    // so the task commit fails
    intercept(FileNotFoundException.class, () ->
        new CommitTaskStage(taskStageConfig).apply(null));
  }

  @Test
  public void testCommitEmptyDirectory() throws Throwable {

    describe("Commit an empty directory as task then job");
    String tid = String.format("task_%03d", 2);
    String taskAttemptId = String.format("%s_%02d",
        tid, 1);
    StageConfig taskStageConfig = createTaskStageConfig(JOB1, tid,
        taskAttemptId);

    // set up the task
    new SetupTaskStage(taskStageConfig).apply("setup");

    CommitTaskStage.Result result = new CommitTaskStage(taskStageConfig)
        .apply(null);

    final TaskManifest manifest = result.getTaskManifest();
    Assertions.assertThat(manifest.getDestDirectories())
        .as("directories to create")
        .isEmpty();
    Assertions.assertThat(manifest.getFilesToCommit())
        .as("files to commit")
        .isEmpty();

    final Path path = result.getPath();

    final String manifestBody = readText(path);

    LOG.info("manifest at {} of length {}:\n{}",
        path, manifestBody.length(), manifestBody);

    // now commit
    final CommitJobStage.Result outcome = new CommitJobStage(getJobStageConfig())
        .apply(new CommitJobStage.Arguments(
            true, true, null,
            new CleanupJobStage.Arguments(
                OP_STAGE_JOB_CLEANUP,
                true,
                true,
                false, false
            )));

    // review success file
    final Path successPath = outcome.getSuccessPath();
    String successBody = readText(successPath);
    LOG.info("successBody at {} of length {}:\n{}",
        successPath, successBody.length(), successBody);

    final ManifestSuccessData successData = outcome.getJobSuccessData();
    Assertions.assertThat(successData.getFilenames())
        .as("Filenames in _SUCCESS")
        .isEmpty();
  }


  @Test
  public void testManifestSaveFailures() throws Throwable {
    describe("Test recovery of manifest save/rename failures");

    UnreliableManifestStoreOperations failures = makeStoreOperationsUnreliable();

    StageConfig stageConfig = createTaskStageConfig(JOB1, TASK1, TASK1_ATTEMPT1);

    new SetupTaskStage(stageConfig).apply("setup");

    final Path manifestDir = stageConfig.getTaskManifestDir();
    // final manifest file is by task ID
    Path manifestFile = manifestPathForTask(manifestDir,
        stageConfig.getTaskId());
    Path manifestTempFile = manifestTempPathForTaskAttempt(manifestDir,
        stageConfig.getTaskAttemptId());

    // manifest save will fail but recover before the task gives up.
    failures.addSaveToFail(manifestTempFile);

    // will fail because too many attempts failed.
    failures.setFailureLimit(SAVE_ATTEMPTS + 1);
    intercept(PathIOException.class, generatedErrorMessage("save"), () ->
        new CommitTaskStage(stageConfig).apply(null));

    // will succeed because the failure limit is set lower
    failures.setFailureLimit(SAVE_ATTEMPTS - 1);
    new CommitTaskStage(stageConfig).apply(null);

    describe("Testing timeouts on rename operations.");
    // now do it for the renames, which will fail after the rename
    failures.reset();
    failures.addTimeoutBeforeRename(manifestTempFile);

    // first verify that if too many attempts fail, the task will fail
    failures.setFailureLimit(SAVE_ATTEMPTS + 1);
    intercept(SocketTimeoutException.class, E_TIMEOUT, () ->
        new CommitTaskStage(stageConfig).apply(null));
    // reduce the limit and expect the stage to succeed.
    failures.setFailureLimit(SAVE_ATTEMPTS - 1);
    new CommitTaskStage(stageConfig).apply(null);
  }

  @Test
  public void testManifestRenameTimeouts() throws Throwable {
    describe("Testing timeouts on rename operations.");

    UnreliableManifestStoreOperations failures = makeStoreOperationsUnreliable();
    StageConfig stageConfig = createTaskStageConfig(JOB1, TASK1, TASK1_ATTEMPT1);

    new SetupTaskStage(stageConfig).apply("setup");

    final Path manifestDir = stageConfig.getTaskManifestDir();
    // final manifest file is by task ID
    Path manifestFile = manifestPathForTask(manifestDir,
        stageConfig.getTaskId());
    Path manifestTempFile = manifestTempPathForTaskAttempt(manifestDir,
        stageConfig.getTaskAttemptId());


    // configure for which will fail after the rename
    failures.reset();
    failures.addTimeoutBeforeRename(manifestTempFile);

    // first verify that if too many attempts fail, the task will fail
    failures.setFailureLimit(SAVE_ATTEMPTS + 1);
    intercept(SocketTimeoutException.class, E_TIMEOUT, () ->
        new CommitTaskStage(stageConfig).apply(null));
    // reduce the limit and expect the stage to succeed.
    failures.setFailureLimit(SAVE_ATTEMPTS - 1);
    new CommitTaskStage(stageConfig).apply(null);
  }

  @Test
  public void testManifestRenameLateTimeouts() throws Throwable {
    describe("Testing timeouts on rename operations.");

    UnreliableManifestStoreOperations failures = makeStoreOperationsUnreliable();
    StageConfig stageConfig = createTaskStageConfig(JOB1, TASK1, TASK1_ATTEMPT1);

    new SetupTaskStage(stageConfig).apply("setup");

    final Path manifestDir = stageConfig.getTaskManifestDir();
    // final manifest file is by task ID
    Path manifestFile = manifestPathForTask(manifestDir,
        stageConfig.getTaskId());
    Path manifestTempFile = manifestTempPathForTaskAttempt(manifestDir,
        stageConfig.getTaskAttemptId());


    // configure for which will fail after the rename
    failures.reset();
    failures.addTimeoutAfterRename(manifestTempFile);

    // first verify that if too many attempts fail, the task will fail
    failures.setFailureLimit(SAVE_ATTEMPTS + 1);
    intercept(SocketTimeoutException.class, E_TIMEOUT, () ->
        new CommitTaskStage(stageConfig).apply(null));

    // reduce the limit and expect the stage to succeed.
    failures.setFailureLimit(SAVE_ATTEMPTS - 1);
    new CommitTaskStage(stageConfig).apply(null);
  }

  @Test
  public void testManifestDeleteInRenameErrorHandlerFailure() throws Throwable {
    describe("Testing failure in the delete call made during cleanup");

    UnreliableManifestStoreOperations failures = makeStoreOperationsUnreliable();
    StageConfig stageConfig = createTaskStageConfig(JOB1, TASK1, TASK1_ATTEMPT1);

    new SetupTaskStage(stageConfig).apply("setup");

    final Path manifestDir = stageConfig.getTaskManifestDir();
    // final manifest file is by task ID
    Path manifestFile = manifestPathForTask(manifestDir,
        stageConfig.getTaskId());
    Path manifestTempFile = manifestTempPathForTaskAttempt(manifestDir,
        stageConfig.getTaskAttemptId());


    // configure for which will fail after the rename
    failures.reset();
    failures.addTimeoutAfterRename(manifestTempFile);

    // first verify that if too many attempts fail, the task will fail
    failures.setFailureLimit(SAVE_ATTEMPTS + 1);
    intercept(SocketTimeoutException.class, E_TIMEOUT, () ->
        new CommitTaskStage(stageConfig).apply(null));

    // reduce the limit and expect the stage to succeed.
    failures.setFailureLimit(SAVE_ATTEMPTS - 1);
    new CommitTaskStage(stageConfig).apply(null);
  }

  /**
   * Make the store operations unreliable.
   * If it already was then reset the failure options.
   * @return the store operations
   */
  private UnreliableManifestStoreOperations makeStoreOperationsUnreliable() {
    UnreliableManifestStoreOperations failures;
    final ManifestStoreOperations wrappedOperations = getStoreOperations();
    if (wrappedOperations instanceof UnreliableManifestStoreOperations) {
      failures = (UnreliableManifestStoreOperations) wrappedOperations;
      failures.reset();
    } else {
      failures = new UnreliableManifestStoreOperations(wrappedOperations);
      setStoreOperations(failures);
    }
    return failures;
  }

}
