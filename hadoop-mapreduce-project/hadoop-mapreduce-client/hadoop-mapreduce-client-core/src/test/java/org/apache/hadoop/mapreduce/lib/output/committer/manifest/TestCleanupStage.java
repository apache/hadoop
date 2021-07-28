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

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.AbstractJobCommitStage.E_TRASH_DISABLED;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.UnreliableStoreOperations.E_TIMEOUT;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test the cleanup stage.
 */
public class TestCleanupStage extends AbstractManifestCommitterTest {

  /**
   * Number of task attempts to create. Manifests are created and written
   * as well as test dirs, but no actual files.
   */
  protected static final int TASK_ATTEMPT_COUNT = 10;

  /**
   * How many delete calls for the root job delete?
   */
  protected static final int ROOT_DELETE_COUNT = 1;

  /**
   * Tocal invocation count for a successful parallel delete job.
   */
  protected static final int PARALLEL_DELETE_COUNT =
      TASK_ATTEMPT_COUNT + ROOT_DELETE_COUNT;

  /**
   * Fault Injection.
   */
  private UnreliableStoreOperations failures;

  /**
   * Manifests created.
   */
  private List<TaskManifest> manifests;

  @Override
  public void setup() throws Exception {
    super.setup();
    failures
        = new UnreliableStoreOperations(createStoreOperations());
    setStoreOperations(failures);
    Path destDir = methodPath();
    StageConfig stageConfig = createStageConfigForJob(JOB1, destDir);
    setJobStageConfig(stageConfig);
    new SetupJobStage(stageConfig).apply(true);

    // lots of tasks, but don't bother creating mock files.
    manifests = executeTaskAttempts(TASK_ATTEMPT_COUNT, 0);
  }

  /**
   * Turn trash on.
   * @return FS config.
   */
  @Override
  protected Configuration createConfiguration() {

    return enableTrash(super.createConfiguration());
  }

  @Test
  public void testCleanupInParallelHealthy() throws Throwable {
    describe("parallel cleanup of TA dirs.");
    cleanup(true, true, false, false,
        CleanupJobStage.Outcome.PARALLEL_DELETE,
        PARALLEL_DELETE_COUNT);
    verifyJobDirsCleanedUp();
  }

  @Test
  public void testCleanupSingletonHealthy() throws Throwable {
    describe("Cleanup with a single delete. Not the default; would be best on HDFS");

    cleanup(true, false, false, false,
        CleanupJobStage.Outcome.DELETED, ROOT_DELETE_COUNT);
    verifyJobDirsCleanedUp();
  }

  @Test
  public void testCleanupNoDir() throws Throwable {
    describe("parallel cleanup MUST not fail if there's no dir");
    // first do the cleanup
    cleanup(true, true, false, false,
        CleanupJobStage.Outcome.PARALLEL_DELETE, PARALLEL_DELETE_COUNT);

    // now expect cleanup by single delete still works
    // the delete count is 0 as pre check skips it
    cleanup(true, false, false, false,
        CleanupJobStage.Outcome.NOTHING_TO_CLEAN_UP, 0);
    // rename
    cleanup(true, true, false, true,
        CleanupJobStage.Outcome.NOTHING_TO_CLEAN_UP, 0);

    // if skipped, that happens first
    cleanup(false, true, false, true,
        CleanupJobStage.Outcome.DISABLED, 0);
  }

  @Test
  public void testRenameToTrash() throws Throwable {
    describe("cleanup by rename to trash");

    CleanupJobStage.CleanupResult result = cleanup(
        true, true, false, true,
        CleanupJobStage.Outcome.RENAMED_TO_TRASH, 0);
    verifyJobDirsCleanedUp();
  }

  @Test
  public void testRenameToTrashFailure() throws Throwable {
    describe("cleanup by rename to trash");
    // trash will fail.
    failures.setTrashDisabled(true);
    intercept(PathIOException.class, E_TRASH_DISABLED, () ->
        cleanup(true, true, false, true,
            CleanupJobStage.Outcome.MOVE_TO_TRASH_FAILED, 0));
  }

  @Test
  public void testDeleteFailureFallbackToRename() throws Throwable {
    describe("cleanup where delete fails but rename() works." +
        " Even without suppressing exceptions");
    failures.addDeletePathToTimeOut(getJobStageConfig().getOutputTempSubDir());
    CleanupJobStage.CleanupResult result = cleanup(true, false, false, false,
        CleanupJobStage.Outcome.RENAMED_TO_TRASH, ROOT_DELETE_COUNT);
    verifyJobDirsCleanedUp();
    // result contains the delete exception
    intercept(PathIOException.class, result.getDirectory().toString(),
        result::maybeRethrowException);
  }

  @Test
  public void testNonparallelDoubleFailure() throws Throwable {
    describe("cleanup where delete fails as does moveToTrash()");
    Path tempSubDir = getJobStageConfig().getOutputTempSubDir();
    failures.addDeletePathToTimeOut(tempSubDir);
    failures.addMoveToTrashToFail(tempSubDir);
    intercept(PathIOException.class, E_TIMEOUT, () ->
        cleanup(true, false, false, false,
            CleanupJobStage.Outcome.FAILURE, ROOT_DELETE_COUNT));
  }

  @Test
  public void testDoubleFailureSuppressing() throws Throwable {
    describe("cleanup where delete fails as does rename()" +
        " but exceptions suppressed");
    Path tempSubDir = getJobStageConfig().getOutputTempSubDir();
    failures.addDeletePathToTimeOut(tempSubDir);
    failures.addMoveToTrashToFail(tempSubDir);
    CleanupJobStage.CleanupResult result = cleanup(true, false, true, false,
        CleanupJobStage.Outcome.FAILURE, ROOT_DELETE_COUNT);

    // now throw it.
    intercept(PathIOException.class, E_TIMEOUT,
        result::maybeRethrowException);
    StoreOperations.MoveToTrashResult moveResult = result.getMoveResult();
    // there's an exception here
    Assertions.assertThat(moveResult.getException())
        .describedAs("exception in " + moveResult)
        .isNotNull();
  }

  @Test
  public void testFailureInParallelDelete() throws Throwable {
    describe("A parallel delete fails, but the base delete works");

    // pick one of the manifests
    TaskManifest manifest = manifests.get(4);
    Path taPath = new Path(manifest.getTaskAttemptDir());
    failures.addDeletePathToFail(taPath);
    CleanupJobStage.CleanupResult result = cleanup(true, true, false, false,
        CleanupJobStage.Outcome.DELETED, PARALLEL_DELETE_COUNT);
  }

  /**
   * If there's no job task attempt subdir then the list of it will raise
   * and FNFE; this MUST be caught and the base delete executed.
   */
  @Test
  public void testParallelDeleteNoTaskAttemptDir() throws Throwable {
    describe("Execute parallel delete where" +
        " the job task directory does not exist");
    StageConfig stageConfig = getJobStageConfig();
    // TA dir doesn't exist, so listing will fail.
    failures.addPathNotFound(stageConfig.getJobAttemptTaskSubDir());
    CleanupJobStage.CleanupResult result = cleanup(true, true, false, false,
        CleanupJobStage.Outcome.DELETED, ROOT_DELETE_COUNT);
  }

}
