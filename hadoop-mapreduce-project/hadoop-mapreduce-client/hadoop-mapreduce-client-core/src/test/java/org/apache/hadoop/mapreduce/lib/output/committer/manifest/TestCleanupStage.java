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

import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.UnreliableManifestStoreOperations;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CleanupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.SetupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageConfig;


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
  private UnreliableManifestStoreOperations failures;

  /**
   * Manifests created.
   */
  private List<TaskManifest> manifests;

  @Override
  public void setup() throws Exception {
    super.setup();
    failures = new UnreliableManifestStoreOperations(
        createManifestStoreOperations());
    setStoreOperations(failures);
    Path destDir = methodPath();
    StageConfig stageConfig = createStageConfigForJob(JOB1, destDir);
    setJobStageConfig(stageConfig);
    new SetupJobStage(stageConfig).apply(true);

    // lots of tasks, but don't bother creating mock files.
    manifests = executeTaskAttempts(TASK_ATTEMPT_COUNT, 0);
  }

  @Test
  public void testCleanupInParallelHealthy() throws Throwable {
    describe("parallel cleanup of TA dirs.");
    cleanup(true, true, false,
        CleanupJobStage.Outcome.PARALLEL_DELETE,
        PARALLEL_DELETE_COUNT);
    verifyJobDirsCleanedUp();
  }

  @Test
  public void testCleanupSingletonHealthy() throws Throwable {
    describe("Cleanup with a single delete. Not the default; would be best on HDFS");

    cleanup(true, false, false,
        CleanupJobStage.Outcome.DELETED, ROOT_DELETE_COUNT);
    verifyJobDirsCleanedUp();
  }

  @Test
  public void testCleanupNoDir() throws Throwable {
    describe("parallel cleanup MUST not fail if there's no dir");
    // first do the cleanup
    cleanup(true, true, false,
        CleanupJobStage.Outcome.PARALLEL_DELETE, PARALLEL_DELETE_COUNT);

    // now expect cleanup by single delete still works
    // the delete count is 0 as pre check skips it
    cleanup(true, false, false,
        CleanupJobStage.Outcome.NOTHING_TO_CLEAN_UP, 0);

    // if skipped, that happens first
    cleanup(false, true, false,
        CleanupJobStage.Outcome.DISABLED, 0);
  }

  @Test
  public void testFailureInParallelDelete() throws Throwable {
    describe("A parallel delete fails, but the base delete works");

    // pick one of the manifests
    TaskManifest manifest = manifests.get(4);
    Path taPath = new Path(manifest.getTaskAttemptDir());
    failures.addDeletePathToFail(taPath);
    cleanup(true, true, false,
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
    cleanup(true, true, false,
        CleanupJobStage.Outcome.DELETED, ROOT_DELETE_COUNT);
  }

}
