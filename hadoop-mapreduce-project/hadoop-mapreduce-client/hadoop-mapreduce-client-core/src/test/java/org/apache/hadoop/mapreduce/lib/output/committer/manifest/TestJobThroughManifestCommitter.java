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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.LoadedManifestData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.OutputValidationException;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.AbortTaskStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CleanupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CommitJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CommitTaskStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.LoadManifestsStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.SetupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.SetupTaskStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageConfig;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.ValidateRenamedFilesStage;
import org.apache.hadoop.net.NetUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.rm;
import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyPathExists;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.DEFAULT_WRITER_QUEUE_CAPACITY;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.JOB_ID_SOURCE_MAPREDUCE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.MANIFEST_COMMITTER_CLASSNAME;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_BYTES_COMMITTED_COUNT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_FILES_COMMITTED_COUNT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CLEANUP;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_COMMIT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterTestSupport.loadAndPrintSuccessData;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterTestSupport.validateGeneratedFiles;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DiagnosticKeys.PRINCIPAL;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DiagnosticKeys.STAGE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.manifestPathForTask;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CleanupJobStage.DISABLED;
import static org.apache.hadoop.security.UserGroupInformation.getCurrentUser;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test IO through the stages.
 * This mimics the workflow of a job with two tasks,
 * the first task has two attempts where the second attempt
 * is committed after the first attempt (simulating the
 * failure-during-task-commit which the v2 algorithm cannot
 * handle).
 *
 * The test is ordered and the output dir is not cleaned up
 * after each test case.
 * The last test case MUST perform the cleanup.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestJobThroughManifestCommitter
    extends AbstractManifestCommitterTest {

  /** Destination directory. */
  private Path destDir;

  /** directory names for the tests. */
  private ManifestCommitterSupport.AttemptDirectories dirs;

  /**
   * To ensure that the local FS has a shared root path, this is static.
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private static Path sharedTestRoot = null;

  /**
   * Job ID.
   */
  private String jobId;

  /**
   * Task 0 attempt 0 ID.
   */
  private String taskAttempt00;

  /**
   * Task 0 attempt 1 ID.
   */
  private String taskAttempt01;

  /**
   * Task 1 attempt 0 ID.
   */
  private String taskAttempt10;

  /**
   * Task 1 attempt 1 ID.
   */
  private String taskAttempt11;

  /**
   * Stage config for TA00.
   */
  private StageConfig ta00Config;

  /**
   * Stage config for TA01.
   */
  private StageConfig ta01Config;

  /**
   * Stage config for TA10.
   */
  private StageConfig ta10Config;

  /**
   * Stage config for TA11.
   */
  private StageConfig ta11Config;

  /**
   * Loaded manifest data, set in job commit and used in validation.
   * This is static so it can be passed from where it is loaded
   * {@link #test_0400_loadManifests()} to subsequent tests.
   */
  private static LoadedManifestData
      loadedManifestData;

  @Override
  public void setup() throws Exception {
    super.setup();
    taskAttempt00 = TASK_IDS.getTaskAttempt(TASK0, TA0);
    taskAttempt01 = TASK_IDS.getTaskAttempt(TASK0, TA1);
    taskAttempt10 = TASK_IDS.getTaskAttempt(TASK1, TA0);
    taskAttempt11 = TASK_IDS.getTaskAttempt(TASK1, TA1);
    setSharedPath(path("TestJobThroughManifestCommitter"));
    // add a dir with a space in.
    destDir = new Path(sharedTestRoot, "out put");
    jobId = TASK_IDS.getJobId();
    // then the specific path underneath that for the attempt.
    dirs = new ManifestCommitterSupport.AttemptDirectories(destDir,
        jobId, 1);

    // config for job attempt 1, task 00
    setJobStageConfig(createStageConfigForJob(JOB1, destDir).build());
    ta00Config = createStageConfig(JOB1, TASK0, TA0, destDir).build();
    ta01Config = createStageConfig(JOB1, TASK0, TA1, destDir).build();
    ta10Config = createStageConfig(JOB1, TASK1, TA0, destDir).build();
    ta11Config = createStageConfig(JOB1, TASK1, TA1, destDir).build();
  }

  /**
   * Test dir deletion is removed from test case teardown so the
   * subsequent tests see the output.
   * @throws IOException failure
   */
  @Override
  protected void deleteTestDirInTeardown() throws IOException {
    /* no-op */
  }

  /**
   * Override point and something to turn on/off when exploring what manifests look like.
   * Stores where storage is billed MUST enable this.
   * @return true if, at the end of the run, the test dir should be deleted.
   */
  protected boolean shouldDeleteTestRootAtEndOfTestRun() {
    return false;
  }

  /**
   * Invoke this to clean up the test directories.
   */
  private void deleteSharedTestRoot() throws IOException {
    describe("Deleting shared test root %s", sharedTestRoot);

    rm(getFileSystem(), sharedTestRoot, true, false);
  }

  /**
   * Set the shared test root if not already set.
   * @param path path to set.
   * @return true if the path was set
   */
  private static synchronized boolean setSharedPath(final Path path) {
    if (sharedTestRoot == null) {
      // set this as needed
      LOG.info("Set shared path to {}", path);
      sharedTestRoot = path;
      return true;
    }
    return false;
  }

  @Test
  public void test_0000_setupTestDir() throws Throwable {
    describe("always ensure directory setup is empty");
    deleteSharedTestRoot();
  }

  @Test
  public void test_0100_setupJobStage() throws Throwable {
    describe("Set up a job");
    verifyPath("Job attempt dir",
        dirs.getJobAttemptDir(),
        new SetupJobStage(getJobStageConfig()).apply(true));
  }

  /**
   * And the check that the stage worked.
   * @throws IOException failure.
   */
  private void verifyJobSetupCompleted() throws IOException {
    assertPathExists("Job attempt dir from test_0100", dirs.getJobAttemptDir());
  }

  @Test
  public void test_0110_setupJobOnlyAllowedOnce() throws Throwable {
    describe("a second creation of a job attempt must fail");
    verifyJobSetupCompleted();
    intercept(FileAlreadyExistsException.class, "", () ->
        new SetupJobStage(getJobStageConfig()).apply(true));
    // job is still there
    assertPathExists("Job attempt dir", dirs.getJobAttemptDir());
  }

  @Test
  public void test_0120_setupJobNewAttemptNumber() throws Throwable {
    describe("Creating a new job attempt is supported");
    verifyJobSetupCompleted();
    Path path = pathMustExist("Job attempt 2 dir",
        new SetupJobStage(createStageConfig(2, -1, 0, destDir))
            .apply(false));
    Assertions.assertThat(path)
        .describedAs("Stage created path")
        .isNotEqualTo(dirs.getJobAttemptDir());
  }

  @Test
  public void test_0200_setupTask00() throws Throwable {
    describe("Set up a task; job must have been set up first");
    verifyJobSetupCompleted();
    verifyPath("Task attempt 00",
        dirs.getTaskAttemptPath(taskAttempt00),
        new SetupTaskStage(ta00Config).apply("first"));
  }

  /**
   * Verify TA00 is set up.
   */
  private void verifyTaskAttempt00SetUp() throws IOException {
    pathMustExist("Dir from taskAttempt00 setup",
        dirs.getTaskAttemptPath(taskAttempt00));
  }

  @Test
  public void test_0210_setupTask00OnlyAllowedOnce() throws Throwable {
    describe("Second attempt to set up task00 must fail.");
    verifyTaskAttempt00SetUp();
    intercept(FileAlreadyExistsException.class, "second", () ->
        new SetupTaskStage(ta00Config).apply("second"));
  }

  @Test
  public void test_0220_setupTask01() throws Throwable {
    describe("Setup task attempt 01");
    verifyTaskAttempt00SetUp();
    verifyPath("Task attempt 01",
        dirs.getTaskAttemptPath(taskAttempt01),
        new SetupTaskStage(ta01Config)
            .apply("01"));
  }

  @Test
  public void test_0230_setupTask10() throws Throwable {
    describe("Setup task attempt 10");
    verifyJobSetupCompleted();
    verifyPath("Task attempt 10",
        dirs.getTaskAttemptPath(taskAttempt10),
        new SetupTaskStage(ta10Config)
            .apply("10"));
  }

  /**
   * Setup then abort task 11 before creating any files;
   * verify that commit fails before creating a manifest file.
   */
  @Test
  public void test_0240_setupThenAbortTask11() throws Throwable {
    describe("Setup then abort task attempt 11");
    verifyJobSetupCompleted();
    Path ta11Path = new SetupTaskStage(ta11Config).apply("11");
    Path deletedDir = new AbortTaskStage(ta11Config).apply(false);
    Assertions.assertThat(ta11Path)
        .isEqualTo(deletedDir);
    assertPathDoesNotExist("aborted directory", ta11Path);
    // execute will fail as there's no dir to list.
    intercept(FileNotFoundException.class, () ->
        new CommitTaskStage(ta11Config).apply(null));
    assertPathDoesNotExist("task manifest",
        manifestPathForTask(dirs.getTaskManifestDir(),
            TASK_IDS.getTaskId(TASK1)));
  }

  /**
   * Execute TA01 by generating a lot of files in its directory
   * then committing the task attempt.
   * The manifest at the task path (i.e. the record of which attempt's
   * output is to be used) MUST now have been generated by this TA.
   */
  @Test
  public void test_0300_executeTask00() throws Throwable {
    describe("Create the files for Task 00, then commit the task");
    List<Path> files = createFilesOrDirs(dirs.getTaskAttemptPath(taskAttempt00),
        "part-00", getExecutorService(),
        DEPTH, WIDTH, FILES_PER_DIRECTORY, false);
    // saves the task manifest to the job dir
    CommitTaskStage.Result result = new CommitTaskStage(ta00Config)
        .apply(null);
    verifyPathExists(getFileSystem(), "manifest",
        result.getPath());

    TaskManifest manifest = result.getTaskManifest();
    manifest.validate();
    // clear the IOStats to reduce the size of the printed JSON.
    manifest.setIOStatistics(null);
    LOG.info("Task Manifest {}", manifest.toJson());
    validateTaskAttemptManifest(this.taskAttempt00, files, manifest);
  }

  /**
   * Validate the manifest of a task attempt.
   * @param attemptId attempt ID
   * @param files files which were created.
   * @param manifest manifest
   * @throws IOException IO problem
   */
  protected void validateTaskAttemptManifest(
      String attemptId,
      List<Path> files,
      TaskManifest manifest) throws IOException {

    verifyManifestTaskAttemptID(manifest, attemptId);

    // validate the manifest
    verifyManifestFilesMatch(manifest, files);
  }

  /**
   * Execute TA01 by generating a lot of files in its directory
   * then committing the task attempt.
   * The manifest at the task path (i.e. the record of which attempt's
   * output is to be used) MUST now have been generated by this TA.
   * Any existing manifest will have been overwritten.
   */
  @Test
  public void test_0310_executeTask01() throws Throwable {
    describe("Create the files for Task 01, then commit the task");
    List<Path> files = createFilesOrDirs(dirs.getTaskAttemptPath(taskAttempt01),
        "part-00", getExecutorService(),
        DEPTH, WIDTH, FILES_PER_DIRECTORY, false);
    // saves the task manifest to the job dir
    CommitTaskStage.Result result = new CommitTaskStage(ta01Config)
        .apply(null);
    Path manifestPath = verifyPathExists(getFileSystem(), "manifest",
        result.getPath()).getPath();

    // load the manifest from the FS, not the return value,
    // so we can verify that last task to commit wins.
    TaskManifest manifest = TaskManifest.load(getFileSystem(), manifestPath);
    manifest.validate();
    // clear the IOStats to reduce the size of the printed JSON.
    manifest.setIOStatistics(null);
    LOG.info("Task Manifest {}", manifest.toJson());

    validateTaskAttemptManifest(taskAttempt01, files, manifest);

  }

  /**
   * Second task writes to more directories, but fewer files per dir.
   * This ensures that there will dirs here which aren't in the first
   * attempt.
   */
  @Test
  public void test_0320_executeTask10() throws Throwable {
    describe("Create the files for Task 10, then commit the task");
    List<Path> files = createFilesOrDirs(
        dirs.getTaskAttemptPath(ta10Config.getTaskAttemptId()),
        "part-01", getExecutorService(),
        DEPTH, WIDTH + 1, FILES_PER_DIRECTORY - 1, false);
    // saves the task manifest to the job dir
    CommitTaskStage.Result result = new CommitTaskStage(ta10Config)
        .apply(null);
    TaskManifest manifest = result.getTaskManifest();
    validateTaskAttemptManifest(taskAttempt10, files, manifest);
  }

  @Test
  public void test_0340_setupThenAbortTask11() throws Throwable {
    describe("Setup then abort task attempt 11");
    Path ta11Path = new SetupTaskStage(ta11Config).apply("11");
    createFilesOrDirs(
        ta11Path,
        "part-01", getExecutorService(),
        2, 1, 1, false);

    new AbortTaskStage(ta11Config).apply(false);
    assertPathDoesNotExist("aborted directory", ta11Path);
    // execute will fail as there's no dir to list.
    intercept(FileNotFoundException.class, () ->
        new CommitTaskStage(ta11Config).apply(null));

    // and the manifest MUST be unchanged from the previous stage
    Path manifestPathForTask1 = manifestPathForTask(dirs.getTaskManifestDir(),
        TASK_IDS.getTaskId(TASK1));
    verifyManifestTaskAttemptID(
        TaskManifest.load(getFileSystem(), manifestPathForTask1),
        taskAttempt10);

  }

  /**
   * Load all the committed manifests, which must be TA01 (last of
   * task 0 to commit) and TA10.
   */
  @Test
  public void test_0400_loadManifests() throws Throwable {
    describe("Load all manifests; committed must be TA01 and TA10");
    File entryFile = File.createTempFile("entry", ".seq");
    LoadManifestsStage.Arguments args = new LoadManifestsStage.Arguments(
        entryFile, DEFAULT_WRITER_QUEUE_CAPACITY);
    LoadManifestsStage.Result result
        = new LoadManifestsStage(getJobStageConfig()).apply(args);

    loadedManifestData = result.getLoadedManifestData();
    Assertions.assertThat(loadedManifestData)
        .describedAs("manifest data from %s", result)
        .isNotNull();

    final LoadManifestsStage.SummaryInfo stageSummary = result.getSummary();
    String summary = stageSummary.toString();
    LOG.info("Manifest summary {}", summary);
    Assertions.assertThat(stageSummary.getTaskAttemptIDs())
        .describedAs("Task attempts in %s", summary)
        .hasSize(2)
        .contains(taskAttempt01, taskAttempt10);
  }

  @Test
  public void test_0410_commitJob() throws Throwable {
    describe("Commit the job");
    CommitJobStage stage = new CommitJobStage(getJobStageConfig());
    stage.apply(new CommitJobStage.Arguments(true, false, null, DISABLED));
  }

  /**
   * Validate that the job output is good by invoking the
   * {@link ValidateRenamedFilesStage} stage to
   * validate all the manifests.
   */
  @Test
  public void test_0420_validateJob() throws Throwable {
    describe("Validate the output of the job through the validation"
        + " stage");
    Assumptions.assumeThat(loadedManifestData)
        .describedAs("Loaded Manifest Data from earlier stage")
        .isNotNull();

    // load in the success data.
    ManifestSuccessData successData = loadAndPrintSuccessData(
        getFileSystem(),
        getJobStageConfig().getJobSuccessMarkerPath());

    // Now verify their files exist, returning the list of renamed files.
    final List<FileEntry> validatedEntries = new ValidateRenamedFilesStage(getJobStageConfig())
        .apply(loadedManifestData.getEntrySequenceData());

    List<String> committedFiles = validatedEntries
        .stream().map(FileEntry::getDest)
        .collect(Collectors.toList());

    // verify that the list of committed files also matches
    // that in the _SUCCESS file
    // note: there's a limit to the #of files in the SUCCESS file
    // to stop writing it slowing down jobs; therefore we don't
    // make a simple "all must match check
    Assertions.assertThat(committedFiles)
        .containsAll(successData.getFilenames());


  }

  @Test
  public void test_0430_validateStatistics() throws Throwable {
    // load in the success data.
    ManifestSuccessData successData = ManifestSuccessData.load(
        getFileSystem(),
        getJobStageConfig().getJobSuccessMarkerPath());
    String json = successData.toJson();
    LOG.info("Success data is {}", json);
    Assertions.assertThat(successData)
        .describedAs("Manifest " + json)
        .returns(NetUtils.getLocalHostname(),
            ManifestSuccessData::getHostname)
        .returns(MANIFEST_COMMITTER_CLASSNAME,
            ManifestSuccessData::getCommitter)
        .returns(jobId,
            ManifestSuccessData::getJobId)
        .returns(true,
            ManifestSuccessData::getSuccess)
        .returns(JOB_ID_SOURCE_MAPREDUCE,
            ManifestSuccessData::getJobIdSource);
    // diagnostics
    Assertions.assertThat(successData.getDiagnostics())
        .containsEntry(PRINCIPAL,
            getCurrentUser().getShortUserName())
        .containsEntry(STAGE, OP_STAGE_JOB_COMMIT);

    // and stats
    IOStatisticsSnapshot iostats = successData.getIOStatistics();

    int files = successData.getFilenames().size();
    verifyStatisticCounterValue(iostats,
        OP_STAGE_JOB_COMMIT, 1);
    assertThatStatisticCounter(iostats,
        COMMITTER_FILES_COMMITTED_COUNT)
        .isGreaterThanOrEqualTo(files);
    Long totalFiles = iostats.counters().get(COMMITTER_FILES_COMMITTED_COUNT);
    verifyStatisticCounterValue(iostats,
        COMMITTER_BYTES_COMMITTED_COUNT, totalFiles * 2);
  }

  @Test
  public void test_0440_validateSuccessFiles() throws Throwable {

    // load in the success data.
    final FileSystem fs = getFileSystem();
    ManifestSuccessData successData = loadAndPrintSuccessData(
        fs,
        getJobStageConfig().getJobSuccessMarkerPath());
    validateGeneratedFiles(fs,
        getJobStageConfig().getDestinationDir(),
        successData, false);
  }

  /**
   * Verify that the validation stage will correctly report a failure
   * if one of the files has as different name.
   */

  @Test
  public void test_0450_validationDetectsFailures() throws Throwable {
    // delete an entry, repeat
    final List<FileEntry> validatedEntries = new ValidateRenamedFilesStage(getJobStageConfig())
        .apply(loadedManifestData.getEntrySequenceData());
    final Path path = validatedEntries.get(0).getDestPath();
    final Path p2 = new Path(path.getParent(), path.getName() + "-renamed");
    final FileSystem fs = getFileSystem();
    fs.rename(path, p2);
    try {
      intercept(OutputValidationException.class, () ->
          new ValidateRenamedFilesStage(getJobStageConfig())
              .apply(loadedManifestData.getEntrySequenceData()));
    } finally {
      // if this doesn't happen, later stages will fail.
      fs.rename(p2, path);
    }
  }

  @Test
  public void test_0900_cleanupJob() throws Throwable {
    describe("Cleanup job");
    CleanupJobStage.Arguments arguments = new CleanupJobStage.Arguments(
        OP_STAGE_JOB_CLEANUP, true, true, false);
    // the first run will list the three task attempt dirs and delete each
    // one before the toplevel dir.
    CleanupJobStage.Result result = new CleanupJobStage(
        getJobStageConfig()).apply(arguments);
    assertCleanupResult(result, CleanupJobStage.Outcome.PARALLEL_DELETE, 1 + 3);
    assertPathDoesNotExist("Job attempt dir", result.getDirectory());

    // not an error if we retry and the dir isn't there
    result = new CleanupJobStage(getJobStageConfig()).apply(arguments);
    assertCleanupResult(result, CleanupJobStage.Outcome.NOTHING_TO_CLEAN_UP, 0);
  }

  /**
   * Needed to clean up the shared test root, as test case teardown
   * does not do it.
   */
  //@Test
  public void test_9999_cleanupTestDir() throws Throwable {
    if (shouldDeleteTestRootAtEndOfTestRun()) {
      deleteSharedTestRoot();
    }
  }

}
