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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.EntryFileIO;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.LoadedManifestData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperations;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.UnreliableManifestStoreOperations;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.RenameFilesStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageConfig;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.SUCCESS_MARKER_FILE_LIMIT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_COMMIT_FILE_RENAME;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterTestSupport.saveManifest;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.getEtag;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.UnreliableManifestStoreOperations.SIMULATED_FAILURE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.AbstractJobOrTaskStage.FAILED_TO_RENAME_PREFIX;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test renaming files with fault injection.
 * This explores etag support and overwrite-on-rename semantics
 * of the target FS, so some of the tests behave differently
 * on different stores.
 */
public class TestRenameStageFailure extends AbstractManifestCommitterTest {

  /**
   * Statistic to look for.
   */
  public static final String RENAME_FAILURES = OP_COMMIT_FILE_RENAME + ".failures";
  private static final int FAILING_FILE_INDEX = 5;

  /**
   * Fault Injection.
   */
  private UnreliableManifestStoreOperations failures;

  /** etags returned in listing/file status operations? */
  private boolean etagsSupported;

  /** etags preserved through rename? */
  private boolean etagsPreserved;

  /** resilient commit expected? */
  private boolean resilientCommit;

  /**
   * Entry file IO.
   */
  private EntryFileIO entryFileIO;

  protected boolean isResilientCommit() {
    return resilientCommit;
  }

  protected boolean isEtagsPreserved() {
    return etagsPreserved;
  }

  protected boolean isEtagsSupported() {
    return etagsSupported;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    final FileSystem fs = getFileSystem();
    final Path methodPath = methodPath();
    etagsSupported = fs.hasPathCapability(methodPath,
        CommonPathCapabilities.ETAGS_AVAILABLE);
    etagsPreserved = fs.hasPathCapability(methodPath,
        CommonPathCapabilities.ETAGS_PRESERVED_IN_RENAME);

    final ManifestStoreOperations wrappedOperations = getStoreOperations();
    failures
        = new UnreliableManifestStoreOperations(wrappedOperations);
    setStoreOperations(failures);
    resilientCommit = wrappedOperations.storeSupportsResilientCommit();
    entryFileIO = new EntryFileIO(getConfiguration());
  }

  /**
   * Does this test suite require rename resilience in the store/FS?
   * @return true if the store operations are resilient.
   */
  protected boolean requireRenameResilience() throws IOException {
    return false;
  }

  @Test
  public void testResilienceAsExpected() throws Throwable {
    Assertions.assertThat(isResilientCommit())
        .describedAs("resilient commit support")
        .isEqualTo(requireRenameResilience());
  }

  @Test
  public void testRenameSourceException() throws Throwable {
    describe("rename fails raising an IOE -expect stage to fail" +
        " and exception message preserved");

    // destination directory.
    Path destDir = methodPath();
    StageConfig stageConfig = createStageConfigForJob(JOB1, destDir);
    Path jobAttemptTaskSubDir = stageConfig.getJobAttemptTaskSubDir();

    // create a manifest with a lot of files, but for
    // which one of whose renames will fail
    TaskManifest manifest = new TaskManifest();
    createFileset(destDir, jobAttemptTaskSubDir, manifest, filesToCreate());
    final List<FileEntry> filesToCommit = manifest.getFilesToCommit();
    final FileEntry entry = filesToCommit.get(FAILING_FILE_INDEX);
    failures.addRenameSourceFilesToFail(entry.getSourcePath());

    // rename MUST fail
    expectRenameFailure(
        new RenameFilesStage(stageConfig),
        manifest,
        filesToCommit.size(),
        SIMULATED_FAILURE,
        PathIOException.class);
  }

  /**
   * Number of files to create; must be more than
   * {@link #FAILING_FILE_INDEX}.
   */
  protected int filesToCreate() {
    return 100;
  }

  @Test
  public void testCommitMissingFile() throws Throwable {
    describe("commit a file which doesn't exist. Expect FNFE always");
    // destination directory.
    Path destDir = methodPath();
    StageConfig stageConfig = createStageConfigForJob(JOB1, destDir);
    Path jobAttemptTaskSubDir = stageConfig.getJobAttemptTaskSubDir();
    TaskManifest manifest = new TaskManifest();
    final List<FileEntry> filesToCommit = manifest.getFilesToCommit();

    Path source = new Path(jobAttemptTaskSubDir, "source.parquet");
    Path dest = new Path(destDir, "destdir.parquet");
    filesToCommit.add(new FileEntry(source, dest, 0, null));
    final FileNotFoundException ex = expectRenameFailure(
        new RenameFilesStage(stageConfig),
        manifest,
        0,
        "",
        FileNotFoundException.class);
    LOG.info("Exception raised: {}", ex.toString());
  }

  /**
   * Verify that when a job is configured to delete target paths,
   * renaming will overwrite them.
   * This test has to use FileSystem contract settings to determine
   * whether or not the FS will actually permit file-over-file rename.
   * As POSIX does, local filesystem tests will not fail if the
   * destination exists.
   * As ABFS and GCS do reject it, they are required to fail the
   * first rename sequence, but succeed once delete.target.paths
   * is true.
   */
  @Test
  public void testDeleteTargetPaths() throws Throwable {
    describe("Verify that target path deletion works");
    // destination directory.
    Path destDir = methodPath();
    StageConfig stageConfig = createStageConfigForJob(JOB1, destDir)
        .withDeleteTargetPaths(true);
    Path jobAttemptTaskSubDir = stageConfig.getJobAttemptTaskSubDir();
    final Path source = new Path(jobAttemptTaskSubDir, "source.txt");
    final Path dest = new Path(destDir, "source.txt");
    final byte[] sourceData = "data".getBytes(StandardCharsets.UTF_8);
    final FileSystem fs = getFileSystem();
    ContractTestUtils.createFile(fs, source, false, sourceData);
    touch(fs, dest);
    TaskManifest manifest = new TaskManifest();
    final FileEntry entry = createEntryWithEtag(source, dest);
    manifest.addFileToCommit(entry);

    List<TaskManifest> manifests = new ArrayList<>();
    manifests.add(manifest);

    // local POSIX filesystems allow rename of file onto file, so
    // don't fail on the rename.
    boolean renameOverwritesDest = isSupported(RENAME_OVERWRITES_DEST);

    if (!renameOverwritesDest) {
      // HDFS, ABFS and GCS do all reject rename of file onto file.
      // ABFS will use its rename operation so will even raise a
      // meaningful exception here.
      final IOException ex = expectRenameFailure(
          new RenameFilesStage(stageConfig.withDeleteTargetPaths(false)),
          manifest,
          0,
          "",
          IOException.class);
      LOG.info("Exception raised: {}", ex.toString());
    }

    final LoadedManifestData manifestData = saveManifest(entryFileIO, manifest);

    // delete target paths and it works
    try {
      new RenameFilesStage(stageConfig.withDeleteTargetPaths(true))
          .apply(Triple.of(manifestData, Collections.emptySet(), SUCCESS_MARKER_FILE_LIMIT));
    } finally {
      manifestData.getEntrySequenceFile().delete();
    }

    // and the new data made it over
    verifyFileContents(fs, dest, sourceData);

    // lets check the etag too, for completeness
    if (isEtagsPreserved()) {
      Assertions.assertThat(getEtag(fs.getFileStatus(dest)))
          .describedAs("Etag of destination file %s", dest)
          .isEqualTo(entry.getEtag());
    }

  }

  @Test
  public void testRenameReturnsFalse() throws Throwable {
    describe("commit where rename() returns false for one file." +
        " Expect failure to be escalated to an IOE");

    Assume.assumeTrue("not used when resilient commits are available",
        !resilientCommit);
    // destination directory.
    Path destDir = methodPath();
    StageConfig stageConfig = createStageConfigForJob(JOB1, destDir);
    Path jobAttemptTaskSubDir = stageConfig.getJobAttemptTaskSubDir();

    // create a manifest with a lot of files, but for
    // which one of whose renames will fail
    TaskManifest manifest = new TaskManifest();
    createFileset(destDir, jobAttemptTaskSubDir, manifest, filesToCreate());

    final List<FileEntry> filesToCommit = manifest.getFilesToCommit();
    final FileEntry entry = filesToCommit.get(FAILING_FILE_INDEX);
    failures.addRenameSourceFilesToFail(entry.getSourcePath());

    // switch to rename returning false.; again, this must
    // be escalated to a failure.
    failures.setRenameToFailWithException(false);
    expectRenameFailure(
        new RenameFilesStage(stageConfig),
        manifest,
        filesToCommit.size(),
        FAILED_TO_RENAME_PREFIX,
        PathIOException.class);
  }

  /**
   * Create the source files for a task.
   * @param destDir destination directory
   * @param taskAttemptDir directory of the task attempt
   * @param manifest manifest to update.
   * @param fileCount how many files.
   */
  private void createFileset(
      final Path destDir,
      final Path taskAttemptDir,
      final TaskManifest manifest,
      final int fileCount) throws IOException {
    final FileSystem fs = getFileSystem();
    for (int i = 0; i < fileCount; i++) {
      String name = String.format("file%04d", i);
      Path src = new Path(taskAttemptDir, name);
      Path dest = new Path(destDir, name);
      touch(fs, src);

      final FileEntry entry = createEntryWithEtag(src, dest);
      manifest.addFileToCommit(entry);
    }
  }

  /**
   * Create a manifest entry, including size.
   * If the FS supports etags, one is retrieved.
   * @param source source
   * @param dest dest
   * @return entry
   * @throws IOException if getFileStatus failed.
   */
  private FileEntry createEntryWithEtag(final Path source,
      final Path dest)
      throws IOException {
    final FileStatus st = getFileSystem().getFileStatus(source);
    final String etag = isEtagsSupported()
        ? getEtag(st)
        : null;

    return new FileEntry(source, dest, st.getLen(), etag);
  }

  /**
   * Execute rename, expecting a failure.
   * The number of files renamed MUST be less than the value of {@code files}
   * @param stage stage
   * @param manifest task manifests
   * @param files number of files being renamed.
   * @param errorText text which must be in the exception string
   * @param exceptionClass class of the exception
   * @return the caught exception
   * @throws Exception if anything else went wrong, or no exception was raised.
   */
  private <E extends Throwable> E expectRenameFailure(
      RenameFilesStage stage,
      TaskManifest manifest,
      int files,
      String errorText,
      Class<E> exceptionClass) throws Exception {

    List<TaskManifest> manifests = new ArrayList<>();
    manifests.add(manifest);
    ProgressCounter progressCounter = getProgressCounter();
    progressCounter.reset();
    IOStatisticsStore iostatistics = stage.getIOStatistics();
    long failures0 = iostatistics.counters().get(RENAME_FAILURES);

    final LoadedManifestData manifestData = saveManifest(entryFileIO, manifest);
    // rename MUST raise an exception.
    E ex;
    try {
      ex = intercept(exceptionClass, errorText, () ->
          stage.apply(Triple.of(manifestData, Collections.emptySet(), SUCCESS_MARKER_FILE_LIMIT)));
    } finally {
      manifestData.getEntrySequenceFile().delete();
    }

    LOG.info("Statistics {}", ioStatisticsToPrettyString(iostatistics));
    // the IOStatistics record the rename as a failure.
    assertThatStatisticCounter(iostatistics, RENAME_FAILURES)
        .isEqualTo(failures0 + 1);

    // count of files committed MUST be less than expected.
    if (files > 0) {

      Assertions.assertThat(stage.getFilesCommitted())
          .describedAs("Files Committed by stage")
          .isNotEmpty()
          .hasSizeLessThan(files);

    }

    // the progress counter will show that the rename did invoke it.
    // there's no assertion on the actual value as it depends on
    // execution time of the threads.

    Assertions.assertThat(progressCounter.value())
        .describedAs("Progress counter %s", progressCounter)
        .isGreaterThan(0);
    return ex;
  }
}
