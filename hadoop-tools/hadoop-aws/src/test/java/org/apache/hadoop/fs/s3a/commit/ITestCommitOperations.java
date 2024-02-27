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

package org.apache.hadoop.fs.s3a.commit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.auth.ProgressCounter;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.fs.s3a.commit.impl.CommitContext;
import org.apache.hadoop.fs.s3a.commit.impl.CommitOperations;
import org.apache.hadoop.fs.s3a.commit.magic.MagicCommitTracker;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.Lists;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.fs.s3a.commit.CommitUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitterTestHelper.assertIsMagicStream;
import static org.apache.hadoop.fs.s3a.commit.MagicCommitPaths.*;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.statistics.impl.EmptyS3AStatisticsContext.EMPTY_BLOCK_OUTPUT_STREAM_STATISTICS;
import static org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test the low-level binding of the S3A FS to the magic commit mechanism,
 * and handling of the commit operations.
 */
public class ITestCommitOperations extends AbstractCommitITest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestCommitOperations.class);
  private static final byte[] DATASET = dataset(1000, 'a', 32);
  private static final String S3A_FACTORY_KEY = String.format(
      COMMITTER_FACTORY_SCHEME_PATTERN, "s3a");
  private static final String JOB_ID = UUID.randomUUID().toString();

  private ProgressCounter progress;

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    bindCommitter(conf, CommitConstants.S3A_COMMITTER_FACTORY,
        CommitConstants.COMMITTER_NAME_MAGIC);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    FileSystem.closeAll();
    super.setup();
    verifyIsMagicCommitFS(getFileSystem());
    progress = new ProgressCounter();
    progress.assertCount("progress", 0);
  }

  @Test
  public void testCreateTrackerNormalPath() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    MagicCommitIntegration integration
        = new MagicCommitIntegration(fs, true);
    String filename = "notdelayed.txt";
    Path destFile = methodSubPath(filename);
    String origKey = fs.pathToKey(destFile);
    PutTracker tracker = integration.createTracker(destFile, origKey,
        EMPTY_BLOCK_OUTPUT_STREAM_STATISTICS);
    Assertions.assertThat(tracker)
        .describedAs("Tracker for %s", destFile)
        .isNotInstanceOf(MagicCommitTracker.class);
  }

  /**
   * On a magic path, the magic tracker is returned.
   * @throws Throwable failure
   */
  @Test
  public void testCreateTrackerMagicPath() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    MagicCommitIntegration integration
        = new MagicCommitIntegration(fs, true);
    String filename = "delayed.txt";
    Path destFile = methodSubPath(filename);
    String origKey = fs.pathToKey(destFile);
    Path pendingPath = makeMagic(destFile);
    verifyIsMagicCommitPath(fs, pendingPath);
    String pendingPathKey = fs.pathToKey(pendingPath);
    Assertions.assertThat(pendingPathKey)
        .describedAs("pending path")
        .endsWith(filename);
    final List<String> elements = splitPathToElements(pendingPath);
    Assertions.assertThat(lastElement(elements))
        .describedAs("splitPathToElements(%s)", pendingPath)
        .isEqualTo(filename);
    List<String> finalDestination = finalDestination(elements);
    Assertions.assertThat(lastElement(finalDestination))
        .describedAs("finalDestination(%s)", pendingPath)
        .isEqualTo(filename);
    Assertions.assertThat(elementsToKey(finalDestination))
        .describedAs("destination key")
        .isEqualTo(origKey);

    PutTracker tracker = integration.createTracker(pendingPath,
        pendingPathKey, EMPTY_BLOCK_OUTPUT_STREAM_STATISTICS);
    Assertions.assertThat(tracker)
        .describedAs("Tracker for %s", pendingPathKey)
        .isInstanceOf(MagicCommitTracker.class);
    Assertions.assertThat(tracker.getDestKey())
        .describedAs("tracker destination key")
        .isEqualTo(origKey);

    assertNotDelayedWrite(new Path(pendingPath,
        "part-0000" + PENDING_SUFFIX));
    assertNotDelayedWrite(new Path(pendingPath,
        "part-0000" + PENDINGSET_SUFFIX));
  }

  private void assertNotDelayedWrite(Path pendingSuffixedPath) {
    Assertions.assertThat(getFileSystem().isMagicCommitPath(pendingSuffixedPath))
        .describedAs("Expected %s to not be magic/delayed write", pendingSuffixedPath)
        .isFalse();
  }

  @Test
  public void testCreateAbortEmptyFile() throws Throwable {
    describe("create then abort an empty file; throttled");
    S3AFileSystem fs = getFileSystem();
    String filename = "empty-abort.txt";
    Path destFile = methodSubPath(filename);
    Path pendingFilePath = makeMagic(destFile);
    touch(fs, pendingFilePath);

    validateIntermediateAndFinalPaths(pendingFilePath, destFile);
    Path pendingDataPath = validatePendingCommitData(filename,
        pendingFilePath);

    CommitOperations actions = newCommitOperations();
    // abort,; rethrow on failure

    LOG.info("Abort call");
    Path parent = pendingDataPath.getParent();
    try (CommitContext commitContext =
             actions.createCommitContextForTesting(parent, JOB_ID, 0)) {
      actions.abortAllSinglePendingCommits(parent, commitContext, true)
          .maybeRethrow();
    }

    assertPathDoesNotExist("pending file not deleted", pendingDataPath);
    assertPathDoesNotExist("dest file was created", destFile);
  }

  /**
   * Create a new commit operations instance for the test FS.
   * @return commit operations.
   * @throws IOException IO failure.
   */
  private CommitOperations newCommitOperations()
      throws IOException {
    return new CommitOperations(getFileSystem());
  }

  /**
   * Create a new path which has the same filename as the dest file, but
   * is in a magic directory under the destination dir.
   * @param destFile final destination file
   * @return magic path
   */
  private static Path makeMagic(Path destFile) {
    return new Path(destFile.getParent(),
        MAGIC_PATH_PREFIX + JOB_ID + '/' + destFile.getName());
  }

  @Test
  public void testCommitEmptyFile() throws Throwable {
    describe("create then commit an empty magic file");
    createCommitAndVerify("empty-commit.txt", new byte[0]);
  }

  @Test
  public void testCommitSmallFile() throws Throwable {
    describe("create then commit a small magic file");
    createCommitAndVerify("small-commit.txt", DATASET);
  }

  @Test
  public void testAbortNonexistentDir() throws Throwable {
    describe("Attempt to abort a directory that does not exist");
    Path destFile = methodSubPath("testAbortNonexistentPath");
    final CommitOperations operations = newCommitOperations();
    try (CommitContext commitContext
             = operations.createCommitContextForTesting(destFile, JOB_ID, 0)) {
      final CommitOperations.MaybeIOE outcome = operations
          .abortAllSinglePendingCommits(destFile, commitContext, true);
      outcome.maybeRethrow();
      Assertions.assertThat(outcome)
          .isEqualTo(CommitOperations.MaybeIOE.NONE);
    }
  }

  @Test
  public void testCommitterFactoryDefault() throws Throwable {
    Configuration conf = new Configuration();
    Path dest = methodPath();
    conf.set(COMMITTER_FACTORY_CLASS,
        MagicS3GuardCommitterFactory.CLASSNAME);
    PathOutputCommitterFactory factory
        = getCommitterFactory(dest, conf);
    PathOutputCommitter committer = factory.createOutputCommitter(
        methodPath(),
        new TaskAttemptContextImpl(getConfiguration(),
            new TaskAttemptID(new TaskID(), 1)));
    assertEquals("Wrong committer",
        MagicS3GuardCommitter.class, committer.getClass());
  }

  @Test
  public void testCommitterFactorySchema() throws Throwable {
    Configuration conf = new Configuration();
    Path dest = methodPath();

    conf.set(S3A_FACTORY_KEY,
        MagicS3GuardCommitterFactory.CLASSNAME);
    PathOutputCommitterFactory factory
        = getCommitterFactory(dest, conf);
    // the casting is an implicit type check
    MagicS3GuardCommitter s3a = (MagicS3GuardCommitter)
        factory.createOutputCommitter(
            methodPath(),
            new TaskAttemptContextImpl(getConfiguration(),
            new TaskAttemptID(new TaskID(), 1)));
    // should never get here
    assertNotNull(s3a);
  }

  @Test
  public void testBaseRelativePath() throws Throwable {
    describe("Test creating file with a __base marker and verify that it ends" +
        " up in where expected");
    S3AFileSystem fs = getFileSystem();
    Path destDir = methodSubPath("testBaseRelativePath");
    fs.delete(destDir, true);
    Path pendingBaseDir = new Path(destDir, MAGIC_PATH_PREFIX + JOB_ID + "/child/" + BASE);
    String child = "subdir/child.txt";
    Path pendingChildPath = new Path(pendingBaseDir, child);
    Path expectedDestPath = new Path(destDir, child);
    assertPathDoesNotExist("dest file was found before upload",
        expectedDestPath);

    createFile(fs, pendingChildPath, true, DATASET);
    commit("child.txt", pendingChildPath, expectedDestPath);
  }

  /**
   * Verify that that when a marker file is renamed, its
   * magic marker attribute is lost.
   */
  @Test
  public void testMarkerFileRename()
      throws Exception {
    S3AFileSystem fs = getFileSystem();
    Path destFile = methodPath();
    Path destDir = destFile.getParent();
    fs.delete(destDir, true);
    Path magicDest = makeMagic(destFile);
    Path magicDir = magicDest.getParent();
    fs.mkdirs(magicDest);

    // use the builder API to verify it works exactly the
    // same.
    FSDataOutputStreamBuilder builder = fs.createFile(magicDest)
        .overwrite(true);
    builder.recursive();
    // this has a broken return type; not sure why
    builder.must(FS_S3A_CREATE_PERFORMANCE, true);

    try (FSDataOutputStream stream = builder.build()) {
      assertIsMagicStream(stream);
      stream.write(DATASET);
    }
    Path magic2 = new Path(magicDir, "magic2");
    // rename the marker
    fs.rename(magicDest, magic2);

    // the renamed file has no header
    getTestHelper().assertFileLacksMarkerHeader(magic2);
    // abort the upload, which is driven by the .pending files
    // there must be 1 deleted file; during test debugging with aborted
    // runs there may be more.
    Assertions.assertThat(newCommitOperations()
        .abortPendingUploadsUnderPath(destDir))
        .describedAs("Aborting all pending uploads under %s", destDir)
        .isGreaterThanOrEqualTo(1);
  }

  /**
   * Create a file through the magic commit mechanism.
   * @param filename file to create (with "MAGIC PATH".)
   * @param data data to write
   * @throws Exception failure
   */
  private void createCommitAndVerify(String filename, byte[] data)
      throws Exception {
    S3AFileSystem fs = getFileSystem();
    Path destFile = methodSubPath(filename);
    fs.delete(destFile.getParent(), true);
    Path magicDest = makeMagic(destFile);
    assertPathDoesNotExist("Magic file should not exist", magicDest);
    long dataSize = data != null ? data.length : 0;
    try (FSDataOutputStream stream = fs.create(magicDest, true)) {
      assertIsMagicStream(stream);
      if (dataSize > 0) {
        stream.write(data);
      }
    }
    getTestHelper().assertIsMarkerFile(magicDest, dataSize);
    commit(filename, destFile);
    verifyFileContents(fs, destFile, data);
    // the destination file doesn't have the attribute
    getTestHelper().assertFileLacksMarkerHeader(destFile);
  }

  /**
   * Commit the file, with before and after checks on the dest and magic
   * values.
   * @param filename filename of file
   * @param destFile destination path of file
   * @throws Exception any failure of the operation
   */
  private void commit(String filename,
      Path destFile) throws Exception {
    commit(filename, makeMagic(destFile), destFile);
  }

  /**
   * Commit to a write to {@code magicFile} which is expected to
   * be saved to {@code destFile}.
   * @param magicFile path to write to
   * @param destFile destination to verify
   */
  private void commit(String filename,
      Path magicFile,
      Path destFile)
      throws IOException {

    final CommitOperations actions = newCommitOperations();
    validateIntermediateAndFinalPaths(magicFile, destFile);
    SinglePendingCommit commit = SinglePendingCommit.load(getFileSystem(),
        validatePendingCommitData(filename, magicFile),
        null,
        SinglePendingCommit.serializer());

    commitOrFail(destFile, commit, actions);
  }

  private void commitOrFail(final Path destFile,
      final SinglePendingCommit commit, final CommitOperations actions)
      throws IOException {
    try (CommitContext commitContext
             = actions.createCommitContextForTesting(destFile, JOB_ID, 0)) {
      commitContext.commitOrFail(commit);
    }
  }

  /**
   * Perform any validation of paths.
   * @param magicFilePath path to magic file
   * @param destFile ultimate destination file
   * @throws IOException IO failure
   */
  private void validateIntermediateAndFinalPaths(Path magicFilePath,
      Path destFile)
      throws IOException {
    assertPathDoesNotExist("dest file was created", destFile);
  }

  /**
   * Validate that a pending commit data file exists, load it and validate
   * its contents.
   * @param filename short file name
   * @param magicFile path that the file thinks that it was written to
   * @return the path to the pending set
   * @throws IOException IO problems
   */
  private Path validatePendingCommitData(String filename,
      Path magicFile) throws IOException {
    S3AFileSystem fs = getFileSystem();
    Path pendingDataPath = new Path(magicFile.getParent(),
        filename + PENDING_SUFFIX);
    FileStatus fileStatus = verifyPathExists(fs, "no pending file",
        pendingDataPath);
    assertTrue("No data in " + fileStatus, fileStatus.getLen() > 0);
    String data = read(fs, pendingDataPath);
    LOG.info("Contents of {}: \n{}", pendingDataPath, data);
    // really read it in and parse
    SinglePendingCommit persisted = SinglePendingCommit.serializer()
        .load(fs, pendingDataPath);
    persisted.validate();
    Assertions.assertThat(persisted.getCreated())
        .describedAs("Created timestamp in %s", persisted)
        .isGreaterThan(0);
    Assertions.assertThat(persisted.getSaved())
        .describedAs("saved timestamp in %s", persisted)
        .isGreaterThan(0);
    List<String> etags = persisted.getEtags();
    Assertions.assertThat(etags)
        .describedAs("Etag list")
        .hasSize(1);
    Assertions.assertThat(CommitOperations.toPartEtags(etags))
        .describedAs("Etags to parts")
        .hasSize(1);
    return pendingDataPath;
  }

  /**
   * Get a method-relative path.
   * @param filename filename
   * @return new path
   * @throws IOException failure to create/parse the path.
   */
  private Path methodSubPath(String filename) throws IOException {
    return new Path(methodPath(), filename);
  }

  @Test
  public void testUploadEmptyFile() throws Throwable {
    describe("Upload a zero byte file to a magic path");
    File tempFile = File.createTempFile("commit", ".txt");
    CommitOperations actions = newCommitOperations();
    Path dest = methodSubPath("testUploadEmptyFile");
    S3AFileSystem fs = getFileSystem();
    fs.delete(dest, false);

    SinglePendingCommit pendingCommit =
        actions.uploadFileToPendingCommit(tempFile,
            dest,
            null,
            DEFAULT_MULTIPART_SIZE,
            progress);

    assertPathDoesNotExist("pending commit", dest);

    commitOrFail(dest, pendingCommit, actions);

    progress.assertCount("Progress counter should be 1.",
        1);
    FileStatus status = verifyPathExists(fs,
        "uploaded file commit", dest);
    Assertions.assertThat(status.getLen())
        .describedAs("Committed File file %s: %s", dest, status)
        .isEqualTo(0);
    getTestHelper().assertFileLacksMarkerHeader(dest);
  }

  @Test
  public void testUploadSmallFile() throws Throwable {
    File tempFile = File.createTempFile("commit", ".txt");
    String text = "hello, world";
    FileUtils.write(tempFile, text, StandardCharsets.UTF_8);
    CommitOperations actions = newCommitOperations();
    Path dest = methodSubPath("testUploadSmallFile");
    S3AFileSystem fs = getFileSystem();
    fs.delete(dest, true);

    assertPathDoesNotExist("test setup", dest);
    SinglePendingCommit pendingCommit =
        actions.uploadFileToPendingCommit(tempFile,
            dest,
            null,
            DEFAULT_MULTIPART_SIZE,
            progress);

    assertPathDoesNotExist("pending commit", dest);

    LOG.debug("Postcommit validation");
    commitOrFail(dest, pendingCommit, actions);

    String s = readUTF8(fs, dest, -1);
    Assertions.assertThat(s)
        .describedAs("contents of committed file %s", dest)
        .isEqualTo(text);
    progress.assertCount("Progress counter should be 1.",
        1);
  }

  @Test
  public void testUploadMissingFile() throws Throwable {
    File tempFile = File.createTempFile("commit", ".txt");
    tempFile.delete();
    CommitOperations actions = newCommitOperations();
    Path dest = methodSubPath("testUploadMissingFile");
    intercept(FileNotFoundException.class, () ->
        actions.uploadFileToPendingCommit(tempFile, dest, null,
            DEFAULT_MULTIPART_SIZE, progress));
    progress.assertCount("Progress counter should be 0.",
        0);
  }

  @Test
  public void testRevertCommit() throws Throwable {
    describe("Revert a commit; the destination file will be deleted");
    Path destFile = methodSubPath("part-0000");
    S3AFileSystem fs = getFileSystem();
    touch(fs, destFile);
    SinglePendingCommit commit = new SinglePendingCommit();
    CommitOperations actions = newCommitOperations();
    commit.setDestinationKey(fs.pathToKey(destFile));
    newCommitOperations().revertCommit(commit);
    assertPathDoesNotExist("should have been reverted", destFile);
  }

  @Test
  public void testRevertMissingCommit() throws Throwable {
    Path destFile = methodSubPath("part-0000");
    S3AFileSystem fs = getFileSystem();
    fs.delete(destFile, false);
    SinglePendingCommit commit = new SinglePendingCommit();
    commit.setDestinationKey(fs.pathToKey(destFile));
    newCommitOperations().revertCommit(commit);
    assertPathDoesNotExist("should have been reverted", destFile);
  }

  @Test
  public void testFailuresInAbortListing() throws Throwable {
    Path path = path("testFailuresInAbort");
    getFileSystem().mkdirs(path);
    LOG.info("Aborting");
    newCommitOperations().abortPendingUploadsUnderPath(path);
    LOG.info("Abort completed");
  }

  /**
   * Test a normal stream still works as expected in a magic filesystem,
   * with a call of {@code hasCapability()} to check that it is normal.
   * @throws Throwable failure
   */
  @Test
  public void testWriteNormalStream() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    Path destFile = path("normal");
    try (FSDataOutputStream out = fs.create(destFile, true)) {
      out.writeChars("data");
      assertFalse("stream has magic output: " + out,
          out.hasCapability(STREAM_CAPABILITY_MAGIC_OUTPUT));
    }
    FileStatus status = fs.getFileStatus(destFile);
    Assertions.assertThat(status.getLen())
        .describedAs("Normal file %s: %s", destFile, status)
        .isGreaterThan(0);
  }

  /**
   * Creates a bulk commit and commits multiple files.
   */
  @Test
  public void testBulkCommitFiles() throws Throwable {
    describe("verify bulk commit");
    File localFile = File.createTempFile("commit", ".txt");
    CommitOperations actions = newCommitOperations();
    Path destDir = methodSubPath("out");
    S3AFileSystem fs = getFileSystem();
    fs.delete(destDir, false);

    Path destFile1 = new Path(destDir, "file1");
    // this subdir will only be created in the commit of file 2
    Path subdir = new Path(destDir, "subdir");
    // file 2
    Path destFile2 = new Path(subdir, "file2");
    Path destFile3 = new Path(subdir, "file3 with space");
    List<Path> destinations = Lists.newArrayList(destFile1, destFile2,
        destFile3);
    List<SinglePendingCommit> commits = new ArrayList<>(3);

    for (Path destination: destinations) {
      SinglePendingCommit commit1 =
          actions.uploadFileToPendingCommit(localFile,
              destination, null,
              DEFAULT_MULTIPART_SIZE,
              progress);
      commits.add(commit1);
    }

    if (!isS3ExpressStorage(fs)) {
      assertPathDoesNotExist("destination dir", destDir);
      assertPathDoesNotExist("subdirectory", subdir);
    }
    LOG.info("Initiating commit operations");
    try (CommitContext commitContext
             = actions.createCommitContextForTesting(destDir, JOB_ID, 0)) {
      LOG.info("Commit #1");
      commitContext.commitOrFail(commits.get(0));
      final String firstCommitContextString = commitContext.toString();
      LOG.info("First Commit state {}", firstCommitContextString);
      assertPathExists("destFile1", destFile1);
      assertPathExists("destination dir", destDir);

      LOG.info("Commit #2");
      commitContext.commitOrFail(commits.get(1));
      assertPathExists("subdirectory", subdir);
      assertPathExists("destFile2", destFile2);
      final String secondCommitContextString = commitContext.toString();
      LOG.info("Second Commit state {}", secondCommitContextString);

      LOG.info("Commit #3");
      commitContext.commitOrFail(commits.get(2));
      assertPathExists("destFile3", destFile3);
    }

  }

}
