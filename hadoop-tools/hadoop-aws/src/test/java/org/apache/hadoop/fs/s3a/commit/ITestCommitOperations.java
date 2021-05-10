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
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.model.PartETag;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.auth.ProgressCounter;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.fs.s3a.commit.magic.MagicCommitTracker;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;
import static org.apache.hadoop.fs.s3a.commit.CommitOperations.extractMagicFileLength;
import static org.apache.hadoop.fs.s3a.commit.CommitUtils.*;
import static org.apache.hadoop.fs.s3a.commit.MagicCommitPaths.*;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory.*;

/**
 * Test the low-level binding of the S3A FS to the magic commit mechanism,
 * and handling of the commit operations.
 * This is done with an inconsistent client.
 */
public class ITestCommitOperations extends AbstractCommitITest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestCommitOperations.class);
  private static final byte[] DATASET = dataset(1000, 'a', 32);
  private static final String S3A_FACTORY_KEY = String.format(
      COMMITTER_FACTORY_SCHEME_PATTERN, "s3a");
  private ProgressCounter progress;

  /**
   * A compile time flag which allows you to disable failure reset before
   * assertions and teardown.
   * As S3A is now required to be resilient to failure on all FS operations,
   * setting it to false ensures that even the assertions are checking
   * the resilience codepaths.
   */
  private static final boolean RESET_FAILURES_ENABLED = false;

  private static final float HIGH_THROTTLE = 0.25f;

  private static final float FULL_THROTTLE = 1.0f;

  private static final int STANDARD_FAILURE_LIMIT = 2;

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    bindCommitter(conf, CommitConstants.S3A_COMMITTER_FACTORY,
        CommitConstants.COMMITTER_NAME_MAGIC);
    return conf;
  }

  @Override
  public boolean useInconsistentClient() {
    return true;
  }

  @Override
  public void setup() throws Exception {
    FileSystem.closeAll();
    super.setup();
    verifyIsMagicCommitFS(getFileSystem());
    // abort,; rethrow on failure
    setThrottling(HIGH_THROTTLE, STANDARD_FAILURE_LIMIT);
    progress = new ProgressCounter();
    progress.assertCount("progress", 0);
  }

  @Test
  public void testCreateTrackerNormalPath() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    MagicCommitIntegration integration
        = new MagicCommitIntegration(fs, true);
    String filename = "notdelayed.txt";
    Path destFile = methodPath(filename);
    String origKey = fs.pathToKey(destFile);
    PutTracker tracker = integration.createTracker(destFile, origKey);
    assertFalse("wrong type: " + tracker + " for " + destFile,
        tracker instanceof MagicCommitTracker);
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
    Path destFile = methodPath(filename);
    String origKey = fs.pathToKey(destFile);
    Path pendingPath = makeMagic(destFile);
    verifyIsMagicCommitPath(fs, pendingPath);
    String pendingPathKey = fs.pathToKey(pendingPath);
    assertTrue("wrong path of " + pendingPathKey,
        pendingPathKey.endsWith(filename));
    final List<String> elements = splitPathToElements(pendingPath);
    assertEquals("splitPathToElements()", filename, lastElement(elements));
    List<String> finalDestination = finalDestination(elements);
    assertEquals("finalDestination()",
        filename,
        lastElement(finalDestination));
    final String destKey = elementsToKey(finalDestination);
    assertEquals("destination key", origKey, destKey);

    PutTracker tracker = integration.createTracker(pendingPath,
        pendingPathKey);
    assertTrue("wrong type: " + tracker + " for " + pendingPathKey,
        tracker instanceof MagicCommitTracker);
    assertEquals("tracker destination key", origKey, tracker.getDestKey());

    Path pendingSuffixedPath = new Path(pendingPath,
        "part-0000" + PENDING_SUFFIX);
    assertFalse("still a delayed complete path " + pendingSuffixedPath,
        fs.isMagicCommitPath(pendingSuffixedPath));
    Path pendingSet = new Path(pendingPath,
        "part-0000" + PENDINGSET_SUFFIX);
    assertFalse("still a delayed complete path " + pendingSet,
        fs.isMagicCommitPath(pendingSet));
  }

  @Test
  public void testCreateAbortEmptyFile() throws Throwable {
    describe("create then abort an empty file; throttled");
    S3AFileSystem fs = getFileSystem();
    String filename = "empty-abort.txt";
    Path destFile = methodPath(filename);
    Path pendingFilePath = makeMagic(destFile);
    touch(fs, pendingFilePath);
    waitForConsistency();
    validateIntermediateAndFinalPaths(pendingFilePath, destFile);
    Path pendingDataPath = validatePendingCommitData(filename,
        pendingFilePath);

    CommitOperations actions = newCommitOperations();
    // abort,; rethrow on failure
    fullThrottle();
    LOG.info("Abort call");
    actions.abortAllSinglePendingCommits(pendingDataPath.getParent(), true)
        .maybeRethrow();
    resetFailures();
    assertPathDoesNotExist("pending file not deleted", pendingDataPath);
    assertPathDoesNotExist("dest file was created", destFile);
  }

  private void fullThrottle() {
    setThrottling(FULL_THROTTLE, STANDARD_FAILURE_LIMIT);
  }

  private CommitOperations newCommitOperations() {
    return new CommitOperations(getFileSystem());
  }

  @Override
  protected void resetFailures() {
    if (!RESET_FAILURES_ENABLED) {
      super.resetFailures();
    }
  }

  /**
   * Create a new path which has the same filename as the dest file, but
   * is in a magic directory under the destination dir.
   * @param destFile final destination file
   * @return magic path
   */
  private static Path makeMagic(Path destFile) {
    return new Path(destFile.getParent(),
        MAGIC + '/' + destFile.getName());
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
    Path destFile = methodPath("testAbortNonexistentPath");
    newCommitOperations()
        .abortAllSinglePendingCommits(destFile, true)
        .maybeRethrow();
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
    Path destDir = methodPath("testBaseRelativePath");
    fs.delete(destDir, true);
    Path pendingBaseDir = new Path(destDir, MAGIC + "/child/" + BASE);
    String child = "subdir/child.txt";
    Path pendingChildPath = new Path(pendingBaseDir, child);
    Path expectedDestPath = new Path(destDir, child);
    assertPathDoesNotExist("dest file was found before upload",
        expectedDestPath);

    createFile(fs, pendingChildPath, true, DATASET);
    commit("child.txt", pendingChildPath, expectedDestPath, 0, 0);
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
    fs.mkdirs(magicDir);

    // use the builder API to verify it works exactly the
    // same.
    try (FSDataOutputStream stream = fs.createFile(magicDest)
        .overwrite(true)
        .recursive()
        .build()) {
      assertIsMagicStream(stream);
      stream.write(DATASET);
    }
    Path magic2 = new Path(magicDir, "magic2");
    // rename the marker
    fs.rename(magicDest, magic2);

    // the renamed file has no header
    Assertions.assertThat(extractMagicFileLength(fs, magic2))
        .describedAs("XAttribute " + XA_MAGIC_MARKER + " of " + magic2)
        .isEmpty();
    // abort the upload, which is driven by the .pending files
    // there must be 1 deleted file; during test debugging with aborted
    // runs there may be more.
    Assertions.assertThat(newCommitOperations()
        .abortPendingUploadsUnderPath(destDir))
        .describedAs("Aborting all pending uploads under %s", destDir)
        .isGreaterThanOrEqualTo(1);
  }

  /**
   * Assert that an output stream is magic.
   * @param stream stream to probe.
   */
  protected void assertIsMagicStream(final FSDataOutputStream stream) {
    Assertions.assertThat(stream.hasCapability(STREAM_CAPABILITY_MAGIC_OUTPUT))
        .describedAs("Stream capability %s in stream %s",
            STREAM_CAPABILITY_MAGIC_OUTPUT, stream)
        .isTrue();
  }

  /**
   * Create a file through the magic commit mechanism.
   * @param filename file to create (with __magic path.)
   * @param data data to write
   * @throws Exception failure
   */
  private void createCommitAndVerify(String filename, byte[] data)
      throws Exception {
    S3AFileSystem fs = getFileSystem();
    Path destFile = methodPath(filename);
    fs.delete(destFile.getParent(), true);
    Path magicDest = makeMagic(destFile);
    assertPathDoesNotExist("Magic file should not exist", magicDest);
    long dataSize = data != null ? data.length : 0;
    try(FSDataOutputStream stream = fs.create(magicDest, true)) {
      assertIsMagicStream(stream);
      if (dataSize > 0) {
        stream.write(data);
      }
      stream.close();
    }
    FileStatus status = getFileStatusEventually(fs, magicDest,
        CONSISTENCY_WAIT);
    assertEquals("Magic marker file is not zero bytes: " + status,
        0, 0);
    Assertions.assertThat(extractMagicFileLength(fs,
        magicDest))
        .describedAs("XAttribute " + XA_MAGIC_MARKER + " of " + magicDest)
        .isNotEmpty()
        .hasValue(dataSize);
    commit(filename, destFile, HIGH_THROTTLE, 0);
    verifyFileContents(fs, destFile, data);
    // the destination file doesn't have the attribute
    Assertions.assertThat(extractMagicFileLength(fs,
        destFile))
        .describedAs("XAttribute " + XA_MAGIC_MARKER + " of " + destFile)
        .isEmpty();
  }

  /**
   * Commit the file, with before and after checks on the dest and magic
   * values.
   * Failures can be set; they'll be reset after the commit.
   * @param filename filename of file
   * @param destFile destination path of file
   * @param throttle probability of commit throttling
   * @param failures failure limit
   * @throws Exception any failure of the operation
   */
  private void commit(String filename,
      Path destFile,
      float throttle,
      int failures) throws Exception {
    commit(filename, makeMagic(destFile), destFile, throttle, failures);
  }

  /**
   * Commit to a write to {@code magicFile} which is expected to
   * be saved to {@code destFile}.
   * Failures can be set; they'll be reset after the commit.
   * @param magicFile path to write to
   * @param destFile destination to verify
   * @param throttle probability of commit throttling
   * @param failures failure limit
   */
  private void commit(String filename,
      Path magicFile,
      Path destFile,
      float throttle, int failures)
      throws IOException {
    resetFailures();
    validateIntermediateAndFinalPaths(magicFile, destFile);
    SinglePendingCommit commit = SinglePendingCommit.load(getFileSystem(),
        validatePendingCommitData(filename, magicFile));
    setThrottling(throttle, failures);
    commitOrFail(destFile, commit, newCommitOperations());
    resetFailures();
    verifyCommitExists(commit);
  }

  private void commitOrFail(final Path destFile,
      final SinglePendingCommit commit, final CommitOperations actions)
      throws IOException {
    try (CommitOperations.CommitContext commitContext
             = actions.initiateCommitOperation(destFile)) {
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
   * Verify that the path at the end of a commit exists.
   * This does not validate the size.
   * @param commit commit to verify
   * @throws FileNotFoundException dest doesn't exist
   * @throws ValidationFailure commit arg is invalid
   * @throws IOException invalid commit, IO failure
   */
  private void verifyCommitExists(SinglePendingCommit commit)
      throws FileNotFoundException, ValidationFailure, IOException {
    commit.validate();
    // this will force an existence check
    Path path = getFileSystem().keyToQualifiedPath(commit.getDestinationKey());
    FileStatus status = getFileSystem().getFileStatus(path);
    LOG.debug("Destination entry: {}", status);
    if (!status.isFile()) {
      throw new PathCommitException(path, "Not a file: " + status);
    }
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
    assertTrue("created timestamp wrong in " + persisted,
        persisted.getCreated() > 0);
    assertTrue("saved timestamp wrong in " + persisted,
        persisted.getSaved() > 0);
    List<String> etags = persisted.getEtags();
    assertEquals("etag list " + persisted, 1, etags.size());
    List<PartETag> partList = CommitOperations.toPartEtags(etags);
    assertEquals("part list " + persisted, 1, partList.size());
    return pendingDataPath;
  }

  /**
   * Get a method-relative path.
   * @param filename filename
   * @return new path
   * @throws IOException failure to create/parse the path.
   */
  private Path methodPath(String filename) throws IOException {
    return new Path(methodPath(), filename);
  }

  /**
   * Get a unique path for a method.
   * @return a path
   * @throws IOException
   */
  protected Path methodPath() throws IOException {
    return path(getMethodName());
  }

  @Test
  public void testUploadEmptyFile() throws Throwable {
    File tempFile = File.createTempFile("commit", ".txt");
    CommitOperations actions = newCommitOperations();
    Path dest = methodPath("testUploadEmptyFile");
    S3AFileSystem fs = getFileSystem();
    fs.delete(dest, false);
    fullThrottle();

    SinglePendingCommit pendingCommit =
        actions.uploadFileToPendingCommit(tempFile,
            dest,
            null,
            DEFAULT_MULTIPART_SIZE,
            progress);
    resetFailures();
    assertPathDoesNotExist("pending commit", dest);
    fullThrottle();
    commitOrFail(dest, pendingCommit, actions);
    resetFailures();
    FileStatus status = verifyPathExists(fs,
        "uploaded file commit", dest);
    progress.assertCount("Progress counter should be 1.",
        1);
    assertEquals("File length in " + status, 0, status.getLen());
  }

  @Test
  public void testUploadSmallFile() throws Throwable {
    File tempFile = File.createTempFile("commit", ".txt");
    String text = "hello, world";
    FileUtils.write(tempFile, text, "UTF-8");
    CommitOperations actions = newCommitOperations();
    Path dest = methodPath("testUploadSmallFile");
    S3AFileSystem fs = getFileSystem();
    fs.delete(dest, true);
    fullThrottle();
    assertPathDoesNotExist("test setup", dest);
    SinglePendingCommit pendingCommit =
        actions.uploadFileToPendingCommit(tempFile,
            dest,
            null,
            DEFAULT_MULTIPART_SIZE,
            progress);
    resetFailures();
    assertPathDoesNotExist("pending commit", dest);
    fullThrottle();
    LOG.debug("Postcommit validation");
    commitOrFail(dest, pendingCommit, actions);
    resetFailures();
    String s = readUTF8(fs, dest, -1);
    assertEquals(text, s);
    progress.assertCount("Progress counter should be 1.",
        1);
  }

  @Test(expected = FileNotFoundException.class)
  public void testUploadMissingFile() throws Throwable {
    File tempFile = File.createTempFile("commit", ".txt");
    tempFile.delete();
    CommitOperations actions = newCommitOperations();
    Path dest = methodPath("testUploadMissingile");
    fullThrottle();
    actions.uploadFileToPendingCommit(tempFile, dest, null,
        DEFAULT_MULTIPART_SIZE, progress);
    progress.assertCount("Progress counter should be 1.",
        1);
  }

  @Test
  public void testRevertCommit() throws Throwable {
    Path destFile = methodPath("part-0000");
    S3AFileSystem fs = getFileSystem();
    touch(fs, destFile);
    CommitOperations actions = newCommitOperations();
    SinglePendingCommit commit = new SinglePendingCommit();
    commit.setDestinationKey(fs.pathToKey(destFile));
    fullThrottle();
    actions.revertCommit(commit, null);
    resetFailures();
    assertPathExists("parent of reverted commit", destFile.getParent());
  }

  @Test
  public void testRevertMissingCommit() throws Throwable {
    Path destFile = methodPath("part-0000");
    S3AFileSystem fs = getFileSystem();
    fs.delete(destFile, false);
    CommitOperations actions = newCommitOperations();
    SinglePendingCommit commit = new SinglePendingCommit();
    commit.setDestinationKey(fs.pathToKey(destFile));
    fullThrottle();
    actions.revertCommit(commit, null);
    resetFailures();
    assertPathExists("parent of reverted (nonexistent) commit",
        destFile.getParent());
  }

  @Test
  public void testFailuresInAbortListing() throws Throwable {
    CommitOperations actions = newCommitOperations();
    Path path = path("testFailuresInAbort");
    getFileSystem().mkdirs(path);
    setThrottling(HIGH_THROTTLE);
    LOG.info("Aborting");
    actions.abortPendingUploadsUnderPath(path);
    LOG.info("Abort completed");
    resetFailures();
  }


  /**
   * Test a normal stream still works as expected in a magic filesystem,
   * with a call of {@code hasCapability()} to check that it is normal.
   * @throws Throwable failure
   */
  @Test
  public void testWriteNormalStream() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    assumeMagicCommitEnabled(fs);
    Path destFile = path("normal");
    try (FSDataOutputStream out = fs.create(destFile, true)) {
      out.writeChars("data");
      assertFalse("stream has magic output: " + out,
          out.hasCapability(STREAM_CAPABILITY_MAGIC_OUTPUT));
      out.close();
    }
    FileStatus status = getFileStatusEventually(fs, destFile,
        CONSISTENCY_WAIT);
    assertTrue("Empty marker file: " + status, status.getLen() > 0);
  }

  /**
   * Creates a bulk commit and commits multiple files.
   * If the DDB metastore is in use, use the instrumentation to
   * verify that the write count is as expected.
   * This is done without actually looking into the store -just monitoring
   * changes in the filesystem's instrumentation counters.
   * As changes to the store may be made during get/list calls,
   * when the counters must be reset before each commit, this must be
   * *after* all probes for the outcome of the previous operation.
   */
  @Test
  public void testBulkCommitFiles() throws Throwable {
    describe("verify bulk commit including metastore update count");
    File localFile = File.createTempFile("commit", ".txt");
    CommitOperations actions = newCommitOperations();
    Path destDir = methodPath("out");
    S3AFileSystem fs = getFileSystem();
    fs.delete(destDir, false);
    fullThrottle();

    Path destFile1 = new Path(destDir, "file1");
    // this subdir will only be created in the commit of file 2
    Path subdir = new Path(destDir, "subdir");
    // file 2
    Path destFile2 = new Path(subdir, "file2");
    Path destFile3 = new Path(subdir, "file3 with space");
    List<Path> destinations = Lists.newArrayList(destFile1, destFile2,
        destFile3);
    List<SinglePendingCommit> commits = new ArrayList<>(3);

    for (Path destination : destinations) {
      SinglePendingCommit commit1 =
          actions.uploadFileToPendingCommit(localFile,
              destination, null,
              DEFAULT_MULTIPART_SIZE,
              progress);
      commits.add(commit1);
    }
    resetFailures();
    assertPathDoesNotExist("destination dir", destDir);
    assertPathDoesNotExist("subdirectory", subdir);
    LOG.info("Initiating commit operations");
    try (CommitOperations.CommitContext commitContext
             = actions.initiateCommitOperation(destDir)) {
      // how many records have been written
      MetricDiff writes = new MetricDiff(fs,
          Statistic.S3GUARD_METADATASTORE_RECORD_WRITES);
      LOG.info("Commit #1");
      commitContext.commitOrFail(commits.get(0));
      final String firstCommitContextString = commitContext.toString();
      LOG.info("First Commit state {}", firstCommitContextString);
      long writesOnFirstCommit = writes.diff();
      assertPathExists("destFile1", destFile1);
      assertPathExists("destination dir", destDir);

      LOG.info("Commit #2");
      writes.reset();
      commitContext.commitOrFail(commits.get(1));
      assertPathExists("subdirectory", subdir);
      assertPathExists("destFile2", destFile2);
      final String secondCommitContextString = commitContext.toString();
      LOG.info("Second Commit state {}", secondCommitContextString);

      if (writesOnFirstCommit != 0) {
        LOG.info("DynamoDB Metastore is in use: checking write count");
        // S3Guard is in use against DDB, so the metrics can be checked
        // to see how many records were updated.
        // there should only be two new entries: one for the file and
        // one for the parent.
        // we include the string values of the contexts because that includes
        // the internals of the bulk operation state.
        writes.assertDiffEquals("Number of records written after commit #2"
                + "; first commit had " + writesOnFirstCommit
                + "; first commit ancestors " + firstCommitContextString
                + "; second commit ancestors: " + secondCommitContextString,
            2);
      }

      LOG.info("Commit #3");
      writes.reset();
      commitContext.commitOrFail(commits.get(2));
      assertPathExists("destFile3", destFile3);
      if (writesOnFirstCommit != 0) {
        // this file is in the same dir as destFile2, so only its entry
        // is added
        writes.assertDiffEquals(
            "Number of records written after third commit; "
                + "first commit had " + writesOnFirstCommit,
            1);
      }
    }
    resetFailures();
  }

}
