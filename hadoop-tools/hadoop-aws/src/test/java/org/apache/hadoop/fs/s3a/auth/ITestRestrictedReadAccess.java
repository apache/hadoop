/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.auth;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.util.concurrent.Callable;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.mapred.LocatedFileStatusFetcher;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_ARN;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.lsR;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.Effects;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.Statement;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.directory;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.statement;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.*;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.bindRolePolicyStatements;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.newAssumedRoleConfig;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.LIST_STATUS_NUM_THREADS;
import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.apache.hadoop.test.GenericTestUtils.failif;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * This test creates a client with no read access to the underlying
 * filesystem and then tries to perform various read operations on it.
 *
 * The tests are all bundled into one big test case.
 * From a purist unit test perspective, this is utterly wrong as it goes
 * against the
 * <i>"Each test case tests exactly one thing"</i>
 * philosophy of JUnit.
 * <p>
 * However is significantly reduces setup costs .
 * as it means that the filesystems and directories only need to be
 * created and destroyed once per suite, rather than
 * once per individual test.
 * <p>
 * All the test probes have informative messages so when a test failure
 * does occur, its cause should be discoverable. It main weaknesses are
 * therefore:
 * <ol>
 *   <li>A failure of an assertion blocks all subsequent assertions from
 *   being checked.</li>
 *   <li>Maintenance is potentially harder.</li>
 * </ol>
 * To simplify maintenance, the operations tested are broken up into
 * their own methods, with fields used to share the restricted role and
 * created paths.
 *
 */
public class ITestRestrictedReadAccess extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestRestrictedReadAccess.class);

  /** Filter to select everything. */
  private static final PathFilter EVERYTHING = t -> true;

  /** Filter to select .txt files. */
  private static final PathFilter TEXT_FILE =
      path -> path.toUri().toString().endsWith(".txt");

  /** The same path filter used in FileInputFormat. */
  private static final PathFilter HIDDEN_FILE_FILTER =
      (p) -> {
        String n = p.getName();
        return !n.startsWith("_") && !n.startsWith(".");
      };

  /**
   * Text found in LocatedFileStatusFetcher exception when the glob
   * returned "null".
   */
  private static final String DOES_NOT_EXIST = "does not exist";

  /**
   * Text found in LocatedFileStatusFetcher exception when
   * the glob returned an empty list.
   */
  private static final String MATCHES_0_FILES = "matches 0 files";

  /**
   * Text used in files.
   */
  public static final byte[] HELLO = "hello".getBytes(StandardCharsets.UTF_8);

  /**
   * Wildcard scan to find *.txt in the no-read directory.
   */
  private Path noReadWildcard;

  private Path basePath;

  private Path noReadDir;

  private Path emptyDir;

  private Path emptyFile;

  private Path subDir;

  private Path subdirFile;

  private Path subDir2;

  private Path subdir2File1;

  private Path subdir2File2;

  private Configuration roleConfig;

  /**
   * A read-only FS; if non-null it is closed in teardown.
   */
  private S3AFileSystem readonlyFS;

  @Override
  public void setup() throws Exception {
    super.setup();
    assumeRoleTests();
  }

  @Override
  public void teardown() throws Exception {
    try {
      super.teardown();
    } finally {
      cleanupWithLogger(LOG, readonlyFS);
    }
  }

  private void assumeRoleTests() {
    assume("No ARN for role tests", !getAssumedRoleARN().isEmpty());
  }

  private String getAssumedRoleARN() {
    return getContract().getConf().getTrimmed(ASSUMED_ROLE_ARN, "");
  }

  /**
   * Create the assumed role configuration.
   * @return a config bonded to the ARN of the assumed role
   */
  public Configuration createAssumedRoleConfig() {
    return createAssumedRoleConfig(getAssumedRoleARN());
  }

  /**
   * Create a config for an assumed role; it also disables FS caching.
   * @param roleARN ARN of role
   * @return the new configuration
   */
  private Configuration createAssumedRoleConfig(String roleARN) {
    return newAssumedRoleConfig(getContract().getConf(), roleARN);
  }

  /**
   * This is a single test case which invokes the individual test cases
   * in sequence.
   */
  @Test
  public void testNoReadAccess() throws Throwable {
    describe("Test failure handling if the client doesn't"
        + " have read access under a path");
    initNoReadAccess();

    // now move up the API Chain, from the calls made by globStatus,
    // to globStatus itself, and then to LocatedFileStatusFetcher,
    // which invokes globStatus

    checkBasicFileOperations();
    checkGlobOperations();
    checkSingleThreadedLocatedFileStatus();
    checkLocatedFileStatusFourThreads();
    checkLocatedFileStatusScanFile();
    checkLocatedFileStatusNonexistentPath();
    checkDeleteOperations();
  }

  /**
   * Initialize the directory tree and the role filesystem.
   */
  public void initNoReadAccess() throws Throwable {
    describe("Setting up filesystem");

    S3AFileSystem realFS = getFileSystem();

    basePath = methodPath();

    // define the paths and create them.
    describe("Creating test directories and files");

    // this is the directory to which the restricted role has no read
    // access.
    noReadDir = new Path(basePath, "noReadDir");
    // wildcard scan to find *.txt
    noReadWildcard = new Path(noReadDir, "*/*.txt");

    // an empty directory directory under the noReadDir
    emptyDir = new Path(noReadDir, "emptyDir");
    realFS.mkdirs(emptyDir);

    // an empty file directory under the noReadDir
    emptyFile = new Path(noReadDir, "emptyFile.txt");
    touch(realFS, emptyFile);

    // a subdirectory
    subDir = new Path(noReadDir, "subDir");

    // and a file in that subdirectory
    subdirFile = new Path(subDir, "subdirFile.txt");
    createFile(realFS, subdirFile, true, HELLO);
    subDir2 = new Path(noReadDir, "subDir2");
    subdir2File1 = new Path(subDir2, "subdir2File1.txt");
    subdir2File2 = new Path(subDir2, "subdir2File2.docx");
    createFile(realFS, subdir2File1, true, HELLO);
    createFile(realFS, subdir2File2, true, HELLO);

    // create a role filesystem which does not have read access under a path
    // it still has write access, which can be explored in the final
    // step to delete files and directories.
    roleConfig = createAssumedRoleConfig();
    bindRolePolicyStatements(roleConfig, STATEMENT_ALLOW_KMS_RW,
        statement(true, S3_ALL_BUCKETS, S3_ALL_OPERATIONS),
        new Statement(Effects.Deny)
            .addActions(S3_ALL_GET)
            .addResources(directory(noReadDir)));
    readonlyFS = (S3AFileSystem) basePath.getFileSystem(roleConfig);

  }

  /**
   * Validate basic IO operations.
   */
  public void checkBasicFileOperations() throws Throwable {

    // this is a LIST call; there's no marker.
    // so the sequence is
    //   - HEAD path -> FNFE
    //   - HEAD path + / -> FNFE
    //   - LIST path -> list results
    // Because the client has list access, this succeeds
    readonlyFS.listStatus(basePath);
    lsR(readonlyFS, basePath, true);


    // this is HEAD + "/" on S3
    readonlyFS.listStatus(emptyDir);

    // a recursive list of the no-read-directory works because
    // there is no directory marker, it becomes a LIST call.
    lsR(readonlyFS, noReadDir, true);

    // similarly, a getFileStatus ends up being a list of the path
    // and so working.
    readonlyFS.getFileStatus(noReadDir);

    readonlyFS.getFileStatus(emptyDir);
    // now look at a file
    accessDenied(() ->
        readonlyFS.getFileStatus(subdirFile));

    // the attempt to read the data will also fail.
    accessDenied(() ->
        ContractTestUtils.readUTF8(readonlyFS, subdirFile, HELLO.length));

    accessDenied(() -> readonlyFS.open(emptyFile));

  }

  /**
   * Explore Glob's recursive scan.
   */
  public void checkGlobOperations() throws Throwable {

    describe("Glob Status operations");
    // baseline: the real filesystem on a subdir
    globFS(getFileSystem(), subdirFile, null, false, 1);
    // a file fails
    globFS(readonlyFS, subdirFile, null, true, 1);
    // empty directories don't fail.
    FileStatus[] st = globFS(readonlyFS, emptyDir, null, false, 1);

    st = globFS(readonlyFS,
        noReadWildcard,
        null, false, 2);

    // there is precisely one .docx file (subdir2File2.docx)
    globFS(readonlyFS,
        new Path(noReadDir, "*/*.docx"),
        null, false, 1);

    // there are no .doc files.
    globFS(readonlyFS,
        new Path(noReadDir, "*/*.doc"),
        null, false, 0);
    globFS(readonlyFS, noReadDir,
        EVERYTHING, false, 1);
    // and a filter without any wildcarded pattern only finds
    // the role dir itself.
    FileStatus[] st2 = globFS(readonlyFS, noReadDir,
        EVERYTHING, false, 1);
    Assertions.assertThat(st2)
        .extracting(FileStatus::getPath)
        .containsExactly(noReadDir);
  }

  /**
   * Run a located file status fetcher against the directory tree.
   */
  public void checkSingleThreadedLocatedFileStatus() throws Throwable {

    describe("LocatedFileStatusFetcher operations");
    // use the same filter as FileInputFormat; single thread.
    roleConfig.setInt(LIST_STATUS_NUM_THREADS, 1);
    LocatedFileStatusFetcher fetcher =
        new LocatedFileStatusFetcher(
            roleConfig,
            new Path[]{basePath},
            true,
            HIDDEN_FILE_FILTER,
            true);
    Assertions.assertThat(fetcher.getFileStatuses())
        .describedAs("result of located scan")
        .flatExtracting(FileStatus::getPath)
        .containsExactlyInAnyOrder(
            emptyFile,
            subdirFile,
            subdir2File1,
            subdir2File2);
  }

  /**
   * Run a located file status fetcher against the directory tree.
   */
  public void checkLocatedFileStatusFourThreads() throws Throwable {

    // four threads and the text filter.
    int threads = 4;
    describe("LocatedFileStatusFetcher with %d", threads);
    roleConfig.setInt(LIST_STATUS_NUM_THREADS, threads);
    LocatedFileStatusFetcher fetcher =
        new LocatedFileStatusFetcher(
            roleConfig,
            new Path[]{noReadWildcard},
            true,
            EVERYTHING,
            true);
    Assertions.assertThat(fetcher.getFileStatuses())
        .describedAs("result of located scan")
        .isNotNull()
        .flatExtracting(FileStatus::getPath)
        .containsExactlyInAnyOrder(subdirFile, subdir2File1);
  }

  /**
   * Run a located file status fetcher against the directory tree.
   */
  public void checkLocatedFileStatusScanFile() throws Throwable {
    // pass in a file as the base of the scan.
    describe("LocatedFileStatusFetcher with file %s", subdirFile);
    roleConfig.setInt(LIST_STATUS_NUM_THREADS, 16);
    LocatedFileStatusFetcher fetcher
        = new LocatedFileStatusFetcher(
        roleConfig,
        new Path[]{subdirFile},
        true,
        TEXT_FILE,
        true);
    accessDenied(() -> fetcher.getFileStatuses());

  }

  /**
   * Explore what happens with a path that does not exist.
   */
  public void checkLocatedFileStatusNonexistentPath() throws Throwable {
    // scan a path that doesn't exist
    Path nonexistent = new Path(noReadDir, "nonexistent");
    InvalidInputException ex = intercept(InvalidInputException.class,
        DOES_NOT_EXIST,
        () -> new LocatedFileStatusFetcher(
            roleConfig,
            new Path[]{nonexistent},
            true,
            EVERYTHING,
            true)
            .getFileStatuses());
    // validate nested exception
    assertExceptionContains(DOES_NOT_EXIST, ex.getCause());

    // a file which exists but which doesn't match the pattern
    // is downgraded to not existing.
    intercept(InvalidInputException.class,
        DOES_NOT_EXIST,
        () -> new LocatedFileStatusFetcher(
            roleConfig,
            new Path[]{noReadDir},
            true,
            TEXT_FILE,
            true)
            .getFileStatuses());

    // a pattern under a nonexistent path is considered to not be a match.
    ex = intercept(
        InvalidInputException.class,
        MATCHES_0_FILES,
        () -> new LocatedFileStatusFetcher(
            roleConfig,
            new Path[]{new Path(nonexistent, "*.txt)")},
            true,
            TEXT_FILE,
            true)
            .getFileStatuses());
    // validate nested exception
    assertExceptionContains(MATCHES_0_FILES, ex.getCause());
  }

  /**
   * Do some cleanup to see what happens with delete calls.
   * Cleanup happens in test teardown anyway; doing it here
   * just makes use of the delete calls to see how delete failures
   * change with permissions.
   */
  public void checkDeleteOperations() throws Throwable {
    describe("Testing delete operations");
    readonlyFS.delete(emptyDir, true);
    // to fail on HEAD
    accessDenied(() -> readonlyFS.delete(emptyFile, true));

    // this will succeed for both
    readonlyFS.delete(subDir, true);
    // after which  it is not there
    fileNotFound(() -> readonlyFS.getFileStatus(subDir));
    // and nor is its child.
    fileNotFound(() -> readonlyFS.getFileStatus(subdirFile));

    // now delete the base path
    readonlyFS.delete(basePath, true);
    // and expect an FNFE
    fileNotFound(() -> readonlyFS.getFileStatus(subDir));
  }

  /**
   * Require an operation to fail with a FileNotFoundException.
   * @param eval closure to evaluate.
   * @param <T> type of callable
   * @return the exception.
   * @throws Exception any other exception
   */
  protected <T> FileNotFoundException fileNotFound(final Callable<T> eval)
      throws Exception {
    return intercept(FileNotFoundException.class, eval);
  }

  /**
   * Require an operation to fail with an AccessDeniedException.
   * @param eval closure to evaluate.
   * @param <T> type of callable
   * @return the exception.
   * @throws Exception any other exception
   */
  protected <T> AccessDeniedException accessDenied(final Callable<T> eval)
      throws Exception {
    return intercept(AccessDeniedException.class, eval);
  }

  /**
   * Assert that a status array has exactly one element and its
   * value is as expected.
   * @param expected expected path
   * @param statuses list of statuses
   */
  protected void assertStatusPathEquals(final Path expected,
      final FileStatus[] statuses) {
    Assertions.assertThat(statuses)
        .describedAs("List of status entries")
        .isNotNull()
        .hasSize(1);
    Assertions.assertThat(statuses[0].getPath())
        .describedAs("Status entry %s", statuses[0])
        .isEqualTo(expected);
  }

  /**
   * Glob under a path with expected outcomes.
   * @param fs filesystem to use
   * @param path path (which can include patterns)
   * @param filter optional filter
   * @param expectAuthFailure is auth failure expected?
   * @param expectedCount expected count of results; -1 means null response
   * @return the result of a successful glob or null if an expected auth
   *          failure was caught.
   * @throws IOException failure.
   */
  protected FileStatus[] globFS(
      final S3AFileSystem fs,
      final Path path,
      final PathFilter filter,
      boolean expectAuthFailure,
      final int expectedCount)
      throws IOException {
    LOG.info("Glob {}", path);
    S3ATestUtils.MetricDiff getMetric = new S3ATestUtils.MetricDiff(fs,
        Statistic.OBJECT_METADATA_REQUESTS);
    S3ATestUtils.MetricDiff listMetric = new S3ATestUtils.MetricDiff(fs,
        Statistic.OBJECT_LIST_REQUEST);
    FileStatus[] st;
    try {
      st = filter == null
          ? fs.globStatus(path)
          : fs.globStatus(path, filter);
      LOG.info("Metrics:\n {},\n {}", getMetric, listMetric);
      if (expectAuthFailure) {
        // should have failed here
        String resultStr;
        if (st == null) {
          resultStr = "A null array";
        } else {
          resultStr = StringUtils.join(st, ",");
        }
        fail(String.format("globStatus(%s) should have raised"
            + " an exception, but returned %s", path, resultStr));
      }
    } catch (AccessDeniedException e) {
      LOG.info("Metrics:\n {},\n {}", getMetric, listMetric);
      failif(!expectAuthFailure, "Access denied in glob of " + path,
          e);
      return null;
    } catch (IOException | RuntimeException e) {
      throw new AssertionError("Other exception raised in glob:" + e, e);
    }
    if (expectedCount < 0) {
      Assertions.assertThat(st)
          .describedAs("Glob of %s", path)
          .isNull();
    } else {
      Assertions.assertThat(st)
          .describedAs("Glob of %s", path)
          .isNotNull()
          .hasSize(expectedCount);
    }
    return st;
  }

}

