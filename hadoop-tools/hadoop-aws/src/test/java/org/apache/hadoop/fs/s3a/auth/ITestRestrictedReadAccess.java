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
import java.nio.charset.Charset;
import java.nio.file.AccessDeniedException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.s3guard.LocalMetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore;
import org.apache.hadoop.mapred.LocatedFileStatusFetcher;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_ARN;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.lsR;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBucketOverrides;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.Effects;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.Statement;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.directory;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.statement;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.*;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.bindRolePolicyStatements;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.newAssumedRoleConfig;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.LIST_STATUS_NUM_THREADS;
import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.apache.hadoop.test.GenericTestUtils.failif;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * This test creates a client with no read access to the underlying
 * filesystem and then tries to perform various read operations on it.
 * S3Guard in non-auth mode always goes to the FS, so we parameterize the
 * test for S3Guard + Auth to see how failures move around.
 * <ol>
 *   <li>Tests only run if an assumed role is provided.</li>
 *   <li>And the s3guard tests use the local metastore if
 *   there was not one already.</li>
 * </ol>
 * The tests are all bundled into one big test case.
 * From a purist unit test perspective, this is utterly wrong as it goes
 * against the
 * <i>"Each test case tests exactly one thing"</i>
 * philosophy of JUnit.
 * <p>
 * However is significantly reduces setup costs on the parameterized test runs,
 * as it means that the filesystems and directories only need to be
 * created and destroyed once per parameterized suite, rather than
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
 */
@SuppressWarnings("ThrowableNotThrown")
@RunWith(Parameterized.class)
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
  public static final byte[] HELLO = "hello".getBytes(Charset.forName("UTF-8"));

  /**
   * Wildcard scan to find *.txt in the no-read directory.
   * When a scan/glob is done with S3Guard in auth mode, the scan will
   * succeed but the file open will fail for any non-empty file.
   * In non-auth mode, the read restrictions will fail the actual scan.
   */
  private Path noReadWildcard;

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"raw", false, false},
        {"nonauth", true, false},
        {"auth", true, true}
    });
  }

  private final String name;

  private final boolean s3guard;

  private final boolean authMode;

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

  /**
   * Test suite setup.
   * @param name name for logs/paths.
   * @param s3guard is S3Guard enabled?
   * @param authMode is S3Guard in auth mode?
   */
  public ITestRestrictedReadAccess(
      final String name,
      final boolean s3guard,
      final boolean authMode) {
    this.name = name;
    this.s3guard = s3guard;
    this.authMode = authMode;
  }

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    String bucketName = getTestBucketName(conf);
    removeBucketOverrides(bucketName, conf,
        S3_METADATA_STORE_IMPL,
        METADATASTORE_AUTHORITATIVE);
    conf.setClass(Constants.S3_METADATA_STORE_IMPL,
        s3guard ?
            LocalMetadataStore.class
            : NullMetadataStore.class,
        MetadataStore.class);
    conf.setBoolean(METADATASTORE_AUTHORITATIVE, authMode);
    disableFilesystemCaching(conf);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    assumeRoleTests();
  }

  @Override
  public void teardown() throws Exception {
    S3AUtils.closeAll(LOG, readonlyFS);
    super.teardown();
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

    // avoiding the parameterization to steer clear of accidentally creating
    // patterns
    basePath = path("testNoReadAccess-" + name);

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
    bindRolePolicyStatements(roleConfig,
        STATEMENT_S3GUARD_CLIENT,
        STATEMENT_ALLOW_SSE_KMS_RW,
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

    // this is HEAD + "/" on S3; get on S3Guard auth
    readonlyFS.listStatus(emptyDir);

    // a recursive list of the no-read-directory works because
    // there is no directory marker, it becomes a LIST call.
    lsR(readonlyFS, noReadDir, true);

    // similarly, a getFileStatus ends up being a list and generating
    // a file status marker.
    readonlyFS.getFileStatus(noReadDir);

    // empty dir checks work!
    readonlyFS.getFileStatus(emptyDir);

    // now look at a file; the outcome depends on the mode.
    if (authMode) {
      // auth mode doesn't check S3, so no failure
      readonlyFS.getFileStatus(subdirFile);
    } else {
      accessDenied(() ->
          readonlyFS.getFileStatus(subdirFile));
    }

    // irrespective of mode, the attempt to read the data will fail.
    // the only variable is where the failure occurs
    accessDenied(() ->
        ContractTestUtils.readUTF8(readonlyFS, subdirFile, HELLO.length));

    // the empty file is interesting
    if (!authMode) {
      // non-auth mode, it fails at some point in the opening process.
      // due to a HEAD being called on the object
      accessDenied(() ->
          ContractTestUtils.readUTF8(readonlyFS, emptyFile, 0));
    } else {
      // auth mode doesn't check the store.
      // Furthermore, because it knows the file length is zero,
      // it returns -1 without even opening the file.
      // This means that permissions on the file do not get checked.
      // See: HADOOP-16464.
      try (FSDataInputStream is = readonlyFS.open(emptyFile)) {
        Assertions.assertThat(is.read())
            .describedAs("read of empty file")
            .isEqualTo(-1);
      }
      readonlyFS.getFileStatus(subdirFile);
    }
  }

  /**
   * Explore Glob's recursive scan.
   */
  public void checkGlobOperations() throws Throwable {

    describe("Glob Status operations");
    // baseline: the real filesystem on a subdir
    globFS(getFileSystem(), subdirFile, null, false, 1);
    // a file fails if not in auth mode
    globFS(readonlyFS, subdirFile, null, !authMode, 1);
    // empty directories don't fail.
    assertStatusPathEquals(emptyDir,
        globFS(readonlyFS, emptyDir, null, false, 1));

    FileStatus[] st = globFS(readonlyFS,
        noReadWildcard,
        null, false, 2);
    Assertions.assertThat(st)
        .extracting(FileStatus::getPath)
        .containsExactlyInAnyOrder(subdirFile, subdir2File1);

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
    LocatedFileStatusFetcher fetcher2 =
        new LocatedFileStatusFetcher(
            roleConfig,
            new Path[]{noReadWildcard},
            true,
            EVERYTHING,
            true);
    Assertions.assertThat(fetcher2.getFileStatuses())
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
    try {
      Iterable<FileStatus> fetched = new LocatedFileStatusFetcher(
          roleConfig,
          new Path[]{subdirFile},
          true,
          TEXT_FILE,
          true).getFileStatuses();
      // when not in auth mode, the HEAD request MUST have failed.
      failif(!authMode, "LocatedFileStatusFetcher(" + subdirFile + ")"
          + " should have failed");
      // and in auth mode, the file MUST have been found.
      Assertions.assertThat(fetched)
          .describedAs("result of located scan")
          .isNotNull()
          .flatExtracting(FileStatus::getPath)
          .containsExactly(subdirFile);
    } catch (AccessDeniedException e) {
      // we require the HEAD request to fail with access denied in non-auth
      // mode, but not in auth mode.
      failif(authMode, "LocatedFileStatusFetcher(" + subdirFile + ")", e);
    }
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
   * change with permissions and S3Guard stettings.
   */
  public void checkDeleteOperations() throws Throwable {
    describe("Testing delete operations");

    if (!authMode) {
      // unguarded or non-auth S3Guard to fail on HEAD + /
      accessDenied(() -> readonlyFS.delete(emptyDir, true));
      // to fail on HEAD
      accessDenied(() -> readonlyFS.delete(emptyFile, true));
    } else {
      // auth mode checks DDB for status and then issues the DELETE
      readonlyFS.delete(emptyDir, true);
      readonlyFS.delete(emptyFile, true);
    }

    // this will succeed for both as there is no subdir marker.
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
        Statistic.OBJECT_LIST_REQUESTS);
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

