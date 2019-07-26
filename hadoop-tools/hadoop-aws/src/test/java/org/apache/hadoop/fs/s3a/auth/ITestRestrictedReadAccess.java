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

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.AccessDeniedException;
import java.util.Arrays;
import java.util.Collection;

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
import static org.apache.hadoop.test.GenericTestUtils.failif;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * This test creates a client with no read access to the underlying
 * filesystem and then tries to perform various read operations on it.
 * S3Guard in auth mode always goes to the FS, so we parameterize the
 * test for S3Guard + Auth to see how failures move around.
 * <ol>
 *   <li>Tests only run if an assumed role is provided.</li>
 *   <li>And the s3guard tests use the local metastore if
 *   there was not one already.</li>
 * </ol>
 * The tests are all bundled into one big file to reduce setup costs
 * on a parameterized test run.
 */
@RunWith(Parameterized.class)
public class ITestRestrictedReadAccess extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestRestrictedReadAccess.class);

  private static final PathFilter ANYTHING = t -> true;

  private static final PathFilter TEXT_FILE =
      path -> path.toUri().toString().endsWith(".txt");

  /**
   * Text found in LocatedFileStatusFetcher exception.
   */
  private static final String DOES_NOT_EXIST = "does not exist";

  /**
   * Text found in LocatedFileStatusFetcher exception.
   */
  private static final String MATCHES_0_FILES = "matches 0 files";

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

  /**
   * A role FS; if non-null it is closed in teardown.
   */
  private S3AFileSystem roleFS;

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
    S3AUtils.closeAll(LOG, roleFS);
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


  @Test
  public void testNoReadAccess() throws Throwable {
    describe("Test failure handling of if the client doesn't"
        + " have read access under a path");
    S3AFileSystem realFS = getFileSystem();

    // avoiding the parameterization to steer clear of accidentally creating
    // patterns
    Path basePath = path("testNoReadAccess-" + name);
    Path noReadDir = new Path(basePath, "noReadDir");
    Path emptyDir = new Path(noReadDir, "emptyDir");
    realFS.mkdirs(emptyDir);
    Path emptyFile = new Path(noReadDir, "emptyFile.txt");
    touch(realFS, emptyFile);
    Path subDir = new Path(noReadDir, "subDir");

    Path subdirFile = new Path(subDir, "subdirFile.txt");
    byte[] data = "hello".getBytes(Charset.forName("UTF-8"));
    createFile(realFS, subdirFile, true, data);
    Path subDir2 = new Path(noReadDir, "subDir2");
    Path subdir2File1 = new Path(subDir2, "subdir2File1.txt");
    Path subdir2File2 = new Path(subDir2, "subdir2File2.docx");
    createFile(realFS, subdir2File1, true, data);
    createFile(realFS, subdir2File2, true, data);

    // create a role filesystem which does not have read access under a path
    Configuration roleConfig = createAssumedRoleConfig();
    bindRolePolicyStatements(roleConfig,
        STATEMENT_S3GUARD_CLIENT,
        STATEMENT_ALLOW_SSE_KMS_RW,
        statement(true, S3_ALL_BUCKETS, S3_ALL_OPERATIONS),
        new Statement(Effects.Deny)
            .addActions(S3_PATH_RW_OPERATIONS)
            .addResources(directory(noReadDir)));
    roleFS = (S3AFileSystem) basePath.getFileSystem(roleConfig);

    // this is a LIST call; there's no marker.
    roleFS.listStatus(basePath);

    // this is HEAD + "/" on S3; get on S3Guard auth
    roleFS.listStatus(emptyDir);

    lsR(roleFS, noReadDir, true);

    roleFS.getFileStatus(noReadDir);
    roleFS.getFileStatus(emptyDir);
    if (authMode) {
      // auth mode doesn't check S3, so no failure
      roleFS.getFileStatus(subdirFile);
    } else {
      intercept(AccessDeniedException.class, () ->
          roleFS.getFileStatus(subdirFile));
    }
    intercept(AccessDeniedException.class, () ->
        ContractTestUtils.readUTF8(roleFS, subdirFile, data.length));
    if (authMode) {
      // auth mode doesn't just not check the store,
      // because it knows the file length is zero,
      // it returns -1 without even opening the file.
      try (FSDataInputStream is = roleFS.open(emptyFile)) {
        Assertions.assertThat(is.read())
            .describedAs("read of empty file")
            .isEqualTo(-1);
      }
      roleFS.getFileStatus(subdirFile);
    } else {
      // non-auth mode, it fails at some point in the opening process.
      intercept(AccessDeniedException.class, () ->
          ContractTestUtils.readUTF8(roleFS, emptyFile, 0));
    }

    // Fun with Glob
    describe("Glob Status operations");
    // baseline: the real filesystem on a subdir
    globFS(realFS, subdirFile, null, false, 1);
    // a file fails if not in auth mode
    globFS(roleFS, subdirFile, null, !authMode, 1);
    // empty directories don't fail.
    assertStatusPathEquals(emptyDir,
        globFS(roleFS, emptyDir, null, false, 1));

    Path textFilePathPattern = new Path(noReadDir, "*/*.txt");
    FileStatus[] st = globFS(roleFS,
        textFilePathPattern,
        null, false, 2);
    Assertions.assertThat(st)
        .extracting("getPath")
        .containsExactlyInAnyOrder(subdirFile, subdir2File1);

    globFS(roleFS,
        new Path(noReadDir, "*/*.docx"),
        null, false, 1);
    globFS(roleFS,
        new Path(noReadDir, "*/*.doc"),
        null, false, 0);
    globFS(roleFS, noReadDir,
        ANYTHING, false, 1);

    // now run a located file status fetcher against it
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

    describe("LocatedFileStatusFetcher with %s", textFilePathPattern);
    roleConfig.setInt(LIST_STATUS_NUM_THREADS, 4);
    LocatedFileStatusFetcher fetcher2 =
        new LocatedFileStatusFetcher(
            roleConfig,
            new Path[]{textFilePathPattern},
            true,
            ANYTHING,
            true);
    Assertions.assertThat(fetcher2.getFileStatuses())
        .describedAs("result of located scan")
        .isNotNull()
        .flatExtracting(FileStatus::getPath)
        .containsExactlyInAnyOrder(subdirFile, subdir2File1);

    // query a file and expect the initial scan to fail except in
    // auth mode.
    describe("LocatedFileStatusFetcher with %s", subdirFile);
    roleConfig.setInt(LIST_STATUS_NUM_THREADS, 16);
    LocatedFileStatusFetcher fetcher3 =
        new LocatedFileStatusFetcher(
            roleConfig,
            new Path[]{subdirFile},
            true,
            TEXT_FILE,
            true);
    try {
      Iterable<FileStatus> fetched = fetcher3.getFileStatuses();
      failif(!authMode, "LocatedFileStatusFetcher(" + subdirFile + ")"
          + " should have failed");
      Assertions.assertThat(fetched)
          .describedAs("result of located scan")
          .isNotNull()
          .flatExtracting(FileStatus::getPath)
          .containsExactly(subdirFile);
    } catch (AccessDeniedException e) {
      failif(authMode, "LocatedFileStatusFetcher(" + subdirFile + ")", e);
    }

    // a file that doesn't exist
    Path nonexistent = new Path(noReadDir, "nonexistent");
    intercept(InvalidInputException.class,
        DOES_NOT_EXIST,
        () -> new LocatedFileStatusFetcher(
            roleConfig,
            new Path[]{nonexistent},
            true,
            ANYTHING,
            true)
            .getFileStatuses());

    // a file which exists but which doesn't match the pattern
    intercept(InvalidInputException.class,
        DOES_NOT_EXIST,
        () -> new LocatedFileStatusFetcher(
            roleConfig,
            new Path[]{noReadDir},
            true,
            TEXT_FILE,
            true)
            .getFileStatuses());

    // a pattern under a nonexistent path.
    intercept(
        InvalidInputException.class,
        MATCHES_0_FILES,
        () -> new LocatedFileStatusFetcher(
            roleConfig,
            new Path[]{new Path(nonexistent, "*.txt)")},
            true,
            TEXT_FILE,
            true)
            .getFileStatuses());
  }

  protected void assertStatusPathEquals(final Path expected,
      final FileStatus[] statuses) {
    Assertions.assertThat(statuses)
        .hasSize(1);
    Assertions.assertThat(statuses[0].getPath())
        .isEqualTo(expected);
  }

  /**
   * Glob.
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

  private static final PathFilter HIDDEN_FILE_FILTER =
      (p) -> {
        String n = p.getName();
        return !n.startsWith("_") && !n.startsWith(".");
      };
}

