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

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"raw", false, false},
        {"nonauth", true, false},
        {"auth", true, true}
    });
  }

  private static final Path ROOT = new Path("/");

  private static final Statement STATEMENT_ALL_BUCKET_READ_ACCESS
      = statement(true, S3_ALL_BUCKETS, S3_BUCKET_READ_OPERATIONS);

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
            : NullMetadataStore.class
        , MetadataStore.class);
    conf.setBoolean(METADATASTORE_AUTHORITATIVE, authMode);
    disableFilesystemCaching(conf);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    assumeRoleTests();
  }

  public Path methodPath() throws IOException {
    return path(getMethodName());
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

    Path noReadFile = new Path(subDir, "noReadFile.txt");
    byte[] data = "hello".getBytes(Charset.forName("UTF-8"));
    createFile(realFS, noReadFile, true, data);

    // create a role filesystem which does not have read access under a path
    Configuration roleConfig = createAssumedRoleConfig();
    bindRolePolicyStatements(roleConfig,
        STATEMENT_S3GUARD_CLIENT,
        STATEMENT_ALLOW_SSE_KMS_RW,
        statement(true, S3_ALL_BUCKETS, S3_ALL_OPERATIONS),
        new Statement(Effects.Deny)
            .addActions(S3_PATH_RW_OPERATIONS)
            .addResources(directory(noReadDir))
    );
    roleFS = (S3AFileSystem) basePath.getFileSystem(roleConfig);

    // this is a LIST call; there's no marker.
    roleFS.listStatus(basePath);

    // this is HEAD + "/" on S3; get on S3Guard auth
    roleFS.listStatus(emptyDir);

    lsR(roleFS, noReadDir, true);

    roleFS.getFileStatus(noReadDir);
    roleFS.getFileStatus(emptyDir);
    if (authMode) {
      roleFS.getFileStatus(noReadFile);
    } else {
      intercept(AccessDeniedException.class, () ->
          roleFS.getFileStatus(noReadFile));
    }
    intercept(AccessDeniedException.class, () ->
        ContractTestUtils.readUTF8(roleFS, noReadFile, data.length));
    if (authMode) {
      try (FSDataInputStream is = roleFS.open(emptyFile)) {
        Assertions.assertThat(is.read())
            .describedAs("read of empty file")
            .isEqualTo(-1);
      }
      roleFS.getFileStatus(noReadFile);
    } else {
      intercept(AccessDeniedException.class, () ->
          ContractTestUtils.readUTF8(roleFS, emptyFile, 0));
    }

    globFS(realFS, noReadFile, true, true, false);
    // a file fails if not in auth mode
    globFS(roleFS, noReadFile, true, true, !authMode);
    // empty directories don't fail.
    assertStatusPathEquals(emptyDir,
        globFS(roleFS, emptyDir, true, true, false));
    assertStatusPathEquals(noReadFile,
        globFS(roleFS,
            new Path(noReadDir, "*/*.txt"),
            true, true, false));
    // now run a located file status fetcher against it
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
   * @param nonNull must the result be non-null
   * @param notEmpty must the result be a non-empty array
   * @param expectAuthFailure is auth failure expected?
   * @return the result of a successful glob or null if an expected auth
   *          failure was caught.
   * @throws IOException failure.
   */
  protected FileStatus[] globFS(
      final S3AFileSystem fs,
      final Path path,
      final boolean nonNull,
      final boolean notEmpty,
      boolean expectAuthFailure)
      throws IOException {
    LOG.info("Glob {}", path);
    S3ATestUtils.MetricDiff getMetric = new S3ATestUtils.MetricDiff(fs,
        Statistic.OBJECT_METADATA_REQUESTS);
    S3ATestUtils.MetricDiff listMetric = new S3ATestUtils.MetricDiff(fs,
        Statistic.OBJECT_LIST_REQUESTS);
    FileStatus[] st = null;
    try {
      st = fs.globStatus(path);
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
    if (nonNull) {
      Assertions.assertThat(st)
          .describedAs("Glob of %s", path)
          .isNotNull();
    }
    if (notEmpty) {
      Assertions.assertThat(st)
          .describedAs("Glob of %s", path)
          .isNotEmpty();
    }
    return st;
  }
}
