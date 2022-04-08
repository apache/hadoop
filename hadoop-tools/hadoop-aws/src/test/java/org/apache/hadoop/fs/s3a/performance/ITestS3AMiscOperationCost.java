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

package org.apache.hadoop.fs.s3a.performance;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.Statistic.AUDIT_SPAN_CREATION;
import static org.apache.hadoop.fs.s3a.Statistic.INVOCATION_GET_CONTENT_SUMMARY;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_LIST_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_METADATA_REQUESTS;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_ENABLED;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.FILESTATUS_DIR_PROBE_L;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.FILE_STATUS_FILE_PROBE;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.LIST_OPERATION;
import static org.apache.hadoop.fs.s3a.performance.OperationCostValidator.probe;

/**
 * Use metrics to assert about the cost of misc operations.
 * Parameterized on directory marker keep vs delete
 */
@RunWith(Parameterized.class)
public class ITestS3AMiscOperationCost extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AMiscOperationCost.class);

  /**
   * Parameter: should auditing be enabled?
   */
  private final boolean auditing;

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"keep-markers-auditing", true, true},
        {"delete-markers-unaudited", false, false}
    });
  }

  public ITestS3AMiscOperationCost(final String name,
      final boolean keepMarkers,
      final boolean auditing) {
    super(keepMarkers);
    this.auditing = auditing;
  }

  @Override
  public Configuration createConfiguration() {
    final Configuration conf = super.createConfiguration();
    removeBaseAndBucketOverrides(conf, AUDIT_ENABLED);
    conf.setBoolean(AUDIT_ENABLED, auditing);
    return conf;
  }

  /**
   * Expected audit count when auditing is enabled; expect 0
   * when disabled.
   * @param expected expected value.
   * @return the probe.
   */
  protected OperationCostValidator.ExpectedProbe withAuditCount(
      final int expected) {
    return probe(AUDIT_SPAN_CREATION,
        auditing ? expected : 0);

  }

  /**
   * Common operation which should be low cost as possible.
   */
  @Test
  public void testMkdirOverDir() throws Throwable {
    describe("create a dir over a dir");
    S3AFileSystem fs = getFileSystem();
    // create base dir with marker
    Path baseDir = dir(methodPath());

    // create the child; only assert on HEAD/GET IO
    verifyMetrics(() -> fs.mkdirs(baseDir),
        withAuditCount(1),
        // full probe on dest plus list only on parent.
        with(OBJECT_METADATA_REQUESTS, 0),
        with(OBJECT_LIST_REQUEST, FILESTATUS_DIR_PROBE_L));
  }

  @Test
  public void testGetContentSummaryRoot() throws Throwable {
    describe("getContentSummary on Root");
    S3AFileSystem fs = getFileSystem();

    Path root = new Path("/");
    verifyMetrics(() -> getContentSummary(root),
        with(INVOCATION_GET_CONTENT_SUMMARY, 1));
  }

  @Test
  public void testGetContentSummaryDir() throws Throwable {
    describe("getContentSummary on test dir with children");
    S3AFileSystem fs = getFileSystem();
    Path baseDir = methodPath();
    Path childDir = new Path(baseDir, "subdir/child");
    touch(fs, childDir);

    // look at path to see if it is a file
    // it is not: so LIST
    final ContentSummary summary = verifyMetrics(
        () -> getContentSummary(baseDir),
        with(INVOCATION_GET_CONTENT_SUMMARY, 1),
        withAuditCount(1),
        always(FILE_STATUS_FILE_PROBE    // look at path to see if it is a file
            .plus(LIST_OPERATION)));         // it is not: so LIST

    Assertions.assertThat(summary.getDirectoryCount())
        .as("Summary " + summary)
        .isEqualTo(2);
    Assertions.assertThat(summary.getFileCount())
        .as("Summary " + summary)
        .isEqualTo(1);
  }

  @Test
  public void testGetContentMissingPath() throws Throwable {
    describe("getContentSummary on a missing path");
    Path baseDir = methodPath();
    verifyMetricsIntercepting(FileNotFoundException.class,
        "", () -> getContentSummary(baseDir),
        with(INVOCATION_GET_CONTENT_SUMMARY, 1),
        withAuditCount(1),
        always(FILE_STATUS_FILE_PROBE
            .plus(FILE_STATUS_FILE_PROBE)
            .plus(LIST_OPERATION)
            .plus(LIST_OPERATION)));
  }

  private ContentSummary getContentSummary(final Path baseDir)
      throws IOException {
    S3AFileSystem fs = getFileSystem();
    return fs.getContentSummary(baseDir);
  }

}
