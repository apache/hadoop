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

package org.apache.hadoop.fs.s3a.audit;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;
import org.apache.hadoop.security.AccessControlException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Statistic.AUDIT_ACCESS_CHECK_FAILURE;
import static org.apache.hadoop.fs.s3a.Statistic.AUDIT_REQUEST_EXECUTION;
import static org.apache.hadoop.fs.s3a.Statistic.INVOCATION_ACCESS;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_IO_REQUEST;
import static org.apache.hadoop.fs.s3a.audit.AuditTestSupport.resetAuditOptions;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_ENABLED;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_SERVICE_CLASSNAME;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.FILE_STATUS_ALL_PROBES;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.FILE_STATUS_FILE_PROBE;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.ROOT_FILE_STATUS_PROBE;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;

/**
 * Test S3A FS Access permit/deny is passed through all the way to the
 * auditor.
 * Uses {@link AccessCheckingAuditor} to enable/disable access.
 * There are not currently any contract tests for this; behaviour
 * based on base FileSystem implementation.
 */
public class ITestAuditAccessChecks extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAuditAccessChecks.class);

  private AccessCheckingAuditor auditor;

  public ITestAuditAccessChecks() {
    super(true);
  }

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    resetAuditOptions(conf);
    conf.set(AUDIT_SERVICE_CLASSNAME, AccessCheckingAuditor.CLASS);
    conf.setBoolean(AUDIT_ENABLED, true);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    auditor = (AccessCheckingAuditor) getFileSystem().getAuditor();
  }

  @Test
  public void testFileAccessAllowed() throws Throwable {
    describe("Enable checkaccess and verify it works with expected"
        + " statistics");
    auditor.setAccessAllowed(true);
    Path path = methodPath();
    S3AFileSystem fs = getFileSystem();
    touch(fs, path);
    verifyMetrics(
        () -> access(fs, path),
        with(INVOCATION_ACCESS, 1),
        whenRaw(FILE_STATUS_FILE_PROBE));
  }

  @Test
  public void testDirAccessAllowed() throws Throwable {
    describe("Enable checkaccess and verify it works with a dir");
    auditor.setAccessAllowed(true);
    Path path = methodPath();
    S3AFileSystem fs = getFileSystem();
    mkdirs(path);
    verifyMetrics(
        () -> access(fs, path),
        with(INVOCATION_ACCESS, 1),
        whenRaw(FILE_STATUS_ALL_PROBES));
  }

  @Test
  public void testRootDirAccessAllowed() throws Throwable {
    describe("Enable checkaccess and verify root dir access");
    auditor.setAccessAllowed(true);
    Path path = new Path("/");
    S3AFileSystem fs = getFileSystem();
    mkdirs(path);
    verifyMetrics(
        () -> access(fs, path),
        with(INVOCATION_ACCESS, 1),
        whenRaw(ROOT_FILE_STATUS_PROBE));
  }

  /**
   * If the test auditor blocks access() calls, then
   * the audit will fall after checking to see if the file
   * exists.
   */
  @Test
  public void testFileAccessDenied() throws Throwable {
    describe("Disable checkaccess and verify it fails");
    auditor.setAccessAllowed(false);
    Path path = methodPath();
    S3AFileSystem fs = getFileSystem();
    touch(fs, path);
    verifyMetricsIntercepting(
        AccessControlException.class,
        "\"" + path + "\"",
        () -> access(fs, path),
        with(INVOCATION_ACCESS, 1),
        with(AUDIT_ACCESS_CHECK_FAILURE, 1),
        // one S3 request: a HEAD.
        with(AUDIT_REQUEST_EXECUTION, 1),
        whenRaw(FILE_STATUS_FILE_PROBE));
  }

  /**
   * If the test auditor blocks access() calls, then
   * the audit will fall after checking to see if the directory
   * exists.
   */
  @Test
  public void testDirAccessDenied() throws Throwable {
    describe("Disable checkaccess and verify it dir access denied");
    auditor.setAccessAllowed(false);
    Path path = methodPath();
    S3AFileSystem fs = getFileSystem();
    mkdirs(path);
    verifyMetricsIntercepting(
        AccessControlException.class,
        "\"" + path + "\"",
        () -> access(fs, path),
        with(INVOCATION_ACCESS, 1),
        // two S3 requests: a HEAD and a LIST.
        with(AUDIT_REQUEST_EXECUTION, 2),
        with(STORE_IO_REQUEST, 2),
        with(AUDIT_ACCESS_CHECK_FAILURE, 1),
        whenRaw(FILE_STATUS_ALL_PROBES));
  }

  /**
   * Missing path will fail with FNFE irrespective of
   * the access permission.
   */
  @Test
  public void testMissingPathAccessFNFE() throws Throwable {
    describe("access() on missing path goes to S3 and fails with FNFE");
    auditor.setAccessAllowed(false);
    Path path = methodPath();
    S3AFileSystem fs = getFileSystem();
    verifyMetricsIntercepting(
        FileNotFoundException.class,
        path.toString(),
        () -> access(fs, path),
        with(INVOCATION_ACCESS, 1),
        // two S3 requests: a HEAD and a LIST.
        with(AUDIT_REQUEST_EXECUTION, 2),
        with(AUDIT_ACCESS_CHECK_FAILURE, 0),
        whenRaw(FILE_STATUS_ALL_PROBES));
  }

  /**
   * Call {@link S3AFileSystem#access(Path, FsAction)}.
   * @param fs filesystem
   * @param path path to check access
   * @return the IOStatistics
   * @throws AccessControlException access denied
   * @throws IOException failure, including permission failure.
   */
  private String access(final S3AFileSystem fs, final Path path)
      throws AccessControlException, IOException {
    fs.access(path, FsAction.ALL);
    return ioStatisticsToPrettyString(fs.getIOStatistics());
  }

}
