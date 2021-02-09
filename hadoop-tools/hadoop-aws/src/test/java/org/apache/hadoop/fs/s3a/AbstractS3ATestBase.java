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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;
import org.apache.hadoop.fs.s3a.tools.MarkerTool;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.io.IOUtils;

import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestDynamoTablePrefix;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestPropertyBool;
import static org.apache.hadoop.fs.s3a.S3AUtils.E_FS_CLOSED;
import static org.apache.hadoop.fs.s3a.tools.MarkerTool.UNLIMITED_LISTING;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;

/**
 * An extension of the contract test base set up for S3A tests.
 */
public abstract class AbstractS3ATestBase extends AbstractFSContractTestBase
    implements S3ATestConstants {
  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractS3ATestBase.class);

  /**
   * Flag to set if S3Guard is downgrading as DDB tables
   * are not found. This is used to exit faster on
   * tests which require a guarded bucket.
   */
  private static boolean s3GuardDowngrading = false;

  /**
   * Flag to set if S3Guard is required for the test
   * suite/run. Will trigger early exit if downgrading is in progress.
   */
  private boolean s3GuardRequired;

  /**
   * FileSystem statistics are collected across every test case.
   */
  protected static final IOStatisticsSnapshot FILESYSTEM_IOSTATS =
      snapshotIOStatistics();

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf, false);
  }

  @Override
  public void setup() throws Exception {
    Thread.currentThread().setName("setup");
    // force load the local FS -not because we want the FS, but we need all
    // filesystems which add default configuration resources to do it before
    // our tests start adding/removing options. See HADOOP-16626.
    FileSystem.getLocal(new Configuration());
    // Force deprecated key load through the
    // static initializers. See: HADOOP-17385
    S3AFileSystem.initializeClass();
    if (isS3GuardRequired()) {
      skipIfS3GuardDowngrading();
    }
    super.setup();
    S3AFileSystem fs = getFileSystem();
    // check that the FS has the expected state
    if (isS3GuardRequired()) {
      downgradeIfUnguarded(fs);
    }
  }

  @Override
  public void teardown() throws Exception {
    Thread.currentThread().setName("teardown");

    maybeAuditTestPath();

    super.teardown();
    if (getFileSystem() != null) {
      FILESYSTEM_IOSTATS.aggregate(getFileSystem().getIOStatistics());
    }
    describe("closing file system");
    IOUtils.closeStream(getFileSystem());
  }

  /**
   * Dump the filesystem statistics after the class.
   */
  @AfterClass
  public static void dumpFileSystemIOStatistics() {
    LOG.info("Aggregate FileSystem Statistics {}",
        ioStatisticsToPrettyString(FILESYSTEM_IOSTATS));
  }

  /**
   * Audit the FS under {@link #methodPath()} if
   * the test option {@link #DIRECTORY_MARKER_AUDIT} is
   * true.
   */
  public void maybeAuditTestPath() {
    final S3AFileSystem fs = getFileSystem();
    if (fs != null) {
      try {
        boolean audit = getTestPropertyBool(fs.getConf(),
            DIRECTORY_MARKER_AUDIT, false);
        Path methodPath = methodPath();
        if (audit
            && !fs.getDirectoryMarkerPolicy()
            .keepDirectoryMarkers(methodPath)
            && fs.isDirectory(methodPath)) {
          MarkerTool.ScanResult result = MarkerTool.execMarkerTool(
              new MarkerTool.ScanArgsBuilder()
                  .withSourceFS(fs)
                  .withPath(methodPath)
                  .withDoPurge(true)
                  .withMinMarkerCount(0)
                  .withMaxMarkerCount(0)
                  .withLimit(UNLIMITED_LISTING)
                  .withNonAuth(false)
                  .build());
          final String resultStr = result.toString();
          assertEquals("Audit of " + methodPath + " failed: "
                  + resultStr,
              0, result.getExitCode());
          assertEquals("Marker Count under " + methodPath
                  + " non-zero: " + resultStr,
              0, result.getFilteredMarkerCount());
        }
      } catch (FileNotFoundException ignored) {
      } catch (Exception e) {
        // If is this is not due to the FS being closed: log.
        if (!e.toString().contains(E_FS_CLOSED)) {
          LOG.warn("Marker Tool Failure", e);
        }
      }
    }
  }

  @Override
  protected int getTestTimeoutMillis() {
    return S3A_TEST_TIMEOUT;
  }

  /**
   * Create a configuration, possibly patching in S3Guard options.
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    return S3ATestUtils.prepareTestConfiguration(conf);
  }

  protected Configuration getConfiguration() {
    return getContract().getConf();
  }

  /**
   * Get the filesystem as an S3A filesystem.
   * @return the typecast FS
   */
  @Override
  public S3AFileSystem getFileSystem() {
    return (S3AFileSystem) super.getFileSystem();
  }

  /**
   * Describe a test in the logs.
   * @param text text to print
   * @param args arguments to format in the printing
   */
  protected void describe(String text, Object... args) {
    LOG.info("\n\n{}: {}\n",
        getMethodName(),
        String.format(text, args));
  }

  /**
   * Write a file, read it back, validate the dataset. Overwrites the file
   * if it is present
   * @param name filename (will have the test path prepended to it)
   * @param len length of file
   * @return the full path to the file
   * @throws IOException any IO problem
   */
  protected Path writeThenReadFile(String name, int len) throws IOException {
    Path path = path(name);
    writeThenReadFile(path, len);
    return path;
  }

  /**
   * Write a file, read it back, validate the dataset. Overwrites the file
   * if it is present
   * @param path path to file
   * @param len length of file
   * @throws IOException any IO problem
   */
  protected void writeThenReadFile(Path path, int len) throws IOException {
    byte[] data = dataset(len, 'a', 'z');
    writeDataset(getFileSystem(), path, data, data.length, 1024 * 1024, true);
    ContractTestUtils.verifyFileContents(getFileSystem(), path, data);
  }

  protected String getTestTableName(String suffix) {
    return getTestDynamoTablePrefix(getConfiguration()) + suffix;
  }

  /**
   * Is S3Guard Downgrading?
   * @return true if a set has concluded that S3Guard is downgrading
   * and that parameterized runs can exit fast.
   */
  protected static boolean isS3GuardDowngrading() {
    return s3GuardDowngrading;
  }

  /**
   * Set the downgrading status; if set this will
   * cause subsequent tests to skip s3guard runs.
   * @param downgrading true if S3Guard is downgrading
   */
  protected static void setS3GuardDowngrading(final boolean downgrading) {
    s3GuardDowngrading = downgrading;
  }

  /**
   * Verify that an FS has S3Guard enabled; if the FS
   * has downgraded then not only skip this test but
   * set the static {@link #s3GuardDowngrading} flag
   * for faster exit of subsequent tests.
   * @param fs filesystem to probe.
   */
  protected static void downgradeIfUnguarded(S3AFileSystem fs) {
    if (!fs.hasMetadataStore()) {
      setS3GuardDowngrading(true);
      // if the FS has fallen back to unguarded, then
      // all tests in a guarded run are going to
      // fail as their cost estimates will be wrong.
      assume("Filesystem does not have a metastore",
          false);
    }
  }

  /**
   * Exit fast on any parameterized test case where S3Guard
   * is downgrading. Call before creating any FS instance.
   */
  protected static void skipIfS3GuardDowngrading() {
    assume("Filesystem does not have a metastore",
        !s3GuardDowngrading);
  }

  /**
   * Declare whether a test run must have S3Guard.
   * @param s3GuardRequired is s3guard required.
   */
  protected final void setS3GuardRequired(final boolean s3GuardRequired) {
    this.s3GuardRequired = s3GuardRequired;
  }

  /**
   * Does this test require S3Guard?
   * @return true if the test expects S3Guard to be live.
   */
  protected final boolean isS3GuardRequired() {
    return s3GuardRequired;
  }
}
