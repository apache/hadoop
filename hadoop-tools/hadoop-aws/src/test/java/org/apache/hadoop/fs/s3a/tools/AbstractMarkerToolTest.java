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

package org.apache.hadoop.fs.s3a.tools;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBucketOverrides;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.VERBOSE;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardToolTestHelper.runS3GuardCommand;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardToolTestHelper.runS3GuardCommandToFailure;
import static org.apache.hadoop.fs.s3a.tools.MarkerTool.UNLIMITED_LISTING;

/**
 * Class for marker tool tests -sets up keeping/deleting filesystems,
 * has methods to invoke.
 */
public class AbstractMarkerToolTest extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractMarkerToolTest.class);

  /** the -verbose option. */
  protected static final String V = AbstractMarkerToolTest.m(VERBOSE);

  /** FS which keeps markers. */
  private S3AFileSystem keepingFS;

  /** FS which deletes markers. */
  private S3AFileSystem deletingFS;

  /** FS which mixes markers; only created in some tests. */
  private S3AFileSystem mixedFS;
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    String bucketName = getTestBucketName(conf);
    removeBaseAndBucketOverrides(bucketName, conf,
        S3A_BUCKET_PROBE,
        DIRECTORY_MARKER_POLICY,
        S3_METADATA_STORE_IMPL,
        METADATASTORE_AUTHORITATIVE,
        AUTHORITATIVE_PATH);
    // base FS is legacy
    conf.set(DIRECTORY_MARKER_POLICY, DIRECTORY_MARKER_POLICY_DELETE);
    conf.set(S3_METADATA_STORE_IMPL, S3GUARD_METASTORE_NULL);

    // turn off bucket probes for a bit of speedup in the connectors we create.
    conf.setInt(S3A_BUCKET_PROBE, 0);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    setKeepingFS(createFS(DIRECTORY_MARKER_POLICY_KEEP, null));
    setDeletingFS(createFS(DIRECTORY_MARKER_POLICY_DELETE, null));
  }

  @Override
  public void teardown() throws Exception {
    // do this ourselves to avoid audits teardown failing
    // when surplus markers are found
    deleteTestDirInTeardown();
    super.teardown();
    IOUtils.cleanupWithLogger(LOG, getKeepingFS(),
        getMixedFS(), getDeletingFS());

  }

  /**
   * FS which deletes markers.
   */
  public S3AFileSystem getDeletingFS() {
    return deletingFS;
  }

  public void setDeletingFS(final S3AFileSystem deletingFS) {
    this.deletingFS = deletingFS;
  }

  /**
   * FS which keeps markers.
   */
  protected S3AFileSystem getKeepingFS() {
    return keepingFS;
  }

  private void setKeepingFS(S3AFileSystem keepingFS) {
    this.keepingFS = keepingFS;
  }

  /** only created on demand. */
  private S3AFileSystem getMixedFS() {
    return mixedFS;
  }

  protected void setMixedFS(S3AFileSystem mixedFS) {
    this.mixedFS = mixedFS;
  }

  /**
   * Get a filename for a temp file.
   * The generated file is deleted.
   *
   * @return a file path for a output file
   */
  protected File tempAuditFile() throws IOException {
    final File audit = File.createTempFile("audit", ".txt");
    audit.delete();
    return audit;
  }

  /**
   * Read the audit output and verify it has the expected number of lines.
   * @param auditFile audit file to read
   * @param expected expected line count
   */
  protected void expectMarkersInOutput(final File auditFile,
      final int expected)
      throws IOException {
    final List<String> lines = readOutput(auditFile);
    Assertions.assertThat(lines)
        .describedAs("Content of %s", auditFile)
        .hasSize(expected);
  }

  /**
   * Read the output file in. Logs the contents at info.
   * @param outputFile audit output file.
   * @return the lines
   */
  protected List<String> readOutput(final File outputFile)
      throws IOException {
    try (FileReader reader = new FileReader(outputFile)) {
      final List<String> lines =
          org.apache.commons.io.IOUtils.readLines(reader);

      LOG.info("contents of output file {}\n{}", outputFile,
          StringUtils.join("\n", lines));
      return lines;
    }
  }

  /**
   * Create a new FS with given marker policy and path.
   * This filesystem MUST be closed in test teardown.
   * @param markerPolicy markers
   * @param authPath authoritative path. If null: no path.
   * @return a new FS.
   */
  protected S3AFileSystem createFS(String markerPolicy,
      String authPath) throws Exception {
    S3AFileSystem testFS = getFileSystem();
    Configuration conf = new Configuration(testFS.getConf());
    URI testFSUri = testFS.getUri();
    String bucketName = getTestBucketName(conf);
    removeBucketOverrides(bucketName, conf,
        DIRECTORY_MARKER_POLICY,
        S3_METADATA_STORE_IMPL,
        BULK_DELETE_PAGE_SIZE,
        AUTHORITATIVE_PATH);
    if (authPath != null) {
      conf.set(AUTHORITATIVE_PATH, authPath);
    }
    // Use a very small page size to force the paging
    // code to be tested.
    conf.setInt(BULK_DELETE_PAGE_SIZE, 2);
    conf.set(S3_METADATA_STORE_IMPL, S3GUARD_METASTORE_NULL);
    conf.set(DIRECTORY_MARKER_POLICY, markerPolicy);
    S3AFileSystem fs2 = new S3AFileSystem();
    fs2.initialize(testFSUri, conf);
    LOG.info("created new filesystem with policy {} and auth path {}",
        markerPolicy,
        (authPath == null ? "(null)": authPath));
    return fs2;
  }

  /**
   * Execute the marker tool, expecting the execution to succeed.
   * @param sourceFS filesystem to use
   * @param path path to scan
   * @param doPurge should markers be purged
   * @param expectedMarkerCount number of markers expected
   * @return the result
   */
  protected MarkerTool.ScanResult markerTool(
      final FileSystem sourceFS,
      final Path path,
      final boolean doPurge,
      final int expectedMarkerCount)
      throws IOException {
    return markerTool(0, sourceFS, path, doPurge,
        expectedMarkerCount,
        UNLIMITED_LISTING, false);
  }

  /**
   * Run a S3GuardTool command from a varags list and the
   * configuration returned by {@code getConfiguration()}.
   * @param args argument list
   * @return the return code
   * @throws Exception any exception
   */
  protected int run(Object... args) throws Exception {
    return runS3GuardCommand(uncachedFSConfig(getConfiguration()), args);
  }

  /**
   * Take a configuration, copy it and disable FS Caching on
   * the new one.
   * @param conf source config
   * @return a new, patched, config
   */
  protected Configuration uncachedFSConfig(final Configuration conf) {
    Configuration c = new Configuration(conf);
    disableFilesystemCaching(c);
    return c;
  }

  /**
   * given an FS instance, create a matching configuration where caching
   * is disabled.
   * @param fs source
   * @return new config.
   */
  protected Configuration uncachedFSConfig(final FileSystem fs) {
    return uncachedFSConfig(fs.getConf());
  }

    /**
     * Run a S3GuardTool command from a varags list, catch any raised
     * ExitException and verify the status code matches that expected.
     * @param status expected status code of the exception
     * @param args argument list
     * @throws Exception any exception
     */
  protected void runToFailure(int status, Object... args)
      throws Exception {
    Configuration conf = uncachedFSConfig(getConfiguration());
    runS3GuardCommandToFailure(conf, status, args);
  }

  /**
   * Given a base and a filename, create a new path.
   * @param base base path
   * @param name name: may be empty, in which case the base path is returned
   * @return a path
   */
  protected static Path toPath(final Path base, final String name) {
    return name.isEmpty() ? base : new Path(base, name);
  }

  /**
   * Execute the marker tool, expecting the execution to
   * return a specific exit code.
   *
   * @param sourceFS filesystem to use
   * @param exitCode exit code to expect.
   * @param path path to scan
   * @param doPurge should markers be purged
   * @param expectedMarkers number of markers expected
   * @param limit limit of files to scan; -1 for 'unlimited'
   * @param nonAuth only use nonauth path count for failure rules
   * @return the result
   */
  public static MarkerTool.ScanResult markerTool(
      final int exitCode,
      final FileSystem sourceFS,
      final Path path,
      final boolean doPurge,
      final int expectedMarkers,
      final int limit,
      final boolean nonAuth) throws IOException {

    MarkerTool.ScanResult result = MarkerTool.execMarkerTool(
        new MarkerTool.ScanArgsBuilder()
            .withSourceFS(sourceFS)
            .withPath(path)
            .withDoPurge(doPurge)
            .withMinMarkerCount(expectedMarkers)
            .withMaxMarkerCount(expectedMarkers)
            .withLimit(limit)
            .withNonAuth(nonAuth)
            .build());
    Assertions.assertThat(result.getExitCode())
        .describedAs("Exit code of marker(%s, %s, %d) -> %s",
            path, doPurge, expectedMarkers, result)
        .isEqualTo(exitCode);
    return result;
  }

  /**
   * Add a "-" prefix to a string.
   * @param s string to prefix
   * @return a string for passing into the CLI
   */
  protected static String m(String s) {
    return "-" + s;
  }

}
