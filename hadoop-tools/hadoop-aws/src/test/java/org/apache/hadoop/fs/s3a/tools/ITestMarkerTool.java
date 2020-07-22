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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.s3a.Constants.AUTHORITATIVE_PATH;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_KEEP;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.S3A_BUCKET_PROBE;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_METASTORE_NULL;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBucketOverrides;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_ACCEPTABLE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test the marker tool and use it to compare the behavior
 * of keeping vs legacy S3A FS instances.
 */
public class ITestMarkerTool extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestMarkerTool.class);


  private S3AFileSystem keepingFS;
  private S3AFileSystem mixedFS;

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    String bucketName = getTestBucketName(conf);
    removeBaseAndBucketOverrides(bucketName, conf,
        S3A_BUCKET_PROBE,
        DIRECTORY_MARKER_POLICY,
        METADATASTORE_AUTHORITATIVE,
        AUTHORITATIVE_PATH);
    // base FS is legacy
    conf.set(DIRECTORY_MARKER_POLICY, DIRECTORY_MARKER_POLICY_DELETE);
    // turn off bucket probes for a bit of speedup in the connectors we create.
    conf.setInt(S3A_BUCKET_PROBE, 0);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    setKeepingFS(createFS(DIRECTORY_MARKER_POLICY_KEEP, null));
  }

  @Override
  public void teardown() throws Exception {
    IOUtils.cleanupWithLogger(LOG, getKeepingFS(), getMixedFS());
    super.teardown();
  }

  /**
   * Create and initialize a new filesystem.
   * This filesystem MUST be closed in test teardown.
   * @param uri FS URI
   * @param config config.
   * @return new instance
   * @throws IOException failure
   */
  private S3AFileSystem createFS(final URI uri, final Configuration config)
      throws IOException {
    S3AFileSystem fs2 = new S3AFileSystem();
    fs2.initialize(uri, config);
    return fs2;
  }

  /**
   * Create a new FS with given marker policy and path.
   * This filesystem MUST be closed in test teardown.
   * @param markerPolicy markers
   * @param authPath authoritative path. If null: no path.
   * @return a new FS.
   */
  private S3AFileSystem createFS(String markerPolicy,
      String authPath) throws Exception {
    S3AFileSystem testFS = getFileSystem();
    Configuration conf = new Configuration(testFS.getConf());
    URI uri = testFS.getUri();
    String bucketName = getTestBucketName(conf);
    removeBucketOverrides(bucketName, conf,
        DIRECTORY_MARKER_POLICY,
        S3_METADATA_STORE_IMPL,
        AUTHORITATIVE_PATH);
    if (authPath != null) {

      conf.set(AUTHORITATIVE_PATH, authPath);
    }
    conf.set(S3_METADATA_STORE_IMPL, S3GUARD_METASTORE_NULL);
    conf.set(DIRECTORY_MARKER_POLICY, markerPolicy);
    final S3AFileSystem newFS = createFS(uri, conf);
    return newFS;
  }

  /**
   * FS which deletes markers.
   */
  private S3AFileSystem getLegacyFS() {
    return getFileSystem();
  }


  /**
   * FS which keeps markers.
   */
  private S3AFileSystem getKeepingFS() {
    return keepingFS;
  }

  private void setKeepingFS(S3AFileSystem keepingFS) {
    this.keepingFS = keepingFS;
  }

  /** only created on demand. */
  private S3AFileSystem getMixedFS() {
    return mixedFS;
  }

  private void setMixedFS(S3AFileSystem mixedFS) {
    this.mixedFS = mixedFS;
  }

  private static class CreatedPaths {
    Path base;
    List<Path> files = new ArrayList<>();
    List<Path> dirs = new ArrayList<>();
    List<Path> emptyDirs = new ArrayList<>();
  }

  private CreatedPaths createPaths(FileSystem fs, Path base)
      throws IOException {
    CreatedPaths r = new CreatedPaths();
    r.base = base;
    Path dir1 = mkdir(r, fs, base, "dir1");
    Path subdir2 = mkdir(r, fs, dir1, "subdir2");
    Path subdir3 = mkdir(r, fs, subdir2, "subdir3");

    // create the emtpy dir
    Path empty = mkdir(r, fs, base, "empty");
    r.emptyDirs.add(empty);
    r.dirs.remove(empty);

    // files
    mkfile(r, fs, dir1, "file1");
    mkfile(r, fs, subdir2, "file2");
    mkfile(r, fs, subdir3, "file3");
    return r;
  }

  private Path mkdir(CreatedPaths r, FileSystem fs, Path base, String name)
      throws IOException {
    Path dir = new Path(base, name);
    fs.mkdirs(dir);
    r.dirs.add(dir);
    return dir;
  }

  private Path mkfile(CreatedPaths r,
      FileSystem fs, Path base, String name) throws IOException {
    Path file = new Path(base, name);
    ContractTestUtils.touch(fs, file);
    r.files.add(file);
    return file;
  }

  private MarkerTool.ScanResult markerTool(
      final FileSystem sourceFS,
      final Path path,
      final boolean doPurge,
      final int expectedMarkerCount)
      throws IOException {
    return markerTool(0, sourceFS, path, doPurge, expectedMarkerCount);
  }

  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  private MarkerTool.ScanResult markerTool(final int expected,
      final FileSystem sourceFS,
      final Path path,
      final boolean doPurge,
      final int expectedMarkerCount) throws IOException {
    MarkerTool tool = new MarkerTool(sourceFS.getConf());
    tool.setVerbose(true);

    MarkerTool.ScanResult result = tool.execute(sourceFS, path, doPurge,
        expectedMarkerCount);
    Assertions.assertThat(result.exitCode)
        .describedAs("Exit code of marker(%s, %s, %d) -> %s",
            path, doPurge, expectedMarkerCount, result)
        .isEqualTo(expected);
    return result;
  }

  @Test
  public void testAuditPruneMarkersLegacyDir() throws Throwable {
    CreatedPaths createdPaths = createPaths(getLegacyFS(), methodPath());
    markerTool(getLegacyFS(), createdPaths.base, false, 0);
    markerTool(getLegacyFS(), createdPaths.base, true, 0);
  }

  @Test
  public void testAuditPruneMarkersKeepingDir() throws Throwable {
    CreatedPaths createdPaths = createPaths(getKeepingFS(), methodPath());

    // audit will find three entries
    int expectedMarkerCount = createdPaths.dirs.size();
    S3AFileSystem fs = getLegacyFS();
    markerTool(EXIT_NOT_ACCEPTABLE, fs,
        createdPaths.base, false, 0);

    markerTool(fs, createdPaths.base, false,
        expectedMarkerCount);
    // we know a purge didn't take place
    markerTool(fs, createdPaths.base, false,
        expectedMarkerCount);
    // purge cleans up
    markerTool(fs, createdPaths.base, true, expectedMarkerCount);
    // and a rerun doesn't find markers
    markerTool(fs, createdPaths.base, true, 0);
  }

  @Test
  public void testRenameKeepingFS() throws Throwable {
    describe("Rename with the keeping FS");
    Path base = methodPath();
    Path source = new Path(base, "source");
    Path dest = new Path(base, "dest");

    S3AFileSystem fs = getKeepingFS();
    CreatedPaths createdPaths = createPaths(fs, source);

    // audit will find three entries
    int expectedMarkerCount = createdPaths.dirs.size();

    markerTool(fs, source, false,
        expectedMarkerCount);
    fs.rename(source, dest);
    assertIsDirectory(dest);

    // there are no markers
    markerTool(fs, dest, false, 0);
  }

}
