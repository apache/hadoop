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

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBucketOverrides;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_ACCEPTABLE;

/**
 * Test the marker tool and use it to compare the behavior
 * of keeping vs legacy S3A FS instances.
 */
public class ITestMarkerTool extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestMarkerTool.class);


  private S3AFileSystem keepingFS;

  private S3AFileSystem mixedFS;

  private int expectedFiles;

  private int expectMarkersUnderDir1;

  private int expectMarkersUnderDir2;

  private int expectMarkers;

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
    URI testFSUri = testFS.getUri();
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
    final S3AFileSystem newFS = createFS(testFSUri, conf);
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

  private static Path mkpath(Path base, final String name) {
    return name.isEmpty() ? base : new Path(base, name);
  }

  /**
   * Tracker of created paths.
   */
  private static class CreatedPaths {

    private Path base;

    private List<Path> files = new ArrayList<>();

    private List<Path> dirs = new ArrayList<>();

    private List<Path> emptyDirs = new ArrayList<>();

    private List<String> filesUnderBase = new ArrayList<>();

    private List<String> dirsUnderBase = new ArrayList<>();

    private List<String> emptyDirsUnderBase = new ArrayList<>();


    private Path mkdir(FileSystem fs, String name)
        throws IOException {
      Path dir = mkpath(base, name);
      fs.mkdirs(dir);
      dirs.add(dir);
      dirsUnderBase.add(name);
      return dir;
    }

    private Path emptydir(FileSystem fs, String name)
        throws IOException {
      Path dir = mkpath(base, name);
      fs.mkdirs(dir);
      emptyDirs.add(dir);
      emptyDirsUnderBase.add(name);
      return dir;
    }

    private Path mkfile(FileSystem fs, String name)
        throws IOException {
      Path file = mkpath(base, name);
      ContractTestUtils.touch(fs, file);
      files.add(file);
      filesUnderBase.add(name);
      return file;
    }
  }

  /**
   * Create the "standard" test paths.
   * @param fs filesystem
   * @param base base dir
   * @return the details on what was created.
   */
  private CreatedPaths createPaths(FileSystem fs, Path base)
      throws IOException {
    CreatedPaths r = new CreatedPaths();
    r.base = base;
    // the directories under which we will create files,
    // so expect to have markers
    r.mkdir(fs, "");
    r.mkdir(fs, "dir1");
    r.mkdir(fs, "dir2");
    r.mkdir(fs, "dir2/dir3");

    // create the empty dirs
    r.emptydir(fs, "empty");
    r.emptydir(fs, "dir2/empty");

    // files
    r.mkfile(fs, "dir1/file1");
    r.mkfile(fs, "dir2/file2");
    r.mkfile(fs, "dir2/dir3/file3");

    expectedFiles = 3;
    expectMarkersUnderDir1 = 1;
    expectMarkersUnderDir2 = 2;
    expectMarkers = expectMarkersUnderDir1 + expectMarkersUnderDir2;
    return r;
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
  private MarkerTool.ScanResult markerTool(
      final int exitCode,
      final FileSystem sourceFS,
      final Path path,
      final boolean doPurge,
      final int expectedMarkerCount) throws IOException {
    MarkerTool tool = new MarkerTool(sourceFS.getConf());
    tool.setVerbose(true);

    MarkerTool.ScanResult result = tool.execute(sourceFS, path, doPurge,
        expectedMarkerCount);
    Assertions.assertThat(result.getExitCode())
        .describedAs("Exit code of marker(%s, %s, %d) -> %s",
            path, doPurge, expectedMarkerCount, result)
        .isEqualTo(exitCode);
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

    // audit will find the expected entries
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
    assertMarkersDeleted(expectedMarkerCount,
        markerTool(fs, createdPaths.base, true, expectedMarkerCount));
    // and a rerun doesn't find markers
    assertMarkersDeleted(0,
        markerTool(fs, createdPaths.base, true, 0));
  }

  @Test
  public void testRenameKeepingFS() throws Throwable {
    describe(
        "Rename with the keeping FS -verify that no markers exist at far end");
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
    LOG.info("Auditing destination paths");
    verifyRenamed(dest, createdPaths);

  }

  void verifyRenamed(final Path dest,
      final CreatedPaths createdPaths) throws IOException {
    // all leaf directories exist
    for (String p : createdPaths.emptyDirsUnderBase) {
      assertIsDirectory(mkpath(dest, p));
    }
    // non-empty dirs
    for (String p : createdPaths.dirsUnderBase) {
      assertIsDirectory(mkpath(dest, p));
    }
    // all files exist
    for (String p : createdPaths.filesUnderBase) {
      assertIsFile(mkpath(dest, p));
    }
  }

  /**
   * Create a FS where only dir2 in the source tree keeps markers;
   * verify all is good.
   */
  @Test
  public void testAuthPathIsMixed() throws Throwable {
    describe("Create a source tree with mixed semantics");
    Path base = methodPath();
    Path source = new Path(base, "source");
    Path dest = new Path(base, "dest");
    Path dir2 = new Path(source, "dir2");
    S3AFileSystem mixedFSDir2 = createFS(DIRECTORY_MARKER_POLICY_AUTHORITATIVE,
        dir2.toUri().toString());
    // some of these paths will retain markers, some will not
    CreatedPaths createdPaths = createPaths(mixedFSDir2, source);

    // markers are only under dir2
    markerTool(mixedFSDir2, mkpath(source, "dir1"), false, 0);
    markerTool(mixedFSDir2, source, false, expectMarkersUnderDir2);

    // if we now rename, all will be good
    mixedFSDir2.rename(source, dest);
    assertIsDirectory(dest);

    // there are no markers
    MarkerTool.ScanResult scanResult = markerTool(mixedFSDir2, dest, false, 0);
    // there are exactly the files we want
    Assertions.assertThat(scanResult)
        .describedAs("Scan result %s", scanResult)
        .extracting(s -> s.getTracker().getFilesFound())
        .isEqualTo(expectedFiles);
    verifyRenamed(dest, createdPaths);
  }

  /**
   * Assert that an expected number of markers were deleted.
   * @param expected expected count.
   * @param result scan result
   */
  private static void assertMarkersDeleted(int expected,
      MarkerTool.ScanResult result) {

    Assertions.assertThat(result.getPurgeSummary())
        .describedAs("Purge result of scan %s", result)
        .isNotNull()
        .extracting(f -> f.getMarkersDeleted())
        .isEqualTo(expected);
  }
}
