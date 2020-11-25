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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_KEEP;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.BucketInfo.BUCKET_INFO;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardToolTestHelper.runS3GuardCommand;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardToolTestHelper.runS3GuardCommandToFailure;
import static org.apache.hadoop.fs.s3a.tools.MarkerTool.*;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_INTERRUPTED;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_ACCEPTABLE;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_FOUND;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_USAGE;

/**
 * Test the marker tool and use it to compare the behavior
 * of keeping vs legacy S3A FS instances.
 */
public class ITestMarkerTool extends AbstractMarkerToolTest {

  protected static final Logger LOG =
      LoggerFactory.getLogger(ITestMarkerTool.class);

  /**
   * How many files to expect.
   */
  private int expectedFileCount;

  /**
   * How many markers to expect under dir1.
   */
  private int expectedMarkersUnderDir1;

  /**
   * How many markers to expect under dir2.
   */
  private int expectedMarkersUnderDir2;

  /**
   * How many markers to expect across both dirs?
   */
  private int expectedMarkers;

  /**
   * How many markers to expect including the base directory?
   */
  private int expectedMarkersWithBaseDir;


  @Test
  public void testCleanMarkersLegacyDir() throws Throwable {
    describe("Clean markers under a deleting FS -expect none");
    CreatedPaths createdPaths = createPaths(getDeletingFS(), methodPath());
    markerTool(getDeletingFS(), createdPaths.base, false, 0);
    markerTool(getDeletingFS(), createdPaths.base, true, 0);
  }

  @Test
  public void testCleanMarkersFileLimit() throws Throwable {
    describe("Clean markers under a keeping FS -with file limit");
    CreatedPaths createdPaths = createPaths(getKeepingFS(), methodPath());

    // audit will be interrupted
    markerTool(EXIT_INTERRUPTED, getDeletingFS(),
        createdPaths.base, false, 0, 1, false);
  }

  @Test
  public void testCleanMarkersKeepingDir() throws Throwable {
    describe("Audit then clean markers under a deleting FS "
        + "-expect markers to be found and then cleaned up");
    CreatedPaths createdPaths = createPaths(getKeepingFS(), methodPath());

    // audit will find the expected entries
    int expectedMarkerCount = createdPaths.dirs.size();
    S3AFileSystem fs = getDeletingFS();
    LOG.info("Auditing a directory with retained markers -expect failure");
    markerTool(EXIT_NOT_ACCEPTABLE, fs,
        createdPaths.base, false, 0, UNLIMITED_LISTING, false);

    LOG.info("Auditing a directory expecting retained markers");
    markerTool(fs, createdPaths.base, false,
        expectedMarkerCount);

    // we require that a purge didn't take place, so run the
    // audit again.
    LOG.info("Auditing a directory expecting retained markers");
    markerTool(fs, createdPaths.base, false,
        expectedMarkerCount);

    LOG.info("Purging a directory of retained markers");
    // purge cleans up
    assertMarkersDeleted(expectedMarkerCount,
        markerTool(fs, createdPaths.base, true, expectedMarkerCount));
    // and a rerun doesn't find markers
    LOG.info("Auditing a directory with retained markers -expect success");
    assertMarkersDeleted(0,
        markerTool(fs, createdPaths.base, true, 0));
  }

  @Test
  public void testRenameKeepingFS() throws Throwable {
    describe("Rename with the keeping FS -verify that no markers"
        + " exist at far end");
    Path base = methodPath();
    Path source = new Path(base, "source");
    Path dest = new Path(base, "dest");

    S3AFileSystem fs = getKeepingFS();
    CreatedPaths createdPaths = createPaths(fs, source);

    // audit will find three entries
    int expectedMarkerCount = createdPaths.dirs.size();

    markerTool(fs, source, false, expectedMarkerCount);
    fs.rename(source, dest);
    assertIsDirectory(dest);

    // there are no markers
    markerTool(fs, dest, false, 0);
    LOG.info("Auditing destination paths");
    verifyRenamed(dest, createdPaths);
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
    // line up for close in teardown
    setMixedFS(mixedFSDir2);
    // some of these paths will retain markers, some will not
    CreatedPaths createdPaths = createPaths(mixedFSDir2, source);

    // markers are only under dir2
    markerTool(mixedFSDir2, toPath(source, "dir1"), false, 0);
    markerTool(mixedFSDir2, source, false, expectedMarkersUnderDir2);

    // full scan of source will fail
    markerTool(EXIT_NOT_ACCEPTABLE,
        mixedFSDir2, source, false, 0, 0, false);

    // but add the -nonauth option and the markers under dir2 are skipped
    markerTool(0, mixedFSDir2, source, false, 0, 0, true);

    // if we now rename, all will be good
    LOG.info("Executing rename");
    mixedFSDir2.rename(source, dest);
    assertIsDirectory(dest);

    // there are no markers
    MarkerTool.ScanResult scanResult = markerTool(mixedFSDir2, dest, false, 0);
    // there are exactly the files we want
    Assertions.assertThat(scanResult)
        .describedAs("Scan result %s", scanResult)
        .extracting(s -> s.getTracker().getFilesFound())
        .isEqualTo(expectedFileCount);
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

  /**
   * Marker tool with no args.
   */
  @Test
  public void testRunNoArgs() throws Throwable {
    runToFailure(EXIT_USAGE, MARKERS);
  }

  @Test
  public void testRunWrongBucket() throws Throwable {
    runToFailure(EXIT_NOT_FOUND, MARKERS,
        AUDIT,
        "s3a://this-bucket-does-not-exist-hopefully");
  }

  /**
   * Run with a path that doesn't exist.
   */
  @Test
  public void testRunUnknownPath() throws Throwable {
    runToFailure(EXIT_NOT_FOUND, MARKERS,
        AUDIT,
        methodPath());
  }

  /**
   * Having both -audit and -clean on the command line is an error.
   */
  @Test
  public void testRunTooManyActions() throws Throwable {
    runToFailure(EXIT_USAGE, MARKERS,
        AUDIT, CLEAN,
        methodPath());
  }

  @Test
  public void testRunAuditWithExpectedMarkers() throws Throwable {
    describe("Run a verbose audit expecting some markers");
    // a run under the keeping FS will create paths
    CreatedPaths createdPaths = createPaths(getKeepingFS(), methodPath());
    final File audit = tempAuditFile();
    run(MARKERS, V,
        AUDIT,
        m(OPT_LIMIT), 0,
        m(OPT_OUT), audit,
        m(OPT_MIN), expectedMarkersWithBaseDir - 1,
        m(OPT_MAX), expectedMarkersWithBaseDir + 1,
        createdPaths.base);
    expectMarkersInOutput(audit, expectedMarkersWithBaseDir);
  }

  @Test
  public void testRunAuditWithExpectedMarkersSwappedMinMax() throws Throwable {
    describe("Run a verbose audit with the min/max ranges swapped;"
        + " see HADOOP-17332");
    // a run under the keeping FS will create paths
    CreatedPaths createdPaths = createPaths(getKeepingFS(), methodPath());
    final File audit = tempAuditFile();
    run(MARKERS, V,
        AUDIT,
        m(OPT_LIMIT), 0,
        m(OPT_OUT), audit,
        m(OPT_MIN), expectedMarkersWithBaseDir + 1,
        m(OPT_MAX), expectedMarkersWithBaseDir - 1,
        createdPaths.base);
    expectMarkersInOutput(audit, expectedMarkersWithBaseDir);
  }

  @Test
  public void testRunAuditWithExcessMarkers() throws Throwable {
    describe("Run a verbose audit failing as surplus markers were found");
    // a run under the keeping FS will create paths
    CreatedPaths createdPaths = createPaths(getKeepingFS(), methodPath());
    final File audit = tempAuditFile();
    runToFailure(EXIT_NOT_ACCEPTABLE, MARKERS, V,
        AUDIT,
        m(OPT_OUT), audit,
        createdPaths.base);
    expectMarkersInOutput(audit, expectedMarkersWithBaseDir);
  }

  @Test
  public void testRunLimitedAudit() throws Throwable {
    describe("Audit with a limited number of files (2)");
    CreatedPaths createdPaths = createPaths(getKeepingFS(), methodPath());
    runToFailure(EXIT_INTERRUPTED,
        MARKERS, V,
        m(OPT_LIMIT), 2,
        CLEAN,
        createdPaths.base);
  }

  /**
   * Run an audit against the landsat bucket.
   * <p></p>
   * This tests paging/scale against a larger bucket without
   * worrying about setup costs.
   */
  @Test
  public void testRunLimitedLandsatAudit() throws Throwable {
    describe("Audit a few thousand landsat objects");
    final File audit = tempAuditFile();

    runToFailure(EXIT_INTERRUPTED,
        MARKERS,
        AUDIT,
        m(OPT_LIMIT), 3000,
        m(OPT_OUT), audit,
        LANDSAT_BUCKET);
    readOutput(audit);
  }

  @Test
  public void testBucketInfoKeepingOnDeleting() throws Throwable {
    describe("Run bucket info with the keeping config on the deleting fs");
    runS3GuardCommandToFailure(uncachedFSConfig(getDeletingFS()),
        EXIT_NOT_ACCEPTABLE,
        BUCKET_INFO,
        m(MARKERS), DIRECTORY_MARKER_POLICY_KEEP,
        methodPath());
  }

  @Test
  public void testBucketInfoKeepingOnKeeping() throws Throwable {
    describe("Run bucket info with the keeping config on the keeping fs");
    runS3GuardCommand(uncachedFSConfig(getKeepingFS()),
        BUCKET_INFO,
        m(MARKERS), DIRECTORY_MARKER_POLICY_KEEP,
        methodPath());
  }

  @Test
  public void testBucketInfoDeletingOnDeleting() throws Throwable {
    describe("Run bucket info with the deleting config on the deleting fs");
    runS3GuardCommand(uncachedFSConfig(getDeletingFS()),
        BUCKET_INFO,
        m(MARKERS), DIRECTORY_MARKER_POLICY_DELETE,
        methodPath());
  }

  @Test
  public void testBucketInfoAuthOnAuth() throws Throwable {
    describe("Run bucket info with the auth FS");
    Path base = methodPath();

    S3AFileSystem authFS = createFS(DIRECTORY_MARKER_POLICY_AUTHORITATIVE,
        base.toUri().toString());
    // line up for close in teardown
    setMixedFS(authFS);
    runS3GuardCommand(uncachedFSConfig(authFS),
        BUCKET_INFO,
        m(MARKERS), DIRECTORY_MARKER_POLICY_AUTHORITATIVE,
        methodPath());
  }

  /**
   * Tracker of created paths.
   */
  private static final class CreatedPaths {

    private final FileSystem fs;

    private final Path base;

    private List<Path> files = new ArrayList<>();

    private List<Path> dirs = new ArrayList<>();

    private List<Path> emptyDirs = new ArrayList<>();

    private List<String> filesUnderBase = new ArrayList<>();

    private List<String> dirsUnderBase = new ArrayList<>();

    private List<String> emptyDirsUnderBase = new ArrayList<>();

    /**
     * Constructor.
     * @param fs filesystem.
     * @param base base directory for all creation operations.
     */
    private CreatedPaths(final FileSystem fs,
        final Path base) {
      this.fs = fs;
      this.base = base;
    }

    /**
     * Make a set of directories.
     * @param names varargs list of paths under the base.
     * @return number of entries created.
     * @throws IOException failure
     */
    private int dirs(String... names) throws IOException {
      for (String name : names) {
        mkdir(name);
      }
      return names.length;
    }

    /**
     * Create a single directory under the base.
     * @param name name/relative names of the directory
     * @return the path of the new entry.
     */
    private Path mkdir(String name) throws IOException {
      Path dir = toPath(base, name);
      fs.mkdirs(dir);
      dirs.add(dir);
      dirsUnderBase.add(name);
      return dir;
    }

    /**
     * Make a set of empty directories.
     * @param names varargs list of paths under the base.
     * @return number of entries created.
     * @throws IOException failure
     */
    private int emptydirs(String... names) throws IOException {
      for (String name : names) {
        emptydir(name);
      }
      return names.length;
    }

    /**
     * Create an empty directory.
     * @param name name under the base dir
     * @return the path
     * @throws IOException failure
     */
    private Path emptydir(String name) throws IOException {
      Path dir = toPath(base, name);
      fs.mkdirs(dir);
      emptyDirs.add(dir);
      emptyDirsUnderBase.add(name);
      return dir;
    }

    /**
     * Make a set of files.
     * @param names varargs list of paths under the base.
     * @return number of entries created.
     * @throws IOException failure
     */
    private int files(String... names) throws IOException {
      for (String name : names) {
        mkfile(name);
      }
      return names.length;
    }

    /**
     * Create a 0-byte file.
     * @param name name under the base dir
     * @return the path
     * @throws IOException failure
     */
    private Path mkfile(String name)
        throws IOException {
      Path file = toPath(base, name);
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
    CreatedPaths r = new CreatedPaths(fs, base);
    // the directories under which we will create files,
    // so expect to have markers
    r.mkdir("");

    // create the empty dirs
    r.emptydir("empty");

    // dir 1 has a file underneath
    r.mkdir("dir1");
    expectedFileCount = r.files("dir1/file1");

    expectedMarkersUnderDir1 = 1;


    // dir2 has a subdir
    r.dirs("dir2", "dir2/dir3");
    // an empty subdir
    r.emptydir("dir2/empty2");

    // and a file under itself and dir3
    expectedFileCount += r.files(
        "dir2/file2",
        "dir2/dir3/file3");


    // wrap up the expectations.
    expectedMarkersUnderDir2 = 2;
    expectedMarkers = expectedMarkersUnderDir1 + expectedMarkersUnderDir2;
    expectedMarkersWithBaseDir = expectedMarkers + 1;
    return r;
  }

  /**
   * Verify that all the paths renamed from the source exist
   * under the destination, including all empty directories.
   * @param dest destination to look under.
   * @param createdPaths list of created paths.
   */
  void verifyRenamed(final Path dest,
      final CreatedPaths createdPaths) throws IOException {
    // all leaf directories exist
    for (String p : createdPaths.emptyDirsUnderBase) {
      assertIsDirectory(toPath(dest, p));
    }
    // non-empty dirs
    for (String p : createdPaths.dirsUnderBase) {
      assertIsDirectory(toPath(dest, p));
    }
    // all files exist
    for (String p : createdPaths.filesUnderBase) {
      assertIsFile(toPath(dest, p));
    }
  }

}
