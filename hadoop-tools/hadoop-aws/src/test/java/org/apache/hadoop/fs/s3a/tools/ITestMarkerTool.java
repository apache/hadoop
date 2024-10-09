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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.test.PublicDatasetTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_KEEP;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.BucketInfo.BUCKET_INFO;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardToolTestHelper.runS3GuardCommand;
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
  public void testCleanMarkersFileLimit() throws Throwable {
    describe("Clean markers under a keeping FS -with file limit");
    CreatedPaths createdPaths = createPaths(getFileSystem(), methodPath());

    // audit will be interrupted
    markerTool(EXIT_INTERRUPTED, getFileSystem(),
        createdPaths.base, false, 0, 1, false);
  }

  @Test
  public void testRenameKeepingFS() throws Throwable {
    describe("Rename with the keeping FS -verify that no markers"
        + " exist at far end");
    Path base = methodPath();
    Path source = new Path(base, "source");
    Path dest = new Path(base, "dest");

    S3AFileSystem fs = getFileSystem();
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
    CreatedPaths createdPaths = createPaths(getFileSystem(), methodPath());
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
    CreatedPaths createdPaths = createPaths(getFileSystem(), methodPath());
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
    CreatedPaths createdPaths = createPaths(getFileSystem(), methodPath());
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
    CreatedPaths createdPaths = createPaths(getFileSystem(), methodPath());
    runToFailure(EXIT_INTERRUPTED,
        MARKERS, V,
        m(OPT_LIMIT), 2,
        CLEAN,
        createdPaths.base);
  }

  /**
   * Run an audit against a bucket with a large number of objects.
   * <p></p>
   * This tests paging/scale against a larger bucket without
   * worrying about setup costs.
   */
  @Test
  public void testRunAuditManyObjectsInBucket() throws Throwable {
    describe("Audit a few thousand objects");
    final File audit = tempAuditFile();

    Configuration conf = super.createConfiguration();
    String bucketUri = PublicDatasetTestUtils.getBucketPrefixWithManyObjects(conf);

    runToFailure(EXIT_INTERRUPTED,
        MARKERS,
        AUDIT,
        m(OPT_LIMIT), 3000,
        m(OPT_OUT), audit,
        bucketUri);
    readOutput(audit);
  }

  @Test
  public void testBucketInfoKeepingOnKeeping() throws Throwable {
    describe("Run bucket info with the keeping config on the keeping fs");
    runS3GuardCommand(uncachedFSConfig(getFileSystem()),
        BUCKET_INFO,
        m(MARKERS), DIRECTORY_MARKER_POLICY_KEEP,
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
