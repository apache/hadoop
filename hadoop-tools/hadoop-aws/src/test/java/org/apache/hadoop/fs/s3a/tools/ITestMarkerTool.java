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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.StringUtils;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBucketOverrides;
import static org.apache.hadoop.fs.s3a.tools.MarkerTool.*;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_INTERRUPTED;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_ACCEPTABLE;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_FOUND;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_USAGE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test the marker tool and use it to compare the behavior
 * of keeping vs legacy S3A FS instances.
 */
public class ITestMarkerTool extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestMarkerTool.class);

  /** the -verbose option. */
  private static final String V = m(VERBOSE);

  /** FS which keeps markers. */
  private S3AFileSystem keepingFS;

  /** FS which deletes markers. */
  private S3AFileSystem deletingFS;

  /** FS which mixes markers; only created in some tests. */
  private S3AFileSystem mixedFS;

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
   * How many markers to expect across both dirs
   */
  private int expectedMarkers;

  /**
   * How many markers to expect including the base directory
   */
  private int expectedMarkersWithBaseDir;

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
    markerTool(mixedFSDir2, topath(source, "dir1"), false, 0);
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

  @Test
  public void testRunNoArgs() throws Throwable {
    runToFailure(EXIT_USAGE, NAME);
  }

  @Test
  public void testRunWrongBucket() throws Throwable {
    runToFailure(EXIT_NOT_FOUND, NAME,
        AUDIT,
        "s3a://this-bucket-does-not-exist-hopefully");
  }

  /**
   * Run with a path that doesn't exist.
   */
  @Test
  public void testRunUnknownPath() throws Throwable {
    runToFailure(EXIT_NOT_FOUND, NAME,
        AUDIT,
        methodPath());
  }

  /**
   * Having both -audit and -clean on the command line is an error.
   */
  @Test
  public void testRunTooManyActions() throws Throwable {
    runToFailure(EXIT_USAGE, NAME,
        AUDIT, CLEAN,
        methodPath());
  }

  @Test
  public void testRunAuditWithExpectedMarkers() throws Throwable {
    describe("Run a verbose audit expecting some markers");
    // a run under the keeping FS will create paths
    CreatedPaths createdPaths = createPaths(getKeepingFS(), methodPath());
    final File audit = tempAuditFile();
    run(NAME, V,
        AUDIT,
        m(OPT_LIMIT), 0,
        m(OPT_OUT), audit,
        m(OPT_EXPECTED), expectedMarkersWithBaseDir,
        createdPaths.base);
    expectMarkersInOutput(audit, expectedMarkersWithBaseDir);
  }

  @Test
  public void testRunAuditWithExcessMarkers() throws Throwable {
    describe("Run a verbose audit failing as surplus markers were found");
    // a run under the keeping FS will create paths
    CreatedPaths createdPaths = createPaths(getKeepingFS(), methodPath());
    final File audit = tempAuditFile();
    runToFailure(EXIT_NOT_ACCEPTABLE, NAME, V,
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
        NAME, V,
        m(OPT_LIMIT), 2,
        CLEAN,
        createdPaths.base);
    run(NAME, V,
        AUDIT,
        createdPaths.base);
  }

  /**
   * Get a filename for a temp file.
   * The generated file is deleted.
   *
   * @return a file path for a output file
   */
  private File tempAuditFile() throws IOException {
    final File audit = File.createTempFile("audit", ".txt");
    audit.delete();
    return audit;
  }

  /**
   * Read the audit output and verify it has the expected number of lines.
   * @param auditFile audit file to read
   * @param expected expected line count
   */
  private void expectMarkersInOutput(final File auditFile,
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
  public List<String> readOutput(final File outputFile)
      throws IOException {
    try (FileReader reader = new FileReader(outputFile)) {
      final List<String> lines =
          org.apache.commons.io.IOUtils.readLines(reader);

      LOG.info("contents of output file {}\n{}", outputFile,
          StringUtils.join("\n", lines));
      return lines;
    }
  }

  private static Path topath(Path base, final String name) {
    return name.isEmpty() ? base : new Path(base, name);
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
        markerPolicy, authPath);
    return fs2;
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
      Path dir = topath(base, name);
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
      Path dir = topath(base, name);
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
      Path file = topath(base, name);
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
   * Execute the marker tool, expecting the execution to succeed.
   * @param sourceFS filesystem to use
   * @param path path to scan
   * @param doPurge should markers be purged
   * @param expectedMarkerCount number of markers expected
   * @return the result
   */
  private MarkerTool.ScanResult markerTool(
      final FileSystem sourceFS,
      final Path path,
      final boolean doPurge,
      final int expectedMarkerCount)
      throws IOException {
    return markerTool(0, sourceFS, path, doPurge, expectedMarkerCount,
        UNLIMITED_LISTING, false);
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
        sourceFS,
        path,
        doPurge,
        expectedMarkers,
        limit, nonAuth);
    Assertions.assertThat(result.getExitCode())
        .describedAs("Exit code of marker(%s, %s, %d) -> %s",
            path, doPurge, expectedMarkers, result)
        .isEqualTo(exitCode);
    return result;
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
      assertIsDirectory(topath(dest, p));
    }
    // non-empty dirs
    for (String p : createdPaths.dirsUnderBase) {
      assertIsDirectory(topath(dest, p));
    }
    // all files exist
    for (String p : createdPaths.filesUnderBase) {
      assertIsFile(topath(dest, p));
    }
  }
  /**
   * Run a S3GuardTool command from a varags list and the
   * configuration returned by {@code getConfiguration()}.
   * @param args argument list
   * @return the return code
   * @throws Exception any exception
   */
  protected int run(Object... args) throws Exception {
    Configuration conf = new Configuration(getConfiguration());
    final String[] argList = Arrays.stream(args)
        .map(Object::toString)
        .collect(Collectors.toList()).toArray(new String[0]);
    disableFilesystemCaching(conf);
    return S3GuardTool.run(conf, argList);
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
    ExitUtil.ExitException ex =
        intercept(ExitUtil.ExitException.class,
            () -> {
              int ec = run(args);
              if (ec != 0) {
                throw new ExitUtil.ExitException(ec, "exit code " + ec);
              }
            });
    if (ex.status != status) {
      throw ex;
    }
  }

  /**
   * Add a - prefix to a string
   * @param s string to prefix
   * @return a string for passing into the CLI
   */
  private static String m(String s) {
    return "-" + s;
  }
}
