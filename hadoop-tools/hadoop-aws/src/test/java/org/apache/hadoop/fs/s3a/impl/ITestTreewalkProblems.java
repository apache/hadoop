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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.MultipartUpload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocatedFileStatusFetcher;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.util.DistCpTestUtils;

import static org.apache.hadoop.fs.CommonPathCapabilities.DIRECTORY_LISTING_INCONSISTENT;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertHasPathCapabilities;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.contract.ContractTestUtils.treeWalk;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_OPERATIONS_PURGE_UPLOADS;
import static org.apache.hadoop.fs.s3a.MultipartTestUtils.assertNoUploadsAt;
import static org.apache.hadoop.fs.s3a.MultipartTestUtils.clearAnyUploads;
import static org.apache.hadoop.fs.s3a.MultipartTestUtils.createMagicFile;
import static org.apache.hadoop.fs.s3a.MultipartTestUtils.magicPath;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.toPathList;
import static org.apache.hadoop.fs.s3a.S3AUtils.HIDDEN_FILE_FILTER;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.MAGIC_COMMITTER_ENABLED;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.LIST_STATUS_NUM_THREADS;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.util.ToolRunner.run;
import static org.apache.hadoop.util.functional.RemoteIterators.foreach;
import static org.apache.hadoop.util.functional.RemoteIterators.remoteIteratorFromArray;
import static org.apache.hadoop.util.functional.RemoteIterators.toList;

/**
 * Test behavior of treewalking when there are pending
 * uploads. All commands MUST work.
 * Currently, the only one which doesn't is distcp;
 * some tests do have different assertions about directories
 * found.
 */
public class ITestTreewalkProblems extends AbstractS3ACostTest {

  /**
   * Exit code to expect on a shell failure.
   */
  public static final int SHELL_FAILURE = 1;

  /**
   * Are directory listings potentially inconsistent?
   */
  private boolean listingInconsistent;

  @Override
  public Configuration createConfiguration() {
    final Configuration conf = super.createConfiguration();
    removeBaseAndBucketOverrides(conf,
        DIRECTORY_OPERATIONS_PURGE_UPLOADS,
        MAGIC_COMMITTER_ENABLED);
    conf.setBoolean(DIRECTORY_OPERATIONS_PURGE_UPLOADS, true);
    conf.setBoolean(MAGIC_COMMITTER_ENABLED, true);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    final S3AFileSystem fs = getFileSystem();
    final Path path = methodPath();
    assertHasPathCapabilities(fs, path, DIRECTORY_OPERATIONS_PURGE_UPLOADS);
    listingInconsistent = fs.hasPathCapability(path, DIRECTORY_LISTING_INCONSISTENT);
    clearAnyUploads(fs, path);
  }

  @Test
  public void testListFilesDeep() throws Throwable {
    final S3AFileSystem fs = getFileSystem();
    final Path src = createDirWithUpload();

    LOG.info("listFiles({}, true)", src);
    foreach(fs.listFiles(src, true), e -> LOG.info("{}", e));

    LOG.info("listFiles({}, true)", src);
    foreach(fs.listLocatedStatus(src), e -> LOG.info("{}", e));

    // and just verify a cleanup works
    Assertions.assertThat(fs.getS3AInternals().abortMultipartUploads(src))
        .describedAs("Aborted uploads under %s", src)
        .isEqualTo(1);
    assertNoUploadsAt(fs, src);
  }

  /**
   * Create a directory methodPath()/src with a magic upload underneath,
   * with the upload pointing at {@code src/subdir/file.txt}.
   * @return the directory created
   * @throws IOException creation problems
   */
  private Path createDirWithUpload() throws IOException {
    final S3AFileSystem fs = getFileSystem();
    final Path src = new Path(methodPath(), "src");
    // create a magic file.
    createMagicFile(fs, src);
    fs.delete(magicPath(src), true);
    return src;
  }

  @Test
  public void testLocatedFileStatusFetcher() throws Throwable {
    describe("Validate mapreduce LocatedFileStatusFetcher");

    final Path src = createDirWithUpload();

    Configuration listConfig = new Configuration(getConfiguration());
    listConfig.setInt(LIST_STATUS_NUM_THREADS, 2);

    LocatedFileStatusFetcher fetcher =
        new LocatedFileStatusFetcher(listConfig, new Path[]{src}, true, HIDDEN_FILE_FILTER, true);
    Assertions.assertThat(fetcher.getFileStatuses()).hasSize(0);
  }

  @Test
  public void testGetContentSummaryDirsAndFiles() throws Throwable {
    describe("FileSystem.getContentSummary()");
    final S3AFileSystem fs = getFileSystem();
    final Path src = createDirWithUpload();
    fs.mkdirs(new Path(src, "child"));
    final Path path = methodPath();
    file(new Path(path, "file"));
    final int dirs = listingInconsistent ? 3 : 3;
    assertContentSummary(path, dirs, 1);
  }

  /**
   * Execute getContentSummary() down a directory tree which only
   * contains a single real directory.
   * This test case has been a bit inconsistent between different store
   * types.
   */
  @Test
  public void testGetContentSummaryPendingDir() throws Throwable {
    describe("FileSystem.getContentSummary() with pending dir");
    assertContentSummary(createDirWithUpload(), 1, 0);
  }

  /**
   * Make an assertions about the content summary of a path.
   * @param path path to scan
   * @param dirs number of directories to find.
   * @param files number of files to find
   * @throws IOException scanning problems
   */
  private void assertContentSummary(
      final Path path,
      final int dirs,
      final int files) throws IOException {
    ContentSummary summary = getFileSystem().getContentSummary(path);
    Assertions.assertThat(summary.getDirectoryCount())
        .describedAs("dir count under %s of %s", path, summary)
        .isEqualTo(dirs);
    Assertions.assertThat(summary.getFileCount())
        .describedAs("filecount count under %s of %s", path, summary)
        .isEqualTo(files);
  }

  /**
   * Execute getContentSummary() down a directory tree which only
   * contains a single real directory.
   */
  @Test
  public void testGetContentSummaryFiles() throws Throwable {
    describe("FileSystem.getContentSummary()");
    final S3AFileSystem fs = getFileSystem();
    final Path src = createDirWithUpload();
    fs.mkdirs(new Path(src, "child"));
    final Path base = methodPath();
    touch(fs, new Path(base, "file"));
    assertContentSummary(base, 3, 1);
  }

  /**
   * Test all the various filesystem.list* calls.
   * Bundled into one test case to reduce setup/teardown overhead.
   */
  @Test
  public void testListStatusOperations() throws Throwable {
    describe("FileSystem liststtus calls");
    final S3AFileSystem fs = getFileSystem();
    final Path base = methodPath();
    final Path src = createDirWithUpload();
    final Path file = new Path(base, "file");
    final Path dir2 = new Path(base, "dir2");
    // path in a child dir
    final Path childfile = new Path(dir2, "childfile");
    file(childfile);
    file(file);
    fs.mkdirs(dir2);

    Assertions.assertThat(toPathList(fs.listStatusIterator(base)))
        .describedAs("listStatusIterator(%s)", base)
        .contains(src, dir2, file);

    Assertions.assertThat(toPathList(remoteIteratorFromArray(fs.listStatus(base))))
        .describedAs("listStatusIterator(%s, false)", false)
        .contains(src, dir2, file);

    Assertions.assertThat(toPathList(fs.listFiles(base, false)))
        .describedAs("listfiles(%s, false)", false)
        .containsExactly(file);

    Assertions.assertThat(toPathList(fs.listFiles(base, true)))
        .describedAs("listfiles(%s, true)", false)
        .containsExactlyInAnyOrder(file, childfile);

    Assertions.assertThat(toPathList(fs.listLocatedStatus(base, (p) -> true)))
        .describedAs("listLocatedStatus(%s, true)", false)
        .contains(src, dir2, file);
    Assertions.assertThat(toPathList(fs.listLocatedStatus(base)))
        .describedAs("listLocatedStatus(%s, true)", false)
        .contains(src, dir2, file);
  }

  @Test
  public void testShellList() throws Throwable {
    describe("Validate hadoop fs -ls sorted and unsorted");
    final S3AFileSystem fs = getFileSystem();
    final Path base = methodPath();
    createDirWithUpload();
    fs.mkdirs(new Path(base, "dir2"));
    // recursive not sorted
    shell(base, "-ls", "-R", base.toUri().toString());

    // recursive sorted
    shell(base, "-ls", "-R", "-S", base.toUri().toString());
  }

  @Test
  public void testShellDu() throws Throwable {
    describe("Validate hadoop fs -du");
    final Path base = methodPath();
    createDirWithUpload();

    shell(base, "-du", base.toUri().toString());
  }

  @Test
  public void testShellDf() throws Throwable {
    describe("Validate hadoop fs -df");
    final Path base = methodPath();

    final String p = base.toUri().toString();
    shell(SHELL_FAILURE, base, "-df", p);
    createDirWithUpload();

    shell(base, "-df", p);
  }

  @Test
  public void testShellFind() throws Throwable {
    describe("Validate hadoop fs -ls -R");
    final Path base = methodPath();
    final String p = base.toUri().toString();
    shell(SHELL_FAILURE, base, "-find", p, "-print");
    createDirWithUpload();
    shell(base, "-find", p, "-print");
  }

  @Test
  public void testDistCp() throws Throwable {
    describe("Validate distcp");
    final S3AFileSystem fs = getFileSystem();
    final Path base = methodPath();
    final Path src = createDirWithUpload();
    final Path dest = new Path(base, "dest");
    file(new Path(src, "real-file"));
    final String options = "-useiterator -update -delete -direct";
    if (!fs.hasPathCapability(base, DIRECTORY_LISTING_INCONSISTENT)) {
      DistCpTestUtils.assertRunDistCp(DistCpConstants.SUCCESS, src.toString(), dest.toString(),
          options, getConfiguration());
    } else {
      // distcp fails if uploads are visible
      intercept(org.junit.ComparisonFailure.class, () -> {
        DistCpTestUtils.assertRunDistCp(DistCpConstants.SUCCESS, src.toString(), dest.toString(),
            options, getConfiguration());
      });
    }

  }

  @Test
  public void testDistCpNoIterator() throws Throwable {
    describe("Validate distcp");
    final S3AFileSystem fs = getFileSystem();
    final Path base = methodPath();
    final Path src = createDirWithUpload();
    final Path dest = new Path(base, "dest");
    file(new Path(src, "real-file"));

    final String options = "-update -delete -direct";
    if (!fs.hasPathCapability(base, DIRECTORY_LISTING_INCONSISTENT)) {
      DistCpTestUtils.assertRunDistCp(DistCpConstants.SUCCESS, src.toString(), dest.toString(),
          options, getConfiguration());
    } else {
      // distcp fails if uploads are visible
      intercept(org.junit.ComparisonFailure.class, () -> {
        DistCpTestUtils.assertRunDistCp(DistCpConstants.SUCCESS, src.toString(), dest.toString(),
            options, getConfiguration());
      });
    }

  }

  /**
   * CTU is also doing treewalking, though it's test only.
   */
  @Test
  public void testContractTestUtilsTreewalk() throws Throwable {
    final S3AFileSystem fs = getFileSystem();
    final Path base = methodPath();
    createDirWithUpload();
    final ContractTestUtils.TreeScanResults treeWalk = treeWalk(fs, base);

    ContractTestUtils.TreeScanResults listing =
        new ContractTestUtils.TreeScanResults(fs.listFiles(base, true));
    treeWalk.assertFieldsEquivalent("treewalk vs listFiles(/, true)", listing, treeWalk.getFiles(),
        listing.getFiles());
  }

  /**
   * Globber is already resilient to missing directories; a relic
   * of the time when HEAD requests on s3 objects could leave the
   * 404 in S3 front end cache.
   */
  @Test
  public void testGlobberTreewalk() throws Throwable {
    final S3AFileSystem fs = getFileSystem();
    final Path base = methodPath();
    final Path src = createDirWithUpload();
    // this is the pending dir
    final Path subdir = new Path(src, "subdir/");
    final Path dest = new Path(base, "dest");
    final Path monday = new Path(dest, "day=monday");
    final Path realFile = file(new Path(monday, "real-file.parquet"));
    assertGlob(fs, new Path(base, "*/*/*.parquet"), realFile);
    if (listingInconsistent) {
      assertGlob(fs, new Path(base, "*"), src, dest);
      assertGlob(fs, new Path(base, "*/*"), subdir, monday);
    } else {
      assertGlob(fs, new Path(base, "*"), src, dest);
      assertGlob(fs, new Path(base, "*/*"), monday);
    }
  }

  private static void assertGlob(final S3AFileSystem fs,
      final Path pattern,
      final Path... expected)
      throws IOException {
    final FileStatus[] globbed = fs.globStatus(pattern,
        (f) -> true);
    final List<Path> paths = Arrays.stream(globbed).map(s -> s.getPath())
        .collect(Collectors.toList());
    Assertions.assertThat(paths)
        .describedAs("glob(%s)", pattern)
        .containsExactlyInAnyOrder(expected);
  }

  @Test
  public void testFileInputFormatSplits() throws Throwable {
    final S3AFileSystem fs = getFileSystem();
    final Path base = methodPath();
    final Path src = createDirWithUpload();
    final Path dest = new Path(base, "dest");
    final Path monday = new Path(dest, "day=monday");
    final int count = 4;
    List<Path> files = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      files.add(file(new Path(monday, "file-0" + i + ".parquet")));
    }
    final JobContextImpl jobContext = new JobContextImpl(getConfiguration(), new JobID("job", 1));
    final JobConf jc = (JobConf) jobContext.getConfiguration();
    jc.set("mapreduce.input.fileinputformat.inputdir", base.toUri().toString());
    jc.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
    final TextInputFormat inputFormat = new TextInputFormat();
    final List<Path> paths = inputFormat.getSplits(jobContext).stream().map(s ->
            ((FileSplit) s).getPath())
        .collect(Collectors.toList());

    Assertions.assertThat(paths)
        .describedAs("input split of base directory")
        .containsExactlyInAnyOrderElementsOf(files);


  }

  /**
   * Exec a shell command; require it to succeed.
   * @param base base dir
   * @param command command sequence
   * @throws Exception failure
   */

  private void shell(final Path base, final String... command) throws Exception {
    shell(0, base, command);
  }

  /**
   * Exec a shell command; require the result to match the expected outcome.
   * @param expected expected outcome
   * @param base base dir
   * @param command command sequence
   * @throws Exception failure
   */

  private void shell(int expected, final Path base, final String... command) throws Exception {
    Assertions.assertThat(run(getConfiguration(), new FsShell(), command))
        .describedAs("%s %s", command[0], base)
        .isEqualTo(expected);
  }

  /**
   * Assert the upload count under a dir is the expected value.
   * Failure message will include the list of entries.
   * @param dir dir
   * @param expected expected count
   * @throws IOException listing problem
   */
  private void assertUploadCount(final Path dir, final int expected) throws IOException {
    Assertions.assertThat(toList(listUploads(dir)))
        .describedAs("uploads under %s", dir)
        .hasSize(expected);
  }

  /**
   * List uploads; use the same APIs that the directory operations use,
   * so implicitly validating them.
   * @param dir directory to list
   * @return full list of entries
   * @throws IOException listing problem
   */
  private RemoteIterator<MultipartUpload> listUploads(Path dir) throws IOException {
    final S3AFileSystem fs = getFileSystem();
    try (AuditSpan ignored = span()) {
      final StoreContext sc = fs.createStoreContext();
      return fs.listUploadsUnderPrefix(sc, sc.pathToKey(dir));
    }
  }
}
