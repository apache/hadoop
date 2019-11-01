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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.impl.StatusProbeEnum;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.Statistic.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.test.GenericTestUtils.getTestDir;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Use metrics to assert about the cost of file status queries.
 * {@link S3AFileSystem#getFileStatus(Path)}.
 * Parameterized on guarded vs raw.
 */
@RunWith(Parameterized.class)
public class ITestS3AFileOperationCost extends AbstractS3ATestBase {

  private MetricDiff metadataRequests;
  private MetricDiff listRequests;

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AFileOperationCost.class);

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"raw", false},
        {"guarded", true}
    });
  }

  private final String name;

  private final boolean s3guard;

  public ITestS3AFileOperationCost(final String name, final boolean s3guard) {
    this.name = name;
    this.s3guard = s3guard;
  }

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    String bucketName = getTestBucketName(conf);
    removeBucketOverrides(bucketName, conf,
        S3_METADATA_STORE_IMPL);
    if (!s3guard) {
      // in a raw run remove all s3guard settings
      removeBaseAndBucketOverrides(bucketName, conf,
          S3_METADATA_STORE_IMPL);
    }
    disableFilesystemCaching(conf);
    return conf;
  }
  @Override
  public void setup() throws Exception {
    super.setup();
    if (s3guard) {
      // s3guard is required for those test runs where any of the
      // guard options are set
      assumeS3GuardState(true, getConfiguration());
    }
    S3AFileSystem fs = getFileSystem();
    metadataRequests = new MetricDiff(fs, OBJECT_METADATA_REQUESTS);
    listRequests = new MetricDiff(fs, OBJECT_LIST_REQUESTS);
    skipDuringFaultInjection(fs);
  }

  @Test
  public void testCostOfGetFileStatusOnFile() throws Throwable {
    describe("performing getFileStatus on a file");
    Path simpleFile = path("simple.txt");
    S3AFileSystem fs = getFileSystem();
    touch(fs, simpleFile);
    resetMetricDiffs();
    FileStatus status = fs.getFileStatus(simpleFile);
    assertTrue("not a file: " + status, status.isFile());
    if (!fs.hasMetadataStore()) {
      metadataRequests.assertDiffEquals(1);
    }
    listRequests.assertDiffEquals(0);
  }

  private void resetMetricDiffs() {
    reset(metadataRequests, listRequests);
  }

  /**
   * Verify that the head and list calls match expectations,
   * then reset the counters ready for the next operation.
   * @param head expected HEAD count
   * @param list expected LIST count
   */
  private void verifyOperationCount(int head, int list) {
    metadataRequests.assertDiffEquals(head);
    listRequests.assertDiffEquals(list);
    metadataRequests.reset();
    listRequests.reset();
  }

  @Test
  public void testCostOfGetFileStatusOnEmptyDir() throws Throwable {
    describe("performing getFileStatus on an empty directory");
    S3AFileSystem fs = getFileSystem();
    Path dir = path("empty");
    fs.mkdirs(dir);
    resetMetricDiffs();
    S3AFileStatus status = fs.innerGetFileStatus(dir, true,
        StatusProbeEnum.ALL);
    assertSame("not empty: " + status, Tristate.TRUE,
        status.isEmptyDirectory());

    if (!fs.hasMetadataStore()) {
      metadataRequests.assertDiffEquals(2);
    }
    listRequests.assertDiffEquals(0);

    // but now only ask for the directories and the file check is skipped.
    resetMetricDiffs();
    fs.innerGetFileStatus(dir, false,
        StatusProbeEnum.DIRECTORIES);
    if (!fs.hasMetadataStore()) {
      metadataRequests.assertDiffEquals(1);
    }
  }

  @Test
  public void testCostOfGetFileStatusOnMissingFile() throws Throwable {
    describe("performing getFileStatus on a missing file");
    S3AFileSystem fs = getFileSystem();
    Path path = path("missing");
    resetMetricDiffs();
    intercept(FileNotFoundException.class,
        () -> fs.getFileStatus(path));
    metadataRequests.assertDiffEquals(2);
    listRequests.assertDiffEquals(1);
  }

  @Test
  public void testCostOfGetFileStatusOnMissingSubPath() throws Throwable {
    describe("performing getFileStatus on a missing file");
    S3AFileSystem fs = getFileSystem();
    Path path = path("missingdir/missingpath");
    resetMetricDiffs();
    intercept(FileNotFoundException.class,
        () -> fs.getFileStatus(path));
    metadataRequests.assertDiffEquals(2);
    listRequests.assertDiffEquals(1);
  }

  @Test
  public void testCostOfGetFileStatusOnNonEmptyDir() throws Throwable {
    describe("performing getFileStatus on a non-empty directory");
    S3AFileSystem fs = getFileSystem();
    Path dir = path("empty");
    fs.mkdirs(dir);
    Path simpleFile = new Path(dir, "simple.txt");
    touch(fs, simpleFile);
    resetMetricDiffs();
    S3AFileStatus status = fs.innerGetFileStatus(dir, true,
        StatusProbeEnum.ALL);
    if (status.isEmptyDirectory() == Tristate.TRUE) {
      // erroneous state
      String fsState = fs.toString();
      fail("FileStatus says directory isempty: " + status
          + "\n" + ContractTestUtils.ls(fs, dir)
          + "\n" + fsState);
    }
    if (!fs.hasMetadataStore()) {
      metadataRequests.assertDiffEquals(2);
      listRequests.assertDiffEquals(1);
    }
  }

  @Test
  public void testCostOfCopyFromLocalFile() throws Throwable {
    describe("testCostOfCopyFromLocalFile");
    File localTestDir = getTestDir("tmp");
    localTestDir.mkdirs();
    File tmpFile = File.createTempFile("tests3acost", ".txt",
        localTestDir);
    tmpFile.delete();
    try {
      URI localFileURI = tmpFile.toURI();
      FileSystem localFS = FileSystem.get(localFileURI,
          getFileSystem().getConf());
      Path localPath = new Path(localFileURI);
      int len = 10 * 1024;
      byte[] data = dataset(len, 'A', 'Z');
      writeDataset(localFS, localPath, data, len, 1024, true);
      S3AFileSystem s3a = getFileSystem();
      MetricDiff copyLocalOps = new MetricDiff(s3a,
          INVOCATION_COPY_FROM_LOCAL_FILE);
      MetricDiff putRequests = new MetricDiff(s3a,
          OBJECT_PUT_REQUESTS);
      MetricDiff putBytes = new MetricDiff(s3a,
          OBJECT_PUT_BYTES);

      Path remotePath = path("copied");
      s3a.copyFromLocalFile(false, true, localPath, remotePath);
      verifyFileContents(s3a, remotePath, data);
      copyLocalOps.assertDiffEquals(1);
      putRequests.assertDiffEquals(1);
      putBytes.assertDiffEquals(len);
      // print final stats
      LOG.info("Filesystem {}", s3a);
    } finally {
      tmpFile.delete();
    }
  }

  private boolean reset(MetricDiff... diffs) {
    for (MetricDiff diff : diffs) {
      diff.reset();
    }
    return true;
  }

  @Test
  public void testFakeDirectoryDeletion() throws Throwable {
    describe("Verify whether create file works after renaming a file. "
        + "In S3, rename deletes any fake directories as a part of "
        + "clean up activity");
    S3AFileSystem fs = getFileSystem();

    Path srcBaseDir = path("src");
    mkdirs(srcBaseDir);
    MetricDiff deleteRequests =
        new MetricDiff(fs, Statistic.OBJECT_DELETE_REQUESTS);
    MetricDiff directoriesDeleted =
        new MetricDiff(fs, Statistic.DIRECTORIES_DELETED);
    MetricDiff fakeDirectoriesDeleted =
        new MetricDiff(fs, Statistic.FAKE_DIRECTORIES_DELETED);
    MetricDiff directoriesCreated =
        new MetricDiff(fs, Statistic.DIRECTORIES_CREATED);

    // when you call toString() on this, you get the stats
    // so it gets auto-evaluated in log calls.
    Object summary = new Object() {
      @Override
      public String toString() {
        return String.format("[%s, %s, %s, %s]",
            directoriesCreated, directoriesDeleted,
            deleteRequests, fakeDirectoriesDeleted);
      }
    };

    // reset operation to invoke
    Callable<Boolean> reset = () ->
        reset(deleteRequests, directoriesCreated, directoriesDeleted,
          fakeDirectoriesDeleted);

    Path srcDir = new Path(srcBaseDir, "1/2/3/4/5/6");
    int srcDirDepth = directoriesInPath(srcDir);
    // one dir created, one removed
    mkdirs(srcDir);
    String state = "after mkdir(srcDir) " + summary;
    directoriesCreated.assertDiffEquals(state, 1);
    deleteRequests.assertDiffEquals(state, 1);
    directoriesDeleted.assertDiffEquals(state, 0);
    // HADOOP-14255 deletes unnecessary fake directory objects in mkdirs()
    fakeDirectoriesDeleted.assertDiffEquals(state, srcDirDepth - 1);
    reset.call();

    // creating a file should trigger demise of the src dir
    final Path srcFilePath = new Path(srcDir, "source.txt");
    touch(fs, srcFilePath);
    state = "after touch(fs, srcFilePath) " + summary;
    deleteRequests.assertDiffEquals(state, 1);
    directoriesCreated.assertDiffEquals(state, 0);
    directoriesDeleted.assertDiffEquals(state, 0);
    fakeDirectoriesDeleted.assertDiffEquals(state, srcDirDepth);

    reset.call();

    // create a directory tree, expect the dir to be created and
    // a request to delete all parent directories made.
    Path destBaseDir = path("dest");
    Path destDir = new Path(destBaseDir, "1/2/3/4/5/6");
    Path destFilePath = new Path(destDir, "dest.txt");
    mkdirs(destDir);
    state = "after mkdir(destDir) " + summary;

    int destDirDepth = directoriesInPath(destDir);
    directoriesCreated.assertDiffEquals(state, 1);
    deleteRequests.assertDiffEquals(state, 1);
    directoriesDeleted.assertDiffEquals(state, 0);
    fakeDirectoriesDeleted.assertDiffEquals(state, destDirDepth - 1);

    // create a new source file.
    // Explicitly use a new path object to guarantee that the parent paths
    // are different object instances
    final Path srcFile2 = new Path(srcDir.toUri() + "/source2.txt");
    touch(fs, srcFile2);

    reset.call();

    // rename the source file to the destination file.
    // this tests the file rename path, not the dir rename path
    // as srcFile2 exists, the parent dir of srcFilePath must not be created.
    fs.rename(srcFilePath, destFilePath);
    state = String.format("after rename(srcFilePath, destFilePath)"
            + " %s dest dir depth=%d",
        summary,
        destDirDepth);

    directoriesCreated.assertDiffEquals(state, 0);
    // one for the renamed file, one for the parent of the dest dir
    deleteRequests.assertDiffEquals(state, 2);
    directoriesDeleted.assertDiffEquals(state, 0);
    fakeDirectoriesDeleted.assertDiffEquals(state, destDirDepth);

    // these asserts come after the checks on iop counts, so they don't
    // interfere
    assertIsFile(destFilePath);
    assertIsDirectory(srcDir);
    assertPathDoesNotExist("should have gone in the rename", srcFilePath);
    reset.call();

    // rename the source file2 to the (no longer existing
    // this tests the file rename path, not the dir rename path
    // as srcFile2 exists, the parent dir of srcFilePath must not be created.
    fs.rename(srcFile2, srcFilePath);
    state = String.format("after rename(%s, %s) %s dest dir depth=%d",
        srcFile2, srcFilePath,
        summary,
        destDirDepth);

    // here we expect there to be no fake directories
    directoriesCreated.assertDiffEquals(state, 0);
    // one for the renamed file only
    deleteRequests.assertDiffEquals(state, 1);
    directoriesDeleted.assertDiffEquals(state, 0);
    fakeDirectoriesDeleted.assertDiffEquals(state, 0);
  }

  private int directoriesInPath(Path path) {
    return path.isRoot() ? 0 : 1 + directoriesInPath(path.getParent());
  }

  @Test
  public void testCostOfRootRename() throws Throwable {
    describe("assert that a root directory rename doesn't"
        + " do much in terms of parent dir operations");
    S3AFileSystem fs = getFileSystem();

    // unique name, so that even when run in parallel tests, there's no conflict
    String uuid = UUID.randomUUID().toString();
    Path src = new Path("/src-" + uuid);
    Path dest = new Path("/dest-" + uuid);

    try {
      MetricDiff deleteRequests =
          new MetricDiff(fs, Statistic.OBJECT_DELETE_REQUESTS);
      MetricDiff directoriesDeleted =
          new MetricDiff(fs, Statistic.DIRECTORIES_DELETED);
      MetricDiff fakeDirectoriesDeleted =
          new MetricDiff(fs, Statistic.FAKE_DIRECTORIES_DELETED);
      MetricDiff directoriesCreated =
          new MetricDiff(fs, Statistic.DIRECTORIES_CREATED);
      touch(fs, src);
      fs.rename(src, dest);
      Object summary = new Object() {
        @Override
        public String toString() {
          return String.format("[%s, %s, %s, %s]",
              directoriesCreated, directoriesDeleted,
              deleteRequests, fakeDirectoriesDeleted);
        }
      };

      String state = String.format("after touch(%s) %s",
          src, summary);
      touch(fs, src);
      fs.rename(src, dest);
      directoriesCreated.assertDiffEquals(state, 0);


      state = String.format("after rename(%s, %s) %s",
          src, dest, summary);
      // here we expect there to be no fake directories
      directoriesCreated.assertDiffEquals(state, 0);
      // one for the renamed file only
      deleteRequests.assertDiffEquals(state, 1);
      directoriesDeleted.assertDiffEquals(state, 0);
      fakeDirectoriesDeleted.assertDiffEquals(state, 0);

      // delete that destination file, assert only the file delete was issued
      reset(deleteRequests, directoriesCreated, directoriesDeleted,
          fakeDirectoriesDeleted);

      fs.delete(dest, false);
      // here we expect there to be no fake directories
      directoriesCreated.assertDiffEquals(state, 0);
      // one for the deleted file
      deleteRequests.assertDiffEquals(state, 1);
      directoriesDeleted.assertDiffEquals(state, 0);
      fakeDirectoriesDeleted.assertDiffEquals(state, 0);
    } finally {
      fs.delete(src, false);
      fs.delete(dest, false);
    }
  }

  @Test
  public void testDirProbes() throws Throwable {
    describe("Test directory probe cost -raw only");
    S3AFileSystem fs = getFileSystem();
    assume("Unguarded FS only", !fs.hasMetadataStore());
    String dir = "testEmptyDirHeadProbe";
    Path emptydir = path(dir);
    // Create the empty directory.
    fs.mkdirs(emptydir);

    // metrics and assertions.
    resetMetricDiffs();

    intercept(FileNotFoundException.class, () ->
        fs.innerGetFileStatus(emptydir, false,
            StatusProbeEnum.HEAD_ONLY));
    verifyOperationCount(1, 0);

    // a LIST will find it -but it doesn't consider it an empty dir.
    S3AFileStatus status = fs.innerGetFileStatus(emptydir, true,
        StatusProbeEnum.LIST_ONLY);
    verifyOperationCount(0, 1);
    Assertions.assertThat(status)
        .describedAs("LIST output is not considered empty")
        .matches(s -> !s.isEmptyDirectory().equals(Tristate.TRUE),  "is empty");

    // finally, skip all probes and expect no operations toThere are
    // take place
    intercept(FileNotFoundException.class, () ->
        fs.innerGetFileStatus(emptydir, false,
            EnumSet.noneOf(StatusProbeEnum.class)));
    verifyOperationCount(0, 0);

    // now add a trailing slash to the key and use the
    // deep internal s3GetFileStatus method call.
    String emptyDirTrailingSlash = fs.pathToKey(emptydir.getParent())
        + "/" + dir +  "/";
    // A HEAD request does not probe for keys with a trailing /
    intercept(FileNotFoundException.class, () ->
        fs.s3GetFileStatus(emptydir, emptyDirTrailingSlash,
            StatusProbeEnum.HEAD_ONLY, null));
    verifyOperationCount(0, 0);

    // but ask for a directory marker and you get the entry
    status = fs.s3GetFileStatus(emptydir,
        emptyDirTrailingSlash,
        StatusProbeEnum.DIR_MARKER_ONLY, null);
    verifyOperationCount(1, 0);
    assertEquals(emptydir, status.getPath());
  }

  @Test
  public void testCreateCost() throws Throwable {
    describe("Test file creation cost -raw only");
    S3AFileSystem fs = getFileSystem();
    assume("Unguarded FS only", !fs.hasMetadataStore());
    resetMetricDiffs();
    Path testFile = path("testCreateCost");

    // when overwrite is false, the path is checked for existence.
    try (FSDataOutputStream out = fs.create(testFile, false)) {
      verifyOperationCount(2, 1);
    }

    // but when true: only the directory checks take place.
    try (FSDataOutputStream out = fs.create(testFile, true)) {
      verifyOperationCount(1, 1);
    }

  }
}
