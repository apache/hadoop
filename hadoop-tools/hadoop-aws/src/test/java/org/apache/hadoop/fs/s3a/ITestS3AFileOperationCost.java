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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Statistic.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.test.GenericTestUtils.getTestDir;
import static org.junit.Assume.assumeFalse;

/**
 * Use metrics to assert about the cost of file status queries.
 * {@link S3AFileSystem#getFileStatus(Path)}.
 */
public class ITestS3AFileOperationCost extends AbstractS3ATestBase {

  private MetricDiff metadataRequests;
  private MetricDiff listRequests;

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AFileOperationCost.class);

  @Override
  public void setup() throws Exception {
    super.setup();
    S3AFileSystem fs = getFileSystem();
    metadataRequests = new MetricDiff(fs, OBJECT_METADATA_REQUESTS);
    listRequests = new MetricDiff(fs, OBJECT_LIST_REQUESTS);
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

  @Test
  public void testCostOfGetFileStatusOnEmptyDir() throws Throwable {
    describe("performing getFileStatus on an empty directory");
    S3AFileSystem fs = getFileSystem();
    Path dir = path("empty");
    fs.mkdirs(dir);
    resetMetricDiffs();
    S3AFileStatus status = fs.innerGetFileStatus(dir, true);
    assertTrue("not empty: " + status,
        status.isEmptyDirectory() == Tristate.TRUE);

    if (!fs.hasMetadataStore()) {
      metadataRequests.assertDiffEquals(2);
    }
    listRequests.assertDiffEquals(0);
  }

  @Test
  public void testCostOfGetFileStatusOnMissingFile() throws Throwable {
    describe("performing getFileStatus on a missing file");
    S3AFileSystem fs = getFileSystem();
    Path path = path("missing");
    resetMetricDiffs();
    try {
      FileStatus status = fs.getFileStatus(path);
      fail("Got a status back from a missing file path " + status);
    } catch (FileNotFoundException expected) {
      // expected
    }
    metadataRequests.assertDiffEquals(2);
    listRequests.assertDiffEquals(1);
  }

  @Test
  public void testCostOfGetFileStatusOnMissingSubPath() throws Throwable {
    describe("performing getFileStatus on a missing file");
    S3AFileSystem fs = getFileSystem();
    Path path = path("missingdir/missingpath");
    resetMetricDiffs();
    try {
      FileStatus status = fs.getFileStatus(path);
      fail("Got a status back from a missing file path " + status);
    } catch (FileNotFoundException expected) {
      // expected
    }
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
    S3AFileStatus status = fs.innerGetFileStatus(dir, true);
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

  private void reset(MetricDiff... diffs) {
    for (MetricDiff diff : diffs) {
      diff.reset();
    }
  }

  @Test
  public void testFakeDirectoryDeletion() throws Throwable {
    describe("Verify whether create file works after renaming a file. "
        + "In S3, rename deletes any fake directories as a part of "
        + "clean up activity");
    S3AFileSystem fs = getFileSystem();

    // As this test uses the s3 metrics to count the number of fake directory
    // operations, it depends on side effects happening internally. With
    // metadata store enabled, it is brittle to change. We disable this test
    // before the internal behavior w/ or w/o metadata store.
    assumeFalse(fs.hasMetadataStore());

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

    Path srcDir = new Path(srcBaseDir, "1/2/3/4/5/6");
    int srcDirDepth = directoriesInPath(srcDir);
    // one dir created, one removed
    mkdirs(srcDir);
    String state = "after mkdir(srcDir)";
    directoriesCreated.assertDiffEquals(state, 1);
    deleteRequests.assertDiffEquals(state, 1);
    directoriesDeleted.assertDiffEquals(state, 0);
    // HADOOP-14255 deletes unnecessary fake directory objects in mkdirs()
    fakeDirectoriesDeleted.assertDiffEquals(state, srcDirDepth - 1);
    reset(deleteRequests, directoriesCreated, directoriesDeleted,
        fakeDirectoriesDeleted);

    // creating a file should trigger demise of the src dir
    final Path srcFilePath = new Path(srcDir, "source.txt");
    touch(fs, srcFilePath);
    state = "after touch(fs, srcFilePath)";
    deleteRequests.assertDiffEquals(state, 1);
    directoriesCreated.assertDiffEquals(state, 0);
    directoriesDeleted.assertDiffEquals(state, 0);
    fakeDirectoriesDeleted.assertDiffEquals(state, srcDirDepth);

    reset(deleteRequests, directoriesCreated, directoriesDeleted,
        fakeDirectoriesDeleted);

    Path destBaseDir = path("dest");
    Path destDir = new Path(destBaseDir, "1/2/3/4/5/6");
    Path destFilePath = new Path(destDir, "dest.txt");
    mkdirs(destDir);
    state = "after mkdir(destDir)";

    int destDirDepth = directoriesInPath(destDir);
    directoriesCreated.assertDiffEquals(state, 1);
    deleteRequests.assertDiffEquals(state, 1);
    directoriesDeleted.assertDiffEquals(state, 0);
    fakeDirectoriesDeleted.assertDiffEquals(state, destDirDepth - 1);
    reset(deleteRequests, directoriesCreated, directoriesDeleted,
        fakeDirectoriesDeleted);

    fs.rename(srcFilePath, destFilePath);
    state = "after rename(srcFilePath, destFilePath)";
    directoriesCreated.assertDiffEquals(state, 1);
    // one for the renamed file, one for the parent
    deleteRequests.assertDiffEquals(state, 2);
    directoriesDeleted.assertDiffEquals(state, 0);
    fakeDirectoriesDeleted.assertDiffEquals(state, destDirDepth);

    reset(deleteRequests, directoriesCreated, directoriesDeleted,
        fakeDirectoriesDeleted);

    assertIsFile(destFilePath);
    assertIsDirectory(srcDir);
  }

  private int directoriesInPath(Path path) {
    return path.isRoot() ? 0 : 1 + directoriesInPath(path.getParent());
  }

}
