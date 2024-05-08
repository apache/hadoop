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
import org.apache.hadoop.fs.s3a.impl.StatusProbeEnum;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;


import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_PERFORMANCE;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.Statistic.*;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.*;
import static org.apache.hadoop.test.GenericTestUtils.getTestDir;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Use metrics to assert about the cost of file API calls.
 * Parameterized on directory marker keep vs delete.
 * When the FS is instantiated with creation performance, things
 * behave differently...its value is that of the marker keep flag,
 * so deletion costs are the same.
 */
@RunWith(Parameterized.class)
public class ITestS3AFileOperationCost extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AFileOperationCost.class);

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"keep-markers", true},
        {"delete-markers", false},
    });
  }

  public ITestS3AFileOperationCost(
      final String name,
      final boolean keepMarkers) {
    super(keepMarkers);
  }

  @Override
  public Configuration createConfiguration() {
    final Configuration conf = super.createConfiguration();
    removeBaseAndBucketOverrides(conf, FS_S3A_CREATE_PERFORMANCE);
    conf.setBoolean(FS_S3A_CREATE_PERFORMANCE, isKeepingMarkers());
    return conf;
  }

  /**
   * Test the cost of {@code listLocatedStatus(file)}.
   */
  @Test
  public void testCostOfLocatedFileStatusOnFile() throws Throwable {
    describe("performing listLocatedStatus on a file");
    Path file = file(methodPath());
    S3AFileSystem fs = getFileSystem();
    verifyMetrics(() -> fs.listLocatedStatus(file),
        always(FILE_STATUS_FILE_PROBE
            .plus(LIST_LOCATED_STATUS_LIST_OP)));
  }

  @Test
  public void testCostOfListLocatedStatusOnEmptyDir() throws Throwable {
    describe("performing listLocatedStatus on an empty dir");
    Path dir = dir(methodPath());
    S3AFileSystem fs = getFileSystem();
    verifyMetrics(() ->
            fs.listLocatedStatus(dir),
        always(LIST_LOCATED_STATUS_LIST_OP
            .plus(GET_FILE_STATUS_ON_EMPTY_DIR)));
  }

  @Test
  public void testCostOfListLocatedStatusOnNonEmptyDir() throws Throwable {
    describe("performing listLocatedStatus on a non empty dir");
    Path dir = dir(methodPath());
    S3AFileSystem fs = getFileSystem();
    Path file = file(new Path(dir, "file.txt"));
    verifyMetrics(() ->
          fs.listLocatedStatus(dir),
        always(LIST_LOCATED_STATUS_LIST_OP));
  }

  @Test
  public void testCostOfListFilesOnFile() throws Throwable {
    describe("Performing listFiles() on a file");
    Path file = path(getMethodName() + ".txt");
    S3AFileSystem fs = getFileSystem();
    touch(fs, file);
    verifyMetrics(() ->
            fs.listFiles(file, true),
        always(LIST_LOCATED_STATUS_LIST_OP
            .plus(GET_FILE_STATUS_ON_FILE)));
  }

  @Test
  public void testCostOfListFilesOnEmptyDir() throws Throwable {
    describe("Perpforming listFiles() on an empty dir with marker");
    // this attem
    Path dir = path(getMethodName());
    S3AFileSystem fs = getFileSystem();
    fs.mkdirs(dir);
    verifyMetrics(() ->
            fs.listFiles(dir, true),
        always(LIST_FILES_LIST_OP
            .plus(GET_FILE_STATUS_ON_EMPTY_DIR)));
  }

  @Test
  public void testCostOfListFilesOnNonEmptyDir() throws Throwable {
    describe("Performing listFiles() on a non empty dir");
    Path dir = path(getMethodName());
    S3AFileSystem fs = getFileSystem();
    fs.mkdirs(dir);
    Path file = new Path(dir, "file.txt");
    touch(fs, file);
    verifyMetrics(() ->
            fs.listFiles(dir, true),
        always(LIST_FILES_LIST_OP));
  }

  @Test
  public void testCostOfListFilesOnNonExistingDir() throws Throwable {
    describe("Performing listFiles() on a non existing dir");
    Path dir = path(getMethodName());
    S3AFileSystem fs = getFileSystem();
    verifyMetricsIntercepting(FileNotFoundException.class, "",
        () -> fs.listFiles(dir, true),
        always(LIST_FILES_LIST_OP
            .plus(GET_FILE_STATUS_FNFE)));
  }

  @Test
  public void testCostOfListStatusOnFile() throws Throwable {
    describe("Performing listStatus() on a file");
    Path file = path(getMethodName() + ".txt");
    S3AFileSystem fs = getFileSystem();
    touch(fs, file);
    verifyMetrics(() ->
            fs.listStatus(file),
        always(LIST_STATUS_LIST_OP
            .plus(GET_FILE_STATUS_ON_FILE)));
  }

  @Test
  public void testCostOfListStatusOnEmptyDir() throws Throwable {
    describe("Performing listStatus() on an empty dir");
    Path dir = path(getMethodName());
    S3AFileSystem fs = getFileSystem();
    fs.mkdirs(dir);
    verifyMetrics(() ->
            fs.listStatus(dir),
        always(LIST_STATUS_LIST_OP
            .plus(GET_FILE_STATUS_ON_EMPTY_DIR)));
  }

  @Test
  public void testCostOfListStatusOnNonEmptyDir() throws Throwable {
    describe("Performing listStatus() on a non empty dir");
    Path dir = path(getMethodName());
    S3AFileSystem fs = getFileSystem();
    fs.mkdirs(dir);
    Path file = new Path(dir, "file.txt");
    touch(fs, file);
    verifyMetrics(() ->
            fs.listStatus(dir),
        always(LIST_STATUS_LIST_OP));
  }

  @Test
  public void testCostOfGetFileStatusOnFile() throws Throwable {
    describe("performing getFileStatus on a file");
    Path simpleFile = file(methodPath());
    S3AFileStatus status = verifyInnerGetFileStatus(simpleFile, true,
        StatusProbeEnum.ALL,
        GET_FILE_STATUS_ON_FILE);
    assertTrue("not a file: " + status, status.isFile());
  }

  @Test
  public void testCostOfGetFileStatusOnEmptyDir() throws Throwable {
    describe("performing getFileStatus on an empty directory");
    Path dir = dir(methodPath());
    S3AFileStatus status = verifyInnerGetFileStatus(dir, true,
        StatusProbeEnum.ALL,
        GET_FILE_STATUS_ON_DIR_MARKER);
    assertSame("not empty: " + status, Tristate.TRUE,
        status.isEmptyDirectory());
    // but now only ask for the directories and the file check is skipped.
    verifyInnerGetFileStatus(dir, false,
        StatusProbeEnum.DIRECTORIES,
        FILE_STATUS_DIR_PROBE);

    // now look at isFile/isDir against the same entry
    isDir(dir, true, FILE_STATUS_DIR_PROBE);
    isFile(dir, false, FILE_STATUS_FILE_PROBE);
  }

  @Test
  public void testCostOfGetFileStatusOnMissingFile() throws Throwable {
    describe("performing getFileStatus on a missing file");
    interceptGetFileStatusFNFE(methodPath(), false,
        StatusProbeEnum.ALL,
        GET_FILE_STATUS_FNFE);
  }

  @Test
  public void testCostOfRootFileStatus() throws Throwable {
    Path root = path("/");
    S3AFileStatus rootStatus = verifyInnerGetFileStatus(
            root,
            false,
            StatusProbeEnum.ALL,
            ROOT_FILE_STATUS_PROBE);
    String rootStatusContent = rootStatus.toString();
    Assertions.assertThat(rootStatus.isDirectory())
            .describedAs("Status returned should be a directory "
                    + rootStatusContent)
            .isEqualTo(true);
    Assertions.assertThat(rootStatus.isEmptyDirectory())
            .isEqualTo(Tristate.UNKNOWN);

    rootStatus = verifyInnerGetFileStatus(
            root,
            true,
            StatusProbeEnum.ALL,
            FILE_STATUS_DIR_PROBE);
    Assertions.assertThat(rootStatus.isDirectory())
            .describedAs("Status returned should be a directory "
                    + rootStatusContent)
            .isEqualTo(true);
    Assertions.assertThat(rootStatus.isEmptyDirectory())
            .isNotEqualByComparingTo(Tristate.UNKNOWN);

  }

  @Test
  public void testIsDirIsFileMissingPath() throws Throwable {
    describe("performing isDir and isFile on a missing file");
    Path path = methodPath();
    // now look at isFile/isDir against the same entry
    isDir(path, false,
        FILE_STATUS_DIR_PROBE);
    isFile(path, false,
        FILE_STATUS_FILE_PROBE);
  }

  @Test
  public void testCostOfGetFileStatusOnNonEmptyDir() throws Throwable {
    describe("performing getFileStatus on a non-empty directory");
    Path dir = dir(methodPath());
    file(new Path(dir, "simple.txt"));
    S3AFileStatus status = verifyInnerGetFileStatus(dir, true,
        StatusProbeEnum.ALL,
        GET_FILE_STATUS_ON_DIR);
    assertEmptyDirStatus(status, Tristate.FALSE);
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


      Path remotePath = methodPath();

      verifyMetrics(() -> {
        s3a.copyFromLocalFile(false, true, localPath, remotePath);
        return "copy";
      },
          with(INVOCATION_COPY_FROM_LOCAL_FILE, 1),
          with(OBJECT_PUT_REQUESTS, 1),
          with(OBJECT_PUT_BYTES, len));
      verifyFileContents(s3a, remotePath, data);
      // print final stats
      LOG.info("Filesystem {}", s3a);
    } finally {
      tmpFile.delete();
    }
  }

  @Test
  public void testDirProbes() throws Throwable {
    describe("Test directory probe cost");
    S3AFileSystem fs = getFileSystem();
    // Create the empty directory.
    Path emptydir = dir(methodPath());

    // head probe fails
    interceptGetFileStatusFNFE(emptydir, false,
        StatusProbeEnum.HEAD_ONLY,
        FILE_STATUS_FILE_PROBE);

    // a LIST will find it and declare as empty
    S3AFileStatus status = verifyInnerGetFileStatus(emptydir, true,
        StatusProbeEnum.LIST_ONLY,
        FILE_STATUS_DIR_PROBE);
    assertEmptyDirStatus(status, Tristate.TRUE);

    // skip all probes and expect no operations to take place
    interceptGetFileStatusFNFE(emptydir, false,
        EnumSet.noneOf(StatusProbeEnum.class),
        NO_IO);

    // now add a trailing slash to the key and use the
    // deep internal s3GetFileStatus method call.
    String emptyDirTrailingSlash = fs.pathToKey(emptydir.getParent())
        + "/" + emptydir.getName() +  "/";
    // A HEAD request does not probe for keys with a trailing /
    interceptOperation(FileNotFoundException.class, "",
        NO_IO, () ->
        fs.s3GetFileStatus(emptydir, emptyDirTrailingSlash,
            StatusProbeEnum.HEAD_ONLY, false));

    // but ask for a directory marker and you get the entry
    status = verify(FILE_STATUS_DIR_PROBE, () ->
        fs.s3GetFileStatus(emptydir,
            emptyDirTrailingSlash,
            StatusProbeEnum.LIST_ONLY,
            true));
    assertEquals(emptydir, status.getPath());
    assertEmptyDirStatus(status, Tristate.TRUE);
  }

  @Test
  public void testNeedEmptyDirectoryProbeRequiresList() throws Throwable {
    S3AFileSystem fs = getFileSystem();

    intercept(IllegalArgumentException.class, "", () ->
            fs.s3GetFileStatus(new Path("/something"), "/something",
                StatusProbeEnum.HEAD_ONLY, true));
  }

  @Test
  public void testCostOfGlobStatus() throws Throwable {
    describe("Test globStatus has expected cost");
    S3AFileSystem fs = getFileSystem();
    Path basePath = path("testCostOfGlobStatus/nextFolder/");

    // create a bunch of files
    int filesToCreate = 10;
    for (int i = 0; i < filesToCreate; i++) {
      file(basePath.suffix("/" + i));
    }

    fs.globStatus(basePath.suffix("/*"));
    // 2 head + 1 list from getFileStatus on path,
    // plus 1 list to match the glob pattern
    verify(LIST_STATUS_LIST_OP,
        () -> fs.globStatus(basePath.suffix("/*")));
  }

  @Test
  public void testCostOfGlobStatusNoSymlinkResolution() throws Throwable {
    describe("Test globStatus does not attempt to resolve symlinks");
    S3AFileSystem fs = getFileSystem();
    Path basePath = path("testCostOfGlobStatusNoSymlinkResolution/f/");

    // create a single file, globStatus returning a single file on a pattern
    // triggers attempts at symlinks resolution if configured
    String fileName = "/notASymlinkDOntResolveMeLikeOne";
    file(basePath.suffix(fileName));
    // unguarded: 2 head + 1 list from getFileStatus on path,
    // plus 1 list to match the glob pattern
    // no additional operations from symlink resolution
    verify(LIST_STATUS_LIST_OP,
        () -> fs.globStatus(basePath.suffix("/*")));
  }


}
