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


import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.StatusProbeEnum;
import org.apache.hadoop.fs.s3a.test.costs.AbstractS3ACostTest;


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


import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Statistic.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.test.costs.HeadListCosts.*;
import static org.apache.hadoop.test.GenericTestUtils.getTestDir;

/**
 * Use metrics to assert about the cost of file API calls.
 * Parameterized on guarded vs raw. and directory marker keep vs delete
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
        {"raw-keep-markers", false, true, false},
        {"raw-delete-markers", false, false, false},
        {"nonauth-keep-markers", true, true, false},
        {"auth-delete-markers", true, false, true}
    });
  }

  public ITestS3AFileOperationCost(final String name,
      final boolean s3guard,
      final boolean keepMarkers,
      final boolean authoritative) {
    super(s3guard, keepMarkers, authoritative);
  }

  /**
   * Test the cost of {@code listLocatedStatus(file)}.
   * There's a minor inefficiency in that calling this on
   * a file in S3Guard still executes a LIST call, even
   * though the file record is in the store.
   */
  @Test
  public void testCostOfLocatedFileStatusOnFile() throws Throwable {
    describe("performing listLocatedStatus on a file");
    Path file = file(methodPath());
    S3AFileSystem fs = getFileSystem();
    verifyMetrics(() -> fs.listLocatedStatus(file),
        rawHeadList(FILESTATUS_FILE_PROBE_H, LIST_LOCATED_STATUS_L),
        authHeadList(0, LIST_LOCATED_STATUS_L),
        nonauthHeadList(0, LIST_LOCATED_STATUS_L));
  }

  @Test
  public void testCostOfListLocatedStatusOnEmptyDir() throws Throwable {
    describe("performing listLocatedStatus on an empty dir");
    Path dir = dir(methodPath());
    S3AFileSystem fs = getFileSystem();
    verifyMetrics(() ->
            fs.listLocatedStatus(dir),
        rawHeadList(FILESTATUS_FILE_PROBE_H,
            LIST_LOCATED_STATUS_L + GETFILESTATUS_DIR_L),
        authHeadList(0, 0),
        nonauthHeadList(0, LIST_LOCATED_STATUS_L));
  }

  @Test
  public void testCostOfListLocatedStatusOnNonEmptyDir() throws Throwable {
    describe("performing listLocatedStatus on a non empty dir");
    Path dir = dir(methodPath());
    S3AFileSystem fs = getFileSystem();
    Path file = file(new Path(dir, "file.txt"));
    verifyMetrics(() ->
          fs.listLocatedStatus(dir),
        rawHeadList(0, LIST_LOCATED_STATUS_L),
        authHeadList(0, 0),
        nonauthHeadList(0, LIST_LOCATED_STATUS_L));
  }

  @Test
  public void testCostOfListFilesOnFile() throws Throwable {
    describe("Performing listFiles() on a file");
    Path file = path(getMethodName() + ".txt");
    S3AFileSystem fs = getFileSystem();
    touch(fs, file);
    verifyMetrics(() ->
            fs.listFiles(file, true),
        rawHeadList(GETFILESTATUS_SINGLE_FILE_H,
            LIST_LOCATED_STATUS_L),
        authoritative(OBJECT_LIST_REQUESTS, 0),
        nonauth(OBJECT_LIST_REQUESTS,
            LIST_FILES_L));
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
        rawHeadList(GETFILESTATUS_EMPTY_DIR_H,
            LIST_FILES_L + GETFILESTATUS_EMPTY_DIR_L),
        authHeadList(0, 0),
        nonauthHeadList(0, LIST_FILES_L));
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
        rawHeadList(0, LIST_FILES_L),
        authHeadList(0, 0),
        nonauthHeadList(0, LIST_FILES_L));
  }

  @Test
  public void testCostOfListFilesOnNonExistingDir() throws Throwable {
    describe("Performing listFiles() on a non existing dir");
    Path dir = path(getMethodName());
    S3AFileSystem fs = getFileSystem();
    verifyMetricsIntercepting(FileNotFoundException.class, "",
        () -> fs.listFiles(dir, true),
        rawHeadList(GETFILESTATUS_FNFE_H,
            GETFILESTATUS_FNFE_L + LIST_FILES_L)

    );
  }

  @Test
  public void testCostOfGetFileStatusOnFile() throws Throwable {
    describe("performing getFileStatus on a file");
    Path simpleFile = file(methodPath());
    S3AFileStatus status = verifyRawInnerGetFileStatus(simpleFile, true,
        StatusProbeEnum.ALL,
        GETFILESTATUS_SINGLE_FILE_H,
        GETFILESTATUS_SINGLE_FILE_L);
    assertTrue("not a file: " + status, status.isFile());
  }


  @Test
  public void testCostOfGetFileStatusOnEmptyDir() throws Throwable {
    describe("performing getFileStatus on an empty directory");
    Path dir = dir(methodPath());
    S3AFileStatus status = verifyRawInnerGetFileStatus(dir, true,
        StatusProbeEnum.ALL,
        GETFILESTATUS_MARKER_H,
        GETFILESTATUS_EMPTY_DIR_L);
    assertSame("not empty: " + status, Tristate.TRUE,
        status.isEmptyDirectory());
    // but now only ask for the directories and the file check is skipped.
    verifyRawInnerGetFileStatus(dir, false,
        StatusProbeEnum.DIRECTORIES,
        FILESTATUS_DIR_PROBE_H,
        GETFILESTATUS_EMPTY_DIR_L);

    // now look at isFile/isDir against the same entry
    isDir(dir, true, FILESTATUS_DIR_PROBE_H,
        GETFILESTATUS_EMPTY_DIR_L);
    isFile(dir, false,
        FILESTATUS_FILE_PROBE_H, FILESTATUS_FILE_PROBE_L);
  }

  @Test
  public void testCostOfGetFileStatusOnMissingFile() throws Throwable {
    describe("performing getFileStatus on a missing file");
    interceptRawGetFileStatusFNFE(methodPath(), false,
        StatusProbeEnum.ALL,
        GETFILESTATUS_FNFE_H,
        GETFILESTATUS_FNFE_L);
  }

  @Test
  public void testIsDirIsFileMissingPath() throws Throwable {
    describe("performing isDir and isFile on a missing file");
    Path path = methodPath();
    // now look at isFile/isDir against the same entry
    isDir(path, false,
        FILESTATUS_DIR_PROBE_H,
        FILESTATUS_DIR_PROBE_L);
    isFile(path, false,
        FILESTATUS_FILE_PROBE_H,
        FILESTATUS_FILE_PROBE_L);
  }

  @Test
  public void testCostOfGetFileStatusOnNonEmptyDir() throws Throwable {
    describe("performing getFileStatus on a non-empty directory");
    Path dir = dir(methodPath());
    file(new Path(dir, "simple.txt"));
    S3AFileStatus status = verifyRawInnerGetFileStatus(dir, true,
            StatusProbeEnum.ALL,
        GETFILESTATUS_DIR_H, GETFILESTATUS_DIR_L);
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
          always(INVOCATION_COPY_FROM_LOCAL_FILE, 1),
          always(OBJECT_PUT_REQUESTS, 1),
          always(OBJECT_PUT_BYTES, len));
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
    assumeUnguarded();
    S3AFileSystem fs = getFileSystem();
    // Create the empty directory.
    Path emptydir = dir(methodPath());

    // head probe fails
    interceptRawGetFileStatusFNFE(emptydir, false,
        StatusProbeEnum.HEAD_ONLY,
        FILESTATUS_FILE_PROBE_H,
        FILESTATUS_FILE_PROBE_L);

    // a LIST will find it and declare as empty
    S3AFileStatus status = verifyRawInnerGetFileStatus(emptydir, true,
        StatusProbeEnum.LIST_ONLY, 0,
        GETFILESTATUS_EMPTY_DIR_L);
    assertEmptyDirStatus(status, Tristate.TRUE);

    // skip all probes and expect no operations to take place
    interceptRawGetFileStatusFNFE(emptydir, false,
        EnumSet.noneOf(StatusProbeEnum.class),
        0, 0);

    // now add a trailing slash to the key and use the
    // deep internal s3GetFileStatus method call.
    String emptyDirTrailingSlash = fs.pathToKey(emptydir.getParent())
        + "/" + emptydir.getName() +  "/";
    // A HEAD request does not probe for keys with a trailing /
    interceptRawHeadList(FileNotFoundException.class, "",
        0, 0, () ->
        fs.s3GetFileStatus(emptydir, emptyDirTrailingSlash,
            StatusProbeEnum.HEAD_ONLY, null, false));

    // but ask for a directory marker and you get the entry
    status = verifyRawHeadList(0, GETFILESTATUS_EMPTY_DIR_L, () ->
        fs.s3GetFileStatus(emptydir,
            emptyDirTrailingSlash,
            StatusProbeEnum.LIST_ONLY,
            null,
            true));
    assertEquals(emptydir, status.getPath());
    assertEmptyDirStatus(status, Tristate.TRUE);
  }

  @Test
  public void testCreateCost() throws Throwable {
    describe("Test file creation cost -raw only");
    assumeUnguarded();
    Path testFile = methodPath();
    // when overwrite is false, the path is checked for existence.
    create(testFile, false,
        CREATE_FILE_NO_OVERWRITE_H, CREATE_FILE_NO_OVERWRITE_L);
    // but when true: only the directory checks take place.
    create(testFile, true, CREATE_FILE_OVERWRITE_H, CREATE_FILE_OVERWRITE_L);
  }

  @Test
  public void testCreateCostFileExists() throws Throwable {
    describe("Test cost of create file failing with existing file");
    assumeUnguarded();
    Path testFile = file(methodPath());

    // now there is a file there, an attempt with overwrite == false will
    // fail on the first HEAD.
    interceptRawHeadList(FileAlreadyExistsException.class, "",
        FILESTATUS_FILE_PROBE_H, 0,
        () -> file(testFile, false));
  }

  @Test
  public void testCreateCostDirExists() throws Throwable {
    describe("Test cost of create file failing with existing dir");
    assumeUnguarded();
    Path testFile = dir(methodPath());

    // now there is a file there, an attempt with overwrite == false will
    // fail on the first HEAD.
    interceptRawHeadList(FileAlreadyExistsException.class, "",
        GETFILESTATUS_MARKER_H, GETFILESTATUS_EMPTY_DIR_L,
        () -> file(testFile, false));
  }

  /**
   * Use the builder API.
   * This always looks for a parent unless the caller says otherwise.
   */
  @Test
  public void testCreateBuilder() throws Throwable {
    describe("Test builder file creation cost -raw only");
    assumeUnguarded();
    Path testFile = methodPath();
    dir(testFile.getParent());

    // builder defaults to looking for parent existence (non-recursive)
    buildFile(testFile, false,  false,
        FILESTATUS_FILE_PROBE_H,   // destination file
        FILESTATUS_DIR_PROBE_L * 2);    // destination file and parent dir
    // recursive = false and overwrite=true:
    // only make sure the dest path isn't a directory.
    buildFile(testFile, true, true,
        FILESTATUS_DIR_PROBE_H, FILESTATUS_DIR_PROBE_L);

    // now there is a file there, an attempt with overwrite == false will
    // fail on the first HEAD.
    interceptRawHeadList(FileAlreadyExistsException.class, "",
        FILESTATUS_FILE_PROBE_H, 0, () ->
            buildFile(testFile, false, true, FILESTATUS_FILE_PROBE_H, 0));
  }

  @Test
  public void testCostOfGlobStatus() throws Throwable {
    describe("Test globStatus has expected cost");
    S3AFileSystem fs = getFileSystem();
    assume("Unguarded FS only", !fs.hasMetadataStore());

    Path basePath = path("testCostOfGlobStatus/nextFolder/");

    // create a bunch of files
    int filesToCreate = 10;
    for (int i = 0; i < filesToCreate; i++) {
      create(basePath.suffix("/" + i));
    }

    fs.globStatus(basePath.suffix("/*"));
    // 2 head + 1 list from getFileStatus on path,
    // plus 1 list to match the glob pattern
    verifyRawHeadList(GETFILESTATUS_DIR_H,
        GETFILESTATUS_DIR_L + 1, () ->
        fs.globStatus(basePath.suffix("/*")));
  }

  @Test
  public void testCostOfGlobStatusNoSymlinkResolution() throws Throwable {
    describe("Test globStatus does not attempt to resolve symlinks");
    S3AFileSystem fs = getFileSystem();
    assume("Unguarded FS only", !fs.hasMetadataStore());

    Path basePath = path("testCostOfGlobStatusNoSymlinkResolution/f/");

    // create a single file, globStatus returning a single file on a pattern
    // triggers attempts at symlinks resolution if configured
    String fileName = "/notASymlinkDOntResolveMeLikeOne";
    create(basePath.suffix(fileName));
    // unguarded: 2 head + 1 list from getFileStatus on path,
    // plus 1 list to match the glob pattern
    // no additional operations from symlink resolution
    verifyRawHeadList(
        GETFILESTATUS_DIR_H,
        GETFILESTATUS_DIR_L + 1, () ->
        fs.globStatus(basePath.suffix("/*")));
  }


}
