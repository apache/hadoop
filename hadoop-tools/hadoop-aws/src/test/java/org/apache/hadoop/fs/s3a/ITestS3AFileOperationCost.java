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
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.impl.StatusProbeEnum;
import org.apache.hadoop.fs.s3a.test.costs.OperationCostValidator;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_KEEP;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.Statistic.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.test.costs.HeadListCosts.*;
import static org.apache.hadoop.fs.s3a.test.costs.OperationCostValidator.probe;
import static org.apache.hadoop.fs.s3a.test.costs.OperationCostValidator.probes;
import static org.apache.hadoop.test.AssertExtensions.dynamicDescription;
import static org.apache.hadoop.test.GenericTestUtils.getTestDir;

/**
 * Use metrics to assert about the cost of file status queries.
 * {@link S3AFileSystem#getFileStatus(Path)}.
 * Parameterized on guarded vs raw. and directory marker keep vs delete
 */
@RunWith(Parameterized.class)
public class ITestS3AFileOperationCost extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AFileOperationCost.class);

  private OperationCostValidator costValidator;

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"raw-keep-markers", false, true},
        {"raw-delete-markers", false, false},
        {"guarded-keep-markers", true, true},
        {"guarded-delete-markers", true, false}
    });
  }

  /**
   * Parameter: should the stores be guarded?
   */
  private final boolean s3guard;

  /**
   * Parameter: should directory markers be retained?
   */
  private final boolean keepMarkers;

  /**
   * Is this an auth mode test run?
   */
  private boolean authoritative;

  /* probe states calculated from the configuration options. */
  boolean isGuarded;
  boolean isRaw;

  boolean isAuthoritative;
  boolean isNonAuth;

  boolean isKeeping;

  boolean isDeleting;

  public ITestS3AFileOperationCost(final String name,
      final boolean s3guard,
      final boolean keepMarkers) {
    this.s3guard = s3guard;
    this.keepMarkers = keepMarkers;
  }

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    String bucketName = getTestBucketName(conf);
    removeBucketOverrides(bucketName, conf,
        S3_METADATA_STORE_IMPL);
    if (!isGuarded()) {
      // in a raw run remove all s3guard settings
      removeBaseAndBucketOverrides(bucketName, conf,
          S3_METADATA_STORE_IMPL);
    }
    // directory marker options
    removeBaseAndBucketOverrides(bucketName, conf,
        DIRECTORY_MARKER_POLICY);
    conf.set(DIRECTORY_MARKER_POLICY,
        keepMarkers
            ? DIRECTORY_MARKER_POLICY_KEEP
            : DIRECTORY_MARKER_POLICY_DELETE);
    conf.setBoolean(METADATASTORE_AUTHORITATIVE, authoritative);
    disableFilesystemCaching(conf);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    if (isGuarded()) {
      // s3guard is required for those test runs where any of the
      // guard options are set
      assumeS3GuardState(true, getConfiguration());
    }
    S3AFileSystem fs = getFileSystem();
    skipDuringFaultInjection(fs);
    authoritative = fs.allowAuthoritative(new Path("/"));

    // build up the states
    isGuarded = isGuarded();

    isRaw = !isGuarded;
    isAuthoritative = isGuarded && authoritative;
    isNonAuth = isGuarded && !authoritative;

    isKeeping = isKeepingMarkers ();

    isDeleting = !isKeeping;

    costValidator = OperationCostValidator.builder(getFileSystem())
        .withMetrics(INVOCATION_COPY_FROM_LOCAL_FILE,
            OBJECT_COPY_REQUESTS,
            OBJECT_DELETE_REQUESTS,
            DIRECTORIES_CREATED,
            DIRECTORIES_DELETED,
            FAKE_DIRECTORIES_DELETED,
            FILES_DELETED,
            OBJECT_LIST_REQUESTS,
            OBJECT_METADATA_REQUESTS,
            OBJECT_PUT_BYTES,
            OBJECT_PUT_REQUESTS)
        .build();
  }

  public void assumeUnguarded() {
    assume("Unguarded FS only", !isGuarded());
  }

  /**
   * Is the store guarded authoritatively on the test path?
   * @return true if the condition is met on this test run.
   */
  public boolean isAuthoritative() {
    return authoritative;
  }

  /**
   * Is the store guarded?
   * @return true if the condition is met on this test run.
   */
  public boolean isGuarded() {
    return s3guard;
  }

  /**
   * Is the store raw?
   * @return true if the condition is met on this test run.
   */
  public boolean isRaw() {
    return isRaw;
  }

  /**
   * Is the store guarded non-authoritatively on the test path?
   * @return true if the condition is met on this test run.
   */
  public boolean isNonAuth() {
    return isNonAuth;
  }

  public boolean isKeeping() {
    return isKeeping;
  }

  public boolean isDeleting() {
    return isDeleting;
  }

  public boolean isKeepingMarkers() {
    return keepMarkers;
  }

  @Test
  public void testCostOfLocatedFileStatusOnFile() throws Throwable {
    describe("performing listLocatedStatus on a file");
    Path file = file(methodPath());
    S3AFileSystem fs = getFileSystem();
    verifyMetrics(() -> fs.listLocatedStatus(file),
        raw(OBJECT_LIST_REQUESTS, LIST_LOCATED_STATUS_L),
        nonauth(OBJECT_LIST_REQUESTS, LIST_LOCATED_STATUS_L),
        raw(OBJECT_METADATA_REQUESTS, FILESTATUS_FILE_PROBE_H));
  }

  @Test
  public void testCostOfListLocatedStatusOnEmptyDir() throws Throwable {
    describe("performing listLocatedStatus on an empty dir");
    Path dir = dir(methodPath());
    S3AFileSystem fs = getFileSystem();
    verifyMetrics(() ->
            fs.listLocatedStatus(dir),
        expectRawHeadList(FILESTATUS_FILE_PROBE_H,
            LIST_LOCATED_STATUS_L + GETFILESTATUS_DIR_L),
        guarded(OBJECT_METADATA_REQUESTS, 0),
        authoritative(OBJECT_LIST_REQUESTS, 0),
        nonauth(OBJECT_LIST_REQUESTS,
            LIST_LOCATED_STATUS_L));
  }

  @Test
  public void testCostOfListLocatedStatusOnNonEmptyDir() throws Throwable {
    describe("performing listLocatedStatus on a non empty dir");
    Path dir = dir(methodPath());
    S3AFileSystem fs = getFileSystem();
    Path file = file(new Path(dir, "file.txt"));
    verifyMetrics(() ->
          fs.listLocatedStatus(dir),
        always(OBJECT_METADATA_REQUESTS, 0),
        raw(OBJECT_LIST_REQUESTS,
            LIST_LOCATED_STATUS_L),
        authoritative(OBJECT_LIST_REQUESTS, 0),
        nonauth(OBJECT_LIST_REQUESTS,
            LIST_LOCATED_STATUS_L));
  }

  @Test
  public void testCostOfListFilesOnFile() throws Throwable {
    describe("Performing listFiles() on a file");
    Path file = path(getMethodName() + ".txt");
    S3AFileSystem fs = getFileSystem();
    touch(fs, file);
    verifyMetrics(() ->
            fs.listFiles(file, true),
        expectRawHeadList(GETFILESTATUS_SINGLE_FILE_H,
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
        expectRawHeadList(GETFILESTATUS_EMPTY_DIR_H,
            LIST_FILES_L + GETFILESTATUS_EMPTY_DIR_L),
        expectAuthHeadList(0, 0),
        expectNonauthHeadList(0, LIST_FILES_L));
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
        expectRawHeadList(0, LIST_FILES_L),
        expectAuthHeadList(0, 0),
        expectNonauthHeadList(0, LIST_FILES_L));
  }

  @Test
  public void testCostOfListFilesOnNonExistingDir() throws Throwable {
    describe("Performing listFiles() on a non existing dir");
    Path dir = path(getMethodName());
    S3AFileSystem fs = getFileSystem();
    verifyMetricsIntercepting(FileNotFoundException.class, "",
        () -> fs.listFiles(dir, true),
        expectRawHeadList(GETFILESTATUS_FNFE_H,
            GETFILESTATUS_FNFE_L + LIST_FILES_L)

    );
  }

  @Test
  public void testCostOfGetFileStatusOnFile() throws Throwable {
    describe("performing getFileStatus on a file");
    Path simpleFile = file(methodPath());
    S3AFileStatus status = verifyRawGetFileStatus(simpleFile, true,
        StatusProbeEnum.ALL,
        GETFILESTATUS_SINGLE_FILE_H,
        GETFILESTATUS_SINGLE_FILE_L);
    assertTrue("not a file: " + status, status.isFile());
  }


  @Test
  public void testCostOfGetFileStatusOnEmptyDir() throws Throwable {
    describe("performing getFileStatus on an empty directory");
    Path dir = dir(methodPath());
    S3AFileStatus status = verifyRawGetFileStatus(dir, true,
        StatusProbeEnum.ALL,
        GETFILESTATUS_MARKER_H,
        GETFILESTATUS_EMPTY_DIR_L);
    assertSame("not empty: " + status, Tristate.TRUE,
        status.isEmptyDirectory());
    // but now only ask for the directories and the file check is skipped.
    verifyRawGetFileStatus(dir, false,
        StatusProbeEnum.DIRECTORIES,
        FILESTATUS_DIR_PROBE_H,
        GETFILESTATUS_EMPTY_DIR_L);

    // now look at isFile/isDir against the same entry
    isDir(dir, true, 0,
        GETFILESTATUS_EMPTY_DIR_L);
    isFile(dir, false,
        GETFILESTATUS_SINGLE_FILE_H, GETFILESTATUS_SINGLE_FILE_L);
  }

  @Test
  public void testCostOfGetFileStatusOnMissingFile() throws Throwable {
    describe("performing getFileStatus on a missing file");
    verifyRawGetFileStatusFNFE(methodPath(), false,
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
    S3AFileStatus status = verifyRawGetFileStatus(dir, true,
            StatusProbeEnum.ALL,
        GETFILESTATUS_DIR_H, GETFILESTATUS_DIR_L);
    assertEmptyDirStatus(status, Tristate.FALSE);
  }

  /**
   * This creates a directory with a child and then deletes it.
   * The parent dir must be found and declared as empty.
   */
  @Test
  public void testDeleteFile() throws Throwable {
    describe("performing getFileStatus on newly emptied directory");
    S3AFileSystem fs = getFileSystem();
    // creates the marker
    Path dir = dir(methodPath());
    // file creation may have deleted that marker, but it may
    // still be there
    Path simpleFile = file(new Path(dir, "simple.txt"));

    verifyMetrics(() -> {
          fs.delete(simpleFile, false);
          return "after fs.delete(simpleFile) " + metricSummary;
        },
        // delete file. For keeping: that's it
        probe(isRaw && isKeeping, OBJECT_METADATA_REQUESTS,
            FILESTATUS_FILE_PROBE_H),
        // if deleting markers, look for the parent too
        probe(isRaw && isDeleting, OBJECT_METADATA_REQUESTS,
            FILESTATUS_FILE_PROBE_H + FILESTATUS_DIR_PROBE_H),
        raw(OBJECT_LIST_REQUESTS,
            FILESTATUS_FILE_PROBE_L + FILESTATUS_DIR_PROBE_L),
        always(DIRECTORIES_DELETED, 0),
        always(FILES_DELETED, 1),

        // keeping: create no parent dirs or delete parents
        keeping(DIRECTORIES_CREATED, 0),
        keeping(OBJECT_DELETE_REQUESTS, DELETE_OBJECT_REQUEST),

        // deleting: create a parent and delete any of its parents
        deleting(DIRECTORIES_CREATED, 1),
        deleting(OBJECT_DELETE_REQUESTS,
            DELETE_OBJECT_REQUEST
                + DELETE_MARKER_REQUEST)
    );
    // there is an empty dir for a parent
    S3AFileStatus status = verifyRawGetFileStatus(dir, true,
        StatusProbeEnum.ALL, GETFILESTATUS_DIR_H, GETFILESTATUS_DIR_L);
    assertEmptyDirStatus(status, Tristate.TRUE);
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
  public void testDirMarkersSubdir() throws Throwable {
    describe("verify cost of deep subdir creation");

    Path subDir = new Path(methodPath(), "1/2/3/4/5/6");
    // one dir created, possibly a parent removed
    verifyMetrics(() -> {
          mkdirs(subDir);
          return "after mkdir(subDir) " + metricSummary;
        },
        always(DIRECTORIES_CREATED, 1),
        always(DIRECTORIES_DELETED, 0),
        keeping(OBJECT_DELETE_REQUESTS, 0),
        keeping(FAKE_DIRECTORIES_DELETED, 0),
        deleting(OBJECT_DELETE_REQUESTS, DELETE_MARKER_REQUEST),
        // delete all possible fake dirs above the subdirectory
        deleting(FAKE_DIRECTORIES_DELETED, directoriesInPath(subDir) - 1));
  }

  @Test
  public void testDirMarkersFileCreation() throws Throwable {
    describe("verify cost of file creation");

    Path srcBaseDir = dir(methodPath());

    Path srcDir = dir(new Path(srcBaseDir, "1/2/3/4/5/6"));

    // creating a file should trigger demise of the src dir marker
    // unless markers are being kept

    verifyMetrics(() -> {
          file(new Path(srcDir, "source.txt"));
          return "after touch(fs, srcFilePath) " + metricSummary;
        },
        always(DIRECTORIES_CREATED, 0),
        always(DIRECTORIES_DELETED, 0),
        // keeping: no delete operations.
        keeping(OBJECT_DELETE_REQUESTS, 0),
        keeping(FAKE_DIRECTORIES_DELETED, 0),
        // delete all possible fake dirs above the file
        deleting(OBJECT_DELETE_REQUESTS, 1),
        deleting(FAKE_DIRECTORIES_DELETED, directoriesInPath(srcDir)));
  }

  @Test
  public void testRenameFileToDifferentDirectory() throws Throwable {
    describe("rename a file to a different directory, "
        + "keeping the source dir present");
    S3AFileSystem fs = getFileSystem();

    Path baseDir = dir(methodPath());

    Path srcDir = new Path(baseDir, "1/2/3/4/5/6");
    final Path srcFilePath = file(new Path(srcDir, "source.txt"));

    // create a new source file.
    // Explicitly use a new path object to guarantee that the parent paths
    // are different object instances and so equals() rather than ==
    // is
    Path parent2 = srcFilePath.getParent();
    Path srcFile2 = file(new Path(parent2, "source2.txt"));
    Assertions.assertThat(srcDir)
        .isNotSameAs(parent2);
    Assertions.assertThat(srcFilePath.getParent())
        .isEqualTo(srcFile2.getParent());

    // create a directory tree, expect the dir to be created and
    // possibly a request to delete all parent directories made.
    Path destBaseDir = new Path(baseDir, "dest");
    Path destDir = dir(new Path(destBaseDir, "a/b/c/d"));
    Path destFilePath = new Path(destDir, "dest.txt");

    // rename the source file to the destination file.
    // this tests file rename, not dir rename
    // as srcFile2 exists, the parent dir of srcFilePath must not be created.
    verifyMetrics(() ->
      execRename(srcFilePath, destFilePath),
        raw(OBJECT_METADATA_REQUESTS,
            RENAME_SINGLE_FILE_RENAME_H),
        raw(OBJECT_LIST_REQUESTS,
            RENAME_SINGLE_FILE_RENAME_DIFFERENT_DIR_L),
        always(DIRECTORIES_CREATED, 0),
        always(DIRECTORIES_DELETED, 0),
        // keeping: only the core delete operation is issued.
        keeping(OBJECT_DELETE_REQUESTS, DELETE_OBJECT_REQUEST),
        keeping(FAKE_DIRECTORIES_DELETED, 0),
        // deleting: delete any fake marker above the destination.
        deleting(OBJECT_DELETE_REQUESTS,
            DELETE_OBJECT_REQUEST + DELETE_MARKER_REQUEST),
        deleting(FAKE_DIRECTORIES_DELETED, directoriesInPath(destDir)));

    assertIsFile(destFilePath);
    assertIsDirectory(srcDir);
    assertPathDoesNotExist("should have gone in the rename", srcFilePath);
  }

  /**
   * Same directory rename is lower cost as there's no need to
   * look for the parent dir of the dest path or worry about
   * deleting markers.
   */
  @Test
  public void testRenameSameDirectory() throws Throwable {
    describe("rename a file to a different directory, "
        + "keeping the source dir present");

    Path baseDir = dir(methodPath());
    final Path sourceFile = file(new Path(baseDir, "source.txt"));

    // create a new source file.
    // Explicitly use a new path object to guarantee that the parent paths
    // are different object instances and so equals() rather than ==
    // is
    Path parent2 = sourceFile.getParent();
    Path destFile = new Path(parent2, "dest");
    verifyMetrics(() ->
            execRename(sourceFile, destFile),
        raw(OBJECT_METADATA_REQUESTS, RENAME_SINGLE_FILE_RENAME_H),
        raw(OBJECT_LIST_REQUESTS, GETFILESTATUS_FNFE_L),
        always(OBJECT_COPY_REQUESTS, 1),
        always(DIRECTORIES_CREATED, 0),
        always(OBJECT_DELETE_REQUESTS, DELETE_OBJECT_REQUEST),
        always(FAKE_DIRECTORIES_DELETED, 0));
  }

  @Test
  public void testCostOfRootRename() throws Throwable {
    describe("assert that a root directory rename doesn't"
        + " do much in terms of parent dir operations");
    S3AFileSystem fs = getFileSystem();

    // unique name, so that even when run in parallel tests, there's no conflict
    String uuid = UUID.randomUUID().toString();
    Path src = file(new Path("/src-" + uuid));
    Path dest = new Path("/dest-" + uuid);
    try {

      verifyMetrics(() -> {
        fs.rename(src, dest);
        return "after fs.rename(/src,/dest) " + metricSummary;
        },
          // TWO HEAD for exists, one for source MD in copy
          expectRawHeadList(RENAME_SINGLE_FILE_RENAME_H,
              GETFILESTATUS_FNFE_L));
          // here we expect there to be no fake directories
          always(DIRECTORIES_CREATED, 0),
          // one for the renamed file only
          always(OBJECT_DELETE_REQUESTS,
              DELETE_OBJECT_REQUEST),
          // no directories are deleted: This is root
          always(DIRECTORIES_DELETED, 0),
          // no fake directories are deleted: This is root
          always(FAKE_DIRECTORIES_DELETED, 0),
          always(FILES_DELETED, 1));

      // delete that destination file, assert only the file delete was issued
      verifyMetrics(() -> {
        fs.delete(dest, false);
        return "after fs.delete(/dest) " + metricSummary;
        },
          always(DIRECTORIES_CREATED, 0),
          always(DIRECTORIES_DELETED, 0),
          always(FAKE_DIRECTORIES_DELETED, 0),
          always(FILES_DELETED, 1),
          always(OBJECT_DELETE_REQUESTS, DELETE_OBJECT_REQUEST),
          expectRawHeadList(FILESTATUS_FILE_PROBE_H, 0)); /* no need to look at parent. */

    } finally {
      fs.delete(src, false);
      fs.delete(dest, false);
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
    verifyRawGetFileStatusFNFE(emptydir, false,
        StatusProbeEnum.HEAD_ONLY,
        FILESTATUS_FILE_PROBE_H,
        FILESTATUS_FILE_PROBE_L);

    // a LIST will find it and declare as empty
    S3AFileStatus status = verifyRawGetFileStatus(emptydir, true,
        StatusProbeEnum.LIST_ONLY, 0,
        GETFILESTATUS_EMPTY_DIR_L);
    assertEmptyDirStatus(status, Tristate.TRUE);

    // skip all probes and expect no operations to take place
    verifyRawGetFileStatusFNFE(emptydir, false,
        EnumSet.noneOf(StatusProbeEnum.class),
        0, 0);

    // now add a trailing slash to the key and use the
    // deep internal s3GetFileStatus method call.
    String emptyDirTrailingSlash = fs.pathToKey(emptydir.getParent())
        + "/" + emptydir.getName() +  "/";
    // A HEAD request does not probe for keys with a trailing /
    verifyRawHeadListIntercepting(FileNotFoundException.class, "",
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

  /**
   * Assert the empty directory status of a file is as expected.
   * @param status status to probe.
   * @param expected expected value
   */
  protected void assertEmptyDirStatus(final S3AFileStatus status,
      final Tristate expected) {
    Assertions.assertThat(status.isEmptyDirectory())
        .describedAs(dynamicDescription(() ->
            "FileStatus says directory is not empty: " + status
                + "\n" + ContractTestUtils.ls(getFileSystem(), status.getPath())))
        .isEqualTo(expected);
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
    verifyRawHeadListIntercepting(FileAlreadyExistsException.class, "",
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
    verifyRawHeadListIntercepting(FileAlreadyExistsException.class, "",
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
    verifyRawHeadListIntercepting(FileAlreadyExistsException.class, "",
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


  /**
   * Touch a file, overwriting.
   * @param path path
   * @return path to new object.
   */
  private Path create(Path path) throws Exception {
    return create(path, true,
        CREATE_FILE_OVERWRITE_H,
        CREATE_FILE_OVERWRITE_L);
  }

  /**
   * Create then close the file.
   * @param path path
   * @param overwrite overwrite flag
   * @param head expected head count
   * @param list expected list count
   * @return path to new object.
   */
  private Path create(Path path, boolean overwrite,
      int head, int list) throws Exception {
    return verifyRawHeadList(head, list, () ->
      file(path, overwrite));
  }

  /**
   * Create then close the file through the builder API.
   * @param path path
   * @param overwrite overwrite flag
   * @param recursive true == skip parent existence check
   * @param head expected head count
   * @param list expected list count
   * @return path to new object.
   */
  private Path buildFile(Path path,
      boolean overwrite,
      boolean recursive,
      int head,
      int list) throws Exception {
    resetStatistics();
    verifyRawHeadList(head, list, () -> {
      FSDataOutputStreamBuilder builder = getFileSystem().createFile(path)
          .overwrite(overwrite);
      if (recursive) {
        builder.recursive();
      }
      FSDataOutputStream stream = builder.build();
      stream.close();
      return stream.toString();
    });
    return path;
  }

  /**
   * Create a directory, returning its path.
   * @param p path to dir.
   * @return path of new dir
   */
  private Path dir(Path p) throws IOException {
    mkdirs(p);
    return p;
  }

  /**
   * Create a file, returning its path.
   * @param p path to file.
   * @return path of new file
   */
  private Path file(Path p) throws IOException {
    return file(p, true);
  }

  /**
   * Create a file, returning its path.
   * @param path path to file.
   * @param overwrite overwrite flag
   * @return path of new file
   */
  private Path file(Path path, final boolean overwrite)
      throws IOException {
    getFileSystem().create(path, overwrite).close();
    return path;
  }


  /**
   * Execute rename, returning the current metrics.
   * For use in l-expressions.
   * @param source source path.
   * @param dest dest path
   * @return a string for exceptions.
   */
  public String execRename(final Path source,
      final Path dest) throws IOException {
    getFileSystem().rename(source, dest);
    return String.format("rename(%s, %s): %s", dest, source, metricSummary);
  }

  /**
   * How many directories are in a path?
   * @param path path to probe.
   * @return the number of entries below root this path is
   */
  private int directoriesInPath(Path path) {
    return path.isRoot() ? 0 : 1 + directoriesInPath(path.getParent());
  }

  /**
   * Reset all the metrics being tracked.
   */
  private void resetStatistics() {
    costValidator.resetMetricDiffs();
  }

  /**
   * Execute a closure and verify the metrics.
   * @param eval closure to evaluate
   * @param expected varargs list of expected diffs
   * @param <T> return type.
   * @return the result of the evaluation
   */
  private <T> T verifyMetrics(
      Callable<T> eval,
      OperationCostValidator.ExpectedProbe... expected) throws Exception {
    return costValidator.verify(eval, expected);

  }

  /**
   * Execute a closure, expecting an exception.
   * Verify the metrics after the exception has been caught and
   * validated.
   * @param clazz type of exception
   * @param text text to look for in exception (optional)
   * @param eval closure to evaluate
   * @param expected varargs list of expected diffs
   * @param <T> return type of closure
   * @param <E> exception type
   * @return the exception caught.
   * @throws Exception any other exception
   */
  private <T, E extends Throwable> E verifyMetricsIntercepting(
      Class<E> clazz,
      String text,
      Callable<T> eval,
      OperationCostValidator.ExpectedProbe... expected) throws Exception {
    return costValidator.verifyIntercepting(clazz, text, eval, expected);
  }

  /**
   * Execute a closure expecting an exception.
   * @param clazz type of exception
   * @param text text to look for in exception (optional)
   * @param head expected head request count.
   * @param list expected list request count.
   * @param eval closure to evaluate
   * @param <T> return type of closure
   * @param <E> exception type
   * @return the exception caught.
   * @throws Exception any other exception
   */
  private <T, E extends Throwable> E verifyRawHeadListIntercepting(
      Class<E> clazz,
      String text,
      int head,
      int list,
      Callable<T> eval) throws Exception {
    return verifyMetricsIntercepting(clazz, text, eval,
        expectRawHeadList(head, list));
  }

  /**
   * Create the probes to expect a given set of head and list requests.
   * @param enabled is the probe enabled?
   * @param head expected HEAD count
   * @param list expected LIST count
   * @return a probe list
   */
  private OperationCostValidator.ExpectedProbe
      expectHeadList(boolean enabled, int head, int list) {
    return probes(enabled,
        probe(OBJECT_METADATA_REQUESTS, head),
        probe(OBJECT_LIST_REQUESTS, list));
  }
  /**
   * Create the probes to expect a given set of head and list requests.
   * @param head expected HEAD count
   * @param list expected LIST count
   * @return a probe list
   */
  private OperationCostValidator.ExpectedProbe
      expectHeadList(int head, int list) {
    return probes(true,
        probe(OBJECT_METADATA_REQUESTS, head),
        probe(OBJECT_LIST_REQUESTS, list));
  }

  /**
   * Declare the expected head and list requests on a raw FS.
   * @param head expected HEAD count
   * @param list expected LIST count
   * @return a probe list
   */
  private OperationCostValidator.ExpectedProbe
      expectRawHeadList(int head, int list) {
    return expectHeadList(isRaw(), head, list);
  }

  /**
   * Declare the expected head and list requests on an authoritative FS.
   * @param head expected HEAD count
   * @param list expected LIST count
   * @return a probe list
   */
  private OperationCostValidator.ExpectedProbe
      expectAuthHeadList(int head, int list) {
    return expectHeadList(isAuthoritative(), head, list);
  }

  /**
   * Declare the expected head and list requests on a
   * non authoritative FS.
   * @param head expected HEAD count
   * @param list expected LIST count
   * @return a probe list
   */
  private OperationCostValidator.ExpectedProbe
      expectNonauthHeadList(int head, int list) {
    return expectHeadList(isNonAuth(), head, list);
  }

  /**
   * Execute a closure expecting a specific number of HEAD/LIST calls
   * on <i>raw</i> S3 stores only.
   * @param head expected head request count.
   * @param list expected list request count.
   * @param eval closure to evaluate
   * @param <T> return type of closure
   * @return the result of the evaluation
   */
  private <T> T verifyRawHeadList(
      int head,
      int list,
      Callable<T> eval) throws Exception {
    return verifyMetrics(eval,
        expectRawHeadList(head, list));
  }

  /**
   * Execute innerGetFileStatus for the given probes
   * and expect in raw FS to have the specific HEAD/LIST count.
   */
  public S3AFileStatus verifyRawGetFileStatus(final Path path,
      boolean needEmptyDirectoryFlag,
      Set<StatusProbeEnum> probes, int head, int list) throws Exception {
    return verifyRawHeadList(head, list, () ->
        getFileSystem().innerGetFileStatus(path, needEmptyDirectoryFlag,
            probes));
  }

  /**
   * Execute innerGetFileStatus for the given probes and expect failure
   * and expect in raw FS to have the specific HEAD/LIST count.
   */
  public void verifyRawGetFileStatusFNFE(final Path path,
      boolean needEmptyDirectoryFlag,
      Set<StatusProbeEnum> probes, int head, int list) throws Exception {
    verifyRawHeadListIntercepting(FileNotFoundException.class, "",
        head, list, () ->
            getFileSystem().innerGetFileStatus(path, needEmptyDirectoryFlag,
                probes));
  }

  /**
   * Probe for a path being a directory.
   * Metrics are only checked on unguarded stores.
   * @param path path
   * @param expected expected outcome
   * @param head head count (unguarded)
   * @param list listCount (unguarded)
   */
  private void isDir(Path path, boolean expected,
      int head, int list) throws Exception {
    boolean b = verifyRawHeadList(head, list, () ->
        getFileSystem().isDirectory(path));
    Assertions.assertThat(b)
        .describedAs("isDirectory(%s)", path)
        .isEqualTo(expected);
  }

  /**
   * Probe for a path being a file.
   * Metrics are only checked on unguarded stores.
   * @param path path
   * @param expected expected outcome
   * @param head head count (unguarded)
   * @param list listCount (unguarded)
   */
  private void isFile(Path path, boolean expected,
      int head, int list) throws Exception {
    boolean b = verifyRawHeadList(head, list, () ->
        getFileSystem().isFile(path));
    Assertions.assertThat(b)
        .describedAs("isFile(%s)", path)
        .isEqualTo(expected);
  }

  /**
   * A metric diff which must always hold.
   * @param Statistic metric source
   * @param expected expected value.
   * @return the diff.
   */
  private OperationCostValidator.ExpectedProbe always(
      final Statistic Statistic, final int expected) {
    return probe(Statistic, expected);
  }

  /**
   * A metric diff which must hold when the fs is unguarded.
   * @param Statistic metric source
   * @param expected expected value.
   * @return the diff.
   */
  private OperationCostValidator.ExpectedProbe raw(
      final Statistic Statistic, final int expected) {
    return probe(isRaw, Statistic, expected);
  }

  /**
   * A metric diff which must hold when the fs is guarded.
   * @param Statistic metric source
   * @param expected expected value.
   * @return the diff.
   */
  private OperationCostValidator.ExpectedProbe guarded(
      final Statistic Statistic,
      final int expected) {
    return probe(isGuarded, Statistic, expected);
  }

  /**
   * A metric diff which must hold when the fs is guarded + authoritative.
   * @param Statistic metric source
   * @param expected expected value.
   * @return the diff.
   */
  private OperationCostValidator.ExpectedProbe authoritative(
      final Statistic Statistic,
      final int expected) {
    return probe(isAuthoritative, Statistic, expected);
  }

  /**
   * A metric diff which must hold when the fs is guarded + authoritative.
   * @param Statistic metric source
   * @param expected expected value.
   * @return the diff.
   */
  private OperationCostValidator.ExpectedProbe nonauth(
      final Statistic Statistic,
      final int expected) {
    return probe(isNonAuth(), Statistic, expected);
  }

  /**
   * A metric diff which must hold when the fs is keeping markers
   * @param Statistic metric source
   * @param expected expected value.
   * @return the diff.
   */
  private OperationCostValidator.ExpectedProbe keeping(
      final Statistic Statistic,
      final int expected) {
    return probe(isKeepingMarkers(), Statistic, expected);
  }

  /**
   * A metric diff which must hold when the fs is keeping markers
   * @param Statistic metric source
   * @param expected expected value.
   * @return the diff.
   */
  private OperationCostValidator.ExpectedProbe deleting(
      final Statistic Statistic,
      final int expected) {
    return probe(isDeleting, Statistic, expected);
  }


  /**
   * A special object whose toString() value is the current
   * state of the metrics.
   */
  private final Object metricSummary = costValidator;

}
