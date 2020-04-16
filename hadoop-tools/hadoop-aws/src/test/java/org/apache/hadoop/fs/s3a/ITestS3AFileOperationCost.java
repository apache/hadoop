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
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileAlreadyExistsException;
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
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_KEEP;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.Statistic.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.test.AssertExtensions.dynamicDescription;
import static org.apache.hadoop.test.GenericTestUtils.getTestDir;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Use metrics to assert about the cost of file status queries.
 * {@link S3AFileSystem#getFileStatus(Path)}.
 * Parameterized on guarded vs raw. and directory marker keep vs delete
 */
@RunWith(Parameterized.class)
public class ITestS3AFileOperationCost extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AFileOperationCost.class);

  /*
   * constants declaring operation costs in HEAD and LIST.
   */


  /* getFileStatus() directory probe only. */
  public static final int GFS_DIR_PROBE_H = 0;

  public static final int GFS_DIR_PROBE_L = 1;

  /* getFileStatus() file probe only. */
  public static final int GFS_FILE_PROBE_H = 1;

  public static final int GFS_FILE_PROBE_L = 0;

  /* getFileStatus() on a file which exists. */
  public static final int GFS_SINGLE_FILE_H = GFS_FILE_PROBE_H;
  public static final int GFS_SINGLE_FILE_L = 0;

  /* getFileStatus() directory marker which exists. */
  public static final int GFS_MARKER_H = GFS_FILE_PROBE_H;
  public static final int GFS_MARKER_L = GFS_DIR_PROBE_L;

  /* getFileStatus() directory which is non-empty. */
  public static final int GFS_DIR_H = GFS_FILE_PROBE_H;
  public static final int GFS_DIR_L = GFS_DIR_PROBE_L;

  /* getFileStatus() no file or dir. */
  public static final int GFS_FNFE_H = GFS_FILE_PROBE_H;
  public static final int GFS_FNFE_L = GFS_DIR_PROBE_L;

  public static final int DELETE_OBJECT_REQUEST = 1;
  public static final int DELETE_MARKER_REQUEST = 1;

  /** listLocatedStatus always does a list. */
  public static final int LLS_ALWAYS_L = 1;

  /** source is found, dest not found, copy metadataRequests */
  public static final int RENAME_SINGLE_FILE_RENAME_H =
      GFS_FILE_PROBE_H + GFS_FNFE_H + 1;

  /**
   * LIST on dest not found, look for dest dir, and then, at
   * end of rename, whether a parent dir needs to be created.
   */
  public static final int RENAME_SINGLE_FILE_RENAME_DIFFERENT_DIR_L =
      GFS_FNFE_L + GFS_DIR_L * 2;

  /* All the metrics which can be used in assertions */

  private MetricDiff copyLocalOps;
  private MetricDiff copyRequests;
  private MetricDiff deleteRequests;
  private MetricDiff directoriesCreated;
  private MetricDiff directoriesDeleted;
  private MetricDiff fakeDirectoriesDeleted;
  private MetricDiff filesDeleted;
  private MetricDiff listRequests;
  private MetricDiff metadataRequests;
  private MetricDiff putBytes;
  private MetricDiff putRequests;

  /**
   * Array of all metrics built up in setup.
   */
  private MetricDiff[] allMetrics;

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

    copyLocalOps = mkdiff(INVOCATION_COPY_FROM_LOCAL_FILE);
    copyRequests = mkdiff(OBJECT_COPY_REQUESTS);
    deleteRequests = mkdiff(OBJECT_DELETE_REQUESTS);
    directoriesCreated = mkdiff(DIRECTORIES_CREATED);
    directoriesDeleted = mkdiff(DIRECTORIES_DELETED);
    fakeDirectoriesDeleted = mkdiff(FAKE_DIRECTORIES_DELETED);
    filesDeleted = mkdiff(FILES_DELETED);
    listRequests = mkdiff(OBJECT_LIST_REQUESTS);
    metadataRequests = mkdiff(OBJECT_METADATA_REQUESTS);
    putBytes = mkdiff(OBJECT_PUT_BYTES);
    putRequests = mkdiff(OBJECT_PUT_REQUESTS);
    allMetrics = new MetricDiff[]{
        copyLocalOps,
        copyRequests,
        deleteRequests,
        directoriesCreated,
        directoriesDeleted,
        fakeDirectoriesDeleted,
        filesDeleted,
        listRequests,
        metadataRequests,
        putBytes,
        putRequests
    };

  }

  public MetricDiff mkdiff(final Statistic statistic) {
    return new MetricDiff(getFileSystem(), statistic);
  }

  public void assumeUnguarded() {
    assume("Unguarded FS only", !isGuarded());
  }

  public boolean isAuthoritative() {
    return authoritative;
  }

  public boolean isGuarded() {
    return s3guard;
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
        raw(listRequests, LLS_ALWAYS_L),
        nonauth(listRequests, LLS_ALWAYS_L),
        raw(metadataRequests, GFS_FILE_PROBE_H));
  }

  @Test
  public void testCostOfListLocatedStatusOnEmptyDir() throws Throwable {
    describe("performing listLocatedStatus on an empty dir");
    Path dir = dir(methodPath());
    S3AFileSystem fs = getFileSystem();
    verifyMetrics(() ->
            fs.listLocatedStatus(dir),
        raw(metadataRequests, GFS_FILE_PROBE_H),
        raw(listRequests, LLS_ALWAYS_L + GFS_DIR_L),
        guarded(metadataRequests, 0),
        authoritative(listRequests, 0),
        nonauth(listRequests, LLS_ALWAYS_L));
  }

  @Test
  public void testCostOfListLocatedStatusOnNonEmptyDir() throws Throwable {
    describe("performing listLocatedStatus on a non empty dir");
    Path dir = dir(methodPath());
    S3AFileSystem fs = getFileSystem();
    Path file = file(new Path(dir, "file.txt"));
    verifyMetrics(() ->
          fs.listLocatedStatus(dir),
        always(metadataRequests, 0),
        raw(listRequests, LLS_ALWAYS_L),
        authoritative(listRequests, 0),
        nonauth(listRequests, LLS_ALWAYS_L));
  }

  @Test
  public void testCostOfGetFileStatusOnFile() throws Throwable {
    describe("performing getFileStatus on a file");
    Path simpleFile = file(methodPath());
    S3AFileSystem fs = getFileSystem();
    S3AFileStatus status = verifyRawGetFileStatus(simpleFile, true,
        StatusProbeEnum.ALL, GFS_SINGLE_FILE_H, GFS_SINGLE_FILE_L);
    assertTrue("not a file: " + status, status.isFile());
  }

  @Test
  public void testCostOfGetFileStatusOnEmptyDir() throws Throwable {
    describe("performing getFileStatus on an empty directory");
    Path dir = dir(methodPath());
    S3AFileStatus status = verifyRawGetFileStatus(dir, true,
            StatusProbeEnum.ALL, GFS_MARKER_H, GFS_MARKER_L);
    assertSame("not empty: " + status, Tristate.TRUE,
        status.isEmptyDirectory());
    // but now only ask for the directories and the file check is skipped.
    verifyRawGetFileStatus(dir, false,
        StatusProbeEnum.DIRECTORIES, GFS_DIR_PROBE_H, GFS_MARKER_L);

    // now look at isFile/isDir against the same entry
    isDir(dir, true, 0, GFS_MARKER_L);
    isFile(dir, false, GFS_SINGLE_FILE_H, GFS_SINGLE_FILE_L);
  }

  @Test
  public void testCostOfGetFileStatusOnMissingFile() throws Throwable {
    describe("performing getFileStatus on a missing file");
    verifyRawGetFileStatusFNFE(methodPath(), false,
        StatusProbeEnum.ALL,
        GFS_FNFE_H, GFS_FNFE_L);
  }

  @Test
  public void testIsDirIsFileMissingPath() throws Throwable {
    describe("performing isDir and isFile on a missing file");
    Path path = methodPath();
    // now look at isFile/isDir against the same entry
    isDir(path, false, GFS_DIR_PROBE_H, GFS_DIR_PROBE_L);
    isFile(path, false, GFS_FILE_PROBE_H, GFS_FILE_PROBE_L);
  }

  @Test
  public void testCostOfGetFileStatusOnNonEmptyDir() throws Throwable {
    describe("performing getFileStatus on a non-empty directory");
    Path dir = dir(methodPath());
    Path simpleFile = file(new Path(dir, "simple.txt"));
    S3AFileStatus status = verifyRawGetFileStatus(dir, true,
            StatusProbeEnum.ALL, GFS_DIR_H, GFS_DIR_L);
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
        // delete file then look for parent
        raw(metadataRequests, GFS_FILE_PROBE_H +
            (isKeepingMarkers()? 0: GFS_DIR_PROBE_H)),
        raw(listRequests, GFS_FILE_PROBE_L + GFS_DIR_PROBE_L),
        always(directoriesDeleted, 0),
        always(filesDeleted, 1),

        // keeping: create no parent dirs or delete parents
        keeping(directoriesCreated, 0),
        keeping(deleteRequests, DELETE_OBJECT_REQUEST),

        // deleting: create a parent and delete any of its parents
        deleting(directoriesCreated, 1),
        deleting(deleteRequests,
            DELETE_OBJECT_REQUEST + DELETE_MARKER_REQUEST)
    );
    // there is an empty dir for a parent
    S3AFileStatus status = verifyRawGetFileStatus(dir, true,
        StatusProbeEnum.ALL, GFS_DIR_H, GFS_DIR_L);
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
          always(copyLocalOps, 1),
          always(putRequests, 1),
          always(putBytes, len));
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
        always(directoriesCreated, 1),
        always(directoriesDeleted, 0),
        keeping(deleteRequests, 0),
        keeping(fakeDirectoriesDeleted, 0),
        deleting(deleteRequests, DELETE_MARKER_REQUEST),
        // delete all possible fake dirs above the subdirectory
        deleting(fakeDirectoriesDeleted, directoriesInPath(subDir) - 1));
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
        always(directoriesCreated, 0),
        always(directoriesDeleted, 0),
        // keeping: no delete operations.
        keeping(deleteRequests, 0),
        keeping(fakeDirectoriesDeleted, 0),
        // delete all possible fake dirs above the file
        deleting(deleteRequests, 1),
        deleting(fakeDirectoriesDeleted, directoriesInPath(srcDir)));
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
        raw(metadataRequests, RENAME_SINGLE_FILE_RENAME_H),
        raw(listRequests, RENAME_SINGLE_FILE_RENAME_DIFFERENT_DIR_L),
        always(directoriesCreated, 0),
        always(directoriesDeleted, 0),
        // keeping: only the core delete operation is issued.
        keeping(deleteRequests, DELETE_OBJECT_REQUEST),
        keeping(fakeDirectoriesDeleted, 0),
        // deleting: delete any fake marker above the destination.
        deleting(deleteRequests,
            DELETE_OBJECT_REQUEST + DELETE_MARKER_REQUEST),
        deleting(fakeDirectoriesDeleted, directoriesInPath(destDir)));

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
        raw(metadataRequests, RENAME_SINGLE_FILE_RENAME_H),
        raw(listRequests, GFS_FNFE_L),
        always(copyRequests, 1),
        always(directoriesCreated, 0),
        always(deleteRequests, DELETE_OBJECT_REQUEST),
        always(fakeDirectoriesDeleted, 0));
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
          raw(metadataRequests, RENAME_SINGLE_FILE_RENAME_H),
          raw(listRequests, GFS_FNFE_L),
          // here we expect there to be no fake directories
          always(directoriesCreated, 0),
          // one for the renamed file only
          always(deleteRequests, DELETE_OBJECT_REQUEST),
          // no directories are deleted: This is root
          always(directoriesDeleted, 0),
          // no fake directories are deleted: This is root
          always(fakeDirectoriesDeleted, 0),
          always(filesDeleted, 1));

      // delete that destination file, assert only the file delete was issued
      verifyMetrics(() -> {
        fs.delete(dest, false);
        return "after fs.delete(/dest) " + metricSummary;
        },
          always(directoriesCreated, 0),
          always(directoriesDeleted, 0),
          always(fakeDirectoriesDeleted, 0),
          always(filesDeleted, 1),
          always(deleteRequests, DELETE_OBJECT_REQUEST),
          raw(metadataRequests, GFS_FILE_PROBE_H),
          raw(listRequests, 0)   /* no need to look at parent. */
          );

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
    verifyRawGetFileStatusFNFE(emptydir, false, StatusProbeEnum.HEAD_ONLY,
        GFS_FILE_PROBE_H, GFS_FILE_PROBE_L);

    // a LIST will find it and declare as empty
    S3AFileStatus status = verifyRawGetFileStatus(emptydir, true,
        StatusProbeEnum.LIST_ONLY, 0, GFS_MARKER_L);
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
    status = verifyRawHeadList(0, GFS_MARKER_L, () ->
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
    create(testFile, false, GFS_FNFE_H, GFS_FNFE_L);
    // but when true: only the directory checks take place.
    create(testFile, true, 0, GFS_FNFE_L);
  }

  @Test
  public void testCreateCostFileExists() throws Throwable {
    describe("Test cost of create file failing with existing file");
    assumeUnguarded();
    Path testFile = file(methodPath());

    // now there is a file there, an attempt with overwrite == false will
    // fail on the first HEAD.
    verifyRawHeadListIntercepting(FileAlreadyExistsException.class, "",
        GFS_FILE_PROBE_H, 0,
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
        GFS_MARKER_H, GFS_MARKER_L,
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
        GFS_FILE_PROBE_H,   // destination file
        GFS_DIR_PROBE_L * 2);    // destination file and parent dir
    // recursive = false and overwrite=true:
    // only make sure the dest path isn't a directory.
    buildFile(testFile, true, true, GFS_DIR_PROBE_H, GFS_DIR_PROBE_L);

    // now there is a file there, an attempt with overwrite == false will
    // fail on the first HEAD.
    verifyRawHeadListIntercepting(FileAlreadyExistsException.class, "",
        GFS_FILE_PROBE_H, 0, () ->
            buildFile(testFile, false, true, GFS_FILE_PROBE_H, 0));
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
      int list) throws IOException {
    resetMetricDiffs();
    FSDataOutputStreamBuilder builder = getFileSystem().createFile(path)
        .overwrite(overwrite);
    if (recursive) {
      builder.recursive();
    }
    builder.build().close();
    verifyOperationCount(head, list);
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

  private boolean reset(MetricDiff... diffs) {
    for (MetricDiff diff : diffs) {
      diff.reset();
    }
    return true;
  }

  /**
   * Reset all the metrics being tracked.
   */
  private void resetMetricDiffs() {
    reset(allMetrics);
  }

  /**
   * Verify that the head and list calls match expectations
   * against unguarded stores.
   * then reset the counters ready for the next operation.
   * @param head expected HEAD count
   * @param list expected LIST count
   */
  private void verifyOperationCount(int head, int list) {
    if (!isGuarded()) {
      metadataRequests.assertDiffEquals(head);
      listRequests.assertDiffEquals(list);
    }
    resetMetricDiffs();
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
      ExpectedDiff... expected) throws Exception {
    resetMetricDiffs();
    T r = eval.call();
    String text = r.toString();
    for (ExpectedDiff ed : expected) {
      ed.verify(text);
    }
    return r;
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
      ExpectedDiff... expected) throws Exception {
    resetMetricDiffs();
    E e = intercept(clazz, eval);
    for (ExpectedDiff ed : expected) {
      ed.verify(text);
    }
    return e;
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
        raw(metadataRequests, head),
        raw(listRequests, list));
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
        raw(metadataRequests, head),
        raw(listRequests, list));
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
   * @param metricDiff metric source
   * @param expected expected value.
   * @return the diff.
   */
  private ExpectedDiff always(final MetricDiff metricDiff, final int expected) {
    return new ExpectedDiff(metricDiff, expected, ExpectedDiffCriteria.Always);
  }

  /**
   * A metric diff which must hold when the fs is unguarded.
   * @param metricDiff metric source
   * @param expected expected value.
   * @return the diff.
   */
  private ExpectedDiff raw(final MetricDiff metricDiff, final int expected) {
    return new ExpectedDiff(metricDiff, expected,
        ExpectedDiffCriteria.Unguarded);
  }

  /**
   * A metric diff which must hold when the fs is guarded.
   * @param metricDiff metric source
   * @param expected expected value.
   * @return the diff.
   */
  private ExpectedDiff guarded(final MetricDiff metricDiff,
      final int expected) {
    return new ExpectedDiff(metricDiff, expected,
        ExpectedDiffCriteria.Guarded);
  }

  /**
   * A metric diff which must hold when the fs is guarded + authoritative.
   * @param metricDiff metric source
   * @param expected expected value.
   * @return the diff.
   */
  private ExpectedDiff authoritative(final MetricDiff metricDiff,
      final int expected) {
    return new ExpectedDiff(metricDiff, expected,
        ExpectedDiffCriteria.Authoritative);
  }

  /**
   * A metric diff which must hold when the fs is guarded + authoritative.
   * @param metricDiff metric source
   * @param expected expected value.
   * @return the diff.
   */
  private ExpectedDiff nonauth(final MetricDiff metricDiff,
      final int expected) {
    return new ExpectedDiff(metricDiff, expected,
        ExpectedDiffCriteria.NonAuth);
  }

  /**
   * A metric diff which must hold when the fs is keeping markers
   * @param metricDiff metric source
   * @param expected expected value.
   * @return the diff.
   */
  private ExpectedDiff keeping(final MetricDiff metricDiff,
      final int expected) {
    return new ExpectedDiff(metricDiff, expected,
        ExpectedDiffCriteria.Keeping);
  }

  /**
   * A metric diff which must hold when the fs is keeping markers
   * @param metricDiff metric source
   * @param expected expected value.
   * @return the diff.
   */
  private ExpectedDiff deleting(final MetricDiff metricDiff,
      final int expected) {
    return new ExpectedDiff(metricDiff, expected,
        ExpectedDiffCriteria.Deleting);
  }

  /**
   * Criteria an for ExpectedDiff to use.
   */
  private enum ExpectedDiffCriteria {
    Guarded,
    Unguarded,
    Always,
    Keeping,
    Deleting,
    Authoritative,
    NonAuth
  }

  /**
   * An expected diff to verify given criteria to trigger an eval.
   */
  private final class ExpectedDiff {

    private final MetricDiff metricDiff;

    private final int expected;

    private final ExpectedDiffCriteria criteria;

    /**
     * Create.
     * @param metricDiff diff to evaluate.
     * @param expected expected value.
     * @param criteria criteria to trigger evaluation.
     */
    private ExpectedDiff(final MetricDiff metricDiff,
        final int expected,
        final ExpectedDiffCriteria criteria) {
      this.metricDiff = metricDiff;
      this.expected = expected;
      this.criteria = criteria;
    }

    /**
     * Verify a diff if the FS instance is compatible.
     * @param message message to print; metric name is appended
     */
    private void verify(String message) {
      boolean isGuarded = isGuarded();
      S3AFileSystem fs = getFileSystem();
      boolean probe;
      switch (criteria) {
      case Guarded:
        probe = isGuarded;
        break;
      case Unguarded:
        probe = !isGuarded;
        break;
      case Authoritative:
        probe = isGuarded && isAuthoritative();
        break;
      case NonAuth:
        probe = isGuarded && !isAuthoritative();
        break;
      case Keeping:
        probe = isKeepingMarkers();
        break;
      case Deleting:
        probe = !isKeepingMarkers();
        break;
      case Always:
      default:
        probe = true;
        break;
      }
      if (probe) {
        metricDiff.assertDiffEquals(criteria + ": " + message, expected);
      }
    }
  }

  /**
   * A special object whose toString() value is the current
   * state of the metrics.
   */
  private final Object metricSummary = new Object() {
    @Override
    public String toString() {
      return Arrays.stream(allMetrics)
          .map(MetricDiff::toString)
          .collect(Collectors.joining(", "));
    }
  };

}
