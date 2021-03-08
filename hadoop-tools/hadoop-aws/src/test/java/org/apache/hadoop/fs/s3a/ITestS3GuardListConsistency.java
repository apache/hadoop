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

import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.contract.s3a.S3AContract;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.FailureInjectionPolicy.*;
import static org.apache.hadoop.fs.s3a.InconsistentAmazonS3Client.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test S3Guard list consistency feature by injecting delayed listObjects()
 * visibility via {@link InconsistentAmazonS3Client}.
 *
 * Tests here generally:
 * 1. Use the inconsistency injection mentioned above.
 * 2. Only run when S3Guard is enabled.
 */
public class ITestS3GuardListConsistency extends AbstractS3ATestBase {

  private Invoker invoker;

  @Override
  public void setup() throws Exception {
    super.setup();
    invoker = new Invoker(new S3ARetryPolicy(getConfiguration()),
        Invoker.NO_OP
    );
    Assume.assumeTrue("No metadata store in test filesystem",
        getFileSystem().hasMetadataStore());
  }

  @Override
  public void teardown() throws Exception {
    clearInconsistency(getFileSystem());
    super.teardown();
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    conf.setClass(S3_CLIENT_FACTORY_IMPL, InconsistentS3ClientFactory.class,
        S3ClientFactory.class);
    // Other configs would break test assumptions
    conf.set(FAIL_INJECT_INCONSISTENCY_KEY, DEFAULT_DELAY_KEY_SUBSTRING);
    conf.setFloat(FAIL_INJECT_INCONSISTENCY_PROBABILITY, 1.0f);
    // this is a long value to guarantee that the inconsistency holds
    // even over long-haul connections, and in the debugger too/
    conf.setLong(FAIL_INJECT_INCONSISTENCY_MSEC, 600_1000L);
    return new S3AContract(conf);
  }

  /**
   * Helper function for other test cases: does a single rename operation and
   * validates the aftermath.
   * @param mkdirs Directories to create
   * @param srcdirs Source paths for rename operation
   * @param dstdirs Destination paths for rename operation
   * @param yesdirs Files that must exist post-rename (e.g. srcdirs children)
   * @param nodirs Files that must not exist post-rename (e.g. dstdirs children)
   * @throws Exception
   */
  private void doTestRenameSequence(Path[] mkdirs, Path[] srcdirs,
      Path[] dstdirs, Path[] yesdirs, Path[] nodirs) throws Exception {
    S3AFileSystem fs = getFileSystem();


    if (mkdirs != null) {
      for (Path mkdir : mkdirs) {
        assertTrue(fs.mkdirs(mkdir));
      }
      clearInconsistency(fs);
    }

    assertEquals("srcdirs and dstdirs must have equal length",
        srcdirs.length, dstdirs.length);
    for (int i = 0; i < srcdirs.length; i++) {
      assertTrue("Rename returned false: " + srcdirs[i] + " -> " + dstdirs[i],
          fs.rename(srcdirs[i], dstdirs[i]));
    }

    for (Path yesdir : yesdirs) {
      assertTrue("Path was supposed to exist: " + yesdir, fs.exists(yesdir));
    }
    for (Path nodir : nodirs) {
      assertFalse("Path is not supposed to exist: " + nodir, fs.exists(nodir));
    }
  }

  /**
   * Delete an array of paths; log exceptions.
   * @param paths paths to delete
   */
  private void deletePathsQuietly(Path...paths) {
    for (Path dir : paths) {
      try {
        getFileSystem().delete(dir, true);
      } catch (IOException e) {
        LOG.info("Failed to delete {}: {}", dir, e.toString());
        LOG.debug("Delete failure:, e");
      }
    }
  }

  /**
   * Tests that after renaming a directory, the original directory and its
   * contents are indeed missing and the corresponding new paths are visible.
   * @throws Exception
   */
  @Test
  public void testConsistentListAfterRename() throws Exception {
    Path d1f = path("d1/f");
    Path d1f2 = path("d1/f-" + DEFAULT_DELAY_KEY_SUBSTRING);
    Path[] mkdirs = {d1f, d1f2};
    Path d1 = path("d1");
    Path[] srcdirs = {d1};
    Path d2 = path("d2");
    Path[] dstdirs = {d2};
    Path d2f2 = path("d2/f-" + DEFAULT_DELAY_KEY_SUBSTRING);
    Path[] yesdirs = {d2, path("d2/f"), d2f2};
    Path[] nodirs = {
        d1, d1f, d1f2};
    try {
      doTestRenameSequence(mkdirs, srcdirs, dstdirs, yesdirs, nodirs);
    } finally {
      clearInconsistency(getFileSystem());
      deletePathsQuietly(d1, d2, d1f, d1f2, d2f2);
    }
  }

  /**
   * Tests a circular sequence of renames to verify that overwriting recently
   * deleted files and reading recently created files from rename operations
   * works as expected.
   * @throws Exception
   */
  @Test
  public void testRollingRenames() throws Exception {
    Path[] dir0 = {path("rolling/1")};
    Path[] dir1 = {path("rolling/2")};
    Path[] dir2 = {path("rolling/3")};
    // These sets have to be in reverse order compared to the movement
    Path[] setA = {dir1[0], dir0[0]};
    Path[] setB = {dir2[0], dir1[0]};
    Path[] setC = {dir0[0], dir2[0]};

    try {
      for(int i = 0; i < 2; i++) {
        Path[] firstSet = i == 0 ? setA : null;
        doTestRenameSequence(firstSet, setA, setB, setB, dir0);
        doTestRenameSequence(null, setB, setC, setC, dir1);
        doTestRenameSequence(null, setC, setA, setA, dir2);
      }

      S3AFileSystem fs = getFileSystem();
      intercept(FileNotFoundException.class, () ->
          fs.rename(dir2[0], dir1[0]));
      assertTrue("Renaming over existing file should have succeeded",
          fs.rename(dir1[0], dir0[0]));
    } finally {
      clearInconsistency(getFileSystem());
      deletePathsQuietly(dir0[0], dir1[0], dir2[0]);
    }
  }

  /**
   * Tests that deleted files immediately stop manifesting in list operations
   * even when the effect in S3 is delayed.
   * @throws Exception
   */
  @Test
  public void testConsistentListAfterDelete() throws Exception {
    S3AFileSystem fs = getFileSystem();

    // Any S3 keys that contain DELAY_KEY_SUBSTRING will be delayed
    // in listObjects() results via InconsistentS3Client
    Path inconsistentPath =
        path("a/b/dir3-" + DEFAULT_DELAY_KEY_SUBSTRING);

    Path dir1 = path("a/b/dir1");
    Path dir2 = path("a/b/dir2");
    Path[] testDirs = {
        dir1,
        dir2,
        inconsistentPath};

    for (Path path : testDirs) {
      assertTrue("Can't create directory: " + path, fs.mkdirs(path));
    }
    clearInconsistency(fs);
    for (Path path : testDirs) {
      assertTrue("Can't delete path: " + path, fs.delete(path, false));
    }

    FileStatus[] paths = fs.listStatus(path("a/b/"));
    List<Path> list = new ArrayList<>();
    for (FileStatus fileState : paths) {
      list.add(fileState.getPath());
    }
    Assertions.assertThat(list)
        .describedAs("Expected deleted files to be excluded")
        .doesNotContain(dir1)
        .doesNotContain(dir2)
        .doesNotContain(inconsistentPath);
  }

  /**
   * Tests that rename immediately after files in the source directory are
   * deleted results in exactly the correct set of destination files and none
   * of the source files.
   * @throws Exception
   */
  @Test
  public void testConsistentRenameAfterDelete() throws Exception {
    S3AFileSystem fs = getFileSystem();

    // Any S3 keys that contain DELAY_KEY_SUBSTRING will be delayed
    // in listObjects() results via InconsistentS3Client
    Path inconsistentPath =
        path("a/b/dir3-" + DEFAULT_DELAY_KEY_SUBSTRING);

    Path[] testDirs = {path("a/b/dir1"),
        path("a/b/dir2"),
        inconsistentPath};

    for (Path path : testDirs) {
      assertTrue(fs.mkdirs(path));
    }
    clearInconsistency(fs);
    assertTrue(fs.delete(testDirs[1], false));
    assertTrue(fs.delete(testDirs[2], false));

    ContractTestUtils.rename(fs, path("a"), path("a3"));
    ContractTestUtils.assertPathsDoNotExist(fs,
            "Source paths shouldn't exist post rename operation",
            testDirs[0], testDirs[1], testDirs[2]);
    FileStatus[] paths = fs.listStatus(path("a3/b"));
    List<Path> list = new ArrayList<>();
    for (FileStatus fileState : paths) {
      list.add(fileState.getPath());
    }
    Assertions.assertThat(list)
        .contains(path("a3/b/dir1"))
        .doesNotContain(path("a3/b/dir2"))
        .doesNotContain(path("a3/b/dir3-" +
            DEFAULT_DELAY_KEY_SUBSTRING));

    intercept(FileNotFoundException.class, "",
        "Recently renamed dir should not be visible",
        () -> S3AUtils.mapLocatedFiles(
            fs.listFilesAndEmptyDirectories(path("a"), true),
            FileStatus::getPath));
  }

  @Test
  public void testConsistentListStatusAfterPut() throws Exception {

    S3AFileSystem fs = getFileSystem();

    // Any S3 keys that contain DELAY_KEY_SUBSTRING will be delayed
    // in listObjects() results via InconsistentS3Client
    Path inconsistentPath =
        path("a/b/dir3-" + DEFAULT_DELAY_KEY_SUBSTRING);

    Path[] testDirs = {path("a/b/dir1"),
        path("a/b/dir2"),
        inconsistentPath};

    for (Path path : testDirs) {
      assertTrue(fs.mkdirs(path));
    }

    FileStatus[] paths = fs.listStatus(path("a/b/"));
    List<Path> list = new ArrayList<>();
    for (FileStatus fileState : paths) {
      list.add(fileState.getPath());
    }
    Assertions.assertThat(list)
        .contains(path("a/b/dir1"))
        .contains(path("a/b/dir2"))
        .contains(inconsistentPath);
  }

  /**
   * Similar to {@link #testConsistentListStatusAfterPut()}, this tests that the
   * FS listLocatedStatus() call will return consistent list.
   */
  @Test
  public void testConsistentListLocatedStatusAfterPut() throws Exception {
    final S3AFileSystem fs = getFileSystem();
    String rootDir = "doTestConsistentListLocatedStatusAfterPut";
    fs.mkdirs(path(rootDir));

    final int[] numOfPaths = {0, 1, 5};
    for (int normalPathNum : numOfPaths) {
      for (int delayedPathNum : new int[] {0, 2}) {
        LOG.info("Testing with normalPathNum={}, delayedPathNum={}",
            normalPathNum, delayedPathNum);
        doTestConsistentListLocatedStatusAfterPut(fs, rootDir, normalPathNum,
            delayedPathNum);
      }
    }
  }

  /**
   * Helper method to implement the tests of consistent listLocatedStatus().
   * @param fs The S3 file system from contract
   * @param normalPathNum number paths listed directly from S3 without delaying
   * @param delayedPathNum number paths listed with delaying
   * @throws Exception
   */
  private void doTestConsistentListLocatedStatusAfterPut(S3AFileSystem fs,
      String rootDir, int normalPathNum, int delayedPathNum) throws Exception {
    final List<Path> testDirs = new ArrayList<>(normalPathNum + delayedPathNum);
    int index = 0;
    for (; index < normalPathNum; index++) {
      testDirs.add(path(rootDir + "/dir-" +
          index));
    }
    for (; index < normalPathNum + delayedPathNum; index++) {
      // Any S3 keys that contain DELAY_KEY_SUBSTRING will be delayed
      // in listObjects() results via InconsistentS3Client
      testDirs.add(path(rootDir + "/dir-" + index +
          DEFAULT_DELAY_KEY_SUBSTRING));
    }

    for (Path path : testDirs) {
      // delete the old test path (if any) so that when we call mkdirs() later,
      // the to delay directories will be tracked via putObject() request.
      fs.delete(path, true);
      assertTrue(fs.mkdirs(path));
    }

    // this should return the union data from S3 and MetadataStore
    final RemoteIterator<LocatedFileStatus> statusIterator =
        fs.listLocatedStatus(path(rootDir + "/"));
    List<Path> list = new ArrayList<>();
    for (; statusIterator.hasNext();) {
      list.add(statusIterator.next().getPath());
    }

    // This should fail without S3Guard, and succeed with it because part of the
    // children under test path are delaying visibility
    for (Path path : testDirs) {
      assertTrue("listLocatedStatus should list " + path, list.contains(path));
    }
  }

  /**
   * Tests that the S3AFS listFiles() call will return consistent file list.
   */
  @Test
  public void testConsistentListFiles() throws Exception {
    final S3AFileSystem fs = getFileSystem();

    final int[] numOfPaths = {0, 2};
    for (int dirNum : numOfPaths) {
      for (int normalFile : numOfPaths) {
        for (int delayedFile : new int[] {0, 1}) {
          for (boolean recursive : new boolean[] {true, false}) {
            doTestListFiles(fs, dirNum, normalFile, delayedFile, recursive);
          }
        }
      }
    }
  }

  /**
   * Helper method to implement the tests of consistent listFiles().
   *
   * The file structure has dirNum subdirectories, and each directory (including
   * the test base directory itself) has normalFileNum normal files and
   * delayedFileNum delayed files.
   *
   * @param fs The S3 file system from contract
   * @param dirNum number of subdirectories
   * @param normalFileNum number files in each directory without delay to list
   * @param delayedFileNum number files in each directory with delay to list
   * @param recursive listFiles recursively if true
   * @throws Exception if any unexpected error
   */
  private void doTestListFiles(S3AFileSystem fs, int dirNum, int normalFileNum,
      int delayedFileNum, boolean recursive) throws Exception {
    describe("Testing dirNum=%d, normalFile=%d, delayedFile=%d, "
        + "recursive=%s", dirNum, normalFileNum, delayedFileNum, recursive);
    final Path baseTestDir = path("doTestListFiles-" + dirNum + "-"
        + normalFileNum + "-" + delayedFileNum + "-" + recursive);
    // delete the old test path (if any) so that when we call mkdirs() later,
    // the to delay sub directories will be tracked via putObject() request.
    fs.delete(baseTestDir, true);

    // make subdirectories (if any)
    final List<Path> testDirs = new ArrayList<>(dirNum + 1);
    assertTrue(fs.mkdirs(baseTestDir));
    testDirs.add(baseTestDir);
    for (int i = 0; i < dirNum; i++) {
      final Path subdir = path(baseTestDir + "/dir-" + i);
      assertTrue(fs.mkdirs(subdir));
      testDirs.add(subdir);
    }

    final Collection<String> fileNames
        = new ArrayList<>(normalFileNum + delayedFileNum);
    int index = 0;
    for (; index < normalFileNum; index++) {
      fileNames.add("file-" + index);
    }
    for (; index < normalFileNum + delayedFileNum; index++) {
      // Any S3 keys that contain DELAY_KEY_SUBSTRING will be delayed
      // in listObjects() results via InconsistentS3Client
      fileNames.add("file-" + index + "-" + DEFAULT_DELAY_KEY_SUBSTRING);
    }

    int filesAndEmptyDirectories = 0;

    // create files under each test directory
    for (Path dir : testDirs) {
      for (String fileName : fileNames) {
        writeTextFile(fs, new Path(dir, fileName), "I, " + fileName, false);
        filesAndEmptyDirectories++;
      }
    }

    // this should return the union data from S3 and MetadataStore
    final RemoteIterator<LocatedFileStatus> statusIterator
        = fs.listFiles(baseTestDir, recursive);
    final Collection<Path> listedFiles = new HashSet<>();
    for (; statusIterator.hasNext();) {
      final FileStatus status = statusIterator.next();
      assertTrue("FileStatus " + status + " is not a file!", status.isFile());
      listedFiles.add(status.getPath());
    }
    LOG.info("S3AFileSystem::listFiles('{}', {}) -> {}",
        baseTestDir, recursive, listedFiles);

    // This should fail without S3Guard, and succeed with it because part of the
    // files to list are delaying visibility
    if (!recursive) {
      // in this case only the top level files are listed
      verifyFileIsListed(listedFiles, baseTestDir, fileNames);
      assertEquals("Unexpected number of files returned by listFiles() call",
          normalFileNum + delayedFileNum, listedFiles.size());
    } else {
      for (Path dir : testDirs) {
        verifyFileIsListed(listedFiles, dir, fileNames);
      }
      assertEquals("Unexpected number of files returned by listFiles() call",
          filesAndEmptyDirectories,
          listedFiles.size());
    }
  }

  private static void verifyFileIsListed(Collection<Path> listedFiles,
      Path currentDir, Collection<String> fileNames) {
    for (String fileName : fileNames) {
      final Path file = new Path(currentDir, fileName);
      assertTrue(file + " should have been listed", listedFiles.contains(file));
    }
  }

  @Test
  public void testCommitByRenameOperations() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    Path work = path("test-commit-by-rename-" + DEFAULT_DELAY_KEY_SUBSTRING);
    Path task00 = new Path(work, "task00");
    fs.mkdirs(task00);
    String name = "part-00";
    try (FSDataOutputStream out =
             fs.create(new Path(task00, name), false)) {
      out.writeChars("hello");
    }
    for (FileStatus stat : fs.listStatus(task00)) {
      fs.rename(stat.getPath(), work);
    }
    List<FileStatus> files = new ArrayList<>(2);
    for (FileStatus stat : fs.listStatus(work)) {
      if (stat.isFile()) {
        files.add(stat);
      }
    }
    assertFalse("renamed file " + name + " not found in " + work,
        files.isEmpty());
    assertEquals("more files found than expected in " + work
        + " " + ls(work), 1, files.size());
    FileStatus status = files.get(0);
    assertEquals("Wrong filename in " + status,
        name, status.getPath().getName());
  }

  @Test
  public void testInconsistentS3ClientDeletes() throws Throwable {
    describe("Verify that delete adds tombstones which block entries"
        + " returned in (inconsistent) listings");
    // Test only implemented for v2 S3 list API
    assumeV2ListAPI();

    S3AFileSystem fs = getFileSystem();
    Path root = path("testInconsistentS3ClientDeletes-"
        + DEFAULT_DELAY_KEY_SUBSTRING);
    for (int i = 0; i < 3; i++) {
      fs.mkdirs(new Path(root, "dir-" + i));
      touch(fs, new Path(root, "file-" + i));
      for (int j = 0; j < 3; j++) {
        touch(fs, new Path(new Path(root, "dir-" + i), "file-" + i + "-" + j));
      }
    }
    clearInconsistency(fs);

    String key = fs.pathToKey(root) + "/";

    LOG.info("Listing objects before executing delete()");
    ListObjectsV2Result preDeleteDelimited = listObjectsV2(fs, key, "/");
    ListObjectsV2Result preDeleteUndelimited = listObjectsV2(fs, key, null);

    LOG.info("Deleting the directory {}", root);
    fs.delete(root, true);
    LOG.info("Delete completed; listing results which must exclude deleted"
        + " paths");

    ListObjectsV2Result postDeleteDelimited = listObjectsV2(fs, key, "/");
    boolean stripTombstones = false;
    assertObjectSummariesEqual(
        "InconsistentAmazonS3Client added back objects incorrectly " +
            "in a non-recursive listing",
        preDeleteDelimited, postDeleteDelimited,
        stripTombstones);

    assertListSizeEqual("InconsistentAmazonS3Client added back prefixes incorrectly " +
            "in a non-recursive listing",
        preDeleteDelimited.getCommonPrefixes(),
        postDeleteDelimited.getCommonPrefixes());
    LOG.info("Executing Deep listing");
    ListObjectsV2Result postDeleteUndelimited = listObjectsV2(fs, key, null);
    assertObjectSummariesEqual("InconsistentAmazonS3Client added back objects"
            + " incorrectly in a recursive listing",
        preDeleteUndelimited, postDeleteUndelimited,
        stripTombstones);

    assertListSizeEqual("InconsistentAmazonS3Client added back prefixes incorrectly " +
            "in a recursive listing",
        preDeleteUndelimited.getCommonPrefixes(),
        postDeleteUndelimited.getCommonPrefixes()
    );
  }

  private void assertObjectSummariesEqual(final String message,
      final ListObjectsV2Result expected,
      final ListObjectsV2Result actual,
      final boolean stripTombstones) {
    assertCollectionsEqual(
        message,
        stringify(expected.getObjectSummaries(), stripTombstones),
        stringify(actual.getObjectSummaries(), stripTombstones));
  }

  List<String> stringify(List<S3ObjectSummary> objects,
      boolean stripTombstones) {
    return objects.stream()
        .filter(s -> !stripTombstones || !(s.getKey().endsWith("/")))
        .map(s -> s.getKey())
        .collect(Collectors.toList());
  }

  /**
   * Require the v2 S3 list API.
   */
  protected void assumeV2ListAPI() {
    Assume.assumeTrue(getConfiguration()
        .getInt(LIST_VERSION, DEFAULT_LIST_VERSION) == 2);
  }

  /**
   * Verify that delayed S3 listings doesn't stop the FS from deleting
   * a directory tree. This has not always been the case; this test
   * verifies the fix and prevents regression.
   */
  @Test
  public void testDeleteUsesS3Guard() throws Throwable {
    describe("Verify that delete() uses S3Guard to get a consistent"
        + " listing of its directory structure");
    assumeV2ListAPI();
    S3AFileSystem fs = getFileSystem();
    Path root = path(
        "testDeleteUsesS3Guard-" + DEFAULT_DELAY_KEY_SUBSTRING);
    for (int i = 0; i < 3; i++) {
      Path path = new Path(root, "file-" + i);
      touch(fs, path);
    }
    // we now expect the listing to miss these
    String key = fs.pathToKey(root) + "/";

    // verify that the inconsistent listing does not show these
    LOG.info("Listing objects before executing delete()");
    List<Path> preDeletePaths = objectsToPaths(listObjectsV2(fs, key, null));
    Assertions.assertThat(preDeletePaths)
        .isEmpty();
    // do the delete
    fs.delete(root, true);

    // now go through every file and verify that it is not there.
    // if you comment out the delete above and run this test case,
    // the assertion will fail; this is how the validity of the assertions
    // were verified.
    clearInconsistency(fs);
    List<Path> postDeletePaths =
        objectsToPaths(listObjectsV2(fs, key, null));
    Assertions.assertThat(postDeletePaths)
        .isEmpty();
  }

  private List<Path> objectsToPaths(ListObjectsV2Result r) {
    S3AFileSystem fs = getFileSystem();
    return r.getObjectSummaries().stream()
        .map(s -> fs.keyToQualifiedPath(s.getKey()))
        .collect(Collectors.toList());
  }

  /**
   * Tests that the file's eTag and versionId are preserved in recursive
   * listings.
   */
  @Test
  public void testListingReturnsVersionMetadata() throws Throwable {
    S3AFileSystem fs = getFileSystem();

    // write simple file
    Path parent = path(getMethodName());
    Path file = new Path(parent, "file1");
    try (FSDataOutputStream outputStream = fs.create(file)) {
      outputStream.writeChars("hello");
    }

    // get individual file status
    FileStatus[] fileStatuses = fs.listStatus(file);
    assertEquals(1, fileStatuses.length);
    S3AFileStatus status = (S3AFileStatus) fileStatuses[0];
    String eTag = status.getETag();
    assertNotNull("Etag in " + eTag, eTag);
    String versionId = status.getVersionId();

    // get status through recursive directory listing
    RemoteIterator<LocatedFileStatus> filesIterator = fs.listFiles(
        parent, true);
    List<LocatedFileStatus> files = Lists.newArrayList();
    while (filesIterator.hasNext()) {
      files.add(filesIterator.next());
    }
    Assertions.assertThat(files)
        .hasSize(1);

    // ensure eTag and versionId are preserved in directory listing
    S3ALocatedFileStatus locatedFileStatus =
        (S3ALocatedFileStatus) files.get(0);
    assertEquals("etag of " + locatedFileStatus,
        eTag, locatedFileStatus.getETag());
    assertEquals("versionID of " + locatedFileStatus,
        versionId, locatedFileStatus.getVersionId());
  }

  /**
   * Assert that the two collections match using
   * object equality of the elements within.
   * @param message text for the assertion
   * @param expected expected list
   * @param actual actual list
   * @param <T> type of list
   */
  private <T> void assertCollectionsEqual(String message,
      Collection<T> expected,
      Collection<T> actual) {
    Assertions.assertThat(actual)
        .describedAs(message)
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  /**
   * Assert that the two list sizes match; failure message includes the lists.
   * @param message text for the assertion
   * @param expected expected list
   * @param actual actual list
   * @param <T> type of list
   */
  private <T> void assertListSizeEqual(String message,
      List<T> expected,
      List<T> actual) {
    String leftContents = expected.stream()
        .map(n -> n.toString())
        .collect(Collectors.joining("\n"));
    String rightContents = actual.stream()
        .map(n -> n.toString())
        .collect(Collectors.joining("\n"));
    String summary = "\nExpected:" + leftContents
        + "\n-----------\n"
        + "Actual:" + rightContents
        + "\n-----------\n";

    if (expected.size() != actual.size()) {
      LOG.error(message + summary);
    }
    assertEquals(message + summary, expected.size(), actual.size());
  }

  /**
   * Retrying v2 list directly through the s3 client.
   * @param fs filesystem
   * @param key key to list under
   * @param delimiter any delimiter
   * @return the listing
   * @throws IOException on error
   */
  @Retries.RetryRaw
  private ListObjectsV2Result listObjectsV2(S3AFileSystem fs,
      String key, String delimiter) throws IOException {
    ListObjectsV2Request k = fs.createListObjectsRequest(key, delimiter)
        .getV2();
    return invoker.retryUntranslated("list", true,
        () -> {
          return fs.getAmazonS3ClientForTesting("list").listObjectsV2(k);
        });
  }

}
