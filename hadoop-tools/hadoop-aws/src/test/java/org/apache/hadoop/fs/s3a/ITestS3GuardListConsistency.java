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
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.junit.Assume;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.FailureInjectionPolicy.*;
import static org.apache.hadoop.fs.s3a.InconsistentAmazonS3Client.*;

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
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    conf.setClass(S3_CLIENT_FACTORY_IMPL, InconsistentS3ClientFactory.class,
        S3ClientFactory.class);
    // Other configs would break test assumptions
    conf.set(FAIL_INJECT_INCONSISTENCY_KEY, DEFAULT_DELAY_KEY_SUBSTRING);
    conf.setFloat(FAIL_INJECT_INCONSISTENCY_PROBABILITY, 1.0f);
    conf.setLong(FAIL_INJECT_INCONSISTENCY_MSEC, DEFAULT_DELAY_KEY_MSEC);
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
    Assume.assumeTrue(fs.hasMetadataStore());

    if (mkdirs != null) {
      for (Path mkdir : mkdirs) {
        assertTrue(fs.mkdirs(mkdir));
      }
      clearInconsistency(fs);
    }

    assertTrue("srcdirs and dstdirs must have equal length",
        srcdirs.length == dstdirs.length);
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
   * Tests that after renaming a directory, the original directory and its
   * contents are indeed missing and the corresponding new paths are visible.
   * @throws Exception
   */
  @Test
  public void testConsistentListAfterRename() throws Exception {
    Path[] mkdirs = {
      path("d1/f"),
      path("d1/f" + DEFAULT_DELAY_KEY_SUBSTRING)
    };
    Path[] srcdirs = {path("d1")};
    Path[] dstdirs = {path("d2")};
    Path[] yesdirs = {path("d2"), path("d2/f"),
        path("d2/f" + DEFAULT_DELAY_KEY_SUBSTRING)};
    Path[] nodirs = {path("d1"), path("d1/f"),
        path("d1/f" + DEFAULT_DELAY_KEY_SUBSTRING)};
    doTestRenameSequence(mkdirs, srcdirs, dstdirs, yesdirs, nodirs);
    getFileSystem().delete(path("d1"), true);
    getFileSystem().delete(path("d2"), true);
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

    for(int i = 0; i < 2; i++) {
      Path[] firstSet = i == 0 ? setA : null;
      doTestRenameSequence(firstSet, setA, setB, setB, dir0);
      doTestRenameSequence(null, setB, setC, setC, dir1);
      doTestRenameSequence(null, setC, setA, setA, dir2);
    }

    S3AFileSystem fs = getFileSystem();
    assertFalse("Renaming deleted file should have failed",
        fs.rename(dir2[0], dir1[0]));
    assertTrue("Renaming over existing file should have succeeded",
        fs.rename(dir1[0], dir0[0]));
  }

  /**
   * Tests that deleted files immediately stop manifesting in list operations
   * even when the effect in S3 is delayed.
   * @throws Exception
   */
  @Test
  public void testConsistentListAfterDelete() throws Exception {
    S3AFileSystem fs = getFileSystem();
    // test will fail if NullMetadataStore (the default) is configured: skip it.
    Assume.assumeTrue(fs.hasMetadataStore());

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
    for (Path path : testDirs) {
      assertTrue(fs.delete(path, false));
    }

    FileStatus[] paths = fs.listStatus(path("a/b/"));
    List<Path> list = new ArrayList<>();
    for (FileStatus fileState : paths) {
      list.add(fileState.getPath());
    }
    assertFalse(list.contains(path("a/b/dir1")));
    assertFalse(list.contains(path("a/b/dir2")));
    // This should fail without S3Guard, and succeed with it.
    assertFalse(list.contains(inconsistentPath));
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
    // test will fail if NullMetadataStore (the default) is configured: skip it.
    Assume.assumeTrue(fs.hasMetadataStore());

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

    fs.rename(path("a"), path("a3"));
    FileStatus[] paths = fs.listStatus(path("a3/b"));
    List<Path> list = new ArrayList<>();
    for (FileStatus fileState : paths) {
      list.add(fileState.getPath());
    }
    assertTrue(list.contains(path("a3/b/dir1")));
    assertFalse(list.contains(path("a3/b/dir2")));
    // This should fail without S3Guard, and succeed with it.
    assertFalse(list.contains(path("a3/b/dir3-" +
        DEFAULT_DELAY_KEY_SUBSTRING)));

    try {
      RemoteIterator<LocatedFileStatus> old = fs.listFilesAndEmptyDirectories(
          path("a"), true);
      fail("Recently renamed dir should not be visible");
    } catch(FileNotFoundException e) {
      // expected
    }
  }

  @Test
  public void testConsistentListStatusAfterPut() throws Exception {

    S3AFileSystem fs = getFileSystem();

    // This test will fail if NullMetadataStore (the default) is configured:
    // skip it.
    Assume.assumeTrue(fs.hasMetadataStore());

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
    assertTrue(list.contains(path("a/b/dir1")));
    assertTrue(list.contains(path("a/b/dir2")));
    // This should fail without S3Guard, and succeed with it.
    assertTrue(list.contains(inconsistentPath));
  }

  /**
   * Similar to {@link #testConsistentListStatusAfterPut()}, this tests that the
   * FS listLocatedStatus() call will return consistent list.
   */
  @Test
  public void testConsistentListLocatedStatusAfterPut() throws Exception {
    final S3AFileSystem fs = getFileSystem();
    // This test will fail if NullMetadataStore (the default) is configured:
    // skip it.
    Assume.assumeTrue(fs.hasMetadataStore());
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
    // This test will fail if NullMetadataStore (the default) is configured:
    // skip it.
    Assume.assumeTrue(fs.hasMetadataStore());

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
    Assume.assumeTrue(fs.hasMetadataStore());
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
    // Test only implemented for v2 S3 list API
    Assume.assumeTrue(getConfiguration()
        .getInt(LIST_VERSION, DEFAULT_LIST_VERSION) == 2);

    S3AFileSystem fs = getFileSystem();
    Path root = path("testInconsistentClient" + DEFAULT_DELAY_KEY_SUBSTRING);
    for (int i = 0; i < 3; i++) {
      fs.mkdirs(new Path(root, "dir" + i));
      touch(fs, new Path(root, "file" + i));
      for (int j = 0; j < 3; j++) {
        touch(fs, new Path(new Path(root, "dir" + i), "file" + i + "-" + j));
      }
    }
    clearInconsistency(fs);

    String key = fs.pathToKey(root) + "/";

    ListObjectsV2Result preDeleteDelimited = listObjectsV2(fs, key, "/");
    ListObjectsV2Result preDeleteUndelimited = listObjectsV2(fs, key, null);

    fs.delete(root, true);

    ListObjectsV2Result postDeleteDelimited = listObjectsV2(fs, key, "/");
    ListObjectsV2Result postDeleteUndelimited = listObjectsV2(fs, key, null);

    assertEquals("InconsistentAmazonS3Client added back objects incorrectly " +
            "in a non-recursive listing",
        preDeleteDelimited.getObjectSummaries().size(),
        postDeleteDelimited.getObjectSummaries().size()
    );
    assertEquals("InconsistentAmazonS3Client added back prefixes incorrectly " +
            "in a non-recursive listing",
        preDeleteDelimited.getCommonPrefixes().size(),
        postDeleteDelimited.getCommonPrefixes().size()
    );
    assertEquals("InconsistentAmazonS3Client added back objects incorrectly " +
            "in a recursive listing",
        preDeleteUndelimited.getObjectSummaries().size(),
        postDeleteUndelimited.getObjectSummaries().size()
    );
    assertEquals("InconsistentAmazonS3Client added back prefixes incorrectly " +
            "in a recursive listing",
        preDeleteUndelimited.getCommonPrefixes().size(),
        postDeleteUndelimited.getCommonPrefixes().size()
    );
  }

  /**
   * retrying v2 list.
   * @param fs
   * @param key
   * @param delimiter
   * @return
   * @throws IOException on error
   */
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
