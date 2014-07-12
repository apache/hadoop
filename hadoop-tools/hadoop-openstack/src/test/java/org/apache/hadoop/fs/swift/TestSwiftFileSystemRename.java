/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftOperationFailedException;
import org.apache.hadoop.fs.swift.util.SwiftTestUtils;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.compareByteArrays;
import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.dataset;
import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.readBytesToString;
import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.readDataset;
import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.writeDataset;

public class TestSwiftFileSystemRename extends SwiftFileSystemBaseTest {

  /**
   * Rename a file into a directory
   *
   * @throws Exception
   */
  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testRenameFileIntoExistingDirectory() throws Exception {
    assumeRenameSupported();

    Path src = path("/test/olddir/file");
    createFile(src);
    Path dst = path("/test/new/newdir");
    fs.mkdirs(dst);
    rename(src, dst, true, false, true);
    Path newFile = path("/test/new/newdir/file");
    if (!fs.exists(newFile)) {
      String ls = ls(dst);
      LOG.info(ls(path("/test/new")));
      LOG.info(ls(path("/test/hadoop")));
      fail("did not find " + newFile + " - directory: " + ls);
    }
    assertTrue("Destination changed",
            fs.exists(path("/test/new/newdir/file")));
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testRenameFile() throws Exception {
    assumeRenameSupported();

    final Path old = new Path("/test/alice/file");
    final Path newPath = new Path("/test/bob/file");
    fs.mkdirs(newPath.getParent());
    final FSDataOutputStream fsDataOutputStream = fs.create(old);
    final byte[] message = "Some data".getBytes();
    fsDataOutputStream.write(message);
    fsDataOutputStream.close();

    assertTrue(fs.exists(old));
    rename(old, newPath, true, false, true);

    final FSDataInputStream bobStream = fs.open(newPath);
    final byte[] bytes = new byte[512];
    final int read = bobStream.read(bytes);
    bobStream.close();
    final byte[] buffer = new byte[read];
    System.arraycopy(bytes, 0, buffer, 0, read);
    assertEquals(new String(message), new String(buffer));
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testRenameDirectory() throws Exception {
    assumeRenameSupported();

    final Path old = new Path("/test/data/logs");
    final Path newPath = new Path("/test/var/logs");
    fs.mkdirs(old);
    fs.mkdirs(newPath.getParent());
    assertTrue(fs.exists(old));
    rename(old, newPath, true, false, true);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testRenameTheSameDirectory() throws Exception {
    assumeRenameSupported();

    final Path old = new Path("/test/usr/data");
    fs.mkdirs(old);
    rename(old, old, false, true, true);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testRenameDirectoryIntoExistingDirectory() throws Exception {
    assumeRenameSupported();

    Path src = path("/test/olddir/dir");
    fs.mkdirs(src);
    createFile(path("/test/olddir/dir/file1"));
    createFile(path("/test/olddir/dir/subdir/file2"));

    Path dst = path("/test/new/newdir");
    fs.mkdirs(dst);
    //this renames into a child
    rename(src, dst, true, false, true);
    assertExists("new dir", path("/test/new/newdir/dir"));
    assertExists("Renamed nested file1", path("/test/new/newdir/dir/file1"));
    assertPathDoesNotExist("Nested file1 should have been deleted",
            path("/test/olddir/dir/file1"));
    assertExists("Renamed nested subdir",
            path("/test/new/newdir/dir/subdir/"));
    assertExists("file under subdir",
            path("/test/new/newdir/dir/subdir/file2"));

    assertPathDoesNotExist("Nested /test/hadoop/dir/subdir/file2 still exists",
            path("/test/olddir/dir/subdir/file2"));
  }

  /**
   * trying to rename a directory onto itself should fail,
   * preserving everything underneath.
   */
  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testRenameDirToSelf() throws Throwable {
    assumeRenameSupported();
    Path parentdir = path("/test/parentdir");
    fs.mkdirs(parentdir);
    Path child = new Path(parentdir, "child");
    createFile(child);

    rename(parentdir, parentdir, false, true, true);
    //verify the child is still there
    assertIsFile(child);
  }

  /**
   * Assert that root directory renames are not allowed
   *
   * @throws Exception on failures
   */
  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testRenameRootDirForbidden() throws Exception {
    assumeRenameSupported();
    rename(path("/"),
            path("/test/newRootDir"),
            false, true, false);
  }

  /**
   * Assert that renaming a parent directory to be a child
   * of itself is forbidden
   *
   * @throws Exception on failures
   */
  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testRenameChildDirForbidden() throws Exception {
    assumeRenameSupported();

    Path parentdir = path("/test/parentdir");
    fs.mkdirs(parentdir);
    Path childFile = new Path(parentdir, "childfile");
    createFile(childFile);
    //verify one level down
    Path childdir = new Path(parentdir, "childdir");
    rename(parentdir, childdir, false, true, false);
    //now another level
    fs.mkdirs(childdir);
    Path childchilddir = new Path(childdir, "childdir");
    rename(parentdir, childchilddir, false, true, false);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testRenameFileAndVerifyContents() throws IOException {
    assumeRenameSupported();

    final Path filePath = new Path("/test/home/user/documents/file.txt");
    final Path newFilePath = new Path("/test/home/user/files/file.txt");
    mkdirs(newFilePath.getParent());
    int len = 1024;
    byte[] dataset = dataset(len, 'A', 26);
    writeDataset(fs, filePath, dataset, len, len, false);
    rename(filePath, newFilePath, true, false, true);
    byte[] dest = readDataset(fs, newFilePath, len);
    compareByteArrays(dataset, dest, len);
    String reread = readBytesToString(fs, newFilePath, 20);
  }


  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testMoveFileUnderParent() throws Throwable {
    if (!renameSupported()) return;
    Path filepath = path("test/file");
    createFile(filepath);
    //HDFS expects rename src, src -> true
    rename(filepath, filepath, true, true, true);
    //verify the file is still there
    assertIsFile(filepath);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testMoveDirUnderParent() throws Throwable {
    if (!renameSupported()) {
      return;
    }
    Path testdir = path("test/dir");
    fs.mkdirs(testdir);
    Path parent = testdir.getParent();
    //the outcome here is ambiguous, so is not checked
    try {
      fs.rename(testdir, parent);
    } catch (SwiftOperationFailedException e) {
      // allowed
    }
    assertExists("Source directory has been deleted ", testdir);
  }

  /**
   * trying to rename a file onto itself should succeed (it's a no-op)
   */
  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testRenameFileToSelf() throws Throwable {
    if (!renameSupported()) return;
    Path filepath = path("test/file");
    createFile(filepath);
    //HDFS expects rename src, src -> true
    rename(filepath, filepath, true, true, true);
    //verify the file is still there
    assertIsFile(filepath);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testRenamedConsistence() throws IOException {
    assumeRenameSupported();
    describe("verify that overwriting a file with new data doesn't impact" +
            " the existing content");

    final Path filePath = new Path("/test/home/user/documents/file.txt");
    final Path newFilePath = new Path("/test/home/user/files/file.txt");
    mkdirs(newFilePath.getParent());
    int len = 1024;
    byte[] dataset = dataset(len, 'A', 26);
    byte[] dataset2 = dataset(len, 'a', 26);
    writeDataset(fs, filePath, dataset, len, len, false);
    rename(filePath, newFilePath, true, false, true);
    SwiftTestUtils.writeAndRead(fs, filePath, dataset2, len, len, false, true);
    byte[] dest = readDataset(fs, newFilePath, len);
    compareByteArrays(dataset, dest, len);
    String reread = readBytesToString(fs, newFilePath, 20);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testRenameMissingFile() throws Throwable {
    assumeRenameSupported();
    Path path = path("/test/RenameMissingFile");
    Path path2 = path("/test/RenameMissingFileDest");
    mkdirs(path("test"));
    rename(path, path2, false, false, false);
  }

}
