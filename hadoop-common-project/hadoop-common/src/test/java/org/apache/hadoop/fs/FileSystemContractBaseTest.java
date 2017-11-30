/**
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

package org.apache.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

/**
 * <p>
 * A collection of tests for the contract of the {@link FileSystem}.
 * This test should be used for general-purpose implementations of
 * {@link FileSystem}, that is, implementations that provide implementations 
 * of all of the functionality of {@link FileSystem}.
 * </p>
 * <p>
 * To test a given {@link FileSystem} implementation create a subclass of this
 * test and add a @Before method to initialize the <code>fs</code>
 * {@link FileSystem} instance variable.
 * </p>
 */
public abstract class FileSystemContractBaseTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemContractBaseTest.class);

  protected final static String TEST_UMASK = "062";
  protected FileSystem fs;
  protected byte[] data = dataset(getBlockSize() * 2, 0, 255);

  @Rule
  public Timeout globalTimeout = new Timeout(getGlobalTimeout());

  /**
   * Get the timeout in milliseconds for each test case.
   * @return a time in milliseconds.
   */
  protected int getGlobalTimeout() {
    return 30 * 1000;
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      // some cases use this absolute path
      if (rootDirTestEnabled()) {
        cleanupDir(path("/FileSystemContractBaseTest"));
      }
      // others use this relative path against test base directory
      cleanupDir(getTestBaseDir());
    }
  }

  private void cleanupDir(Path p) {
    try {
      LOG.info("Deleting " + p);
      fs.delete(p, true);
    } catch (IOException e) {
      LOG.error("Error deleting test dir: " + p, e);
    }
  }

  /**
   * Test base directory for resolving relative test paths.
   *
   * The default value is /user/$USER/FileSystemContractBaseTest. Subclass may
   * set specific test base directory.
   */
  protected Path getTestBaseDir() {
    return new Path(fs.getWorkingDirectory(), "FileSystemContractBaseTest");
  }

  /**
   * For absolute path return the fully qualified path while for relative path
   * return the fully qualified path against {@link #getTestBaseDir()}.
   */
  protected final Path path(String pathString) {
    Path p = new Path(pathString).makeQualified(fs.getUri(), getTestBaseDir());
    LOG.info("Resolving {} -> {}", pathString, p);
    return p;
  }

  protected int getBlockSize() {
    return 1024;
  }
  
  protected String getDefaultWorkingDirectory() {
    return "/user/" + System.getProperty("user.name");
  }

  /**
   * Override this if the filesystem does not support rename
   * @return true if the FS supports rename -and rename related tests
   * should be run
   */
  protected boolean renameSupported() {
    return true;
  }

  /**
   * Override this if the filesystem does not enable testing root directories.
   *
   * If this returns true, the test will create and delete test directories and
   * files under root directory, which may have side effects, e.g. fail tests
   * with PermissionDenied exceptions.
   */
  protected boolean rootDirTestEnabled() {
    return true;
  }

  /**
   * Override this if the filesystem is not case sensitive
   * @return true if the case detection/preservation tests should run
   */
  protected boolean filesystemIsCaseSensitive() {
    return true;
  }

  @Test
  public void testFsStatus() throws Exception {
    FsStatus fsStatus = fs.getStatus();
    assertNotNull(fsStatus);
    //used, free and capacity are non-negative longs
    assertTrue(fsStatus.getUsed() >= 0);
    assertTrue(fsStatus.getRemaining() >= 0);
    assertTrue(fsStatus.getCapacity() >= 0);
  }
  
  @Test
  public void testWorkingDirectory() throws Exception {

    Path workDir = path(getDefaultWorkingDirectory());
    assertEquals(workDir, fs.getWorkingDirectory());

    fs.setWorkingDirectory(fs.makeQualified(new Path(".")));
    assertEquals(workDir, fs.getWorkingDirectory());

    fs.setWorkingDirectory(fs.makeQualified(new Path("..")));
    assertEquals(workDir.getParent(), fs.getWorkingDirectory());

    Path relativeDir = fs.makeQualified(new Path("testWorkingDirectory"));
    fs.setWorkingDirectory(relativeDir);
    assertEquals(relativeDir, fs.getWorkingDirectory());
    
    Path absoluteDir = path("/FileSystemContractBaseTest/testWorkingDirectory");
    fs.setWorkingDirectory(absoluteDir);
    assertEquals(absoluteDir, fs.getWorkingDirectory());

  }

  @Test
  public void testMkdirs() throws Exception {
    Path testDir = path("testMkdirs");
    assertFalse(fs.exists(testDir));
    assertFalse(fs.isFile(testDir));

    assertTrue(fs.mkdirs(testDir));

    assertTrue(fs.exists(testDir));
    assertFalse(fs.isFile(testDir));
    
    assertTrue(fs.mkdirs(testDir));

    assertTrue(fs.exists(testDir));
    assertTrue("Should be a directory", fs.isDirectory(testDir));
    assertFalse(fs.isFile(testDir));

    Path parentDir = testDir.getParent();
    assertTrue(fs.exists(parentDir));
    assertFalse(fs.isFile(parentDir));

    Path grandparentDir = parentDir.getParent();
    assertTrue(fs.exists(grandparentDir));
    assertFalse(fs.isFile(grandparentDir));

  }

  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = path("testMkdirsFailsForSubdirectoryOfExistingFile");
    assertFalse(fs.exists(testDir));
    assertTrue(fs.mkdirs(testDir));
    assertTrue(fs.exists(testDir));

    createFile(path("testMkdirsFailsForSubdirectoryOfExistingFile/file"));

    Path testSubDir = path(
        "testMkdirsFailsForSubdirectoryOfExistingFile/file/subdir");
    try {
      fs.mkdirs(testSubDir);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }

    try {
      assertFalse(fs.exists(testSubDir));
    } catch (AccessControlException e) {
      // Expected : HDFS-11132 Checks on paths under file may be rejected by
      // file missing execute permission.
    }

    Path testDeepSubDir = path(
        "testMkdirsFailsForSubdirectoryOfExistingFile/file/deep/sub/dir");
    try {
      fs.mkdirs(testDeepSubDir);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }

    try {
      assertFalse(fs.exists(testDeepSubDir));
    } catch (AccessControlException e) {
      // Expected : HDFS-11132 Checks on paths under file may be rejected by
      // file missing execute permission.
    }

  }

  @Test
  public void testMkdirsWithUmask() throws Exception {
    Configuration conf = fs.getConf();
    String oldUmask = conf.get(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY);
    try {
      conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, TEST_UMASK);
      final Path dir = path("newDir");
      assertTrue(fs.mkdirs(dir, new FsPermission((short) 0777)));
      FileStatus status = fs.getFileStatus(dir);
      assertTrue(status.isDirectory());
      assertEquals((short) 0715, status.getPermission().toShort());
    } finally {
      conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, oldUmask);
    }
  }

  @Test
  public void testGetFileStatusThrowsExceptionForNonExistentFile() 
    throws Exception {
    try {
      fs.getFileStatus(
          path("testGetFileStatusThrowsExceptionForNonExistentFile/file"));
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      // expected
    }
  }

  @Test
  public void testListStatusThrowsExceptionForNonExistentFile() throws Exception {
    try {
      fs.listStatus(
          path("testListStatusThrowsExceptionForNonExistentFile/file"));
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // expected
    }
  }

  @Test
  public void testListStatus() throws Exception {
    final Path[] testDirs = {
        path("testListStatus/a"),
        path("testListStatus/b"),
        path("testListStatus/c/1")
    };
    assertFalse(fs.exists(testDirs[0]));

    for (Path path : testDirs) {
      assertTrue(fs.mkdirs(path));
    }

    FileStatus[] paths = fs.listStatus(path("."));
    assertEquals(1, paths.length);
    assertEquals(path("testListStatus"), paths[0].getPath());

    paths = fs.listStatus(path("testListStatus"));
    assertEquals(3, paths.length);
    ArrayList<Path> list = new ArrayList<Path>();
    for (FileStatus fileState : paths) {
      list.add(fileState.getPath());
    }
    assertTrue(list.contains(path("testListStatus/a")));
    assertTrue(list.contains(path("testListStatus/b")));
    assertTrue(list.contains(path("testListStatus/c")));

    paths = fs.listStatus(path("testListStatus/a"));
    assertEquals(0, paths.length);
  }

  @Test
  public void testWriteReadAndDeleteEmptyFile() throws Exception {
    writeReadAndDelete(0);
  }

  @Test
  public void testWriteReadAndDeleteHalfABlock() throws Exception {
    writeReadAndDelete(getBlockSize() / 2);
  }

  @Test
  public void testWriteReadAndDeleteOneBlock() throws Exception {
    writeReadAndDelete(getBlockSize());
  }

  @Test
  public void testWriteReadAndDeleteOneAndAHalfBlocks() throws Exception {
    writeReadAndDelete(getBlockSize() + (getBlockSize() / 2));
  }

  @Test
  public void testWriteReadAndDeleteTwoBlocks() throws Exception {
    writeReadAndDelete(getBlockSize() * 2);
  }

  /**
   * Write a dataset, read it back in and verify that they match.
   * Afterwards, the file is deleted.
   * @param len length of data
   * @throws IOException on IO failures
   */
  protected void writeReadAndDelete(int len) throws IOException {
    Path path = path("writeReadAndDelete/file");
    writeAndRead(path, data, len, false, true);
  }

  @Test
  public void testOverwrite() throws IOException {
    Path path = path("testOverwrite/file");
    
    fs.mkdirs(path.getParent());

    createFile(path);
    
    assertTrue("Exists", fs.exists(path));
    assertEquals("Length", data.length, fs.getFileStatus(path).getLen());
    
    try {
      fs.create(path, false).close();
      fail("Should throw IOException.");
    } catch (IOException e) {
      // Expected
    }
    
    FSDataOutputStream out = fs.create(path, true);
    out.write(data, 0, data.length);
    out.close();
    
    assertTrue("Exists", fs.exists(path));
    assertEquals("Length", data.length, fs.getFileStatus(path).getLen());
    
  }

  @Test
  public void testWriteInNonExistentDirectory() throws IOException {
    Path path = path("testWriteInNonExistentDirectory/file");
    assertFalse("Parent exists", fs.exists(path.getParent()));
    createFile(path);
    
    assertTrue("Exists", fs.exists(path));
    assertEquals("Length", data.length, fs.getFileStatus(path).getLen());
    assertTrue("Parent exists", fs.exists(path.getParent()));
  }

  @Test
  public void testDeleteNonExistentFile() throws IOException {
    Path path = path("testDeleteNonExistentFile/file");
    assertFalse("Path exists: " + path, fs.exists(path));
    assertFalse("No deletion", fs.delete(path, true));
  }

  @Test
  public void testDeleteRecursively() throws IOException {
    Path dir = path("testDeleteRecursively");
    Path file = path("testDeleteRecursively/file");
    Path subdir = path("testDeleteRecursively/subdir");
    
    createFile(file);
    assertTrue("Created subdir", fs.mkdirs(subdir));
    
    assertTrue("File exists", fs.exists(file));
    assertTrue("Dir exists", fs.exists(dir));
    assertTrue("Subdir exists", fs.exists(subdir));
    
    try {
      fs.delete(dir, false);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    assertTrue("File still exists", fs.exists(file));
    assertTrue("Dir still exists", fs.exists(dir));
    assertTrue("Subdir still exists", fs.exists(subdir));
    
    assertTrue("Deleted", fs.delete(dir, true));
    assertFalse("File doesn't exist", fs.exists(file));
    assertFalse("Dir doesn't exist", fs.exists(dir));
    assertFalse("Subdir doesn't exist", fs.exists(subdir));
  }

  @Test
  public void testDeleteEmptyDirectory() throws IOException {
    Path dir = path("testDeleteEmptyDirectory");
    assertTrue(fs.mkdirs(dir));
    assertTrue("Dir exists", fs.exists(dir));
    assertTrue("Deleted", fs.delete(dir, false));
    assertFalse("Dir doesn't exist", fs.exists(dir));
  }

  @Test
  public void testRenameNonExistentPath() throws Exception {
    assumeTrue(renameSupported());
    
    Path src = path("testRenameNonExistentPath/path");
    Path dst = path("testRenameNonExistentPathNew/newpath");
    rename(src, dst, false, false, false);
  }

  @Test
  public void testRenameFileMoveToNonExistentDirectory() throws Exception {
    assumeTrue(renameSupported());
    
    Path src = path("testRenameFileMoveToNonExistentDirectory/file");
    createFile(src);
    Path dst = path("testRenameFileMoveToNonExistentDirectoryNew/newfile");
    rename(src, dst, false, true, false);
  }

  @Test
  public void testRenameFileMoveToExistingDirectory() throws Exception {
    assumeTrue(renameSupported());
    
    Path src = path("testRenameFileMoveToExistingDirectory/file");
    createFile(src);
    Path dst = path("testRenameFileMoveToExistingDirectoryNew/newfile");
    fs.mkdirs(dst.getParent());
    rename(src, dst, true, false, true);
  }

  @Test
  public void testRenameFileAsExistingFile() throws Exception {
    assumeTrue(renameSupported());
    
    Path src = path("testRenameFileAsExistingFile/file");
    createFile(src);
    Path dst = path("testRenameFileAsExistingFileNew/newfile");
    createFile(dst);
    rename(src, dst, false, true, true);
  }

  @Test
  public void testRenameFileAsExistingDirectory() throws Exception {
    assumeTrue(renameSupported());
    
    Path src = path("testRenameFileAsExistingDirectory/file");
    createFile(src);
    Path dst = path("testRenameFileAsExistingDirectoryNew/newdir");
    fs.mkdirs(dst);
    rename(src, dst, true, false, true);
    assertIsFile(path("testRenameFileAsExistingDirectoryNew/newdir/file"));
  }

  @Test
  public void testRenameDirectoryMoveToNonExistentDirectory() 
    throws Exception {
    assumeTrue(renameSupported());
    
    Path src = path("testRenameDirectoryMoveToNonExistentDirectory/dir");
    fs.mkdirs(src);
    Path dst = path("testRenameDirectoryMoveToNonExistentDirectoryNew/newdir");
    rename(src, dst, false, true, false);
  }

  @Test
  public void testRenameDirectoryMoveToExistingDirectory() throws Exception {
    assumeTrue(renameSupported());
    Path src = path("testRenameDirectoryMoveToExistingDirectory/dir");
    fs.mkdirs(src);
    createFile(path(src + "/file1"));
    createFile(path(src + "/subdir/file2"));
    
    Path dst = path("testRenameDirectoryMoveToExistingDirectoryNew/newdir");
    fs.mkdirs(dst.getParent());
    rename(src, dst, true, false, true);
    
    assertFalse("Nested file1 exists",
        fs.exists(path(src + "/file1")));
    assertFalse("Nested file2 exists",
        fs.exists(path(src + "/subdir/file2")));
    assertTrue("Renamed nested file1 exists",
        fs.exists(path(dst + "/file1")));
    assertTrue("Renamed nested exists",
        fs.exists(path(dst + "/subdir/file2")));
  }

  @Test
  public void testRenameDirectoryAsExistingFile() throws Exception {
    assumeTrue(renameSupported());
    
    Path src = path("testRenameDirectoryAsExistingFile/dir");
    fs.mkdirs(src);
    Path dst = path("testRenameDirectoryAsExistingFileNew/newfile");
    createFile(dst);
    rename(src, dst, false, true, true);
  }

  @Test
  public void testRenameDirectoryAsExistingDirectory() throws Exception {
    assumeTrue(renameSupported());
    final Path src = path("testRenameDirectoryAsExistingDirectory/dir");
    fs.mkdirs(src);
    createFile(path(src + "/file1"));
    createFile(path(src + "/subdir/file2"));

    final Path dst = path("testRenameDirectoryAsExistingDirectoryNew/newdir");
    fs.mkdirs(dst);
    rename(src, dst, true, false, true);
    assertTrue("Destination changed",
        fs.exists(path(dst + "/dir")));
    assertFalse("Nested file1 exists",
        fs.exists(path(src + "/file1")));
    assertFalse("Nested file2 exists",
        fs.exists(path(src + "/dir/subdir/file2")));
    assertTrue("Renamed nested file1 exists",
        fs.exists(path(dst + "/dir/file1")));
    assertTrue("Renamed nested exists",
        fs.exists(path(dst + "/dir/subdir/file2")));
  }

  @Test
  public void testInputStreamClosedTwice() throws IOException {
    //HADOOP-4760 according to Closeable#close() closing already-closed 
    //streams should have no effect. 
    Path src = path("testInputStreamClosedTwice/file");
    createFile(src);
    FSDataInputStream in = fs.open(src);
    in.close();
    in.close();
  }

  @Test
  public void testOutputStreamClosedTwice() throws IOException {
    //HADOOP-4760 according to Closeable#close() closing already-closed 
    //streams should have no effect. 
    Path src = path("testOutputStreamClosedTwice/file");
    FSDataOutputStream out = fs.create(src);
    out.writeChar('H'); //write some data
    out.close();
    out.close();
  }

  protected void createFile(Path path) throws IOException {
    FSDataOutputStream out = fs.create(path);
    out.write(data, 0, data.length);
    out.close();
  }
  
  protected void rename(Path src, Path dst, boolean renameSucceeded,
      boolean srcExists, boolean dstExists) throws IOException {
    assertEquals("Rename result", renameSucceeded, fs.rename(src, dst));
    assertEquals("Source exists", srcExists, fs.exists(src));
    assertEquals("Destination exists" + dst, dstExists, fs.exists(dst));
  }

  /**
   * Verify that if you take an existing file and overwrite it, the new values
   * get picked up.
   * This is a test for the behavior of eventually consistent
   * filesystems.
   *
   * @throws Exception on any failure
   */
  @Test
  public void testOverWriteAndRead() throws Exception {
    int blockSize = getBlockSize();

    byte[] filedata1 = dataset(blockSize * 2, 'A', 26);
    byte[] filedata2 = dataset(blockSize * 2, 'a', 26);
    Path path = path("testOverWriteAndRead/file-overwrite");
    writeAndRead(path, filedata1, blockSize, true, false);
    writeAndRead(path, filedata2, blockSize, true, false);
    writeAndRead(path, filedata1, blockSize * 2, true, false);
    writeAndRead(path, filedata2, blockSize * 2, true, false);
    writeAndRead(path, filedata1, blockSize, true, false);
    writeAndRead(path, filedata2, blockSize * 2, true, false);
  }

  /**
   * Assert that a filesystem is case sensitive.
   * This is done by creating a mixed-case filename and asserting that
   * its lower case version is not there.
   * @throws Exception
   */
  @Test
  public void testFilesystemIsCaseSensitive() throws Exception {
    if (!filesystemIsCaseSensitive()) {
      LOG.info("Skipping test");
      return;
    }
    String mixedCaseFilename = "testFilesystemIsCaseSensitive";
    Path upper = path(mixedCaseFilename);
    Path lower = path(StringUtils.toLowerCase(mixedCaseFilename));
    assertFalse("File exists" + upper, fs.exists(upper));
    assertFalse("File exists" + lower, fs.exists(lower));
    FSDataOutputStream out = fs.create(upper);
    out.writeUTF("UPPER");
    out.close();
    FileStatus upperStatus = fs.getFileStatus(upper);
    assertTrue("File does not exist" + upper, fs.exists(upper));
    //verify the lower-case version of the filename doesn't exist
    assertFalse("File exists" + lower, fs.exists(lower));
    //now overwrite the lower case version of the filename with a
    //new version.
    out = fs.create(lower);
    out.writeUTF("l");
    out.close();
    assertTrue("File does not exist" + lower, fs.exists(lower));
    //verify the length of the upper file hasn't changed
    FileStatus newStatus = fs.getFileStatus(upper);
    assertEquals("Expected status:" + upperStatus
                 + " actual status " + newStatus,
                 upperStatus.getLen(),
                 newStatus.getLen()); }

  /**
   * Asserts that a zero byte file has a status of file and not
   * directory or symlink
   * @throws Exception on failures
   */
  @Test
  public void testZeroByteFilesAreFiles() throws Exception {
    Path src = path("testZeroByteFilesAreFiles");
    //create a zero byte file
    FSDataOutputStream out = fs.create(src);
    out.close();
    assertIsFile(src);
  }

  /**
   * Asserts that a zero byte file has a status of file and not
   * directory or symlink
   * @throws Exception on failures
   */
  @Test
  public void testMultiByteFilesAreFiles() throws Exception {
    Path src = path("testMultiByteFilesAreFiles");
    FSDataOutputStream out = fs.create(src);
    out.writeUTF("testMultiByteFilesAreFiles");
    out.close();
    assertIsFile(src);
  }

  /**
   * Assert that root directory renames are not allowed
   * @throws Exception on failures
   */
  @Test
  public void testRootDirAlwaysExists() throws Exception {
    //this will throw an exception if the path is not found
    fs.getFileStatus(path("/"));
    //this catches overrides of the base exists() method that don't
    //use getFileStatus() as an existence probe
    assertTrue("FileSystem.exists() fails for root", fs.exists(path("/")));
  }

  /**
   * Assert that root directory renames are not allowed
   * @throws Exception on failures
   */
  @Test
  public void testRenameRootDirForbidden() throws Exception {
    assumeTrue(rootDirTestEnabled());
    assumeTrue(renameSupported());

    rename(path("/"),
           path("testRenameRootDirForbidden"),
           false, true, false);
  }

  /**
   * Assert that renaming a parent directory to be a child
   * of itself is forbidden
   * @throws Exception on failures
   */
  @Test
  public void testRenameChildDirForbidden() throws Exception {
    assumeTrue(renameSupported());
    LOG.info("testRenameChildDirForbidden");
    Path parentdir = path("testRenameChildDirForbidden");
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

  /**
   * This a sanity check to make sure that any filesystem's handling of
   * renames empty dirs doesn't cause any regressions.
   */
  public void testRenameEmptyToDirWithSamePrefixAllowed() throws Throwable {
    assumeTrue(renameSupported());
    Path parentdir = path("testRenameEmptyToDirWithSamePrefixAllowed");
    fs.mkdirs(parentdir);
    Path dest = path("testRenameEmptyToDirWithSamePrefixAllowedDest");
    rename(parentdir, dest, true, false, true);
  }

  /**
   * This a sanity check to make sure that any filesystem's handling of
   * renames non-empty dirs doesn't cause any regressions.
   */
  @Test
  public void testRenameToDirWithSamePrefixAllowed() throws Throwable {
    assumeTrue(renameSupported());
    final Path parentdir = path("testRenameToDirWithSamePrefixAllowed");
    fs.mkdirs(parentdir);
    // Before renaming, we create one file under the source parent directory
    createFile(new Path(parentdir, "mychild"));
    final Path dest = path("testRenameToDirWithSamePrefixAllowedDest");
    rename(parentdir, dest, true, false, true);
  }

  /**
   * trying to rename a directory onto itself should fail,
   * preserving everything underneath.
   */
  @Test
  public void testRenameDirToSelf() throws Throwable {
    assumeTrue(renameSupported());
    Path parentdir = path("testRenameDirToSelf");
    fs.mkdirs(parentdir);
    Path child = new Path(parentdir, "child");
    createFile(child);

    rename(parentdir, parentdir, false, true, true);
    //verify the child is still there
    assertIsFile(child);
  }

  /**
   * trying to rename a directory onto its parent dir will build
   * a destination path of its original name, which should then fail.
   * The source path and the destination path should still exist afterwards
   */
  @Test
  public void testMoveDirUnderParent() throws Throwable {
    assumeTrue(renameSupported());
    Path testdir = path("testMoveDirUnderParent");
    fs.mkdirs(testdir);
    Path parent = testdir.getParent();
    //the outcome here is ambiguous, so is not checked
    fs.rename(testdir, parent);
    assertEquals("Source exists: " + testdir, true, fs.exists(testdir));
    assertEquals("Destination exists" + parent, true, fs.exists(parent));
  }

  /**
   * trying to rename a file onto itself should succeed (it's a no-op)
   *
   */
  @Test
  public void testRenameFileToSelf() throws Throwable {
    assumeTrue(renameSupported());
    Path filepath = path("testRenameFileToSelf");
    createFile(filepath);
    //HDFS expects rename src, src -> true
    rename(filepath, filepath, true, true, true);
    //verify the file is still there
    assertIsFile(filepath);
  }

  /**
   * trying to move a file into it's parent dir should succeed
   * again: no-op
   */
  @Test
  public void testMoveFileUnderParent() throws Throwable {
    assumeTrue(renameSupported());
    Path filepath = path("testMoveFileUnderParent");
    createFile(filepath);
    //HDFS expects rename src, src -> true
    rename(filepath, filepath, true, true, true);
    //verify the file is still there
    assertIsFile(filepath);
  }

  @Test
  public void testLSRootDir() throws Throwable {
    assumeTrue(rootDirTestEnabled());

    Path dir = path("/");
    Path child = path("/FileSystemContractBaseTest");
    createFile(child);
    assertListFilesFinds(dir, child);
  }

  @Test
  public void testListStatusRootDir() throws Throwable {
    assumeTrue(rootDirTestEnabled());

    Path dir = path("/");
    Path child  = path("/FileSystemContractBaseTest");
    createFile(child);
    assertListStatusFinds(dir, child);
  }

  private void assertListFilesFinds(Path dir, Path subdir) throws IOException {
    RemoteIterator<LocatedFileStatus> iterator =
      fs.listFiles(dir, true);
    boolean found = false;
    StringBuilder builder = new StringBuilder();
    while (iterator.hasNext()) {
      LocatedFileStatus next =  iterator.next();
      builder.append(next.toString()).append('\n');
      if (next.getPath().equals(subdir)) {
        found = true;
      }
    }
    assertTrue("Path " + subdir
               + " not found in directory " + dir + ":" + builder,
               found);
  }

  private void assertListStatusFinds(Path dir, Path subdir) throws IOException {
    FileStatus[] stats = fs.listStatus(dir);
    boolean found = false;
    StringBuilder builder = new StringBuilder();
    for (FileStatus stat : stats) {
      builder.append(stat.toString()).append('\n');
      if (stat.getPath().equals(subdir)) {
        found = true;
      }
    }
    assertTrue("Path " + subdir
               + " not found in directory " + dir + ":" + builder,
               found);
  }


  /**
   * Assert that a file exists and whose {@link FileStatus} entry
   * declares that this is a file and not a symlink or directory.
   * @param filename name of the file
   * @throws IOException IO problems during file operations
   */
  private void assertIsFile(Path filename) throws IOException {
    assertTrue("Does not exist: " + filename, fs.exists(filename));
    FileStatus status = fs.getFileStatus(filename);
    String fileInfo = filename + "  " + status;
    assertTrue("Not a file " + fileInfo, status.isFile());
    assertFalse("File claims to be a symlink " + fileInfo,
                status.isSymlink());
    assertFalse("File claims to be a directory " + fileInfo,
                status.isDirectory());
  }

  /**
   *
   * Write a file and read it in, validating the result. Optional flags control
   * whether file overwrite operations should be enabled, and whether the
   * file should be deleted afterwards.
   *
   * If there is a mismatch between what was written and what was expected,
   * a small range of bytes either side of the first error are logged to aid
   * diagnosing what problem occurred -whether it was a previous file
   * or a corrupting of the current file. This assumes that two
   * sequential runs to the same path use datasets with different character
   * moduli.
   *
   * @param path path to write to
   * @param len length of data
   * @param overwrite should the create option allow overwrites?
   * @param delete should the file be deleted afterwards? -with a verification
   * that it worked. Deletion is not attempted if an assertion has failed
   * earlier -it is not in a <code>finally{}</code> block.
   * @throws IOException IO problems
   */
  protected void writeAndRead(Path path, byte[] src, int len,
                              boolean overwrite,
                              boolean delete) throws IOException {
    assertTrue("Not enough data in source array to write " + len + " bytes",
               src.length >= len);
    fs.mkdirs(path.getParent());

    FSDataOutputStream out = fs.create(path, overwrite,
                                       fs.getConf().getInt("io.file.buffer.size",
                                                           4096),
                                       (short) 1, getBlockSize());
    out.write(src, 0, len);
    out.close();

    assertTrue("Exists", fs.exists(path));
    assertEquals("Length", len, fs.getFileStatus(path).getLen());

    FSDataInputStream in = fs.open(path);
    byte[] buf = new byte[len];
    in.readFully(0, buf);
    in.close();

    assertEquals(len, buf.length);
    int errors = 0;
    int first_error_byte = -1;
    for (int i = 0; i < len; i++) {
      if (src[i] != buf[i]) {
        if (errors == 0) {
          first_error_byte = i;
        }
        errors++;
      }
    }

    if (errors > 0) {
      String message = String.format(" %d errors in file of length %d",
                                     errors, len);
      LOG.warn(message);
      // the range either side of the first error to print
      // this is a purely arbitrary number, to aid user debugging
      final int overlap = 10;
      for (int i = Math.max(0, first_error_byte - overlap);
           i < Math.min(first_error_byte + overlap, len);
           i++) {
        byte actual = buf[i];
        byte expected = src[i];
        String letter = toChar(actual);
        String line = String.format("[%04d] %2x %s\n", i, actual, letter);
        if (expected != actual) {
          line = String.format("[%04d] %2x %s -expected %2x %s\n",
                               i,
                               actual,
                               letter,
                               expected,
                               toChar(expected));
        }
        LOG.warn(line);
      }
      fail(message);
    }

    if (delete) {
      boolean deleted = fs.delete(path, false);
      assertTrue("Deleted", deleted);
      assertFalse("No longer exists", fs.exists(path));
    }
  }

  /**
   * Convert a byte to a character for printing. If the
   * byte value is < 32 -and hence unprintable- the byte is
   * returned as a two digit hex value
   * @param b byte
   * @return the printable character string
   */
  protected String toChar(byte b) {
    if (b >= 0x20) {
      return Character.toString((char) b);
    } else {
      return String.format("%02x", b);
    }
  }

  /**
   * Create a dataset for use in the tests; all data is in the range
   * base to (base+modulo-1) inclusive
   * @param len length of data
   * @param base base of the data
   * @param modulo the modulo
   * @return the newly generated dataset
   */
  protected byte[] dataset(int len, int base, int modulo) {
    byte[] dataset = new byte[len];
    for (int i = 0; i < len; i++) {
      dataset[i] = (byte) (base + (i % modulo));
    }
    return dataset;
  }
}
