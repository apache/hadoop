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

import java.io.*;
import java.util.ArrayList;
import java.util.EnumSet;

import junit.framework.Assert;

import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Test;

/**
 * <p>
 * A collection of tests for the {@link FileContext} to test path names passed
 * as URIs. This test should be used for testing an instance of FileContext that
 * has been initialized to a specific default FileSystem such a LocalFileSystem,
 * HDFS,S3, etc, and where path names are passed that are URIs in a different
 * FileSystem.
 * </p>
 * 
 * <p>
 * To test a given {@link FileSystem} implementation create a subclass of this
 * test and override {@link #setUp()} to initialize the <code>fc1</code> and
 * <code>fc2</code>
 * 
 * The tests will do operations on fc1 that use a URI in fc2
 * 
 * {@link FileContext} instance variable.
 * </p>
 */
public abstract class FileContextURIBase {
  private static final String basePath = System.getProperty("test.build.data",
      "build/test/data") + "/testContextURI";
  private static final Path BASE = new Path(basePath);
  protected FileContext fc1;
  protected FileContext fc2;

  private static int BLOCK_SIZE = 1024;

  private static byte[] data = new byte[BLOCK_SIZE * 2]; // two blocks of data
  {
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 10);
    }
  }

  //Helper method to make path qualified
  protected Path qualifiedPath(String path, FileContext fc) {
    return fc.makeQualified(new Path(BASE, path));
  }

  // Helper method to create file and write data to file
  protected void createFile(Path path, FileContext fc) throws IOException {
    FSDataOutputStream out = fc.create(path, EnumSet.of(CreateFlag.CREATE),
        Options.CreateOpts.createParent());
    out.write(data, 0, data.length);
    out.close();
  }

  @After
  public void tearDown() throws Exception {
    // Clean up after test completion
    // No need to clean fc1 as fc1 and fc2 points same location
    fc2.delete(BASE, true);
  }

  @Test
  public void testCreateFile() throws IOException {
    String fileNames[] = { 
        "testFile", "test File",
        "test*File", "test#File",
        "test1234", "1234Test",
        "test)File", "test_File", 
        "()&^%$#@!~_+}{><?",
        "  ", "^ " };

    for (String f : fileNames) {
      // Create a file on fc2's file system using fc1
      Path testPath = qualifiedPath(f, fc2);
      // Ensure file does not exist
      Assert.assertFalse(fc2.exists(testPath));

      // Now create file
      createFile(testPath, fc1);
      // Ensure fc2 has the created file
      Assert.assertTrue(fc2.exists(testPath));
    }
  }

  @Test
  public void testCreateFileWithNullName() throws IOException {
    String fileName = null;

    try {

      Path testPath = qualifiedPath(fileName, fc2);
      // Ensure file does not exist
      Assert.assertFalse(fc2.exists(testPath));

      // Create a file on fc2's file system using fc1
      createFile(testPath, fc1);
      Assert.fail("Create file with null name should throw IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // expected
    }

  }

  @Test
  public void testCreateExistingFile() throws IOException {
    String fileName = "testFile";
    Path testPath = qualifiedPath(fileName, fc2);

    // Ensure file does not exist
    Assert.assertFalse(fc2.exists(testPath));

    // Create a file on fc2's file system using fc1
    createFile(testPath, fc1);

    // Create same file with fc1
    try {
      createFile(testPath, fc2);
      Assert.fail("Create existing file should throw an IOException.");
    } catch (IOException e) {
      // expected
    }

    // Ensure fc2 has the created file
    Assert.assertTrue(fc2.exists(testPath));
  }

  @Test
  public void testCreateFileInNonExistingDirectory() throws IOException {
    String fileName = "testDir/testFile";

    Path testPath = qualifiedPath(fileName, fc2);

    // Ensure file does not exist
    Assert.assertFalse(fc2.exists(testPath));

    // Create a file on fc2's file system using fc1
    createFile(testPath, fc1);

    // Ensure using fc2 that file is created
    Assert.assertTrue(fc2.isDirectory(testPath.getParent()));
    Assert.assertEquals("testDir", testPath.getParent().getName());
    Assert.assertTrue(fc2.exists(testPath));

  }

  @Test
  public void testCreateDirectory() throws IOException {

    Path path = qualifiedPath("test/hadoop", fc2);
    Path falsePath = qualifiedPath("path/doesnot.exist", fc2);
    Path subDirPath = qualifiedPath("dir0", fc2);

    // Ensure that testPath does not exist in fc1
    Assert.assertFalse(fc1.exists(path));
    Assert.assertFalse(fc1.isFile(path));
    Assert.assertFalse(fc1.isDirectory(path));

    // Create a directory on fc2's file system using fc1
   fc1.mkdir(path, FsPermission.getDefault(), true);

    // Ensure fc2 has directory
    Assert.assertTrue(fc2.isDirectory(path));
    Assert.assertTrue(fc2.exists(path));
    Assert.assertFalse(fc2.isFile(path));

    // Test to create same dir twice, (HDFS mkdir is similar to mkdir -p )
   fc1.mkdir(subDirPath, FsPermission.getDefault(), true);
    // This should not throw exception
   fc1.mkdir(subDirPath, FsPermission.getDefault(), true);

    // Create Sub Dirs
   fc1.mkdir(subDirPath, FsPermission.getDefault(), true);

    // Check parent dir
    Path parentDir = path.getParent();
    Assert.assertTrue(fc2.exists(parentDir));
    Assert.assertFalse(fc2.isFile(parentDir));

    // Check parent parent dir
    Path grandparentDir = parentDir.getParent();
    Assert.assertTrue(fc2.exists(grandparentDir));
    Assert.assertFalse(fc2.isFile(grandparentDir));

    // Negative test cases
    Assert.assertFalse(fc2.exists(falsePath));
    Assert.assertFalse(fc2.isDirectory(falsePath));

    // TestCase - Create multiple directories
    String dirNames[] = { 
        "createTest/testDir", "createTest/test Dir",
        "deleteTest/test*Dir", "deleteTest/test#Dir", 
        "deleteTest/test1234", "deleteTest/test_DIr", 
        "deleteTest/1234Test", "deleteTest/test)Dir",
        "deleteTest/()&^%$#@!~_+}{><?", "  ", "^ " };

    for (String f : dirNames) {
      // Create a file on fc2's file system using fc1
      Path testPath = qualifiedPath(f, fc2);
      // Ensure file does not exist
      Assert.assertFalse(fc2.exists(testPath));

      // Now create directory
     fc1.mkdir(testPath, FsPermission.getDefault(), true);
      // Ensure fc2 has the created directory
      Assert.assertTrue(fc2.exists(testPath));
      Assert.assertTrue(fc2.isDirectory(testPath));
    }

  }

  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = qualifiedPath("test/hadoop", fc2);
    Assert.assertFalse(fc2.exists(testDir));
    fc2.mkdir(testDir, FsPermission.getDefault(), true);
    Assert.assertTrue(fc2.exists(testDir));

    // Create file on fc1 using fc2 context
    createFile(qualifiedPath("test/hadoop/file", fc2), fc1);

    Path testSubDir = qualifiedPath("test/hadoop/file/subdir", fc2);
    try {
      fc1.mkdir(testSubDir, FsPermission.getDefault(), true);
      Assert.fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    Assert.assertFalse(fc1.exists(testSubDir));

    Path testDeepSubDir = qualifiedPath("test/hadoop/file/deep/sub/dir", fc1);
    try {
      fc2.mkdir(testDeepSubDir, FsPermission.getDefault(), true);
      Assert.fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    Assert.assertFalse(fc1.exists(testDeepSubDir));

  }

  @Test
  public void testIsDirectory() throws IOException {
    String dirName = "dirTest";
    String invalidDir = "nonExistantDir";
    String rootDir = "/";

    Path existingPath = qualifiedPath(dirName, fc2);
    Path nonExistingPath = qualifiedPath(invalidDir, fc2);
    Path pathToRootDir = qualifiedPath(rootDir, fc2);

    // Create a directory on fc2's file system using fc1
    fc1.mkdir(existingPath, FsPermission.getDefault(), true);

    // Ensure fc2 has directory
    Assert.assertTrue(fc2.isDirectory(existingPath));
    Assert.assertTrue(fc2.isDirectory(pathToRootDir));

    // Negative test case
    Assert.assertFalse(fc2.isDirectory(nonExistingPath));

  }

  @Test
  public void testDeleteFile() throws IOException {
    Path testPath = qualifiedPath("testFile", fc2);

    // Ensure file does not exist
    Assert.assertFalse(fc2.exists(testPath));

    // First create a file on file system using fc1
    createFile(testPath, fc1);

    // Ensure file exist
    Assert.assertTrue(fc2.exists(testPath));

    // Delete file using fc2
    fc2.delete(testPath, false);

    // Ensure fc2 does not have deleted file
    Assert.assertFalse(fc2.exists(testPath));

  }

  @Test
  public void testDeleteNonExistingFile() throws IOException {
    String testFileName = "testFile";
    Path testPath = qualifiedPath(testFileName, fc2);

    // TestCase1 : Test delete on file never existed
    // Ensure file does not exist
    Assert.assertFalse(fc2.exists(testPath));

    // Delete on non existing file should return false
    Assert.assertFalse(fc2.delete(testPath, false));

    // TestCase2 : Create , Delete , Delete file
    // Create a file on fc2's file system using fc1
    createFile(testPath, fc1);
    // Ensure file exist
    Assert.assertTrue(fc2.exists(testPath));

    // Delete test file, deleting existing file should return true
    Assert.assertTrue(fc2.delete(testPath, false));
    // Ensure file does not exist
    Assert.assertFalse(fc2.exists(testPath));
    // Delete on non existing file should return false
    Assert.assertFalse(fc2.delete(testPath, false));

  }

  @Test
  public void testDeleteNonExistingFileInDir() throws IOException {
    String testFileInDir = "testDir/testDir/TestFile";
    Path testPath = qualifiedPath(testFileInDir, fc2);

    // TestCase1 : Test delete on file never existed
    // Ensure file does not exist
    Assert.assertFalse(fc2.exists(testPath));

    // Delete on non existing file should return false
    Assert.assertFalse(fc2.delete(testPath, false));

    // TestCase2 : Create , Delete , Delete file
    // Create a file on fc2's file system using fc1
    createFile(testPath, fc1);
    // Ensure file exist
    Assert.assertTrue(fc2.exists(testPath));

    // Delete test file, deleting existing file should return true
    Assert.assertTrue(fc2.delete(testPath, false));
    // Ensure file does not exist
    Assert.assertFalse(fc2.exists(testPath));
    // Delete on non existing file should return false
    Assert.assertFalse(fc2.delete(testPath, false));

  }

  @Test
  public void testDeleteDirectory() throws IOException {
    String dirName = "dirTest";
    Path testDirPath = qualifiedPath(dirName, fc2);
    // Ensure directory does not exist
    Assert.assertFalse(fc2.exists(testDirPath));

    // Create a directory on fc2's file system using fc1
   fc1.mkdir(testDirPath, FsPermission.getDefault(), true);

    // Ensure dir is created
    Assert.assertTrue(fc2.exists(testDirPath));
    Assert.assertTrue(fc2.isDirectory(testDirPath));

    fc2.delete(testDirPath, true);

    // Ensure that directory is deleted
    Assert.assertFalse(fc2.isDirectory(testDirPath));

    // TestCase - Create and delete multiple directories
    String dirNames[] = { 
        "deleteTest/testDir", "deleteTest/test Dir",
        "deleteTest/test*Dir", "deleteTest/test#Dir", 
        "deleteTest/test1234", "deleteTest/1234Test", 
        "deleteTest/test)Dir", "deleteTest/test_DIr",
        "deleteTest/()&^%$#@!~_+}{><?", "  ", "^ " };

    for (String f : dirNames) {
      // Create a file on fc2's file system using fc1
      Path testPath = qualifiedPath(f, fc2);
      // Ensure file does not exist
      Assert.assertFalse(fc2.exists(testPath));

      // Now create directory
     fc1.mkdir(testPath, FsPermission.getDefault(), true);
      // Ensure fc2 has the created directory
      Assert.assertTrue(fc2.exists(testPath));
      Assert.assertTrue(fc2.isDirectory(testPath));
      // Delete dir
      Assert.assertTrue(fc2.delete(testPath, true));
      // verify if directory is deleted
      Assert.assertFalse(fc2.exists(testPath));
      Assert.assertFalse(fc2.isDirectory(testPath));
    }
  }

  @Test
  public void testDeleteNonExistingDirectory() throws IOException {
    String testDirName = "testFile";
    Path testPath = qualifiedPath(testDirName, fc2);

    // TestCase1 : Test delete on directory never existed
    // Ensure directory does not exist
    Assert.assertFalse(fc2.exists(testPath));

    // Delete on non existing directory should return false
    Assert.assertFalse(fc2.delete(testPath, false));

    // TestCase2 : Create dir, Delete dir, Delete dir
    // Create a file on fc2's file system using fc1

    fc1.mkdir(testPath, FsPermission.getDefault(), true);
    // Ensure dir exist
    Assert.assertTrue(fc2.exists(testPath));

    // Delete test file, deleting existing file should return true
    Assert.assertTrue(fc2.delete(testPath, false));
    // Ensure file does not exist
    Assert.assertFalse(fc2.exists(testPath));
    // Delete on non existing file should return false
    Assert.assertFalse(fc2.delete(testPath, false));
  }

  @Test
  public void testModificationTime() throws IOException {
    String testFile = "file1";
    long fc2ModificationTime, fc1ModificationTime;

    Path testPath = qualifiedPath(testFile, fc2);

    // Create a file on fc2's file system using fc1
    createFile(testPath, fc1);
    // Get modification time using fc2 and fc1
    fc1ModificationTime = fc1.getFileStatus(testPath).getModificationTime();
    fc2ModificationTime = fc2.getFileStatus(testPath).getModificationTime();
    // Ensure fc1 and fc2 reports same modification time
    Assert.assertEquals(fc1ModificationTime, fc2ModificationTime);
  }

  @Test
  public void testFileStatus() throws IOException {
    String fileName = "file1";
    Path path2 = fc2.makeQualified(new Path(BASE, fileName));

    // Create a file on fc2's file system using fc1
    createFile(path2, fc1);
    FsStatus fc2Status = fc2.getFsStatus(path2);

    // FsStatus , used, free and capacity are non-negative longs
    Assert.assertNotNull(fc2Status);
    Assert.assertTrue(fc2Status.getCapacity() > 0);
    Assert.assertTrue(fc2Status.getRemaining() > 0);
    Assert.assertTrue(fc2Status.getUsed() > 0);

  }

  @Test
  public void testGetFileStatusThrowsExceptionForNonExistentFile()
      throws Exception {
    String testFile = "test/hadoop/fileDoesNotExist";
    Path testPath = qualifiedPath(testFile, fc2);
    try {
      fc1.getFileStatus(testPath);
      Assert.fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      // expected
    }
  }

  @Test
  public void testListStatusThrowsExceptionForNonExistentFile()
      throws Exception {
    String testFile = "test/hadoop/file";
    Path testPath = qualifiedPath(testFile, fc2);
    try {
      fc1.listStatus(testPath);
      Assert.fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // expected
    }
  }

    
  @Test
  public void testListStatus() throws Exception {
    final String hPrefix = "test/hadoop";
    final String[] dirs = { 
        hPrefix + "/a", 
        hPrefix + "/b", 
        hPrefix + "/c",
        hPrefix + "/1", 
        hPrefix + "/#@#@",
        hPrefix + "/&*#$#$@234"};
    ArrayList<Path> testDirs = new ArrayList<Path>();

    for (String d : dirs) {
      testDirs.add(qualifiedPath(d, fc2));
    }
    Assert.assertFalse(fc1.exists(testDirs.get(0)));

    for (Path path : testDirs) {
     fc1.mkdir(path, FsPermission.getDefault(), true);
    }

    FileStatus[] paths = fc1.listStatus(qualifiedPath("test", fc1));
    Assert.assertEquals(1, paths.length);
    Assert.assertEquals(qualifiedPath(hPrefix, fc1), paths[0].getPath());

    paths = fc1.listStatus(qualifiedPath(hPrefix, fc1));
    Assert.assertEquals(6, paths.length);
    for (int i = 0; i < dirs.length; i++) {
      boolean found = false;
      for (int j = 0; j < paths.length; j++) {
        if (qualifiedPath(dirs[i],fc1).equals(paths[j].getPath())) {
          found = true;
        }
      }
      Assert.assertTrue(dirs[i] + " not found", found);
    }

    paths = fc1.listStatus(qualifiedPath(dirs[0], fc1));
    Assert.assertEquals(0, paths.length);
  }
}
