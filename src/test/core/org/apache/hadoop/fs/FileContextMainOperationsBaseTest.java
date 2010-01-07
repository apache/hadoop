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
import java.util.EnumSet;

import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.fs.FileContextTestHelper.*;

/**
 * <p>
 * A collection of tests for the {@link FileContext}.
 * This test should be used for testing an instance of FileContext
 *  that has been initialized to a specific default FileSystem such a
 *  LocalFileSystem, HDFS,S3, etc.
 * </p>
 * <p>
 * To test a given {@link FileSystem} implementation create a subclass of this
 * test and override {@link #setUp()} to initialize the <code>fc</code> 
 * {@link FileContext} instance variable.
 * 
 * Since this a junit 4 you can also do a single setup before 
 * the start of any tests.
 * E.g.
 *     @BeforeClass   public static void clusterSetupAtBegining()
 *     @AfterClass    public static void ClusterShutdownAtEnd()
 * </p>
 */
public abstract class FileContextMainOperationsBaseTest  {
  
  private static String TEST_DIR_AAA2 = "test/hadoop2/aaa";
  private static String TEST_DIR_AAA = "test/hadoop/aaa";
  private static String TEST_DIR_AXA = "test/hadoop/axa";
  private static String TEST_DIR_AXX = "test/hadoop/axx";
  private static int numBlocks = 2;
  
  static  final String LOCAL_FS_ROOT_URI = "file:///tmp/test";
  
  
  protected static FileContext fc;
  
  final private static PathFilter DEFAULT_FILTER = new PathFilter() {
    public boolean accept(final Path file) {
      return true;
    }
  };

  //A test filter with returns any path containing a "b" 
  final private static PathFilter TEST_X_FILTER = new PathFilter() {
    public boolean accept(Path file) {
      if(file.getName().contains("x") || file.toString().contains("X"))
        return true;
      else
        return false;
    }     
  };
  
  private static byte[] data = getFileData(numBlocks,
      getDefaultBlockSize());
  
  @Before
  public void setUp() throws Exception {
    fc.mkdir(getTestRootPath(fc, "test"), FileContext.DEFAULT_PERM, true);
  }
  
  @After
  public void tearDown() throws Exception {
    fc.delete(getTestRootPath(fc, "test"), true);
    fc.delete(new Path(LOCAL_FS_ROOT_URI), true);
  }
  
  
  protected Path getDefaultWorkingDirectory() throws IOException {
    return getTestRootPath(fc,
        "/user/" + System.getProperty("user.name")).makeQualified(
        fc.getDefaultFileSystem().getUri(), fc.getWorkingDirectory());
  }

  protected boolean renameSupported() {
    return true;
  }

  
  protected IOException unwrapException(IOException e) {
    return e;
  }
  
  @Test
  public void testFsStatus() throws Exception {
    FsStatus fsStatus = fc.getFsStatus(null);
    Assert.assertNotNull(fsStatus);
    //used, free and capacity are non-negative longs
    Assert.assertTrue(fsStatus.getUsed() >= 0);
    Assert.assertTrue(fsStatus.getRemaining() >= 0);
    Assert.assertTrue(fsStatus.getCapacity() >= 0);
  }
  
  @Test
  public void testWorkingDirectory() throws Exception {

    // First we cd to our test root
    Path workDir = new Path(getTestRootDir(fc), new Path("test"));
    fc.setWorkingDirectory(workDir);
    Assert.assertEquals(workDir, fc.getWorkingDirectory());

    fc.setWorkingDirectory(new Path("."));
    Assert.assertEquals(workDir, fc.getWorkingDirectory());

    fc.setWorkingDirectory(new Path(".."));
    Assert.assertEquals(workDir.getParent(), fc.getWorkingDirectory());
    
    // cd using a relative path

    // Go back to our test root
    workDir = getTestRootPath(fc, "test");
    fc.setWorkingDirectory(workDir);
    Assert.assertEquals(workDir, fc.getWorkingDirectory());
    
    Path relativeDir = new Path("existingDir1");
    Path absoluteDir = new Path(workDir,"existingDir1");
    fc.mkdir(absoluteDir, FileContext.DEFAULT_PERM, true);
    fc.setWorkingDirectory(relativeDir);
    Assert.assertEquals(absoluteDir, fc.getWorkingDirectory());
    // cd using a absolute path
    absoluteDir = getTestRootPath(fc, "test/existingDir2");
    fc.mkdir(absoluteDir, FileContext.DEFAULT_PERM, true);
    fc.setWorkingDirectory(absoluteDir);
    Assert.assertEquals(absoluteDir, fc.getWorkingDirectory());
    
    // Now open a file relative to the wd we just set above.
    Path absolutePath = new Path(absoluteDir, "foo");
    fc.create(absolutePath, EnumSet.of(CreateFlag.CREATE)).close();
    fc.open(new Path("foo")).close();

    absoluteDir = getTestRootPath(fc, "nonexistingPath");
    try {
      fc.setWorkingDirectory(absoluteDir);
      Assert.fail("cd to non existing dir should have failed");
    } catch (Exception e) {
      // Exception as expected
    }
    
    // Try a URI

    absoluteDir = new Path(LOCAL_FS_ROOT_URI + "/existingDir");
    fc.mkdir(absoluteDir, FileContext.DEFAULT_PERM, true);
    fc.setWorkingDirectory(absoluteDir);
    Assert.assertEquals(absoluteDir, fc.getWorkingDirectory());

  }
  
  @Test
  public void testMkdirs() throws Exception {
    Path testDir = getTestRootPath(fc, "test/hadoop");
    Assert.assertFalse(fc.exists(testDir));
    Assert.assertFalse(fc.isFile(testDir));

    fc.mkdir(testDir, FsPermission.getDefault(), true);

    Assert.assertTrue(fc.exists(testDir));
    Assert.assertFalse(fc.isFile(testDir));
    
    fc.mkdir(testDir, FsPermission.getDefault(), true);

    Assert.assertTrue(fc.exists(testDir));
    Assert.assertFalse(fc.isFile(testDir));

    Path parentDir = testDir.getParent();
    Assert.assertTrue(fc.exists(parentDir));
    Assert.assertFalse(fc.isFile(parentDir));

    Path grandparentDir = parentDir.getParent();
    Assert.assertTrue(fc.exists(grandparentDir));
    Assert.assertFalse(fc.isFile(grandparentDir));
    
  }
  
  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = getTestRootPath(fc, "test/hadoop");
    Assert.assertFalse(fc.exists(testDir));
    fc.mkdir(testDir, FsPermission.getDefault(), true);
    Assert.assertTrue(fc.exists(testDir));
    
    createFile(getTestRootPath(fc, "test/hadoop/file"));
    
    Path testSubDir = getTestRootPath(fc, "test/hadoop/file/subdir");
    try {
      fc.mkdir(testSubDir, FsPermission.getDefault(), true);
      Assert.fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    Assert.assertFalse(fc.exists(testSubDir));
    
    Path testDeepSubDir = getTestRootPath(fc, "test/hadoop/file/deep/sub/dir");
    try {
      fc.mkdir(testDeepSubDir, FsPermission.getDefault(), true);
      Assert.fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    Assert.assertFalse(fc.exists(testDeepSubDir));
    
  }
  
  @Test
  public void testGetFileStatusThrowsExceptionForNonExistentFile() 
    throws Exception {
    try {
      fc.getFileStatus(getTestRootPath(fc, "test/hadoop/file"));
      Assert.fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      // expected
    }
  } 
  
  public void testListStatusThrowsExceptionForNonExistentFile()
                                                    throws Exception {
    try {
      fc.listStatus(getTestRootPath(fc, "test/hadoop/file"));
      Assert.fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // expected
    }
  }
  
  @Test
  public void testListStatus() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, "test/hadoop/a"),
        getTestRootPath(fc, "test/hadoop/b"),
        getTestRootPath(fc, "test/hadoop/c/1"), };
    Assert.assertFalse(fc.exists(testDirs[0]));

    for (Path path : testDirs) {
      fc.mkdir(path, FsPermission.getDefault(), true);
    }

    FileStatus[] paths = fc.listStatus(getTestRootPath(fc, "test"));
    Assert.assertEquals(1, paths.length);
    Assert.assertEquals(getTestRootPath(fc, "test/hadoop"), paths[0].getPath());

    paths = fc.listStatus(getTestRootPath(fc, "test/hadoop"));
    Assert.assertEquals(3, paths.length);

    Assert.assertTrue(containsPath(getTestRootPath(fc, "test/hadoop/a"),
        paths));
    Assert.assertTrue(containsPath(getTestRootPath(fc, "test/hadoop/b"),
        paths));
    Assert.assertTrue(containsPath(getTestRootPath(fc, "test/hadoop/c"),
        paths));

    paths = fc.listStatus(getTestRootPath(fc, "test/hadoop/a"));
    Assert.assertEquals(0, paths.length);
  }
  
  @Test
  public void testListStatusFilterWithNoMatches() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA2),
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX), };
    
   if (fc.exists(testDirs[0]) == false) {
     for (Path path : testDirs) {
       fc.mkdir(path, FsPermission.getDefault(), true);
     }
   }

    // listStatus with filters returns empty correctly
    FileStatus[] filteredPaths = fc.util().listStatus(
        getTestRootPath(fc, "test"), TEST_X_FILTER);
    Assert.assertEquals(0,filteredPaths.length);
    
  }
  
  public void testListStatusFilterWithSomeMatches() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AAA2), };

    if (fc.exists(testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }

    // should return 2 paths ("/test/hadoop/axa" and "/test/hadoop/axx")
    FileStatus[] filteredPaths = fc.util()
        .listStatus(getTestRootPath(fc, "test/hadoop"),
            TEST_X_FILTER);
    Assert.assertEquals(2,filteredPaths.length);
    Assert.assertTrue(containsPath(getTestRootPath(fc,
        TEST_DIR_AXA), filteredPaths));
    Assert.assertTrue(containsPath(getTestRootPath(fc,
        TEST_DIR_AXX), filteredPaths));
  }
  
  @Test
  public void testGlobStatusThrowsExceptionForNonExistentFile() throws Exception {
    try {
      // This should throw a FileNotFoundException
      fc.util().globStatus(
          getTestRootPath(fc, "test/hadoopfsdf/?"));
      Assert.fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // expected
    }
  }
  
  @Test
  public void testGlobStatusWithNoMatchesInPath() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AAA2), };

    if (fc.exists(testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }

    // should return nothing
    FileStatus[] paths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop/?"));
    Assert.assertEquals(0, paths.length);
  }
  
  @Test
  public void testGlobStatusSomeMatchesInDirectories() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AAA2), };

    if (fc.exists(testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }
    
    // Should return two items ("/test/hadoop" and "/test/hadoop2")
    FileStatus[] paths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop*"));
    Assert.assertEquals(2, paths.length);
    Assert.assertTrue(containsPath(getTestRootPath(fc,
        "test/hadoop"), paths));
    Assert.assertTrue(containsPath(getTestRootPath(fc,
        "test/hadoop2"), paths));
  }
  
  @Test
  public void testGlobStatusWithMultipleWildCardMatches() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AAA2), };

    if (fc.exists(testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }

    //Should return all 4 items ("/test/hadoop/aaa", "/test/hadoop/axa"
    //"/test/hadoop/axx", and "/test/hadoop2/axx")
    FileStatus[] paths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop*/*"));
    Assert.assertEquals(4, paths.length);
    Assert.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AAA), paths));
    Assert.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXA), paths));
    Assert.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXX), paths));
    Assert.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AAA2), paths));
  }
  
  @Test
  public void testGlobStatusWithMultipleMatchesOfSingleChar() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AAA2), };

    if (fc.exists(testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }
    
    //Should return only 2 items ("/test/hadoop/axa", "/test/hadoop/axx")
    FileStatus[] paths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop/ax?"));
    Assert.assertEquals(2, paths.length);
    Assert.assertTrue(containsPath(getTestRootPath(fc,
        TEST_DIR_AXA), paths));
    Assert.assertTrue(containsPath(getTestRootPath(fc,
        TEST_DIR_AXX), paths));
  }
  
  @Test
  public void testGlobStatusFilterWithEmptyPathResults() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AXX), };

    if (fc.exists(testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }
    
    //This should return an empty set
    FileStatus[] filteredPaths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop/?"),
        DEFAULT_FILTER);
    Assert.assertEquals(0,filteredPaths.length);
  }
  
  @Test
  public void testGlobStatusFilterWithSomePathMatchesAndTrivialFilter()
      throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AXX), };

    if (fc.exists(testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }
    
    //This should return all three (aaa, axa, axx)
    FileStatus[] filteredPaths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop/*"),
        DEFAULT_FILTER);
    Assert.assertEquals(3, filteredPaths.length);
    Assert.assertTrue(containsPath(getTestRootPath(fc,
        TEST_DIR_AAA), filteredPaths));
    Assert.assertTrue(containsPath(getTestRootPath(fc,
        TEST_DIR_AXA), filteredPaths));
    Assert.assertTrue(containsPath(getTestRootPath(fc,
        TEST_DIR_AXX), filteredPaths));
  }
  
  @Test
  public void testGlobStatusFilterWithMultipleWildCardMatchesAndTrivialFilter()
      throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AXX), };

    if (fc.exists(testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }
    
    //This should return all three (aaa, axa, axx)
    FileStatus[] filteredPaths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop/a??"),
        DEFAULT_FILTER);
    Assert.assertEquals(3, filteredPaths.length);
    Assert.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AAA),
        filteredPaths));
    Assert.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXA),
        filteredPaths));
    Assert.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXX),
        filteredPaths));
  }
  
  @Test
  public void testGlobStatusFilterWithMultiplePathMatchesAndNonTrivialFilter()
      throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AXX), };

    if (fc.exists(testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }
    
    //This should return two (axa, axx)
    FileStatus[] filteredPaths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop/*"),
        TEST_X_FILTER);
    Assert.assertEquals(2, filteredPaths.length);
    Assert.assertTrue(containsPath(getTestRootPath(fc,
        TEST_DIR_AXA), filteredPaths));
    Assert.assertTrue(containsPath(getTestRootPath(fc,
        TEST_DIR_AXX), filteredPaths));
  }
  
  @Test
  public void testGlobStatusFilterWithNoMatchingPathsAndNonTrivialFilter()
      throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AXX), };

    if (fc.exists(testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }
    
    //This should return an empty set
    FileStatus[] filteredPaths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop/?"),
        TEST_X_FILTER);
    Assert.assertEquals(0,filteredPaths.length);
  }
  
  @Test
  public void testGlobStatusFilterWithMultiplePathWildcardsAndNonTrivialFilter()
      throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AXX), };

    if (fc.exists(testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }
    
    //This should return two (axa, axx)
    FileStatus[] filteredPaths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop/a??"),
        TEST_X_FILTER);
    Assert.assertEquals(2, filteredPaths.length);
    Assert.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXA),
        filteredPaths));
    Assert.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXX),
        filteredPaths));
  }
  
  @Test
  public void testWriteReadAndDeleteEmptyFile() throws Exception {
    writeReadAndDelete(0);
  }

  @Test
  public void testWriteReadAndDeleteHalfABlock() throws Exception {
    writeReadAndDelete(getDefaultBlockSize() / 2);
  }

  @Test
  public void testWriteReadAndDeleteOneBlock() throws Exception {
    writeReadAndDelete(getDefaultBlockSize());
  }
  
  @Test
  public void testWriteReadAndDeleteOneAndAHalfBlocks() throws Exception {
    int blockSize = getDefaultBlockSize();
    writeReadAndDelete(blockSize + (blockSize / 2));
  }
  
  @Test
  public void testWriteReadAndDeleteTwoBlocks() throws Exception {
    writeReadAndDelete(getDefaultBlockSize() * 2);
  }
  
  private void writeReadAndDelete(int len) throws IOException {
    Path path = getTestRootPath(fc, "test/hadoop/file");
    
    fc.mkdir(path.getParent(), FsPermission.getDefault(), true);

    FSDataOutputStream out = fc.create(path, EnumSet.of(CreateFlag.CREATE),
        CreateOpts.repFac((short) 1), CreateOpts
            .blockSize(getDefaultBlockSize()));
    out.write(data, 0, len);
    out.close();

    Assert.assertTrue("Exists", fc.exists(path));
    Assert.assertEquals("Length", len, fc.getFileStatus(path).getLen());

    FSDataInputStream in = fc.open(path);
    byte[] buf = new byte[len];
    in.readFully(0, buf);
    in.close();

    Assert.assertEquals(len, buf.length);
    for (int i = 0; i < buf.length; i++) {
      Assert.assertEquals("Position " + i, data[i], buf[i]);
    }
    
    Assert.assertTrue("Deleted", fc.delete(path, false));
    
    Assert.assertFalse("No longer exists", fc.exists(path));

  }
  
  @Test
  public void testOverwrite() throws IOException {
    Path path = getTestRootPath(fc, "test/hadoop/file");
    
    fc.mkdir(path.getParent(), FsPermission.getDefault(), true);

    createFile(path);
    
    Assert.assertTrue("Exists", fc.exists(path));
    Assert.assertEquals("Length", data.length, fc.getFileStatus(path).getLen());
    
    try {
      fc.create(path, EnumSet.of(CreateFlag.CREATE));
      Assert.fail("Should throw IOException.");
    } catch (IOException e) {
      // Expected
    }
    
    FSDataOutputStream out = fc.create(path,EnumSet.of(CreateFlag.OVERWRITE));
    out.write(data, 0, data.length);
    out.close();
    
    Assert.assertTrue("Exists", fc.exists(path));
    Assert.assertEquals("Length", data.length, fc.getFileStatus(path).getLen());
    
  }
  
  @Test
  public void testWriteInNonExistentDirectory() throws IOException {
    Path path = getTestRootPath(fc, "test/hadoop/file");
    Assert.assertFalse("Parent doesn't exist", fc.exists(path.getParent()));
    createFile(path);
    
    Assert.assertTrue("Exists", fc.exists(path));
    Assert.assertEquals("Length", data.length, fc.getFileStatus(path).getLen());
    Assert.assertTrue("Parent exists", fc.exists(path.getParent()));
  }

  @Test
  public void testDeleteNonExistentFile() throws IOException {
    Path path = getTestRootPath(fc, "test/hadoop/file");    
    Assert.assertFalse("Doesn't exist", fc.exists(path));
    Assert.assertFalse("No deletion", fc.delete(path, true));
  }
  
  @Test
  public void testDeleteRecursively() throws IOException {
    Path dir = getTestRootPath(fc, "test/hadoop");
    Path file = getTestRootPath(fc, "test/hadoop/file");
    Path subdir = getTestRootPath(fc, "test/hadoop/subdir");
    
    createFile(file);
    fc.mkdir(subdir,FsPermission.getDefault(), true);
    
    Assert.assertTrue("File exists", fc.exists(file));
    Assert.assertTrue("Dir exists", fc.exists(dir));
    Assert.assertTrue("Subdir exists", fc.exists(subdir));
    
    try {
      fc.delete(dir, false);
      Assert.fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    Assert.assertTrue("File still exists", fc.exists(file));
    Assert.assertTrue("Dir still exists", fc.exists(dir));
    Assert.assertTrue("Subdir still exists", fc.exists(subdir));
    
    Assert.assertTrue("Deleted", fc.delete(dir, true));
    Assert.assertFalse("File doesn't exist", fc.exists(file));
    Assert.assertFalse("Dir doesn't exist", fc.exists(dir));
    Assert.assertFalse("Subdir doesn't exist", fc.exists(subdir));
  }
  
  @Test
  public void testDeleteEmptyDirectory() throws IOException {
    Path dir = getTestRootPath(fc, "test/hadoop");
    fc.mkdir(dir, FsPermission.getDefault(), true);
    Assert.assertTrue("Dir exists", fc.exists(dir));
    Assert.assertTrue("Deleted", fc.delete(dir, false));
    Assert.assertFalse("Dir doesn't exist", fc.exists(dir));
  }
  
  @Test
  public void testRenameNonExistentPath() throws Exception {
    if (!renameSupported()) return;
    Path src = getTestRootPath(fc, "test/hadoop/nonExistent");
    Path dst = getTestRootPath(fc, "test/new/newpath");
    try {
      rename(src, dst, false, false, false, Rename.NONE);
      Assert.fail("Should throw FileNotFoundException");
    } catch (IOException e) {
      Assert.assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }

    try {
      rename(src, dst, false, false, false, Rename.OVERWRITE);
      Assert.fail("Should throw FileNotFoundException");
    } catch (IOException e) {
      Assert.assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }
  }

  @Test
  public void testRenameFileToNonExistentDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fc, "test/hadoop/file");
    createFile(src);
    Path dst = getTestRootPath(fc, "test/nonExistent/newfile");
    
    try {
      rename(src, dst, false, true, false, Rename.NONE);
      Assert.fail("Expected exception is not thrown");
    } catch (IOException e) {
      Assert.assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }

    try {
      rename(src, dst, false, true, false, Rename.OVERWRITE);
      Assert.fail("Expected exception is not thrown");
    } catch (IOException e) {
      Assert.assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }
  }

  @Test
  public void testRenameFileToDestinationWithParentFile() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fc, "test/hadoop/file");
    createFile(src);
    Path dst = getTestRootPath(fc, "test/parentFile/newfile");
    createFile(dst.getParent());
    
    try {
      rename(src, dst, false, true, false, Rename.NONE);
      Assert.fail("Expected exception is not thrown");
    } catch (IOException e) {
    }

    try {
      rename(src, dst, false, true, false, Rename.OVERWRITE);
      Assert.fail("Expected exception is not thrown");
    } catch (IOException e) {
    }
  }

  @Test
  public void testRenameFileToExistingParent() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fc, "test/hadoop/file");
    createFile(src);
    Path dst = getTestRootPath(fc, "test/new/newfile");
    fc.mkdir(dst.getParent(), FileContext.DEFAULT_PERM, true);
    rename(src, dst, true, false, true, Rename.OVERWRITE);
  }

  @Test
  public void testRenameFileAsExistingFile() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fc, "test/hadoop/file");
    createFile(src);
    Path dst = getTestRootPath(fc, "test/new/existingFile");
    createFile(dst);
    
    // Fails without overwrite option
    try {
      rename(src, dst, false, true, false, Rename.NONE);
      Assert.fail("Expected exception is not thrown");
    } catch (IOException e) {
      Assert.assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    
    // Succeeds with overwrite option
    rename(src, dst, true, false, true, Rename.OVERWRITE);
  }

  @Test
  public void testRenameFileAsExistingDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fc, "test/hadoop/file");
    createFile(src);
    Path dst = getTestRootPath(fc, "test/new/existingDir");
    fc.mkdir(dst, FileContext.DEFAULT_PERM, true);
    
    // Fails without overwrite option
    try {
      rename(src, dst, false, false, true, Rename.NONE);
      Assert.fail("Expected exception is not thrown");
    } catch (IOException e) {
    }
    
    // File cannot be renamed as directory
    try {
      rename(src, dst, false, false, true, Rename.OVERWRITE);
      Assert.fail("Expected exception is not thrown");
    } catch (IOException e) {
    }
  }

  @Test
  public void testRenameDirectoryToNonExistentParent() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fc, "test/hadoop/dir");
    fc.mkdir(src, FileContext.DEFAULT_PERM, true);
    Path dst = getTestRootPath(fc, "test/nonExistent/newdir");
    
    try {
      rename(src, dst, false, true, false, Rename.NONE);
      Assert.fail("Expected exception is not thrown");
    } catch (IOException e) {
      Assert.assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }

    try {
      rename(src, dst, false, true, false, Rename.OVERWRITE);
      Assert.fail("Expected exception is not thrown");
    } catch (IOException e) {
      Assert.assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }
  }

  @Test
  public void testRenameDirectoryAsNonExistentDirectory() throws Exception {
    testRenameDirectoryAsNonExistentDirectory(Rename.NONE);
    tearDown();
    testRenameDirectoryAsNonExistentDirectory(Rename.OVERWRITE);
  }

  private void testRenameDirectoryAsNonExistentDirectory(Rename... options) throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fc, "test/hadoop/dir");
    fc.mkdir(src, FileContext.DEFAULT_PERM, true);
    createFile(getTestRootPath(fc, "test/hadoop/dir/file1"));
    createFile(getTestRootPath(fc, "test/hadoop/dir/subdir/file2"));
    
    Path dst = getTestRootPath(fc, "test/new/newdir");
    fc.mkdir(dst.getParent(), FileContext.DEFAULT_PERM, true);
    
    rename(src, dst, true, false, true, options);
    Assert.assertFalse("Nested file1 exists", 
        fc.exists(getTestRootPath(fc, "test/hadoop/dir/file1")));
    Assert.assertFalse("Nested file2 exists", 
        fc.exists(getTestRootPath(fc, "test/hadoop/dir/subdir/file2")));
    Assert.assertTrue("Renamed nested file1 exists", fc
        .exists(getTestRootPath(fc, "test/new/newdir/file1")));
    Assert.assertTrue("Renamed nested exists", 
        fc.exists(getTestRootPath(fc, "test/new/newdir/subdir/file2")));
  }

  @Test
  public void testRenameDirectoryAsNonEmptyDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fc, "test/hadoop/dir");
    fc.mkdir(src, FileContext.DEFAULT_PERM, true);
    createFile(getTestRootPath(fc, "test/hadoop/dir/file1"));
    createFile(getTestRootPath(fc, "test/hadoop/dir/subdir/file2"));
    
    Path dst = getTestRootPath(fc, "test/new/newdir");
    fc.mkdir(dst, FileContext.DEFAULT_PERM, true);
    createFile(getTestRootPath(fc, "test/new/newdir/file1"));
    // Fails without overwrite option
    try {
      rename(src, dst, false, true, false, Rename.NONE);
      Assert.fail("Expected exception is not thrown");
    } catch (IOException e) {
      Assert.assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Succeeds with overwrite option
    try {
      rename(src, dst, false, true, false, Rename.OVERWRITE);
      Assert.fail("Expected exception is not thrown");
    } catch (IOException ex) {
      // Expected exception
    }
  }

  @Test
  public void testRenameDirectoryAsFile() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fc, "test/hadoop/dir");
    fc.mkdir(src, FileContext.DEFAULT_PERM, true);
    Path dst = getTestRootPath(fc, "test/new/newfile");
    createFile(dst);
    // Fails without overwrite option
    try {
      rename(src, dst, false, true, true, Rename.NONE);
      Assert.fail("Expected exception is not thrown");
    } catch (IOException e) {
    }
    // Directory cannot be renamed as existing file
    try {
      rename(src, dst, false, true, true, Rename.OVERWRITE);
      Assert.fail("Expected exception is not thrown");
    } catch (IOException ex) {
    }
  }

  @Test
  public void testInputStreamClosedTwice() throws IOException {
    //HADOOP-4760 according to Closeable#close() closing already-closed 
    //streams should have no effect. 
    Path src = getTestRootPath(fc, "test/hadoop/file");
    createFile(src);
    FSDataInputStream in = fc.open(src);
    in.close();
    in.close();
  }
  
  @Test
  public void testOutputStreamClosedTwice() throws IOException {
    //HADOOP-4760 according to Closeable#close() closing already-closed 
    //streams should have no effect. 
    Path src = getTestRootPath(fc, "test/hadoop/file");
    FSDataOutputStream out = fc.create(src, EnumSet.of(CreateFlag.CREATE),
            Options.CreateOpts.createParent());
    
    out.writeChar('H'); //write some data
    out.close();
    out.close();
  }

  protected void createFile(Path path) throws IOException {
    FSDataOutputStream out = fc.create(path, EnumSet.of(CreateFlag.CREATE),
        Options.CreateOpts.createParent());
    out.write(data, 0, data.length);
    out.close();
  }

  private void rename(Path src, Path dst, boolean renameShouldSucceed,
      boolean srcExists, boolean dstExists, Rename... options)
      throws IOException {
    fc.rename(src, dst, options);
    if (!renameShouldSucceed)
      Assert.fail("rename should have thrown exception");
    Assert.assertEquals("Source exists", srcExists, fc.exists(src));
    Assert.assertEquals("Destination exists", dstExists, fc.exists(dst));
  }
  private boolean containsPath(Path path, FileStatus[] filteredPaths)
    throws IOException {
    for(int i = 0; i < filteredPaths.length; i ++) { 
      if (getTestRootPath(fc, path.toString()).equals(
          filteredPaths[i].getPath()))
        return true;
      }
    return false;
 }
}
