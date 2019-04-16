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
import java.io.InputStream;
import java.net.URI;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * <p>
 * A collection of tests for the {@link FileSystem}.
 * This test should be used for testing an instance of FileSystem
 *  that has been initialized to a specific default FileSystem such a
 *  LocalFileSystem, HDFS,S3, etc.
 * </p>
 * <p>
 * To test a given {@link FileSystem} implementation create a subclass of this
 * test and override {@link #setUp()} to initialize the <code>fSys</code> 
 * {@link FileSystem} instance variable.
 * 
 * Since this a junit 4 you can also do a single setup before 
 * the start of any tests.
 * E.g.
 *     @BeforeClass   public static void clusterSetupAtBeginning()
 *     @AfterClass    public static void ClusterShutdownAtEnd()
 * </p>
 */
public abstract class FSMainOperationsBaseTest extends FileSystemTestHelper {
  
  private static String TEST_DIR_AAA2 = "test/hadoop2/aaa";
  private static String TEST_DIR_AAA = "test/hadoop/aaa";
  private static String TEST_DIR_AXA = "test/hadoop/axa";
  private static String TEST_DIR_AXX = "test/hadoop/axx";
  private static int numBlocks = 2;
  

  protected FileSystem fSys;
  
  private static final PathFilter DEFAULT_FILTER = (file) -> true;

  //A test filter with returns any path containing an "x" or "X"
  final private static PathFilter TEST_X_FILTER =
      (file) -> (file.getName().contains("x") || file.getName().contains("X"));

  protected static final byte[] data = getFileData(numBlocks,
      getDefaultBlockSize());
  
  protected abstract FileSystem createFileSystem() throws Exception;

  public FSMainOperationsBaseTest() {
  }
  
  public FSMainOperationsBaseTest(String testRootDir) {
      super(testRootDir);
  }
  
  @Before
  public void setUp() throws Exception {
    checkNotNull(testRootDir, "testRootDir");
    fSys = checkNotNull(createFileSystem(), "filesystem");
    fSys.mkdirs(checkNotNull(getTestRootPath(fSys, "test")));
  }
  
  @After
  public void tearDown() throws Exception {
    if (fSys != null) {
      fSys.delete(new Path(getAbsoluteTestRootPath(fSys), new Path("test")), true);
    }
  }

  protected Path getDefaultWorkingDirectory() throws IOException {
    return getTestRootPath(fSys,
        "/user/" + System.getProperty("user.name")).makeQualified(
        fSys.getUri(), fSys.getWorkingDirectory());
  }

  /**
   * Override point: is rename supported.
   * @return true if rename is supported.
   */
  protected boolean renameSupported() {
    return true;
  }

  /**
   * Skip the test if rename is not supported.
   */
  protected void assumeRenameSupported() {
    assumeTrue("Rename is not supported", renameSupported());
  }

  /**
   * Override point: are posix-style permissions supported.
   * @return true if permissions are supported.
   */
  protected boolean permissionsSupported() {
    return true;
  }

  /**
   * Skip the test if permissions are not supported.
   */
  protected void assumePermissionsSupported() {
    assumeTrue("Permissions are not supported", permissionsSupported());
  }

  @Test
  public void testFsStatus() throws Exception {
    FsStatus fsStatus = fSys.getStatus(null);
    assertNotNull(fsStatus);
    //used, free and capacity are non-negative longs
    assertTrue(fsStatus.getUsed() >= 0);
    assertTrue(fsStatus.getRemaining() >= 0);
    assertTrue(fsStatus.getCapacity() >= 0);
  }
  
  @Test
  public void testWorkingDirectory() throws Exception {

    // First we cd to our test root
    Path workDir = new Path(getAbsoluteTestRootPath(fSys), new Path("test"));
    fSys.setWorkingDirectory(workDir);
    assertEquals(workDir, fSys.getWorkingDirectory());

    fSys.setWorkingDirectory(new Path("."));
    assertEquals(workDir, fSys.getWorkingDirectory());

    fSys.setWorkingDirectory(new Path(".."));
    assertEquals(workDir.getParent(), fSys.getWorkingDirectory());
    
    // cd using a relative path

    // Go back to our test root
    workDir = new Path(getAbsoluteTestRootPath(fSys), new Path("test"));
    fSys.setWorkingDirectory(workDir);
    assertEquals(workDir, fSys.getWorkingDirectory());
    
    Path relativeDir = new Path("existingDir1");
    Path absoluteDir = new Path(workDir,"existingDir1");
    fSys.mkdirs(absoluteDir);
    fSys.setWorkingDirectory(relativeDir);
    assertEquals(absoluteDir, fSys.getWorkingDirectory());
    // cd using a absolute path
    absoluteDir = getTestRootPath(fSys, "test/existingDir2");
    fSys.mkdirs(absoluteDir);
    fSys.setWorkingDirectory(absoluteDir);
    assertEquals(absoluteDir, fSys.getWorkingDirectory());
    
    // Now open a file relative to the wd we just set above.
    Path absolutePath = new Path(absoluteDir, "foo");
    createFile(fSys, absolutePath);
    fSys.open(new Path("foo")).close();
    
    
    // Now mkdir relative to the dir we cd'ed to
    fSys.mkdirs(new Path("newDir"));
    assertTrue(isDir(fSys, new Path(absoluteDir, "newDir")));

    /**
     * We cannot test this because FileSystem has never checked for
     * existence of working dir - fixing  this would break compatibility,
     * 
    absoluteDir = getTestRootPath(fSys, "nonexistingPath");
    try {
      fSys.setWorkingDirectory(absoluteDir);
      Assert.fail("cd to non existing dir should have failed");
    } catch (Exception e) {
      // Exception as expected
    }
    */
  }
  
  
  // Try a URI
  
  @Test
  public void testWDAbsolute() throws IOException {
    Path absoluteDir = new Path(fSys.getUri() + "/test/existingDir");
    fSys.mkdirs(absoluteDir);
    fSys.setWorkingDirectory(absoluteDir);
    assertEquals(absoluteDir, fSys.getWorkingDirectory());
  }
  
  @Test
  public void testMkdirs() throws Exception {
    Path testDir = getTestRootPath(fSys, "test/hadoop");
    assertFalse(exists(fSys, testDir));
    assertFalse(isFile(fSys, testDir));

    fSys.mkdirs(testDir);

    assertTrue(exists(fSys, testDir));
    assertFalse(isFile(fSys, testDir));
    
    fSys.mkdirs(testDir);

    assertTrue(exists(fSys, testDir));
    assertFalse(isFile(fSys, testDir));

    Path parentDir = testDir.getParent();
    assertTrue(exists(fSys, parentDir));
    assertFalse(isFile(fSys, parentDir));

    Path grandparentDir = parentDir.getParent();
    assertTrue(exists(fSys, grandparentDir));
    assertFalse(isFile(fSys, grandparentDir));
    
  }
  
  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = getTestRootPath(fSys, "test/hadoop");
    assertFalse(exists(fSys, testDir));
    fSys.mkdirs(testDir);
    assertTrue(exists(fSys, testDir));
    
    createFile(getTestRootPath(fSys, "test/hadoop/file"));
    
    Path testSubDir = getTestRootPath(fSys, "test/hadoop/file/subdir");
    intercept(IOException.class, () ->
      fSys.mkdirs(testSubDir));
    assertFalse(exists(fSys, testSubDir));
    
    Path testDeepSubDir = getTestRootPath(fSys, "test/hadoop/file/deep/sub/dir");
    intercept(IOException.class, () ->
        fSys.mkdirs(testDeepSubDir));
    assertFalse(exists(fSys, testDeepSubDir));
    
  }
  
  @Test
  public void testGetFileStatusThrowsExceptionForNonExistentFile()
      throws Exception {
    intercept(FileNotFoundException.class, () ->
      fSys.getFileStatus(getTestRootPath(fSys, "test/hadoop/file")));
  }
  
  @Test
  public void testListStatusThrowsExceptionForNonExistentFile()
      throws Exception {
    intercept(FileNotFoundException.class, () ->
      fSys.listStatus(getTestRootPath(fSys, "test/hadoop/file")));
  }

  @Test
  public void testListStatusThrowsExceptionForUnreadableDir()
  throws Exception {
    assumePermissionsSupported();
    Path testRootDir = getTestRootPath(fSys, "test/hadoop/dir");
    Path obscuredDir = new Path(testRootDir, "foo");
    Path subDir = new Path(obscuredDir, "bar"); //so foo is non-empty
    fSys.mkdirs(subDir);
    fSys.setPermission(obscuredDir, new FsPermission((short)0)); //no access
    try {
      intercept(IOException.class, () ->
          fSys.listStatus(obscuredDir));
    } finally {
      // make sure the test directory can be deleted
      fSys.setPermission(obscuredDir, new FsPermission((short)0755)); //default
    }
  }


  @Test
  public void testListStatus() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, "test/hadoop/a"),
        getTestRootPath(fSys, "test/hadoop/b"),
        getTestRootPath(fSys, "test/hadoop/c/1"), };
    assertFalse(exists(fSys, testDirs[0]));

    for (Path path : testDirs) {
      fSys.mkdirs(path);
    }

    // test listStatus that returns an array
    FileStatus[] paths = fSys.listStatus(getTestRootPath(fSys, "test"));
    assertEquals(1, paths.length);
    assertEquals(getTestRootPath(fSys, "test/hadoop"), paths[0].getPath());

    paths = fSys.listStatus(getTestRootPath(fSys, "test/hadoop"));
    assertEquals(3, paths.length);

    assertTrue(containsTestRootPath(getTestRootPath(fSys, "test/hadoop/a"),
        paths));
    assertTrue(containsTestRootPath(getTestRootPath(fSys, "test/hadoop/b"),
        paths));
    assertTrue(containsTestRootPath(getTestRootPath(fSys, "test/hadoop/c"),
        paths));

    paths = fSys.listStatus(getTestRootPath(fSys, "test/hadoop/a"));
    assertEquals(0, paths.length);
    
  }
  
  @Test
  public void testListStatusFilterWithNoMatches() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA2),
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX), };
    
   if (exists(fSys, testDirs[0]) == false) {
     for (Path path : testDirs) {
       fSys.mkdirs(path);
     }
   }

    // listStatus with filters returns empty correctly
    FileStatus[] filteredPaths = fSys.listStatus(
        getTestRootPath(fSys, "test"), TEST_X_FILTER);
    assertEquals(0,filteredPaths.length);
    
  }
  
  @Test
  public void testListStatusFilterWithSomeMatches() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AAA2), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }

    // should return 2 paths ("/test/hadoop/axa" and "/test/hadoop/axx")
    FileStatus[] filteredPaths = fSys.listStatus(
        getTestRootPath(fSys, "test/hadoop"), TEST_X_FILTER);
    assertEquals(2, filteredPaths.length);
    assertTrue(containsTestRootPath(getTestRootPath(fSys,
        TEST_DIR_AXA), filteredPaths));
    assertTrue(containsTestRootPath(getTestRootPath(fSys,
        TEST_DIR_AXX), filteredPaths));
  }
  
  @Test
  public void testGlobStatusNonExistentFile() throws Exception {
    FileStatus[] paths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoopfsdf"));
    assertNull(paths);

    paths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoopfsdf/?"));
    assertEquals(0, paths.length);
    paths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoopfsdf/xyz*/?"));
    assertEquals(0, paths.length);
  }
  
  @Test
  public void testGlobStatusWithNoMatchesInPath() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AAA2), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }

    // should return nothing
    FileStatus[] paths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop/?"));
    assertEquals(0, paths.length);
  }
  
  @Test
  public void testGlobStatusSomeMatchesInDirectories() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AAA2), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }
    
    // Should return two items ("/test/hadoop" and "/test/hadoop2")
    FileStatus[] paths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop*"));
    assertEquals(2, paths.length);
    assertTrue(containsTestRootPath(getTestRootPath(fSys,
        "test/hadoop"), paths));
    assertTrue(containsTestRootPath(getTestRootPath(fSys,
        "test/hadoop2"), paths));
  }
  
  @Test
  public void testGlobStatusWithMultipleWildCardMatches() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AAA2), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }

    //Should return all 4 items ("/test/hadoop/aaa", "/test/hadoop/axa"
    //"/test/hadoop/axx", and "/test/hadoop2/axx")
    FileStatus[] paths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop*/*"));
    assertEquals(4, paths.length);
    assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AAA), paths));
    assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXA), paths));
    assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXX), paths));
    assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AAA2), paths));
  }
  
  @Test
  public void testGlobStatusWithMultipleMatchesOfSingleChar() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AAA2), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }
    
    //Should return only 2 items ("/test/hadoop/axa", "/test/hadoop/axx")
    FileStatus[] paths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop/ax?"));
    assertEquals(2, paths.length);
    assertTrue(containsTestRootPath(getTestRootPath(fSys,
        TEST_DIR_AXA), paths));
    assertTrue(containsTestRootPath(getTestRootPath(fSys,
        TEST_DIR_AXX), paths));
  }
  
  @Test
  public void testGlobStatusFilterWithEmptyPathResults() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AXX), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }
    
    //This should return an empty set
    FileStatus[] filteredPaths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop/?"),
        DEFAULT_FILTER);
    assertEquals(0, filteredPaths.length);
  }
  
  @Test
  public void testGlobStatusFilterWithSomePathMatchesAndTrivialFilter()
      throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AXX), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }
    
    //This should return all three (aaa, axa, axx)
    FileStatus[] filteredPaths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop/*"),
        DEFAULT_FILTER);
    assertEquals(3, filteredPaths.length);
    assertTrue(containsTestRootPath(getTestRootPath(fSys,
        TEST_DIR_AAA), filteredPaths));
    assertTrue(containsTestRootPath(getTestRootPath(fSys,
        TEST_DIR_AXA), filteredPaths));
    assertTrue(containsTestRootPath(getTestRootPath(fSys,
        TEST_DIR_AXX), filteredPaths));
  }
  
  @Test
  public void testGlobStatusFilterWithMultipleWildCardMatchesAndTrivialFilter()
      throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AXX), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }
    
    //This should return all three (aaa, axa, axx)
    FileStatus[] filteredPaths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop/a??"),
        DEFAULT_FILTER);
    assertEquals(3, filteredPaths.length);
    assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AAA),
        filteredPaths));
    assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXA),
        filteredPaths));
    assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXX),
        filteredPaths));
  }
  
  @Test
  public void testGlobStatusFilterWithMultiplePathMatchesAndNonTrivialFilter()
      throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AXX), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }
    
    //This should return two (axa, axx)
    FileStatus[] filteredPaths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop/*"),
        TEST_X_FILTER);
    assertEquals(2, filteredPaths.length);
    assertTrue(containsTestRootPath(getTestRootPath(fSys,
        TEST_DIR_AXA), filteredPaths));
    assertTrue(containsTestRootPath(getTestRootPath(fSys,
        TEST_DIR_AXX), filteredPaths));
  }
  
  @Test
  public void testGlobStatusFilterWithNoMatchingPathsAndNonTrivialFilter()
      throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AXX), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }
    
    //This should return an empty set
    FileStatus[] filteredPaths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop/?"),
        TEST_X_FILTER);
    assertEquals(0, filteredPaths.length);
  }
  
  @Test
  public void testGlobStatusFilterWithMultiplePathWildcardsAndNonTrivialFilter()
      throws Exception {
    Path[] testDirs = {
        getTestRootPath(fSys, TEST_DIR_AAA),
        getTestRootPath(fSys, TEST_DIR_AXA),
        getTestRootPath(fSys, TEST_DIR_AXX),
        getTestRootPath(fSys, TEST_DIR_AXX), };

    if (exists(fSys, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fSys.mkdirs(path);
      }
    }
    
    //This should return two (axa, axx)
    FileStatus[] filteredPaths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoop/a??"),
        TEST_X_FILTER);
    assertEquals(2, filteredPaths.length);
    assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXA),
        filteredPaths));
    assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXX),
        filteredPaths));
  }

  @Test
  public void testGlobStatusThrowsExceptionForUnreadableDir()
      throws Exception {
    assumePermissionsSupported();
    Path testRootDir = getTestRootPath(fSys, "test/hadoop/dir");
    Path obscuredDir = new Path(testRootDir, "foo");
    Path subDir = new Path(obscuredDir, "bar"); //so foo is non-empty
    fSys.mkdirs(subDir);
    fSys.setPermission(obscuredDir, new FsPermission((short)0)); //no access
    try {
      intercept(IOException.class, () ->
          fSys.globStatus(getTestRootPath(fSys, "test/hadoop/dir/foo/*")));
    } finally {
      // make sure the test directory can be deleted
      fSys.setPermission(obscuredDir, new FsPermission((short)0755)); //default
    }
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
  
  protected void writeReadAndDelete(int len) throws IOException {
    Path path = getTestRootPath(fSys, "test/hadoop/file");
    
    fSys.mkdirs(path.getParent());

   
    try(FSDataOutputStream out =
      fSys.create(path, false, 4096, (short) 1, getDefaultBlockSize())) {
      out.write(data, 0, len);
    }

    assertTrue("Exists", exists(fSys, path));
    assertEquals("Length", len, fSys.getFileStatus(path).getLen());

    byte[] buf = new byte[len];
    try(FSDataInputStream in = fSys.open(path)) {
      in.readFully(0, buf);
    }

    assertEquals(len, buf.length);
    for (int i = 0; i < buf.length; i++) {
      assertEquals("Position " + i, data[i], buf[i]);
    }
    
    assertTrue("Deleted", fSys.delete(path, false));
    
    assertFalse("No longer exists", exists(fSys, path));

  }
  
  @Test
  public void testOverwrite() throws Exception {
    Path path = getTestRootPath(fSys, "test/hadoop/file");
    
    fSys.mkdirs(path.getParent());

    createFile(path);
    
    assertTrue("Exists", exists(fSys, path));
    assertEquals("Length", data.length, fSys.getFileStatus(path).getLen());
    intercept(IOException.class, () ->
      createFile(path));

    try (FSDataOutputStream out = fSys.create(path, true, 4096)) {
      out.write(data, 0, data.length);
    }

    assertTrue("Exists", exists(fSys, path));
    assertEquals("Length", data.length, fSys.getFileStatus(path).getLen());
    
  }
  
  @Test
  public void testWriteInNonExistentDirectory() throws IOException {
    Path path = getTestRootPath(fSys, "test/hadoop/file");
    assertFalse("Parent doesn't exist", exists(fSys, path.getParent()));
    createFile(path);
    
    assertTrue("Exists", exists(fSys, path));
    assertEquals("Length", data.length, fSys.getFileStatus(path).getLen());
    assertTrue("Parent exists", exists(fSys, path.getParent()));
  }

  @Test
  public void testDeleteNonExistentFile() throws IOException {
    Path path = getTestRootPath(fSys, "test/hadoop/file");    
    assertFalse("Doesn't exist", exists(fSys, path));
    assertFalse("No deletion", fSys.delete(path, true));
  }
  
  @Test
  public void testDeleteRecursively() throws Exception {
    Path dir = getTestRootPath(fSys, "test/hadoop");
    Path file = getTestRootPath(fSys, "test/hadoop/file");
    Path subdir = getTestRootPath(fSys, "test/hadoop/subdir");
    
    createFile(file);
    fSys.mkdirs(subdir);
    
    assertTrue("File exists", exists(fSys, file));
    assertTrue("Dir exists", exists(fSys, dir));
    assertTrue("Subdir exists", exists(fSys, subdir));
    intercept(IOException.class, () ->
      fSys.delete(dir, false));
    assertTrue("File still exists", exists(fSys, file));
    assertTrue("Dir still exists", exists(fSys, dir));
    assertTrue("Subdir still exists", exists(fSys, subdir));
    
    assertTrue("Deleted", fSys.delete(dir, true));
    assertFalse("File doesn't exist", exists(fSys, file));
    assertFalse("Dir doesn't exist", exists(fSys, dir));
    assertFalse("Subdir doesn't exist", exists(fSys, subdir));
  }
  
  @Test
  public void testDeleteEmptyDirectory() throws IOException {
    Path dir = getTestRootPath(fSys, "test/hadoop");
    fSys.mkdirs(dir);
    assertTrue("Dir exists", exists(fSys, dir));
    assertTrue("Deleted", fSys.delete(dir, false));
    assertFalse("Dir doesn't exist", exists(fSys, dir));
  }
  
  @Test
  public void testRenameNonExistentPath() throws Exception {
    assumeRenameSupported();
    Path src = getTestRootPath(fSys, "test/hadoop/nonExistent");
    Path dst = getTestRootPath(fSys, "test/new/newpath");
    intercept(FileNotFoundException.class, () ->
      rename(src, dst, false, false, false, Rename.NONE));
    intercept(FileNotFoundException.class, () ->
      rename(src, dst, false, false, false, Rename.OVERWRITE));
  }

  @Test
  public void testRenameFileToNonExistentDirectory() throws Exception {
    assumeRenameSupported();
    
    Path src = getTestRootPath(fSys, "test/hadoop/file");
    createFile(src);
    Path dst = getTestRootPath(fSys, "test/nonExistent/newfile");
    intercept(IOException.class, () ->
      rename(src, dst, false, true, false, Rename.NONE));
    intercept(IOException.class, () ->
      rename(src, dst, false, true, false, Rename.OVERWRITE));
  }

  @Test
  public void testRenameFileToDestinationWithParentFile() throws Exception {
    assumeRenameSupported();
    
    Path src = getTestRootPath(fSys, "test/hadoop/file");
    createFile(src);
    Path dst = getTestRootPath(fSys, "test/parentFile/newfile");
    createFile(dst.getParent());
    intercept(IOException.class, () ->
      rename(src, dst, false, true, false, Rename.NONE));
    intercept(IOException.class, () ->
      rename(src, dst, false, true, false, Rename.OVERWRITE));
  }

  @Test
  public void testRenameFileToExistingParent() throws Exception {
    assumeRenameSupported();

    Path src = getTestRootPath(fSys, "test/hadoop/file");
    createFile(src);
    Path dst = getTestRootPath(fSys, "test/new/newfile");
    fSys.mkdirs(dst.getParent());
    rename(src, dst, true, false, true, Rename.OVERWRITE);
  }

  @Test
  public void testRenameFileToItself() throws Exception {
    assumeRenameSupported();

    Path src = getTestRootPath(fSys, "test/hadoop/file");
    createFile(src);
    intercept(FileAlreadyExistsException.class, () ->
      rename(src, src, false, true, false, Rename.NONE));
    // Also fails with overwrite
    intercept(IOException.class, () ->
        rename(src, src, false, true, false, Rename.OVERWRITE));
  }

  @Test
  public void testRenameFileAsExistingFile() throws Exception {
    assumeRenameSupported();

    Path src = getTestRootPath(fSys, "test/hadoop/file");
    createFile(src);
    Path dst = getTestRootPath(fSys, "test/new/existingFile");
    createFile(dst);
    
    // Fails without overwrite option
    intercept(FileAlreadyExistsException.class, () ->
      rename(src, dst, false, true, false, Rename.NONE));

    // Succeeds with overwrite option
    rename(src, dst, true, false, true, Rename.OVERWRITE);
  }

  @Test
  public void testRenameFileAsExistingDirectory() throws Exception {
    assumeRenameSupported();

    Path src = getTestRootPath(fSys, "test/hadoop/file");
    createFile(src);
    Path dst = getTestRootPath(fSys, "test/new/existingDir");
    fSys.mkdirs(dst);
    
    // Fails without overwrite option
    intercept(IOException.class, () ->
        rename(src, dst, false, false, true, Rename.NONE));


    // File cannot be renamed as directory
    intercept(IOException.class, () ->
        rename(src, dst, false, false, true, Rename.OVERWRITE));
  }

  @Test
  public void testRenameDirectoryToItself() throws Exception {
    assumeRenameSupported();
    Path src = getTestRootPath(fSys, "test/hadoop/dir");
    fSys.mkdirs(src);
    intercept(FileAlreadyExistsException.class, () ->
      rename(src, src, false, true, false, Rename.NONE));
    // Also fails with overwrite
    intercept(IOException.class, () ->
        rename(src, src, false, true, false, Rename.OVERWRITE));
  }

  @Test
  public void testRenameDirectoryToNonExistentParent() throws Exception {
    assumeRenameSupported();

    Path src = getTestRootPath(fSys, "test/hadoop/dir");
    fSys.mkdirs(src);
    Path dst = getTestRootPath(fSys, "test/nonExistent/newdir");
    intercept(FileNotFoundException.class, () ->
        rename(src, dst, false, true, false, Rename.NONE));

    intercept(FileNotFoundException.class, () ->
        rename(src, dst, false, true, false, Rename.OVERWRITE));
  }

  @Test
  public void testRenameDirectoryAsNonExistentDirectory() throws Exception {
    doTestRenameDirectoryAsNonExistentDirectory(Rename.NONE);
    tearDown();
    doTestRenameDirectoryAsNonExistentDirectory(Rename.OVERWRITE);
  }

  private void doTestRenameDirectoryAsNonExistentDirectory(Rename... options) 
  throws Exception {
    assumeRenameSupported();
    
    Path src = getTestRootPath(fSys, "test/hadoop/dir");
    fSys.mkdirs(src);
    createFile(getTestRootPath(fSys, "test/hadoop/dir/file1"));
    createFile(getTestRootPath(fSys, "test/hadoop/dir/subdir/file2"));
    
    Path dst = getTestRootPath(fSys, "test/new/newdir");
    fSys.mkdirs(dst.getParent());
    
    rename(src, dst, true, false, true, options);
    assertFalse("Nested file1 exists",
        exists(fSys, getTestRootPath(fSys, "test/hadoop/dir/file1")));
    assertFalse("Nested file2 exists",
        exists(fSys, getTestRootPath(fSys, "test/hadoop/dir/subdir/file2")));
    assertTrue("Renamed nested file1 exists",
        exists(fSys, getTestRootPath(fSys, "test/new/newdir/file1")));
    assertTrue("Renamed nested exists",
        exists(fSys, getTestRootPath(fSys, "test/new/newdir/subdir/file2")));
  }

  @Test
  public void testRenameDirectoryAsEmptyDirectory() throws Exception {
    assumeRenameSupported();
    
    Path src = getTestRootPath(fSys, "test/hadoop/dir");
    fSys.mkdirs(src);
    createFile(getTestRootPath(fSys, "test/hadoop/dir/file1"));
    createFile(getTestRootPath(fSys, "test/hadoop/dir/subdir/file2"));
    
    Path dst = getTestRootPath(fSys, "test/new/newdir");
    fSys.mkdirs(dst);

    // Fails without overwrite option
    intercept(IOException.class, () ->
      rename(src, dst, false, true, false, Rename.NONE));
    // Succeeds with the overwrite option
    rename(src, dst, true, false, true, Rename.OVERWRITE);
  }

  @Test
  public void testRenameDirectoryAsNonEmptyDirectory() throws Exception {
    assumeRenameSupported();

    Path src = getTestRootPath(fSys, "test/hadoop/dir");
    fSys.mkdirs(src);
    createFile(getTestRootPath(fSys, "test/hadoop/dir/file1"));
    createFile(getTestRootPath(fSys, "test/hadoop/dir/subdir/file2"));

    Path dst = getTestRootPath(fSys, "test/new/newdir");
    fSys.mkdirs(dst);
    createFile(getTestRootPath(fSys, "test/new/newdir/file1"));
    // Fails without overwrite option
    intercept(IOException.class, () ->
      rename(src, dst, false, true, false, Rename.NONE));
    // Fails even with the overwrite option
    intercept(IOException.class, () ->
      rename(src, dst, false, true, false, Rename.OVERWRITE));
  }

  @Test
  public void testRenameDirectoryAsFile() throws Exception {
    assumeRenameSupported();
    
    Path src = getTestRootPath(fSys, "test/hadoop/dir");
    fSys.mkdirs(src);
    Path dst = getTestRootPath(fSys, "test/new/newfile");
    createFile(dst);
    // Fails without overwrite option
    intercept(IOException.class, () ->
      rename(src, dst, false, true, true, Rename.NONE));
    // Directory cannot be renamed as existing file
    intercept(IOException.class, () ->
        rename(src, dst, false, true, true, Rename.OVERWRITE));
  }

  @Test
  public void testInputStreamClosedTwice() throws IOException {
    //HADOOP-4760 according to Closeable#close() closing already-closed 
    //streams should have no effect. 
    Path src = getTestRootPath(fSys, "test/hadoop/file");
    createFile(src);
    FSDataInputStream in = fSys.open(src);
    in.close();
    in.close();
  }
  
  @Test
  public void testOutputStreamClosedTwice() throws IOException {
    //HADOOP-4760 according to Closeable#close() closing already-closed 
    //streams should have no effect. 
    Path src = getTestRootPath(fSys, "test/hadoop/file");
    FSDataOutputStream out = fSys.create(src);
    
    out.writeChar('H'); //write some data
    out.close();
    out.close();
  }

  
  @Test
  public void testGetWrappedInputStream() throws IOException {
    Path src = getTestRootPath(fSys, "test/hadoop/file");
    createFile(src);
    FSDataInputStream in = fSys.open(src);
    InputStream is = in.getWrappedStream();
    in.close();
    assertNotNull(is);
  }
  
  @Test
  public void testCopyToLocalWithUseRawLocalFileSystemOption() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fSys = new RawLocalFileSystem();
    Path fileToFS = new Path(getTestRootDir(), "fs.txt");
    Path fileToLFS = new Path(getTestRootDir(), "test.txt");
    Path crcFileAtLFS = new Path(getTestRootDir(), ".test.txt.crc");
    fSys.initialize(new URI("file:///"), conf);
    writeFile(fSys, fileToFS);
    if (fSys.exists(crcFileAtLFS))
      assertTrue("CRC files not deleted", fSys
          .delete(crcFileAtLFS, true));
    fSys.copyToLocalFile(false, fileToFS, fileToLFS, true);
    assertFalse("CRC files are created", fSys.exists(crcFileAtLFS));
  }

  private void writeFile(FileSystem fs, Path name) throws IOException {
    try (FSDataOutputStream stm = fs.create(name)) {
      stm.writeBytes("42\n");
    }
  }
  
  protected void createFile(Path path) throws IOException {
    createFile(fSys, path);
  }

  @SuppressWarnings("deprecation")
  private void rename(Path src, Path dst, boolean renameShouldSucceed,
      boolean srcExists, boolean dstExists, Rename... options)
      throws IOException {
    fSys.rename(src, dst, options);
    if (!renameShouldSucceed) {
      fail("rename should have thrown exception");
    }
    assertEquals("Source exists", srcExists, exists(fSys, src));
    assertEquals("Destination exists", dstExists, exists(fSys, dst));
  }

  private boolean containsTestRootPath(Path path, FileStatus[] filteredPaths)
    throws IOException {
      Path testRootPath = getTestRootPath(fSys, path.toString());
    for(int i = 0; i < filteredPaths.length; i ++) { 
      if (testRootPath.equals(
          filteredPaths[i].getPath()))
        return true;
      }
    return false;
 }

}
