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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.eclipse.jetty.util.log.Log;

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
 *     @BeforeClass   public static void clusterSetupAtBegining()
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
  
  final private static PathFilter DEFAULT_FILTER = new PathFilter() {
    @Override
    public boolean accept(final Path file) {
      return true;
    }
  };

  //A test filter with returns any path containing an "x" or "X"
  final private static PathFilter TEST_X_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path file) {
      if(file.getName().contains("x") || file.getName().contains("X"))
        return true;
      else
        return false;
    }     
  };
  
  protected static final byte[] data = getFileData(numBlocks,
      getDefaultBlockSize());
  
  abstract protected FileSystem createFileSystem() throws Exception;

  public FSMainOperationsBaseTest() {
  }
  
  public FSMainOperationsBaseTest(String testRootDir) {
      super(testRootDir);
  }
  
  @BeforeEach
  public void setUp() throws Exception {
    fSys = createFileSystem();
    fSys.mkdirs(getTestRootPath(fSys, "test"));
  }
  
  @AfterEach
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

  protected boolean renameSupported() {
    return true;
  }

  
  protected IOException unwrapException(IOException e) {
    return e;
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
      fail("cd to non existing dir should have failed");
    } catch (Exception e) {
      // Exception as expected
    }
    */
  }
  
  
  // Try a URI
  
  @Test
  public void testWDAbsolute() throws IOException {
    Path absoluteDir = getTestRootPath(fSys, "test/existingDir");
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
    try {
      fSys.mkdirs(testSubDir);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    assertFalse(exists(fSys, testSubDir));
    
    Path testDeepSubDir = getTestRootPath(fSys, "test/hadoop/file/deep/sub/dir");
    try {
      fSys.mkdirs(testDeepSubDir);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    assertFalse(exists(fSys, testDeepSubDir));
    
  }
  
  @Test
  public void testGetFileStatusThrowsExceptionForNonExistentFile() 
    throws Exception {
    try {
      fSys.getFileStatus(getTestRootPath(fSys, "test/hadoop/file"));
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      // expected
    }
  } 
  
  @Test
  public void testListStatusThrowsExceptionForNonExistentFile()
  throws Exception {
    try {
      fSys.listStatus(getTestRootPath(fSys, "test/hadoop/file"));
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // expected
    }
  }

  @Test
  public void testListStatusThrowsExceptionForUnreadableDir()
  throws Exception {
    Path testRootDir = getTestRootPath(fSys, "test/hadoop/dir");
    Path obscuredDir = new Path(testRootDir, "foo");
    Path subDir = new Path(obscuredDir, "bar"); //so foo is non-empty
    fSys.mkdirs(subDir);
    fSys.setPermission(obscuredDir, new FsPermission((short)0)); //no access
    try {
      fSys.listStatus(obscuredDir);
      fail("Should throw IOException");
    } catch (IOException ioe) {
      // expected
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
    assertEquals(0, filteredPaths.length);
    
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
    Path testRootDir = getTestRootPath(fSys, "test/hadoop/dir");
    Path obscuredDir = new Path(testRootDir, "foo");
    Path subDir = new Path(obscuredDir, "bar"); //so foo is non-empty
    fSys.mkdirs(subDir);
    fSys.setPermission(obscuredDir, new FsPermission((short)0)); //no access
    try {
      fSys.globStatus(getTestRootPath(fSys, "test/hadoop/dir/foo/*"));
      fail("Should throw IOException");
    } catch (IOException ioe) {
      // expected
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

   
    FSDataOutputStream out = 
      fSys.create(path, false, 4096, (short) 1, getDefaultBlockSize() );
    out.write(data, 0, len);
    out.close();

    assertTrue(exists(fSys, path), "Exists");
    assertEquals(len, fSys.getFileStatus(path).getLen(), "Length");

    FSDataInputStream in = fSys.open(path);
    byte[] buf = new byte[len];
    in.readFully(0, buf);
    in.close();

    assertEquals(len, buf.length);
    for (int i = 0; i < buf.length; i++) {
      assertEquals(data[i], buf[i], "Position " + i);
    }
    
    assertTrue(fSys.delete(path, false), "Deleted");
    
    assertFalse(exists(fSys, path), "No longer exists");

  }
  
  @Test
  public void testOverwrite() throws IOException {
    Path path = getTestRootPath(fSys, "test/hadoop/file");
    
    fSys.mkdirs(path.getParent());

    createFile(path);
    
    assertTrue(exists(fSys, path), "Exists");
    assertEquals(data.length, fSys.getFileStatus(path).getLen(), "Length");
    
    try {
      createFile(path);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // Expected
    }
    
    FSDataOutputStream out = fSys.create(path, true, 4096);
    out.write(data, 0, data.length);
    out.close();
    
    assertTrue(exists(fSys, path), "Exists");
    assertEquals(data.length, fSys.getFileStatus(path).getLen(), "Length");
    
  }
  
  @Test
  public void testWriteInNonExistentDirectory() throws IOException {
    Path path = getTestRootPath(fSys, "test/hadoop/file");
    assertFalse(exists(fSys, path.getParent()), "Parent doesn't exist");
    createFile(path);
    
    assertTrue(exists(fSys, path), "Exists");
    assertEquals(data.length, fSys.getFileStatus(path).getLen(), "Length");
    assertTrue(exists(fSys, path.getParent()), "Parent exists");
  }

  @Test
  public void testDeleteNonExistentFile() throws IOException {
    Path path = getTestRootPath(fSys, "test/hadoop/file");    
    assertFalse(exists(fSys, path), "Doesn't exist");
    assertFalse(fSys.delete(path, true), "No deletion");
  }
  
  @Test
  public void testDeleteRecursively() throws IOException {
    Path dir = getTestRootPath(fSys, "test/hadoop");
    Path file = getTestRootPath(fSys, "test/hadoop/file");
    Path subdir = getTestRootPath(fSys, "test/hadoop/subdir");
    
    createFile(file);
    fSys.mkdirs(subdir);
    
    assertTrue(exists(fSys, file), "File exists");
    assertTrue(exists(fSys, dir), "Dir exists");
    assertTrue(exists(fSys, subdir), "Subdir exists");
    
    try {
      fSys.delete(dir, false);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    assertTrue(exists(fSys, file), "File still exists");
    assertTrue(exists(fSys, dir), "Dir still exists");
    assertTrue(exists(fSys, subdir), "Subdir still exists");
    
    assertTrue(fSys.delete(dir, true), "Deleted");
    assertFalse(exists(fSys, file), "File doesn't exist");
    assertFalse(exists(fSys, dir), "Dir doesn't exist");
    assertFalse(exists(fSys, subdir), "Subdir doesn't exist");
  }
  
  @Test
  public void testDeleteEmptyDirectory() throws IOException {
    Path dir = getTestRootPath(fSys, "test/hadoop");
    fSys.mkdirs(dir);
    assertTrue(exists(fSys, dir), "Dir exists");
    assertTrue(fSys.delete(dir, false), "Deleted");
    assertFalse(exists(fSys, dir), "Dir doesn't exist");
  }
  
  @Test
  public void testRenameNonExistentPath() throws Exception {
    if (!renameSupported()) return;
    Path src = getTestRootPath(fSys, "test/hadoop/nonExistent");
    Path dst = getTestRootPath(fSys, "test/new/newpath");
    try {
      rename(src, dst, false, false, false, Rename.NONE);
      fail("Should throw FileNotFoundException");
    } catch (IOException e) {
      Log.getLog().info("XXX", e);
      assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }

    try {
      rename(src, dst, false, false, false, Rename.OVERWRITE);
      fail("Should throw FileNotFoundException");
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }
  }

  @Test
  public void testRenameFileToNonExistentDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fSys, "test/hadoop/file");
    createFile(src);
    Path dst = getTestRootPath(fSys, "test/nonExistent/newfile");
    
    try {
      rename(src, dst, false, true, false, Rename.NONE);
      fail("Expected exception was not thrown");
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }

    try {
      rename(src, dst, false, true, false, Rename.OVERWRITE);
      fail("Expected exception was not thrown");
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }
  }

  @Test
  public void testRenameFileToDestinationWithParentFile() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fSys, "test/hadoop/file");
    createFile(src);
    Path dst = getTestRootPath(fSys, "test/parentFile/newfile");
    createFile(dst.getParent());
    
    try {
      rename(src, dst, false, true, false, Rename.NONE);
      fail("Expected exception was not thrown");
    } catch (IOException e) {
    }

    try {
      rename(src, dst, false, true, false, Rename.OVERWRITE);
      fail("Expected exception was not thrown");
    } catch (IOException e) {
    }
  }

  @Test
  public void testRenameFileToExistingParent() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fSys, "test/hadoop/file");
    createFile(src);
    Path dst = getTestRootPath(fSys, "test/new/newfile");
    fSys.mkdirs(dst.getParent());
    rename(src, dst, true, false, true, Rename.OVERWRITE);
  }

  @Test
  public void testRenameFileToItself() throws Exception {
    if (!renameSupported()) return;
    Path src = getTestRootPath(fSys, "test/hadoop/file");
    createFile(src);
    try {
      rename(src, src, false, true, false, Rename.NONE);
      fail("Renamed file to itself");
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Also fails with overwrite
    try {
      rename(src, src, false, true, false, Rename.OVERWRITE);
      fail("Renamed file to itself");
    } catch (IOException e) {
      // worked
    }
  }
  
  @Test
  public void testRenameFileAsExistingFile() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fSys, "test/hadoop/file");
    createFile(src);
    Path dst = getTestRootPath(fSys, "test/new/existingFile");
    createFile(dst);
    
    // Fails without overwrite option
    try {
      rename(src, dst, false, true, false, Rename.NONE);
      fail("Expected exception was not thrown");
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    
    // Succeeds with overwrite option
    rename(src, dst, true, false, true, Rename.OVERWRITE);
  }

  @Test
  public void testRenameFileAsExistingDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fSys, "test/hadoop/file");
    createFile(src);
    Path dst = getTestRootPath(fSys, "test/new/existingDir");
    fSys.mkdirs(dst);
    
    // Fails without overwrite option
    try {
      rename(src, dst, false, false, true, Rename.NONE);
      fail("Expected exception was not thrown");
    } catch (IOException e) {
    }
    
    // File cannot be renamed as directory
    try {
      rename(src, dst, false, false, true, Rename.OVERWRITE);
      fail("Expected exception was not thrown");
    } catch (IOException e) {
    }
  }

  @Test
  public void testRenameDirectoryToItself() throws Exception {
    if (!renameSupported()) return;
    Path src = getTestRootPath(fSys, "test/hadoop/dir");
    fSys.mkdirs(src);
    try {
      rename(src, src, false, true, false, Rename.NONE);
      fail("Renamed directory to itself");
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Also fails with overwrite
    try {
      rename(src, src, false, true, false, Rename.OVERWRITE);
      fail("Renamed directory to itself");
    } catch (IOException e) {
      // worked      
    }
  }

  @Test
  public void testRenameDirectoryToNonExistentParent() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fSys, "test/hadoop/dir");
    fSys.mkdirs(src);
    Path dst = getTestRootPath(fSys, "test/nonExistent/newdir");
    
    try {
      rename(src, dst, false, true, false, Rename.NONE);
      fail("Expected exception was not thrown");
    } catch (IOException e) {
      IOException ioException = unwrapException(e);
      if (!(ioException instanceof FileNotFoundException)) {
        throw ioException;
      }
    }

    try {
      rename(src, dst, false, true, false, Rename.OVERWRITE);
      fail("Expected exception was not thrown");
    } catch (IOException e) {
      IOException ioException = unwrapException(e);
      if (!(ioException instanceof FileNotFoundException)) {
        throw ioException;
      }
    }
  }

  @Test
  public void testRenameDirectoryAsNonExistentDirectory() throws Exception {
    doTestRenameDirectoryAsNonExistentDirectory(Rename.NONE);
    tearDown();
    doTestRenameDirectoryAsNonExistentDirectory(Rename.OVERWRITE);
  }

  private void doTestRenameDirectoryAsNonExistentDirectory(Rename... options) 
  throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fSys, "test/hadoop/dir");
    fSys.mkdirs(src);
    createFile(getTestRootPath(fSys, "test/hadoop/dir/file1"));
    createFile(getTestRootPath(fSys, "test/hadoop/dir/subdir/file2"));
    
    Path dst = getTestRootPath(fSys, "test/new/newdir");
    fSys.mkdirs(dst.getParent());
    
    rename(src, dst, true, false, true, options);
    assertFalse(exists(fSys, getTestRootPath(fSys,
        "test/hadoop/dir/file1")), "Nested file1 exists");
    assertFalse(exists(fSys, getTestRootPath(fSys, "test/hadoop/dir/subdir/file2")),
        "Nested file2 exists");
    assertTrue(exists(fSys, getTestRootPath(fSys, "test/new/newdir/file1")),
        "Renamed nested file1 exists");
    assertTrue(exists(fSys, getTestRootPath(fSys, "test/new/newdir/subdir/file2")),
        "Renamed nested exists");
  }

  @Test
  public void testRenameDirectoryAsEmptyDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fSys, "test/hadoop/dir");
    fSys.mkdirs(src);
    createFile(getTestRootPath(fSys, "test/hadoop/dir/file1"));
    createFile(getTestRootPath(fSys, "test/hadoop/dir/subdir/file2"));
    
    Path dst = getTestRootPath(fSys, "test/new/newdir");
    fSys.mkdirs(dst);

    // Fails without overwrite option
    try {
      rename(src, dst, false, true, false, Rename.NONE);
      fail("Expected exception was not thrown");
    } catch (IOException e) {
      // Expected (cannot over-write non-empty destination)
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Succeeds with the overwrite option
    rename(src, dst, true, false, true, Rename.OVERWRITE);
  }

  @Test
  public void testRenameDirectoryAsNonEmptyDirectory() throws Exception {
    if (!renameSupported()) return;

    Path src = getTestRootPath(fSys, "test/hadoop/dir");
    fSys.mkdirs(src);
    createFile(getTestRootPath(fSys, "test/hadoop/dir/file1"));
    createFile(getTestRootPath(fSys, "test/hadoop/dir/subdir/file2"));

    Path dst = getTestRootPath(fSys, "test/new/newdir");
    fSys.mkdirs(dst);
    createFile(getTestRootPath(fSys, "test/new/newdir/file1"));
    // Fails without overwrite option
    try {
      rename(src, dst, false, true, false, Rename.NONE);
      fail("Expected exception was not thrown");
    } catch (IOException e) {
      // Expected (cannot over-write non-empty destination)
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Fails even with the overwrite option
    try {
      rename(src, dst, false, true, false, Rename.OVERWRITE);
      fail("Expected exception was not thrown");
    } catch (IOException ex) {
      // Expected (cannot over-write non-empty destination)
    }
  }

  @Test
  public void testRenameDirectoryAsFile() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fSys, "test/hadoop/dir");
    fSys.mkdirs(src);
    Path dst = getTestRootPath(fSys, "test/new/newfile");
    createFile(dst);
    // Fails without overwrite option
    try {
      rename(src, dst, false, true, true, Rename.NONE);
      fail("Expected exception was not thrown");
    } catch (IOException e) {
    }
    // Directory cannot be renamed as existing file
    try {
      rename(src, dst, false, true, true, Rename.OVERWRITE);
      fail("Expected exception was not thrown");
    } catch (IOException ex) {
    }
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
      assertTrue(fSys.delete(crcFileAtLFS, true), "CRC files not deleted");
    fSys.copyToLocalFile(false, fileToFS, fileToLFS, true);
    assertFalse(fSys.exists(crcFileAtLFS), "CRC files are created");
  }

  private void writeFile(FileSystem fs, Path name) throws IOException {
    FSDataOutputStream stm = fs.create(name);
    try {
      stm.writeBytes("42\n");
    } finally {
      stm.close();
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
    if (!renameShouldSucceed)
      fail("rename should have thrown exception");
    assertEquals(srcExists, exists(fSys, src), "Source exists");
    assertEquals(dstExists, exists(fSys, dst), "Destination exists");
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
