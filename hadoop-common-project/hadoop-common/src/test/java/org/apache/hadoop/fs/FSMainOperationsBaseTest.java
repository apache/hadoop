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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
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
    Assertions.assertNotNull(fsStatus);
    //used, free and capacity are non-negative longs
    Assertions.assertTrue(fsStatus.getUsed() >= 0);
    Assertions.assertTrue(fsStatus.getRemaining() >= 0);
    Assertions.assertTrue(fsStatus.getCapacity() >= 0);
  }
  
  @Test
  public void testWorkingDirectory() throws Exception {

    // First we cd to our test root
    Path workDir = new Path(getAbsoluteTestRootPath(fSys), new Path("test"));
    fSys.setWorkingDirectory(workDir);
    Assertions.assertEquals(workDir, fSys.getWorkingDirectory());

    fSys.setWorkingDirectory(new Path("."));
    Assertions.assertEquals(workDir, fSys.getWorkingDirectory());

    fSys.setWorkingDirectory(new Path(".."));
    Assertions.assertEquals(workDir.getParent(), fSys.getWorkingDirectory());
    
    // cd using a relative path

    // Go back to our test root
    workDir = new Path(getAbsoluteTestRootPath(fSys), new Path("test"));
    fSys.setWorkingDirectory(workDir);
    Assertions.assertEquals(workDir, fSys.getWorkingDirectory());
    
    Path relativeDir = new Path("existingDir1");
    Path absoluteDir = new Path(workDir,"existingDir1");
    fSys.mkdirs(absoluteDir);
    fSys.setWorkingDirectory(relativeDir);
    Assertions.assertEquals(absoluteDir, fSys.getWorkingDirectory());
    // cd using a absolute path
    absoluteDir = getTestRootPath(fSys, "test/existingDir2");
    fSys.mkdirs(absoluteDir);
    fSys.setWorkingDirectory(absoluteDir);
    Assertions.assertEquals(absoluteDir, fSys.getWorkingDirectory());
    
    // Now open a file relative to the wd we just set above.
    Path absolutePath = new Path(absoluteDir, "foo");
    createFile(fSys, absolutePath);
    fSys.open(new Path("foo")).close();
    
    
    // Now mkdir relative to the dir we cd'ed to
    fSys.mkdirs(new Path("newDir"));
    Assertions.assertTrue(isDir(fSys, new Path(absoluteDir, "newDir")));

    /**
     * We cannot test this because FileSystem has never checked for
     * existence of working dir - fixing  this would break compatibility,
     * 
    absoluteDir = getTestRootPath(fSys, "nonexistingPath");
    try {
      fSys.setWorkingDirectory(absoluteDir);
      Assertions.fail("cd to non existing dir should have failed");
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
    Assertions.assertEquals(absoluteDir, fSys.getWorkingDirectory());
  }
  
  @Test
  public void testMkdirs() throws Exception {
    Path testDir = getTestRootPath(fSys, "test/hadoop");
    Assertions.assertFalse(exists(fSys, testDir));
    Assertions.assertFalse(isFile(fSys, testDir));

    fSys.mkdirs(testDir);

    Assertions.assertTrue(exists(fSys, testDir));
    Assertions.assertFalse(isFile(fSys, testDir));
    
    fSys.mkdirs(testDir);

    Assertions.assertTrue(exists(fSys, testDir));
    Assertions.assertFalse(isFile(fSys, testDir));

    Path parentDir = testDir.getParent();
    Assertions.assertTrue(exists(fSys, parentDir));
    Assertions.assertFalse(isFile(fSys, parentDir));

    Path grandparentDir = parentDir.getParent();
    Assertions.assertTrue(exists(fSys, grandparentDir));
    Assertions.assertFalse(isFile(fSys, grandparentDir));
    
  }
  
  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = getTestRootPath(fSys, "test/hadoop");
    Assertions.assertFalse(exists(fSys, testDir));
    fSys.mkdirs(testDir);
    Assertions.assertTrue(exists(fSys, testDir));
    
    createFile(getTestRootPath(fSys, "test/hadoop/file"));
    
    Path testSubDir = getTestRootPath(fSys, "test/hadoop/file/subdir");
    try {
      fSys.mkdirs(testSubDir);
      Assertions.fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    Assertions.assertFalse(exists(fSys, testSubDir));
    
    Path testDeepSubDir = getTestRootPath(fSys, "test/hadoop/file/deep/sub/dir");
    try {
      fSys.mkdirs(testDeepSubDir);
      Assertions.fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    Assertions.assertFalse(exists(fSys, testDeepSubDir));
    
  }
  
  @Test
  public void testGetFileStatusThrowsExceptionForNonExistentFile() 
    throws Exception {
    try {
      fSys.getFileStatus(getTestRootPath(fSys, "test/hadoop/file"));
      Assertions.fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      // expected
    }
  } 
  
  @Test
  public void testListStatusThrowsExceptionForNonExistentFile()
  throws Exception {
    try {
      fSys.listStatus(getTestRootPath(fSys, "test/hadoop/file"));
      Assertions.fail("Should throw FileNotFoundException");
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
      Assertions.fail("Should throw IOException");
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
    Assertions.assertFalse(exists(fSys, testDirs[0]));

    for (Path path : testDirs) {
      fSys.mkdirs(path);
    }

    // test listStatus that returns an array
    FileStatus[] paths = fSys.listStatus(getTestRootPath(fSys, "test"));
    Assertions.assertEquals(1, paths.length);
    Assertions.assertEquals(getTestRootPath(fSys, "test/hadoop"), paths[0].getPath());

    paths = fSys.listStatus(getTestRootPath(fSys, "test/hadoop"));
    Assertions.assertEquals(3, paths.length);

    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys, "test/hadoop/a"),
        paths));
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys, "test/hadoop/b"),
        paths));
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys, "test/hadoop/c"),
        paths));

    paths = fSys.listStatus(getTestRootPath(fSys, "test/hadoop/a"));
    Assertions.assertEquals(0, paths.length);
    
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
    Assertions.assertEquals(0,filteredPaths.length);
    
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
    Assertions.assertEquals(2,filteredPaths.length);
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys,
        TEST_DIR_AXA), filteredPaths));
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys,
        TEST_DIR_AXX), filteredPaths));
  }
  
  @Test
  public void testGlobStatusNonExistentFile() throws Exception {
    FileStatus[] paths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoopfsdf"));
    Assertions.assertNull(paths);

    paths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoopfsdf/?"));
    Assertions.assertEquals(0, paths.length);
    paths = fSys.globStatus(
        getTestRootPath(fSys, "test/hadoopfsdf/xyz*/?"));
    Assertions.assertEquals(0, paths.length);
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
    Assertions.assertEquals(0, paths.length);
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
    Assertions.assertEquals(2, paths.length);
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys,
        "test/hadoop"), paths));
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys,
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
    Assertions.assertEquals(4, paths.length);
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AAA), paths));
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXA), paths));
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXX), paths));
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AAA2), paths));
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
    Assertions.assertEquals(2, paths.length);
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys,
        TEST_DIR_AXA), paths));
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys,
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
    Assertions.assertEquals(0,filteredPaths.length);
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
    Assertions.assertEquals(3, filteredPaths.length);
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys,
        TEST_DIR_AAA), filteredPaths));
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys,
        TEST_DIR_AXA), filteredPaths));
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys,
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
    Assertions.assertEquals(3, filteredPaths.length);
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AAA),
        filteredPaths));
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXA),
        filteredPaths));
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXX),
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
    Assertions.assertEquals(2, filteredPaths.length);
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys,
        TEST_DIR_AXA), filteredPaths));
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys,
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
    Assertions.assertEquals(0,filteredPaths.length);
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
    Assertions.assertEquals(2, filteredPaths.length);
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXA),
        filteredPaths));
    Assertions.assertTrue(containsTestRootPath(getTestRootPath(fSys, TEST_DIR_AXX),
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
      Assertions.fail("Should throw IOException");
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

    Assertions.assertTrue(exists(fSys, path), "Exists");
    Assertions.assertEquals(len, fSys.getFileStatus(path).getLen(), "Length");

    FSDataInputStream in = fSys.open(path);
    byte[] buf = new byte[len];
    in.readFully(0, buf);
    in.close();

    Assertions.assertEquals(len, buf.length);
    for (int i = 0; i < buf.length; i++) {
      Assertions.assertEquals(data[i], buf[i], "Position " + i);
    }
    
    Assertions.assertTrue(fSys.delete(path, false), "Deleted");
    
    Assertions.assertFalse(exists(fSys, path), "No longer exists");

  }
  
  @Test
  public void testOverwrite() throws IOException {
    Path path = getTestRootPath(fSys, "test/hadoop/file");
    
    fSys.mkdirs(path.getParent());

    createFile(path);
    
    Assertions.assertTrue(exists(fSys, path), "Exists");
    Assertions.assertEquals(data.length, fSys.getFileStatus(path).getLen(), "Length");
    
    try {
      createFile(path);
      Assertions.fail("Should throw IOException.");
    } catch (IOException e) {
      // Expected
    }
    
    FSDataOutputStream out = fSys.create(path, true, 4096);
    out.write(data, 0, data.length);
    out.close();
    
    Assertions.assertTrue(exists(fSys, path), "Exists");
    Assertions.assertEquals(data.length, fSys.getFileStatus(path).getLen(), "Length");
    
  }
  
  @Test
  public void testWriteInNonExistentDirectory() throws IOException {
    Path path = getTestRootPath(fSys, "test/hadoop/file");
    Assertions.assertFalse(exists(fSys, path.getParent()), "Parent doesn't exist");
    createFile(path);
    
    Assertions.assertTrue(exists(fSys, path), "Exists");
    Assertions.assertEquals(data.length, fSys.getFileStatus(path).getLen(), "Length");
    Assertions.assertTrue(exists(fSys, path.getParent()), "Parent exists");
  }

  @Test
  public void testDeleteNonExistentFile() throws IOException {
    Path path = getTestRootPath(fSys, "test/hadoop/file");    
    Assertions.assertFalse(exists(fSys, path), "Doesn't exist");
    Assertions.assertFalse(fSys.delete(path, true), "No deletion");
  }
  
  @Test
  public void testDeleteRecursively() throws IOException {
    Path dir = getTestRootPath(fSys, "test/hadoop");
    Path file = getTestRootPath(fSys, "test/hadoop/file");
    Path subdir = getTestRootPath(fSys, "test/hadoop/subdir");
    
    createFile(file);
    fSys.mkdirs(subdir);
    
    Assertions.assertTrue(exists(fSys, file), "File exists");
    Assertions.assertTrue(exists(fSys, dir), "Dir exists");
    Assertions.assertTrue(exists(fSys, subdir), "Subdir exists");
    
    try {
      fSys.delete(dir, false);
      Assertions.fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    Assertions.assertTrue(exists(fSys, file), "File still exists");
    Assertions.assertTrue(exists(fSys, dir), "Dir still exists");
    Assertions.assertTrue(exists(fSys, subdir), "Subdir still exists");
    
    Assertions.assertTrue(fSys.delete(dir, true), "Deleted");
    Assertions.assertFalse(exists(fSys, file), "File doesn't exist");
    Assertions.assertFalse(exists(fSys, dir), "Dir doesn't exist");
    Assertions.assertFalse(exists(fSys, subdir), "Subdir doesn't exist");
  }
  
  @Test
  public void testDeleteEmptyDirectory() throws IOException {
    Path dir = getTestRootPath(fSys, "test/hadoop");
    fSys.mkdirs(dir);
    Assertions.assertTrue(exists(fSys, dir), "Dir exists");
    Assertions.assertTrue(fSys.delete(dir, false), "Deleted");
    Assertions.assertFalse(exists(fSys, dir), "Dir doesn't exist");
  }
  
  @Test
  public void testRenameNonExistentPath() throws Exception {
    if (!renameSupported()) return;
    Path src = getTestRootPath(fSys, "test/hadoop/nonExistent");
    Path dst = getTestRootPath(fSys, "test/new/newpath");
    try {
      rename(src, dst, false, false, false, Rename.NONE);
      Assertions.fail("Should throw FileNotFoundException");
    } catch (IOException e) {
      Log.getLog().info("XXX", e);
      Assertions.assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }

    try {
      rename(src, dst, false, false, false, Rename.OVERWRITE);
      Assertions.fail("Should throw FileNotFoundException");
    } catch (IOException e) {
      Assertions.assertTrue(unwrapException(e) instanceof FileNotFoundException);
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
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
      Assertions.assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }

    try {
      rename(src, dst, false, true, false, Rename.OVERWRITE);
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
      Assertions.assertTrue(unwrapException(e) instanceof FileNotFoundException);
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
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
    }

    try {
      rename(src, dst, false, true, false, Rename.OVERWRITE);
      Assertions.fail("Expected exception was not thrown");
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
      Assertions.fail("Renamed file to itself");
    } catch (IOException e) {
      Assertions.assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Also fails with overwrite
    try {
      rename(src, src, false, true, false, Rename.OVERWRITE);
      Assertions.fail("Renamed file to itself");
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
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
      Assertions.assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
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
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
    }
    
    // File cannot be renamed as directory
    try {
      rename(src, dst, false, false, true, Rename.OVERWRITE);
      Assertions.fail("Expected exception was not thrown");
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
      Assertions.fail("Renamed directory to itself");
    } catch (IOException e) {
      Assertions.assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Also fails with overwrite
    try {
      rename(src, src, false, true, false, Rename.OVERWRITE);
      Assertions.fail("Renamed directory to itself");
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
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
      IOException ioException = unwrapException(e);
      if (!(ioException instanceof FileNotFoundException)) {
        throw ioException;
      }
    }

    try {
      rename(src, dst, false, true, false, Rename.OVERWRITE);
      Assertions.fail("Expected exception was not thrown");
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
    Assertions.assertFalse(
        exists(fSys, getTestRootPath(fSys, "test/hadoop/dir/file1")), "Nested file1 exists");
    Assertions.assertFalse(
        exists(fSys, getTestRootPath(fSys, "test/hadoop/dir/subdir/file2")), "Nested file2 exists");
    Assertions.assertTrue(
       exists(fSys, getTestRootPath(fSys, "test/new/newdir/file1")), "Renamed nested file1 exists");
    Assertions.assertTrue(
        exists(fSys, getTestRootPath(fSys, "test/new/newdir/subdir/file2")), "Renamed nested exists");
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
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
      // Expected (cannot over-write non-empty destination)
      Assertions.assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
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
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
      // Expected (cannot over-write non-empty destination)
      Assertions.assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Fails even with the overwrite option
    try {
      rename(src, dst, false, true, false, Rename.OVERWRITE);
      Assertions.fail("Expected exception was not thrown");
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
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
    }
    // Directory cannot be renamed as existing file
    try {
      rename(src, dst, false, true, true, Rename.OVERWRITE);
      Assertions.fail("Expected exception was not thrown");
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
    Assertions.assertNotNull(is);  
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
      Assertions.assertTrue(fSys
          .delete(crcFileAtLFS, true), "CRC files not deleted");
    fSys.copyToLocalFile(false, fileToFS, fileToLFS, true);
    Assertions.assertFalse(fSys.exists(crcFileAtLFS), "CRC files are created");
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
      Assertions.fail("rename should have thrown exception");
    Assertions.assertEquals(srcExists, exists(fSys, src), "Source exists");
    Assertions.assertEquals(dstExists, exists(fSys, dst), "Destination exists");
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
