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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.FileContextTestHelper.*;
import static org.apache.hadoop.fs.CreateFlag.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;

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

  protected static final Logger LOG =
      LoggerFactory.getLogger(FileContextMainOperationsBaseTest.class);
  
  private static String TEST_DIR_AAA2 = "test/hadoop2/aaa";
  private static String TEST_DIR_AAA = "test/hadoop/aaa";
  private static String TEST_DIR_AXA = "test/hadoop/axa";
  private static String TEST_DIR_AXX = "test/hadoop/axx";
  private static int numBlocks = 2;

  public Path localFsRootPath;

  protected final FileContextTestHelper fileContextTestHelper =
    createFileContextHelper();

  /**
   * Create the test helper.
   * Important: this is invoked during the construction of the base class,
   * so is very brittle.
   * @return a test helper.
   */
  protected FileContextTestHelper createFileContextHelper() {
    return new FileContextTestHelper();
  }

  protected static FileContext fc;
  
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
  
  private static final byte[] data = getFileData(numBlocks,
      getDefaultBlockSize());

  @BeforeEach
  public void setUp() throws Exception {
    File testBuildData = GenericTestUtils.getRandomizedTestDir();
    Path rootPath = new Path(testBuildData.getAbsolutePath(), 
            "root-uri");
    localFsRootPath = rootPath.makeQualified(LocalFileSystem.NAME, null);
    fc.mkdir(getTestRootPath(fc, "test"), FileContext.DEFAULT_PERM, true);
  }
  
  @AfterEach
  public void tearDown() throws Exception {
    if (fc != null) {
      final Path testRoot = fileContextTestHelper.getAbsoluteTestRootPath(fc);
      LOG.info("Deleting test root path {}", testRoot);
      try {
        fc.delete(testRoot, true);
      } catch (Exception e) {
        LOG.error("Error when deleting test root path " + testRoot, e);
      }

      try {
        fc.delete(localFsRootPath, true);
      } catch (Exception e) {
        LOG.error("Error when deleting localFsRootPath " + localFsRootPath, e);
      }
    }
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
    Assertions.assertNotNull(fsStatus);
    //used, free and capacity are non-negative longs
    Assertions.assertTrue(fsStatus.getUsed() >= 0);
    Assertions.assertTrue(fsStatus.getRemaining() >= 0);
    Assertions.assertTrue(fsStatus.getCapacity() >= 0);
  }
  
  @Test
  public void testWorkingDirectory() throws Exception {

    // First we cd to our test root
    Path workDir = new Path(fileContextTestHelper.getAbsoluteTestRootPath(fc), new Path("test"));
    fc.setWorkingDirectory(workDir);
    Assertions.assertEquals(workDir, fc.getWorkingDirectory());

    fc.setWorkingDirectory(new Path("."));
    Assertions.assertEquals(workDir, fc.getWorkingDirectory());

    fc.setWorkingDirectory(new Path(".."));
    Assertions.assertEquals(workDir.getParent(), fc.getWorkingDirectory());
    
    // cd using a relative path

    // Go back to our test root
    workDir = new Path(fileContextTestHelper.getAbsoluteTestRootPath(fc), new Path("test"));
    fc.setWorkingDirectory(workDir);
    Assertions.assertEquals(workDir, fc.getWorkingDirectory());
    
    Path relativeDir = new Path("existingDir1");
    Path absoluteDir = new Path(workDir,"existingDir1");
    fc.mkdir(absoluteDir, FileContext.DEFAULT_PERM, true);
    fc.setWorkingDirectory(relativeDir);
    Assertions.assertEquals(absoluteDir, fc.getWorkingDirectory());
    // cd using a absolute path
    absoluteDir = getTestRootPath(fc, "test/existingDir2");
    fc.mkdir(absoluteDir, FileContext.DEFAULT_PERM, true);
    fc.setWorkingDirectory(absoluteDir);
    Assertions.assertEquals(absoluteDir, fc.getWorkingDirectory());
    
    // Now open a file relative to the wd we just set above.
    Path absolutePath = new Path(absoluteDir, "foo");
    fc.create(absolutePath, EnumSet.of(CREATE)).close();
    fc.open(new Path("foo")).close();
    
    
    // Now mkdir relative to the dir we cd'ed to
    fc.mkdir(new Path("newDir"), FileContext.DEFAULT_PERM, true);
    Assertions.assertTrue(isDir(fc, new Path(absoluteDir, "newDir")));

    absoluteDir = getTestRootPath(fc, "nonexistingPath");
    try {
      fc.setWorkingDirectory(absoluteDir);
      Assertions.fail("cd to non existing dir should have failed");
    } catch (Exception e) {
      // Exception as expected
    }
    
    // Try a URI

    absoluteDir = new Path(localFsRootPath, "existingDir");
    fc.mkdir(absoluteDir, FileContext.DEFAULT_PERM, true);
    fc.setWorkingDirectory(absoluteDir);
    Assertions.assertEquals(absoluteDir, fc.getWorkingDirectory());

    Path aRegularFile = new Path("aRegularFile");
    createFile(aRegularFile);
    try {
      fc.setWorkingDirectory(aRegularFile);
      fail("An IOException expected.");
    } catch (IOException ioe) {
      // okay
    }
  }
  
  @Test
  public void testMkdirs() throws Exception {
    Path testDir = getTestRootPath(fc, "test/hadoop");
    Assertions.assertFalse(exists(fc, testDir));
    Assertions.assertFalse(isFile(fc, testDir));

    fc.mkdir(testDir, FsPermission.getDefault(), true);

    Assertions.assertTrue(exists(fc, testDir));
    Assertions.assertFalse(isFile(fc, testDir));
    
    fc.mkdir(testDir, FsPermission.getDefault(), true);

    Assertions.assertTrue(exists(fc, testDir));
    Assertions.assertFalse(isFile(fc, testDir));

    Path parentDir = testDir.getParent();
    Assertions.assertTrue(exists(fc, parentDir));
    Assertions.assertFalse(isFile(fc, parentDir));

    Path grandparentDir = parentDir.getParent();
    Assertions.assertTrue(exists(fc, grandparentDir));
    Assertions.assertFalse(isFile(fc, grandparentDir));
    
  }
  
  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = getTestRootPath(fc, "test/hadoop");
    Assertions.assertFalse(exists(fc, testDir));
    fc.mkdir(testDir, FsPermission.getDefault(), true);
    Assertions.assertTrue(exists(fc, testDir));
    
    createFile(getTestRootPath(fc, "test/hadoop/file"));
    
    Path testSubDir = getTestRootPath(fc, "test/hadoop/file/subdir");
    try {
      fc.mkdir(testSubDir, FsPermission.getDefault(), true);
      Assertions.fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }

    try {
      Assertions.assertFalse(exists(fc, testSubDir));
    } catch (AccessControlException e) {
      // Expected : HDFS-11132 Checks on paths under file may be rejected by
      // file missing execute permission.
    }

    Path testDeepSubDir = getTestRootPath(fc, "test/hadoop/file/deep/sub/dir");
    try {
      fc.mkdir(testDeepSubDir, FsPermission.getDefault(), true);
      Assertions.fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }

    try {
      Assertions.assertFalse(exists(fc, testDeepSubDir));
    } catch (AccessControlException e) {
      // Expected : HDFS-11132 Checks on paths under file may be rejected by
      // file missing execute permission.
    }

  }
  
  @Test
  public void testGetFileStatusThrowsExceptionForNonExistentFile() 
    throws Exception {
    try {
      fc.getFileStatus(getTestRootPath(fc, "test/hadoop/file"));
      Assertions.fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      // expected
    }
  } 

  @Test
  public void testListStatusThrowsExceptionForNonExistentFile()
                                                    throws Exception {
    try {
      fc.listStatus(getTestRootPath(fc, "test/hadoop/file"));
      Assertions.fail("Should throw FileNotFoundException");
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
    Assertions.assertFalse(exists(fc, testDirs[0]));

    for (Path path : testDirs) {
      fc.mkdir(path, FsPermission.getDefault(), true);
    }

    // test listStatus that returns an array
    FileStatus[] paths = fc.util().listStatus(getTestRootPath(fc, "test"));
    Assertions.assertEquals(1, paths.length);
    Assertions.assertEquals(getTestRootPath(fc, "test/hadoop"), paths[0].getPath());

    paths = fc.util().listStatus(getTestRootPath(fc, "test/hadoop"));
    Assertions.assertEquals(3, paths.length);

    Assertions.assertTrue(containsPath(getTestRootPath(fc, "test/hadoop/a"),
        paths));
    Assertions.assertTrue(containsPath(getTestRootPath(fc, "test/hadoop/b"),
        paths));
    Assertions.assertTrue(containsPath(getTestRootPath(fc, "test/hadoop/c"),
        paths));

    paths = fc.util().listStatus(getTestRootPath(fc, "test/hadoop/a"));
    Assertions.assertEquals(0, paths.length);
    
    // test listStatus that returns an iterator
    RemoteIterator<FileStatus> pathsIterator = 
      fc.listStatus(getTestRootPath(fc, "test"));
    Assertions.assertEquals(getTestRootPath(fc, "test/hadoop"), 
        pathsIterator.next().getPath());
    Assertions.assertFalse(pathsIterator.hasNext());

    pathsIterator = fc.listStatus(getTestRootPath(fc, "test/hadoop"));
    FileStatus[] subdirs = new FileStatus[3];
    int i=0;
    while(i<3 && pathsIterator.hasNext()) {
      subdirs[i++] = pathsIterator.next();
    }
    Assertions.assertFalse(pathsIterator.hasNext());
    Assertions.assertTrue(i==3);
    
    Assertions.assertTrue(containsPath(getTestRootPath(fc, "test/hadoop/a"),
        subdirs));
    Assertions.assertTrue(containsPath(getTestRootPath(fc, "test/hadoop/b"),
        subdirs));
    Assertions.assertTrue(containsPath(getTestRootPath(fc, "test/hadoop/c"),
        subdirs));

    pathsIterator = fc.listStatus(getTestRootPath(fc, "test/hadoop/a"));
    Assertions.assertFalse(pathsIterator.hasNext());
  }

  @Test
  public void testListFiles() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, "test/dir1"),
        getTestRootPath(fc, "test/dir1/dir1"),
        getTestRootPath(fc, "test/dir2")
    };
    Path[] testFiles = {
        new Path(testDirs[0], "file1"),
        new Path(testDirs[0], "file2"),
        new Path(testDirs[1], "file2"),
        new Path(testDirs[2], "file1")
    };

    for (Path path : testDirs) {
      fc.mkdir(path, FsPermission.getDefault(), true);
    }
    for (Path p : testFiles) {
      FSDataOutputStream out = fc.create(p).build();
      out.writeByte(0);
      out.close();
    }

    RemoteIterator<LocatedFileStatus> filesIterator =
        fc.util().listFiles(getTestRootPath(fc, "test"), true);
    LocatedFileStatus[] fileStats =
        new LocatedFileStatus[testFiles.length];
    for (int i = 0; i < fileStats.length; i++) {
      assertTrue(filesIterator.hasNext());
      fileStats[i] = filesIterator.next();
    }
    assertFalse(filesIterator.hasNext());

    for (Path p : testFiles) {
      assertTrue(containsPath(p, fileStats));
    }
  }
  
  @Test
  public void testListStatusFilterWithNoMatches() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA2),
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX), };
    
   if (exists(fc, testDirs[0]) == false) {
     for (Path path : testDirs) {
       fc.mkdir(path, FsPermission.getDefault(), true);
     }
   }

    // listStatus with filters returns empty correctly
    FileStatus[] filteredPaths = fc.util().listStatus(
        getTestRootPath(fc, "test"), TEST_X_FILTER);
    Assertions.assertEquals(0,filteredPaths.length);
    
  }
  
  @Test
  public void testListStatusFilterWithSomeMatches() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AAA2), };

    if (exists(fc, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }

    // should return 2 paths ("/test/hadoop/axa" and "/test/hadoop/axx")
    FileStatus[] filteredPaths = fc.util()
        .listStatus(getTestRootPath(fc, "test/hadoop"),
            TEST_X_FILTER);
    Assertions.assertEquals(2,filteredPaths.length);
    Assertions.assertTrue(containsPath(getTestRootPath(fc,
        TEST_DIR_AXA), filteredPaths));
    Assertions.assertTrue(containsPath(getTestRootPath(fc,
        TEST_DIR_AXX), filteredPaths));
  }
  
  @Test
  public void testGlobStatusNonExistentFile() throws Exception {
    FileStatus[] paths = fc.util().globStatus(
          getTestRootPath(fc, "test/hadoopfsdf"));
    Assertions.assertNull(paths);

    paths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoopfsdf/?"));
    Assertions.assertEquals(0, paths.length);
    paths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoopfsdf/xyz*/?"));
    Assertions.assertEquals(0, paths.length);
  }
  
  @Test
  public void testGlobStatusWithNoMatchesInPath() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AAA2), };

    if (exists(fc, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }

    // should return nothing
    FileStatus[] paths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop/?"));
    Assertions.assertEquals(0, paths.length);
  }
  
  @Test
  public void testGlobStatusSomeMatchesInDirectories() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AAA2), };

    if (exists(fc, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }
    
    // Should return two items ("/test/hadoop" and "/test/hadoop2")
    FileStatus[] paths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop*"));
    Assertions.assertEquals(2, paths.length);
    Assertions.assertTrue(containsPath(getTestRootPath(fc,
        "test/hadoop"), paths));
    Assertions.assertTrue(containsPath(getTestRootPath(fc,
        "test/hadoop2"), paths));
  }
  
  @Test
  public void testGlobStatusWithMultipleWildCardMatches() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AAA2), };

    if (exists(fc, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }

    //Should return all 4 items ("/test/hadoop/aaa", "/test/hadoop/axa"
    //"/test/hadoop/axx", and "/test/hadoop2/axx")
    FileStatus[] paths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop*/*"));
    Assertions.assertEquals(4, paths.length);
    Assertions.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AAA), paths));
    Assertions.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXA), paths));
    Assertions.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXX), paths));
    Assertions.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AAA2), paths));
  }
  
  @Test
  public void testGlobStatusWithMultipleMatchesOfSingleChar() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AAA2), };

    if (exists(fc, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }
    
    //Should return only 2 items ("/test/hadoop/axa", "/test/hadoop/axx")
    FileStatus[] paths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop/ax?"));
    Assertions.assertEquals(2, paths.length);
    Assertions.assertTrue(containsPath(getTestRootPath(fc,
        TEST_DIR_AXA), paths));
    Assertions.assertTrue(containsPath(getTestRootPath(fc,
        TEST_DIR_AXX), paths));
  }
  
  @Test
  public void testGlobStatusFilterWithEmptyPathResults() throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AXX), };

    if (exists(fc, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }
    
    //This should return an empty set
    FileStatus[] filteredPaths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop/?"),
        DEFAULT_FILTER);
    Assertions.assertEquals(0,filteredPaths.length);
  }
  
  @Test
  public void testGlobStatusFilterWithSomePathMatchesAndTrivialFilter()
      throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AXX), };

    if (exists(fc, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }
    
    //This should return all three (aaa, axa, axx)
    FileStatus[] filteredPaths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop/*"),
        DEFAULT_FILTER);
    Assertions.assertEquals(3, filteredPaths.length);
    Assertions.assertTrue(containsPath(getTestRootPath(fc,
        TEST_DIR_AAA), filteredPaths));
    Assertions.assertTrue(containsPath(getTestRootPath(fc,
        TEST_DIR_AXA), filteredPaths));
    Assertions.assertTrue(containsPath(getTestRootPath(fc,
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

    if (exists(fc, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }
    
    //This should return all three (aaa, axa, axx)
    FileStatus[] filteredPaths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop/a??"),
        DEFAULT_FILTER);
    Assertions.assertEquals(3, filteredPaths.length);
    Assertions.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AAA),
        filteredPaths));
    Assertions.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXA),
        filteredPaths));
    Assertions.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXX),
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

    if (exists(fc, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }
    
    //This should return two (axa, axx)
    FileStatus[] filteredPaths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop/*"),
        TEST_X_FILTER);
    Assertions.assertEquals(2, filteredPaths.length);
    Assertions.assertTrue(containsPath(getTestRootPath(fc,
        TEST_DIR_AXA), filteredPaths));
    Assertions.assertTrue(containsPath(getTestRootPath(fc,
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

    if (exists(fc, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }
    
    //This should return an empty set
    FileStatus[] filteredPaths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop/?"),
        TEST_X_FILTER);
    Assertions.assertEquals(0,filteredPaths.length);
  }
  
  @Test
  public void testGlobStatusFilterWithMultiplePathWildcardsAndNonTrivialFilter()
      throws Exception {
    Path[] testDirs = {
        getTestRootPath(fc, TEST_DIR_AAA),
        getTestRootPath(fc, TEST_DIR_AXA),
        getTestRootPath(fc, TEST_DIR_AXX),
        getTestRootPath(fc, TEST_DIR_AXX), };

    if (exists(fc, testDirs[0]) == false) {
      for (Path path : testDirs) {
        fc.mkdir(path, FsPermission.getDefault(), true);
      }
    }
    
    //This should return two (axa, axx)
    FileStatus[] filteredPaths = fc.util().globStatus(
        getTestRootPath(fc, "test/hadoop/a??"),
        TEST_X_FILTER);
    Assertions.assertEquals(2, filteredPaths.length);
    Assertions.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXA),
        filteredPaths));
    Assertions.assertTrue(containsPath(getTestRootPath(fc, TEST_DIR_AXX),
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

    FSDataOutputStream out = fc.create(path, EnumSet.of(CREATE),
        CreateOpts.repFac((short) 1), CreateOpts
            .blockSize(getDefaultBlockSize()));
    out.write(data, 0, len);
    out.close();

    Assertions.assertTrue(exists(fc, path), "Exists");
    Assertions.assertEquals(len, fc.getFileStatus(path).getLen(), "Length");

    FSDataInputStream in = fc.open(path);
    byte[] buf = new byte[len];
    in.readFully(0, buf);
    in.close();

    Assertions.assertEquals(len, buf.length);
    for (int i = 0; i < buf.length; i++) {
      Assertions.assertEquals(data[i], buf[i], "Position " + i);
    }
    
    Assertions.assertTrue(fc.delete(path, false), "Deleted");
    
    Assertions.assertFalse(exists(fc, path), "No longer exists");

  }

  @Test
  public void testNullCreateFlag() throws IOException {
    assertThrows(HadoopIllegalArgumentException.class, () -> {
      Path p = getTestRootPath(fc, "test/file");
      fc.create(p, null);
      Assertions.fail("Excepted exception not thrown");
    });
  }
  
  @Test
  public void testEmptyCreateFlag() throws IOException {
    assertThrows(HadoopIllegalArgumentException.class, ()->{
      Path p = getTestRootPath(fc, "test/file");
      fc.create(p, EnumSet.noneOf(CreateFlag.class));
      Assertions.fail("Excepted exception not thrown");
    });
  }

  @Test
  public void testCreateFlagCreateExistingFile() throws IOException {
    assertThrows(FileAlreadyExistsException.class, () -> {
      Path p = getTestRootPath(fc, "test/testCreateFlagCreateExistingFile");
      createFile(p);
      fc.create(p, EnumSet.of(CREATE));
      Assertions.fail("Excepted exception not thrown");
    });
  }

  @Test
  public void testCreateFlagOverwriteNonExistingFile() throws IOException {
    assertThrows(FileNotFoundException.class, () -> {
      Path p = getTestRootPath(fc, "test/testCreateFlagOverwriteNonExistingFile");
      fc.create(p, EnumSet.of(OVERWRITE));
      Assertions.fail("Excepted exception not thrown");
    });
  }
  
  @Test
  public void testCreateFlagOverwriteExistingFile() throws IOException {
    Path p = getTestRootPath(fc, "test/testCreateFlagOverwriteExistingFile");
    createFile(p);
    FSDataOutputStream out = fc.create(p, EnumSet.of(OVERWRITE));
    writeData(fc, p, out, data, data.length);
  }

  @Test
  public void testCreateFlagAppendNonExistingFile() throws IOException {
    assertThrows(FileNotFoundException.class, () -> {
      Path p = getTestRootPath(fc, "test/testCreateFlagAppendNonExistingFile");
      fc.create(p, EnumSet.of(APPEND));
      Assertions.fail("Excepted exception not thrown");
    });
  }
  
  @Test
  public void testCreateFlagAppendExistingFile() throws IOException {
    Path p = getTestRootPath(fc, "test/testCreateFlagAppendExistingFile");
    createFile(p);
    FSDataOutputStream out = fc.create(p, EnumSet.of(APPEND));
    writeData(fc, p, out, data, 2 * data.length);
  }
  
  @Test
  public void testCreateFlagCreateAppendNonExistingFile() throws IOException {
    Path p = getTestRootPath(fc, "test/testCreateFlagCreateAppendNonExistingFile");
    FSDataOutputStream out = fc.create(p, EnumSet.of(CREATE, APPEND));
    writeData(fc, p, out, data, data.length);
  }
  
  @Test
  public void testCreateFlagCreateAppendExistingFile() throws IOException {
    Path p = getTestRootPath(fc, "test/testCreateFlagCreateAppendExistingFile");
    createFile(p);
    FSDataOutputStream out = fc.create(p, EnumSet.of(CREATE, APPEND));
    writeData(fc, p, out, data, 2*data.length);
  }
  
  @Test
  public void testCreateFlagAppendOverwrite() throws IOException {
    assertThrows(HadoopIllegalArgumentException.class, () -> {
      Path p = getTestRootPath(fc, "test/nonExistent");
      fc.create(p, EnumSet.of(APPEND, OVERWRITE));
      Assertions.fail("Excepted exception not thrown");
    });
  }
  
  @Test
  public void testCreateFlagAppendCreateOverwrite() throws IOException {
    assertThrows(HadoopIllegalArgumentException.class, () -> {
      Path p = getTestRootPath(fc, "test/nonExistent");
      fc.create(p, EnumSet.of(CREATE, APPEND, OVERWRITE));
      Assertions.fail("Excepted exception not thrown");
    });
  }

  @Test
  public void testBuilderCreateNonExistingFile() throws IOException {
    Path p = getTestRootPath(fc, "test/testBuilderCreateNonExistingFile");
    FSDataOutputStream out = fc.create(p).build();
    writeData(fc, p, out, data, data.length);
  }

  @Test
  public void testBuilderCreateExistingFile() throws IOException {
    Path p = getTestRootPath(fc, "test/testBuilderCreateExistingFile");
    createFile(p);
    FSDataOutputStream out = fc.create(p).overwrite(true).build();
    writeData(fc, p, out, data, data.length);
  }

  @Test
  public void testBuilderCreateAppendNonExistingFile() throws IOException {
    Path p = getTestRootPath(fc, "test/testBuilderCreateAppendNonExistingFile");
    FSDataOutputStream out = fc.create(p).append().build();
    writeData(fc, p, out, data, data.length);
  }

  @Test
  public void testBuilderCreateAppendExistingFile() throws IOException {
    Path p = getTestRootPath(fc, "test/testBuilderCreateAppendExistingFile");
    createFile(p);
    FSDataOutputStream out = fc.create(p).append().build();
    writeData(fc, p, out, data, 2 * data.length);
  }

  @Test
  public void testBuilderCreateRecursive() throws IOException {
    Path p = getTestRootPath(fc, "test/parent/no/exist/file1");
    try (FSDataOutputStream out = fc.create(p).build()) {
      fail("Should throw FileNotFoundException on non-exist directory");
    } catch (FileNotFoundException e) {
    }

    FSDataOutputStream out = fc.create(p).recursive().build();
    writeData(fc, p, out, data, data.length);
  }

  private static void writeData(FileContext fc, Path p, FSDataOutputStream out,
      byte[] data, long expectedLen) throws IOException {
    out.write(data, 0, data.length);
    out.close();
    Assertions.assertTrue(exists(fc, p), "Exists");
    Assertions.assertEquals(expectedLen, fc.getFileStatus(p).getLen(), "Length");
  }
  
  @Test
  public void testWriteInNonExistentDirectory() throws IOException {
    Path path = getTestRootPath(fc, "test/hadoop/file");
    Assertions.assertFalse(exists(fc, path.getParent()), "Parent doesn't exist");
    createFile(path);
    
    Assertions.assertTrue(exists(fc, path), "Exists");
    Assertions.assertEquals(data.length, fc.getFileStatus(path).getLen(), "Length");
    Assertions.assertTrue(exists(fc, path.getParent()), "Parent exists");
  }

  @Test
  public void testDeleteNonExistentFile() throws IOException {
    Path path = getTestRootPath(fc, "test/hadoop/file");    
    Assertions.assertFalse(exists(fc, path), "Doesn't exist");
    Assertions.assertFalse(fc.delete(path, true), "No deletion");
  }
  
  @Test
  public void testDeleteRecursively() throws IOException {
    Path dir = getTestRootPath(fc, "test/hadoop");
    Path file = getTestRootPath(fc, "test/hadoop/file");
    Path subdir = getTestRootPath(fc, "test/hadoop/subdir");
    
    createFile(file);
    fc.mkdir(subdir,FsPermission.getDefault(), true);
    
    Assertions.assertTrue(exists(fc, file), "File exists");
    Assertions.assertTrue(exists(fc, dir), "Dir exists");
    Assertions.assertTrue(exists(fc, subdir), "Subdir exists");
    
    try {
      fc.delete(dir, false);
      Assertions.fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    Assertions.assertTrue(exists(fc, file), "File still exists");
    Assertions.assertTrue(exists(fc, dir), "Dir still exists");
    Assertions.assertTrue(exists(fc, subdir), "Subdir still exists");
    
    Assertions.assertTrue(fc.delete(dir, true), "Deleted");
    Assertions.assertFalse(exists(fc, file), "File doesn't exist");
    Assertions.assertFalse(exists(fc, dir), "Dir doesn't exist");
    Assertions.assertFalse(exists(fc, subdir), "Subdir doesn't exist");
  }
  
  @Test
  public void testDeleteEmptyDirectory() throws IOException {
    Path dir = getTestRootPath(fc, "test/hadoop");
    fc.mkdir(dir, FsPermission.getDefault(), true);
    Assertions.assertTrue(exists(fc, dir), "Dir exists");
    Assertions.assertTrue(fc.delete(dir, false), "Deleted");
    Assertions.assertFalse(exists(fc, dir), "Dir doesn't exist");
  }
  
  @Test
  public void testRenameNonExistentPath() throws Exception {
    if (!renameSupported()) return;
    Path src = getTestRootPath(fc, "test/hadoop/nonExistent");
    Path dst = getTestRootPath(fc, "test/new/newpath");
    try {
      rename(src, dst, false, false, Rename.NONE);
      Assertions.fail("Should throw FileNotFoundException");
    } catch (IOException e) {
      Assertions.assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }

    try {
      rename(src, dst, false, false, Rename.OVERWRITE);
      Assertions.fail("Should throw FileNotFoundException");
    } catch (IOException e) {
      Assertions.assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }
  }

  @Test
  public void testRenameFileToNonExistentDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fc, "test/hadoop/file");
    createFile(src);
    Path dst = getTestRootPath(fc, "test/nonExistent/newfile");
    
    try {
      rename(src, dst, true, false, Rename.NONE);
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
      Assertions.assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }

    try {
      rename(src, dst, true, false, Rename.OVERWRITE);
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
      Assertions.assertTrue(unwrapException(e) instanceof FileNotFoundException);
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
      rename(src, dst, true, false, Rename.NONE);
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
    }

    try {
      rename(src, dst, true, false, Rename.OVERWRITE);
      Assertions.fail("Expected exception was not thrown");
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
    rename(src, dst, false, true, Rename.OVERWRITE);
  }

  @Test
  public void testRenameFileToItself() throws Exception {
    if (!renameSupported()) return;
    Path src = getTestRootPath(fc, "test/hadoop/file");
    createFile(src);
    try {
      rename(src, src, true, true, Rename.NONE);
      Assertions.fail("Renamed file to itself");
    } catch (IOException e) {
      Assertions.assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Also fails with overwrite
    try {
      rename(src, src, true, true, Rename.OVERWRITE);
      Assertions.fail("Renamed file to itself");
    } catch (IOException e) {
      Assertions.assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
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
      rename(src, dst, true, true, Rename.NONE);
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
      Assertions.assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    
    // Succeeds with overwrite option
    rename(src, dst, false, true, Rename.OVERWRITE);
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
      rename(src, dst, true, true, Rename.NONE);
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
    }
    
    // File cannot be renamed as directory
    try {
      rename(src, dst, true, true, Rename.OVERWRITE);
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
    }
  }

  @Test
  public void testRenameDirectoryToItself() throws Exception {
    if (!renameSupported()) return;
    Path src = getTestRootPath(fc, "test/hadoop/dir");
    fc.mkdir(src, FileContext.DEFAULT_PERM, true);
    try {
      rename(src, src, true, true, Rename.NONE);
      Assertions.fail("Renamed directory to itself");
    } catch (IOException e) {
      Assertions.assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Also fails with overwrite
    try {
      rename(src, src, true, true, Rename.OVERWRITE);
      Assertions.fail("Renamed directory to itself");
    } catch (IOException e) {
      Assertions.assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);      
    }
  }

  @Test
  public void testRenameDirectoryToNonExistentParent() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fc, "test/hadoop/dir");
    fc.mkdir(src, FileContext.DEFAULT_PERM, true);
    Path dst = getTestRootPath(fc, "test/nonExistent/newdir");
    
    try {
      rename(src, dst, true, false, Rename.NONE);
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
      Assertions.assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }

    try {
      rename(src, dst, true, false, Rename.OVERWRITE);
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
      Assertions.assertTrue(unwrapException(e) instanceof FileNotFoundException);
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
    
    rename(src, dst, false, true, options);
    Assertions.assertFalse(
        exists(fc, getTestRootPath(fc, "test/hadoop/dir/file1")), "Nested file1 exists");
    Assertions.assertFalse(
        exists(fc, getTestRootPath(fc, "test/hadoop/dir/subdir/file2")), "Nested file2 exists");
    Assertions.assertTrue(
       exists(fc, getTestRootPath(fc, "test/new/newdir/file1")), "Renamed nested file1 exists");
    Assertions.assertTrue(
        exists(fc, getTestRootPath(fc, "test/new/newdir/subdir/file2")), "Renamed nested exists");
  }

  @Test
  public void testRenameDirectoryAsEmptyDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = getTestRootPath(fc, "test/hadoop/dir");
    fc.mkdir(src, FileContext.DEFAULT_PERM, true);
    createFile(getTestRootPath(fc, "test/hadoop/dir/file1"));
    createFile(getTestRootPath(fc, "test/hadoop/dir/subdir/file2"));
    
    Path dst = getTestRootPath(fc, "test/new/newdir");
    fc.mkdir(dst, FileContext.DEFAULT_PERM, true);

    // Fails without overwrite option
    try {
      rename(src, dst, true, true, Rename.NONE);
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
      // Expected (cannot over-write non-empty destination)
      Assertions.assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Succeeds with the overwrite option
    rename(src, dst, false, true, Rename.OVERWRITE);
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
      rename(src, dst, true, true, Rename.NONE);
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
      // Expected (cannot over-write non-empty destination)
      Assertions.assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Fails even with the overwrite option
    try {
      rename(src, dst, true, true, Rename.OVERWRITE);
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException ex) {
      // Expected (cannot over-write non-empty destination)
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
      rename(src, dst, true, true, Rename.NONE);
      Assertions.fail("Expected exception was not thrown");
    } catch (IOException e) {
    }
    // Directory cannot be renamed as existing file
    try {
      rename(src, dst, true, true, Rename.OVERWRITE);
      Assertions.fail("Expected exception was not thrown");
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
    FSDataOutputStream out = fc.create(src, EnumSet.of(CREATE),
            Options.CreateOpts.createParent());
    
    out.writeChar('H'); //write some data
    out.close();
    out.close();
  }

  @Test
  /** Test FileContext APIs when symlinks are not supported */
  public void testUnsupportedSymlink() throws IOException {
    Path file = getTestRootPath(fc, "file");
    Path link = getTestRootPath(fc, "linkToFile");
    if (!fc.getDefaultFileSystem().supportsSymlinks()) {
      try {
        fc.createSymlink(file, link, false);
        Assertions.fail("Created a symlink on a file system that "+
                    "does not support symlinks.");
      } catch (UnsupportedOperationException e) {
        // Expected
      }
      createFile(file);
      try {
        fc.getLinkTarget(file);
        Assertions.fail("Got a link target on a file system that "+
                    "does not support symlinks.");
      } catch (IOException e) {
        // Expected
      }
      Assertions.assertEquals(fc.getFileStatus(file), fc.getFileLinkStatus(file));
    }
  }
  
  protected void createFile(Path path) throws IOException {
    FSDataOutputStream out = fc.create(path, EnumSet.of(CREATE),
        Options.CreateOpts.createParent());
    out.write(data, 0, data.length);
    out.close();
  }

  protected void rename(Path src, Path dst, boolean srcExists,
      boolean dstExists, Rename... options) throws IOException {
    try {
      fc.rename(src, dst, options);
    } finally {
      Assertions.assertEquals(srcExists, exists(fc, src), "Source exists");
      Assertions.assertEquals(dstExists, exists(fc, dst), "Destination exists");
    }
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

  @Test
  public void testOpen2() throws IOException {
    final Path rootPath = getTestRootPath(fc, "test");
    //final Path rootPath = getAbsoluteTestRootPath(fc);
    final Path path = new Path(rootPath, "zoo");
    createFile(path);
    final long length = fc.getFileStatus(path).getLen();
    try (FSDataInputStream fsdis = fc.open(path, 2048)) {
      byte[] bb = new byte[(int) length];
      fsdis.readFully(bb);
      assertArrayEquals(data, bb);
    }
  }

  @Test
  public void testSetVerifyChecksum() throws IOException {
    final Path rootPath = getTestRootPath(fc, "test");
    final Path path = new Path(rootPath, "zoo");

    FSDataOutputStream out = fc.create(path, EnumSet.of(CREATE),
        Options.CreateOpts.createParent());
    try {
      // instruct FS to verify checksum through the FileContext:
      fc.setVerifyChecksum(true, path);
      out.write(data, 0, data.length);
    } finally {
      out.close();
    }

    // NB: underlying FS may be different (this is an abstract test),
    // so we cannot assert .zoo.crc existence.
    // Instead, we check that the file is read correctly:
    FileStatus fileStatus = fc.getFileStatus(path);
    final long len = fileStatus.getLen();
    assertTrue(len == data.length);
    byte[] bb = new byte[(int)len];
    FSDataInputStream fsdis = fc.open(path);
    try {
      fsdis.readFully(bb);
    } finally {
      fsdis.close();
    }
    assertArrayEquals(data, bb);
  }

  @Test
  public void testListCorruptFileBlocks() throws IOException {
    final Path rootPath = getTestRootPath(fc, "test");
    final Path path = new Path(rootPath, "zoo");
    createFile(path);
    try {
      final RemoteIterator<Path> remoteIterator = fc
          .listCorruptFileBlocks(path);
      if (listCorruptedBlocksSupported()) {
        assertTrue(remoteIterator != null);
        Path p;
        while (remoteIterator.hasNext()) {
          p = remoteIterator.next();
          System.out.println("corrupted block: " + p);
        }
        try {
          remoteIterator.next();
          fail();
        } catch (NoSuchElementException nsee) {
          // okay
        }
      } else {
        fail();
      }
    } catch (UnsupportedOperationException uoe) {
      if (listCorruptedBlocksSupported()) {
        fail(uoe.toString());
      } else {
        // okay
      }
    }
  }

  protected abstract boolean listCorruptedBlocksSupported();

  @Test
  public void testDeleteOnExitUnexisting() throws IOException {
    final Path rootPath = getTestRootPath(fc, "test");
    final Path path = new Path(rootPath, "zoo");
    boolean registered = fc.deleteOnExit(path);
    // because "zoo" does not exist:
    assertTrue(!registered);
  }

  @Test
  public void testFileContextStatistics() throws IOException {
    FileContext.clearStatistics();

    final Path rootPath = getTestRootPath(fc, "test");
    final Path path = new Path(rootPath, "zoo");
    createFile(path);
    byte[] bb = new byte[data.length];
    FSDataInputStream fsdis = fc.open(path);
    try {
      fsdis.readFully(bb);
    } finally {
      fsdis.close();
    }
    assertArrayEquals(data, bb);

    FileContext.printStatistics();
  }

  @Test
  /*
   * Test method
   *  org.apache.hadoop.fs.FileContext.getFileContext(AbstractFileSystem)
   */
  public void testGetFileContext1() throws IOException {
    final Path rootPath = getTestRootPath(fc, "test");
    AbstractFileSystem asf = fc.getDefaultFileSystem();
    // create FileContext using the protected #getFileContext(1) method:
    FileContext fc2 = FileContext.getFileContext(asf);
    // Now just check that this context can do something reasonable:
    final Path path = new Path(rootPath, "zoo");
    FSDataOutputStream out = fc2.create(path, EnumSet.of(CREATE),
        Options.CreateOpts.createParent());
    out.close();
    Path pathResolved = fc2.resolvePath(path);
    assertEquals(pathResolved.toUri().getPath(), path.toUri().getPath());
  }
  
  private Path getTestRootPath(FileContext fc, String pathString) {
    return fileContextTestHelper.getTestRootPath(fc, pathString);
  }

  /**
   * Create a path under the test path.
   * @param filepath path string in
   * @return a path qualified by the test filesystem
   * @throws IOException IO problems
   */
  protected Path path(String filepath) throws IOException {
    return getTestRootPath(fc, filepath);
  }

  /**
   * Describe a test. This is a replacement for javadocs
   * where the tests role is printed in the log output
   * @param text description
   */
  protected void describe(String text) {
    LOG.info(text);
  }

  @Test
  public void testOpenFileRead() throws Exception {
    final Path path = path("testOpenFileRead");
    createFile(path);
    final long length = fc.getFileStatus(path).getLen();
    try (FSDataInputStream fsdis = fc.openFile(path)
        .opt("fs.test.something", true)
        .opt("fs.test.something2", 3)
        .opt("fs.test.something3", "3")
        .build().get()) {
      byte[] bb = new byte[(int) length];
      fsdis.readFully(bb);
      assertArrayEquals(data, bb);
    }
  }

  @Test
  public void testOpenFileUnknownOption() throws Throwable {
    describe("calling openFile fails when a 'must()' option is unknown");

    final Path path = path("testOpenFileUnknownOption");
    FutureDataInputStreamBuilder builder =
        fc.openFile(path)
            .opt("fs.test.something", true)
            .must("fs.test.something", true);
    intercept(IllegalArgumentException.class,
        () -> builder.build());
  }

  @Test
  public void testOpenFileLazyFail() throws Throwable {
    describe("openFile fails on a missing file in the get() and not before");
    FutureDataInputStreamBuilder builder =
        fc.openFile(path("testOpenFileUnknownOption"))
            .opt("fs.test.something", true);
    interceptFuture(FileNotFoundException.class, "", builder.build());
  }

  @Test
  public void testOpenFileApplyRead() throws Throwable {
    describe("use the apply sequence");
    Path path = path("testOpenFileApplyRead");
    createFile(path);
    CompletableFuture<Long> readAllBytes = fc.openFile(path)
        .build()
        .thenApply(ContractTestUtils::readStream);
    assertEquals(
       data.length
,         (long)readAllBytes.get(), "Wrong number of bytes read from stream");
  }

  @Test
  public void testOpenFileApplyAsyncRead() throws Throwable {
    describe("verify that async accept callbacks are evaluated");
    Path path = path("testOpenFileApplyAsyncRead");
    createFile(path);
    CompletableFuture<FSDataInputStream> future = fc.openFile(path).build();
    AtomicBoolean accepted = new AtomicBoolean(false);
    future.thenAcceptAsync(i -> accepted.set(true)).get();
    assertTrue(
       accepted.get(), "async accept operation not invoked");
  }

}
