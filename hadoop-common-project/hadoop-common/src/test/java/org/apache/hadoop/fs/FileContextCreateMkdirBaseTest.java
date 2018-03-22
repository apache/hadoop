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


import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.apache.hadoop.fs.FileContextTestHelper.*;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsDirectory;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;

import org.apache.hadoop.test.GenericTestUtils;
import org.slf4j.event.Level;

/**
 * <p>
 * A collection of tests for the {@link FileContext}, create method
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

public abstract class FileContextCreateMkdirBaseTest {

  protected final FileContextTestHelper fileContextTestHelper;
  protected static FileContext fc;
      
  static {
    GenericTestUtils.setLogLevel(FileSystem.LOG, Level.DEBUG);
  }
  
  public FileContextCreateMkdirBaseTest() {
      fileContextTestHelper = createFileContextHelper();
  }

  protected FileContextTestHelper createFileContextHelper() {
    return new FileContextTestHelper();
  }

  @Before
  public void setUp() throws Exception {
    fc.mkdir(getTestRootPath(fc), FileContext.DEFAULT_PERM, true);
  }

  @After
  public void tearDown() throws Exception {
    fc.delete(getTestRootPath(fc), true);
  }
  
  
  
  ///////////////////////
  //      Test Mkdir
  ////////////////////////
  
  @Test
  public void testMkdirNonRecursiveWithExistingDir() throws IOException {
    Path f = getTestRootPath(fc, "aDir");
    fc.mkdir(f, FileContext.DEFAULT_PERM, false);
    Assert.assertTrue(isDir(fc, f));
  }
  
  @Test
  public void testMkdirNonRecursiveWithNonExistingDir() {
    try {
      fc.mkdir(getTestRootPath(fc,"NonExistant/aDir"),
          FileContext.DEFAULT_PERM, false);
      Assert.fail("Mkdir with non existing parent dir should have failed");
    } catch (IOException e) {
      // failed As expected
    }
  }
  
  
  @Test
  public void testMkdirRecursiveWithExistingDir() throws IOException {
    Path f = getTestRootPath(fc, "aDir");
    fc.mkdir(f, FileContext.DEFAULT_PERM, true);
    Assert.assertTrue(isDir(fc, f));
  }
  
  
  @Test
  public void testMkdirRecursiveWithNonExistingDir() throws IOException {
    Path f = getTestRootPath(fc, "NonExistant2/aDir");
    fc.mkdir(f, FileContext.DEFAULT_PERM, true);
    Assert.assertTrue(isDir(fc, f));
  }

  @Test
  public void testMkdirsRecursiveWithExistingDir() throws IOException {
    Path f = getTestRootPath(fc, "aDir/bDir/cDir");
    fc.mkdir(f, FileContext.DEFAULT_PERM, true);
    assertIsDirectory(fc.getFileStatus(f));
    assertIsDirectory(fc.getFileStatus(f.getParent()));
    assertIsDirectory(fc.getFileStatus(f.getParent().getParent()));
  }

  @Test
  public void testMkdirRecursiveWithExistingFile() throws IOException {
    Path f = getTestRootPath(fc, "NonExistant3/aDir");
    fc.mkdir(f, FileContext.DEFAULT_PERM, true);
    assertIsDirectory(fc.getFileStatus(f));
    assertIsDirectory(fc.getFileStatus(f.getParent()));

    // create a sample file
    Path filePath = new Path(f.getParent(), "test.txt");
    createFile(fc, filePath);
    assertIsFile(filePath, fc.getFileStatus(filePath));

    // try creating another folder which conflicts with filePath
    Path dirPath = new Path(filePath, "bDir/cDir");
    try {
      fc.mkdir(dirPath, FileContext.DEFAULT_PERM, true);
      Assert.fail("Mkdir for " + dirPath
          + " should have failed as a file was present");
    } catch(IOException e) {
      // failed as expected
    }
  }

  @Test
  public void testWithRename() throws IOException, InterruptedException {
    Path root = getTestRootPath(fc);
    Path f = new Path(root, "d1/d2/d3");
    fc.mkdir(f, FileContext.DEFAULT_PERM, true);
    assertIsDirectory(fc.getFileStatus(new Path(root, "d1")));
    assertIsDirectory(fc.getFileStatus(new Path(root, "d1/d2")));
    assertIsDirectory(fc.getFileStatus(new Path(root, "d1/d2/d3")));

    // create a sample file f.txt
    Path fPath = new Path(root, "d1/d2/f.txt");
    createFile(fc, fPath);
    assertIsFile(fPath, fc.getFileStatus(fPath));
    assertIsDirectory(fc.getFileStatus(new Path(root, "d1")));
    assertIsDirectory(fc.getFileStatus(new Path(root, "d1/d2")));
    assertIsDirectory(fc.getFileStatus(new Path(root, "d1/d2/d3")));

    // create a sample file f2.txt
    Path f2Path = new Path(getTestRootPath(fc), "d1/d2/d3/f2.txt");
    createFile(fc, f2Path);
    assertIsFile(fPath, fc.getFileStatus(f2Path));
    assertIsDirectory(fc.getFileStatus(new Path(root, "d1")));
    assertIsDirectory(fc.getFileStatus(new Path(root, "d1/d2")));
    assertIsDirectory(fc.getFileStatus(new Path(root, "d1/d2/d3")));

    //rename d1/d2/d3 d1/d4
    fc.rename(new Path(root, "d1/d2/d3"), new Path(root, "d1/d4"));
    assertIsDirectory(fc.getFileStatus(new Path(root, "d1")));
    assertIsDirectory(fc.getFileStatus(new Path(root, "d1/d4")));
    Path f2NewPath = new Path(root, "d1/d4/f2.txt");
    assertIsFile(f2NewPath, fc.getFileStatus(f2NewPath));
  }


  ///////////////////////
  //      Test Create
  ////////////////////////
  @Test
  public void testCreateNonRecursiveWithExistingDir() throws IOException {
    Path f = getTestRootPath(fc, "foo");
    createFile(fc, f);
    Assert.assertTrue(isFile(fc, f));
  }
  
  @Test
  public void testCreateNonRecursiveWithNonExistingDir() {
    try {
      createFileNonRecursive(fc, getTestRootPath(fc, "NonExisting/foo"));
      Assert.fail("Create with non existing parent dir should have failed");
    } catch (IOException e) {
      // As expected
    }
  }
  
  
  @Test
  public void testCreateRecursiveWithExistingDir() throws IOException {
    Path f = getTestRootPath(fc,"foo");
    createFile(fc, f);
    Assert.assertTrue(isFile(fc, f));
  }
  
  
  @Test
  public void testCreateRecursiveWithNonExistingDir() throws IOException {
    Path f = getTestRootPath(fc,"NonExisting/foo");
    createFile(fc, f);
    Assert.assertTrue(isFile(fc, f));
  }

  private Path getTestRootPath(FileContext fc) {
    return fileContextTestHelper.getTestRootPath(fc);
  }

  private Path getTestRootPath(FileContext fc, String pathString) {
    return fileContextTestHelper.getTestRootPath(fc, pathString);
  }

}
