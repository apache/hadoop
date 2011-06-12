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
import java.util.EnumSet;

import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

public class FileContextCreateMkdirBaseTest {
   
  protected static FileContext fc;
  static final String TEST_ROOT_DIR = new Path(System.getProperty(
      "test.build.data", "build/test/data")).toString().replace(' ', '_')
      + "/test";
  
  
  protected Path getTestRootRelativePath(String pathString) {
    return fc.makeQualified(new Path(TEST_ROOT_DIR, pathString));
  }
  
  private Path rootPath = null;
  protected Path getTestRoot() {
    if (rootPath == null) {
      rootPath = fc.makeQualified(new Path(TEST_ROOT_DIR));
    }
    return rootPath;   
  }


  {
    try {
      ((org.apache.commons.logging.impl.Log4JLogger)FileSystem.LOG).getLogger()
      .setLevel(org.apache.log4j.Level.DEBUG);
    }
    catch(Exception e) {
      System.out.println("Cannot change log level\n"
          + StringUtils.stringifyException(e));
    }
  }
  


  @Before
  public void setUp() throws Exception {
    fc.mkdir(getTestRoot(), FileContext.DEFAULT_PERM, true);
  }

  @After
  public void tearDown() throws Exception {
    fc.delete(getTestRoot(), true);
  }
  
  
  
  ///////////////////////
  //      Test Mkdir
  ////////////////////////
  
  @Test
  public void testMkdirNonRecursiveWithExistingDir() throws IOException {
    Path f = getTestRootRelativePath("aDir");
    fc.mkdir(f, FileContext.DEFAULT_PERM, false);
    Assert.assertTrue(fc.isDirectory(f));
  }
  
  @Test
  public void testMkdirNonRecursiveWithNonExistingDir() {
    try {
      fc.mkdir(getTestRootRelativePath("NonExistant/aDir"),
          FileContext.DEFAULT_PERM, false);
      Assert.fail("Mkdir with non existing parent dir should have failed");
    } catch (IOException e) {
      // failed As expected
    }
  }
  
  
  @Test
  public void testMkdirRecursiveWithExistingDir() throws IOException {
    Path f = getTestRootRelativePath("aDir");
    fc.mkdir(f, FileContext.DEFAULT_PERM, true);
    Assert.assertTrue(fc.isDirectory(f));
  }
  
  
  @Test
  public void testMkdirRecursiveWithNonExistingDir() throws IOException {
    Path f = getTestRootRelativePath("NonExistant2/aDir");
    fc.mkdir(f, FileContext.DEFAULT_PERM, true);
    Assert.assertTrue(fc.isDirectory(f));
  }
 
  ///////////////////////
  //      Test Create
  ////////////////////////
  @Test
  public void testCreateNonRecursiveWithExistingDir() throws IOException {
    Path f = getTestRootRelativePath("foo");
    createFile(f);
    Assert.assertTrue(fc.isFile(f));
  }
  
  @Test
  public void testCreateNonRecursiveWithNonExistingDir() {
    try {
      createFile(getTestRootRelativePath("NonExisting/foo"));
      Assert.fail("Create with non existing parent dir should have failed");
    } catch (IOException e) {
      // As expected
    }
  }
  
  
  @Test
  public void testCreateRecursiveWithExistingDir() throws IOException {
    Path f = getTestRootRelativePath("foo");
    createFile(f, CreateOpts.createParent());
    Assert.assertTrue(fc.isFile(f));
  }
  
  
  @Test
  public void testCreateRecursiveWithNonExistingDir() throws IOException {
    Path f = getTestRootRelativePath("NonExisting/foo");
    createFile(f, CreateOpts.createParent());
    Assert.assertTrue(fc.isFile(f));
  }
  
  
  protected static int getBlockSize() {
    return 1024;
  }
  
  private static byte[] data = new byte[getBlockSize() * 2]; // two blocks of data
  {
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 10);
    }
  }

  protected void createFile(Path path, 
      CreateOpts.CreateParent ... opt) throws IOException {
    
    FSDataOutputStream out = fc.create(path,EnumSet.of(CreateFlag.CREATE), opt);
    out.write(data, 0, data.length);
    out.close();
  }
}
