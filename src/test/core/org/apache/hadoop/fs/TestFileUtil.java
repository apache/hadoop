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
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestFileUtil {
  final static private File TEST_DIR = new File(System.getProperty(
      "test.build.data", "/tmp"), "fu");
  private static String FILE = "x";
  private static String LINK = "y";
  private static String DIR = "dir";
  private File del = new File(TEST_DIR, "del");
  private File tmp = new File(TEST_DIR, "tmp");
  private File dir1 = new File(del, DIR + "1");
  private File dir2 = new File(del, DIR + "2");

  /**
   * Creates directories del and tmp for testing.
   * 
   * Contents of them are
   * dir:tmp: 
   *   file: x
   * dir:del:
   *   file: x
   *   dir: dir1 : file:x
   *   dir: dir2 : file:x
   *   link: y to tmp/x
   *   link: tmpDir to tmp
   */
  private void setupDirs() throws IOException {
    Assert.assertFalse(del.exists());
    Assert.assertFalse(tmp.exists());
    del.mkdirs();
    tmp.mkdirs();
    new File(del, FILE).createNewFile();
    File tmpFile = new File(tmp, FILE);
    tmpFile.createNewFile();

    // create directories 
    dir1.mkdirs();
    dir2.mkdirs();
    new File(dir1, FILE).createNewFile();
    new File(dir2, FILE).createNewFile();

    // create a symlink to file
    File link = new File(del, LINK);
    FileUtil.symLink(tmpFile.toString(), link.toString());

    // create a symlink to dir
    File linkDir = new File(del, "tmpDir");
    FileUtil.symLink(tmp.toString(), linkDir.toString());
    Assert.assertEquals(5, del.listFiles().length);
  }

  @After
  public void tearDown() throws IOException {
    FileUtil.fullyDelete(del);
    FileUtil.fullyDelete(tmp);
  }

  @Test
  public void testFullyDelete() throws IOException {
    setupDirs();
    boolean ret = FileUtil.fullyDelete(del);
    Assert.assertTrue(ret);
    Assert.assertFalse(del.exists());
    validateTmpDir();
  }

  @Test
  public void testFullyDeleteContents() throws IOException {
    setupDirs();
    boolean ret = FileUtil.fullyDeleteContents(del);
    Assert.assertTrue(ret);
    Assert.assertTrue(del.exists());
    Assert.assertEquals(0, del.listFiles().length);
    validateTmpDir();
  }

  private void validateTmpDir() {
    Assert.assertTrue(tmp.exists());
    Assert.assertEquals(1, tmp.listFiles().length);
    Assert.assertTrue(new File(tmp, FILE).exists());
  }
  
  /**
   * Creates a directory which can not be deleted.
   * 
   * Contents of the directory are :
   * dir : del
   *   dir : dir1 : x. this directory is not writable
   * @throws IOException
   */
  private void setupDirsAndNonWritablePermissions() throws IOException {
    Assert.assertFalse(del.exists());
    del.mkdirs();
    new File(del, FILE).createNewFile();

    // create directory 
    dir1.mkdirs();
    new File(dir1, FILE).createNewFile();
    changePermissions(false);
  }
  
  // sets writable permissions for dir1
  private void changePermissions(boolean perm) {
    dir1.setWritable(perm);
  }

  // validates the return value
  // validates the directory:dir1 exists
  // sets writable permissions for the directory so that it can be deleted in 
  // tearDown() 
  private void validateAndSetWritablePermissions(boolean ret) {
    Assert.assertFalse(ret);
    Assert.assertTrue(dir1.exists());
    changePermissions(true);
  }
  
  @Test
  public void testFailFullyDelete() throws IOException {
    setupDirsAndNonWritablePermissions();
    boolean ret = FileUtil.fullyDelete(del);
    validateAndSetWritablePermissions(ret);
  }

  @Test
  public void testFailFullyDeleteContents() throws IOException {
    setupDirsAndNonWritablePermissions();
    boolean ret = FileUtil.fullyDeleteContents(del);
    validateAndSetWritablePermissions(ret);
  }
}
