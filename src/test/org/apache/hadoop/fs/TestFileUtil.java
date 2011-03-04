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
import org.junit.Before;
import org.junit.Test;

public class TestFileUtil {
  final static private File TEST_DIR = new File(System.getProperty(
      "test.build.data", "/tmp"), "fu");
  static String FILE = "x";
  static String LINK = "y";
  static String DIR = "dir";
  File del = new File(TEST_DIR, "del");
  File tmp = new File(TEST_DIR, "tmp");

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
  @Before
  public void setUp() throws IOException {
    Assert.assertFalse(del.exists());
    Assert.assertFalse(tmp.exists());
    del.mkdirs();
    tmp.mkdirs();
    new File(del, FILE).createNewFile();
    File tmpFile = new File(tmp, FILE);
    tmpFile.createNewFile();

    // create directories 
    File one = new File(del, DIR + "1");
    one.mkdirs();
    File two = new File(del, DIR + "2");
    two.mkdirs();
    new File(one, FILE).createNewFile();
    new File(two, FILE).createNewFile();

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
    FileUtil.fullyDelete(del);
    Assert.assertFalse(del.exists());
    validateTmpDir();
  }

  @Test
  public void testFullyDeleteContents() throws IOException {
    FileUtil.fullyDeleteContents(del);
    Assert.assertTrue(del.exists());
    Assert.assertEquals(0, del.listFiles().length);
    validateTmpDir();
  }

  private void validateTmpDir() {
    Assert.assertTrue(tmp.exists());
    Assert.assertEquals(1, tmp.listFiles().length);
    Assert.assertTrue(new File(tmp, FILE).exists());
  }
}
