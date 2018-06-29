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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;

import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestFileUtilsMkDir {

  private static final File TEST_DIR = GenericTestUtils.getTestDir("nm");
  private final File del = new File(TEST_DIR, "del");

  @Before
  public void before() {
    cleanupImpl();
  }

  @After
  public void tearDown() {
    cleanupImpl();
  }

  private void cleanupImpl() {
    FileUtil.fullyDelete(del, true);
    Assert.assertTrue(!del.exists());
  }

  /**
   * This test validates the correctness of {@link FileUtil#mkDirs(String)} in
   * case of null pointer inputs.
   *
   * @throws IOException
   */
  @Test
  public void testMkDirsWithNullInput() throws IOException {
    int result = FileUtil.mkDirs(null);
    Assert.assertEquals(1, result);
  }

  /**
   * This test validates the correctness of {@link FileUtil#mkDirs(String)}.
   *
   * @throws IOException
   */
  @Test
  public void testMkDirs() throws IOException {
    Assert.assertFalse(del.exists());
    del.mkdirs();

    File directory = new File(del, "newDirectory");
    Assert.assertFalse(directory.exists());

    // Create the directory
    int result = FileUtil.mkDirs(directory.getAbsolutePath());
    Assert.assertEquals(0, result);

    Assert.assertTrue(directory.exists());
    Assert.assertTrue(directory.isDirectory());

    directory.delete();
  }

  /**
   * This test validates the correctness of {@link FileUtil#mkDirs(String)} in
   * case of multiple parents.
   *
   * @throws IOException
   */
  @Test
  public void testMkDirsMultipleParents() throws IOException {
    Assert.assertFalse(del.exists());
    del.mkdirs();

    File directory = new File(del, "newParent1/newParent2/newDirectory");
    Assert.assertFalse(directory.exists());

    // Create the directory
    int result = FileUtil.mkDirs(directory.getAbsolutePath());
    Assert.assertEquals(0, result);

    Assert.assertTrue(directory.exists());
    Assert.assertTrue(directory.isDirectory());

    Assert.assertTrue(directory.getParentFile().exists());
    Assert.assertTrue(directory.getParentFile().isDirectory());

    Assert.assertTrue(directory.getParentFile().getParentFile().exists());
    Assert.assertTrue(directory.getParentFile().getParentFile().isDirectory());

    directory.getParentFile().getParentFile().delete();
    directory.getParentFile().delete();
    directory.delete();
  }

  /**
   * This test validates the correctness of {@link FileUtil#mkDirs(String)} in
   * case of multiple same executions of MkDir.
   *
   * @throws IOException
   */
  @Test
  public void testMkDirsMultipleTimes() throws IOException {
    Assert.assertFalse(del.exists());
    del.mkdirs();

    File directory = new File(del, "newDirectory");
    Assert.assertFalse(directory.exists());

    // Create the directory
    int result = FileUtil.mkDirs(directory.getAbsolutePath());
    Assert.assertEquals(0, result);

    Assert.assertTrue(directory.exists());
    Assert.assertTrue(directory.isDirectory());

    result = FileUtil.mkDirs(directory.getAbsolutePath());
    Assert.assertEquals(0, result);

    directory.delete();
  }

  /**
   * This test validates the correctness of {@link FileUtil#mkDirs(String)} in
   * case of a creation over a file.
   *
   * @throws IOException
   */
  @Test
  public void testMkDirsOverAFile() throws IOException {
    Assert.assertFalse(del.exists());
    del.mkdirs();

    File falseDirectory = new File(del, "newDirectory");
    Assert.assertFalse(falseDirectory.exists());

    byte[] data = "some data".getBytes();

    // write some data to the file
    FileOutputStream os = new FileOutputStream(falseDirectory);
    os.write(data);
    os.close();

    // Create the directory
    try {
      FileUtil.mkDirs(falseDirectory.getAbsolutePath());
      Assert.fail("The test should fail with FileAlreadyExistsException");
    } catch (FileAlreadyExistsException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          "Can not create a directory since a file is already present"));
    }
  }

  /**
   * This test validates the correctness of {@link FileUtil#mkDirs(String)} in
   * case of a creation underneath a file.
   *
   * @throws IOException
   */
  @Test
  public void testMkDirsUnderNeathAFile() throws IOException {
    Assert.assertFalse(del.exists());
    del.mkdirs();

    File newFile = new File(del, "newFile");
    Assert.assertFalse(newFile.exists());

    byte[] data = "some data".getBytes();

    // write some data to the file
    FileOutputStream os = new FileOutputStream(newFile);
    os.write(data);
    os.close();

    File falseDirectory = new File(del, "newFile/newDirectory");

    // Create the directory
    int result = FileUtil.mkDirs(falseDirectory.getAbsolutePath());
    Assert.assertEquals(1, result);

    Assert.assertFalse(falseDirectory.exists());
  }
}
