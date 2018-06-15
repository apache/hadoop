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

package org.apache.hadoop.fs.azurebfs;

import java.io.FileNotFoundException;
import java.util.EnumSet;

import org.junit.Test;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test create operation.
 */
public class ITestAzureBlobFileSystemCreate extends DependencyInjectedTest {
  private static final Path TEST_FILE_PATH = new Path("testfile");
  private static final Path TEST_FOLDER_PATH = new Path("testFolder");
  private static final String TEST_CHILD_FILE = "childFile";
  public ITestAzureBlobFileSystemCreate() {
    super();
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void testCreateFileWithExistingDir() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.mkdirs(TEST_FOLDER_PATH);
    fs.create(TEST_FOLDER_PATH);
  }

  @Test
  public void testEnsureFileCreated() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.create(TEST_FILE_PATH);

    FileStatus fileStatus = fs.getFileStatus(TEST_FILE_PATH);
    assertNotNull(fileStatus);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path testFile = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
    try {
      fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null);
      assertTrue("Should've thrown", false);
    } catch (FileNotFoundException e) {
    }
    fs.mkdirs(TEST_FOLDER_PATH);
    fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null)
        .close();
    assertTrue(fs.exists(testFile));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive1() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path testFile = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
    try {
      fs.createNonRecursive(testFile, FsPermission.getDefault(), EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE), 1024, (short) 1, 1024, null);
      assertTrue("Should've thrown", false);
    } catch (FileNotFoundException e) {
    }
    fs.mkdirs(TEST_FOLDER_PATH);
    fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null)
        .close();
    assertTrue(fs.exists(testFile));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive2() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();

    Path testFile = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
    try {
      fs.createNonRecursive(testFile, FsPermission.getDefault(), false, 1024, (short) 1, 1024, null);
      assertTrue("Should've thrown", false);
    } catch (FileNotFoundException e) {
    }
    fs.mkdirs(TEST_FOLDER_PATH);
    fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null)
        .close();
    assertTrue(fs.exists(testFile));
  }
}
