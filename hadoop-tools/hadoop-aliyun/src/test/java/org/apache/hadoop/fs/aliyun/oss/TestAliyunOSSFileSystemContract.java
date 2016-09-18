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

package org.apache.hadoop.fs.aliyun.oss;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Tests a live Aliyun OSS system.
 *
 * This uses BlockJUnit4ClassRunner because FileSystemContractBaseTest from
 * TestCase which uses the old Junit3 runner that doesn't ignore assumptions
 * properly making it impossible to skip the tests if we don't have a valid
 * bucket.
 */
public class TestAliyunOSSFileSystemContract
    extends FileSystemContractBaseTest {
  public static final String TEST_FS_OSS_NAME = "test.fs.oss.name";
  private static String testRootPath =
      AliyunOSSTestUtils.generateUniqueTestPath();

  @Override
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    fs = AliyunOSSTestUtils.createTestFileSystem(conf);
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.delete(super.path(testRootPath), true);
    }
    super.tearDown();
  }

  @Override
  protected Path path(String path) {
    if (path.startsWith("/")) {
      return super.path(testRootPath + path);
    } else {
      return super.path(testRootPath + "/" + path);
    }
  }

  @Override
  public void testMkdirsWithUmask() throws Exception {
    // not supported
  }

  @Override
  public void testRootDirAlwaysExists() throws Exception {
    //this will throw an exception if the path is not found
    fs.getFileStatus(super.path("/"));
    //this catches overrides of the base exists() method that don't
    //use getFileStatus() as an existence probe
    assertTrue("FileSystem.exists() fails for root",
        fs.exists(super.path("/")));
  }

  @Override
  public void testRenameRootDirForbidden() throws Exception {
    if (!renameSupported()) {
      return;
    }
    rename(super.path("/"),
           super.path("/test/newRootDir"),
           false, true, false);
  }

  public void testDeleteSubdir() throws IOException {
    Path parentDir = this.path("/test/hadoop");
    Path file = this.path("/test/hadoop/file");
    Path subdir = this.path("/test/hadoop/subdir");
    this.createFile(file);

    assertTrue("Created subdir", this.fs.mkdirs(subdir));
    assertTrue("File exists", this.fs.exists(file));
    assertTrue("Parent dir exists", this.fs.exists(parentDir));
    assertTrue("Subdir exists", this.fs.exists(subdir));

    assertTrue("Deleted subdir", this.fs.delete(subdir, true));
    assertTrue("Parent should exist", this.fs.exists(parentDir));

    assertTrue("Deleted file", this.fs.delete(file, false));
    assertTrue("Parent should exist", this.fs.exists(parentDir));
  }


  @Override
  protected boolean renameSupported() {
    return true;
  }

  @Override
  public void testRenameNonExistentPath() throws Exception {
    if (this.renameSupported()) {
      Path src = this.path("/test/hadoop/path");
      Path dst = this.path("/test/new/newpath");
      try {
        super.rename(src, dst, false, false, false);
        fail("Should throw FileNotFoundException!");
      } catch (FileNotFoundException e) {
        // expected
      }
    }
  }

  @Override
  public void testRenameFileMoveToNonExistentDirectory() throws Exception {
    if (this.renameSupported()) {
      Path src = this.path("/test/hadoop/file");
      this.createFile(src);
      Path dst = this.path("/test/new/newfile");
      try {
        super.rename(src, dst, false, true, false);
        fail("Should throw FileNotFoundException!");
      } catch (FileNotFoundException e) {
        // expected
      }
    }
  }

  @Override
  public void testRenameDirectoryMoveToNonExistentDirectory() throws Exception {
    if (this.renameSupported()) {
      Path src = this.path("/test/hadoop/dir");
      this.fs.mkdirs(src);
      Path dst = this.path("/test/new/newdir");
      try {
        super.rename(src, dst, false, true, false);
        fail("Should throw FileNotFoundException!");
      } catch (FileNotFoundException e) {
        // expected
      }
    }
  }

  @Override
  public void testRenameFileMoveToExistingDirectory() throws Exception {
    super.testRenameFileMoveToExistingDirectory();
  }

  @Override
  public void testRenameFileAsExistingFile() throws Exception {
    if (this.renameSupported()) {
      Path src = this.path("/test/hadoop/file");
      this.createFile(src);
      Path dst = this.path("/test/new/newfile");
      this.createFile(dst);
      try {
        super.rename(src, dst, false, true, true);
        fail("Should throw FileAlreadyExistsException");
      } catch (FileAlreadyExistsException e) {
        // expected
      }
    }
  }

  @Override
  public void testRenameDirectoryAsExistingFile() throws Exception {
    if (this.renameSupported()) {
      Path src = this.path("/test/hadoop/dir");
      this.fs.mkdirs(src);
      Path dst = this.path("/test/new/newfile");
      this.createFile(dst);
      try {
        super.rename(src, dst, false, true, true);
        fail("Should throw FileAlreadyExistsException");
      } catch (FileAlreadyExistsException e) {
        // expected
      }
    }
  }

  public void testGetFileStatusFileAndDirectory() throws Exception {
    Path filePath = this.path("/test/oss/file1");
    this.createFile(filePath);
    assertTrue("Should be file", this.fs.getFileStatus(filePath).isFile());
    assertFalse("Should not be directory",
        this.fs.getFileStatus(filePath).isDirectory());

    Path dirPath = this.path("/test/oss/dir");
    this.fs.mkdirs(dirPath);
    assertTrue("Should be directory",
        this.fs.getFileStatus(dirPath).isDirectory());
    assertFalse("Should not be file", this.fs.getFileStatus(dirPath).isFile());
  }

  public void testMkdirsForExistingFile() throws Exception {
    Path testFile = this.path("/test/hadoop/file");
    assertFalse(this.fs.exists(testFile));
    this.createFile(testFile);
    assertTrue(this.fs.exists(testFile));
    try {
      this.fs.mkdirs(testFile);
      fail("Should throw FileAlreadyExistsException!");
    } catch (FileAlreadyExistsException e) {
      // expected
    }
  }

  public void testWorkingDirectory() throws Exception {
    Path workDir = super.path(this.getDefaultWorkingDirectory());
    assertEquals(workDir, this.fs.getWorkingDirectory());
    this.fs.setWorkingDirectory(super.path("."));
    assertEquals(workDir, this.fs.getWorkingDirectory());
    this.fs.setWorkingDirectory(super.path(".."));
    assertEquals(workDir.getParent(), this.fs.getWorkingDirectory());
    Path relativeDir = super.path("hadoop");
    this.fs.setWorkingDirectory(relativeDir);
    assertEquals(relativeDir, this.fs.getWorkingDirectory());
    Path absoluteDir = super.path("/test/hadoop");
    this.fs.setWorkingDirectory(absoluteDir);
    assertEquals(absoluteDir, this.fs.getWorkingDirectory());
  }

}
