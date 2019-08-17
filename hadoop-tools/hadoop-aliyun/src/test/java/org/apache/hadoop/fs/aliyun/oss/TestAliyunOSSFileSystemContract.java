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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

/**
 * Tests a live Aliyun OSS system.
 */
public class TestAliyunOSSFileSystemContract
    extends FileSystemContractBaseTest {
  public static final String TEST_FS_OSS_NAME = "test.fs.oss.name";
  private static Path testRootPath =
      new Path(AliyunOSSTestUtils.generateUniqueTestPath());

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    fs = AliyunOSSTestUtils.createTestFileSystem(conf);
    assumeNotNull(fs);
  }

  @Override
  public Path getTestBaseDir() {
    return testRootPath;
  }

  @Test
  public void testMkdirsWithUmask() throws Exception {
    // not supported
  }

  @Test
  public void testRootDirAlwaysExists() throws Exception {
    //this will throw an exception if the path is not found
    fs.getFileStatus(super.path("/"));
    //this catches overrides of the base exists() method that don't
    //use getFileStatus() as an existence probe
    assertTrue("FileSystem.exists() fails for root",
        fs.exists(super.path("/")));
  }

  @Test
  public void testRenameRootDirForbidden() throws Exception {
    assumeTrue(renameSupported());
    rename(super.path("/"),
           super.path("/test/newRootDir"),
           false, true, false);
  }

  @Test
  public void testListStatus() throws IOException {
    Path file = this.path("/test/hadoop/file");
    this.createFile(file);
    assertTrue("File exists", this.fs.exists(file));
    FileStatus fs = this.fs.getFileStatus(file);
    assertEquals(fs.getOwner(),
        UserGroupInformation.getCurrentUser().getShortUserName());
    assertEquals(fs.getGroup(),
        UserGroupInformation.getCurrentUser().getShortUserName());
  }

  @Test
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

  @Test
  public void testRenameNonExistentPath() throws Exception {
    assumeTrue(renameSupported());
    Path src = this.path("/test/hadoop/path");
    Path dst = this.path("/test/new/newpath");
    try {
      super.rename(src, dst, false, false, false);
      fail("Should throw FileNotFoundException!");
    } catch (FileNotFoundException e) {
      // expected
    }
  }

  @Test
  public void testRenameFileMoveToNonExistentDirectory() throws Exception {
    assumeTrue(renameSupported());
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

  @Test
  public void testRenameDirectoryConcurrent() throws Exception {
    assumeTrue(renameSupported());
    Path src = this.path("/test/hadoop/file/");
    Path child1 = this.path("/test/hadoop/file/1");
    Path child2 = this.path("/test/hadoop/file/2");
    Path child3 = this.path("/test/hadoop/file/3");
    Path child4 = this.path("/test/hadoop/file/4");

    this.createFile(child1);
    this.createFile(child2);
    this.createFile(child3);
    this.createFile(child4);

    Path dst = this.path("/test/new");
    super.rename(src, dst, true, false, true);
    assertEquals(4, this.fs.listStatus(dst).length);
  }

  @Test
  public void testRenameDirectoryCopyTaskAllSucceed() throws Exception {
    assumeTrue(renameSupported());
    Path srcOne = this.path("/test/hadoop/file/1");
    this.createFile(srcOne);

    Path dstOne = this.path("/test/new/file/1");
    Path dstTwo = this.path("/test/new/file/2");
    AliyunOSSCopyFileContext copyFileContext = new AliyunOSSCopyFileContext();
    AliyunOSSFileSystemStore store = ((AliyunOSSFileSystem)this.fs).getStore();
    store.storeEmptyFile("test/new/file/");
    AliyunOSSCopyFileTask oneCopyFileTask = new AliyunOSSCopyFileTask(
        store, srcOne.toUri().getPath().substring(1), data.length,
        dstOne.toUri().getPath().substring(1), copyFileContext);
    oneCopyFileTask.run();
    assumeFalse(copyFileContext.isCopyFailure());

    AliyunOSSCopyFileTask twoCopyFileTask = new AliyunOSSCopyFileTask(
        store, srcOne.toUri().getPath().substring(1), data.length,
        dstTwo.toUri().getPath().substring(1), copyFileContext);
    twoCopyFileTask.run();
    assumeFalse(copyFileContext.isCopyFailure());

    copyFileContext.lock();
    try {
      copyFileContext.awaitAllFinish(2);
    } catch (InterruptedException e) {
      throw new Exception(e);
    } finally {
      copyFileContext.unlock();
    }
    assumeFalse(copyFileContext.isCopyFailure());
  }

  @Test
  public void testRenameDirectoryCopyTaskAllFailed() throws Exception {
    assumeTrue(renameSupported());
    Path srcOne = this.path("/test/hadoop/file/1");
    this.createFile(srcOne);

    Path dstOne = new Path("1");
    Path dstTwo = new Path("2");
    AliyunOSSCopyFileContext copyFileContext = new AliyunOSSCopyFileContext();
    AliyunOSSFileSystemStore store = ((AliyunOSSFileSystem)this.fs).getStore();
    //store.storeEmptyFile("test/new/file/");
    AliyunOSSCopyFileTask oneCopyFileTask = new AliyunOSSCopyFileTask(
        store, srcOne.toUri().getPath().substring(1), data.length,
        dstOne.toUri().getPath().substring(1), copyFileContext);
    oneCopyFileTask.run();
    assumeTrue(copyFileContext.isCopyFailure());

    AliyunOSSCopyFileTask twoCopyFileTask = new AliyunOSSCopyFileTask(
        store, srcOne.toUri().getPath().substring(1), data.length,
        dstTwo.toUri().getPath().substring(1), copyFileContext);
    twoCopyFileTask.run();
    assumeTrue(copyFileContext.isCopyFailure());

    copyFileContext.lock();
    try {
      copyFileContext.awaitAllFinish(2);
    } catch (InterruptedException e) {
      throw new Exception(e);
    } finally {
      copyFileContext.unlock();
    }
    assumeTrue(copyFileContext.isCopyFailure());
  }

  @Test
  public void testRenameDirectoryCopyTaskPartialFailed() throws Exception {
    assumeTrue(renameSupported());
    Path srcOne = this.path("/test/hadoop/file/1");
    this.createFile(srcOne);

    Path dstOne = new Path("1");
    Path dstTwo = new Path("/test/new/file/2");
    Path dstThree = new Path("3");
    AliyunOSSCopyFileContext copyFileContext = new AliyunOSSCopyFileContext();
    AliyunOSSFileSystemStore store = ((AliyunOSSFileSystem)this.fs).getStore();
    //store.storeEmptyFile("test/new/file/");
    AliyunOSSCopyFileTask oneCopyFileTask = new AliyunOSSCopyFileTask(
        store, srcOne.toUri().getPath().substring(1), data.length,
        dstOne.toUri().getPath().substring(1), copyFileContext);
    oneCopyFileTask.run();
    assumeTrue(copyFileContext.isCopyFailure());

    AliyunOSSCopyFileTask twoCopyFileTask = new AliyunOSSCopyFileTask(
        store, srcOne.toUri().getPath().substring(1), data.length,
        dstTwo.toUri().getPath().substring(1), copyFileContext);
    twoCopyFileTask.run();
    assumeTrue(copyFileContext.isCopyFailure());

    AliyunOSSCopyFileTask threeCopyFileTask = new AliyunOSSCopyFileTask(
        store, srcOne.toUri().getPath().substring(1), data.length,
        dstThree.toUri().getPath().substring(1), copyFileContext);
    threeCopyFileTask.run();
    assumeTrue(copyFileContext.isCopyFailure());

    copyFileContext.lock();
    try {
      copyFileContext.awaitAllFinish(3);
    } catch (InterruptedException e) {
      throw new Exception(e);
    } finally {
      copyFileContext.unlock();
    }
    assumeTrue(copyFileContext.isCopyFailure());
  }

  @Test
  public void testRenameDirectoryMoveToNonExistentDirectory() throws Exception {
    assumeTrue(renameSupported());
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

  @Test
  public void testRenameFileMoveToExistingDirectory() throws Exception {
    super.testRenameFileMoveToExistingDirectory();
  }

  @Test
  public void testRenameFileAsExistingFile() throws Exception {
    assumeTrue(renameSupported());
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

  @Test
  public void testRenameDirectoryAsExistingFile() throws Exception {
    assumeTrue(renameSupported());
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

  @Test
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

    Path parentPath = this.path("/test/oss");
    for (FileStatus fileStatus: fs.listStatus(parentPath)) {
      assertTrue("file and directory should be new",
          fileStatus.getModificationTime() > 0L);
    }
  }

  @Test
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

  @Test
  public void testRenameChangingDirShouldFail() throws Exception {
    testRenameDir(true, false, false);
    testRenameDir(true, true, true);
  }

  @Test
  public void testRenameDir() throws Exception {
    testRenameDir(false, true, false);
    testRenameDir(false, true, true);
  }

  private void testRenameDir(boolean changing, boolean result, boolean empty)
      throws Exception {
    fs.getConf().setLong(Constants.FS_OSS_BLOCK_SIZE_KEY, 1024);
    String key = "a/b/test.file";
    for (int i = 0; i < 100; i++) {
      if (empty) {
        fs.createNewFile(this.path(key + "." + i));
      } else {
        createFile(this.path(key + "." + i));
      }
    }

    Path srcPath = this.path("a");
    Path dstPath = this.path("b");
    TestRenameTask task = new TestRenameTask(fs, srcPath, dstPath);
    Thread thread = new Thread(task);
    thread.start();
    while (!task.isRunning()) {
      Thread.sleep(1000);
    }

    if (changing) {
      fs.delete(this.path("a/b"), true);
    }

    thread.join();
    assertEquals(result, task.isSucceed());
  }

  class TestRenameTask implements Runnable {
    private FileSystem fs;
    private Path srcPath;
    private Path dstPath;
    private boolean result;
    private boolean running;
    TestRenameTask(FileSystem fs, Path srcPath, Path dstPath) {
      this.fs = fs;
      this.srcPath = srcPath;
      this.dstPath = dstPath;
      this.result = false;
      this.running = false;
    }

    boolean isSucceed() {
      return this.result;
    }

    boolean isRunning() {
      return this.running;
    }
    @Override
    public void run() {
      try {
        running = true;
        result = fs.rename(srcPath, dstPath);
      } catch (Exception e) {
      }
    }
  }

  protected int getGlobalTimeout() {
    return 120 * 1000;
  }
}
