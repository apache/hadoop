/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.getFileStatusEventually;
import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile;

/**
 * Test creating files, overwrite options etc.
 */
public abstract class AbstractContractCreateTest extends
                                                 AbstractFSContractTestBase {

  /**
   * How long to wait for a path to become visible.
   */
  public static final int CREATE_TIMEOUT = 15000;

  protected Path path(String filepath, boolean useBuilder) throws IOException {
    return super.path(filepath  + (useBuilder ? "" : "-builder"));
  }

  private void testCreateNewFile(boolean useBuilder) throws Throwable {
    describe("Foundational 'create a file' test, using builder API=" +
        useBuilder);
    Path path = path("testCreateNewFile", useBuilder);
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), path, data, data.length, 1024 * 1024, false,
        useBuilder);
    ContractTestUtils.verifyFileContents(getFileSystem(), path, data);
  }

  @Test
  public void testCreateNewFile() throws Throwable {
    testCreateNewFile(true);
    testCreateNewFile(false);
  }

  private void testCreateFileOverExistingFileNoOverwrite(boolean useBuilder)
      throws Throwable {
    describe("Verify overwriting an existing file fails, using builder API=" +
        useBuilder);
    Path path = path("testCreateFileOverExistingFileNoOverwrite", useBuilder);
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), path, data, data.length, 1024, false);
    byte[] data2 = dataset(10 * 1024, 'A', 'Z');
    try {
      writeDataset(getFileSystem(), path, data2, data2.length, 1024, false,
          useBuilder);
      fail("writing without overwrite unexpectedly succeeded");
    } catch (FileAlreadyExistsException expected) {
      //expected
      handleExpectedException(expected);
    } catch (IOException relaxed) {
      handleRelaxedException("Creating a file over a file with overwrite==false",
                             "FileAlreadyExistsException",
                             relaxed);
    }
  }

  @Test
  public void testCreateFileOverExistingFileNoOverwrite() throws Throwable {
    testCreateFileOverExistingFileNoOverwrite(false);
    testCreateFileOverExistingFileNoOverwrite(true);
  }

  private void testOverwriteExistingFile(boolean useBuilder) throws Throwable {
    describe("Overwrite an existing file and verify the new data is there, " +
        "use builder API=" + useBuilder);
    Path path = path("testOverwriteExistingFile", useBuilder);
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), path, data, data.length, 1024, false,
        useBuilder);
    ContractTestUtils.verifyFileContents(getFileSystem(), path, data);
    byte[] data2 = dataset(10 * 1024, 'A', 'Z');
    writeDataset(getFileSystem(), path, data2, data2.length, 1024, true,
        useBuilder);
    ContractTestUtils.verifyFileContents(getFileSystem(), path, data2);
  }

  /**
   * This test catches some eventual consistency problems that blobstores exhibit,
   * as we are implicitly verifying that updates are consistent. This
   * is why different file lengths and datasets are used
   * @throws Throwable
   */
  @Test
  public void testOverwriteExistingFile() throws Throwable {
    testOverwriteExistingFile(false);
    testOverwriteExistingFile(true);
  }

  private void testOverwriteEmptyDirectory(boolean useBuilder)
      throws Throwable {
    describe("verify trying to create a file over an empty dir fails, " +
        "use builder API=" + useBuilder);
    Path path = path("testOverwriteEmptyDirectory");
    mkdirs(path);
    assertIsDirectory(path);
    byte[] data = dataset(256, 'a', 'z');
    try {
      writeDataset(getFileSystem(), path, data, data.length, 1024, true,
          useBuilder);
      assertIsDirectory(path);
      fail("write of file over empty dir succeeded");
    } catch (FileAlreadyExistsException expected) {
      //expected
      handleExpectedException(expected);
    } catch (FileNotFoundException e) {
      handleRelaxedException("overwriting a dir with a file ",
                             "FileAlreadyExistsException",
                             e);
    } catch (IOException e) {
      handleRelaxedException("overwriting a dir with a file ",
                             "FileAlreadyExistsException",
                             e);
    }
    assertIsDirectory(path);
  }

  @Test
  public void testOverwriteEmptyDirectory() throws Throwable {
    testOverwriteEmptyDirectory(false);
    testOverwriteEmptyDirectory(true);
  }

  private void testOverwriteNonEmptyDirectory(boolean useBuilder)
      throws Throwable {
    describe("verify trying to create a file over a non-empty dir fails, " +
        "use builder API=" + useBuilder);
    Path path = path("testOverwriteNonEmptyDirectory");
    mkdirs(path);
    try {
      assertIsDirectory(path);
    } catch (AssertionError failure) {
      if (isSupported(CREATE_OVERWRITES_DIRECTORY)) {
        // file/directory hack surfaces here
        throw new AssumptionViolatedException(failure.toString(), failure);
      }
      // else: rethrow
      throw failure;
    }
    Path child = new Path(path, "child");
    writeTextFile(getFileSystem(), child, "child file", true);
    byte[] data = dataset(256, 'a', 'z');
    try {
      writeDataset(getFileSystem(), path, data, data.length, 1024,
                   true, useBuilder);
      FileStatus status = getFileSystem().getFileStatus(path);

      boolean isDir = status.isDirectory();
      if (!isDir && isSupported(CREATE_OVERWRITES_DIRECTORY)) {
        // For some file systems, downgrade to a skip so that the failure is
        // visible in test results.
        skip("This Filesystem allows a file to overwrite a directory");
      }
      fail("write of file over dir succeeded");
    } catch (FileAlreadyExistsException expected) {
      //expected
      handleExpectedException(expected);
    } catch (FileNotFoundException e) {
      handleRelaxedException("overwriting a dir with a file ",
                             "FileAlreadyExistsException",
                             e);
    } catch (IOException e) {
      handleRelaxedException("overwriting a dir with a file ",
                             "FileAlreadyExistsException",
                             e);
    }
    assertIsDirectory(path);
    assertIsFile(child);
  }

  @Test
  public void testOverwriteNonEmptyDirectory() throws Throwable {
    testOverwriteNonEmptyDirectory(false);
    testOverwriteNonEmptyDirectory(true);
  }

  @Test
  public void testCreatedFileIsImmediatelyVisible() throws Throwable {
    describe("verify that a newly created file exists as soon as open returns");
    Path path = path("testCreatedFileIsImmediatelyVisible");
    try(FSDataOutputStream out = getFileSystem().create(path,
                                   false,
                                   4096,
                                   (short) 1,
                                   1024)) {
      if (!getFileSystem().exists(path)) {

        if (isSupported(CREATE_VISIBILITY_DELAYED)) {
          // For some file systems, downgrade to a skip so that the failure is
          // visible in test results.
          skip("This Filesystem delays visibility of newly created files");
        }
        assertPathExists("expected path to be visible before anything written",
                         path);
      }
    }
  }

  @Test
  public void testCreatedFileIsVisibleOnFlush() throws Throwable {
    describe("verify that a newly created file exists once a flush has taken "
        + "place");
    Path path = path("testCreatedFileIsVisibleOnFlush");
    FileSystem fs = getFileSystem();
    try(FSDataOutputStream out = fs.create(path,
          false,
          4096,
          (short) 1,
          1024)) {
      out.write('a');
      out.flush();
      if (!fs.exists(path)) {
        if (isSupported(IS_BLOBSTORE) ||
            isSupported(CREATE_VISIBILITY_DELAYED)) {
          // object store or some file systems: downgrade to a skip so that the
          // failure is visible in test results
          skip("For object store or some file systems, newly created files are"
              + " not immediately visible");
        }
        assertPathExists("expected path to be visible before file closed",
            path);
      }
    }
  }

  @Test
  public void testCreatedFileIsEventuallyVisible() throws Throwable {
    describe("verify a written to file is visible after the stream is closed");
    Path path = path("testCreatedFileIsEventuallyVisible");
    FileSystem fs = getFileSystem();
    try(
      FSDataOutputStream out = fs.create(path,
          false,
          4096,
          (short) 1,
          1024)
      ) {
      out.write(0x01);
      out.close();
      getFileStatusEventually(fs, path, CREATE_TIMEOUT);
    }
  }

  @Test
  public void testFileStatusBlocksizeNonEmptyFile() throws Throwable {
    describe("validate the block size of a filesystem and files within it");
    FileSystem fs = getFileSystem();

    long rootPath = fs.getDefaultBlockSize(path("/"));
    assertTrue("Root block size is invalid " + rootPath,
        rootPath > 0);

    Path path = path("testFileStatusBlocksizeNonEmptyFile");
    byte[] data = dataset(256, 'a', 'z');

    writeDataset(fs, path, data, data.length, 1024 * 1024, false);

    validateBlockSize(fs, path, 1);
  }

  @Test
  public void testFileStatusBlocksizeEmptyFile() throws Throwable {
    describe("check that an empty file may return a 0-byte blocksize");
    FileSystem fs = getFileSystem();
    Path path = path("testFileStatusBlocksizeEmptyFile");
    ContractTestUtils.touch(fs, path);
    validateBlockSize(fs, path, 0);
  }

  private void validateBlockSize(FileSystem fs, Path path, int minValue)
      throws IOException, InterruptedException {
    FileStatus status =
        getFileStatusEventually(fs, path, CREATE_TIMEOUT);
    String statusDetails = status.toString();
    assertTrue("File status block size too low:  " + statusDetails
            + " min value: " + minValue,
        status.getBlockSize() >= minValue);
    long defaultBlockSize = fs.getDefaultBlockSize(path);
    assertTrue("fs.getDefaultBlockSize(" + path + ") size " +
            defaultBlockSize + " is below the minimum of " + minValue,
        defaultBlockSize >= minValue);
  }

  @Test
  public void testCreateMakesParentDirs() throws Throwable {
    describe("check that after creating a file its parent directories exist");
    FileSystem fs = getFileSystem();
    Path grandparent = path("testCreateCreatesAndPopulatesParents");
    Path parent = new Path(grandparent, "parent");
    Path child = new Path(parent, "child");
    touch(fs, child);
    assertEquals("List status of parent should include the 1 child file",
        1, fs.listStatus(parent).length);
    assertTrue("Parent directory does not appear to be a directory",
        fs.getFileStatus(parent).isDirectory());
    assertEquals("List status of grandparent should include the 1 parent dir",
        1, fs.listStatus(grandparent).length);
    assertTrue("Grandparent directory does not appear to be a directory",
        fs.getFileStatus(grandparent).isDirectory());
  }
}
