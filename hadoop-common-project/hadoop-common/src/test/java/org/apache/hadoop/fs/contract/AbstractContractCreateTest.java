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

  @Test
  public void testCreateNewFile() throws Throwable {
    describe("Foundational 'create a file' test");
    Path path = path("testCreateNewFile");
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), path, data, data.length, 1024 * 1024, false);
    ContractTestUtils.verifyFileContents(getFileSystem(), path, data);
  }

  @Test
  public void testCreateFileOverExistingFileNoOverwrite() throws Throwable {
    describe("Verify overwriting an existing file fails");
    Path path = path("testCreateFileOverExistingFileNoOverwrite");
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), path, data, data.length, 1024, false);
    byte[] data2 = dataset(10 * 1024, 'A', 'Z');
    try {
      writeDataset(getFileSystem(), path, data2, data2.length, 1024, false);
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

  /**
   * This test catches some eventual consistency problems that blobstores exhibit,
   * as we are implicitly verifying that updates are consistent. This
   * is why different file lengths and datasets are used
   * @throws Throwable
   */
  @Test
  public void testOverwriteExistingFile() throws Throwable {
    describe("Overwrite an existing file and verify the new data is there");
    Path path = path("testOverwriteExistingFile");
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), path, data, data.length, 1024, false);
    ContractTestUtils.verifyFileContents(getFileSystem(), path, data);
    byte[] data2 = dataset(10 * 1024, 'A', 'Z');
    writeDataset(getFileSystem(), path, data2, data2.length, 1024, true);
    ContractTestUtils.verifyFileContents(getFileSystem(), path, data2);
  }

  @Test
  public void testOverwriteEmptyDirectory() throws Throwable {
    describe("verify trying to create a file over an empty dir fails");
    Path path = path("testOverwriteEmptyDirectory");
    mkdirs(path);
    assertIsDirectory(path);
    byte[] data = dataset(256, 'a', 'z');
    try {
      writeDataset(getFileSystem(), path, data, data.length, 1024, true);
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
  public void testOverwriteNonEmptyDirectory() throws Throwable {
    describe("verify trying to create a file over a non-empty dir fails");
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
                   true);
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

        if (isSupported(IS_BLOBSTORE)) {
          // object store: downgrade to a skip so that the failure is visible
          // in test results
          skip("Filesystem is an object store and newly created files are not "
              + "immediately visible");
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

}
