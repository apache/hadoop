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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile;

/**
 * Test creating files, overwrite options &c
 */
public abstract class AbstractContractCreateTest extends
                                                 AbstractFSContractTestBase {

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
      if (isSupported(IS_BLOBSTORE)) {
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
      if (!isDir && isSupported(IS_BLOBSTORE)) {
        // object store: downgrade to a skip so that the failure is visible
        // in test results
        skip("Object store allows a file to overwrite a directory");
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

        if (isSupported(IS_BLOBSTORE)) {
          // object store: downgrade to a skip so that the failure is visible
          // in test results
          skip("Filesystem is an object store and newly created files are not immediately visible");
        }
        assertPathExists("expected path to be visible before anything written",
                         path);
      }
    }
  }
}
