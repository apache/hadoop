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

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;

/**
 * Test directory operations
 */
public abstract class AbstractContractMkdirTest extends AbstractFSContractTestBase {

  @Test
  public void testMkDirRmDir() throws Throwable {
    FileSystem fs = getFileSystem();

    Path dir = path("testMkDirRmDir");
    assertPathDoesNotExist("directory already exists", dir);
    fs.mkdirs(dir);
    assertPathExists("mkdir failed", dir);
    assertDeleted(dir, false);
  }

  @Test
  public void testMkDirRmRfDir() throws Throwable {
    describe("create a directory then recursive delete it");
    FileSystem fs = getFileSystem();
    Path dir = path("testMkDirRmRfDir");
    assertPathDoesNotExist("directory already exists", dir);
    fs.mkdirs(dir);
    assertPathExists("mkdir failed", dir);
    assertDeleted(dir, true);
  }

  @Test
  public void testNoMkdirOverFile() throws Throwable {
    describe("try to mkdir over a file");
    FileSystem fs = getFileSystem();
    Path path = path("testNoMkdirOverFile");
    byte[] dataset = dataset(1024, ' ', 'z');
    createFile(getFileSystem(), path, false, dataset);
    try {
      boolean made = fs.mkdirs(path);
      fail("mkdirs did not fail over a file but returned " + made
            + "; " + ls(path));
    } catch (ParentNotDirectoryException | FileAlreadyExistsException e) {
      //parent is a directory
      handleExpectedException(e);
    } catch (IOException e) {
      //here the FS says "no create"
      handleRelaxedException("mkdirs", "FileAlreadyExistsException", e);
    }
    assertIsFile(path);
    byte[] bytes = ContractTestUtils.readDataset(getFileSystem(), path,
                                                 dataset.length);
    ContractTestUtils.compareByteArrays(dataset, bytes, dataset.length);
    assertPathExists("mkdir failed", path);
    assertDeleted(path, true);
  }

  @Test
  public void testMkdirOverParentFile() throws Throwable {
    describe("try to mkdir where a parent is a file");
    FileSystem fs = getFileSystem();
    Path path = path("testMkdirOverParentFile");
    byte[] dataset = dataset(1024, ' ', 'z');
    createFile(getFileSystem(), path, false, dataset);
    Path child = new Path(path,"child-to-mkdir");
    try {
      boolean made = fs.mkdirs(child);
      fail("mkdirs did not fail over a file but returned " + made
           + "; " + ls(path));
    } catch (ParentNotDirectoryException | FileAlreadyExistsException e) {
      //parent is a directory
      handleExpectedException(e);
    } catch (IOException e) {
      handleRelaxedException("mkdirs", "ParentNotDirectoryException", e);
    }
    assertIsFile(path);
    byte[] bytes = ContractTestUtils.readDataset(getFileSystem(), path,
                                                 dataset.length);
    ContractTestUtils.compareByteArrays(dataset, bytes, dataset.length);
    assertPathExists("mkdir failed", path);
    assertDeleted(path, true);
  }

  @Test
  public void testMkdirSlashHandling() throws Throwable {
    describe("verify mkdir slash handling");
    FileSystem fs = getFileSystem();

    // No trailing slash
    assertTrue(fs.mkdirs(path("testmkdir/a")));
    assertPathExists("mkdir without trailing slash failed",
        path("testmkdir/a"));

    // With trailing slash
    assertTrue(fs.mkdirs(path("testmkdir/b/")));
    assertPathExists("mkdir with trailing slash failed", path("testmkdir/b/"));

    // Mismatched slashes
    assertPathExists("check path existence without trailing slash failed",
        path("testmkdir/b"));
  }
}
