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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;

/**
 * This class does things to the root directory.
 * Only subclass this for tests against transient filesystems where
 * you don't care about the data.
 */
public abstract class AbstractContractRootDirectoryTest extends AbstractFSContractTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractContractRootDirectoryTest.class);

  @Override
  public void setup() throws Exception {
    super.setup();
    skipIfUnsupported(TEST_ROOT_TESTS_ENABLED);
  }

  @Test
  public void testMkDirDepth1() throws Throwable {
    FileSystem fs = getFileSystem();
    Path dir = new Path("/testmkdirdepth1");
    assertPathDoesNotExist("directory already exists", dir);
    fs.mkdirs(dir);
    assertIsDirectory(dir);
    assertPathExists("directory already exists", dir);
    assertDeleted(dir, true);
  }

  @Test
  public void testRmEmptyRootDirNonRecursive() throws Throwable {
    //extra sanity checks here to avoid support calls about complete loss of data
    skipIfUnsupported(TEST_ROOT_TESTS_ENABLED);
    Path root = new Path("/");
    assertIsDirectory(root);
    boolean deleted = getFileSystem().delete(root, true);
    LOG.info("rm / of empty dir result is {}", deleted);
    assertIsDirectory(root);
  }

  @Test
  public void testRmNonEmptyRootDirNonRecursive() throws Throwable {
    //extra sanity checks here to avoid support calls about complete loss of data
    skipIfUnsupported(TEST_ROOT_TESTS_ENABLED);
    Path root = new Path("/");
    String touchfile = "/testRmNonEmptyRootDirNonRecursive";
    Path file = new Path(touchfile);
    ContractTestUtils.touch(getFileSystem(), file);
    assertIsDirectory(root);
    try {
      boolean deleted = getFileSystem().delete(root, false);
      fail("non recursive delete should have raised an exception," +
           " but completed with exit code " + deleted);
    } catch (IOException e) {
      //expected
      handleExpectedException(e);
    } finally {
      getFileSystem().delete(file, false);
    }
    assertIsDirectory(root);
  }

  @Test
  public void testRmRootRecursive() throws Throwable {
    //extra sanity checks here to avoid support calls about complete loss of data
    skipIfUnsupported(TEST_ROOT_TESTS_ENABLED);
    Path root = new Path("/");
    assertIsDirectory(root);
    Path file = new Path("/testRmRootRecursive");
    ContractTestUtils.touch(getFileSystem(), file);
    boolean deleted = getFileSystem().delete(root, true);
    assertIsDirectory(root);
    LOG.info("rm -rf / result is {}", deleted);
    if (deleted) {
      assertPathDoesNotExist("expected file to be deleted", file);
    } else {
      assertPathExists("expected file to be preserved", file);;
    }
  }

  @Test
  public void testCreateFileOverRoot() throws Throwable {
    Path root = new Path("/");
    byte[] dataset = dataset(1024, ' ', 'z');
    try {
      createFile(getFileSystem(), root, false, dataset);
      fail("expected an exception, got a file created over root: " + ls(root));
    } catch (IOException e) {
      //expected
      handleExpectedException(e);
    }
    assertIsDirectory(root);
  }

  @Test
  public void testListEmptyRootDirectory() throws IOException {
    //extra sanity checks here to avoid support calls about complete loss of data
    skipIfUnsupported(TEST_ROOT_TESTS_ENABLED);
    FileSystem fs = getFileSystem();
    Path root = new Path("/");
    FileStatus[] statuses = fs.listStatus(root);
    for (FileStatus status : statuses) {
      ContractTestUtils.assertDeleted(fs, status.getPath(), true);
    }
    assertEquals("listStatus on empty root-directory returned a non-empty list",
        0, fs.listStatus(root).length);
  }
}
