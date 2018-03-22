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
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;

/**
 * Test append -if supported
 */
public abstract class AbstractContractAppendTest extends AbstractFSContractTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractContractAppendTest.class);

  private Path testPath;
  private Path target;

  @Override
  public void setup() throws Exception {
    super.setup();
    skipIfUnsupported(SUPPORTS_APPEND);

    //delete the test directory
    testPath = path("test");
    target = new Path(testPath, "target");
  }

  @Test
  public void testAppendToEmptyFile() throws Throwable {
    touch(getFileSystem(), target);
    byte[] dataset = dataset(256, 'a', 'z');
    try (FSDataOutputStream outputStream = getFileSystem().append(target)) {
      outputStream.write(dataset);
    }
    byte[] bytes = ContractTestUtils.readDataset(getFileSystem(), target,
                                                 dataset.length);
    ContractTestUtils.compareByteArrays(dataset, bytes, dataset.length);
  }

  @Test
  public void testBuilderAppendToEmptyFile() throws Throwable {
    touch(getFileSystem(), target);
    byte[] dataset = dataset(256, 'a', 'z');
    try (FSDataOutputStream outputStream =
             getFileSystem().appendFile(target).build()) {
      outputStream.write(dataset);
    }
    byte[] bytes = ContractTestUtils.readDataset(getFileSystem(), target,
        dataset.length);
    ContractTestUtils.compareByteArrays(dataset, bytes, dataset.length);
  }

  @Test
  public void testAppendNonexistentFile() throws Throwable {
    try {
      FSDataOutputStream out = getFileSystem().append(target);
      //got here: trouble
      out.close();
      fail("expected a failure");
    } catch (Exception e) {
      //expected
      handleExpectedException(e);
    }
  }

  @Test
  public void testAppendToExistingFile() throws Throwable {
    byte[] original = dataset(8192, 'A', 'Z');
    byte[] appended = dataset(8192, '0', '9');
    createFile(getFileSystem(), target, false, original);
    try (FSDataOutputStream out = getFileSystem().append(target)) {
      out.write(appended);
    }
    byte[] bytes = ContractTestUtils.readDataset(getFileSystem(), target,
                                                 original.length + appended.length);
    ContractTestUtils.validateFileContent(bytes,
            new byte[] [] { original, appended });
  }

  @Test
  public void testBuilderAppendToExistingFile() throws Throwable {
    byte[] original = dataset(8192, 'A', 'Z');
    byte[] appended = dataset(8192, '0', '9');
    createFile(getFileSystem(), target, false, original);
    try (FSDataOutputStream out = getFileSystem().appendFile(target).build()) {
      out.write(appended);
    }
    byte[] bytes = ContractTestUtils.readDataset(getFileSystem(), target,
        original.length + appended.length);
    ContractTestUtils.validateFileContent(bytes,
        new byte[][]{original, appended});
  }

  @Test
  public void testAppendMissingTarget() throws Throwable {
    try {
      FSDataOutputStream out = getFileSystem().append(target);
      //got here: trouble
      out.close();
      fail("expected a failure");
    } catch (Exception e) {
      //expected
      handleExpectedException(e);
    }
  }

  @Test
  public void testRenameFileBeingAppended() throws Throwable {
    touch(getFileSystem(), target);
    assertPathExists("original file does not exist", target);
    byte[] dataset = dataset(256, 'a', 'z');
    FSDataOutputStream outputStream = getFileSystem().append(target);
    outputStream.write(dataset);
    Path renamed = new Path(testPath, "renamed");
    rename(target, renamed);
    outputStream.close();
    String listing = ls(testPath);

    //expected: the stream goes to the file that was being renamed, not
    //the original path
    assertPathExists("renamed destination file does not exist", renamed);

    assertPathDoesNotExist("Source file found after rename during append:\n" +
                           listing, target);
    byte[] bytes = ContractTestUtils.readDataset(getFileSystem(), renamed,
                                                 dataset.length);
    ContractTestUtils.compareByteArrays(dataset, bytes, dataset.length);
  }
}
