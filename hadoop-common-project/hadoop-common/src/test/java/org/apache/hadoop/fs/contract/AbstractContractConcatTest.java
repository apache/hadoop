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

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertFileHasLength;
import static org.apache.hadoop.fs.contract.ContractTestUtils.cleanup;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;

/**
 * Test concat -if supported
 */
public abstract class AbstractContractConcatTest extends AbstractFSContractTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractContractConcatTest.class);

  private Path testPath;
  private Path srcFile;
  private Path zeroByteFile;
  private Path target;

  @Override
  public void setup() throws Exception {
    super.setup();
    skipIfUnsupported(SUPPORTS_CONCAT);

    //delete the test directory
    testPath = path("test");
    srcFile = new Path(testPath, "small.txt");
    zeroByteFile = new Path(testPath, "zero.txt");
    target = new Path(testPath, "target");

    byte[] block = dataset(TEST_FILE_LEN, 0, 255);
    createFile(getFileSystem(), srcFile, true, block);
    touch(getFileSystem(), zeroByteFile);
  }

  @Test
  public void testConcatEmptyFiles() throws Throwable {
    touch(getFileSystem(), target);
    try {
      getFileSystem().concat(target, new Path[0]);
      fail("expected a failure");
    } catch (Exception e) {
      //expected
      handleExpectedException(e);
    }
  }

  @Test
  public void testConcatMissingTarget() throws Throwable {
    try {
      getFileSystem().concat(target,
                             new Path[] { zeroByteFile});
      fail("expected a failure");
    } catch (Exception e) {
      //expected
      handleExpectedException(e);
    }
  }

  @Test
  public void testConcatFileOnFile() throws Throwable {
    byte[] block = dataset(TEST_FILE_LEN, 0, 255);
    createFile(getFileSystem(), target, false, block);
    getFileSystem().concat(target,
                           new Path[] {srcFile});
    assertFileHasLength(getFileSystem(), target, TEST_FILE_LEN *2);
    ContractTestUtils.validateFileContent(
      ContractTestUtils.readDataset(getFileSystem(),
                                    target, TEST_FILE_LEN * 2),
      new byte[][]{block, block});
  }

  @Test
  public void testConcatOnSelf() throws Throwable {
    byte[] block = dataset(TEST_FILE_LEN, 0, 255);
    createFile(getFileSystem(), target, false, block);
    try {
      getFileSystem().concat(target,
                             new Path[]{target});
    } catch (Exception e) {
      //expected
      handleExpectedException(e);
    }
  }



}
