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

package org.apache.hadoop.fs.contract.s3a;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractMkdirTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.setPerformanceFlags;

/**
 * Test mkdir operations on S3A with create performance mode.
 */
public class ITestS3AContractMkdirWithCreatePerf extends AbstractContractMkdirTest {

  @Override
  protected Configuration createConfiguration() {
    return setPerformanceFlags(
        super.createConfiguration(),
        "create,mkdir");
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  @Test
  public void testMkdirOverParentFile() throws Throwable {
    describe("try to mkdir where a parent is a file, should pass");
    FileSystem fs = getFileSystem();
    Path path = methodPath();
    byte[] dataset = dataset(1024, ' ', 'z');
    createFile(getFileSystem(), path, false, dataset);
    Path child = new Path(path, "child-to-mkdir");
    boolean childCreated = fs.mkdirs(child);
    assertTrue("Child dir is created", childCreated);
    assertIsFile(path);
    byte[] bytes = ContractTestUtils.readDataset(getFileSystem(), path, dataset.length);
    ContractTestUtils.compareByteArrays(dataset, bytes, dataset.length);
    assertPathExists("mkdir failed", child);
    assertDeleted(child, true);
  }

}
