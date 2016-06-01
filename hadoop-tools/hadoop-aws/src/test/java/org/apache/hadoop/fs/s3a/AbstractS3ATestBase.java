/*
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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;

/**
 * An extension of the contract test base set up for S3A tests.
 */
public abstract class AbstractS3ATestBase extends AbstractFSContractTestBase
    implements S3ATestConstants {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
    IOUtils.closeStream(getFileSystem());
  }

  @Rule
  public TestName methodName = new TestName();

  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit-" + methodName.getMethodName());
  }

  protected Configuration getConfiguration() {
    return getContract().getConf();
  }

  /**
   * Get the filesystem as an S3A filesystem.
   * @return the typecast FS
   */
  @Override
  public S3AFileSystem getFileSystem() {
    return (S3AFileSystem) super.getFileSystem();
  }

  /**
   * Write a file, read it back, validate the dataset. Overwrites the file
   * if it is present
   * @param name filename (will have the test path prepended to it)
   * @param len length of file
   * @return the full path to the file
   * @throws IOException any IO problem
   */
  protected Path writeThenReadFile(String name, int len) throws IOException {
    Path path = path(name);
    byte[] data = dataset(len, 'a', 'z');
    writeDataset(getFileSystem(), path, data, data.length, 1024 * 1024, true);
    ContractTestUtils.verifyFileContents(getFileSystem(), path, data);
    return path;
  }

  /**
   * Assert that an exception failed with a specific status code.
   * @param e exception
   * @param code expected status code
   * @throws AWSS3IOException rethrown if the status code does not match.
   */
  protected void assertStatusCode(AWSS3IOException e, int code)
      throws AWSS3IOException {
    if (e.getStatusCode() != code) {
      throw e;
    }
  }
}
