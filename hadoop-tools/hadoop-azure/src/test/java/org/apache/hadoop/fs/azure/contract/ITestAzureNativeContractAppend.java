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

package org.apache.hadoop.fs.azure.contract;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractAppendTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Append test, skipping one of them.
 */

public class ITestAzureNativeContractAppend extends AbstractContractAppendTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new NativeAzureFileSystemContract(conf);
  }

  @Override
  public void testRenameFileBeingAppended() throws Throwable {
    skip("Skipping as renaming an opened file is not supported");
  }

  /**
   * Wasb returns a different exception, so change the intercept logic here.
   */
  @Override
  @Test
  public void testAppendDirectory() throws Exception {
    final FileSystem fs = getFileSystem();

    final Path folderPath = path("testAppendDirectory");
    fs.mkdirs(folderPath);
    intercept(FileNotFoundException.class,
        () -> fs.append(folderPath));
  }
}
