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

package org.apache.hadoop.fs.azure;

import static org.assertj.core.api.Assumptions.assumeThat;

import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.integration.AzureTestUtils;
import org.apache.hadoop.test.TestName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Run the {@code FileSystemContractBaseTest} tests against the emulator
 */
public class ITestNativeAzureFileSystemContractEmulator extends
    FileSystemContractBaseTest {
  private AzureBlobStorageTestAccount testAccount;
  private Path basePath;

  @RegisterExtension
  private TestName methodName = new TestName();

  private void nameThread() {
    Thread.currentThread().setName("JUnit-" + methodName.getMethodName());
  }

  @BeforeEach
  public void setUp() throws Exception {
    nameThread();
    testAccount = AzureBlobStorageTestAccount.createForEmulator();
    if (testAccount != null) {
      fs = testAccount.getFileSystem();
    }
    assumeThat(fs)
        .as("FileSystem must not be null for this test")
        .isNotNull();
    basePath = fs.makeQualified(
        AzureTestUtils.createTestPath(
            new Path("ITestNativeAzureFileSystemContractEmulator")));
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    testAccount = AzureTestUtils.cleanup(testAccount);
    fs = null;
  }
}
