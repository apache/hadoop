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

package org.apache.hadoop.fs.azurebfs;

import org.junit.Ignore;

import org.apache.hadoop.fs.FSMainOperationsBaseTest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.contract.ABFSContractTestBinding;

/**
 * Test AzureBlobFileSystem main operations.
 * */
public class ITestAzureBlobFileSystemMainOperation extends FSMainOperationsBaseTest {

  private static final String TEST_ROOT_DIR =
          "/tmp/TestAzureBlobFileSystemMainOperations";

  private final ABFSContractTestBinding binding;

  public ITestAzureBlobFileSystemMainOperation () throws Exception {
    super(TEST_ROOT_DIR);
    // Note: There are shared resources in this test suite (eg: "test/new/newfile")
    // To make sure this test suite can be ran in parallel, different containers
    // will be used for each test.
    binding = new ABFSContractTestBinding(false);
  }

  @Override
  public void setUp() throws Exception {
    binding.setup();
    fSys = binding.getFileSystem();
  }

  @Override
  public void tearDown() throws Exception {
    // Note: Because "tearDown()" is called during the testing,
    // here we should not call binding.tearDown() to destroy the container.
    // Instead we should remove the test containers manually with
    // AbfsTestUtils.
    super.tearDown();
  }

  @Override
  protected FileSystem createFileSystem() throws Exception {
    return fSys;
  }

  @Override
  @Ignore("Permission check for getFileInfo doesn't match the HdfsPermissionsGuide")
  public void testListStatusThrowsExceptionForUnreadableDir() {
    // Permission Checks:
    // https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html
  }

  @Override
  @Ignore("Permission check for getFileInfo doesn't match the HdfsPermissionsGuide")
  public void testGlobStatusThrowsExceptionForUnreadableDir() {
    // Permission Checks:
    // https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html
  }
}
