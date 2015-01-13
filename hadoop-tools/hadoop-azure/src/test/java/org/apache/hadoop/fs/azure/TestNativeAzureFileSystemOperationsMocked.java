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

import static org.junit.Assume.assumeTrue;

import org.apache.hadoop.fs.FSMainOperationsBaseTest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestNativeAzureFileSystemOperationsMocked extends
    FSMainOperationsBaseTest {

  private static final String TEST_ROOT_DIR =
      "/tmp/TestNativeAzureFileSystemOperationsMocked";
  
  public TestNativeAzureFileSystemOperationsMocked (){
    super(TEST_ROOT_DIR);
  }

  @Override
  public void setUp() throws Exception {
    fSys = AzureBlobStorageTestAccount.createMock().getFileSystem();
  }

  @Override
  protected FileSystem createFileSystem() throws Exception {
    return AzureBlobStorageTestAccount.createMock().getFileSystem();
  }

  public void testListStatusThrowsExceptionForUnreadableDir() throws Exception {
    System.out
        .println("Skipping testListStatusThrowsExceptionForUnreadableDir since WASB"
            + " doesn't honor directory permissions.");
    assumeTrue(!Path.WINDOWS);
  }

  @Override
  public String getTestRootDir() {
    return TEST_ROOT_DIR;
  }

  @Override
  public Path getTestRootPath(FileSystem fSys) {
    return fSys.makeQualified(new Path(TEST_ROOT_DIR));
  }

  @Override
  public Path getTestRootPath(FileSystem fSys, String pathString) {
    return fSys.makeQualified(new Path(TEST_ROOT_DIR, pathString));
  }

  @Override
  public Path getAbsoluteTestRootPath(FileSystem fSys) {
    Path testRootPath = new Path(TEST_ROOT_DIR);
    if (testRootPath.isAbsolute()) {
      return testRootPath;
    } else {
      return new Path(fSys.getWorkingDirectory(), TEST_ROOT_DIR);
    }
  }
}
