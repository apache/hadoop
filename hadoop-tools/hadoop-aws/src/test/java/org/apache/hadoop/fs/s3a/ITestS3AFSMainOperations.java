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

import org.junit.Ignore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSMainOperationsBaseTest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.s3a.S3AContract;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.createTestPath;

/**
 * S3A Test suite for the FSMainOperationsBaseTest tests.
 */
public class ITestS3AFSMainOperations extends FSMainOperationsBaseTest {


  public ITestS3AFSMainOperations() {
    super(createTestPath(
        new Path("/ITestS3AFSMainOperations")).toUri().toString());
  }

  @Override
  protected FileSystem createFileSystem() throws Exception {
    S3AContract contract = new S3AContract(new Configuration());
    contract.init();
    return contract.getTestFileSystem();
  }

  @Override
  @Ignore("Permissions not supported")
  public void testListStatusThrowsExceptionForUnreadableDir() {
  }

  @Override
  @Ignore("Permissions not supported")
  public void testGlobStatusThrowsExceptionForUnreadableDir() {
  }

  @Override
  @Ignore("local FS path setup broken")
  public void testCopyToLocalWithUseRawLocalFileSystemOption()
      throws Exception {
  }

}
