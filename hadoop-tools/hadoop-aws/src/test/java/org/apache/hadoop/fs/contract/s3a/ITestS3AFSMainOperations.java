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

package org.apache.hadoop.fs.contract.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSMainOperationsBaseTest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.s3a.S3ATestUtils;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.maybeEnableS3Guard;

/**
 * S3A version of the {@link FSMainOperationsBaseTest} test suite.
 */
public class ITestS3AFSMainOperations extends FSMainOperationsBaseTest {

  public ITestS3AFSMainOperations() {
    super(S3ATestUtils.createTestPath(
        new Path("/ITestS3AFSMainOperations")).toString());
  }

  @Override
  public void setUp() throws Exception {
    AbstractFSContract contract = createContract(createConfiguration());
    contract.init();
    //extract the test FS
    fSys = contract.getTestFileSystem();
    super.setUp();
  }

  @Override
  protected FileSystem createFileSystem() throws Exception {
    return checkNotNull(fSys);
  }

  /**
   * Create a configuration, possibly patching in S3Guard options.
   * @return a configuration
   */
  private Configuration createConfiguration() {
    Configuration conf = new Configuration();
    // patch in S3Guard options
    maybeEnableS3Guard(conf);
    return conf;
  }

  private AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  /**
   * Override point: are posix-style permissions supported.
   * @return false
   */
  @Override
  protected boolean permissionsSupported() {
    return false;
  }

}
