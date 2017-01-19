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
package org.apache.hadoop.fs.contract.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractContractGetFileStatusTest;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3ATestUtils;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.maybeEnableS3Guard;

/**
 * S3A contract tests covering getFileStatus.
 */
public class ITestS3AContractGetFileStatus
    extends AbstractContractGetFileStatusTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  @Override
  public void teardown() throws Exception {
    getLog().info("FS details {}", getFileSystem());
    super.teardown();
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    // aggressively low page size forces tests to go multipage
    conf.setInt(Constants.MAX_PAGING_KEYS, 2);
    // patch in S3Guard options
    maybeEnableS3Guard(conf);
    return conf;
  }
}
