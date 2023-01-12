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

package org.apache.hadoop.fs.azurebfs.commit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contract.ABFSContractTestBinding;
import org.apache.hadoop.fs.azurebfs.contract.AbfsFileSystemContract;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.TestRenameStageFailure;
/**
 * Rename failure logic on ABFS.
 * This will go through the resilient rename operation.
 */
public class ITestAbfsRenameStageFailure extends TestRenameStageFailure {

  /**
   * How many files to create.
   */
  private static final int FILES_TO_CREATE = 20;

  private final ABFSContractTestBinding binding;

  public ITestAbfsRenameStageFailure() throws Exception {
    binding = new ABFSContractTestBinding();
  }

  protected boolean isNamespaceEnabled() throws AzureBlobFileSystemException {
    AzureBlobFileSystem fs = (AzureBlobFileSystem) getFileSystem();
    return fs.getAbfsStore().getIsNamespaceEnabled(AbstractAbfsIntegrationTest.getSampleTracingContext(fs, false));
  }

  @Override
  public void setup() throws Exception {
    binding.setup();
    super.setup();
  }

  @Override
  protected Configuration createConfiguration() {
    return AbfsCommitTestHelper.prepareTestConfiguration(binding);
  }

  @Override
  protected AbstractFSContract createContract(final Configuration conf) {
    return new AbfsFileSystemContract(conf, binding.isSecureMode());
  }

  @Override
  protected boolean requireRenameResilience() throws AzureBlobFileSystemException {
    return isNamespaceEnabled();
  }

  @Override
  protected int filesToCreate() {
    return FILES_TO_CREATE;
  }
}
