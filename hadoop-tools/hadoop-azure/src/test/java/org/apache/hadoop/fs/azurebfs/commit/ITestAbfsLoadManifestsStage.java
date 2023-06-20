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
import org.apache.hadoop.fs.azure.integration.AzureTestConstants;
import org.apache.hadoop.fs.azurebfs.contract.ABFSContractTestBinding;
import org.apache.hadoop.fs.azurebfs.contract.AbfsFileSystemContract;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterTestSupport;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.TestLoadManifestsStage;

/**
 * ABFS storage test of saving and loading a large number
 * of manifests.
 */
public class ITestAbfsLoadManifestsStage extends TestLoadManifestsStage {

  private final ABFSContractTestBinding binding;

  public ITestAbfsLoadManifestsStage() throws Exception {
    binding = new ABFSContractTestBinding();
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
  protected int getTestTimeoutMillis() {
    return AzureTestConstants.SCALE_TEST_TIMEOUT_MILLIS;
  }

  /**
   * @return a smaller number of TAs than the base test suite does.
   */
  @Override
  protected int numberOfTaskAttempts() {
    return ManifestCommitterTestSupport.NUMBER_OF_TASK_ATTEMPTS_SMALL;
  }
}
