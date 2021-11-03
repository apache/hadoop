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

package org.apache.hadoop.fs.azurebfs.contract;

import org.assertj.core.api.Assertions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.contract.AbstractContractRenameTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractOptions;

import static org.apache.hadoop.fs.contract.ContractOptions.RENAME_RETURNS_FALSE_IF_DEST_EXISTS;
import static org.apache.hadoop.fs.contract.ContractOptions.RENAME_RETURNS_FALSE_IF_SOURCE_MISSING;

/**
 * Contract test for rename operation  with abfs set to raise exceptions.
 * This requires patching both the config used by the FS
 * and that reported by the contract.
 */
public class ITestAbfsFileSystemRenameRaisesExceptions extends AbstractContractRenameTest {
  private final ABFSContractTestBinding binding;

  public ITestAbfsFileSystemRenameRaisesExceptions() throws Exception {
    binding = new ABFSContractTestBinding();
  }

  @Override
  public void setup() throws Exception {
    binding.setup();
    super.setup();
    // Base rename contract test class re-uses the test folder
    // This leads to failures when the test is re-run as same ABFS test
    // containers are re-used for test run and creation of source and
    // destination test paths fail, as they are already present.
    binding.getFileSystem().delete(binding.getTestPath(), true);
    final FileSystem fs = getFileSystem();
    Assertions.assertThat(fs.getConf().getBoolean(ConfigurationKeys.FS_AZURE_RENAME_RAISES_EXCEPTIONS, false))
        .describedAs("FS raises exceptions on rename %s", fs)
        .isTrue();
  }

  @Override
  protected Configuration createConfiguration() {
    final Configuration conf = binding.getRawConfiguration();
    conf.setBoolean(ConfigurationKeys.FS_AZURE_RENAME_RAISES_EXCEPTIONS,
        true);
    return conf;
  }

  @Override
  protected AbstractFSContract createContract(final Configuration conf) {
    final AbfsFileSystemContract contract = new AbfsFileSystemContract(conf,
        binding.isSecureMode());
    // get the contract conf after abfs.xml is loaded, and patch it
    final Configuration contractConf = contract.getConf();
    contractConf.setBoolean(FS_CONTRACT_KEY + RENAME_RETURNS_FALSE_IF_SOURCE_MISSING,
        false);
    contractConf.setBoolean(FS_CONTRACT_KEY + RENAME_RETURNS_FALSE_IF_DEST_EXISTS,
        false);
    // check it went through
    Assertions.assertThat(contract.isSupported(RENAME_RETURNS_FALSE_IF_SOURCE_MISSING, true))
        .describedAs("isSupported(RENAME_RETURNS_FALSE_IF_SOURCE_MISSING)")
        .isFalse();
    return contract;
  }
}
