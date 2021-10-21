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

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contract.ABFSContractTestBinding;
import org.apache.hadoop.fs.azurebfs.contract.AbfsFileSystemContract;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.AbstractManifestCommitterTest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.StoreOperations;

import static org.apache.hadoop.fs.azurebfs.commit.AbfsCommitTestHelper.prepareTestConfiguration;

public class ITestAbfsManifestStoreOperations extends AbstractManifestCommitterTest {

  private final ABFSContractTestBinding binding;

  public ITestAbfsManifestStoreOperations() throws Exception {
    binding = new ABFSContractTestBinding();
  }

  @Override
  public void setup() throws Exception {
    binding.setup();
    super.setup();
  }

  @Override
  protected Configuration createConfiguration() {
    return enableManifestCommitter(prepareTestConfiguration(binding));
  }

  @Override
  protected AbstractFSContract createContract(final Configuration conf) {
    return new AbfsFileSystemContract(conf, binding.isSecureMode());
  }

  @Test
  public void testEtagConsistencyAcrossListAndHead() throws Throwable {
    final Path path = methodPath();
    final FileSystem fs = getFileSystem();
    ContractTestUtils.touch(fs, path);
    final StoreOperations operations = createStoreOperations();
    final FileStatus st = operations.getFileStatus(path);
    final String etag = operations.getEtag(st);
    Assertions.assertThat(etag)
        .describedAs("Etag of %s", st)
        .isNotBlank();

    final FileStatus[] statuses = fs.listStatus(path);
    Assertions.assertThat(statuses)
        .describedAs("List(%s)", path)
        .hasSize(1);
    final FileStatus lsStatus = statuses[0];
    Assertions.assertThat(operations.getEtag(lsStatus))
        .describedAs("etag of list status (%s) compared to HEAD value of %s", lsStatus, st)
        .isEqualTo(etag);
  }
}
