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
import org.apache.hadoop.fs.azurebfs.contract.ABFSContractTestBinding;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterTestSupport;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_STORE_OPERATIONS_CLASS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_VALIDATE_OUTPUT;

/**
 * Helper methods for committer tests on ABFS.
 */
final class AbfsCommitTestHelper {
  private AbfsCommitTestHelper() {
  }

  /**
   * Prepare the test configuration.
   * @param contractTestBinding test binding
   * @return an extraced and patched configuration.
   */
  static Configuration prepareTestConfiguration(
      ABFSContractTestBinding contractTestBinding) {
    final Configuration conf = ManifestCommitterTestSupport.enableTrash(
        contractTestBinding.getRawConfiguration());

    // use ABFS Store operations
    conf.set(OPT_STORE_OPERATIONS_CLASS,
        AbfsManifestStoreOperations.NAME);

    // validate output, including etags
    conf.setBoolean(OPT_VALIDATE_OUTPUT, true);
    return conf;
  }
}
