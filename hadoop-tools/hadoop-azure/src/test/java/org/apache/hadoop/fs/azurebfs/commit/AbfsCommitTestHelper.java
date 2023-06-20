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
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_READ_SMALL_FILES_COMPLETELY;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_STORE_OPERATIONS_CLASS;

/**
 * Helper methods for committer tests on ABFS.
 */
final class AbfsCommitTestHelper {
  private AbfsCommitTestHelper() {
  }

  /**
   * Prepare the test configuration.
   * @param contractTestBinding test binding
   * @return an extracted and patched configuration.
   */
  static Configuration prepareTestConfiguration(
      ABFSContractTestBinding contractTestBinding) {
    final Configuration conf =
        contractTestBinding.getRawConfiguration();

    // use ABFS Store operations
    conf.set(OPT_STORE_OPERATIONS_CLASS,
        AbfsManifestStoreOperations.NAME);
    // turn on small file read if not explicitly set to a value.
    conf.setBooleanIfUnset(AZURE_READ_SMALL_FILES_COMPLETELY, true);
    // use a larger thread pool to compensate for latencies
    final String size = Integer.toString(192);
    conf.setIfUnset(ManifestCommitterConstants.OPT_IO_PROCESSORS, size);
    conf.setIfUnset(ManifestCommitterConstants.OPT_WRITER_QUEUE_CAPACITY, size);
    // no need for parallel delete here as we aren't at the scale where unified delete
    // is going to time out
    conf.setBooleanIfUnset(ManifestCommitterConstants.OPT_CLEANUP_PARALLEL_DELETE, false);

    return conf;
  }
}
