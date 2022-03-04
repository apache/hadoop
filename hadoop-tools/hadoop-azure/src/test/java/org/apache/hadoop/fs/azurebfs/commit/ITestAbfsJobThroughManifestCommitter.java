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

import java.io.IOException;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contract.ABFSContractTestBinding;
import org.apache.hadoop.fs.azurebfs.contract.AbfsFileSystemContract;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.TestJobThroughManifestCommitter;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperations;

import static org.apache.hadoop.fs.azurebfs.commit.AbfsCommitTestHelper.prepareTestConfiguration;

/**
 * Test the Manifest committer stages against ABFS.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ITestAbfsJobThroughManifestCommitter
    extends TestJobThroughManifestCommitter {

  private final ABFSContractTestBinding binding;

  public ITestAbfsJobThroughManifestCommitter() throws Exception {
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

  @Override
  protected boolean shouldDeleteTestRootAtEndOfTestRun() {
    return true;
  }

  /**
   * Add read of manifest and validate of output's etags.
   * @param attemptId attempt ID
   * @param files files which were created.
   * @param manifest manifest
   * @throws IOException failure
   */
  @Override
  protected void validateTaskAttemptManifest(String attemptId,
      List<Path> files,
      TaskManifest manifest) throws IOException {
    super.validateTaskAttemptManifest(attemptId, files, manifest);
    final List<FileEntry> commit = manifest.getFilesToCommit();
    final ManifestStoreOperations operations = getStoreOperations();
    for (FileEntry entry : commit) {
      Assertions.assertThat(entry.getEtag())
          .describedAs("Etag of %s", entry)
          .isNotEmpty();
      final FileStatus sourceStatus = operations.getFileStatus(entry.getSourcePath());
      final String etag = ManifestCommitterSupport.getEtag(sourceStatus);
      Assertions.assertThat(etag)
          .describedAs("Etag of %s", sourceStatus)
          .isEqualTo(entry.getEtag());
    }
  }
}
