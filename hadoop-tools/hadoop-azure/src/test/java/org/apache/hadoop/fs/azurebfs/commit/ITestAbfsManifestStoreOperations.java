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

import java.nio.charset.StandardCharsets;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contract.ABFSContractTestBinding;
import org.apache.hadoop.fs.azurebfs.contract.AbfsFileSystemContract;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.AbstractManifestCommitterTest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperations;

import static org.apache.hadoop.fs.CommonPathCapabilities.ETAGS_PRESERVED_IN_RENAME;
import static org.apache.hadoop.fs.azurebfs.commit.AbfsCommitTestHelper.prepareTestConfiguration;
import static org.junit.Assume.assumeTrue;

/**
 * Test {@link AbfsManifestStoreOperations}.
 * As this looks at etag handling through FS operations, it's actually testing how etags work
 * in ABFS (preservation across renames) and in the client (are they consistent
 * in LIST and HEAD calls).
 *
 * Skipped when tested against wasb-compatible stores.
 */
public class ITestAbfsManifestStoreOperations extends AbstractManifestCommitterTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAbfsManifestStoreOperations.class);

  private final ABFSContractTestBinding binding;

  public ITestAbfsManifestStoreOperations() throws Exception {
    binding = new ABFSContractTestBinding();
  }

  @Override
  public void setup() throws Exception {
    binding.setup();
    super.setup();

    // skip tests on non-HNS stores
    assumeTrue("Resilient rename not available",
        getFileSystem().hasPathCapability(getContract().getTestPath(),
            ETAGS_PRESERVED_IN_RENAME));

  }

  @Override
  protected Configuration createConfiguration() {
    return enableManifestCommitter(prepareTestConfiguration(binding));
  }

  @Override
  protected AbstractFSContract createContract(final Configuration conf) {
    return new AbfsFileSystemContract(conf, binding.isSecureMode());
  }

  /**
   * basic consistency across operations, as well as being non-empty.
   */
  @Test
  public void testEtagConsistencyAcrossListAndHead() throws Throwable {
    describe("Etag values must be non-empty and consistent across LIST and HEAD Calls.");
    final Path path = methodPath();
    final FileSystem fs = getFileSystem();
    ContractTestUtils.touch(fs, path);
    final ManifestStoreOperations operations = createManifestStoreOperations();
    Assertions.assertThat(operations)
        .describedAs("Store operations class loaded via Configuration")
        .isInstanceOf(AbfsManifestStoreOperations.class);

    final FileStatus st = operations.getFileStatus(path);
    final String etag = operations.getEtag(st);
    Assertions.assertThat(etag)
        .describedAs("Etag of %s", st)
        .isNotBlank();
    LOG.info("etag of empty file is \"{}\"", etag);

    final FileStatus[] statuses = fs.listStatus(path);
    Assertions.assertThat(statuses)
        .describedAs("List(%s)", path)
        .hasSize(1);
    final FileStatus lsStatus = statuses[0];
    Assertions.assertThat(operations.getEtag(lsStatus))
        .describedAs("etag of list status (%s) compared to HEAD value of %s", lsStatus, st)
        .isEqualTo(etag);
  }

  @Test
  public void testEtagsOfDifferentDataDifferent() throws Throwable {
    describe("Verify that two different blocks of data written have different tags");

    final Path path = methodPath();
    final FileSystem fs = getFileSystem();
    Path src = new Path(path, "src");

    ContractTestUtils.createFile(fs, src, true,
        "data1234".getBytes(StandardCharsets.UTF_8));
    final ManifestStoreOperations operations = createManifestStoreOperations();
    final FileStatus srcStatus = operations.getFileStatus(src);
    final String srcTag = operations.getEtag(srcStatus);
    LOG.info("etag of file 1 is \"{}\"", srcTag);

    // now overwrite with data of same length
    // (ensure that path or length aren't used exclusively as tag)
    ContractTestUtils.createFile(fs, src, true,
        "1234data".getBytes(StandardCharsets.UTF_8));

    // validate
    final String tag2 = operations.getEtag(operations.getFileStatus(src));
    LOG.info("etag of file 2 is \"{}\"", tag2);

    Assertions.assertThat(tag2)
        .describedAs("etag of updated file")
        .isNotEqualTo(srcTag);
  }

  @Test
  public void testEtagConsistencyAcrossRename() throws Throwable {
    describe("Verify that when a file is renamed, the etag remains unchanged");
    final Path path = methodPath();
    final FileSystem fs = getFileSystem();
    Path src = new Path(path, "src");
    Path dest = new Path(path, "dest");

    ContractTestUtils.createFile(fs, src, true,
        "sample data".getBytes(StandardCharsets.UTF_8));
    final ManifestStoreOperations operations = createManifestStoreOperations();
    final FileStatus srcStatus = operations.getFileStatus(src);
    final String srcTag = operations.getEtag(srcStatus);
    LOG.info("etag of short file is \"{}\"", srcTag);

    Assertions.assertThat(srcTag)
        .describedAs("Etag of %s", srcStatus)
        .isNotBlank();

    // rename
    operations.commitFile(new FileEntry(src, dest, 0, srcTag));

    // validate
    FileStatus destStatus = operations.getFileStatus(dest);
    final String destTag = operations.getEtag(destStatus);
    Assertions.assertThat(destTag)
        .describedAs("etag of list status (%s) compared to HEAD value of %s", destStatus, srcStatus)
        .isEqualTo(srcTag);
  }

}
