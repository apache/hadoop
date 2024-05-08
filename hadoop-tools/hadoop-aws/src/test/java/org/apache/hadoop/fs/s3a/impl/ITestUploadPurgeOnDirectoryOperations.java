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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.MultipartUpload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;
import org.apache.hadoop.fs.store.audit.AuditSpan;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertHasPathCapabilities;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_OPERATIONS_PURGE_UPLOADS;
import static org.apache.hadoop.fs.s3a.MultipartTestUtils.clearAnyUploads;
import static org.apache.hadoop.fs.s3a.MultipartTestUtils.createMagicFile;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.Statistic.MULTIPART_UPLOAD_LIST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_MULTIPART_UPLOAD_ABORTED;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_MULTIPART_UPLOAD_LIST;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.MAGIC_COMMITTER_ENABLED;
import static org.apache.hadoop.util.functional.RemoteIterators.toList;

/**
 * Test behavior of purging uploads in rename and delete.
 * S3 Express tests automatically set this; it is explicitly set for the rest.
 */
public class ITestUploadPurgeOnDirectoryOperations extends AbstractS3ACostTest {

  @Override
  public Configuration createConfiguration() {
    final Configuration conf = super.createConfiguration();
    removeBaseAndBucketOverrides(conf,
        DIRECTORY_OPERATIONS_PURGE_UPLOADS,
        MAGIC_COMMITTER_ENABLED);
    conf.setBoolean(DIRECTORY_OPERATIONS_PURGE_UPLOADS, true);
    conf.setBoolean(MAGIC_COMMITTER_ENABLED, true);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    final S3AFileSystem fs = getFileSystem();
    assertHasPathCapabilities(fs, new Path("/"),
        DIRECTORY_OPERATIONS_PURGE_UPLOADS);
    clearAnyUploads(fs, methodPath());
  }

  @Test
  public void testDeleteWithPendingUpload() throws Throwable {

    final S3AFileSystem fs = getFileSystem();
    final Path dir = methodPath();

    // create a magic file.
    createMagicFile(fs, dir);

    // and there's a pending upload
    assertUploadCount(dir, 1);

    // delete the dir, with a cost of 1 abort, 1 list.
    verifyMetrics(() -> fs.delete(dir, true),
        with(OBJECT_MULTIPART_UPLOAD_ABORTED, 1), // abort
        with(OBJECT_MULTIPART_UPLOAD_LIST, 1),    // HTTP request inside iterator
        with(MULTIPART_UPLOAD_LIST, 0));          // api list call


    // and the pending upload is gone
    assertUploadCount(dir, 0);
  }

  @Test
  public void testRenameWithPendingUpload() throws Throwable {

    final S3AFileSystem fs = getFileSystem();
    final Path base = methodPath();
    final Path dir = new Path(base, "src");
    final Path dest = new Path(base, "dest");

    // create a magic file.
    createMagicFile(fs, dir);

    // and there's a pending upload
    assertUploadCount(dir, 1);

    // rename the dir, with a cost of 1 abort, 1 list.
    verifyMetrics(() -> fs.rename(dir, dest),
        with(OBJECT_MULTIPART_UPLOAD_ABORTED, 1), // abort
        with(OBJECT_MULTIPART_UPLOAD_LIST, 1),    // HTTP request inside iterator
        with(MULTIPART_UPLOAD_LIST, 0));          // api list call

    // and there isn't
    assertUploadCount(dir, 0);
  }

  /**
   * Assert the upload count under a dir is the expected value.
   * Failure message will include the list of entries.
   * @param dir dir
   * @param expected expected count
   * @throws IOException listing problem
   */
  private void assertUploadCount(final Path dir, final int expected) throws IOException {
    Assertions.assertThat(toList(listUploads(dir)))
        .describedAs("uploads under %s", dir)
        .hasSize(expected);
  }

  /**
   * List uploads; use the same APIs that the directory operations use,
   * so implicitly validating them.
   * @param dir directory to list
   * @return full list of entries
   * @throws IOException listing problem
   */
  private RemoteIterator<MultipartUpload> listUploads(Path dir) throws IOException {
    final S3AFileSystem fs = getFileSystem();
    try (AuditSpan ignored = span()) {
      final StoreContext sc = fs.createStoreContext();
      return fs.listUploadsUnderPrefix(sc, sc.pathToKey(dir));
    }
  }
}
