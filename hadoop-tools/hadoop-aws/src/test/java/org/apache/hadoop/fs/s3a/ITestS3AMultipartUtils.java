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

package org.apache.hadoop.fs.s3a;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.MultipartUpload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.store.audit.AuditSpan;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.util.functional.RemoteIterators.foreach;

/**
 * Tests for {@link MultipartUtils}.
 */
public class ITestS3AMultipartUtils extends AbstractS3ATestBase {

  private static final int UPLOAD_LEN = 1024;
  private static final String PART_FILENAME_BASE = "pending-part";
  private static final int LIST_BATCH_SIZE = 2;
  private static final int NUM_KEYS = 5;


  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    // Forces listings to come back in multiple batches to test that part of
    // the iterators.
    conf.setInt(Constants.MAX_PAGING_KEYS, LIST_BATCH_SIZE);
    return conf;
  }

  /**
   * Main test case for upload part listing and iterator paging.
   * @throws Exception on failure.
   */
  @Test
  public void testListMultipartUploads() throws Exception {
    S3AFileSystem fs = getFileSystem();
    Set<MultipartTestUtils.IdKey> keySet = new HashSet<>();
    try (AuditSpan span = span()) {
      // 1. Create NUM_KEYS pending upload parts
      for (int i = 0; i < NUM_KEYS; i++) {
        Path filePath = getPartFilename(i);
        String key = fs.pathToKey(filePath);
        describe("creating upload part with key %s", key);
        // create a multipart upload
        MultipartTestUtils.IdKey idKey = MultipartTestUtils
            .createPartUpload(fs, key, UPLOAD_LEN,
            1);
        keySet.add(idKey);
      }

      // 2. Verify all uploads are found listing by prefix
      describe("Verifying upload list by prefix");
      MultipartUtils.UploadIterator uploads = fs.listUploads(getPartPrefix(fs));
      assertUploadsPresent(uploads, keySet);

      // 3. Verify all uploads are found listing without prefix
      describe("Verifying list all uploads");
      uploads = fs.listUploads(null);
      assertUploadsPresent(uploads, keySet);

    } finally {
      // 4. Delete all uploads we created
      MultipartTestUtils.cleanupParts(fs, keySet);
    }
  }

  /**
   * Assert that all provided multipart uploads are contained in the upload
   * iterator's results.
   * @param list upload iterator
   * @param ourUploads set up uploads that should be present
   * @throws IOException on I/O error
   */
  private void assertUploadsPresent(MultipartUtils.UploadIterator list,
      Set<MultipartTestUtils.IdKey> ourUploads) throws IOException {

    // Don't modify passed-in set, use copy.
    Set<MultipartTestUtils.IdKey> uploads = new HashSet<>(ourUploads);
    foreach(list, (upload) -> {
      MultipartTestUtils.IdKey listing = toIdKey(upload);
      if (uploads.remove(listing)) {
        LOG.debug("Matched: {},{}", listing.getKey(), listing.getUploadId());
      } else {
        LOG.debug("Not our upload {},{}", listing.getKey(),
            listing.getUploadId());
      }
    });
    Assertions.assertThat(uploads)
        .describedAs("Uploads which we expected to be listed.")
        .isEmpty();
  }

  private MultipartTestUtils.IdKey toIdKey(MultipartUpload mu) {
    return new MultipartTestUtils.IdKey(mu.key(), mu.uploadId());
  }

  private Path getPartFilename(int index) throws IOException {
    return path(String.format("%s-%d", PART_FILENAME_BASE, index));
  }

  private String getPartPrefix(S3AFileSystem fs) throws IOException {
    return fs.pathToKey(path("blah").getParent());
  }

}