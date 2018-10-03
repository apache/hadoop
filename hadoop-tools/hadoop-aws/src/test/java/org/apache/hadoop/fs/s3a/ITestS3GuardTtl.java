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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.s3guard.DirListingMetadata;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.S3Guard;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.isMetadataStoreAuthoritative;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.metadataStorePersistsAuthoritativeBit;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * These tests are testing the S3Guard TTL (time to live) features.
 */
public class ITestS3GuardTtl extends AbstractS3ATestBase {

  /**
   * Patch the configuration - this test needs disabled filesystem caching.
   * These tests modify the fs instance that would cause flaky tests.
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration configuration = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(configuration);
    return S3ATestUtils.prepareTestConfiguration(configuration);
  }

  @Test
  public void testDirectoryListingAuthoritativeTtl() throws Exception {

    final S3AFileSystem fs = getFileSystem();
    Assume.assumeTrue(fs.hasMetadataStore());
    final MetadataStore ms = fs.getMetadataStore();

    Assume.assumeTrue("MetadataStore should be capable for authoritative "
            + "storage of directories to run this test.",
        metadataStorePersistsAuthoritativeBit(ms));

    Assume.assumeTrue("MetadataStore should be authoritative for this test",
        isMetadataStoreAuthoritative(getFileSystem().getConf()));

    S3Guard.ITtlTimeProvider mockTimeProvider =
        mock(S3Guard.ITtlTimeProvider.class);
    S3Guard.ITtlTimeProvider restoreTimeProvider = fs.getTtlTimeProvider();
    fs.setTtlTimeProvider(mockTimeProvider);
    when(mockTimeProvider.getNow()).thenReturn(100L);
    when(mockTimeProvider.getAuthoritativeDirTtl()).thenReturn(1L);

    Path dir = path("ttl/");
    Path file = path("ttl/afile");

    try {
      fs.mkdirs(dir);
      touch(fs, file);

      // get an authoritative listing in ms
      fs.listStatus(dir);
      // check if authoritative
      DirListingMetadata dirListing =
          S3Guard.listChildrenWithTtl(ms, dir, mockTimeProvider);
      assertTrue("Listing should be authoritative.",
          dirListing.isAuthoritative());
      // change the time, and assume it's not authoritative anymore
      when(mockTimeProvider.getNow()).thenReturn(102L);
      dirListing = S3Guard.listChildrenWithTtl(ms, dir, mockTimeProvider);
      assertFalse("Listing should not be authoritative.",
          dirListing.isAuthoritative());

      // get an authoritative listing in ms again - retain test
      fs.listStatus(dir);
      // check if authoritative
      dirListing = S3Guard.listChildrenWithTtl(ms, dir, mockTimeProvider);
      assertTrue("Listing shoud be authoritative after listStatus.",
          dirListing.isAuthoritative());
    } finally {
      fs.delete(dir, true);
      fs.setTtlTimeProvider(restoreTimeProvider);
    }
  }
}
