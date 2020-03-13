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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.s3guard.DirListingMetadata;
import org.apache.hadoop.fs.s3a.s3guard.S3Guard;

import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.junit.Assume.assumeTrue;

/**
 * Test cases that validate S3Guard's behavior for writing things like
 * directory listings back to the MetadataStore.
 */
public class ITestS3GuardWriteBack extends AbstractS3ATestBase {

  @Override
  public void setup() throws Exception {
    assumeTrue("dirListingUnion always writes back records",
        !S3Guard.DIR_MERGE_UPDATES_ALL_RECORDS_NONAUTH);
    super.setup();
  }

  /**
   * In listStatus(), when S3Guard is enabled, the full listing for a
   * directory is "written back" to the MetadataStore before the listing is
   * returned.  Currently this "write back" behavior occurs when
   * fs.s3a.metadatastore.authoritative is true.  This test validates this
   * behavior.
   * @throws Exception on failure
   */
  @Test
  public void testListStatusWriteBack() throws Exception {
    assumeTrue(getFileSystem().hasMetadataStore());

    Path directory = path("ListStatusWriteBack");

    // "raw" S3AFileSystem without S3Guard
    S3AFileSystem noS3Guard = createTestFS(directory.toUri(), true, false);

    // Another with S3Guard and write-back disabled
    S3AFileSystem noWriteBack = createTestFS(directory.toUri(), false, false);

    // Another S3Guard and write-back enabled
    S3AFileSystem yesWriteBack = createTestFS(directory.toUri(), false, true);

    // delete the existing directory (in case of last test failure)
    noS3Guard.delete(directory, true);
    // Create a directory on S3 only
    Path onS3 = new Path(directory, "OnS3");
    noS3Guard.mkdirs(onS3);
    // Create a directory on both S3 and metadata store
    Path onS3AndMS = new Path(directory, "OnS3AndMS");
    ContractTestUtils.assertPathDoesNotExist(noWriteBack, "path", onS3AndMS);
    noWriteBack.mkdirs(onS3AndMS);

    FileStatus[] fsResults;
    DirListingMetadata mdResults;

    // FS should return both even though S3Guard is not writing back to MS
    fsResults = noWriteBack.listStatus(directory);
    assertEquals("Filesystem enabled S3Guard without write back should have "
            + "both /OnS3 and /OnS3AndMS: " + Arrays.toString(fsResults),
        2, fsResults.length);

    // Metadata store without write-back should still only contain /OnS3AndMS,
    // because newly discovered /OnS3 is not written back to metadata store
    mdResults = noWriteBack.getMetadataStore().listChildren(directory);
    assertNotNull("No results from noWriteBack listChildren " + directory,
        mdResults);
    assertEquals("Metadata store without write back should still only know "
            + "about /OnS3AndMS, but it has: " + mdResults,
        1, mdResults.numEntries());

    // FS should return both (and will write it back)
    fsResults = yesWriteBack.listStatus(directory);
    assertEquals("Filesystem enabled S3Guard with write back should have"
            + " both /OnS3 and /OnS3AndMS: " + Arrays.toString(fsResults),
        2, fsResults.length);

    // Metadata store with write-back should contain both because the newly
    // discovered /OnS3 should have been written back to metadata store
    mdResults = yesWriteBack.getMetadataStore().listChildren(directory);
    assertEquals("Unexpected number of results from metadata store. "
            + "Should have /OnS3 and /OnS3AndMS: " + mdResults,
        2, mdResults.numEntries());

    // If we don't clean this up, the next test run will fail because it will
    // have recorded /OnS3 being deleted even after it's written to noS3Guard.
    getFileSystem().getMetadataStore().forgetMetadata(onS3);
  }

  /**
   * Create a separate S3AFileSystem instance for testing.
   * There's a bit of complexity as it forces pushes up s3guard options from
   * the base values to the per-bucket options. This stops explicit bucket
   * settings in test XML configs from unintentionally breaking tests.
   */
  private S3AFileSystem createTestFS(URI fsURI, boolean disableS3Guard,
      boolean authoritativeMeta) throws IOException {
    Configuration conf;

    // Create a FileSystem that is S3-backed only
    conf = createConfiguration();
    String host = fsURI.getHost();
    String metastore;

    metastore = S3GUARD_METASTORE_NULL;
    if (!disableS3Guard) {
      // pick up the metadata store used by the main test
      metastore = getFileSystem().getConf().get(S3_METADATA_STORE_IMPL);
      assertNotEquals(S3GUARD_METASTORE_NULL, metastore);
    }

    conf.set(Constants.S3_METADATA_STORE_IMPL, metastore);
    conf.setBoolean(METADATASTORE_AUTHORITATIVE, authoritativeMeta);
    conf.unset(AUTHORITATIVE_PATH);
    S3AUtils.setBucketOption(conf, host,
        METADATASTORE_AUTHORITATIVE,
        Boolean.toString(authoritativeMeta));
    S3AUtils.setBucketOption(conf, host,
        S3_METADATA_STORE_IMPL, metastore);

    S3AFileSystem fs = asS3AFS(FileSystem.newInstance(fsURI, conf));
    // do a check to verify that everything got through
    assertEquals("Metadata store should have been disabled: " + fs,
        disableS3Guard, !fs.hasMetadataStore());
    assertEquals("metastore option did not propagate",
        metastore, fs.getConf().get(S3_METADATA_STORE_IMPL));

    return fs;

  }

  private static S3AFileSystem asS3AFS(FileSystem fs) {
    assertTrue("Not a S3AFileSystem: " + fs, fs instanceof S3AFileSystem);
    return (S3AFileSystem)fs;
  }

}
