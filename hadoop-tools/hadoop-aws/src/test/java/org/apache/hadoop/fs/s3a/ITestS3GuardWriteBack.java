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
import org.apache.hadoop.fs.s3a.s3guard.DirListingMetadata;
import org.junit.Assume;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

/**
 * Test cases that validate S3Guard's behavior for writing things like
 * directory listings back to the MetadataStore.
 */
public class ITestS3GuardWriteBack extends AbstractS3ATestBase {

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
    Assume.assumeTrue(getFileSystem().hasMetadataStore());

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
    noS3Guard.mkdirs(new Path(directory, "OnS3"));
    // Create a directory on both S3 and metadata store
    Path p = new Path(directory, "OnS3AndMS");
    assertPathDoesntExist(noWriteBack, p);
    noWriteBack.mkdirs(p);

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
    assertEquals("Metadata store without write back should still only know "
            + "about /OnS3AndMS, but it has: " + mdResults,
        1, mdResults.numEntries());

    // FS should return both (and will write it back)
    fsResults = yesWriteBack.listStatus(directory);
    assertEquals("Filesystem enabled S3Guard with write back should have "
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
    getFileSystem().getMetadataStore().forgetMetadata(
        new Path(directory, "OnS3"));
  }

  /** Create a separate S3AFileSystem instance for testing. */
  private S3AFileSystem createTestFS(URI fsURI, boolean disableS3Guard,
      boolean authoritativeMeta) throws IOException {
    Configuration conf;

    // Create a FileSystem that is S3-backed only
    conf = createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    if (disableS3Guard) {
      conf.set(Constants.S3_METADATA_STORE_IMPL,
          Constants.S3GUARD_METASTORE_NULL);
    } else {
      S3ATestUtils.maybeEnableS3Guard(conf);
      conf.setBoolean(Constants.METADATASTORE_AUTHORITATIVE, authoritativeMeta);
    }
    FileSystem fs = FileSystem.get(fsURI, conf);
    return asS3AFS(fs);
  }

  private static S3AFileSystem asS3AFS(FileSystem fs) {
    assertTrue("Not a S3AFileSystem: " + fs, fs instanceof S3AFileSystem);
    return (S3AFileSystem)fs;
  }

  private static void assertPathDoesntExist(FileSystem fs, Path p)
      throws IOException {
    try {
      FileStatus s = fs.getFileStatus(p);
    } catch (FileNotFoundException e) {
      return;
    }
    fail("Path should not exist: " + p);
  }

}
