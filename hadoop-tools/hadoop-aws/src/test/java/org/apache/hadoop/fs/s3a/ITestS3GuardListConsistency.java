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
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.apache.hadoop.fs.s3a.s3guard.DirListingMetadata;
import org.apache.hadoop.fs.s3a.s3guard.S3Guard;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.s3a.Constants.*;

/**
 * Test S3Guard list consistency feature by injecting delayed listObjects()
 * visibility via {@link InconsistentAmazonS3Client}.
 */
public class ITestS3GuardListConsistency extends AbstractS3ATestBase {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    conf.setClass(S3_CLIENT_FACTORY_IMPL, InconsistentS3ClientFactory.class,
        S3ClientFactory.class);
    return new S3AContract(conf);
  }

  @Test
  public void testConsistentList() throws Exception {

    S3AFileSystem fs = getFileSystem();

    // This test will fail if NullMetadataStore (the default) is configured:
    // skip it.
    Assume.assumeTrue(fs.hasMetadataStore());

    // Any S3 keys that contain DELAY_KEY_SUBSTRING will be delayed
    // in listObjects() results via InconsistentS3Client
    Path inconsistentPath =
        path("a/b/dir3-" + InconsistentAmazonS3Client.DELAY_KEY_SUBSTRING);

    Path[] testDirs = {path("a/b/dir1"),
        path("a/b/dir2"),
        inconsistentPath};

    for (Path path : testDirs) {
      assertTrue(fs.mkdirs(path));
    }

    FileStatus[] paths = fs.listStatus(path("a/b/"));
    List<Path> list = new ArrayList<>();
    for (FileStatus fileState : paths) {
      list.add(fileState.getPath());
    }
    assertTrue(list.contains(path("a/b/dir1")));
    assertTrue(list.contains(path("a/b/dir2")));
    // This should fail without S3Guard, and succeed with it.
    assertTrue(list.contains(inconsistentPath));
  }

  @Test
  public void testListStatusWriteBack() throws Exception {
    Assume.assumeTrue(getFileSystem().hasMetadataStore());

    Configuration conf;
    Path directory = path("ListStatusWriteBack");

    // Create a FileSystem that is S3-backed only
    conf = createConfiguration();
    conf.setBoolean("fs.s3a.impl.disable.cache", true);
    conf.set(Constants.S3_METADATA_STORE_IMPL,
        Constants.S3GUARD_METASTORE_NULL);
    FileSystem noS3Guard = FileSystem.get(directory.toUri(), conf);

    // Create a FileSystem with S3Guard and write-back disabled
    conf = createConfiguration();
    S3ATestUtils.maybeEnableS3Guard(conf);
    conf.setBoolean("fs.s3a.impl.disable.cache", true);
    conf.setBoolean(Constants.METADATASTORE_AUTHORITATIVE, false);
    FileSystem noWriteBack = FileSystem.get(directory.toUri(), conf);

    // Create a FileSystem with S3Guard and write-back enabled
    conf = createConfiguration();
    S3ATestUtils.maybeEnableS3Guard(conf);
    conf.setBoolean("fs.s3a.impl.disable.cache", true);
    conf.setBoolean(Constants.METADATASTORE_AUTHORITATIVE, true);
    FileSystem yesWriteBack = FileSystem.get(directory.toUri(), conf);

    // Create a directory on S3 only
    noS3Guard.mkdirs(new Path(directory, "123"));
    // Create a directory on metastore only
    noWriteBack.mkdirs(new Path(directory, "XYZ"));

    FileStatus[] fsResults;
    DirListingMetadata mdResults;

    // FS should return both
    fsResults = noWriteBack.listStatus(directory);
    assertTrue("Unexpected number of results from filesystem. " +
            "Should have /XYZ and /123: " + fsResults.toString(),
        fsResults.length == 2);

    // Metastore without write-back should still only contain 1
    mdResults = S3Guard.getMetadataStore(noWriteBack).listChildren(directory);
    assertTrue("Unexpected number of results from metastore. " +
            "Metastore should only know about /XYZ: " + mdResults.toString(),
        mdResults.numEntries() == 1);

    // FS should return both (and will write it back)
    fsResults = yesWriteBack.listStatus(directory);
    assertTrue("Unexpected number of results from filesystem. " +
            "Should have /XYZ and /123: " + fsResults.toString(),
        fsResults.length == 2);

    // Metastore should not contain both
    mdResults = S3Guard.getMetadataStore(yesWriteBack).listChildren(directory);
    assertTrue("Unexpected number of results from metastore. " +
            "Should have /XYZ and /123: " + mdResults.toString(),
        mdResults.numEntries() == 2);
  }
}
