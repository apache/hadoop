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

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.s3a.s3guard.DDBPathMetadata;
import org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assumeFilesystemHasMetadatastore;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getStatusWithEmptyDirFlag;

/**
 * Test logic around whether or not a directory is empty, with S3Guard enabled.
 * The fact that S3AFileStatus has an isEmptyDirectory flag in it makes caching
 * S3AFileStatus's really tricky, as the flag can change as a side effect of
 * changes to other paths.
 * After S3Guard is merged to trunk, we should try to remove the
 * isEmptyDirectory flag from S3AFileStatus, or maintain it outside
 * of the MetadataStore.
 */
public class ITestS3GuardEmptyDirs extends AbstractS3ATestBase {

  @Override
  public void setup() throws Exception {
    super.setup();
    assumeFilesystemHasMetadatastore(getFileSystem());
  }

  @Test
  public void testEmptyDirs() throws Exception {
    S3AFileSystem fs = getFileSystem();
    MetadataStore configuredMs = fs.getMetadataStore();
    Path existingDir = path("existing-dir");
    Path existingFile = path("existing-dir/existing-file");
    try {
      // 1. Simulate files already existing in the bucket before we started our
      // cluster.  Temporarily disable the MetadataStore so it doesn't witness
      // us creating these files.

      fs.setMetadataStore(new NullMetadataStore());
      assertTrue(fs.mkdirs(existingDir));
      touch(fs, existingFile);


      // 2. Simulate (from MetadataStore's perspective) starting our cluster and
      // creating a file in an existing directory.
      fs.setMetadataStore(configuredMs);  // "start cluster"
      Path newFile = path("existing-dir/new-file");
      touch(fs, newFile);

      S3AFileStatus status = fs.innerGetFileStatus(existingDir, true);
      assertEquals("Should not be empty dir", Tristate.FALSE,
          status.isEmptyDirectory());

      // 3. Assert that removing the only file the MetadataStore witnessed
      // being created doesn't cause it to think the directory is now empty.
      fs.delete(newFile, false);
      status = fs.innerGetFileStatus(existingDir, true);
      assertEquals("Should not be empty dir", Tristate.FALSE,
          status.isEmptyDirectory());

      // 4. Assert that removing the final file, that existed "before"
      // MetadataStore started, *does* cause the directory to be marked empty.
      fs.delete(existingFile, false);
      status = fs.innerGetFileStatus(existingDir, true);
      assertEquals("Should be empty dir now", Tristate.TRUE,
          status.isEmptyDirectory());
    } finally {
      configuredMs.forgetMetadata(existingFile);
      configuredMs.forgetMetadata(existingDir);
    }
  }

  /**
   * Test tombstones don't get in the way of a listing of the
   * root dir.
   * This test needs to create a path which appears first in the listing,
   * and an entry which can come later. To allow the test to proceed
   * while other tests are running, the filename "0000" is used for that
   * deleted entry.
   */
  @Test
  public void testRootTombstones() throws Throwable {
    S3AFileSystem fs = getFileSystem();

    // Create the first and last files.
    Path root = fs.makeQualified(new Path("/"));
    // use something ahead of all the ASCII alphabet characters so
    // even during parallel test runs, this test is expected to work.
    String first = "0000";
    Path firstPath = new Path(root, first);

    // this path is near the bottom of the ASCII string space.
    // This isn't so critical.
    String last = "zzzz";
    Path lastPath = new Path(root, last);
    touch(fs, firstPath);
    touch(fs, lastPath);
    // Delete first entry (+assert tombstone)
    assertDeleted(firstPath, false);
    DynamoDBMetadataStore ddbMs = getRequiredDDBMetastore(fs);
    DDBPathMetadata firstMD = ddbMs.get(firstPath);
    assertNotNull("No MD for " + firstPath, firstMD);
    assertTrue("Not a tombstone " + firstMD,
        firstMD.isDeleted());
    // PUT child to store
    Path child = new Path(firstPath, "child");
    StoreContext ctx = fs.createStoreContext();
    String childKey = ctx.pathToKey(child);
    String rootKey = ctx.pathToKey(root);
    AmazonS3 s3 = fs.getAmazonS3ClientForTesting("LIST");
    String bucket = ctx.getBucket();
    try {
      createEmptyObject(fs, childKey);

      // Do a list
      ListObjectsRequest listReq = new ListObjectsRequest(
          bucket, rootKey, "", "/", 10);
      ObjectListing listing = s3.listObjects(listReq);

      // the listing has the first path as a prefix, because of the child
      Assertions.assertThat(listing.getCommonPrefixes())
          .describedAs("The prefixes of a LIST of %s", root)
          .contains(first + "/");

      // and the last file is one of the files
      Stream<String> files = listing.getObjectSummaries()
          .stream()
          .map(s -> s.getKey());
      Assertions.assertThat(files)
          .describedAs("The files of a LIST of %s", root)
          .contains(last);

      // verify absolutely that the last file exists
      assertPathExists("last file", lastPath);

      // do a getFile status with empty dir flag
      S3AFileStatus rootStatus = getStatusWithEmptyDirFlag(fs, root);
      assertNonEmptyDir(rootStatus);
    } finally {
      // try to recover from the defective state.
      s3.deleteObject(bucket, childKey);
      fs.delete(lastPath, true);
      ddbMs.forgetMetadata(firstPath);
    }
  }

  protected void assertNonEmptyDir(final S3AFileStatus status) {
    assertEquals("Should not be empty dir: " + status, Tristate.FALSE,
        status.isEmptyDirectory());
  }

  /**
   * Get the DynamoDB metastore; assume false if it is of a different
   * type.
   * @return extracted and cast metadata store.
   */
  @SuppressWarnings("ConstantConditions")
  private DynamoDBMetadataStore getRequiredDDBMetastore(S3AFileSystem fs) {
    MetadataStore ms = fs.getMetadataStore();
    assume("Not a DynamoDBMetadataStore: " + ms,
        ms instanceof DynamoDBMetadataStore);
    return (DynamoDBMetadataStore) ms;
  }

  /**
   * From {@code S3AFileSystem.createEmptyObject()}.
   * @param fs filesystem
   * @param key key
   * @throws IOException failure
   */
  private void createEmptyObject(S3AFileSystem fs, String key)
      throws IOException {
    final InputStream im = new InputStream() {
      @Override
      public int read() throws IOException {
        return -1;
      }
    };

    PutObjectRequest putObjectRequest = fs.newPutObjectRequest(key,
        fs.newObjectMetadata(0L),
        im);
    AmazonS3 s3 = fs.getAmazonS3ClientForTesting("PUT");
    s3.putObject(putObjectRequest);
  }

}
