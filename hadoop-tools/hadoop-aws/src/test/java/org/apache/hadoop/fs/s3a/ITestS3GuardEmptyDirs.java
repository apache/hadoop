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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.StatusProbeEnum;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.s3a.s3guard.DDBPathMetadata;
import org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertRenameOutcome;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
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

  /**
   * Rename an empty directory, verify that the empty dir
   * marker moves in both S3Guard and in the S3A FS.
   */
  @Test
  public void testRenameEmptyDir() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    Path basePath = path(getMethodName());
    Path sourceDir = new Path(basePath, "AAA-source");
    String sourceDirMarker = fs.pathToKey(sourceDir) + "/";
    Path destDir = new Path(basePath, "BBB-dest");
    String destDirMarker = fs.pathToKey(destDir) + "/";
    // set things up.
    mkdirs(sourceDir);
    // there's source directory marker
    fs.getObjectMetadata(sourceDirMarker);
    S3AFileStatus srcStatus = getEmptyDirStatus(sourceDir);
    assertEquals("Must be an empty dir: " + srcStatus, Tristate.TRUE,
        srcStatus.isEmptyDirectory());
    // do the rename
    assertRenameOutcome(fs, sourceDir, destDir, true);
    S3AFileStatus destStatus = getEmptyDirStatus(destDir);
    assertEquals("Must be an empty dir: " + destStatus, Tristate.TRUE,
        destStatus.isEmptyDirectory());
    // source does not exist.
    intercept(FileNotFoundException.class,
        () -> getEmptyDirStatus(sourceDir));
    // and verify that there's no dir marker hidden under a tombstone
    intercept(FileNotFoundException.class,
        () -> Invoker.once("HEAD", sourceDirMarker, () -> {
          ObjectMetadata md = fs.getObjectMetadata(sourceDirMarker);
          return String.format("Object %s of length %d",
              sourceDirMarker, md.getInstanceLength());
        }));

    // the parent dir mustn't be confused
    S3AFileStatus baseStatus = getEmptyDirStatus(basePath);
    assertEquals("Must not be an empty dir: " + baseStatus, Tristate.FALSE,
        baseStatus.isEmptyDirectory());
    // and verify the dest dir has a marker
    fs.getObjectMetadata(destDirMarker);
  }

  private S3AFileStatus getEmptyDirStatus(Path dir) throws IOException {
    return getFileSystem().innerGetFileStatus(dir, true, StatusProbeEnum.ALL);
  }

  @Test
  public void testEmptyDirs() throws Exception {
    S3AFileSystem fs = getFileSystem();
    assumeFilesystemHasMetadatastore(getFileSystem());
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

      S3AFileStatus status = fs.innerGetFileStatus(existingDir, true,
          StatusProbeEnum.ALL);
      assertEquals("Should not be empty dir", Tristate.FALSE,
          status.isEmptyDirectory());

      // 3. Assert that removing the only file the MetadataStore witnessed
      // being created doesn't cause it to think the directory is now empty.
      fs.delete(newFile, false);
      status = fs.innerGetFileStatus(existingDir, true, StatusProbeEnum.ALL);
      assertEquals("Should not be empty dir", Tristate.FALSE,
          status.isEmptyDirectory());

      // 4. Assert that removing the final file, that existed "before"
      // MetadataStore started, *does* cause the directory to be marked empty.
      fs.delete(existingFile, false);
      status = fs.innerGetFileStatus(existingDir, true, StatusProbeEnum.ALL);
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
  public void testTombstonesAndEmptyDirectories() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    assumeFilesystemHasMetadatastore(getFileSystem());

    // Create the first and last files.
    Path base = path(getMethodName());
    // use something ahead of all the ASCII alphabet characters so
    // even during parallel test runs, this test is expected to work.
    String first = "0000";
    Path firstPath = new Path(base, first);

    // this path is near the bottom of the ASCII string space.
    // This isn't so critical.
    String last = "zzzz";
    Path lastPath = new Path(base, last);
    touch(fs, firstPath);
    touch(fs, lastPath);
    // Delete first entry (+assert tombstone)
    assertDeleted(firstPath, false);
    DynamoDBMetadataStore ddbMs = getRequiredDDBMetastore(fs);
    DDBPathMetadata firstMD = ddbMs.get(firstPath);
    assertNotNull("No MD for " + firstPath, firstMD);
    assertTrue("Not a tombstone " + firstMD,
        firstMD.isDeleted());
    // PUT child to store going past the FS entirely.
    // This is not going to show up on S3Guard.
    Path child = new Path(firstPath, "child");
    StoreContext ctx = fs.createStoreContext();
    String childKey = ctx.pathToKey(child);
    String baseKey = ctx.pathToKey(base) + "/";
    AmazonS3 s3 = fs.getAmazonS3ClientForTesting("LIST");
    String bucket = ctx.getBucket();
    try {
      createEmptyObject(fs, childKey);

      // Do a list
      ListObjectsV2Request listReq = new ListObjectsV2Request()
          .withBucketName(bucket)
          .withPrefix(baseKey)
          .withMaxKeys(10)
          .withDelimiter("/");
      ListObjectsV2Result listing = s3.listObjectsV2(listReq);

      // the listing has the first path as a prefix, because of the child
      Assertions.assertThat(listing.getCommonPrefixes())
          .describedAs("The prefixes of a LIST of %s", base)
          .contains(baseKey + first + "/");

      // and the last file is one of the files
      Stream<String> files = listing.getObjectSummaries()
          .stream()
          .map(S3ObjectSummary::getKey);
      Assertions.assertThat(files)
          .describedAs("The files of a LIST of %s", base)
          .contains(baseKey + last);

      // verify absolutely that the last file exists
      assertPathExists("last file", lastPath);

      boolean isDDB = fs.getMetadataStore() instanceof DynamoDBMetadataStore;
      // if DDB is the metastore, then we expect no FS requests to be made
      // at all.
      S3ATestUtils.MetricDiff listMetric = new S3ATestUtils.MetricDiff(fs,
          Statistic.OBJECT_LIST_REQUEST);
      S3ATestUtils.MetricDiff getMetric = new S3ATestUtils.MetricDiff(fs,
          Statistic.OBJECT_METADATA_REQUESTS);
      // do a getFile status with empty dir flag
      S3AFileStatus status = getStatusWithEmptyDirFlag(fs, base);
      assertNonEmptyDir(status);
      if (isDDB) {
        listMetric.assertDiffEquals(
            "FileSystem called S3 LIST rather than use DynamoDB",
            0);
        getMetric.assertDiffEquals(
            "FileSystem called S3 GET rather than use DynamoDB",
            0);
        LOG.info("Verified that DDB directory status was accepted");
      }

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
   */
  private void createEmptyObject(S3AFileSystem fs, String key) {
    final InputStream im = new InputStream() {
      @Override
      public int read() {
        return -1;
      }
    };

    PutObjectRequest putObjectRequest = fs.newPutObjectRequest(key,
        fs.newObjectMetadata(0L),
        im);
    AmazonS3 s3 = fs.getAmazonS3ClientForTesting("PUT");
    s3.putObject(putObjectRequest);
  }

  @Test
  public void testDirMarkerDelete() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    assumeFilesystemHasMetadatastore(getFileSystem());
    Path baseDir = methodPath();
    Path subFile = new Path(baseDir, "subdir/file.txt");
    // adds the s3guard entry
    fs.mkdirs(baseDir);
    touch(fs, subFile);
    // PUT a marker
    createEmptyObject(fs, fs.pathToKey(baseDir) + "/");
    fs.delete(baseDir, true);
    assertPathDoesNotExist("Should have been deleted", baseDir);

    // now create the dir again
    fs.mkdirs(baseDir);
    FileStatus fileStatus = fs.getFileStatus(baseDir);
    Assertions.assertThat(fileStatus)
        .matches(FileStatus::isDirectory, "Not a directory");
    Assertions.assertThat(fs.listStatus(baseDir))
        .describedAs("listing of %s", baseDir)
        .isEmpty();
  }
}
