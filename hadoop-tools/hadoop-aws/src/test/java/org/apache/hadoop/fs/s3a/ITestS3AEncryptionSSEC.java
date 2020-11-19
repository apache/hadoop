/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.Arrays;
import java.util.Collection;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_KEEP;
import static org.apache.hadoop.fs.s3a.Constants.ETAG_CHECKSUM_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Concrete class that extends {@link AbstractTestS3AEncryption}
 * and tests SSE-C encryption.
 * HEAD requests against SSE-C-encrypted data will fail if the wrong key
 * is presented, so the tests are very brittle to S3Guard being on vs. off.
 * Equally "vexing" has been the optimizations of getFileStatus(), wherein
 * LIST comes before HEAD path + /
 */
@RunWith(Parameterized.class)
public class ITestS3AEncryptionSSEC extends AbstractTestS3AEncryption {

  private static final String SERVICE_AMAZON_S3_STATUS_CODE_403
      = "Service: Amazon S3; Status Code: 403;";
  private static final String KEY_1
      = "4niV/jPK5VFRHY+KNb6wtqYd4xXyMgdJ9XQJpcQUVbs=";
  private static final String KEY_2
      = "G61nz31Q7+zpjJWbakxfTOZW4VS0UmQWAq2YXhcTXoo=";
  private static final String KEY_3
      = "NTx0dUPrxoo9+LbNiT/gqf3z9jILqL6ilismFmJO50U=";
  private static final String KEY_4
      = "msdo3VvvZznp66Gth58a91Hxe/UpExMkwU9BHkIjfW8=";
  private static final int TEST_FILE_LEN = 2048;

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"raw-keep-markers", false, true},
        {"raw-delete-markers", false, false},
        {"guarded-keep-markers", true, true},
        {"guarded-delete-markers", true, false}
    });
  }

  /**
   * Parameter: should the stores be guarded?
   */
  private final boolean s3guard;

  /**
   * Parameter: should directory markers be retained?
   */
  private final boolean keepMarkers;

  /**
   * Filesystem created with a different key.
   */
  private S3AFileSystem fsKeyB;

  public ITestS3AEncryptionSSEC(final String name,
      final boolean s3guard,
      final boolean keepMarkers) {
    this.s3guard = s3guard;
    this.keepMarkers = keepMarkers;
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    disableFilesystemCaching(conf);
    String bucketName = getTestBucketName(conf);
    removeBucketOverrides(bucketName, conf,
        S3_METADATA_STORE_IMPL);
    if (!s3guard) {
      // in a raw run remove all s3guard settings
      removeBaseAndBucketOverrides(bucketName, conf,
          S3_METADATA_STORE_IMPL);
    }
    // directory marker options
    removeBaseAndBucketOverrides(bucketName, conf,
        DIRECTORY_MARKER_POLICY,
        ETAG_CHECKSUM_ENABLED,
        SERVER_SIDE_ENCRYPTION_ALGORITHM,
        SERVER_SIDE_ENCRYPTION_KEY);
    conf.set(DIRECTORY_MARKER_POLICY,
        keepMarkers
            ? DIRECTORY_MARKER_POLICY_KEEP
            : DIRECTORY_MARKER_POLICY_DELETE);
    conf.set(SERVER_SIDE_ENCRYPTION_ALGORITHM,
        getSSEAlgorithm().getMethod());
    conf.set(SERVER_SIDE_ENCRYPTION_KEY, KEY_1);
    conf.setBoolean(ETAG_CHECKSUM_ENABLED, true);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    assumeEnabled();
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
    IOUtils.closeStream(fsKeyB);
  }

  /**
   * This will create and write to a file using encryption key A, then attempt
   * to read from it again with encryption key B.  This will not work as it
   * cannot decrypt the file.
   *
   * This is expected AWS S3 SSE-C behavior.
   *
   * @throws Exception
   */
  @Test
  public void testCreateFileAndReadWithDifferentEncryptionKey() throws
      Exception {
    intercept(AccessDeniedException.class,
        SERVICE_AMAZON_S3_STATUS_CODE_403,
        () -> {
          int len = TEST_FILE_LEN;
          describe("Create an encrypted file of size " + len);
          Path src = path("testCreateFileAndReadWithDifferentEncryptionKey");
          writeThenReadFile(src, len);

          //extract the test FS
          fsKeyB = createNewFileSystemWithSSECKey(
              "kX7SdwVc/1VXJr76kfKnkQ3ONYhxianyL2+C3rPVT9s=");
          byte[] data = dataset(len, 'a', 'z');
          ContractTestUtils.verifyFileContents(fsKeyB, src, data);
          return fsKeyB.getFileStatus(src);
        });
  }

  /**
   *
   * You can use a different key under a sub directory, even if you
   * do not have permissions to read the marker.
   * @throws Exception
   */
  @Test
  public void testCreateSubdirWithDifferentKey() throws Exception {
    Path base = path("testCreateSubdirWithDifferentKey");
    Path nestedDirectory = new Path(base, "nestedDir");
    fsKeyB = createNewFileSystemWithSSECKey(
        KEY_2);
    getFileSystem().mkdirs(base);
    fsKeyB.mkdirs(nestedDirectory);
  }

  /**
   * Ensures a file can't be created with keyA and then renamed with a different
   * key.
   *
   * This is expected AWS S3 SSE-C behavior.
   *
   * @throws Exception
   */
  @Test
  public void testCreateFileThenMoveWithDifferentSSECKey() throws Exception {
    intercept(AccessDeniedException.class,
        SERVICE_AMAZON_S3_STATUS_CODE_403,
        () -> {
          int len = TEST_FILE_LEN;
          Path src = path(createFilename(len));
          writeThenReadFile(src, len);
          fsKeyB = createNewFileSystemWithSSECKey(KEY_3);
          Path dest = path(createFilename("different-path.txt"));
          getFileSystem().mkdirs(dest.getParent());
          return fsKeyB.rename(src, dest);
        });
  }

  /**
   * General test to make sure move works with SSE-C with the same key, unlike
   * with multiple keys.
   *
   * @throws Exception
   */
  @Test
  public void testRenameFile() throws Exception {
    Path src = path("original-path.txt");
    writeThenReadFile(src, TEST_FILE_LEN);
    Path newPath = path("different-path.txt");
    getFileSystem().rename(src, newPath);
    byte[] data = dataset(TEST_FILE_LEN, 'a', 'z');
    ContractTestUtils.verifyFileContents(getFileSystem(), newPath, data);
  }

  /**
   * Directory listings always work.
   * @throws Exception
   */
  @Test
  public void testListEncryptedDir() throws Exception {

    Path pathABC = path("testListEncryptedDir/a/b/c/");
    Path pathAB = pathABC.getParent();
    Path pathA = pathAB.getParent();

    Path nestedDirectory = createTestPath(pathABC);
    assertTrue(getFileSystem().mkdirs(nestedDirectory));

    fsKeyB = createNewFileSystemWithSSECKey(KEY_4);

    fsKeyB.listFiles(pathA, true);
    fsKeyB.listFiles(pathAB, true);
    fsKeyB.listFiles(pathABC, false);

    Configuration conf = this.createConfiguration();
    conf.unset(SERVER_SIDE_ENCRYPTION_ALGORITHM);
    conf.unset(SERVER_SIDE_ENCRYPTION_KEY);

    S3AContract contract = (S3AContract) createContract(conf);
    contract.init();
    FileSystem unencryptedFileSystem = contract.getTestFileSystem();

    //unencrypted can access until the final directory
    unencryptedFileSystem.listFiles(pathA, true);
    unencryptedFileSystem.listFiles(pathAB, true);
    unencryptedFileSystem.listFiles(pathABC, false);
  }

  /**
   * listStatus also works with encrypted directories and key mismatch.
   */
  @Test
  public void testListStatusEncryptedDir() throws Exception {

    Path pathABC = path("testListStatusEncryptedDir/a/b/c/");
    Path pathAB = pathABC.getParent();
    Path pathA = pathAB.getParent();
    assertTrue(getFileSystem().mkdirs(pathABC));

    fsKeyB = createNewFileSystemWithSSECKey(KEY_4);

    fsKeyB.listStatus(pathA);
    fsKeyB.listStatus(pathAB);

    // this used to raise 403, but with LIST before HEAD,
    // no longer true.
    fsKeyB.listStatus(pathABC);

    //Now try it with an unencrypted filesystem.
    Configuration conf = createConfiguration();
    conf.unset(SERVER_SIDE_ENCRYPTION_ALGORITHM);
    conf.unset(SERVER_SIDE_ENCRYPTION_KEY);

    S3AContract contract = (S3AContract) createContract(conf);
    contract.init();
    FileSystem unencryptedFileSystem = contract.getTestFileSystem();

    //unencrypted can access until the final directory
    unencryptedFileSystem.listStatus(pathA);
    unencryptedFileSystem.listStatus(pathAB);
    unencryptedFileSystem.listStatus(pathABC);
  }

  /**
   * An encrypted file cannot have its metadata read.
   * @throws Exception
   */
  @Test
  public void testListStatusEncryptedFile() throws Exception {
    Path pathABC = path("testListStatusEncryptedFile/a/b/c/");
    assertTrue("mkdirs failed", getFileSystem().mkdirs(pathABC));

    Path fileToStat = new Path(pathABC, "fileToStat.txt");
    writeThenReadFile(fileToStat, TEST_FILE_LEN);

    fsKeyB = createNewFileSystemWithSSECKey(KEY_4);

    //Until this point, no exception is thrown about access
    if (statusProbesCheckS3(fsKeyB, fileToStat)) {
      intercept(AccessDeniedException.class,
          SERVICE_AMAZON_S3_STATUS_CODE_403,
          () -> fsKeyB.listStatus(fileToStat));
    } else {
      fsKeyB.listStatus(fileToStat);
    }
  }

  /**
   * Do file status probes check S3?
   * @param fs filesystem
   * @param path file path
   * @return true if check for a path being a file will issue a HEAD request.
   */
  private boolean statusProbesCheckS3(S3AFileSystem fs, Path path) {
    return !fs.hasMetadataStore() || !fs.allowAuthoritative(path);
  }

  /**
   * It is possible to delete directories without the proper encryption key and
   * the hierarchy above it.
   *
   * @throws Exception
   */
  @Test
  public void testDeleteEncryptedObjectWithDifferentKey() throws Exception {
    //requireUnguardedFilesystem();
    Path pathABC = path("testDeleteEncryptedObjectWithDifferentKey/a/b/c/");

    Path pathAB = pathABC.getParent();
    Path pathA = pathAB.getParent();
    assertTrue(getFileSystem().mkdirs(pathABC));
    Path fileToDelete = new Path(pathABC, "filetobedeleted.txt");
    writeThenReadFile(fileToDelete, TEST_FILE_LEN);
    fsKeyB = createNewFileSystemWithSSECKey(KEY_4);
    if (statusProbesCheckS3(fsKeyB, fileToDelete)) {
      intercept(AccessDeniedException.class,
          SERVICE_AMAZON_S3_STATUS_CODE_403,
          () -> fsKeyB.delete(fileToDelete, false));
    } else {
      fsKeyB.delete(fileToDelete, false);
    }
    //This is possible
    fsKeyB.delete(pathABC, true);
    fsKeyB.delete(pathAB, true);
    fsKeyB.delete(pathA, true);
    assertPathDoesNotExist("expected recursive delete", fileToDelete);
  }

  /**
   * getFileChecksum always goes to S3, so when
   * the caller lacks permissions, it fails irrespective
   * of guard.
   */
  @Test
  public void testChecksumRequiresReadAccess() throws Throwable {
    Path path = path("tagged-file");
    S3AFileSystem fs = getFileSystem();
    touch(fs, path);
    Assertions.assertThat(fs.getFileChecksum(path))
        .isNotNull();
    fsKeyB = createNewFileSystemWithSSECKey(KEY_4);
    intercept(AccessDeniedException.class,
        SERVICE_AMAZON_S3_STATUS_CODE_403,
        () -> fsKeyB.getFileChecksum(path));
  }

  private S3AFileSystem createNewFileSystemWithSSECKey(String sseCKey) throws
      IOException {
    Configuration conf = this.createConfiguration();
    conf.set(SERVER_SIDE_ENCRYPTION_KEY, sseCKey);

    S3AContract contract = (S3AContract) createContract(conf);
    contract.init();
    FileSystem fileSystem = contract.getTestFileSystem();
    return (S3AFileSystem) fileSystem;
  }

  @Override
  protected S3AEncryptionMethods getSSEAlgorithm() {
    return S3AEncryptionMethods.SSE_C;
  }

}
