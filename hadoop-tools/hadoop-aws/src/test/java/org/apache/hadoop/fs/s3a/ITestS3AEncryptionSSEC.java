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
import java.util.concurrent.Callable;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Concrete class that extends {@link AbstractTestS3AEncryption}
 * and tests SSE-C encryption.
 */
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
   * Filesystem created with a different key.
   */
  private FileSystem fsKeyB;

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    disableFilesystemCaching(conf);
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM,
        getSSEAlgorithm().getMethod());
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_KEY, KEY_1);
    return conf;
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
    assumeEnabled();
    skipIfEncryptionTestsDisabled(getConfiguration());

    intercept(AccessDeniedException.class,
        SERVICE_AMAZON_S3_STATUS_CODE_403,
        new Callable<FileStatus>() {
          @Override
          public FileStatus call() throws Exception {
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
          }
        });
  }

  /**
   * While each object has its own key and should be distinct, this verifies
   * that hadoop treats object keys as a filesystem path.  So if a top level
   * dir is encrypted with keyA, a sublevel dir cannot be accessed with a
   * different keyB.
   *
   * This is expected AWS S3 SSE-C behavior.
   *
   * @throws Exception
   */
  @Test
  public void testCreateSubdirWithDifferentKey() throws Exception {
    assumeEnabled();
    skipIfEncryptionTestsDisabled(getConfiguration());
    assumeS3GuardState(false, getConfiguration());

    intercept(AccessDeniedException.class,
        "Service: Amazon S3; Status Code: 403;",
        new Callable<FileStatus>() {
          @Override
          public FileStatus call() throws Exception {
            Path base = path("testCreateSubdirWithDifferentKey");
            Path nestedDirectory = new Path(base, "nestedDir");
            fsKeyB = createNewFileSystemWithSSECKey(
                KEY_2);
            getFileSystem().mkdirs(base);
            fsKeyB.mkdirs(nestedDirectory);
            // expected to fail
            return fsKeyB.getFileStatus(nestedDirectory);
          }
        });
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
    assumeEnabled();
    skipIfEncryptionTestsDisabled(getConfiguration());

    intercept(AccessDeniedException.class,
        SERVICE_AMAZON_S3_STATUS_CODE_403,
        new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            int len = TEST_FILE_LEN;
            Path src = path(createFilename(len));
            writeThenReadFile(src, len);
            fsKeyB = createNewFileSystemWithSSECKey(KEY_3);
            Path dest = path(createFilename("different-path.txt"));
            getFileSystem().mkdirs(dest.getParent());
            return fsKeyB.rename(src, dest);
          }
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
    assumeEnabled();
    skipIfEncryptionTestsDisabled(getConfiguration());

    Path src = path("original-path.txt");
    writeThenReadFile(src, TEST_FILE_LEN);
    Path newPath = path("different-path.txt");
    getFileSystem().rename(src, newPath);
    byte[] data = dataset(TEST_FILE_LEN, 'a', 'z');
    ContractTestUtils.verifyFileContents(getFileSystem(), newPath, data);
  }

  /**
   * It is possible to list the contents of a directory up to the actual
   * end of the nested directories.  This is due to how S3A mocks the
   * directories and how prefixes work in S3.
   * @throws Exception
   */
  @Test
  public void testListEncryptedDir() throws Exception {
    assumeEnabled();
    skipIfEncryptionTestsDisabled(getConfiguration());
    assumeS3GuardState(false, getConfiguration());

    final Path pathABC = path("testListEncryptedDir/a/b/c/");
    final Path pathAB = pathABC.getParent();
    final Path pathA = pathAB.getParent();

    final Path nestedDirectory = createTestPath(pathABC);
    assertTrue(getFileSystem().mkdirs(nestedDirectory));

    fsKeyB = createNewFileSystemWithSSECKey(KEY_4);

    fsKeyB.listFiles(pathA, true);
    fsKeyB.listFiles(pathAB, true);

    //Until this point, no exception is thrown about access
    intercept(AccessDeniedException.class,
        SERVICE_AMAZON_S3_STATUS_CODE_403,
        new Callable<RemoteIterator<LocatedFileStatus>>() {
          @Override
          public RemoteIterator<LocatedFileStatus> call() throws Exception {
            return fsKeyB.listFiles(pathABC, false);
          }
        });

    Configuration conf = this.createConfiguration();
    conf.unset(Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM);
    conf.unset(Constants.SERVER_SIDE_ENCRYPTION_KEY);

    S3AContract contract = (S3AContract) createContract(conf);
    contract.init();
    final FileSystem unencryptedFileSystem = contract.getTestFileSystem();

    //unencrypted can access until the final directory
    unencryptedFileSystem.listFiles(pathA, true);
    unencryptedFileSystem.listFiles(pathAB, true);
    AWSS3IOException ex = intercept(AWSS3IOException.class,
        new Callable<RemoteIterator<LocatedFileStatus>>() {
          @Override
          public RemoteIterator<LocatedFileStatus> call() throws Exception {
            return unencryptedFileSystem.listFiles(pathABC, false);
          }
        });
    assertStatusCode(ex, 400);
  }

  /**
   * Much like the above list encrypted directory test, you cannot get the
   * metadata of an object without the correct encryption key.
   * @throws Exception
   */
  @Test
  public void testListStatusEncryptedDir() throws Exception {
    assumeEnabled();
    skipIfEncryptionTestsDisabled(getConfiguration());
    assumeS3GuardState(false, getConfiguration());

    final Path pathABC = path("testListStatusEncryptedDir/a/b/c/");
    final Path pathAB = pathABC.getParent();
    final Path pathA = pathAB.getParent();
    assertTrue(getFileSystem().mkdirs(pathABC));

    fsKeyB = createNewFileSystemWithSSECKey(KEY_4);

    fsKeyB.listStatus(pathA);
    fsKeyB.listStatus(pathAB);

    //Until this point, no exception is thrown about access
    intercept(AccessDeniedException.class,
        SERVICE_AMAZON_S3_STATUS_CODE_403,
        new Callable<FileStatus[]>() {
          @Override
          public FileStatus[] call() throws Exception {
            return fsKeyB.listStatus(pathABC);
          }
        });

    //Now try it with an unencrypted filesystem.
    Configuration conf = createConfiguration();
    conf.unset(Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM);
    conf.unset(Constants.SERVER_SIDE_ENCRYPTION_KEY);

    S3AContract contract = (S3AContract) createContract(conf);
    contract.init();
    final FileSystem unencryptedFileSystem = contract.getTestFileSystem();

    //unencrypted can access until the final directory
    unencryptedFileSystem.listStatus(pathA);
    unencryptedFileSystem.listStatus(pathAB);

    AWSS3IOException ex = intercept(AWSS3IOException.class,
        new Callable<FileStatus[]>() {
          @Override
          public FileStatus[] call() throws Exception {
            return unencryptedFileSystem.listStatus(pathABC);
          }
        });
    assertStatusCode(ex, 400);
  }

  /**
   * Much like trying to access a encrypted directory, an encrypted file cannot
   * have its metadata read, since both are technically an object.
   * @throws Exception
   */
  @Test
  public void testListStatusEncryptedFile() throws Exception {
    assumeEnabled();
    skipIfEncryptionTestsDisabled(getConfiguration());
    assumeS3GuardState(false, getConfiguration());
    final Path pathABC = path("testListStatusEncryptedFile/a/b/c/");
    assertTrue(getFileSystem().mkdirs(pathABC));

    final Path fileToStat = new Path(pathABC, "fileToStat.txt");
    writeThenReadFile(fileToStat, TEST_FILE_LEN);

    fsKeyB = createNewFileSystemWithSSECKey(KEY_4);

    //Until this point, no exception is thrown about access
    intercept(AccessDeniedException.class,
        SERVICE_AMAZON_S3_STATUS_CODE_403,
        new Callable<FileStatus[]>() {
          @Override
          public FileStatus[] call() throws Exception {
            return fsKeyB.listStatus(S3ATestUtils.createTestPath(fileToStat));
      }});
  }


  /**
   * It is possible to delete directories without the proper encryption key and
   * the hierarchy above it.
   *
   * @throws Exception
   */
  @Test
  public void testDeleteEncryptedObjectWithDifferentKey() throws Exception {
    assumeEnabled();
    skipIfEncryptionTestsDisabled(getConfiguration());
    assumeS3GuardState(false, getConfiguration());
    final Path pathABC = path("testDeleteEncryptedObjectWithDifferentKey/a/b/c/");

    final Path pathAB = pathABC.getParent();
    final Path pathA = pathAB.getParent();
    assertTrue(getFileSystem().mkdirs(pathABC));
    final Path fileToDelete = new Path(pathABC, "filetobedeleted.txt");
    writeThenReadFile(fileToDelete, TEST_FILE_LEN);
    fsKeyB = createNewFileSystemWithSSECKey(KEY_4);
    intercept(AccessDeniedException.class,
        SERVICE_AMAZON_S3_STATUS_CODE_403,
          new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
              return fsKeyB.delete(fileToDelete, false);
            }
          });

    //This is possible
    fsKeyB.delete(pathABC, true);
    fsKeyB.delete(pathAB, true);
    fsKeyB.delete(pathA, true);
    assertPathDoesNotExist("expected recursive delete", fileToDelete);
  }

  private FileSystem createNewFileSystemWithSSECKey(String sseCKey) throws
      IOException {
    Configuration conf = this.createConfiguration();
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_KEY, sseCKey);

    S3AContract contract = (S3AContract) createContract(conf);
    contract.init();
    FileSystem fileSystem = contract.getTestFileSystem();
    return fileSystem;
  }

  @Override
  protected S3AEncryptionMethods getSSEAlgorithm() {
    return S3AEncryptionMethods.SSE_C;
  }

}
