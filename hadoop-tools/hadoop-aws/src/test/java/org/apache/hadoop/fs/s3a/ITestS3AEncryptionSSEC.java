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

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.rm;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfEncryptionTestsDisabled;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.junit.Test;

/**
 * Concrete class that extends {@link AbstractTestS3AEncryption}
 * and tests SSE-C encryption.
 */
public class ITestS3AEncryptionSSEC extends AbstractTestS3AEncryption {

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM,
        getSSEAlgorithm().getMethod());
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_KEY,
        "4niV/jPK5VFRHY+KNb6wtqYd4xXyMgdJ9XQJpcQUVbs=");
    return conf;
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

    final Path[] path = new Path[1];
    intercept(java.nio.file.AccessDeniedException.class,
        "Service: Amazon S3; Status Code: 403;", () -> {

        int len = 2048;
        describe("Create an encrypted file of size " + len);
        String src = createFilename(len);
        path[0] = writeThenReadFile(src, len);

        //extract the test FS
        FileSystem fileSystem = createNewFileSystemWithSSECKey(
            "kX7SdwVc/1VXJr76kfKnkQ3ONYhxianyL2+C3rPVT9s=");
        byte[] data = dataset(len, 'a', 'z');
        ContractTestUtils.verifyFileContents(fileSystem, path[0], data);
        throw new Exception("Fail");
      });
  }

  /**
   * While each object has it's own key and should be distinct, this verifies
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

    final Path[] path = new Path[1];
    intercept(java.nio.file.AccessDeniedException.class,
        "Service: Amazon S3; Status Code: 403;", () -> {

        path[0] = S3ATestUtils.createTestPath(
          new Path(createFilename("dir/"))
        );
        Path nestedDirectory = S3ATestUtils.createTestPath(
            new Path(createFilename("dir/nestedDir/"))
        );
        FileSystem fsKeyB = createNewFileSystemWithSSECKey(
            "G61nz31Q7+zpjJWbakxfTOZW4VS0UmQWAq2YXhcTXoo=");
        getFileSystem().mkdirs(path[0]);
        fsKeyB.mkdirs(nestedDirectory);

        throw new Exception("Exception should be thrown.");
      });
    rm(getFileSystem(), path[0], true, false);
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

    final Path[] path = new Path[1];
    intercept(java.nio.file.AccessDeniedException.class,
        "Service: Amazon S3; Status Code: 403;", () -> {

        int len = 2048;
        String src = createFilename(len);
        path[0] = writeThenReadFile(src, len);

        FileSystem fsKeyB = createNewFileSystemWithSSECKey(
            "NTx0dUPrxoo9+LbNiT/gqf3z9jILqL6ilismFmJO50U=");
        fsKeyB.rename(path[0], new Path(createFilename("different-path.txt")));

        throw new Exception("Exception should be thrown.");
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

    String src = createFilename("original-path.txt");
    Path path = writeThenReadFile(src, 2048);
    Path newPath = path(createFilename("different-path.txt"));
    getFileSystem().rename(path, newPath);
    byte[] data = dataset(2048, 'a', 'z');
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

    Path nestedDirectory = S3ATestUtils.createTestPath(
         path(createFilename("/a/b/c/"))
    );
    assertTrue(getFileSystem().mkdirs(nestedDirectory));

    FileSystem fsKeyB = createNewFileSystemWithSSECKey(
        "msdo3VvvZznp66Gth58a91Hxe/UpExMkwU9BHkIjfW8=");

    fsKeyB.listFiles(S3ATestUtils.createTestPath(
        path(createFilename("/a/"))
    ), true);
    fsKeyB.listFiles(S3ATestUtils.createTestPath(
        path(createFilename("/a/b/"))
    ), true);

    //Until this point, no exception is thrown about access
    intercept(java.nio.file.AccessDeniedException.class,
        "Service: Amazon S3; Status Code: 403;", () -> {
        fsKeyB.listFiles(S3ATestUtils.createTestPath(
            path(createFilename("/a/b/c/"))
        ), false);
        throw new Exception("Exception should be thrown.");
      });

    Configuration conf = this.createConfiguration();
    conf.unset(Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM);
    conf.unset(Constants.SERVER_SIDE_ENCRYPTION_KEY);

    S3AContract contract = (S3AContract) createContract(conf);
    contract.init();
    FileSystem unencryptedFileSystem = contract.getTestFileSystem();

    //unencrypted can access until the final directory
    unencryptedFileSystem.listFiles(S3ATestUtils.createTestPath(
        path(createFilename("/a/"))
    ), true);
    unencryptedFileSystem.listFiles(S3ATestUtils.createTestPath(
        path(createFilename("/a/b/"))
    ), true);
    intercept(org.apache.hadoop.fs.s3a.AWSS3IOException.class,
        "Bad Request (Service: Amazon S3; Status Code: 400; Error" +
          " Code: 400 Bad Request;", () -> {

        unencryptedFileSystem.listFiles(S3ATestUtils.createTestPath(
            path(createFilename("/a/b/c/"))
        ), false);
        throw new Exception("Exception should be thrown.");
      });
    rm(getFileSystem(), path(createFilename("/")), true, false);
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

    Path nestedDirectory = S3ATestUtils.createTestPath(
         path(createFilename("/a/b/c/"))
    );
    assertTrue(getFileSystem().mkdirs(nestedDirectory));

    FileSystem fsKeyB = createNewFileSystemWithSSECKey(
        "msdo3VvvZznp66Gth58a91Hxe/UpExMkwU9BHkIjfW8=");

    fsKeyB.listStatus(S3ATestUtils.createTestPath(
        path(createFilename("/a/"))));
    fsKeyB.listStatus(S3ATestUtils.createTestPath(
        path(createFilename("/a/b/"))));

    //Until this point, no exception is thrown about access
    intercept(java.nio.file.AccessDeniedException.class,
        "Service: Amazon S3; Status Code: 403;", () -> {
        fsKeyB.listStatus(S3ATestUtils.createTestPath(
            path(createFilename("/a/b/c/"))));

        throw new Exception("Exception should be thrown.");
        });

    //Now try it with an unencrypted filesystem.
    Configuration conf = this.createConfiguration();
    conf.unset(Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM);
    conf.unset(Constants.SERVER_SIDE_ENCRYPTION_KEY);

    S3AContract contract = (S3AContract) createContract(conf);
    contract.init();
    FileSystem unencryptedFileSystem = contract.getTestFileSystem();

    //unencrypted can access until the final directory
    unencryptedFileSystem.listStatus(S3ATestUtils.createTestPath(
        path(createFilename("/a/"))));
    unencryptedFileSystem.listStatus(S3ATestUtils.createTestPath(
        path(createFilename("/a/b/"))));

    intercept(org.apache.hadoop.fs.s3a.AWSS3IOException.class,
        "Bad Request (Service: Amazon S3; Status Code: 400; Error Code: 400" +
        " Bad Request;", () -> {

        unencryptedFileSystem.listStatus(S3ATestUtils.createTestPath(
            path(createFilename("/a/b/c/"))));
        throw new Exception("Exception should be thrown.");
        });
    rm(getFileSystem(), path(createFilename("/")), true, false);
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

    Path nestedDirectory = S3ATestUtils.createTestPath(
        path(createFilename("/a/b/c/"))
    );
    assertTrue(getFileSystem().mkdirs(nestedDirectory));

    String src = createFilename("/a/b/c/fileToStat.txt");
    Path fileToStat =  writeThenReadFile(src, 2048);

    FileSystem fsKeyB = createNewFileSystemWithSSECKey(
        "msdo3VvvZznp66Gth58a91Hxe/UpExMkwU9BHkIjfW8=");

    //Until this point, no exception is thrown about access
    intercept(java.nio.file.AccessDeniedException.class,
        "Service: Amazon S3; Status Code: 403;", () -> {
        fsKeyB.listStatus(S3ATestUtils.createTestPath(fileToStat));

        throw new Exception("Exception should be thrown.");
      });
    rm(getFileSystem(), path(createFilename("/")), true, false);
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

    Path nestedDirectory = S3ATestUtils.createTestPath(
        path(createFilename("/a/b/c/"))
    );
    assertTrue(getFileSystem().mkdirs(nestedDirectory));
    String src = createFilename("/a/b/c/filetobedeleted.txt");
    Path fileToDelete =  writeThenReadFile(src, 2048);

    FileSystem fsKeyB = createNewFileSystemWithSSECKey(
        "msdo3VvvZznp66Gth58a91Hxe/UpExMkwU9BHkIjfW8=");
    intercept(java.nio.file.AccessDeniedException.class,
        "Forbidden (Service: Amazon S3; Status Code: 403; Error Code: " +
        "403 Forbidden", () -> {

        fsKeyB.delete(fileToDelete, false);
        throw new Exception("Exception should be thrown.");
      });

    //This is possible
    fsKeyB.delete(S3ATestUtils.createTestPath(
        path(createFilename("/a/b/c/"))), true);
    fsKeyB.delete(S3ATestUtils.createTestPath(
        path(createFilename("/a/b/"))), true);
    fsKeyB.delete(S3ATestUtils.createTestPath(
        path(createFilename("/a/"))), true);
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
