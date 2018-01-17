/**
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

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.store.EtagChecksum;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;

/**
 * Tests of the S3A FileSystem which don't have a specific home and can share
 * a filesystem instance with others..
 */
public class ITestS3AMiscOperations extends AbstractS3ATestBase {

  private static final byte[] HELLO = "hello".getBytes(StandardCharsets.UTF_8);

  @Test
  public void testCreateNonRecursiveSuccess() throws IOException {
    Path shouldWork = path("nonrecursivenode");
    try(FSDataOutputStream out = createNonRecursive(shouldWork)) {
      out.write(0);
      out.close();
    }
    assertIsFile(shouldWork);
  }

  @Test(expected = FileNotFoundException.class)
  public void testCreateNonRecursiveNoParent() throws IOException {
    createNonRecursive(path("/recursive/node"));
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void testCreateNonRecursiveParentIsFile() throws IOException {
    Path parent = path("/file.txt");
    touch(getFileSystem(), parent);
    createNonRecursive(new Path(parent, "fail"));
  }

  @Test
  public void testPutObjectDirect() throws Throwable {
    final S3AFileSystem fs = getFileSystem();
    ObjectMetadata metadata = fs.newObjectMetadata(-1);
    metadata.setContentLength(-1);
    Path path = path("putDirect");
    final PutObjectRequest put = new PutObjectRequest(fs.getBucket(),
        path.toUri().getPath(),
        new ByteArrayInputStream("PUT".getBytes()),
        metadata);
    LambdaTestUtils.intercept(IllegalStateException.class,
        () -> fs.putObjectDirect(put));
    assertPathDoesNotExist("put object was created", path);
  }

  private FSDataOutputStream createNonRecursive(Path path) throws IOException {
    return getFileSystem().createNonRecursive(path, false, 4096,
        (short) 3, (short) 4096,
        null);
  }

  /**
   * Touch a path, return the full path.
   * @param name relative name
   * @return the path
   * @throws IOException IO failure
   */
  Path touchFile(String name) throws IOException {
    Path path = path(name);
    touch(getFileSystem(), path);
    return path;
  }

  /**
   * Create a file with the data, return the path.
   * @param name relative name
   * @param data data to write
   * @return the path
   * @throws IOException IO failure
   */
  Path mkFile(String name, byte[] data) throws IOException {
    final Path f = path(name);
    createFile(getFileSystem(), f, true, data);
    return f;
  }

  /**
   * The assumption here is that 0-byte files uploaded in a single PUT
   * always have the same checksum, including stores with encryption.
   * @throws Throwable on a failure
   */
  @Test
  public void testEmptyFileChecksums() throws Throwable {
    final S3AFileSystem fs = getFileSystem();
    Path file1 = touchFile("file1");
    EtagChecksum checksum1 = fs.getFileChecksum(file1, 0);
    LOG.info("Checksum for {}: {}", file1, checksum1);
    assertNotNull("file 1 checksum", checksum1);
    assertNotEquals("file 1 checksum", 0, checksum1.getLength());
    assertEquals("checksums", checksum1,
        fs.getFileChecksum(touchFile("file2"), 0));
  }

  /**
   * Verify that different file contents have different
   * checksums, and that that they aren't the same as the empty file.
   * @throws Throwable failure
   */
  @Test
  public void testNonEmptyFileChecksums() throws Throwable {
    final S3AFileSystem fs = getFileSystem();
    final Path file3 = mkFile("file3", HELLO);
    final EtagChecksum checksum1 = fs.getFileChecksum(file3, 0);
    assertNotNull("file 3 checksum", checksum1);
    final Path file4 = touchFile("file4");
    final EtagChecksum checksum2 = fs.getFileChecksum(file4, 0);
    assertNotEquals("checksums", checksum1, checksum2);
    // overwrite
    createFile(fs, file4, true,
        "hello, world".getBytes(StandardCharsets.UTF_8));
    assertNotEquals(checksum2, fs.getFileChecksum(file4, 0));
  }

  /**
   * Verify that on an unencrypted store, the checksum of two non-empty
   * (single PUT) files is the same if the data is the same.
   * This will fail if the bucket has S3 default encryption enabled.
   * @throws Throwable failure
   */
  @Test
  public void testNonEmptyFileChecksumsUnencrypted() throws Throwable {
    Assume.assumeTrue(encryptionAlgorithm().equals(S3AEncryptionMethods.NONE));
    final S3AFileSystem fs = getFileSystem();
    final EtagChecksum checksum1 =
        fs.getFileChecksum(mkFile("file5", HELLO), 0);
    assertNotNull("file 3 checksum", checksum1);
    assertEquals("checksums", checksum1,
        fs.getFileChecksum(mkFile("file6", HELLO), 0));
  }

  private S3AEncryptionMethods encryptionAlgorithm() {
    return getFileSystem().getServerSideEncryptionAlgorithm();
  }

  @Test
  public void testNegativeLength() throws Throwable {
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () -> getFileSystem().getFileChecksum(mkFile("negative", HELLO), -1));
  }

  @Test
  public void testLengthPastEOF() throws Throwable {
    final S3AFileSystem fs = getFileSystem();
    Path f = mkFile("file5", HELLO);
    assertEquals(
        fs.getFileChecksum(f, HELLO.length),
        fs.getFileChecksum(f, HELLO.length * 2));
  }

  @Test
  public void testS3AToStringUnitialized() throws Throwable {
    try(S3AFileSystem fs = new S3AFileSystem()) {
      fs.toString();
    }
  }

}
