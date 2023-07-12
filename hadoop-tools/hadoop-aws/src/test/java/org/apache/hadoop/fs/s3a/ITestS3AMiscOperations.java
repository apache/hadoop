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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetBucketEncryptionResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.fs.store.EtagChecksum;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertHasPathCapabilities;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertLacksPathCapabilities;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.XA_ETAG;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests of the S3A FileSystem which don't have a specific home and can share
 * a filesystem instance with others.
 * Checksums are turned on unless explicitly disabled for a test case.
 */
public class ITestS3AMiscOperations extends AbstractS3ATestBase {

  private static final byte[] HELLO = "hello".getBytes(StandardCharsets.UTF_8);

  @Override
  public void setup() throws Exception {
    super.setup();
    // checksums are forced on.
    enableChecksums(true);
  }

  @SuppressWarnings("deprecation")
  @Override
  protected Configuration createConfiguration() {
    final Configuration conf = super.createConfiguration();
    removeBaseAndBucketOverrides(conf,
        S3_ENCRYPTION_ALGORITHM,
        S3_ENCRYPTION_KEY,
        SERVER_SIDE_ENCRYPTION_ALGORITHM,
        SERVER_SIDE_ENCRYPTION_KEY);
    return conf;
  }

  /**
   * Turn checksums on.
   * Relies on the FS not caching the configuration option
   * @param enabled enabled flag.
   */
  protected void enableChecksums(final boolean enabled) {
    getFileSystem().getConf().setBoolean(Constants.ETAG_CHECKSUM_ENABLED,
        enabled);
  }

  @Test
  public void testCreateNonRecursiveSuccess() throws IOException {
    Path shouldWork = path("nonrecursivenode");
    try(FSDataOutputStream out = createNonRecursive(shouldWork)) {
      out.write(0);
      out.close();
    }
    assertIsFile(shouldWork);
  }

  @Test
  public void testPutObjectDirect() throws Throwable {
    final S3AFileSystem fs = getFileSystem();
    try (AuditSpan span = span()) {
      ObjectMetadata metadata = fs.newObjectMetadata(-1);
      metadata.setContentLength(-1);
      Path path = path("putDirect");
      final PutObjectRequest put = new PutObjectRequest(fs.getBucket(),
          path.toUri().getPath(),
          new ByteArrayInputStream("PUT".getBytes()),
          metadata);
      LambdaTestUtils.intercept(IllegalStateException.class,
          () -> fs.putObjectDirect(put, PutObjectOptions.keepingDirs(), null));
      assertPathDoesNotExist("put object was created", path);
    }
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
   * This will be skipped if the bucket has S3 default encryption enabled.
   * @throws Throwable on a failure
   */
  @Test
  public void testEmptyFileChecksums() throws Throwable {
    assumeNoDefaultEncryption();
    final S3AFileSystem fs = getFileSystem();
    Path file1 = touchFile("file1");
    EtagChecksum checksum1 = fs.getFileChecksum(file1, 0);
    LOG.info("Checksum for {}: {}", file1, checksum1);
    assertHasPathCapabilities(fs, file1,
        CommonPathCapabilities.FS_CHECKSUMS);
    assertNotNull("Null file 1 checksum", checksum1);
    assertNotEquals("file 1 checksum", 0, checksum1.getLength());
    assertEquals("checksums of empty files", checksum1,
        fs.getFileChecksum(touchFile("file2"), 0));
    Assertions.assertThat(fs.getXAttr(file1, XA_ETAG))
        .describedAs("etag from xattr")
        .isEqualTo(checksum1.getBytes());
  }

  /**
   * Skip a test if we can get the default encryption on a bucket and it is
   * non-null.
   */
  private void assumeNoDefaultEncryption() throws IOException {
    try {
      skipIfClientSideEncryption();
      Assume.assumeThat(getDefaultEncryption(), nullValue());
    } catch (AccessDeniedException e) {
      // if the user can't check the default encryption, assume that it is
      // null and keep going
      LOG.warn("User does not have permission to call getBucketEncryption()");
    }
  }

  /**
   * Make sure that when checksums are disabled, the caller
   * gets null back.
   */
  @Test
  public void testChecksumDisabled() throws Throwable {
    // checksums are forced off.
    enableChecksums(false);
    final S3AFileSystem fs = getFileSystem();
    Path file1 = touchFile("file1");
    EtagChecksum checksum1 = fs.getFileChecksum(file1, 0);
    assertLacksPathCapabilities(fs, file1,
        CommonPathCapabilities.FS_CHECKSUMS);
    assertNull("Checksums are being generated", checksum1);
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
    Assertions.assertThat(fs.getXAttr(file3, XA_ETAG))
        .describedAs("etag from xattr")
        .isEqualTo(checksum1.getBytes());
  }

  /**
   * Verify that on an unencrypted store, the checksum of two non-empty
   * (single PUT) files is the same if the data is the same.
   * This will be skipped if the bucket has S3 default encryption enabled.
   * @throws Throwable failure
   */
  @Test
  public void testNonEmptyFileChecksumsUnencrypted() throws Throwable {
    Assume.assumeTrue(encryptionAlgorithm().equals(S3AEncryptionMethods.NONE));
    assumeNoDefaultEncryption();
    final S3AFileSystem fs = getFileSystem();
    final EtagChecksum checksum1 =
        fs.getFileChecksum(mkFile("file5", HELLO), 0);
    assertNotNull("file 3 checksum", checksum1);
    assertEquals("checksums", checksum1,
        fs.getFileChecksum(mkFile("file6", HELLO), 0));
  }

  private S3AEncryptionMethods encryptionAlgorithm() {
    return getFileSystem().getS3EncryptionAlgorithm();
  }

  @Test
  public void testNegativeLength() throws Throwable {
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () -> getFileSystem().getFileChecksum(mkFile("negative", HELLO), -1));
  }

  @Test
  public void testNegativeLengthDisabledChecksum() throws Throwable {
    enableChecksums(false);
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () -> getFileSystem().getFileChecksum(mkFile("negative", HELLO), -1));
  }

  @Test
  public void testChecksumLengthPastEOF() throws Throwable {
    enableChecksums(true);
    final S3AFileSystem fs = getFileSystem();
    Path f = mkFile("file5", HELLO);
    EtagChecksum l = fs.getFileChecksum(f, HELLO.length);
    assertNotNull("Null checksum", l);
    assertEquals(l, fs.getFileChecksum(f, HELLO.length * 2));
  }

  @Test
  public void testS3AToStringUnitialized() throws Throwable {
    try(S3AFileSystem fs = new S3AFileSystem()) {
      fs.toString();
    }
  }

  @Test
  public void testS3AIOStatisticsUninitialized() throws Throwable {
    try (S3AFileSystem fs = new S3AFileSystem()) {
      fs.getIOStatistics();
    }

  }
  /**
   * Verify that paths with a trailing "/" are fixed up.
   */
  @Test
  public void testPathFixup() throws Throwable {
    final S3AFileSystem fs = getFileSystem();
    Path path = fs.makeQualified(new Path("path"));
    String trailing = path.toUri().toString() + "/";
    verifyNoTrailingSlash("path from string",
        new Path(trailing));

    // here the problem: the URI constructor doesn't strip trailing "/" chars
    URI trailingURI = verifyTrailingSlash("trailingURI", new URI(trailing));
    Path pathFromTrailingURI =
        verifyTrailingSlash("pathFromTrailingURI", new Path(trailingURI));

    // here is the fixup
    verifyNoTrailingSlash(
        "path from fs.makeQualified()",
        fs.makeQualified(pathFromTrailingURI));
  }

  /**
   * Verify that paths with a trailing "//" are fixed up.
   */
  @Test
  public void testPathDoubleSlashFixup() throws Throwable {
    final S3AFileSystem fs = getFileSystem();
    Path path = fs.makeQualified(new Path("path"));
    String trailing2 = path.toUri().toString() + "//";
    verifyNoTrailingSlash("path from string",
        new Path(trailing2));

    // here the problem: the URI constructor doesn't strip trailing "/" chars
    URI trailingURI = new URI(trailing2);
    Path pathFromTrailingURI =
        verifyTrailingSlash("pathFromTrailingURI", new Path(trailingURI));

    // here is the fixup
    verifyNoTrailingSlash(
        "path from fs.makeQualified()",
        fs.makeQualified(pathFromTrailingURI));
  }

  /**
   * Verify that root path fixup does retain any trailing "/", because
   * that matters.
   */
  @Test
  public void testRootPathFixup() throws Throwable {
    final S3AFileSystem fs = getFileSystem();
    // fs.getURI() actually returns a path without any trailing /
    String baseFsURI = fs.getUri().toString();
    Path rootPath_from_FS_URI = verifyNoTrailingSlash("root", new Path(baseFsURI));

    // add a single / to a string
    String trailing = verifyTrailingSlash("FS URI",
        baseFsURI + "/");
    Path root_path_from_trailing_string =
        verifyTrailingSlash("root path from string", new Path(trailing));

    // now verify that the URI constructor retrains that /
    URI trailingURI = verifyTrailingSlash("trailingURI", new URI(trailing));
    Path pathFromTrailingURI =
        verifyTrailingSlash("pathFromTrailingURI", new Path(trailingURI));

    // Root path fixup is expected to retain that trailing /
    Path pathFromQualify = verifyTrailingSlash(
        "path from fs.makeQualified()",
        fs.makeQualified(pathFromTrailingURI));
    assertEquals(root_path_from_trailing_string, pathFromQualify);

    // and if you fix up the root path without a string, you get
    // back a root path without a string
    Path pathFromRootQualify = verifyNoTrailingSlash(
        "path from fs.makeQualified(" + baseFsURI +")",
        fs.makeQualified(rootPath_from_FS_URI));

    assertEquals(rootPath_from_FS_URI, pathFromRootQualify);
    assertNotEquals(rootPath_from_FS_URI, root_path_from_trailing_string);
  }

  /**
   * Verify that an object's string value path has a single trailing / symbol;
   * returns the object.
   * @param role role for error messages
   * @param o object
   * @param <T> type of object
   * @return the object.
   */
  private static <T> T verifyTrailingSlash(String role, T o) {
    String s = o.toString();
    assertTrue(role + " lacks trailing slash " + s,
        s.endsWith("/"));
    assertFalse(role + " has double trailing slash " + s,
        s.endsWith("//"));
    return o;
  }

  /**
   * Verify that an object's string value path has no trailing / symbol;
   * returns the object.
   * @param role role for error messages
   * @param o object
   * @param <T> type of object
   * @return the object.
   */
  private static <T> T verifyNoTrailingSlash(String role, T o) {
    String s = o.toString();
    assertFalse(role + " has trailing slash " + s,
        s.endsWith("/"));
    return o;
  }

  /**
   * Gets default encryption settings for the bucket or returns null if default
   * encryption is disabled.
   */
  private GetBucketEncryptionResult getDefaultEncryption() throws IOException {
    S3AFileSystem fs = getFileSystem();
    AmazonS3 s3 = fs.getAmazonS3ClientForTesting("check default encryption");
    try {
      return Invoker.once("getBucketEncryption()",
          fs.getBucket(),
          () -> s3.getBucketEncryption(fs.getBucket()));
    } catch (FileNotFoundException e) {
      return null;
    }
  }

}
