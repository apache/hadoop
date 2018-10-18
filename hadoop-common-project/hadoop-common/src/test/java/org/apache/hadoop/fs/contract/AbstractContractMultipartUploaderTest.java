/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.fs.contract;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Charsets;
import org.junit.Test;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.BBUploadHandle;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MultipartUploader;
import org.apache.hadoop.fs.MultipartUploaderFactory;
import org.apache.hadoop.fs.PartHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.UploadHandle;

import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyPathExists;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public abstract class AbstractContractMultipartUploaderTest extends
    AbstractFSContractTestBase {

  /**
   * The payload is the part number repeated for the length of the part.
   * This makes checking the correctness of the upload straightforward.
   * @param partNumber part number
   * @return the bytes to upload.
   */
  private byte[] generatePayload(int partNumber) {
    int sizeInBytes = partSizeInBytes();
    ByteBuffer buffer = ByteBuffer.allocate(sizeInBytes);
    for (int i=0 ; i < sizeInBytes/(Integer.SIZE/Byte.SIZE); ++i) {
      buffer.putInt(partNumber);
    }
    return buffer.array();
  }

  /**
   * Load a path, make an MD5 digest.
   * @param path path to load
   * @return the digest array
   * @throws IOException failure to read or digest the file.
   */
  protected byte[] digest(Path path) throws IOException {
    FileSystem fs = getFileSystem();
    try (InputStream in = fs.open(path)) {
      byte[] fdData = IOUtils.toByteArray(in);
      MessageDigest newDigest = DigestUtils.getMd5Digest();
      return newDigest.digest(fdData);
    }
  }

  /**
   * Get the partition size in bytes to use for each upload.
   * @return a number > 0
   */
  protected abstract int partSizeInBytes();

  /**
   * Get the number of test payloads to upload.
   * @return a number > 1
   */
  protected int getTestPayloadCount() {
    return 10;
  }

  /**
   * Assert that a multipart upload is successful.
   * @throws Exception failure
   */
  @Test
  public void testSingleUpload() throws Exception {
    FileSystem fs = getFileSystem();
    Path file = path("testSingleUpload");
    MultipartUploader mpu = MultipartUploaderFactory.get(fs, null);
    UploadHandle uploadHandle = mpu.initialize(file);
    List<Pair<Integer, PartHandle>> partHandles = new ArrayList<>();
    MessageDigest origDigest = DigestUtils.getMd5Digest();
    byte[] payload = generatePayload(1);
    origDigest.update(payload);
    InputStream is = new ByteArrayInputStream(payload);
    PartHandle partHandle = mpu.putPart(file, is, 1, uploadHandle,
        payload.length);
    partHandles.add(Pair.of(1, partHandle));
    PathHandle fd = completeUpload(file, mpu, uploadHandle, partHandles,
        origDigest,
        payload.length);

    // Complete is idempotent
    PathHandle fd2 = mpu.complete(file, partHandles, uploadHandle);
    assertArrayEquals("Path handles differ", fd.toByteArray(),
        fd2.toByteArray());
  }

  private PathHandle completeUpload(final Path file,
      final MultipartUploader mpu,
      final UploadHandle uploadHandle,
      final List<Pair<Integer, PartHandle>> partHandles,
      final MessageDigest origDigest,
      final int expectedLength) throws IOException {
    PathHandle fd = mpu.complete(file, partHandles, uploadHandle);

    FileStatus status = verifyPathExists(getFileSystem(),
        "Completed file", file);
    assertEquals("length of " + status,
        expectedLength, status.getLen());

    assertArrayEquals("digest of source and " + file
            + " differ",
        origDigest.digest(), digest(file));
    return fd;
  }

  /**
   * Assert that a multipart upload is successful.
   * @throws Exception failure
   */
  @Test
  public void testMultipartUpload() throws Exception {
    FileSystem fs = getFileSystem();
    Path file = path("testMultipartUpload");
    MultipartUploader mpu = MultipartUploaderFactory.get(fs, null);
    UploadHandle uploadHandle = mpu.initialize(file);
    List<Pair<Integer, PartHandle>> partHandles = new ArrayList<>();
    MessageDigest origDigest = DigestUtils.getMd5Digest();
    final int payloadCount = getTestPayloadCount();
    for (int i = 1; i <= payloadCount; ++i) {
      byte[] payload = generatePayload(i);
      origDigest.update(payload);
      InputStream is = new ByteArrayInputStream(payload);
      PartHandle partHandle = mpu.putPart(file, is, i, uploadHandle,
          payload.length);
      partHandles.add(Pair.of(i, partHandle));
    }
    completeUpload(file, mpu, uploadHandle, partHandles, origDigest,
        payloadCount * partSizeInBytes());
  }

  /**
   * Assert that a multipart upload is successful when a single empty part is
   * uploaded.
   * @throws Exception failure
   */
  @Test
  public void testMultipartUploadEmptyPart() throws Exception {
    FileSystem fs = getFileSystem();
    Path file = path("testMultipartUpload");
    MultipartUploader mpu = MultipartUploaderFactory.get(fs, null);
    UploadHandle uploadHandle = mpu.initialize(file);
    List<Pair<Integer, PartHandle>> partHandles = new ArrayList<>();
    MessageDigest origDigest = DigestUtils.getMd5Digest();
    byte[] payload = new byte[0];
    origDigest.update(payload);
    InputStream is = new ByteArrayInputStream(payload);
    PartHandle partHandle = mpu.putPart(file, is, 0, uploadHandle,
        payload.length);
      partHandles.add(Pair.of(0, partHandle));
    completeUpload(file, mpu, uploadHandle, partHandles, origDigest, 0);
  }

  /**
   * Assert that a multipart upload is successful even when the parts are
   * given in the reverse order.
   */
  @Test
  public void testMultipartUploadReverseOrder() throws Exception {
    FileSystem fs = getFileSystem();
    Path file = path("testMultipartUploadReverseOrder");
    MultipartUploader mpu = MultipartUploaderFactory.get(fs, null);
    UploadHandle uploadHandle = mpu.initialize(file);
    List<Pair<Integer, PartHandle>> partHandles = new ArrayList<>();
    MessageDigest origDigest = DigestUtils.getMd5Digest();
    final int payloadCount = getTestPayloadCount();
    for (int i = 1; i <= payloadCount; ++i) {
      byte[] payload = generatePayload(i);
      origDigest.update(payload);
    }
    for (int i = payloadCount; i > 0; --i) {
      byte[] payload = generatePayload(i);
      InputStream is = new ByteArrayInputStream(payload);
      PartHandle partHandle = mpu.putPart(file, is, i, uploadHandle,
          payload.length);
      partHandles.add(Pair.of(i, partHandle));
    }
    completeUpload(file, mpu, uploadHandle, partHandles, origDigest,
        payloadCount * partSizeInBytes());
  }

  /**
   * Assert that a multipart upload is successful even when the parts are
   * given in reverse order and the part numbers are not contiguous.
   */
  @Test
  public void testMultipartUploadReverseOrderNonContiguousPartNumbers()
      throws Exception {
    describe("Upload in reverse order and the part numbers are not contiguous");
    FileSystem fs = getFileSystem();
    Path file = path("testMultipartUploadReverseOrderNonContiguousPartNumbers");
    MultipartUploader mpu = MultipartUploaderFactory.get(fs, null);
    UploadHandle uploadHandle = mpu.initialize(file);
    List<Pair<Integer, PartHandle>> partHandles = new ArrayList<>();
    MessageDigest origDigest = DigestUtils.getMd5Digest();
    int payloadCount = 2 * getTestPayloadCount();
    for (int i = 2; i <= payloadCount; i += 2) {
      byte[] payload = generatePayload(i);
      origDigest.update(payload);
    }
    for (int i = payloadCount; i > 0; i -= 2) {
      byte[] payload = generatePayload(i);
      InputStream is = new ByteArrayInputStream(payload);
      PartHandle partHandle = mpu.putPart(file, is, i, uploadHandle,
          payload.length);
      partHandles.add(Pair.of(i, partHandle));
    }
    completeUpload(file, mpu, uploadHandle, partHandles, origDigest,
        getTestPayloadCount() * partSizeInBytes());
  }

  /**
   * Assert that when we abort a multipart upload, the resulting file does
   * not show up.
   */
  @Test
  public void testMultipartUploadAbort() throws Exception {
    describe("Upload and then abort it before completing");
    FileSystem fs = getFileSystem();
    Path file = path("testMultipartUploadAbort");
    MultipartUploader mpu = MultipartUploaderFactory.get(fs, null);
    UploadHandle uploadHandle = mpu.initialize(file);
    List<Pair<Integer, PartHandle>> partHandles = new ArrayList<>();
    for (int i = 20; i >= 10; --i) {
      byte[] payload = generatePayload(i);
      InputStream is = new ByteArrayInputStream(payload);
      PartHandle partHandle = mpu.putPart(file, is, i, uploadHandle,
          payload.length);
      partHandles.add(Pair.of(i, partHandle));
    }
    mpu.abort(file, uploadHandle);

    String contents = "ThisIsPart49\n";
    int len = contents.getBytes(Charsets.UTF_8).length;
    InputStream is = IOUtils.toInputStream(contents, "UTF-8");

    intercept(IOException.class,
        () -> mpu.putPart(file, is, 49, uploadHandle, len));
    intercept(IOException.class,
        () -> mpu.complete(file, partHandles, uploadHandle));

    assertPathDoesNotExist("Uploaded file should not exist", file);
  }

  /**
   * Trying to abort from an invalid handle must fail.
   */
  @Test
  public void testAbortUnknownUpload() throws Exception {
    FileSystem fs = getFileSystem();
    Path file = path("testAbortUnknownUpload");
    MultipartUploader mpu = MultipartUploaderFactory.get(fs, null);
    ByteBuffer byteBuffer = ByteBuffer.wrap(
        "invalid-handle".getBytes(Charsets.UTF_8));
    UploadHandle uploadHandle = BBUploadHandle.from(byteBuffer);
    intercept(FileNotFoundException.class, () -> mpu.abort(file, uploadHandle));
  }

  /**
   * Trying to abort with a handle of size 0 must fail.
   */
  @Test
  public void testAbortEmptyUploadHandle() throws Exception {
    FileSystem fs = getFileSystem();
    Path file = path("testAbortEmptyUpload");
    MultipartUploader mpu = MultipartUploaderFactory.get(fs, null);
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[0]);
    UploadHandle uploadHandle = BBUploadHandle.from(byteBuffer);
    intercept(IllegalArgumentException.class,
        () -> mpu.abort(file, uploadHandle));
  }

  /**
   * When we complete with no parts provided, it must fail.
   */
  @Test
  public void testCompleteEmptyUpload() throws Exception {
    describe("Expect an empty MPU to fail, but still be abortable");
    FileSystem fs = getFileSystem();
    Path dest = path("testCompleteEmptyUpload");
    MultipartUploader mpu = MultipartUploaderFactory.get(fs, null);
    UploadHandle handle = mpu.initialize(dest);
    intercept(IOException.class,
        () -> mpu.complete(dest, new ArrayList<>(), handle));
    mpu.abort(dest, handle);
  }

  /**
   * When we pass empty uploadID, putPart throws IllegalArgumentException.
   * @throws Exception
   */
  @Test
  public void testPutPartEmptyUploadID() throws Exception {
    describe("Expect IllegalArgumentException when putPart uploadID is empty");
    FileSystem fs = getFileSystem();
    Path dest = path("testCompleteEmptyUpload");
    MultipartUploader mpu = MultipartUploaderFactory.get(fs, null);
    mpu.initialize(dest);
    UploadHandle emptyHandle =
        BBUploadHandle.from(ByteBuffer.wrap(new byte[0]));
    byte[] payload = generatePayload(1);
    InputStream is = new ByteArrayInputStream(payload);
    intercept(IllegalArgumentException.class,
        () -> mpu.putPart(dest, is, 1, emptyHandle, payload.length));
  }

  /**
   * When we pass empty uploadID, complete throws IllegalArgumentException.
   * @throws Exception
   */
  @Test
  public void testCompleteEmptyUploadID() throws Exception {
    describe("Expect IllegalArgumentException when complete uploadID is empty");
    FileSystem fs = getFileSystem();
    Path dest = path("testCompleteEmptyUpload");
    MultipartUploader mpu = MultipartUploaderFactory.get(fs, null);
    UploadHandle realHandle = mpu.initialize(dest);
    UploadHandle emptyHandle =
        BBUploadHandle.from(ByteBuffer.wrap(new byte[0]));
    List<Pair<Integer, PartHandle>> partHandles = new ArrayList<>();
    byte[] payload = generatePayload(1);
    InputStream is = new ByteArrayInputStream(payload);
    PartHandle partHandle = mpu.putPart(dest, is, 1, realHandle,
        payload.length);
    partHandles.add(Pair.of(1, partHandle));

    intercept(IllegalArgumentException.class,
        () -> mpu.complete(dest, partHandles, emptyHandle));
  }
}
