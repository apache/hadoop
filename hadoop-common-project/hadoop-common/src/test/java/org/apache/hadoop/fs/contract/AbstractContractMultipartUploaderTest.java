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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.BBUploadHandle;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MultipartUploader;
import org.apache.hadoop.fs.PartHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.UploadHandle;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyPathExists;
import static org.apache.hadoop.fs.impl.FutureIOSupport.awaitFuture;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsSourceToString;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;
import static org.apache.hadoop.test.LambdaTestUtils.eventually;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Tests of multipart uploads.
 * <p></p>
 * <i>Note</i>: some of the tests get a random uploader between
 * the two which are available. If tests fail intermittently,
 * it may be because different uploaders are being selected.
 */
public abstract class AbstractContractMultipartUploaderTest extends
    AbstractFSContractTestBase {

  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractContractMultipartUploaderTest.class);

  /**
   * Size of very small uploads.
   * Enough to be non empty, not big enough to cause delays on uploads.
   */
  protected static final int SMALL_FILE = 100;

  protected static final int CONSISTENCY_INTERVAL = 1000;

  private MultipartUploader uploader0;
  private MultipartUploader uploader1;
  private final Random random = new Random();
  private UploadHandle activeUpload;
  private Path activeUploadPath;

  @Override
  public void setup() throws Exception {
    super.setup();

    final FileSystem fs = getFileSystem();
    Path testPath = getContract().getTestPath();
    uploader0 = fs.createMultipartUploader(testPath).build();
    uploader1 = fs.createMultipartUploader(testPath).build();
  }

  @Override
  public void teardown() throws Exception {
    MultipartUploader uploader = getUploader(1);
    if (uploader != null) {
      if (activeUpload != null) {
          abortUploadQuietly(activeUpload, activeUploadPath);
      }
      try {
        // round off with an abort of all uploads
        Path teardown = getContract().getTestPath();
        LOG.info("Teardown: aborting outstanding uploads under {}", teardown);
        CompletableFuture<Integer> f
            = uploader.abortUploadsUnderPath(teardown);
        f.get();
        LOG.info("Statistics {}",
            ioStatisticsSourceToString(uploader));
      } catch (Exception e) {
        LOG.warn("Exeception in teardown", e);
      }
    }

    cleanupWithLogger(LOG, uploader0, uploader1);
    super.teardown();
  }

  /**
   * Get a test path based on the method name.
   * @return a path to use in the test
   * @throws IOException failure to build the path name up.
   */
  protected Path methodPath() throws IOException {
    return path(getMethodName());
  }

  /**
   * The payload is the part number repeated for the length of the part.
   * This makes checking the correctness of the upload straightforward.
   * @param partNumber part number
   * @return the bytes to upload.
   */
  private byte[] generatePayload(int partNumber) {
    return generatePayload(partNumber, partSizeInBytes());
  }

  /**
   * Generate a payload of a given size; part number is used
   * for all the values.
   * @param partNumber part number
   * @param size size in bytes
   * @return the bytes to upload.
   */
  private byte[] generatePayload(final int partNumber, final int size) {
    ByteBuffer buffer = ByteBuffer.allocate(size);
    for (int i=0; i < size /(Integer.SIZE/Byte.SIZE); ++i) {
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
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    try (InputStream in = getFileSystem().open(path)) {
      byte[] fdData = IOUtils.toByteArray(in);
      MessageDigest newDigest = DigestUtils.getMd5Digest();
      byte[] digest = newDigest.digest(fdData);
      return digest;
    } finally {
      timer.end("Download and digest of path %s", path);
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
   * How long in milliseconds for propagation of
   * store changes, including update/delete/list
   * to be everywhere.
   * If 0: the FS is consistent.
   * @return a time in milliseconds.
   */
  protected int timeToBecomeConsistentMillis() {
    return 0;
  }

  /**
   * Does a call to finalize an upload (either complete or abort) consume the
   * uploadID immediately or is it reaped at a later point in time?
   * @return true if the uploadID will be consumed immediately (and no longer
   * resuable).
   */
  protected abstract boolean finalizeConsumesUploadIdImmediately();

  /**
   * Does the store support concurrent uploads to the same destination path?
   * @return true if concurrent uploads are supported.
   */
  protected abstract boolean supportsConcurrentUploadsToSamePath();

  /**
   * Pick a multipart uploader from the index value.
   * @param index index of upload
   * @return an uploader
   */
  protected MultipartUploader getUploader(int index) {
    return (index % 2 == 0) ? uploader0 : uploader1;
  }

  /**
   * Pick a multipart uploader at random.
   * @return an uploader
   */
  protected MultipartUploader getRandomUploader() {
    return getUploader(random.nextInt(10));
  }

  /**
   * Assert that a multipart upload is successful.
   * @throws Exception failure
   */
  @Test
  public void testSingleUpload() throws Exception {
    Path file = methodPath();
    UploadHandle uploadHandle = startUpload(file);
    Map<Integer, PartHandle> partHandles = new HashMap<>();
    MessageDigest origDigest = DigestUtils.getMd5Digest();
    int size = SMALL_FILE;
    byte[] payload = generatePayload(1, size);
    origDigest.update(payload);
    // use a single uploader
    // note: the same is used here as it found a bug in the S3Guard
    // DDB bulk operation state upload -the previous operation had
    // added an entry to the ongoing state; this second call
    // was interpreted as an inconsistent write.
    MultipartUploader completer = uploader0;
    // and upload with uploader 1 to validate cross-uploader uploads
    PartHandle partHandle = putPart(file, uploadHandle, 1, payload);
    partHandles.put(1, partHandle);
    PathHandle fd = complete(completer, uploadHandle, file,
        partHandles);

    validateUpload(file, origDigest, size);

    // verify that if the implementation processes data immediately
    // then a second attempt at the upload will fail.
    if (finalizeConsumesUploadIdImmediately()) {
      intercept(FileNotFoundException.class,
          () -> complete(completer, uploadHandle, file, partHandles));
    } else {
      // otherwise, the same or other uploader can try again.
      PathHandle fd2 = complete(completer, uploadHandle, file, partHandles);
      assertArrayEquals("Path handles differ", fd.toByteArray(),
          fd2.toByteArray());
    }
  }

  /**
   * Complete IO for a specific uploader; await the response.
   * @param uploader uploader
   * @param uploadHandle Identifier
   * @param file  Target path for upload
   * @param partHandles handles map of part number to part handle
   * @return unique PathHandle identifier for the uploaded file.
   */
  protected PathHandle complete(
      final MultipartUploader uploader,
      final UploadHandle uploadHandle,
      final Path file,
      final Map<Integer, PartHandle> partHandles)
      throws IOException {
    try (DurationInfo d =
             new DurationInfo(LOG, "Complete upload to %s", file)) {
      return awaitFuture(
          uploader.complete(uploadHandle, file, partHandles));
    }
  }

  /**
   * start an upload.
   * This saves the path and upload handle as the active
   * upload, for aborting in teardown
   * @param dest destination
   * @return the handle
   * @throws IOException failure to initialize
   */
  protected UploadHandle startUpload(final Path dest) throws IOException {
    activeUploadPath = dest;
    activeUpload = awaitFuture(getRandomUploader().startUpload(dest));
    return activeUpload;
  }

  /**
   * Generate then upload a part.
   * @param file destination
   * @param uploadHandle handle
   * @param index index of part
   * @param origDigest digest to build up. May be null
   * @return the part handle
   * @throws IOException IO failure.
   */
  protected PartHandle buildAndPutPart(
      final Path file,
      final UploadHandle uploadHandle,
      final int index,
      final MessageDigest origDigest) throws IOException {
    byte[] payload = generatePayload(index);
    if (origDigest != null) {
      origDigest.update(payload);
    }
    return putPart(file, uploadHandle, index, payload);
  }

  /**
   * Put a part.
   * The entire byte array is uploaded.
   * @param file destination
   * @param uploadHandle handle
   * @param index index of part
   * @param payload byte array of payload
   * @return the part handle
   * @throws IOException IO failure.
   */
  protected PartHandle putPart(final Path file,
      final UploadHandle uploadHandle,
      final int index,
      final byte[] payload) throws IOException {
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    PartHandle partHandle;
    try (DurationInfo d =
             new DurationInfo(LOG, "Put part %d (size %s) %s",
                 index,
                 payload.length,
                 file)) {
      partHandle = awaitFuture(getUploader(index)
          .putPart(uploadHandle, index, file,
              new ByteArrayInputStream(payload),
              payload.length));
    }
    timer.end("Uploaded part %s", index);
    LOG.info("Upload bandwidth {} MB/s",
        timer.bandwidthDescription(payload.length));
    return partHandle;
  }

  /**
   * Complete an upload with a random uploader.
   * @param file destination
   * @param uploadHandle handle
   * @param partHandles map of handles
   * @param origDigest digest of source data (may be null)
   * @param expectedLength expected length of result.
   * @return the path handle from the upload.
   * @throws IOException IO failure
   */
  private PathHandle completeUpload(final Path file,
      final UploadHandle uploadHandle,
      final Map<Integer, PartHandle> partHandles,
      final MessageDigest origDigest,
      final int expectedLength) throws IOException {
    PathHandle fd = complete(file, uploadHandle, partHandles);

    validateUpload(file, origDigest, expectedLength);
    return fd;
  }

  /**
   * Complete an upload with a random uploader.
   * @param file destination
   * @param origDigest digest of source data (may be null)
   * @param expectedLength expected length of result.
   * @throws IOException IO failure
   */
  private void validateUpload(final Path file,
      final MessageDigest origDigest,
      final int expectedLength) throws IOException {
    verifyPathExists(getFileSystem(),
         "Completed file", file);
    verifyFileLength(file, expectedLength);

    if (origDigest != null) {
      verifyContents(file, origDigest, expectedLength);
    }
  }

  /**
   * Verify the contents of a file.
   * @param file path
   * @param origDigest digest
   * @param expectedLength expected length (for logging download bandwidth)
   * @throws IOException IO failure
   */
  protected void verifyContents(final Path file,
                                final MessageDigest origDigest,
                                final int expectedLength) throws IOException {
    ContractTestUtils.NanoTimer timer2 = new ContractTestUtils.NanoTimer();
    Assertions.assertThat(digest(file))
        .describedAs("digest of uploaded file %s", file)
        .isEqualTo(origDigest.digest());
    timer2.end("Completed digest", file);
    LOG.info("Download bandwidth {} MB/s",
        timer2.bandwidthDescription(expectedLength));
  }

  /**
   * Verify the length of a file.
   * @param file path
   * @param expectedLength expected length
   * @throws IOException IO failure
   */
  private void verifyFileLength(final Path file, final long expectedLength)
      throws IOException {
    FileStatus st = getFileSystem().getFileStatus(file);
    Assertions.assertThat(st)
        .describedAs("Uploaded file %s", st)
        .matches(FileStatus::isFile)
        .extracting(FileStatus::getLen)
        .isEqualTo(expectedLength);
  }

  /**
   * Perform the inner complete without verification.
   * @param file destination path
   * @param uploadHandle upload handle
   * @param partHandles map of parts
   * @return the path handle from the upload.
   * @throws IOException IO failure
   */
  private PathHandle complete(final Path file,
      final UploadHandle uploadHandle,
      final Map<Integer, PartHandle> partHandles) throws IOException {
    return complete(getRandomUploader(), uploadHandle, file,
        partHandles);
  }

  /**
   * Abort an upload.
   * @param uploadHandle handle
   * @param file path
   * @throws IOException failure
   */
  private void abortUpload(UploadHandle uploadHandle,
      final Path file)
      throws IOException {
    try (DurationInfo d =
             new DurationInfo(LOG, "Abort upload to %s", file)) {
      awaitFuture(getRandomUploader().abort(uploadHandle, file));
    }
  }

  /**
   * Abort an upload; swallows exceptions.
   * @param uploadHandle handle
   * @param file path
   */
  private void abortUploadQuietly(UploadHandle uploadHandle, Path file) {
    try {
      abortUpload(uploadHandle, file);
    } catch (FileNotFoundException ignored) {
    } catch (Exception e) {
      LOG.info("aborting {}: {}", file, e.toString());
    }
  }

  /**
   * Assert that a multipart upload is successful.
   * @throws Exception failure
   */
  @Test
  public void testMultipartUpload() throws Exception {
    Path file = methodPath();
    UploadHandle uploadHandle = startUpload(file);
    Map<Integer, PartHandle> partHandles = new HashMap<>();
    MessageDigest origDigest = DigestUtils.getMd5Digest();
    int payloadCount = getTestPayloadCount();
    for (int i = 1; i <= payloadCount; ++i) {
      PartHandle partHandle = buildAndPutPart(file, uploadHandle, i,
          origDigest);
      partHandles.put(i, partHandle);
    }
    completeUpload(file, uploadHandle, partHandles, origDigest,
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
    try (MultipartUploader uploader =
        fs.createMultipartUploader(file).build()) {
      UploadHandle uploadHandle = uploader.startUpload(file).get();

      Map<Integer, PartHandle> partHandles = new HashMap<>();
      MessageDigest origDigest = DigestUtils.getMd5Digest();
      byte[] payload = new byte[0];
      origDigest.update(payload);
      InputStream is = new ByteArrayInputStream(payload);
      PartHandle partHandle = awaitFuture(
          uploader.putPart(uploadHandle, 1, file, is, payload.length));
      partHandles.put(1, partHandle);
      completeUpload(file, uploadHandle, partHandles, origDigest, 0);
    }
  }

  /**
   * Assert that a multipart upload is successful.
   * @throws Exception failure
   */
  @Test
  public void testUploadEmptyBlock() throws Exception {
    Path file = methodPath();
    UploadHandle uploadHandle = startUpload(file);
    Map<Integer, PartHandle> partHandles = new HashMap<>();
    partHandles.put(1, putPart(file, uploadHandle, 1, new byte[0]));
    completeUpload(file, uploadHandle, partHandles, null, 0);
  }

  /**
   * Assert that a multipart upload is successful even when the parts are
   * given in the reverse order.
   */
  @Test
  public void testMultipartUploadReverseOrder() throws Exception {
    Path file = methodPath();
    UploadHandle uploadHandle = startUpload(file);
    Map<Integer, PartHandle> partHandles = new HashMap<>();
    MessageDigest origDigest = DigestUtils.getMd5Digest();
    int payloadCount = getTestPayloadCount();
    for (int i = 1; i <= payloadCount; ++i) {
      byte[] payload = generatePayload(i);
      origDigest.update(payload);
    }
    for (int i = payloadCount; i > 0; --i) {
      partHandles.put(i, buildAndPutPart(file, uploadHandle, i, null));
    }
    completeUpload(file, uploadHandle, partHandles, origDigest,
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
    Path file = methodPath();
    UploadHandle uploadHandle = startUpload(file);
    MessageDigest origDigest = DigestUtils.getMd5Digest();
    int payloadCount = 2 * getTestPayloadCount();
    for (int i = 2; i <= payloadCount; i += 2) {
      byte[] payload = generatePayload(i);
      origDigest.update(payload);
    }
    Map<Integer, PartHandle> partHandles = new HashMap<>();
    for (int i = payloadCount; i > 0; i -= 2) {
      partHandles.put(i, buildAndPutPart(file, uploadHandle, i, null));
    }
    completeUpload(file, uploadHandle, partHandles, origDigest,
        getTestPayloadCount() * partSizeInBytes());
  }

  /**
   * Assert that when we abort a multipart upload, the resulting file does
   * not show up.
   */
  @Test
  public void testMultipartUploadAbort() throws Exception {
    describe("Upload and then abort it before completing");
    Path file = methodPath();
    UploadHandle uploadHandle = startUpload(file);
    Map<Integer, PartHandle> partHandles = new HashMap<>();
    for (int i = 12; i > 10; i--) {
      partHandles.put(i, buildAndPutPart(file, uploadHandle, i, null));
    }
    abortUpload(uploadHandle, file);

    String contents = "ThisIsPart49\n";
    int len = contents.getBytes(Charsets.UTF_8).length;
    InputStream is = IOUtils.toInputStream(contents, "UTF-8");

    intercept(IOException.class,
        () -> awaitFuture(
            uploader0.putPart(uploadHandle, 49, file, is, len)));
    intercept(IOException.class,
        () -> complete(uploader0, uploadHandle, file, partHandles));

    assertPathDoesNotExist("Uploaded file should not exist", file);

    // A second abort should be an FileNotFoundException if the UploadHandle is
    // consumed by finalization operations (complete, abort).
    if (finalizeConsumesUploadIdImmediately()) {
      intercept(FileNotFoundException.class,
          () -> abortUpload(uploadHandle, file));
    } else {
      abortUpload(uploadHandle, file);
    }
  }

  /**
   * Trying to abort from an invalid handle must fail.
   */
  @Test
  public void testAbortUnknownUpload() throws Exception {
    Path file = methodPath();
    ByteBuffer byteBuffer = ByteBuffer.wrap(
        "invalid-handle".getBytes(Charsets.UTF_8));
    intercept(FileNotFoundException.class,
        () -> abortUpload(BBUploadHandle.from(byteBuffer), file));
  }

  /**
   * Trying to abort an upload with no data does not create a file.
   */
  @Test
  public void testAbortEmptyUpload() throws Exception {
    describe("initialize upload and abort before uploading data");
    Path file = methodPath();
    abortUpload(startUpload(file), file);
    assertPathDoesNotExist("Uploaded file should not exist", file);
  }


  /**
   * Trying to abort an upload with no data does not create a file.
   */
  @Test
  public void testAbortAllPendingUploads() throws Exception {
    describe("initialize upload and abort the pending upload");
    Path path = methodPath();
    Path file = new Path(path, "child");
    UploadHandle upload = startUpload(file);
    try {
      CompletableFuture<Integer> oF
          = getRandomUploader().abortUploadsUnderPath(path.getParent());
      int abortedUploads = awaitFuture(oF);
      if (abortedUploads >= 0) {
        // uploads can be aborted
        Assertions.assertThat(abortedUploads)
            .describedAs("Number of uploads aborted")
            .isGreaterThanOrEqualTo(1);
        assertPathDoesNotExist("Uploaded file should not exist", file);
      }
    } finally {
      abortUploadQuietly(upload, file);
    }
  }

  /**
   * Trying to abort with a handle of size 0 must fail.
   */
  @Test
  public void testAbortEmptyUploadHandle() throws Exception {
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[0]);
    intercept(IllegalArgumentException.class,
        () -> abortUpload(BBUploadHandle.from(byteBuffer), methodPath()));
  }

  /**
   * When we complete with no parts provided, it must fail.
   */
  @Test
  public void testCompleteEmptyUpload() throws Exception {
    describe("Expect an empty MPU to fail, but still be abortable");
    Path dest = methodPath();
    UploadHandle handle = startUpload(dest);
    intercept(IllegalArgumentException.class,
        () -> complete(uploader0, handle, dest, new HashMap<>()));
    abortUpload(handle, dest);
  }

  /**
   * When we pass empty uploadID, putPart throws IllegalArgumentException.
   */
  @Test
  public void testPutPartEmptyUploadID() throws Exception {
    describe("Expect IllegalArgumentException when putPart uploadID is empty");
    Path dest = methodPath();
    UploadHandle emptyHandle =
        BBUploadHandle.from(ByteBuffer.wrap(new byte[0]));
    byte[] payload = generatePayload(1);
    InputStream is = new ByteArrayInputStream(payload);
    intercept(IllegalArgumentException.class,
        () -> uploader0.putPart(emptyHandle, 1, dest, is, payload.length));
  }

  /**
   * When we pass empty uploadID, complete throws IllegalArgumentException.
   */
  @Test
  public void testCompleteEmptyUploadID() throws Exception {
    describe("Expect IllegalArgumentException when complete uploadID is empty");
    Path dest = methodPath();
    UploadHandle realHandle = startUpload(dest);
    UploadHandle emptyHandle =
        BBUploadHandle.from(ByteBuffer.wrap(new byte[0]));
    Map<Integer, PartHandle> partHandles = new HashMap<>();
    PartHandle partHandle = putPart(dest, realHandle, 1,
        generatePayload(1, SMALL_FILE));
    partHandles.put(1, partHandle);

    intercept(IllegalArgumentException.class,
        () -> complete(uploader0, emptyHandle, dest, partHandles));

    // and, while things are setup, attempt to complete with
    // a part index of 0
    partHandles.clear();
    partHandles.put(0, partHandle);
    intercept(IllegalArgumentException.class,
        () -> complete(uploader0, realHandle, dest, partHandles));
  }

  /**
   * Assert that upon completion, a directory in the way of the file will
   * result in a failure. This test only applies to backing stores with a
   * concept of directories.
   * @throws Exception failure
   */
  @Test
  public void testDirectoryInTheWay() throws Exception {
    FileSystem fs = getFileSystem();
    Path file = methodPath();
    UploadHandle uploadHandle = startUpload(file);
    Map<Integer, PartHandle> partHandles = new HashMap<>();
    int size = SMALL_FILE;
    PartHandle partHandle = putPart(file, uploadHandle, 1,
        generatePayload(1, size));
    partHandles.put(1, partHandle);

    fs.mkdirs(file);
    intercept(IOException.class,
        () -> completeUpload(file, uploadHandle, partHandles, null,
            size));
    // abort should still work
    abortUpload(uploadHandle, file);
  }

  @Test
  public void testConcurrentUploads() throws Throwable {

    // if the FS doesn't support concurrent uploads, this test is
    // required to fail during the second initialization.
    boolean concurrent = supportsConcurrentUploadsToSamePath();

    describe("testing concurrent uploads, MPU support for this is "
        + concurrent);
    Path file = methodPath();
    int size1 = SMALL_FILE;
    int partId1 = 1;
    byte[] payload1 = generatePayload(partId1, size1);
    MessageDigest digest1 = DigestUtils.getMd5Digest();
    digest1.update(payload1);
    UploadHandle upload1 = startUpload(file);
    Map<Integer, PartHandle> partHandles1 = new HashMap<>();

    // initiate part 2
    // by using a different size, it's straightforward to see which
    // version is visible, before reading/digesting the contents
    int size2 = size1 * 2;
    int partId2 = 2;
    byte[] payload2 = generatePayload(partId1, size2);
    MessageDigest digest2 = DigestUtils.getMd5Digest();
    digest2.update(payload2);

    UploadHandle upload2;
    try {
      upload2 = startUpload(file);
      Assume.assumeTrue(
          "The Filesystem is unexpectedly supporting concurrent uploads",
          concurrent);
    } catch (IOException e) {
      if (!concurrent) {
        // this is expected, so end the test
        LOG.debug("Expected exception raised on concurrent uploads", e);
        return;
      } else {
        throw e;
      }
    }
    Map<Integer, PartHandle> partHandles2 = new HashMap<>();

    assertNotEquals("Upload handles match", upload1, upload2);

    // put part 1
    partHandles1.put(partId1, putPart(file, upload1, partId1, payload1));

    // put part2
    partHandles2.put(partId2, putPart(file, upload2, partId2, payload2));

    // complete part u1. expect its size and digest to
    // be as expected.
    completeUpload(file, upload1, partHandles1, digest1, size1);

    // now upload part 2.
    complete(file, upload2, partHandles2);

    // and await the visible length to match, if this FS is not
    // consistent.
    final int consistencyDelay = timeToBecomeConsistentMillis();
    if (consistencyDelay > 0) {
      eventually(consistencyDelay,
          () -> verifyFileLength(file, size2),
          new LambdaTestUtils.ProportionalRetryInterval(
              CONSISTENCY_INTERVAL,
              consistencyDelay));
    }

    verifyContents(file, digest2, size2);
  }

  @Test
  public void testPathCapabilities() throws Throwable {
    FileSystem fs = getFileSystem();
    Assertions.assertThat(fs.hasPathCapability(getContract().getTestPath(),
        CommonPathCapabilities.FS_MULTIPART_UPLOADER))
        .describedAs("fs %s, lacks multipart upload capability", fs)
        .isTrue();
  }
}
