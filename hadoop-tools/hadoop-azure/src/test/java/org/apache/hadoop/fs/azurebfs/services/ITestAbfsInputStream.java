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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_FS_AZURE_BLOB_LAYOUT_CACHE_MAX_COUNT;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamTestUtils.HUNDRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class ITestAbfsInputStream extends AbstractAbfsIntegrationTest {

  private final AbfsInputStreamTestUtils abfsInputStreamTestUtils;
  public ITestAbfsInputStream() throws Exception {
    this.abfsInputStreamTestUtils = new AbfsInputStreamTestUtils(this);
  }

  @Test
  public void testWithNoOptimization() throws Exception {
    for (int i = 2; i <= 7; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(false, false, fileSize);
      String fileName = methodName.getMethodName() + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      testWithNoOptimization(fs, testFilePath, HUNDRED, fileContent);
    }
  }

  protected void testWithNoOptimization(final FileSystem fs,
      final Path testFilePath, final int seekPos, final byte[] fileContent)
      throws IOException {
    FSDataInputStream iStream = fs.open(testFilePath);
    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();

      iStream = new FSDataInputStream(abfsInputStream);
      abfsInputStreamTestUtils.seek(iStream, seekPos);
      long totalBytesRead = 0;
      int length = HUNDRED * HUNDRED;
      do {
        byte[] buffer = new byte[length];
        int bytesRead = iStream.read(buffer, 0, length);
        totalBytesRead += bytesRead;
        if ((totalBytesRead + seekPos) >= fileContent.length) {
          length = (fileContent.length - seekPos) % length;
        }
        assertEquals(length, bytesRead);
        abfsInputStreamTestUtils.assertContentReadCorrectly(fileContent,
            (int) (seekPos + totalBytesRead - length), length, buffer, testFilePath);

        assertTrue(abfsInputStream.getFCursor() >= seekPos + totalBytesRead);
        assertTrue(abfsInputStream.getFCursorAfterLastRead() >= seekPos + totalBytesRead);
        assertTrue(abfsInputStream.getBCursor() >= totalBytesRead % abfsInputStream.getBufferSize());
        assertTrue(abfsInputStream.getLimit() >= totalBytesRead % abfsInputStream.getBufferSize());
      } while (totalBytesRead + seekPos < fileContent.length);
    } finally {
      iStream.close();
    }
  }

  @Test
  public void testExceptionInOptimization() throws Exception {
    for (int i = 2; i <= 7; i++) {
      int fileSize = i * ONE_MB;
      final AzureBlobFileSystem fs = getFileSystem(true, true, fileSize);
      String fileName = methodName.getMethodName() + i;
      byte[] fileContent = getRandomBytesArray(fileSize);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);
      testExceptionInOptimization(fs, testFilePath, fileSize - HUNDRED,
          fileSize / 4, fileContent);
    }
  }

  /**
   * Testing the back reference being passed down to AbfsInputStream.
   */
  @Test
  public void testAzureBlobFileSystemBackReferenceInInputStream()
      throws IOException {
    Path path = path(getMethodName());
    // Create a file then open it to verify if this input stream contains any
    // back reference.
    try (FSDataOutputStream out = getFileSystem().create(path);
        FSDataInputStream in = getFileSystem().open(path)) {
      AbfsInputStream abfsInputStream = (AbfsInputStream) in.getWrappedStream();

      assertThat(abfsInputStream.getFsBackRef().isNull())
          .describedAs("BackReference in input stream should not be null")
          .isFalse();
    }
  }

  /**
   * Testing Get Blob API
   * @throws Exception if any exception occurs
   */
  @Test
  public void testGetBlobLayoutAPI() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    int fileSize = 100 * ONE_MB;
    Path testPath = createFileWithContent(fs, "/testFile", getRandomBytesArray(fileSize));
    try (AbfsInputStream stream = (AbfsInputStream) fs.open(testPath).getWrappedStream()) {
      int bytesRead = stream.read(new byte[fileSize], 0 , fileSize);
      assertEquals(fileSize, bytesRead);
    }
  }

  /**
   * Verifies that a request for a sub-interval (e.g., 20-50) is correctly coalesced
   * into an existing, larger in-flight request (e.g., 0-100).
   * <p>
   * The test ensures that the sub-range future does not trigger new IO and completes
   * automatically when the parent future completes.
   */
  @Test
  public void testSubIntervalCoalescing() throws IOException {
    assumeThat(getFileSystem().getAbfsStore()
        .getAbfsConfiguration()
        .isDataLocalityEnabled()).isTrue();
    AzureBlobFileSystem fs = getFileSystem();
    FileInfo fileInfo = setupTestFile(fs, "/testCoalesce", 100);
    CompletableFuture<Void> bigFuture = new CompletableFuture<>();

    getCache().processInFlightPromises(fileInfo.eTag, (list) -> {
      list.add(new BlobLayoutCache.InFlightPromise(0, 100, bigFuture));
      return CompletableFuture.completedFuture(null);
    });

    try (AbfsInputStream stream = getStream(fs, fileInfo.path)) {
      CompletableFuture<Void> subRangeFuture = stream.registerAndFetch(20, 50,
          20, 50, getTestTracingContext(fs, false));

      Assertions.assertThat(subRangeFuture)
          .describedAs(
              "Sub-range (20-50) should be covered by existing (0-100) and stay pending.")
          .isNotCompleted();

      bigFuture.complete(null);

      Assertions.assertThat(subRangeFuture)
          .describedAs(
              "Sub-range future should complete immediately once the parent range (0-100) completes.")
          .isCompleted();
    }
  }

  /**
   * Tests the interval subtraction logic when a new request spans across multiple
   * existing in-flight promises.
   * <p>
   * If 0-10 and 20-30 are in-flight, a request for 5-25 should identify that 11-19
   * is a "bridge gap" that needs a new asynchronous fetch.
   */
  @Test
  public void testBridgeGapSplitting() throws IOException {
    assumeThat(getFileSystem().getAbfsStore()
        .getAbfsConfiguration()
        .isDataLocalityEnabled()).isTrue();
    AzureBlobFileSystem fs = getFileSystem();
    FileInfo fileInfo = setupTestFile(fs, "/testBridge", 100);
    CompletableFuture<Void> f1 = new CompletableFuture<>();
    CompletableFuture<Void> f2 = new CompletableFuture<>();

    getCache().processInFlightPromises(fileInfo.eTag, (list) -> {
      list.add(new BlobLayoutCache.InFlightPromise(0, 10, f1));
      list.add(new BlobLayoutCache.InFlightPromise(20, 30, f2));
      return CompletableFuture.completedFuture(null);
    });

    try (AbfsInputStream stream = getStream(fs, fileInfo.path)) {
      // Requesting 5-25: 5-10 is in f1, 20-25 is in f2. Only 11-19 is a new gap.
      CompletableFuture<Void> bridgeFuture = stream.registerAndFetch(5, 25, 5,
          25, getTestTracingContext(fs, false));

      Assertions.assertThat(bridgeFuture)
          .describedAs(
              "Composite future must wait for the internal 'bridge' fetch (11-19) to trigger and finish.")
          .isNotCompleted();

      f1.complete(null);
      f2.complete(null);

      Assertions.assertThat(bridgeFuture)
          .describedAs(
              "Even if flanking ranges complete, the bridge future must wait for the middle gap fetch.")
          .isNotCompleted();
    }
  }

  /**
   * Validates the "fail-fast" behavior of the composite future.
   * <p>
   * If a large range is split into multiple dependencies and any single dependency
   * fails exceptionally, the aggregate future returned to the caller should fail
   * immediately without waiting for other dependencies to resolve.
   */
  @Test
  public void testPartialFailureShortCircuit() throws IOException {
    assumeThat(getFileSystem().getAbfsStore()
        .getAbfsConfiguration()
        .isDataLocalityEnabled()).isTrue();
    AzureBlobFileSystem fs = getFileSystem();
    FileInfo fileInfo = setupTestFile(fs, "/testFail", 100);
    CompletableFuture<Void> failingFuture = new CompletableFuture<>();

    getCache().processInFlightPromises(fileInfo.eTag, (list) -> {
      list.add(new BlobLayoutCache.InFlightPromise(11, 20, failingFuture));
      return CompletableFuture.completedFuture(null);
    });

    try (AbfsInputStream stream = getStream(fs, fileInfo.path)) {
      CompletableFuture<Void> resultFuture = stream.registerAndFetch(0, 20, 0,
          20, getTestTracingContext(fs, false));

      failingFuture.completeExceptionally(
          new IOException("Simulated Network Error"));

      Assertions.assertThat(resultFuture)
          .describedAs(
              "The aggregate future must fail immediately when any dependency fails, regardless of other pending ranges.")
          .isCompletedExceptionally();
    }
  }

  /**
   * Ensures that the interval arithmetic correctly handles boundary conditions,
   * specifically single-byte requests (where start == end).
   * <p>
   * This prevents off-by-one errors in logic like {@code p.start() - 1} from
   * causing underflows or invalid ranges.
   */
  @Test
  public void testSingleByteRangeBoundary() throws Exception {
    assumeThat(getFileSystem().getAbfsStore()
        .getAbfsConfiguration()
        .isDataLocalityEnabled()).isTrue();
    AzureBlobFileSystem fs = getFileSystem();
    FileInfo fileInfo = setupTestFile(fs, "/testBoundary", 100);

    try (AbfsInputStream stream = getStream(fs, fileInfo.path)) {
      CompletableFuture<Void> resultFuture = stream.registerAndFetch(10, 10, 10,
          10, getTestTracingContext(fs, false));

      Assertions.assertThat(resultFuture)
          .describedAs(
              "The registerAndFetch logic must support single-byte requests (start == end).")
          .isNotNull();

      resultFuture.get(5, TimeUnit.SECONDS);

      Assertions.assertThat(resultFuture)
          .describedAs(
              "The single-byte future should complete successfully once the underlying IO is finished.")
          .isCompleted();
    }
  }

  /**
   * Tests a partial overlap scenario where part of the requested range is in-flight
   * and part is a gap.
   * <p>
   * If 10-20 is already in-flight and a new request for 0-20 arrives:
   * 1. A new fetch should be triggered for the gap (0-9).
   * 2. The resulting future should only complete when BOTH the new fetch (0-9)
   * and the existing promise (10-20) are finished.
   */
  @Test
  public void testPartialOverlapWithInFlightRange() throws IOException {
    assumeThat(getFileSystem().getAbfsStore()
        .getAbfsConfiguration()
        .isDataLocalityEnabled()).isTrue();
    AzureBlobFileSystem fs = getFileSystem();
    FileInfo fileInfo = setupTestFile(fs, "/testPartialOverlap", 100);
    String eTag = fileInfo.eTag;
    Set<CompletableFuture<Void>> dependencies = new HashSet<>();
    CompletableFuture<Void> existingFuture = new CompletableFuture<>();

    // 1. Pre-register 10-20 as an in-flight promise
    getCache().processInFlightPromises(eTag, (list) -> {
      list.add(new BlobLayoutCache.InFlightPromise(10, 20, existingFuture));
      return CompletableFuture.completedFuture(null);
    });
    dependencies.add(existingFuture);
    try (AbfsInputStream stream = getStream(fs, fileInfo.path)) {
      // 2. Request 0-20.
      // Logic should:
      // - Subtract 10-20 from 0-20 -> Gap remains: 0-9.
      // - Create a new internal future for 0-9.
      // - Return a composite future (0-9 fetch AND existingFuture).
      CompletableFuture<Void> aggregateFuture = stream.registerAndFetch(0, 20,
          0, 20, getTestTracingContext(fs, false));
      dependencies.add(aggregateFuture);

      Assertions.assertThat(aggregateFuture)
          .describedAs(
              "Aggregate future should be pending because it's waiting on both the new 0-9 gap and the existing 10-20 promise.")
          .isNotCompleted();

      // 3. Complete the pre-existing 10-20 promise
      existingFuture.complete(null);

      Assertions.assertThat(aggregateFuture)
          .describedAs(
              "Aggregate future should still be pending because the 0-9 gap fetch is still in progress.")
          .isNotCompleted();

      // 4. Once the internal executeFetch for 0-9 completes (handled by the stream),
      // the aggregateFuture will complete.
      CompletableFuture.allOf(dependencies.toArray(new CompletableFuture[0]))
          .get(10_000, TimeUnit.MILLISECONDS);

      // check both aggregateFuture and existingFuture are completed
      Assertions.assertThat(existingFuture)
          .describedAs(
              "The existing promise (10-20) should be completed.")
          .isCompleted();
      Assertions.assertThat(aggregateFuture)
          .describedAs(
              "The existing promise (10-20) should be completed.")
          .isCompleted();
      List<BlobLayout.BlobRange> gaps = getCache().getGaps(eTag, 0, 20);
      Assertions.assertThat(gaps.size())
          .describedAs(
              "After completion, there should be gaps from 10-20 range.")
          .isEqualTo(1);
      Assertions.assertThat(gaps.get(0))
          .describedAs(
              "After completion, there should be gaps from 10-20 range.")
          .isEqualTo(new BlobLayout.BlobRange(10, 20, null));

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Helper class to hold metadata for a created test file.
   */
  private record FileInfo(Path path, String eTag) {}

  /**
   * Sets up a test file with random content and returns both its Path and ETag.
   *
   * @param fs   The {@link AzureBlobFileSystem} instance.
   * @param path The string representation of the path.
   * @param size The size of the file in bytes.
   * @return A {@link FileInfo} object containing the created Path and ETag.
   * @throws IOException If file creation or status retrieval fails.
   */
  private FileInfo setupTestFile(AzureBlobFileSystem fs, String path, int size)
      throws IOException {
    Path testPath = createFileWithContent(fs, path, getRandomBytesArray(size));
    FileStatus fileStatus = fs.getFileStatus(testPath);
    return new FileInfo(testPath, ((VersionedFileStatus) fileStatus).getEtag());
  }

  /**
   * Helper method to get blob layout cache instance
   * @return BlobLayoutCache instance
   */
  private BlobLayoutCache getCache() {
    return BlobLayoutCache.getInstance(1,
        DEFAULT_FS_AZURE_BLOB_LAYOUT_CACHE_MAX_COUNT);
  }

  /**
   * Input stream helper to access the protected registerAndFetch method for testing.
   *
   * @param fs File system instance
   * @param path path to the file
   * @return AbfsInputStream instance
   * @throws IOException in case of IO errors
   */
  private AbfsInputStream getStream(AzureBlobFileSystem fs, Path path)
      throws IOException {
    return (AbfsInputStream) fs.open(path).getWrappedStream();
  }

  private void testExceptionInOptimization(final FileSystem fs,
      final Path testFilePath,
      final int seekPos, final int length, final byte[] fileContent)
      throws IOException {

    FSDataInputStream iStream = fs.open(testFilePath);
    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      abfsInputStream = spy(abfsInputStream);
      doThrow(new IOException())
          .doCallRealMethod()
          .when(abfsInputStream)
          .readRemote(anyLong(), any(), anyInt(), anyInt(),
              any(TracingContext.class), any());

      iStream = new FSDataInputStream(abfsInputStream);
      abfsInputStreamTestUtils.verifyAbfsInputStreamBaseStateBeforeSeek(abfsInputStream);
      abfsInputStreamTestUtils.seek(iStream, seekPos);
      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      long actualLength = length;
      if (seekPos + length > fileContent.length) {
        long delta = seekPos + length - fileContent.length;
        actualLength = length - delta;
      }
      assertEquals(bytesRead, actualLength);
      abfsInputStreamTestUtils.assertContentReadCorrectly(fileContent, seekPos,
          (int) actualLength, buffer, testFilePath);
      assertEquals(fileContent.length, abfsInputStream.getFCursor());
      assertEquals(fileContent.length, abfsInputStream.getFCursorAfterLastRead());
      assertEquals(actualLength, abfsInputStream.getBCursor());
      assertTrue(abfsInputStream.getLimit() >= actualLength);
    } finally {
      iStream.close();
    }
  }

  private AzureBlobFileSystem getFileSystem(boolean optimizeFooterRead,
      boolean readSmallFileCompletely, int fileSize) throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    getAbfsStore(fs).getAbfsConfiguration()
        .setOptimizeFooterRead(optimizeFooterRead);
    getAbfsStore(fs).getAbfsConfiguration()
        .setIsChecksumValidationEnabled(true);
    if (fileSize <= getAbfsStore(fs).getAbfsConfiguration()
        .getReadBufferSize()) {
      getAbfsStore(fs).getAbfsConfiguration()
          .setReadSmallFilesCompletely(readSmallFileCompletely);
    }
    return fs;
  }
}
