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

package org.apache.hadoop.fs.azurebfs.services;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_FS_AZURE_BLOB_LAYOUT_CACHE_MAX_COUNT;

public class BlobLayoutCacheTest {

  private BlobLayoutCache cache;

  private final long contentLength = 100;

  private BlobLayoutResponse layoutResponse;

  @BeforeEach
  public void setUp() {
    cache = BlobLayoutCache.getInstance(1L,
        DEFAULT_FS_AZURE_BLOB_LAYOUT_CACHE_MAX_COUNT);

    this.layoutResponse = new BlobLayoutResponse();
    layoutResponse.setRanges(List.of(new BlobLayoutResponse.Range(0, 9, 0),
        new BlobLayoutResponse.Range(10, 19, 1),
        new BlobLayoutResponse.Range(20, 29, 2),
        new BlobLayoutResponse.Range(30, 39, 3)));
    layoutResponse.setEndpoints(
        Set.of(new BlobLayoutResponse.Endpoint(0, "host-a"),
            new BlobLayoutResponse.Endpoint(1, "host-b"),
            new BlobLayoutResponse.Endpoint(2, "host-c"),
            new BlobLayoutResponse.Endpoint(3, "host-d")));
  }

  /**
   * Tests registering and deregistering a stream in the BlobLayoutCache.
   * <p>
   * Verifies that after registering a stream, the blob layout is empty if no elements are added.
   * After deregistering, the blob layout remains empty, and no exceptions are thrown.
   */
  @Test
  public void testRegisterAndDeregisterStream() {
    String eTag = new Throwable().getStackTrace()[0].getMethodName();
    cache.registerStream(eTag, contentLength);
    List<BlobLayout.BlobRange> blobRangeList = cache.getBlobLayout(eTag, 0,
        4);
    Assertions.assertThat(blobRangeList)
        .describedAs("List should be empty in case no element is added.")
        .hasSize(0);
    cache.deregisterStream(eTag);
    blobRangeList = cache.getBlobLayout(eTag, 0, 4);
    Assertions.assertThat(blobRangeList)
        .describedAs(
            "In case stream is deregistered, doesn't mean data is removed immediately from the cache.")
        .hasSize(0);
    // No direct assertion, but should not throw
  }

  /**
   * Tests putting and retrieving a blob layout in the BlobLayoutCache.
   * <p>
   * Verifies that after putting a blob layout, the correct ranges and hosts are returned
   * for a specified range.
   */
  @Test
  public void testPutBlobLayoutAndGetBlobLayout() {
    String eTag = new Throwable().getStackTrace()[0].getMethodName();
    cache.registerStream(eTag, contentLength);
    cache.putBlobLayout(eTag, layoutResponse, contentLength);

    // If start = 0, and end = 10 (both inclusive), get blob layout will return two entries
    // 0-9 -> host-a
    // 10-10 -> host-b
    List<BlobLayout.BlobRange> blobRangeList = cache.getBlobLayout(eTag, 0, 10);
    Assertions.assertThat(blobRangeList)
        .describedAs("List should contains 2 elements.")
        .hasSize(2);
    Assertions.assertThat(blobRangeList.get(0).host())
        .describedAs("First range should be host-a.")
        .isEqualTo("host-a");
    Assertions.assertThat(blobRangeList.get(1).host())
        .describedAs("Second range should be host-b.")
        .isEqualTo("host-b");
    cache.deregisterStream(eTag);
  }

  /**
   * Tests gap detection when content length is greater than the last byte present in the layout.
   * <p>
   * Verifies that gaps are correctly identified and returned for the specified range.
   */
  @Test
  public void testGapWithContentLengthGreaterThanLastBytePresent() {
    String eTag = new Throwable().getStackTrace()[0].getMethodName();
    cache.registerStream(eTag, contentLength);
    cache.putBlobLayout(eTag, layoutResponse, contentLength);

    // If start = 35, and end = 45 (both inclusive), get Gaps will return one entry
    // 40-45 -> null
    List<BlobLayout.BlobRange> gaps = cache.getGaps(eTag, 35, 45);
    Assertions.assertThat(gaps)
        .describedAs("List should contains 1 elements.")
        .hasSize(1);
    Assertions.assertThat(gaps.get(0).start())
        .describedAs("Gap should start from 40.")
        .isEqualTo(40);
    Assertions.assertThat(gaps.get(0).end())
        .describedAs("Gap should end at 45.")
        .isEqualTo(45);
    cache.deregisterStream(eTag);
  }

  /**
   * Tests gap detection when content length is equal to the last byte present in the layout.
   * <p>
   * Verifies that no gaps are returned, and the correct blob range and host are returned for the specified range.
   */
  @Test
  public void testGapWithContentLengthEqualToLastBytePresent() {
    String eTag = new Throwable().getStackTrace()[0].getMethodName();
    cache.registerStream(eTag, 40);
    cache.putBlobLayout(eTag, layoutResponse, 40);

    // If start = 35, and end = 45 (both inclusive), get Gaps will return zero entry
    // as file size is not more than 39, so there is no gap.
    List<BlobLayout.BlobRange> gaps = cache.getGaps(eTag, 35, 45);
    Assertions.assertThat(gaps)
        .describedAs("List should contains 0 elements.")
        .hasSize(0);
    // This will return 1 entry for 35-39 -> host-d
    List<BlobLayout.BlobRange> blobRangeList = cache.getBlobLayout(eTag, 35,
        45);
    Assertions.assertThat(blobRangeList)
        .describedAs("List should contains 1 elements.")
        .hasSize(1);
    Assertions.assertThat(blobRangeList.get(0).host())
        .describedAs("Host should be host-d.")
        .isEqualTo("host-d");
    cache.deregisterStream(eTag);
  }

  /**
   * Tests putting blob layouts with gaps and filling those gaps.
   * <p>
   * Verifies that gaps are correctly identified, filled, and the correct blob ranges and hosts are returned.
   */
  @Test
  public void testPutBlobLayoutWithGap() {
    String eTag = new Throwable().getStackTrace()[0].getMethodName();
    cache.registerStream(eTag, contentLength);
    cache.putBlobLayout(eTag, layoutResponse, contentLength);
    BlobLayoutResponse blobLayoutResponse = new BlobLayoutResponse();
    blobLayoutResponse.setRanges(
        List.of(new BlobLayoutResponse.Range(45, 54, 0)));
    blobLayoutResponse.setEndpoints(
        Set.of(new BlobLayoutResponse.Endpoint(0, "host-a")));
    cache.putBlobLayout(eTag, blobLayoutResponse, contentLength);

    // Layout present for 35-39 & 45-54.
    // Gap: 40-44 & 55-65
    List<BlobLayout.BlobRange> gaps = cache.getGaps(eTag, 35, 65);
    Assertions.assertThat(gaps)
        .describedAs("List should contains 2 elements.")
        .hasSize(2);
    Assertions.assertThat(gaps.get(0).start())
        .describedAs("First gap should start from 40.")
        .isEqualTo(40);
    Assertions.assertThat(gaps.get(0).end())
        .describedAs("First gap should end to 44.")
        .isEqualTo(44);
    Assertions.assertThat(gaps.get(1).start())
        .describedAs("Second gap should start from 55.")
        .isEqualTo(55);
    Assertions.assertThat(gaps.get(1).end())
        .describedAs("Second gap should end to 65.")
        .isEqualTo(65);

    // Gap fill
    blobLayoutResponse = new BlobLayoutResponse();
    blobLayoutResponse.setRanges(
        List.of(new BlobLayoutResponse.Range(35, 44, 0)));
    blobLayoutResponse.setEndpoints(
        Set.of(new BlobLayoutResponse.Endpoint(0, "host-d")));
    cache.putBlobLayout(eTag, blobLayoutResponse, contentLength);
    gaps = cache.getGaps(eTag, 35, 65);
    // Only one gap remaining - 55-65
    Assertions.assertThat(gaps)
        .describedAs("List should contains 0 elements.")
        .hasSize(1);
    Assertions.assertThat(gaps.get(0).start())
        .describedAs("Gap should start from 55.")
        .isEqualTo(55);
    Assertions.assertThat(gaps.get(0).end())
        .describedAs("Gap should end to 65.")
        .isEqualTo(65);

    List<BlobLayout.BlobRange> blobRanges = cache.getBlobLayout(eTag, 35, 65);
    // 1st - 35-44 -> host-d
    // 2nd - 45-54 -> host-a
    Assertions.assertThat(blobRanges)
        .describedAs("List should contains 2 elements.")
        .hasSize(2);
    Assertions.assertThat(blobRanges.get(0).host())
        .describedAs("First range host should be host-d.")
        .isEqualTo("host-d");
    Assertions.assertThat(blobRanges.get(1).host())
        .describedAs("Second range host should be host-a.")
        .isEqualTo("host-a");
    cache.deregisterStream(eTag);
  }

  /**
   * Tests cache eviction behavior in BlobLayoutCache.
   * <p>
   * Verifies that after cache eviction, blob layout retrieval returns null.
   */
  @Test
  public void testCacheEviction() throws InterruptedException {
    String eTag = new Throwable().getStackTrace()[0].getMethodName();
    cache.registerStream(eTag, contentLength);
    cache.putBlobLayout(eTag, layoutResponse, contentLength);
    List<BlobLayout.BlobRange> blobRanges = cache.getBlobLayout(eTag, 0, 10);
    Assertions.assertThat(blobRanges)
        .describedAs("List should contains 2 elements.")
        .isNotNull();
    cache.deregisterStream(eTag);
    // wait for some time such that cache will be evicted.
    Thread.sleep(TimeUnit.MINUTES.toMillis(1) + 1000);
    blobRanges = cache.getBlobLayout(eTag, 0, 10);
    Assertions.assertThat(blobRanges)
        .describedAs("List should be null.")
        .isNull();
  }

  /**
   * Tests bridge gap detection with back fill in BlobLayoutCache.
   * <p>
   * Verifies that the correct bridge gap is identified and returned when back filling.
   */
  @Test
  public void testBridgeGapWithBackFill() {
    String eTag = new Throwable().getStackTrace()[0].getMethodName();
    cache.registerStream(eTag, contentLength);
    // Since no data is present, contentLength = 100, pos = 95, maxFetch = 66
    // pos + maxFetch > contentLength, so the gap will be from 36-99.
    BlobLayout.BlobRange blobRange = cache.getBridgeGap(eTag, 95, 64);
    Assertions.assertThat(blobRange)
        .describedAs("Bridge gap should be present.")
        .isNotNull();
    Assertions.assertThat(blobRange.start())
        .describedAs("Bridge gap should be start.")
        .isEqualTo(36);
    Assertions.assertThat(blobRange.end())
        .describedAs("Bridge gap should be end.")
        .isEqualTo(99);
  }

  /**
   * Tests bridge gap detection with forward fill in BlobLayoutCache.
   * <p>
   * Verifies that the correct bridge gap is identified and returned when forward filling.
   */
  @Test
  public void testBridgeGapWithForwardFill() {
    String eTag = new Throwable().getStackTrace()[0].getMethodName();
    cache.registerStream(eTag, contentLength);
    // Since no data is present, contentLength = 100, pos = 30, maxFetch = 66
    // pos + maxFetch < contentLength, so the gap will be from 30-93.
    BlobLayout.BlobRange blobRange = cache.getBridgeGap(eTag, 30, 64);
    Assertions.assertThat(blobRange)
        .describedAs("Bridge gap should be present.")
        .isNotNull();
    Assertions.assertThat(blobRange.start())
        .describedAs("Bridge gap should be start.")
        .isEqualTo(30);
    Assertions.assertThat(blobRange.end())
        .describedAs("Bridge gap should be end.")
        .isEqualTo(93);
  }

  /**
   * Tests that processInFlightPromises correctly initializes a new list for a new eTag.
   */
  @Test
  public void testProcessInFlightPromisesInitialization() {
    String eTag = "test-new-etag";
    CompletableFuture<Void> result = cache.processInFlightPromises(eTag, (promiseList) -> {
      Assertions.assertThat(promiseList)
          .describedAs("Promise list should be initialized if null.")
          .isNotNull();
      Assertions.assertThat(promiseList).isEmpty();
      return CompletableFuture.completedFuture(null);
    });
    Assertions.assertThat(result).isCompleted();
  }

  /**
   * Tests that updates to the promiseList persist across multiple calls for the same eTag.
   */
  @Test
  public void testProcessInFlightPromisesPersistence() {
    String eTag = "test-persistence-etag";
    CompletableFuture<Void> firstFuture = new CompletableFuture<>();

    // First call: Add a promise
    cache.processInFlightPromises(eTag, (list) -> {
      list.add(new BlobLayoutCache.InFlightPromise(0, 10, firstFuture));
      return CompletableFuture.completedFuture(null);
    });

    // Second call: Verify promise exists
    cache.processInFlightPromises(eTag, (list) -> {
      Assertions.assertThat(list).hasSize(1);
      Assertions.assertThat(list.get(0).start()).isEqualTo(0);
      return CompletableFuture.completedFuture(null);
    });
  }

  /**
   * Tests the error propagation from the provided action lambda to the returned future.
   */
  @Test
  public void testProcessInFlightPromisesErrorPropagation() {
    String eTag = "test-error-etag";
    RuntimeException expectedEx = new RuntimeException("Action failed");

    CompletableFuture<Void> result = cache.processInFlightPromises(eTag, (list) -> {
      CompletableFuture<Void> failed = new CompletableFuture<>();
      failed.completeExceptionally(expectedEx);
      return failed;
    });

    Assertions.assertThat(result).isCompletedExceptionally();
    Assertions.assertThatThrownBy(result::get)
        .hasCause(expectedEx);
  }

  /**
   * Tests thread safety by simulating multiple threads accessing the same eTag.
   * ConcurrentHashMap.compute should serialize these operations.
   */
  @Test
  public void testProcessInFlightPromisesConcurrency() throws Exception {
    String eTag = "concurrent-etag";
    int threadCount = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);

    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          cache.processInFlightPromises(eTag, (list) -> {
            // Simulate some work inside the compute block
            list.add(new BlobLayoutCache.InFlightPromise(0, 1, new CompletableFuture<>()));
            return CompletableFuture.completedFuture(null);
          });
        } finally {
          latch.countDown();
        }
      });
    }

    latch.await(5, TimeUnit.SECONDS);

    // Verify all additions were successful
    cache.processInFlightPromises(eTag, (list) -> {
      Assertions.assertThat(list).hasSize(threadCount);
      return CompletableFuture.completedFuture(null);
    });

    executor.shutdown();
  }

  /**
   * Tests that the action can return a future that completes later (Async).
   */
  @Test
  public void testProcessInFlightPromisesAsyncAction() {
    String eTag = "async-etag";
    CompletableFuture<Void> actionResult = new CompletableFuture<>();

    CompletableFuture<Void> returnedFuture = cache.processInFlightPromises(eTag, (list) -> actionResult);

    Assertions.assertThat(returnedFuture).isNotCompleted();

    actionResult.complete(null);

    Assertions.assertThat(returnedFuture).isCompleted();
  }
}
