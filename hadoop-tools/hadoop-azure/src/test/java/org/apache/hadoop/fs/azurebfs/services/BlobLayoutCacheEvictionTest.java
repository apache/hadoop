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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.github.benmanes.caffeine.cache.Ticker;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;

/**
 * Unit tests covering weight-based and idle-based eviction of
 * {@link BlobLayoutCache}, plus regressions for the availability/recovery and
 * stream-counter lifecycle bugs. These tests use isolated (non-singleton) cache
 * instances with an injectable {@link Ticker} so eviction can be exercised
 * deterministically without real time passing.
 */
public class BlobLayoutCacheEvictionTest {

  private static final long CONTENT_LENGTH = 100;

  private static final long EVICTION_MINUTES = 1;

  private static final long LARGE_WEIGHT = 1000;

  private static final long ONE_MINUTE_NANOS = TimeUnit.MINUTES.toNanos(1);

  /**
   * A controllable time source so idle-based eviction can be tested without
   * sleeping. Implements Caffeine's {@link Ticker} and is also used by the cache
   * for its own idle bookkeeping (via {@code newInstanceForTesting}).
   */
  private static final class FakeTicker implements Ticker {
    private final AtomicLong nanos = new AtomicLong(0);

    @Override
    public long read() {
      return nanos.get();
    }

    void advance(long deltaNanos) {
      nanos.addAndGet(deltaNanos);
    }
  }

  private BlobLayoutResponse responseWithRanges(int rangeCount) {
    BlobLayoutResponse response = new BlobLayoutResponse();
    List<BlobLayoutResponse.Range> ranges = new java.util.ArrayList<>();
    Set<BlobLayoutResponse.Endpoint> endpoints = new java.util.HashSet<>();
    long cursor = 0;
    for (int i = 0; i < rangeCount; i++) {
      ranges.add(new BlobLayoutResponse.Range(cursor, cursor + 9, i));
      endpoints.add(new BlobLayoutResponse.Endpoint(i, "host-" + i));
      cursor += 10;
    }
    response.setRanges(ranges);
    response.setEndpoints(endpoints);
    return response;
  }

  // ---------------------------------------------------------------------------
  // Weight-based eviction (Bug #1: entry weight never updated after addRange).
  // ---------------------------------------------------------------------------

  /**
   * With a small weight budget, inserting more total ranges than the budget must
   * evict entries. Before the fix the weigher always saw 0 (ranges were added by
   * mutating the entry in place, which Caffeine never re-weighed), so nothing was
   * ever evicted and all entries stayed resident.
   */
  @Test
  public void testWeightBasedEvictionRemovesEntries() {
    long maxWeight = 2;
    BlobLayoutCache cache = BlobLayoutCache.newInstanceForTesting(
        EVICTION_MINUTES, maxWeight, new FakeTicker());

    int entryCount = 5;
    for (int i = 0; i < entryCount; i++) {
      cache.putBlobLayout("etag-" + i, responseWithRanges(1), CONTENT_LENGTH);
    }

    Assertions.assertThat(cache.estimatedSize())
        .describedAs("Total weight (ranges) must be bounded by maxWeight=%d, so "
            + "most of the %d single-range entries must be evicted.", maxWeight,
            entryCount)
        .isLessThanOrEqualTo(maxWeight)
        .isLessThan(entryCount);
  }

  /**
   * An entry's weight must reflect the number of ranges added via putBlobLayout.
   * Two entries whose combined range count exceeds the budget must trigger
   * eviction. Before the fix both would have weighed 0 and both would have
   * remained resident.
   */
  @Test
  public void testWeightReflectsRangeCount() {
    long maxWeight = 3;
    BlobLayoutCache cache = BlobLayoutCache.newInstanceForTesting(
        EVICTION_MINUTES, maxWeight, new FakeTicker());

    cache.putBlobLayout("light", responseWithRanges(1), CONTENT_LENGTH);
    // 4 ranges added to a budget of 3 pushes the combined weight (5) over the
    // limit, so at least one entry must be evicted.
    cache.putBlobLayout("heavy", responseWithRanges(4), CONTENT_LENGTH);

    Assertions.assertThat(cache.estimatedSize())
        .describedAs("Combined range weight (5) exceeds budget (%d); at least one "
            + "entry must be evicted. If weight were stuck at 0 both would "
            + "survive.", maxWeight)
        .isLessThan(2)
        .isGreaterThanOrEqualTo(1);
  }

  /**
   * When the total weight stays within the budget, nothing should be evicted.
   */
  @Test
  public void testNoEvictionWhenUnderWeightBudget() {
    BlobLayoutCache cache = BlobLayoutCache.newInstanceForTesting(
        EVICTION_MINUTES, LARGE_WEIGHT, new FakeTicker());

    for (int i = 0; i < 3; i++) {
      cache.putBlobLayout("etag-" + i, responseWithRanges(1), CONTENT_LENGTH);
    }

    Assertions.assertThat(cache.estimatedSize())
        .describedAs("No entry should be evicted while under the weight budget.")
        .isEqualTo(3);
  }

  // ---------------------------------------------------------------------------
  // Idle-timeout eviction (uses the injected ticker for deterministic time).
  // ---------------------------------------------------------------------------

  /**
   * After the last stream is deregistered and the idle timeout elapses, the entry
   * must be evicted.
   */
  @Test
  public void testIdleEvictionAfterDeregister() {
    FakeTicker ticker = new FakeTicker();
    BlobLayoutCache cache = BlobLayoutCache.newInstanceForTesting(
        EVICTION_MINUTES, LARGE_WEIGHT, ticker);
    String eTag = "idle-evict";

    cache.registerStream(eTag, CONTENT_LENGTH);
    cache.putBlobLayout(eTag, responseWithRanges(4), CONTENT_LENGTH);
    cache.deregisterStream(eTag);

    // Advance just past the idle timeout.
    ticker.advance(ONE_MINUTE_NANOS + 1);

    Assertions.assertThat(cache.isCached(eTag))
        .describedAs("Entry should be evicted once idle timeout elapses.")
        .isFalse();
  }

  /**
   * An entry with an active stream must never be idle-evicted, regardless of how
   * much time passes.
   */
  @Test
  public void testActiveStreamPreventsIdleEviction() {
    FakeTicker ticker = new FakeTicker();
    BlobLayoutCache cache = BlobLayoutCache.newInstanceForTesting(
        EVICTION_MINUTES, LARGE_WEIGHT, ticker);
    String eTag = "active-no-evict";

    cache.registerStream(eTag, CONTENT_LENGTH);
    cache.putBlobLayout(eTag, responseWithRanges(4), CONTENT_LENGTH);

    // No deregister -> stream is active. Advance far beyond the timeout.
    ticker.advance(ONE_MINUTE_NANOS * 100);

    Assertions.assertThat(cache.isCached(eTag))
        .describedAs("Entry must not be evicted while a stream is active.")
        .isTrue();
  }

  /**
   * Re-registering a stream before the idle timeout fires must cancel the pending
   * eviction.
   */
  @Test
  public void testReRegisterBeforeTimeoutPreventsEviction() {
    FakeTicker ticker = new FakeTicker();
    BlobLayoutCache cache = BlobLayoutCache.newInstanceForTesting(
        EVICTION_MINUTES, LARGE_WEIGHT, ticker);
    String eTag = "re-register";

    cache.registerStream(eTag, CONTENT_LENGTH);
    cache.putBlobLayout(eTag, responseWithRanges(4), CONTENT_LENGTH);
    cache.deregisterStream(eTag);

    // Advance to just under the timeout, then re-open the stream.
    ticker.advance(ONE_MINUTE_NANOS / 2);
    cache.registerStream(eTag, CONTENT_LENGTH);

    // Advance well beyond the original timeout.
    ticker.advance(ONE_MINUTE_NANOS * 2);

    Assertions.assertThat(cache.isCached(eTag))
        .describedAs("Re-registered (active) entry must survive the original "
            + "idle deadline.")
        .isTrue();
  }

  /**
   * A freshly deregistered entry must survive right up to the timeout boundary.
   */
  @Test
  public void testNoEvictionJustBeforeTimeout() {
    FakeTicker ticker = new FakeTicker();
    BlobLayoutCache cache = BlobLayoutCache.newInstanceForTesting(
        EVICTION_MINUTES, LARGE_WEIGHT, ticker);
    String eTag = "almost-evict";

    cache.registerStream(eTag, CONTENT_LENGTH);
    cache.putBlobLayout(eTag, responseWithRanges(4), CONTENT_LENGTH);
    cache.deregisterStream(eTag);

    ticker.advance(ONE_MINUTE_NANOS - 1);

    Assertions.assertThat(cache.isCached(eTag))
        .describedAs("Entry should still be present just before the timeout.")
        .isTrue();
  }

  // ---------------------------------------------------------------------------
  // Availability recovery (Bug #2 / #3).
  // ---------------------------------------------------------------------------

  /**
   * A transient (null-response) fetch failure must not poison the entry: the
   * layout stays available and usable, and the correct content length is honored
   * so gap computation is accurate.
   */
  @Test
  public void testTransientFailureDoesNotPoisonEntry() {
    BlobLayoutCache cache = BlobLayoutCache.newInstanceForTesting(
        EVICTION_MINUTES, LARGE_WEIGHT, new FakeTicker());
    String eTag = "transient-fail";

    // Simulate a transient fetch failure on a fresh key.
    cache.putBlobLayout(eTag, null, CONTENT_LENGTH);

    // Layout must remain available (not null) rather than being poisoned...
    List<BlobLayout.BlobRange> layout = cache.getBlobLayout(eTag, 0, 50);
    Assertions.assertThat(layout)
        .describedAs("Transient failure must not mark the layout unavailable.")
        .isNotNull()
        .isEmpty();

    // ...and the real content length must be used so the whole window is a gap.
    List<BlobLayout.BlobRange> gaps = cache.getGaps(eTag, 0, 50);
    Assertions.assertThat(gaps)
        .describedAs("With no ranges cached, the full window must be a gap.")
        .hasSize(1);
    Assertions.assertThat(gaps.get(0).start()).isEqualTo(0);
    Assertions.assertThat(gaps.get(0).end()).isEqualTo(50);
  }

  /**
   * A genuine "no distributed layout" signal (empty server response) marks the
   * entry unavailable, and a later successful fetch must recover availability.
   */
  @Test
  public void testUnavailableEntryRecoversOnSuccessfulFetch() {
    BlobLayoutCache cache = BlobLayoutCache.newInstanceForTesting(
        EVICTION_MINUTES, LARGE_WEIGHT, new FakeTicker());
    String eTag = "recoverable";

    cache.registerStream(eTag, CONTENT_LENGTH);

    // Empty server response -> no distributed layout -> unavailable.
    cache.putBlobLayout(eTag, new BlobLayoutResponse(), CONTENT_LENGTH);
    Assertions.assertThat(cache.getBlobLayout(eTag, 0, 10))
        .describedAs("Empty server response should mark the layout unavailable.")
        .isNull();

    // A later successful fetch must recover availability.
    cache.putBlobLayout(eTag, responseWithRanges(4), CONTENT_LENGTH);
    List<BlobLayout.BlobRange> layout = cache.getBlobLayout(eTag, 0, 10);
    Assertions.assertThat(layout)
        .describedAs("Successful fetch must recover a previously unavailable "
            + "layout.")
        .isNotNull()
        .isNotEmpty();
  }

  // ---------------------------------------------------------------------------
  // Stream-counter lifecycle (Bug #6: counter could go negative).
  // ---------------------------------------------------------------------------

  /**
   * Unbalanced (extra) deregister calls must not corrupt the active-stream count.
   * After over-deregistering, a single register must still mark the entry active
   * so it is protected from idle eviction.
   */
  @Test
  public void testExtraDeregisterDoesNotCorruptActiveCount() {
    FakeTicker ticker = new FakeTicker();
    BlobLayoutCache cache = BlobLayoutCache.newInstanceForTesting(
        EVICTION_MINUTES, LARGE_WEIGHT, ticker);
    String eTag = "counter-guard";

    cache.registerStream(eTag, CONTENT_LENGTH);
    cache.putBlobLayout(eTag, responseWithRanges(4), CONTENT_LENGTH);

    // Over-deregister: without clamping this would drive the counter negative.
    cache.deregisterStream(eTag);
    cache.deregisterStream(eTag);

    // A single register must now make the entry active again.
    cache.registerStream(eTag, CONTENT_LENGTH);

    ticker.advance(ONE_MINUTE_NANOS * 10);

    Assertions.assertThat(cache.isCached(eTag))
        .describedAs("A single register after over-deregistering must restore "
            + "active state and prevent idle eviction.")
        .isTrue();
  }

  // ---------------------------------------------------------------------------
  // Partial / corrupted data and eviction-during-read scenarios.
  // ---------------------------------------------------------------------------

  /**
   * Builds a response with {@code rangeCount} ranges but intentionally omits the
   * endpoint mapping for {@code missingEndpointIndex}, simulating a partial /
   * corrupted server payload where some ranges have no resolvable host.
   */
  private BlobLayoutResponse responseWithMissingEndpoint(int rangeCount,
      int missingEndpointIndex) {
    BlobLayoutResponse response = new BlobLayoutResponse();
    List<BlobLayoutResponse.Range> ranges = new java.util.ArrayList<>();
    Set<BlobLayoutResponse.Endpoint> endpoints = new java.util.HashSet<>();
    long cursor = 0;
    for (int i = 0; i < rangeCount; i++) {
      ranges.add(new BlobLayoutResponse.Range(cursor, cursor + 9, i));
      if (i != missingEndpointIndex) {
        endpoints.add(new BlobLayoutResponse.Endpoint(i, "host-" + i));
      }
      cursor += 10;
    }
    response.setRanges(ranges);
    response.setEndpoints(endpoints);
    return response;
  }

  /**
   * A partial payload (one range has no matching endpoint host) must cache only the
   * resolvable ranges; the bytes of the unmapped range must remain a gap so the
   * reader re-fetches them rather than trusting an incomplete layout.
   */
  @Test
  public void testPartialLayoutLeavesGapForUnmappedRange() {
    BlobLayoutCache cache = BlobLayoutCache.newInstanceForTesting(
        EVICTION_MINUTES, LARGE_WEIGHT, new FakeTicker());
    String eTag = "partial-layout";

    cache.registerStream(eTag, CONTENT_LENGTH);
    // Ranges 0-9 (ep0), 10-19 (ep1), 20-29 (ep2); endpoint 1 host is missing.
    cache.putBlobLayout(eTag, responseWithMissingEndpoint(3, 1), CONTENT_LENGTH);

    List<BlobLayout.BlobRange> layout = cache.getBlobLayout(eTag, 0, 29);
    Assertions.assertThat(layout)
        .describedAs("Only ranges with a resolvable host must be cached.")
        .hasSize(2);

    List<BlobLayout.BlobRange> gaps = cache.getGaps(eTag, 0, 29);
    Assertions.assertThat(gaps)
        .describedAs("The unmapped range's bytes must remain a gap to retry.")
        .hasSize(1);
    Assertions.assertThat(gaps.get(0).start()).isEqualTo(10);
    Assertions.assertThat(gaps.get(0).end()).isEqualTo(19);
  }

  /**
   * Models the cache being evicted "during" a read: after a successful layout is
   * cached and observed, idle eviction removes the entry. A subsequent read must
   * return null (cache miss) so the reader falls back to a normal fetch instead of
   * dereferencing a stale/absent entry.
   */
  @Test
  public void testEntryEvictedBetweenReadsReturnsNull() {
    FakeTicker ticker = new FakeTicker();
    BlobLayoutCache cache = BlobLayoutCache.newInstanceForTesting(
        EVICTION_MINUTES, LARGE_WEIGHT, ticker);
    String eTag = "evicted-mid-read";

    cache.registerStream(eTag, CONTENT_LENGTH);
    cache.putBlobLayout(eTag, responseWithRanges(4), CONTENT_LENGTH);

    // First read sees the cached layout.
    Assertions.assertThat(cache.getBlobLayout(eTag, 0, 10))
        .describedAs("Layout must be present for the first read.")
        .isNotNull()
        .isNotEmpty();

    // Stream closes and the idle timeout elapses -> entry evicted.
    cache.deregisterStream(eTag);
    ticker.advance(ONE_MINUTE_NANOS * 10);
    cache.cleanUp();

    Assertions.assertThat(cache.isCached(eTag))
        .describedAs("Entry must be idle-evicted after the stream closes.")
        .isFalse();
    Assertions.assertThat(cache.getBlobLayout(eTag, 0, 10))
        .describedAs("A read after eviction must return null (cache miss).")
        .isNull();
    Assertions.assertThat(cache.getGaps(eTag, 0, 10))
        .describedAs("Gaps after eviction must also return null (cache miss).")
        .isNull();
  }

  /**
   * When an entry is evicted, its in-flight promise list must be cleaned up by the
   * removal listener so evicted keys do not leak pending-fetch bookkeeping.
   */
  @Test
  public void testEvictionClearsInFlightPromises() {
    FakeTicker ticker = new FakeTicker();
    BlobLayoutCache cache = BlobLayoutCache.newInstanceForTesting(
        EVICTION_MINUTES, LARGE_WEIGHT, ticker);
    String eTag = "promise-cleanup";

    cache.registerStream(eTag, CONTENT_LENGTH);
    cache.putBlobLayout(eTag, responseWithRanges(2), CONTENT_LENGTH);

    // Register an in-flight promise for this eTag.
    cache.processInFlightPromises(eTag, promises -> {
      promises.add(new BlobLayoutCache.InFlightPromise(0, 9,
          new java.util.concurrent.CompletableFuture<>()));
      return java.util.concurrent.CompletableFuture.completedFuture(null);
    });
    Assertions.assertThat(cache.getInFlightPromiseCount(eTag))
        .describedAs("Promise must be registered before eviction.")
        .isEqualTo(1);

    // Force idle eviction of the entry.
    cache.deregisterStream(eTag);
    ticker.advance(ONE_MINUTE_NANOS * 10);
    cache.cleanUp();

    Assertions.assertThat(cache.isCached(eTag))
        .describedAs("Entry must be evicted.")
        .isFalse();
    Assertions.assertThat(cache.getInFlightPromiseCount(eTag))
        .describedAs("Eviction must clear the in-flight promise registry for the "
            + "key so no fetch bookkeeping leaks.")
        .isEqualTo(0);
  }

  /**
   * After an entry is evicted, a late-arriving successful fetch must transparently
   * re-create the entry so the layout self-heals rather than being permanently
   * lost.
   */
  @Test
  public void testPutRecreatesEntryAfterEviction() {
    FakeTicker ticker = new FakeTicker();
    BlobLayoutCache cache = BlobLayoutCache.newInstanceForTesting(
        EVICTION_MINUTES, LARGE_WEIGHT, ticker);
    String eTag = "self-heal";

    cache.registerStream(eTag, CONTENT_LENGTH);
    cache.putBlobLayout(eTag, responseWithRanges(2), CONTENT_LENGTH);

    cache.deregisterStream(eTag);
    ticker.advance(ONE_MINUTE_NANOS * 10);
    cache.cleanUp();
    Assertions.assertThat(cache.isCached(eTag)).isFalse();

    // A fetch that completes after eviction must recreate the entry.
    cache.putBlobLayout(eTag, responseWithRanges(3), CONTENT_LENGTH);
    List<BlobLayout.BlobRange> layout = cache.getBlobLayout(eTag, 0, 29);
    Assertions.assertThat(layout)
        .describedAs("A put after eviction must recreate a usable layout.")
        .isNotNull()
        .isNotEmpty();
  }
}
