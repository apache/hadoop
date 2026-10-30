/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Ticker;
import org.checkerframework.checker.nullness.qual.NonNull;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import static org.apache.hadoop.fs.azurebfs.services.AbfsInputStream.LOG;

/**
 * BlobLayoutCache is a singleton class that manages caching of BlobLayout objects for Azure Blob File System (ABFS).
 * It uses a Caffeine cache to store layout information for blobs, optimizing range fetches and minimizing redundant network calls.
 *
 * <p>Key Features:</p>
 * <ul>
 *   <li>Evicts cache entries based on idle timeout and cache weight (number of cached ranges).</li>
 *   <li>Tracks active streams to prevent eviction while a stream is open.</li>
 *   <li>Manages in-flight promises for range fetches to avoid duplicate requests.</li>
 *   <li>Provides methods to register/deregister streams, retrieve cached layouts, compute gaps, and manage promises.</li>
 * </ul>
 *
 * <p>Usage:</p>
 * <ul>
 *   <li>Call {@link #registerStream(String, long)} when a stream is opened.</li>
 *   <li>Call {@link #deregisterStream(String)} when a stream is closed.</li>
 *   <li>Use {@link #getBlobLayout(String, long, long)} and {@link #getGaps(String, long, long)} to query cached layout info.</li>
 *   <li>Use {@link #putBlobLayout(String, BlobLayoutResponse, long)} to update the cache with new layout info.</li>
 *   <li>Use {@link #getBridgeGap(String, long, long)} to determine the next range to fetch, considering in-flight and cached ranges.</li>
 *   <li>Use {@link #removePromise(String, long, long)} to remove completed in-flight promises.</li>
 * </ul>
 */
public final class BlobLayoutCache {

  /**
   * LayoutEntry represents a cache entry for a blob's layout.
   * It tracks the BlobLayout, active stream count, and last deregistered time for eviction logic.
   */
  private static class LayoutEntry {

    /** The BlobLayout instance for this entry. */
    private final BlobLayout layout;

    /** Indicates if the layout is present and valid. */
    private AtomicBoolean isLayoutPresent = new AtomicBoolean(true);

    /** Number of active streams using this layout. */
    private final AtomicInteger activeStreams = new AtomicInteger(0);

    /** Last time (in nanoseconds) when all streams were deregistered. Used for idle eviction. */
    private final AtomicLong lastDeregisteredTimeInNanos = new AtomicLong(
        Long.MAX_VALUE);

    /**
     * Constructs a LayoutEntry with the given content length.
     * @param contentLength the length of the blob content
     */
    LayoutEntry(long contentLength) {
      this.layout = new BlobLayout(contentLength);
    }

    /**
     * Checks if the layout is unavailable (not present).
     * @return true if layout is unavailable, false otherwise
     */
    boolean isLayoutUnavailable() {
      return !isLayoutPresent.get();
    }
  }

  /**
   * Caffeine Cache instance replacing ConcurrentHashMap.
   * Uses weighted eviction based on the number of ranges stored in each layout.
   */
  private final Cache<String, LayoutEntry> cache;

  /** Idle timeout in milliseconds for cache eviction. */
  private final long idleTimeoutMs;

  /** Maximum cache weight (total number of cached ranges) before eviction. */
  private final long maxCacheWeight;

  /** Time source used for idle-eviction bookkeeping (injectable for tests). */
  private final Ticker ticker;

  /** Singleton instance of BlobLayoutCache. */
  private static volatile BlobLayoutCache instance = null;

  /** Guards against repeatedly logging the singleton config-mismatch warning. */
  private static volatile boolean configMismatchWarned = false;

  /**
   * Registry of in-flight promises for range fetches, keyed by file ETag.
   * Each value is a thread-safe list of InFlightPromise objects.
   */
  private final ConcurrentHashMap<String, CopyOnWriteArrayList<InFlightPromise>>
      promiseRegistry = new ConcurrentHashMap<>();

  /**
   * InFlightPromise represents a pending fetch for a specific range.
   * @param start the start offset of the range
   * @param end the end offset of the range
   * @param future the CompletableFuture representing the fetch operation
   */
  public record InFlightPromise(long start, long end,
      CompletableFuture<Void> future) {}

  /**
   * Executor to asynchronously execute get blob layout calls
   */
  private static final ExecutorService FETCH_EXECUTOR = new ThreadPoolExecutor(
      8, 32, 60L, TimeUnit.SECONDS,
      new LinkedBlockingQueue<>(1024), // Bounded queue to prevent OOM
      new ThreadFactoryBuilder()
          .setNameFormat("abfs-blob-layout-fetch-%d")
          .setDaemon(true)
          .build(),
      // AbortPolicy keeps the executor non-blocking: when the pool and queue are
      // saturated, submission throws RejectedExecutionException instead of running
      // the fetch on the caller (read) thread. The submitter is responsible for
      // cleaning up the in-flight promise so a later read can retry the gap.
      new ThreadPoolExecutor.AbortPolicy());

  /**
   * Optional override for the fetch executor, used only by tests to inject a
   * deterministic executor (e.g. one that always rejects). {@code null} means the
   * shared {@link #FETCH_EXECUTOR} is used.
   */
  private static volatile ExecutorService fetchExecutorOverride = null;

  static {
    // Add fetch executor to JVM shutdown hook to ensure orderly shutdown of thread during JVM shutdown.
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      FETCH_EXECUTOR.shutdown();
      try {
        if (!FETCH_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
          FETCH_EXECUTOR.shutdownNow();
        }
      } catch (InterruptedException e) {
        FETCH_EXECUTOR.shutdownNow();
      }
    }));
  }

  /**
   * Private constructor for singleton pattern.
   * @param evictionTime the idle timeout in minutes for cache eviction
   * @param maxCacheWeight the maximum total number of cached ranges before eviction
   * @param ticker the time source used for idle-eviction bookkeeping
   */
  private BlobLayoutCache(long evictionTime, long maxCacheWeight, Ticker ticker) {
    this.idleTimeoutMs = TimeUnit.MINUTES.toMillis(evictionTime);
    this.maxCacheWeight = maxCacheWeight;
    this.ticker = ticker;
    this.cache = Caffeine.newBuilder()
        .maximumWeight(maxCacheWeight)
        .ticker(ticker)
        // Weight is determined by the number of cached ranges in the layout
        .weigher(
            (String key, LayoutEntry entry) -> entry.layout.getRangeMapSize())
        .expireAfter(new Expiry<String, LayoutEntry>() {
          /**
           * Calculates the expiry time for a cache entry based on active streams and idle time.
           * @param value the LayoutEntry
           * @param currentTime the current time in nanoseconds
           * @return the time in nanoseconds until expiry
           */
          private long calculateExpiry(LayoutEntry value, long currentTime) {
            long lastDeregistered = value.lastDeregisteredTimeInNanos.get();
            // Do not expire while a stream is active, or before the first
            // deregistration has happened (sentinel Long.MAX_VALUE). The latter
            // check also avoids long overflow in the subtraction below.
            if (value.activeStreams.get() > 0
                || lastDeregistered == Long.MAX_VALUE) {
              return Long.MAX_VALUE;
            }

            // How long has it been since the last stream closed?
            long elapsedNanos = currentTime - lastDeregistered;
            long thresholdNanos = TimeUnit.MILLISECONDS.toNanos(
                idleTimeoutMs);

            // If we are already past the threshold, expire immediately (return 0)
            // Otherwise, return the remaining time until the threshold is hit
            return Math.max(0, thresholdNanos - elapsedNanos);
          }

          @Override
          public long expireAfterCreate(@NonNull String key,
              @NonNull LayoutEntry value, long currentTime) {
            return calculateExpiry(value, currentTime);
          }

          @Override
          public long expireAfterUpdate(@NonNull String key,
              @NonNull LayoutEntry value, long currentTime,
              long currentDuration) {
            return calculateExpiry(value, currentTime);
          }

          @Override
          public long expireAfterRead(@NonNull String key,
              @NonNull LayoutEntry value, long currentTime,
              long currentDuration) {
            // LEASE LOGIC: If streams are active, the entry never expires.
            // Once activeStreams == 0, the 5-minute idle clock starts.
            return calculateExpiry(value, currentTime);
          }
        })
        .removalListener((key, value, cause) -> {
          // Cleanup corresponding promises when an entry is evicted from cache
          if (key != null) {
            promiseRegistry.remove(key);
          }
        })
        .build();
  }

  /**
   * Returns the process-wide singleton instance of BlobLayoutCache, creating it
   * if necessary.
   * <p>
   * The cache is intentionally shared across all filesystem instances in the JVM
   * so that its total memory footprint stays bounded by a single weight budget.
   * As a consequence, the {@code evictionTime} and {@code maxCacheWeight} supplied
   * by the <em>first</em> caller win; values from later callers are ignored. When a
   * later caller requests a different configuration, a one-time warning is logged
   * rather than silently discarding it.
   * </p>
   * @param evictionTime the idle timeout in minutes for cache eviction
   * @param maxCacheWeight the maximum total number of cached ranges before eviction
   * @return the singleton BlobLayoutCache instance
   */
  public static BlobLayoutCache getInstance(long evictionTime,
      long maxCacheWeight) {
    BlobLayoutCache local = instance;
    if (local == null) {
      synchronized (BlobLayoutCache.class) {
        local = instance;
        if (local == null) {
          local = new BlobLayoutCache(evictionTime, maxCacheWeight,
              Ticker.systemTicker());
          instance = local;
        }
      }
    }
    warnIfConfigDiffers(local, evictionTime, maxCacheWeight);
    return local;
  }

  /**
   * Logs a one-time warning if a caller requests a configuration that differs
   * from the one the singleton was initialized with.
   */
  private static void warnIfConfigDiffers(BlobLayoutCache existing,
      long evictionTime, long maxCacheWeight) {
    if (configMismatchWarned) {
      return;
    }
    long requestedIdleMs = TimeUnit.MINUTES.toMillis(evictionTime);
    if (existing.idleTimeoutMs != requestedIdleMs
        || existing.maxCacheWeight != maxCacheWeight) {
      configMismatchWarned = true;
      LOG.warn("BlobLayoutCache is process-wide and already initialized with "
              + "evictionMs={}, maxWeight={}; ignoring requested evictionMs={}, "
              + "maxWeight={}.", existing.idleTimeoutMs, existing.maxCacheWeight,
          requestedIdleMs, maxCacheWeight);
    }
  }

  /**
   * Creates an isolated (non-singleton) cache instance for deterministic testing,
   * allowing a custom {@link Ticker} so idle-based eviction can be exercised
   * without real time passing.
   * @param evictionTime the idle timeout in minutes for cache eviction
   * @param maxCacheWeight the maximum total number of cached ranges before eviction
   * @param ticker the time source to use
   * @return a fresh, isolated BlobLayoutCache instance
   */
  @VisibleForTesting
  static BlobLayoutCache newInstanceForTesting(long evictionTime,
      long maxCacheWeight, Ticker ticker) {
    return new BlobLayoutCache(evictionTime, maxCacheWeight, ticker);
  }

  /**
   * Forces any pending cache maintenance (eviction/expiration) to run. For tests.
   */
  @VisibleForTesting
  void cleanUp() {
    cache.cleanUp();
  }

  /**
   * Returns true if an entry for the given key is currently present (after running
   * pending maintenance). For tests.
   * @param key the cache key (file ETag)
   * @return true if the entry is still cached
   */
  @VisibleForTesting
  boolean isCached(String key) {
    cache.cleanUp();
    return cache.getIfPresent(key) != null;
  }

  /**
   * Returns the approximate number of entries in the cache (after running pending
   * maintenance). For tests.
   * @return the estimated cache size
   */
  @VisibleForTesting
  long estimatedSize() {
    cache.cleanUp();
    return cache.estimatedSize();
  }

  /**
   * Returns the shared executor used to run asynchronous blob layout fetch tasks.
   * Package-private: intended for use by {@code AbfsInputStream} only.
   * @return the shared fetch executor
   */
  static ExecutorService getFetchExecutor() {
    ExecutorService override = fetchExecutorOverride;
    return override != null ? override : FETCH_EXECUTOR;
  }

  /**
   * Overrides the fetch executor for tests so rejection/non-blocking behaviour can
   * be exercised deterministically. Callers must invoke
   * {@link #resetFetchExecutorForTesting()} afterwards.
   * @param executor the executor to use, or {@code null} to reset
   */
  @VisibleForTesting
  static void setFetchExecutorForTesting(ExecutorService executor) {
    fetchExecutorOverride = executor;
  }

  /**
   * Restores the default shared fetch executor after a test override. For tests.
   */
  @VisibleForTesting
  static void resetFetchExecutorForTesting() {
    fetchExecutorOverride = null;
  }

  /**
   * Returns the number of in-flight fetch promises currently registered for the
   * given file ETag. Used by tests to assert that promises are not leaked. For tests.
   * @param fileETag the file ETag key
   * @return the number of in-flight promises, or 0 if none are registered
   */
  @VisibleForTesting
  int getInFlightPromiseCount(String fileETag) {
    CopyOnWriteArrayList<InFlightPromise> promises = promiseRegistry.get(fileETag);
    return promises == null ? 0 : promises.size();
  }

  /**
   * Registers a stream for the given file ETag and content length.
   * Increments the active stream count and blocks eviction.
   * @param fileETag the file ETag
   * @param contentLength the content length of the blob
   */
  public void registerStream(String fileETag, long contentLength) {
    cache.asMap().compute(fileETag, (key, entry) -> {
      if (entry == null) {
        entry = new LayoutEntry(contentLength);
      }
      entry.activeStreams.incrementAndGet();
      entry.lastDeregisteredTimeInNanos.set(
          Long.MAX_VALUE); // Block eviction logic
      return entry;
    });
  }

  /**
   * Deregisters a stream for the given file ETag.
   * Decrements the active stream count and updates the last deregistered time for eviction.
   * @param fileETag the file ETag
   */
  public void deregisterStream(String fileETag) {
    cache.asMap().computeIfPresent(fileETag, (key, entry) -> {
      // Clamp at zero so unbalanced deregister calls cannot drive the counter
      // negative and keep resetting the idle clock.
      int remaining = entry.activeStreams.updateAndGet(v -> v > 0 ? v - 1 : 0);
      if (remaining == 0) {
        entry.lastDeregisteredTimeInNanos.set(ticker.read());
      }
      return entry;
    });
  }

  /**
   * Retrieves the cached blob layout ranges for the specified key and range.
   * @param key the cache key (file ETag)
   * @param start the start offset
   * @param end the end offset
   * @return a list of BlobRange objects, or null if not available
   */
  public List<BlobLayout.BlobRange> getBlobLayout(final String key,
      final long start, final long end) {
    LayoutEntry layoutEntry = cache.getIfPresent(key);
    return (layoutEntry == null || layoutEntry.isLayoutUnavailable())
        ? null : layoutEntry.layout.getRanges(start, end);
  }

  /**
   * Retrieves the gaps (unfetched ranges) in the cached layout for the specified key and range.
   * @param key the cache key (file ETag)
   * @param start the start offset
   * @param end the end offset
   * @return a list of BlobRange objects representing gaps, or null if not available
   */
  public List<BlobLayout.BlobRange> getGaps(final String key,
      final long start, final long end) {
    LayoutEntry layoutEntry = cache.getIfPresent(key);
    return layoutEntry == null || layoutEntry.isLayoutUnavailable()
        ? null : layoutEntry.layout.getGaps(start, end);
  }

  /**
   * Determines the next range to fetch (bridge gap) for the given file ETag, position, and max fetch size.
   * Considers both cached and in-flight (promised) ranges to avoid redundant fetches.
   * @param fileETag the file ETag
   * @param pos the current position
   * @param maxFetch the maximum fetch size
   * @return a BlobRange representing the next gap to fetch, or null if none
   */
  public BlobLayout.BlobRange getBridgeGap(String fileETag,
      long pos,
      long maxFetch) {
    LayoutEntry layoutEntry = cache.getIfPresent(fileETag);
    if (layoutEntry == null || layoutEntry.isLayoutUnavailable()) {
      return null;
    }

    long fetchStart = layoutEntry.layout.getFetchStart(pos, maxFetch);
    if (fetchStart == -1) {
      return null;
    }

    long nextPromisedStart = Long.MAX_VALUE;
    CopyOnWriteArrayList<InFlightPromise> filePromises = promiseRegistry.get(
        fileETag);
    if (filePromises != null) {
      for (InFlightPromise p : filePromises) {
        if (p.start() > pos && p.start() < nextPromisedStart) {
          nextPromisedStart = p.start();
        }
      }
    }

    long nextCachedStart = layoutEntry.layout.getNextCachedStart(pos);
    long nextWall = Math.min(nextCachedStart, nextPromisedStart);

    long fetchEnd;
    if (nextWall != Long.MAX_VALUE && (nextWall - fetchStart <= maxFetch)) {
      fetchEnd = nextWall - 1;
    } else {
      fetchEnd = Math.min(layoutEntry.layout.getContentLength() - 1,
          fetchStart + maxFetch - 1);
    }

    return new BlobLayout.BlobRange(fetchStart, fetchEnd, null);
  }

  /**
   * Updates the cache with a new BlobLayoutResponse for the specified key.
   * <p>
   * Semantics:
   * <ul>
   *   <li>A {@code null} response represents a <em>transient</em> fetch failure. The
   *   entry is left untouched (still available) so the next read can retry the gap;
   *   it is <strong>not</strong> permanently poisoned.</li>
   *   <li>A non-null response with no endpoints and no ranges means the server has
   *   no distributed layout for this blob. The entry is marked unavailable so callers
   *   fall back to host-URL based fetches.</li>
   *   <li>A non-null response with layout data is merged in and the entry is (re)marked
   *   available, allowing recovery from a previous unavailable state.</li>
   * </ul>
   * The update runs inside {@link java.util.concurrent.ConcurrentHashMap#compute}
   * (via the cache's map view) so that Caffeine re-evaluates the entry weight after
   * ranges are added; otherwise weight-based eviction would never see the growth.
   * </p>
   * @param key the cache key (file ETag)
   * @param layoutResponse the BlobLayoutResponse containing new layout info
   * @param contentLength the content length of the blob
   */
  public void putBlobLayout(final String key,
      final BlobLayoutResponse layoutResponse,
      final long contentLength) {
    cache.asMap().compute(key, (k, existing) -> {
      LayoutEntry entry = (existing != null)
          ? existing : new LayoutEntry(contentLength);

      if (layoutResponse == null) {
        // Transient failure: keep the entry retryable, do not poison it.
        LOG.debug("Null layout response for key: {}. Will retry on next access.",
            key);
        return entry;
      }

      if (layoutResponse.getEndpoints().isEmpty()
          && layoutResponse.getRanges().isEmpty()) {
        // Server reports no distributed layout for this blob.
        entry.isLayoutPresent.set(false);
        return entry;
      }

      Map<Integer, String> endpointValueMap = layoutResponse.getEndpoints()
          .stream()
          .collect(Collectors.toMap(
              BlobLayoutResponse.Endpoint::index,
              BlobLayoutResponse.Endpoint::value,
              (existingValue, replacement) -> existingValue
          ));

      entry.layout.addRange(layoutResponse.getRanges(), endpointValueMap);
      // Recover availability in case the entry was previously marked unavailable.
      entry.isLayoutPresent.set(true);
      return entry;
    });
  }

  /**
   * Removes a completed in-flight promise for the specified file ETag and range.
   * Cleans up the promise registry if no promises remain for the file.
   * @param fileETag the file ETag
   * @param start the start offset of the range
   * @param end the end offset of the range
   */
  public void removePromise(String fileETag, long start, long end) {
    promiseRegistry.computeIfPresent(fileETag, (key, list) -> {
      list.removeIf(p -> p.start() == start && p.end() == end);
      return list.isEmpty() ? null : list;
    });
  }

  /**
   * Atomically applies the given action to the in-flight promise list for the specified eTag.
   * <p>
   * The action receives the current (possibly empty) promise list, may mutate it to register
   * new in-flight fetches, and returns a {@link CompletableFuture} representing when all
   * required ranges are satisfied. The mutation and future creation happen inside a single
   * {@link java.util.concurrent.ConcurrentHashMap#compute} call, ensuring visibility to
   * concurrent callers.
   * </p>
   * @param eTag the file ETag identifying the promise list
   * @param action the logic to apply; receives the live promise list and returns a future
   * @return the future returned by the action
   */
  public CompletableFuture<Void> processInFlightPromises(String eTag,
      Function<List<InFlightPromise>, CompletableFuture<Void>> action) {

    // We use an AtomicReference to get the result out of the compute block
    AtomicReference<CompletableFuture<Void>> resultRef
        = new AtomicReference<>();

    promiseRegistry.compute(eTag, (key, promiseList) -> {
      if (promiseList == null) {
        promiseList = new CopyOnWriteArrayList<>();
      }

      // Execute the logic provided by the caller
      resultRef.set(action.apply(promiseList));

      return promiseList;
    });

    return resultRef.get();
  }
}
