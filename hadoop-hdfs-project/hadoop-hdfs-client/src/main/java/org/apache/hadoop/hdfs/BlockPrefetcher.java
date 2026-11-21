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
package org.apache.hadoop.hdfs;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.VisibleForTesting;

/**
 * Parallel, chunked sequential read-ahead prefetcher for a single
 * {@link DFSInputStream}.
 *
 * <p>The file is divided into HDFS blocks. The prefetcher keeps a sliding
 * window of up to {@code N = prefetch.size / maxBlockSize} reusable per-block
 * buffers and fetches up to {@code dfs.client.prefetch.threads} blocks
 * <em>concurrently</em> (each a {@link CacheBlock} with its own
 * {@code byte[maxBlockSize]}). Each block is filled in
 * {@code dfs.client.prefetch.chunk.size} pieces; after every chunk the fetch
 * publishes how many bytes are ready via an {@link AtomicLong}, so the reader
 * can consume a block's prefix before the whole block has landed.
 *
 * <p>The reader never blocks on a prefetch. When it reaches an offset that the
 * block's fetch has not yet reached (i.e. the reader has out-run the prefetch),
 * that prefetch is <em>cancelled</em> and the reader falls back to a direct
 * DataNode read for the remainder — the already-fetched prefix is served from
 * cache and the suffix is read remotely, so no byte is fetched twice.
 *
 * <p>The block the reader first lands on (after open or seek) is always read
 * through the normal path; only blocks ahead of the cursor are prefetched.
 * Single-block files never prefetch.
 *
 * <p>Thread-safety: window/buffer bookkeeping is guarded by {@link #lock}. A
 * block's data array has exactly one writer (its fetch thread) and one reader
 * (the foreground); they synchronize lock-free through {@link CacheBlock#
 * bytesReady} (the reader only ever touches bytes below {@code bytesReady},
 * all of which were written before that value was published). Fetch threads
 * perform their (blocking) I/O outside {@link #lock} and never acquire the
 * {@code DFSInputStream} monitor.
 */
@InterfaceAudience.Private
class BlockPrefetcher {

  private static final int SUCCESS = 0;
  private static final int CANCELLED = 1;
  private static final int FAILED = 2;

  /** One HDFS block held in memory, filled incrementally. */
  static final class CacheBlock {
    final byte[] data;            // length == maxBlockSize, reused
    final long blockIndex;
    final long startOffset;       // absolute file offset of block start
    final long endOffset;         // exclusive
    final AtomicLong bytesReady = new AtomicLong(0); // [start, start+ready)
    volatile boolean cancelled;
    volatile boolean done;
    volatile boolean failed;
    // Locality of the reader that filled this block, recorded before any bytes
    // are published (so a reader that observes bytesReady > 0 also observes
    // these). Used to attribute cache-served bytes to the correct
    // local/remote/short-circuit bucket in the read statistics.
    volatile boolean shortCircuit;
    volatile int networkDistance;    // 0 == local, > 0 == remote
    long lastAccessNanos;

    CacheBlock(byte[] data, long blockIndex, long startOffset, long endOffset,
        long nowNanos) {
      this.data = data;
      this.blockIndex = blockIndex;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
      this.lastAccessNanos = nowNanos;
    }

    /** Record the filling reader's locality (called by the fetch thread). */
    void recordLocality(boolean sc, int distance) {
      this.shortCircuit = sc;
      this.networkDistance = distance;
    }

    int length() {
      return (int) (endOffset - startOffset);
    }
  }

  /** Lightweight counters for observability/testing. */
  static final class Metrics {
    long hits;            // reads served from cache
    long misses;          // reads that fell back to the direct/sync path
    long bytesServed;     // bytes copied out of the cache
    long bytesReadDirect; // bytes read straight from a DataNode (cache miss)
    long prefetchSubmitted;
    long prefetchRejected;
    long prefetchOk;
    long prefetchFailed;
    long cancellations;   // prefetches cancelled because the reader caught up
    long bytesPrefetched;
    long evictions;
  }

  private final DFSInputStream in;
  private final DFSClient dfsClient;
  private final long blockSize;      // file's uniform block size (indexing)
  private final int maxBlockSize;    // per-block buffer size
  private final int numBuffers;      // N
  private final int threads;         // max concurrent fetches for this stream
  private final int chunkSize;
  private final long reservedBytes;
  private final long ttlNanos;

  private final ReentrantLock lock = new ReentrantLock();
  private final Map<Long, CacheBlock> window = new HashMap<>(8);
  // Block indices already counted as a prefetch failure. schedule() re-submits a
  // block whose locations are not yet cached on every foreground read, so the
  // same block can fail thousands of times; deduping here makes prefetchFailed
  // count DISTINCT blocks that failed to prefetch (comparable to prefetchOk)
  // instead of per-attempt retries. A failed block leaves the window as it fails,
  // so evictStale() prunes this set by index range to the active read-ahead range
  // [curIdx, curIdx + numBuffers) on every foreground read and evictAll() clears
  // it on unbuffer()/close(); it therefore stays bounded by numBuffers and never
  // grows with stream length. Dedup is thus per window residency: a backward seek
  // that re-approaches an already-pruned failed block may count it again, which is
  // acceptable since the retry storm this fixes happens within one window while the
  // reader is stalled. If a previously-failed block is later prefetched
  // successfully, complete()'s success path removes it here and decrements
  // prefetchFailed, so a recovered block is attributed to prefetchOk rather than
  // counted in both. Never contacts the NameNode.
  private final Set<Long> failedBlockIdx = new HashSet<>();
  private final Deque<byte[]> freeBuffers = new ArrayDeque<>();
  private int allocatedBuffers;
  private int inFlight;

  // Locality of the block that served the most recent cache hit. Written and
  // read on the foreground thread only (serve runs on the caller's thread and
  // consumePrefetched reads these immediately afterwards), so plain fields are
  // sufficient here; the cross-thread visibility of the underlying CacheBlock
  // fields is handled by their volatile declarations.
  private boolean lastServedShortCircuit;
  private int lastServedNetworkDistance;

  private boolean closed;
  private boolean budgetReleased;    // ensures reservedBytes is returned once
  // After close()/onUnbuffer() the pool has been dropped and (for close) the
  // reservation returned; buffers of still-in-flight fetches that return via
  // complete() must NOT be re-pooled or they would pin heap past the released
  // budget. While draining, complete() drops such arrays and decrements
  // allocatedBuffers instead. A subsequent read re-enables pooling (schedule()).
  private boolean draining;
  private final Metrics metrics = new Metrics();
  private ScheduledFuture<?> metricsTask;

  private BlockPrefetcher(DFSInputStream in, DFSClient dfsClient, long blockSize,
      int numBuffers, int threads, int chunkSize, long reservedBytes,
      long ttlNanos) {
    this.in = in;
    this.dfsClient = dfsClient;
    this.blockSize = blockSize;
    this.maxBlockSize = (int) Math.min(blockSize, (long) Integer.MAX_VALUE);
    this.numBuffers = numBuffers;
    this.threads = threads;
    this.chunkSize = chunkSize;
    this.reservedBytes = reservedBytes;
    this.ttlNanos = ttlNanos;
  }

  /** Start periodic metric logging if configured. */
  private void startMetricsLogging() {
    long intervalMs = dfsClient.getConf().getPrefetchMetricsLogIntervalMs();
    if (intervalMs <= 0) {
      return;
    }
    metricsTask = DFSClient.getPrefetchMetricsExecutor().scheduleAtFixedRate(
        new Runnable() {
          @Override
          public void run() {
            logMetrics();
          }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
  }

  /** Add bytes that were read directly from a DataNode (cache miss/catch-up). */
  void recordDirectRead(int n) {
    if (n <= 0) {
      return;
    }
    lock.lock();
    try {
      metrics.bytesReadDirect += n;
    } finally {
      lock.unlock();
    }
  }

  /** Bytes currently resident in the cache (sum of fetched-so-far per block). */
  private long residentBytesLocked() {
    long sum = 0;
    for (CacheBlock cb : window.values()) {
      sum += cb.bytesReady.get();
    }
    return sum;
  }

  private static long mb(long bytes) {
    return bytes / (1024 * 1024);
  }

  /** Emit one read-cache metrics line for this stream. */
  private void logMetrics() {
    long cacheBytes;
    long directBytes;
    long resident;
    int buffers;
    long prefetchOk;
    long failed;
    long cancels;
    lock.lock();
    try {
      cacheBytes = metrics.bytesServed;
      directBytes = metrics.bytesReadDirect;
      resident = residentBytesLocked();
      buffers = allocatedBuffers;
      prefetchOk = metrics.prefetchOk;
      failed = metrics.prefetchFailed;
      cancels = metrics.cancellations;
    } finally {
      lock.unlock();
    }
    long totalRead = cacheBytes + directBytes;
    double hitPct = totalRead == 0 ? 0.0 : (100.0 * cacheBytes / totalRead);
    DFSClient.LOG.info(
        "HDFS read-cache metrics src={} caching=ENABLED cacheReadBytes={} ({} MB) "
            + "directReadBytes={} ({} MB) cacheHitRatio={}% cacheResidentBytes={} "
            + "({} MB) buffers={}/{} bufferCapacityMB={} prefetchOk={} "
            + "prefetchFailed={} cancellations={}",
        in.getSrc(), cacheBytes, mb(cacheBytes), directBytes, mb(directBytes),
        String.format("%.1f", hitPct), resident, mb(resident), buffers,
        numBuffers, mb((long) numBuffers * maxBlockSize), prefetchOk, failed,
        cancels);
  }

  /**
   * Create a prefetcher for {@code in}, or return {@code null} if prefetch is
   * disabled, the file has a single block, the budget is too small to hold two
   * blocks, or the global byte budget is exhausted.
   */
  static BlockPrefetcher maybeCreate(DFSInputStream in, DFSClient dfsClient) {
    if (!dfsClient.getConf().isPrefetchEnabled()
        || dfsClient.getConf().getPrefetchThreadpoolSize() <= 0
        || !dfsClient.isPrefetchEnabled()
        || !in.prefetchEligible()) {
      // The static thread pool is process-wide and is created by the first
      // client that enables prefetch, so dfsClient.isPrefetchEnabled() alone
      // is a JVM-wide signal. Also honour this client's own configuration so a
      // client with prefetch disabled never prefetches, even after another
      // client in the same JVM has enabled it.
      return null;
    }
    long blockSize = in.getBlockSizeForPrefetch();
    if (blockSize <= 0 || blockSize > Integer.MAX_VALUE) {
      return null;
    }
    long size = dfsClient.getConf().getPrefetchTotalSize();
    long fileLen = in.getFileLength();
    // Cap the read-ahead depth (and thus the reservation) by the number of
    // blocks the file actually has, so a small file does not reserve the whole
    // per-stream budget and needlessly starve other concurrent streams out of
    // the shared prefetch byte budget.
    long blocksInFile = (fileLen + blockSize - 1) / blockSize;
    long depth = Math.min(size / blockSize, blocksInFile);
    int n = (int) Math.min(depth, (long) Integer.MAX_VALUE);
    if (n < 2) {
      return null; // need room for at least the current block + one ahead
    }
    long reserve = (long) n * blockSize;
    if (!dfsClient.tryReservePrefetchBytes(reserve)) {
      return null;
    }
    int threads = Math.max(1, dfsClient.getConf().getPrefetchThreads());
    int chunkSize = Math.max(64 * 1024,
        dfsClient.getConf().getPrefetchChunkSize());
    long ttl = TimeUnit.MILLISECONDS.toNanos(
        dfsClient.getConf().getPrefetchTtlMs());
    try {
      BlockPrefetcher p = new BlockPrefetcher(in, dfsClient, blockSize, n,
          threads, chunkSize, reserve, ttl);
      if (dfsClient.getConf().isPrefetchMetricsLogEnabled()) {
        p.startMetricsLogging();
      }
      return p;
    } catch (RuntimeException | Error e) {
      // The global byte budget was already reserved above. If constructing the
      // prefetcher or starting its metrics task fails, the prefetcher is never
      // assigned to the stream and thus never close()d, so return the
      // reservation here to avoid permanently leaking it from the JVM-wide
      // prefetch budget.
      dfsClient.releasePrefetchBytes(reserve);
      throw e;
    }
  }

  private static long now() {
    return System.nanoTime();
  }

  /** Locality of the most recent cache hit; valid only on the foreground
   * thread immediately after a successful {@link #read}. */
  boolean lastServedShortCircuit() {
    return lastServedShortCircuit;
  }

  int lastServedNetworkDistance() {
    return lastServedNetworkDistance;
  }

  /**
   * Serve {@code len} bytes at file offset {@code pos} from the cache (possibly
   * a short read up to the fetched-so-far boundary), after scheduling read
   * ahead. Returns the number of bytes copied, or 0 if the caller should read
   * directly from the DataNode (block not prefetched, or reader has caught up).
   */
  int read(long pos, byte[] buf, int off, int len) {
    schedule(pos);
    lock.lock();
    try {
      return serveLocked(pos, buf, off, len);
    } finally {
      lock.unlock();
    }
  }

  /** ByteBuffer variant of {@link #read(long, byte[], int, int)}. */
  int read(long pos, ByteBuffer dst) {
    int len = dst.remaining();
    if (len == 0) {
      return 0;
    }
    schedule(pos);
    lock.lock();
    try {
      if (closed) {
        return 0;
      }
      long idx = pos / blockSize;
      CacheBlock cb = window.get(idx);
      if (cb == null || cb.failed) {
        if (cb != null) {
          removeAndRecycle(idx, cb);
        }
        metrics.misses++;
        return 0;
      }
      long availEnd = cb.startOffset + cb.bytesReady.get();
      if (pos < availEnd) {
        int n = (int) Math.min((long) len, Math.min(availEnd, cb.endOffset) - pos);
        dst.put(cb.data, (int) (pos - cb.startOffset), n);
        cb.lastAccessNanos = now();
        lastServedShortCircuit = cb.shortCircuit;
        lastServedNetworkDistance = cb.networkDistance;
        metrics.hits++;
        metrics.bytesServed += n;
        return n;
      }
      if (!cb.done) {
        cb.cancelled = true;
        metrics.cancellations++;
      }
      removeAndRecycle(idx, cb);
      metrics.misses++;
      return 0;
    } finally {
      lock.unlock();
    }
  }

  /** Caller must hold {@link #lock}. */
  private int serveLocked(long pos, byte[] buf, int off, int len) {
    if (closed) {
      return 0;
    }
    long idx = pos / blockSize;
    CacheBlock cb = window.get(idx);
    if (cb == null || cb.failed) {
      if (cb != null) {
        removeAndRecycle(idx, cb);
      }
      metrics.misses++;
      return 0;
    }
    long ready = cb.bytesReady.get();
    long availEnd = cb.startOffset + ready;
    if (pos < availEnd) {
      long blockEnd = cb.endOffset;
      int n = (int) Math.min((long) len, Math.min(availEnd, blockEnd) - pos);
      System.arraycopy(cb.data, (int) (pos - cb.startOffset), buf, off, n);
      cb.lastAccessNanos = now();
      lastServedShortCircuit = cb.shortCircuit;
      lastServedNetworkDistance = cb.networkDistance;
      metrics.hits++;
      metrics.bytesServed += n;
      return n;
    }
    // Reader has caught up to (or passed) this block's fetch front: cancel the
    // prefetch and let the caller read the remainder directly.
    if (!cb.done) {
      cb.cancelled = true;
      metrics.cancellations++;
    }
    removeAndRecycle(idx, cb);
    metrics.misses++;
    return 0;
  }

  /**
   * Recycle behind-cursor / stale blocks and top up the read-ahead window with
   * up to {@code threads} concurrent fetches of the blocks ahead of the cursor.
   */
  private void schedule(long pos) {
    lock.lock();
    try {
      if (closed) {
        return;
      }
      draining = false;
      long curIdx = pos / blockSize;
      long fileLen = in.getFileLength();
      if (fileLen <= 0) {
        return;
      }
      long lastIdx = (fileLen - 1) / blockSize;
      evictStale(curIdx);

      long maxAhead = Math.min(curIdx + numBuffers - 1, lastIdx);
      for (long idx = curIdx + 1; idx <= maxAhead; idx++) {
        if (window.containsKey(idx)) {
          continue;
        }
        if (inFlight >= threads) {
          break;
        }
        byte[] backing = pollOrAllocateBuffer();
        if (backing == null) {
          break;
        }
        startPrefetch(idx, fileLen, backing);
      }
    } finally {
      lock.unlock();
    }
  }

  /** Caller must hold {@link #lock}. */
  private void startPrefetch(long idx, long fileLen, byte[] backing) {
    long start = idx * blockSize;
    long end = Math.min(start + blockSize, fileLen);
    final CacheBlock cb = new CacheBlock(backing, idx, start, end, now());
    window.put(idx, cb);
    inFlight++;
    try {
      dfsClient.getPrefetchThreadPool().submit(new Runnable() {
        @Override
        public void run() {
          fillBlock(cb);
        }
      });
      metrics.prefetchSubmitted++;
    } catch (RejectedExecutionException ree) {
      window.remove(idx);
      inFlight--;
      freeBuffers.offer(backing);
      metrics.prefetchRejected++;
    }
  }

  /** Caller must hold {@link #lock}. */
  private byte[] pollOrAllocateBuffer() {
    byte[] b = freeBuffers.poll();
    if (b == null && allocatedBuffers < numBuffers) {
      b = new byte[maxBlockSize];
      allocatedBuffers++;
    }
    return b;
  }

  /** Background worker: fill a block in chunks, publishing progress. */
  private void fillBlock(CacheBlock cb) {
    int outcome = FAILED;
    try {
      in.prefetchBlockChunked(cb, chunkSize, () -> cb.cancelled || closed);
      outcome = (cb.cancelled || closed) ? CANCELLED : SUCCESS;
    } catch (Exception e) {
      // Only ordinary failures are treated as a prefetch miss; fatal VM errors
      // (OutOfMemoryError, StackOverflowError, ...) propagate to the worker
      // thread's uncaught handler instead of being silently swallowed. The
      // finally block below still runs, releasing this block's buffer. The
      // throwable is passed to the logger so the full stack trace is retained.
      DFSClient.LOG.debug("Prefetch of block {} ({} bytes @ {}) failed",
          cb.blockIndex, cb.length(), cb.startOffset, e);
      outcome = FAILED;
    } finally {
      complete(cb, outcome);
    }
  }

  private void complete(CacheBlock cb, int outcome) {
    boolean releaseBudget = false;
    lock.lock();
    try {
      cb.done = true;
      inFlight--;
      boolean keep = outcome == SUCCESS && !cb.cancelled && !closed
          && window.get(cb.blockIndex) == cb;
      if (keep) {
        if (failedBlockIdx.remove(cb.blockIndex)) {
          // This block failed an earlier prefetch attempt but has now been
          // prefetched successfully; attribute it to prefetchOk instead of
          // leaving it double-counted as a failure. The decrement balances the
          // increment made when it was first added below (never goes negative).
          metrics.prefetchFailed--;
        }
        metrics.prefetchOk++;
        metrics.bytesPrefetched += cb.length();
      } else {
        cb.failed = outcome == FAILED;
        if (window.get(cb.blockIndex) == cb) {
          window.remove(cb.blockIndex);
        }
        if (draining) {
          // close()/onUnbuffer() dropped the pool and (for close) returned the
          // reservation; do not re-pool this late in-flight array. Drop it so
          // it becomes GC-eligible and keep allocatedBuffers honest.
          allocatedBuffers--;
        } else {
          freeBuffers.offer(cb.data);
        }
        if (outcome == FAILED && failedBlockIdx.add(cb.blockIndex)) {
          metrics.prefetchFailed++;
        }
      }
      // If the stream was closed while this fetch was in flight, its buffer is
      // now reclaimed; return the global budget once the last in-flight fetch
      // drains so the reservation outlives the buffers it accounts for.
      if (closed && inFlight == 0 && !budgetReleased) {
        budgetReleased = true;
        releaseBudget = true;
      }
    } finally {
      lock.unlock();
    }
    if (releaseBudget) {
      dfsClient.releasePrefetchBytes(reservedBytes);
    }
  }

  /** Caller must hold {@link #lock}. */
  private void evictStale(long curIdx) {
    long nowNanos = now();
    Iterator<Map.Entry<Long, CacheBlock>> it = window.entrySet().iterator();
    while (it.hasNext()) {
      CacheBlock cb = it.next().getValue();
      boolean behind = cb.blockIndex < curIdx;
      boolean expired = cb.blockIndex != curIdx
          && (nowNanos - cb.lastAccessNanos) > ttlNanos;
      if (behind || expired) {
        it.remove();
        recycle(cb);
        metrics.evictions++;
      }
    }
    // Failed blocks are removed from the window when they fail, so they are not
    // reachable by the window loop above. schedule() only ever (re-)submits
    // indices in (curIdx, curIdx + numBuffers), so drop any failure record
    // outside that range; these can never be retried and would otherwise leak
    // for the life of the stream. Keep the marker AT curIdx (idx == curIdx): an
    // earlier retry fetch for the block the reader just entered may still be in
    // flight, and if it recovers, complete()'s success path needs the marker
    // present to move that block from prefetchFailed to prefetchOk. Anything
    // strictly behind the cursor can no longer be retried or recovered. This
    // bounds failedBlockIdx to at most numBuffers entries.
    if (!failedBlockIdx.isEmpty()) {
      failedBlockIdx.removeIf(idx -> idx < curIdx || idx >= curIdx + numBuffers);
    }
  }

  /** Caller must hold {@link #lock}. */
  private void removeAndRecycle(long idx, CacheBlock cb) {
    window.remove(idx);
    recycle(cb);
  }

  /** Caller must hold {@link #lock}. */
  private void recycle(CacheBlock cb) {
    if (cb.done) {
      freeBuffers.offer(cb.data);
    } else {
      cb.cancelled = true; // in-flight fetch recycles on completion
    }
  }

  /** Caller must hold {@link #lock}. */
  private void evictAll() {
    for (CacheBlock cb : window.values()) {
      recycle(cb);
    }
    window.clear();
    failedBlockIdx.clear();
  }

  /**
   * Re-anchor the read-ahead window to a seek target. HDFS blocks are
   * immutable (prefetch is disabled for under-construction files), so a
   * buffered block that falls within the read-ahead window of the new landing
   * block is still valid and is kept; blocks behind the new cursor, or beyond
   * the new read-ahead horizon (e.g. now-far-ahead blocks after a large
   * backward seek, which would otherwise starve the new window of buffers),
   * are dropped. This preserves the read-ahead across intra-block, forward,
   * and backward-within-block seeks instead of tearing it down and re-fetching
   * from scratch.
   */
  void onSeek(long targetPos) {
    lock.lock();
    try {
      if (closed) {
        return;
      }
      long newIdx = targetPos / blockSize;
      long maxKeep = newIdx + numBuffers - 1;
      Iterator<Map.Entry<Long, CacheBlock>> it = window.entrySet().iterator();
      while (it.hasNext()) {
        CacheBlock cb = it.next().getValue();
        if (cb.blockIndex < newIdx || cb.blockIndex > maxKeep) {
          it.remove();
          recycle(cb);
          metrics.evictions++;
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Release cached buffers on unbuffer(). {@code unbuffer()} is an explicit
   * request from the caller (typically a pooled/cached stream that stays open
   * but goes idle) to give back client-side heap, so unlike normal window
   * churn and {@link #onSeek} — where recycled arrays are deliberately kept in
   * {@link #freeBuffers} for reuse — this also drops the pooled arrays and
   * resets the allocation high-water mark so they become GC-eligible. Future
   * reads re-allocate lazily up to {@code numBuffers} again. The global byte
   * budget reservation is intentionally retained until {@link #close()}.
   */
  void onUnbuffer() {
    lock.lock();
    try {
      if (!closed) {
        evictAll();
        freeBuffers.clear();
        // evictAll() only cancels in-flight blocks; their buffers are still
        // held by fetch threads and returned later by complete(). Keep the
        // counter honest for those live arrays (set to inFlight, not 0) so
        // resumed reads cannot allocate past the reserved numBuffers budget,
        // and mark draining so those late returns are dropped (not re-pooled).
        allocatedBuffers = inFlight;
        draining = true;
      }
    } finally {
      lock.unlock();
    }
  }

  /** Free buffers and return the reserved global budget. */
  void close() {
    boolean releaseNow = false;
    lock.lock();
    try {
      if (closed) {
        return;
      }
      closed = true;
      evictAll();
      // evictAll() recycles done blocks (and, later, cancelled in-flight
      // blocks via complete()) into freeBuffers. Since close() hands the global
      // budget back below, drop those pooled arrays too — mirroring
      // onUnbuffer() — so a closed-but-still-referenced stream does not keep up
      // to dfs.client.prefetch.size of heap pinned after its reservation has
      // been returned. allocatedBuffers is set to inFlight (not 0) so any
      // still-live in-flight arrays stay accounted for.
      freeBuffers.clear();
      allocatedBuffers = inFlight;
      draining = true;
      // evictAll() only cancels in-flight blocks; their buffers are still
      // owned by fetch threads and returned later by complete(). Return the
      // global budget now only if nothing is in flight; otherwise the last
      // complete() that drains inFlight to 0 returns it, so live prefetch heap
      // never transiently exceeds dfs.client.prefetch.max.bytes.
      if (inFlight == 0 && !budgetReleased) {
        budgetReleased = true;
        releaseNow = true;
      }
    } finally {
      lock.unlock();
    }
    if (metricsTask != null) {
      metricsTask.cancel(false);
    }
    if (releaseNow) {
      dfsClient.releasePrefetchBytes(reservedBytes);
    }
  }

  @VisibleForTesting
  int getNumBuffers() {
    return numBuffers;
  }

  @VisibleForTesting
  int getAllocatedBuffers() {
    lock.lock();
    try {
      return allocatedBuffers;
    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting
  int getFreeBufferCount() {
    lock.lock();
    try {
      return freeBuffers.size();
    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting
  int getInFlight() {
    lock.lock();
    try {
      return inFlight;
    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting
  int getMaxBlockSize() {
    return maxBlockSize;
  }

  @VisibleForTesting
  Metrics getMetrics() {
    lock.lock();
    try {
      Metrics s = new Metrics();
      s.hits = metrics.hits;
      s.misses = metrics.misses;
      s.bytesServed = metrics.bytesServed;
      s.bytesReadDirect = metrics.bytesReadDirect;
      s.prefetchSubmitted = metrics.prefetchSubmitted;
      s.prefetchRejected = metrics.prefetchRejected;
      s.prefetchOk = metrics.prefetchOk;
      s.prefetchFailed = metrics.prefetchFailed;
      s.cancellations = metrics.cancellations;
      s.bytesPrefetched = metrics.bytesPrefetched;
      s.evictions = metrics.evictions;
      return s;
    } finally {
      lock.unlock();
    }
  }
}
