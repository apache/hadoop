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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;

/**
 * BlobLayout manages the layout of cached blob ranges for Azure Blob File System (ABFS).
 * <p>
 * It tracks which byte ranges of a blob are cached, merges overlapping/adjacent ranges,
 * identifies gaps for efficient fetching, and provides methods to query and update the layout.
 * </p>
 * <p>
 * Thread safety: All range operations are lock-free and thread-safe using ConcurrentSkipListMap.
 * </p>
 */
public class BlobLayout {

  /**
   * BlobRange represents a byte range [start, end] on a blob, optionally associated with a host.
   * @param start the start offset (inclusive)
   * @param end the end offset (inclusive)
   * @param host the endpoint/host serving this range (may be null for gaps)
   */
  public record BlobRange(long start, long end, String host) {}

  /**
   * Map of range start offset to BlobRange, sorted by start offset.
   * Used to efficiently merge, search, and update cached ranges.
   */
  private final ConcurrentSkipListMap<Long, BlobRange> rangeMap;

  /**
   * The total content length of the blob.
   */
  private final long contentLength;

  /**
   * Returns the content length of the blob.
   * @return the content length
   */
  public long getContentLength() {
    return contentLength;
  }

  /**
   * Constructs a BlobLayout for a blob of the given content length.
   * @param contentLength the total length of the blob
   */
  public BlobLayout(final long contentLength) {
    this.rangeMap = new ConcurrentSkipListMap<>();
    this.contentLength = contentLength;
  }

  /**
   * Adds a list of ranges to the layout, associating each with its endpoint/host.
   * Fast, lock-free write. Multiple threads can write different ranges simultaneously.
   * If a range with the same start exists, it is updated.
   * Overlapping starts are handled during read/merge.
   * @param ranges the list of ranges to add
   * @param endpointValueMap map from endpoint index to host value
   */
  public void addRange(List<BlobLayoutResponse.Range> ranges,
      final Map<Integer, String> endpointValueMap) {
    for (var range : ranges) {
      String host = endpointValueMap.get(range.endpointIndex());
      if (host != null) {
        // We use put() directly. If a range with the same start exists,
        // it is updated. Overlapping starts are handled during read.
        rangeMap.put(
            range.start(), new BlobRange(range.start(), range.end(), host));
      }
    }
  }

  /**
   * Returns a merged and clipped list of cached ranges within the requested [start, end] window.
   * Overlapping/adjacent ranges with the same host are merged.
   * @param start the start offset (inclusive)
   * @param end the end offset (inclusive)
   * @return list of BlobRange objects covering the requested window
   */
  public List<BlobRange> getRanges(long start, long end) {
    List<BlobRange> merged = getMergedRangesInternal(start, end);
    List<BlobRange> clipped = new ArrayList<>();

    for (BlobRange range : merged) {
      // Clip the range to the requested window
      long clippedStart = Math.max(range.start(), start);
      long clippedEnd = Math.min(range.end(), end);

      if (clippedStart <= clippedEnd) {
        clipped.add(new BlobRange(clippedStart, clippedEnd, range.host()));
      }
    }
    return clipped;
  }

  /**
   * Identifies exact gaps (unfetched byte ranges) within the requested [start, end] window.
   * Gaps are returned as BlobRange objects with null host.
   * @param start the start offset (inclusive)
   * @param end the end offset (inclusive)
   * @return list of BlobRange objects representing gaps
   */
  public List<BlobRange> getGaps(long start, long end) {
    List<BlobRange> gaps = new ArrayList<>();
    if (start >= contentLength) {
      return gaps;
    }

    long effectiveEnd = Math.min(end, contentLength - 1);
    long currentPos = start;

    // Use the merged view to find gaps accurately
    List<BlobRange> existing = getMergedRangesInternal(start, effectiveEnd);

    for (BlobRange range : existing) {
      if (range.start() > currentPos) {
        gaps.add(new BlobRange(currentPos, range.start() - 1, null));
      }
      currentPos = Math.max(currentPos, range.end() + 1);
    }

    if (currentPos <= effectiveEnd) {
      gaps.add(new BlobRange(currentPos, effectiveEnd, null));
    }
    return gaps;
  }

  /**
   * Returns the next gap (range to fetch) starting at or after the given position, up to maxFetchSize bytes.
   * If the position is already cached, returns null.
   * @param pos the current position
   * @param maxFetchSize the maximum fetch size
   * @return a BlobRange representing the next gap to fetch, or null if already cached
   */
  public BlobRange getBridgeGap(long pos, long maxFetchSize) {
    // 1. Find the cached blocks surrounding the current position
    Map.Entry<Long, BlobRange> floorEntry = rangeMap.floorEntry(pos);
    Map.Entry<Long, BlobRange> ceilingEntry = rangeMap.higherEntry(pos);

    // If already cached, no fetch needed
    if (floorEntry != null && floorEntry.getValue().end() >= pos) {
      return null;
    }

    // 2. Determine Fetch Start: Right after the previous block
    long fetchStart = (floorEntry != null)
        ? floorEntry.getValue().end() + 1
        : 0;

    // 3. Determine Fetch End:
    long fetchEnd = getFetchEnd(maxFetchSize, ceilingEntry, fetchStart);

    return new BlobRange(fetchStart, fetchEnd, null);
  }

  /**
   * Returns the start offset of the next cached range after the given position.
   * @param pos the position to search from
   * @return the start offset of the next cached range, or Long.MAX_VALUE if none
   */
  public long getNextCachedStart(long pos) {
    Map.Entry<Long, BlobRange> ceilingEntry = rangeMap.higherEntry(pos);
    return (ceilingEntry != null)
        ? ceilingEntry.getValue().start()
        : Long.MAX_VALUE;
  }

  /**
   * Returns the fetch start offset for a gap at the given position, considering maxFetch and content length.
   * If the position is already cached, returns -1.
   * @param pos the current position
   * @param maxFetch the maximum fetch size
   * @return the fetch start offset, or -1 if already cached
   */
  public long getFetchStart(long pos, long maxFetch) {
    Map.Entry<Long, BlobRange> floorEntry = rangeMap.floorEntry(pos);
    if (floorEntry != null && floorEntry.getValue().end() >= pos) {
      return -1; // Already cached
    }

    long start = 0;
    if (floorEntry != null) {
      start = floorEntry.getValue().end() + 1;
    }
    if (maxFetch + pos <= contentLength) {
      return pos;
    }
    return Math.max(contentLength - maxFetch, start);
  }

  /**
   * Helper to determine the fetch end offset for a gap, considering the next block and maxFetchSize.
   * @param maxFetchSize the maximum fetch size
   * @param ceilingEntry the next cached block after fetchStart
   * @param fetchStart the fetch start offset
   * @return the fetch end offset
   */
  private long getFetchEnd(final long maxFetchSize,
      final Map.Entry<Long, BlobRange> ceilingEntry,
      final long fetchStart) {
    long fetchEnd;
    if (ceilingEntry != null) {
      // There is a block ahead. Try to bridge the gap entirely.
      long nextBlockStart = ceilingEntry.getValue().start();

      // If the hole is small (e.g., < 64MB), bridge it perfectly
      if (nextBlockStart - fetchStart <= maxFetchSize) {
        fetchEnd = nextBlockStart - 1;
      } else {
        // Hole is too big, just take a 64MB slice
        fetchEnd = fetchStart + maxFetchSize - 1;
      }
    } else {
      // No block ahead, fetch a 64MB slice or up to EOF
      fetchEnd = Math.min(contentLength - 1, fetchStart + maxFetchSize - 1);
    }
    return fetchEnd;
  }

  /**
   * Internal logic to combine overlapping/adjacent ranges from the map.
   * Only ranges with the same host are merged.
   * @param start the start offset (inclusive)
   * @param end the end offset (inclusive)
   * @return list of merged BlobRange objects
   */
  private List<BlobRange> getMergedRangesInternal(long start, long end) {
    Map.Entry<Long, BlobRange> floorEntry = rangeMap.floorEntry(start);
    long searchStart = (floorEntry != null) ? floorEntry.getKey() : start;

    var potentialMatches = rangeMap.subMap(searchStart, true, end, true)
        .values();
    List<BlobRange> merged = new ArrayList<>();
    BlobRange current = null;

    for (BlobRange next : potentialMatches) {
      if (next.end() < start) {
        continue; // Skip ranges that end before our window
      }

      if (current == null) {
        current = next;
      } else {
        // Merge if overlapping or adjacent AND same host
        if (next.start() <= current.end() + 1 && Objects.equals(next.host(),
            current.host())) {
          current = new BlobRange(current.start(),
              Math.max(current.end(), next.end()), current.host());
        } else {
          merged.add(current);
          current = next;
        }
      }
    }
    if (current != null) {
      merged.add(current);
    }
    return merged;
  }

  /**
   * Returns the number of cached ranges currently tracked in the layout.
   * @return the number of cached ranges
   */
  public int getRangeMapSize() {
    return rangeMap.size();
  }
}
