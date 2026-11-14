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

import org.apache.hadoop.fs.azurebfs.contracts.services.BlobLayoutResponse;
import org.apache.hadoop.fs.azurebfs.services.BlobLayout.BlobRange;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link BlobLayout} class, focusing on the merging and clipping of overlapping blob ranges.
 * <p>
 * These tests verify that the BlobLayout correctly merges overlapping ranges and clips the output
 * to the requested range, preserving the host information. The test scenarios are based on a specific
 * set of overlapping input ranges and validate both edge and mid-block clipping behavior.
 */
public class BlobLayoutTest {

  /**
   * Verifies that overlapping and fragmented blob ranges are merged correctly and that the
   * returned range is clipped to the requested bounds. Also checks that the host information
   * is preserved in the merged result.
   *
   * Scenario:
   * - Adds overlapping ranges to the BlobLayout.
   * - Queries a sub-range (0-2) and expects a single merged and clipped range.
   * - Queries a mid-block range (5-9) and expects a single merged and clipped range.
   */
  @Test
  public void testOverlappingFragmentedMerging() {
    // Content length doesn't matter much for this specific test
    BlobLayout layout = getBlobLayout();

    // 2. Query exactly 0-2
    List<BlobRange> result = layout.getRanges(0, 2);

    // Verification
    Assertions.assertThat(result.size())
        .describedAs("Expected exactly one merged range")
        .isEqualTo(1);
    BlobRange range = result.get(0);

    // Even though the internal merged block is 0-10, the output must be clipped to the request
    Assertions.assertThat(range.start())
        .describedAs("Start should be clipped to 0")
        .isEqualTo(0);
    Assertions.assertThat(range.end())
        .describedAs("End should be clipped to 2")
        .isEqualTo(2);
    Assertions.assertThat(range.host())
        .describedAs("Host should be preserved")
        .isEqualTo("host-a");

    // 3. Query 5-9 (to check mid-block clipping)
    List<BlobRange> result2 = layout.getRanges(5, 9);
    Assertions.assertThat(result2.size())
        .describedAs("Expected exactly one merged range")
        .isEqualTo(1);
    Assertions.assertThat(result2.get(0).start())
        .describedAs("Start should be clipped to 5")
        .isEqualTo(5);
    Assertions.assertThat(result2.get(0).end())
        .describedAs("End should be clipped to 9")
        .isEqualTo(9);
  }

  /**
   * Partial/corrupt data at the model layer: a range whose endpoint index has no
   * matching host mapping is silently dropped by {@link BlobLayout#addRange}. The
   * covered bytes must therefore still appear as a gap so the reader retries them,
   * rather than being treated as cached-but-hostless.
   */
  @Test
  public void testRangeWithoutHostMappingIsSkippedAndRemainsGap() {
    BlobLayout layout = new BlobLayout(100);

    Map<Integer, String> hostMap = new HashMap<>();
    hostMap.put(0, "host-a"); // endpoint 1 intentionally unmapped

    // 0-9 maps to host-a (kept); 10-19 maps to endpoint 1 (no host -> dropped).
    List<BlobLayoutResponse.Range> input = List.of(
        new BlobLayoutResponse.Range(0, 9, 0),
        new BlobLayoutResponse.Range(10, 19, 1));
    layout.addRange(input, hostMap);

    Assertions.assertThat(layout.getRangeMapSize())
        .describedAs("Only the range with a resolvable host must be cached")
        .isEqualTo(1);

    List<BlobRange> ranges = layout.getRanges(0, 19);
    Assertions.assertThat(ranges)
        .describedAs("Only the hosted range is returned")
        .hasSize(1);
    Assertions.assertThat(ranges.get(0).host()).isEqualTo("host-a");

    List<BlobRange> gaps = layout.getGaps(0, 19);
    Assertions.assertThat(gaps)
        .describedAs("The dropped range's bytes must remain a gap to retry")
        .hasSize(1);
    Assertions.assertThat(gaps.get(0).start()).isEqualTo(10);
    Assertions.assertThat(gaps.get(0).end()).isEqualTo(19);
  }

  /**
   * Partial coverage: when only a prefix of the requested window is cached, the
   * uncovered suffix (clipped to content length) must be reported as a gap.
   */
  @Test
  public void testGapsReportedForPartiallyCoveredWindow() {
    BlobLayout layout = new BlobLayout(50);

    Map<Integer, String> hostMap = new HashMap<>();
    hostMap.put(0, "host-a");
    layout.addRange(
        List.of(new BlobLayoutResponse.Range(0, 19, 0)), hostMap);

    // Request 0-49 but only 0-19 is cached.
    List<BlobRange> gaps = layout.getGaps(0, 49);
    Assertions.assertThat(gaps)
        .describedAs("Single trailing gap expected for uncovered suffix")
        .hasSize(1);
    Assertions.assertThat(gaps.get(0).start()).isEqualTo(20);
    Assertions.assertThat(gaps.get(0).end()).isEqualTo(49);
    Assertions.assertThat(gaps.get(0).host())
        .describedAs("Gaps carry a null host")
        .isNull();
  }

  /**
   * Gaps must be clipped to the blob content length: requesting past EOF must not
   * produce a gap beyond {@code contentLength - 1}.
   */
  @Test
  public void testGapsClippedToContentLength() {
    BlobLayout layout = new BlobLayout(30);

    Map<Integer, String> hostMap = new HashMap<>();
    hostMap.put(0, "host-a");
    layout.addRange(
        List.of(new BlobLayoutResponse.Range(0, 9, 0)), hostMap);

    List<BlobRange> gaps = layout.getGaps(0, 1000);
    Assertions.assertThat(gaps).hasSize(1);
    Assertions.assertThat(gaps.get(0).start()).isEqualTo(10);
    Assertions.assertThat(gaps.get(0).end())
        .describedAs("Gap end must be clipped to contentLength - 1")
        .isEqualTo(29);

    // A request entirely at/after EOF yields no gaps.
    Assertions.assertThat(layout.getGaps(30, 100))
        .describedAs("No gap for a window starting at/after EOF")
        .isEmpty();
  }

  /**
   * An empty layout (nothing cached) must report the entire requested window (up to
   * content length) as a single gap.
   */
  @Test
  public void testEmptyLayoutReportsWholeWindowAsGap() {
    BlobLayout layout = new BlobLayout(100);

    List<BlobRange> gaps = layout.getGaps(0, 99);
    Assertions.assertThat(gaps).hasSize(1);
    Assertions.assertThat(gaps.get(0).start()).isEqualTo(0);
    Assertions.assertThat(gaps.get(0).end()).isEqualTo(99);

    Assertions.assertThat(layout.getRanges(0, 99))
        .describedAs("No cached ranges in an empty layout")
        .isEmpty();
  }

  /**
   * Helper method to create a {@link BlobLayout} instance pre-populated with a specific set of
   * overlapping ranges and a dummy host map. The ranges are designed to test merging and clipping logic.
   *
   * @return a BlobLayout instance with predefined ranges and host mapping
   */
  private static @NonNull BlobLayout getBlobLayout() {
    BlobLayout layout = new BlobLayout(100);

    // Define a dummy host map
    Map<Integer, String> hostMap = new HashMap<>();
    hostMap.put(1, "host-a");

    // 1. Populate the cache with your specific overlapping scenario
    // 0-4 -> h, 0-8 -> h, 4-8 -> h, 6-10 -> h
    List<BlobLayoutResponse.Range> input = List.of(
        new BlobLayoutResponse.Range(0, 4, 1),
        new BlobLayoutResponse.Range(0, 8, 1),
        new BlobLayoutResponse.Range(4, 8, 1),
        new BlobLayoutResponse.Range(6, 10, 1)
    );

    layout.addRange(input, hostMap);
    return layout;
  }
}
