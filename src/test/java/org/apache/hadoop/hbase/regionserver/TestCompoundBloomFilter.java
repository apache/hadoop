/*
 * Copyright 2009 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.TestHFileWriterV2;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.ByteBloomFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CompoundBloomFilter;
import org.apache.hadoop.hbase.util.CompoundBloomFilterBase;
import org.apache.hadoop.hbase.util.CompoundBloomFilterWriter;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests writing Bloom filter blocks in the same part of the file as data
 * blocks.
 */
public class TestCompoundBloomFilter {

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private static final Log LOG = LogFactory.getLog(
      TestCompoundBloomFilter.class);

  private static final int NUM_TESTS = 9;
  private static final BloomType BLOOM_TYPES[] = { BloomType.ROW,
      BloomType.ROW, BloomType.ROWCOL, BloomType.ROWCOL, BloomType.ROW,
      BloomType.ROWCOL, BloomType.ROWCOL, BloomType.ROWCOL, BloomType.ROW };

  private static final int NUM_KV[];
  static {
    final int N = 10000; // Only used in initialization.
    NUM_KV = new int[] { 21870, N, N, N, N, 1000, N, 7500, 7500};
    assert NUM_KV.length == NUM_TESTS;
  }

  private static final int BLOCK_SIZES[];
  static {
    final int blkSize = 65536;
    BLOCK_SIZES = new int[] { 512, 1000, blkSize, blkSize, blkSize, 128, 300,
        blkSize, blkSize };
    assert BLOCK_SIZES.length == NUM_TESTS;
  }

  /**
   * Be careful not to specify too high a Bloom filter block size, otherwise
   * there will only be one oversized chunk and the observed false positive
   * rate will be too low.
   */
  private static final int BLOOM_BLOCK_SIZES[] = { 1000, 4096, 4096, 4096,
      8192, 128, 1024, 600, 600 };
  static { assert BLOOM_BLOCK_SIZES.length == NUM_TESTS; }

  private static final double TARGET_ERROR_RATES[] = { 0.025, 0.01, 0.015,
      0.01, 0.03, 0.01, 0.01, 0.07, 0.07 };
  static { assert TARGET_ERROR_RATES.length == NUM_TESTS; }

  /** A false positive rate that is obviously too high. */
  private static final double TOO_HIGH_ERROR_RATE;
  static {
    double m = 0;
    for (double errorRate : TARGET_ERROR_RATES)
      m = Math.max(m, errorRate);
    TOO_HIGH_ERROR_RATE = m + 0.03;
  }

  private static Configuration conf;
  private FileSystem fs;
  private BlockCache blockCache;

  /** A message of the form "in test#<number>:" to include in logging. */
  private String testIdMsg;

  private static final int GENERATION_SEED = 2319;
  private static final int EVALUATION_SEED = 135;

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();

    // This test requires the most recent HFile format (i.e. v2).
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MAX_FORMAT_VERSION);

    fs = FileSystem.get(conf);

    blockCache = StoreFile.getBlockCache(conf);
    assertNotNull(blockCache);
  }

  private List<KeyValue> createSortedKeyValues(Random rand, int n) {
    List<KeyValue> kvList = new ArrayList<KeyValue>(n);
    for (int i = 0; i < n; ++i)
      kvList.add(TestHFileWriterV2.randomKeyValue(rand));
    Collections.sort(kvList, KeyValue.COMPARATOR);
    return kvList;
  }

  @Test
  public void testCompoundBloomFilter() throws IOException {
    conf.setBoolean(BloomFilterFactory.IO_STOREFILE_BLOOM_ENABLED, true);
    for (int t = 0; t < NUM_TESTS; ++t) {
      conf.setFloat(BloomFilterFactory.IO_STOREFILE_BLOOM_ERROR_RATE,
          (float) TARGET_ERROR_RATES[t]);

      testIdMsg = "in test #" + t + ":";
      Random generationRand = new Random(GENERATION_SEED);
      List<KeyValue> kvs = createSortedKeyValues(generationRand, NUM_KV[t]);
      BloomType bt = BLOOM_TYPES[t];
      Path sfPath = writeStoreFile(t, bt, kvs);
      readStoreFile(t, bt, kvs, sfPath);
    }
  }

  /**
   * Validates the false positive ratio by computing its z-value and comparing
   * it to the provided threshold.
   *
   * @param falsePosRate experimental positive rate
   * @param nTrials the number of calls to
   *          {@link StoreFile.Reader#shouldSeek(Scan, java.util.SortedSet)}.
   * @param zValueBoundary z-value boundary, positive for an upper bound and
   *          negative for a lower bound
   * @param cbf the compound Bloom filter we are using
   * @param additionalMsg additional message to include in log output and
   *          assertion failures
   */
  private void validateFalsePosRate(double falsePosRate, int nTrials,
      double zValueBoundary, CompoundBloomFilter cbf, String additionalMsg) {
    double p = BloomFilterFactory.getErrorRate(conf);
    double zValue = (falsePosRate - p) / Math.sqrt(p * (1 - p) / nTrials);

    String assortedStatsStr = " (targetErrorRate=" + p + ", falsePosRate="
        + falsePosRate + ", nTrials=" + nTrials + ")";
    LOG.info("z-value is " + zValue + assortedStatsStr);

    boolean isUpperBound = zValueBoundary > 0;

    if (isUpperBound && zValue > zValueBoundary ||
        !isUpperBound && zValue < zValueBoundary) {
      String errorMsg = "False positive rate z-value " + zValue + " is "
          + (isUpperBound ? "higher" : "lower") + " than " + zValueBoundary
          + assortedStatsStr + ". Per-chunk stats:\n"
          + cbf.formatTestingStats();
      fail(errorMsg + additionalMsg);
    }
  }

  private void readStoreFile(int t, BloomType bt, List<KeyValue> kvs,
      Path sfPath) throws IOException {
    StoreFile sf = new StoreFile(fs, sfPath, true, conf, bt, false);
    StoreFile.Reader r = sf.createReader();
    final boolean pread = true; // does not really matter
    StoreFileScanner scanner = r.getStoreFileScanner(true, pread);

    {
      // Test for false negatives (not allowed).
      int numChecked = 0;
      for (KeyValue kv : kvs) {
        byte[] row = kv.getRow();
        boolean present = isInBloom(scanner, row, kv.getQualifier());
        assertTrue(testIdMsg + " Bloom filter false negative on row "
            + Bytes.toStringBinary(row) + " after " + numChecked
            + " successful checks", present);
        ++numChecked;
      }
    }

    // Test for false positives (some percentage allowed). We test in two modes:
    // "fake lookup" which ignores the key distribution, and production mode.
    for (boolean fakeLookupEnabled : new boolean[] { true, false }) {
      ByteBloomFilter.setFakeLookupMode(fakeLookupEnabled);
      try {
        String fakeLookupModeStr = ", fake lookup is " + (fakeLookupEnabled ?
            "enabled" : "disabled");
        CompoundBloomFilter cbf = (CompoundBloomFilter) r.getBloomFilter();
        cbf.enableTestingStats();
        int numFalsePos = 0;
        Random rand = new Random(EVALUATION_SEED);
        int nTrials = NUM_KV[t] * 10;
        for (int i = 0; i < nTrials; ++i) {
          byte[] query = TestHFileWriterV2.randomRowOrQualifier(rand);
          if (isInBloom(scanner, query, bt, rand)) {
            numFalsePos += 1;
          }
        }
        double falsePosRate = numFalsePos * 1.0 / nTrials;
        LOG.debug(String.format(testIdMsg
            + " False positives: %d out of %d (%f)",
            numFalsePos, nTrials, falsePosRate) + fakeLookupModeStr);

        // Check for obvious Bloom filter crashes.
        assertTrue("False positive is too high: " + falsePosRate + " (greater "
            + "than " + TOO_HIGH_ERROR_RATE + ")" + fakeLookupModeStr,
            falsePosRate < TOO_HIGH_ERROR_RATE);

        // Now a more precise check to see if the false positive rate is not
        // too high. The reason we use a relaxed restriction for the real-world
        // case as opposed to the "fake lookup" case is that our hash functions
        // are not completely independent.

        double maxZValue = fakeLookupEnabled ? 1.96 : 2.5;
        validateFalsePosRate(falsePosRate, nTrials, maxZValue, cbf,
            fakeLookupModeStr);

        // For checking the lower bound we need to eliminate the last chunk,
        // because it is frequently smaller and the false positive rate in it
        // is too low. This does not help if there is only one under-sized
        // chunk, though.
        int nChunks = cbf.getNumChunks();
        if (nChunks > 1) {
          numFalsePos -= cbf.getNumPositivesForTesting(nChunks - 1);
          nTrials -= cbf.getNumQueriesForTesting(nChunks - 1);
          falsePosRate = numFalsePos * 1.0 / nTrials;
          LOG.info(testIdMsg + " False positive rate without last chunk is " +
              falsePosRate + fakeLookupModeStr);
        }

        validateFalsePosRate(falsePosRate, nTrials, -2.58, cbf,
            fakeLookupModeStr);
      } finally {
        ByteBloomFilter.setFakeLookupMode(false);
      }
    }

    r.close();
  }

  private boolean isInBloom(StoreFileScanner scanner, byte[] row, BloomType bt,
      Random rand) {
    return isInBloom(scanner, row,
        TestHFileWriterV2.randomRowOrQualifier(rand));
  }

  private boolean isInBloom(StoreFileScanner scanner, byte[] row,
      byte[] qualifier) {
    Scan scan = new Scan(row, row);
    TreeSet<byte[]> columns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    columns.add(qualifier);
    return scanner.shouldSeek(scan, columns);
  }

  private Path writeStoreFile(int t, BloomType bt, List<KeyValue> kvs)
      throws IOException {
    conf.setInt(BloomFilterFactory.IO_STOREFILE_BLOOM_BLOCK_SIZE,
        BLOOM_BLOCK_SIZES[t]);
    conf.setBoolean(HFile.CACHE_BLOCKS_ON_WRITE_KEY, true);

    StoreFile.Writer w = StoreFile.createWriter(fs,
        HBaseTestingUtility.getTestDir(), BLOCK_SIZES[t], null, null, conf,
        bt, 0);

    assertTrue(w.hasBloom());
    assertTrue(w.getBloomWriter() instanceof CompoundBloomFilterWriter);
    CompoundBloomFilterWriter cbbf =
        (CompoundBloomFilterWriter) w.getBloomWriter();

    int keyCount = 0;
    KeyValue prev = null;
    LOG.debug("Total keys/values to insert: " + kvs.size());
    for (KeyValue kv : kvs) {
      w.append(kv);

      // Validate the key count in the Bloom filter.
      boolean newKey = true;
      if (prev != null) {
        newKey = !(bt == BloomType.ROW ? KeyValue.COMPARATOR.matchingRows(kv,
            prev) : KeyValue.COMPARATOR.matchingRowColumn(kv, prev));
      }
      if (newKey)
        ++keyCount;
      assertEquals(keyCount, cbbf.getKeyCount());

      prev = kv;
    }
    w.close();

    return w.getPath();
  }

  @Test
  public void testCompoundBloomSizing() {
    int bloomBlockByteSize = 4096;
    int bloomBlockBitSize = bloomBlockByteSize * 8;
    double targetErrorRate = 0.01;
    long maxKeysPerChunk = ByteBloomFilter.idealMaxKeys(bloomBlockBitSize,
        targetErrorRate);

    long bloomSize1 = bloomBlockByteSize * 8;
    long bloomSize2 = ByteBloomFilter.computeBitSize(maxKeysPerChunk,
        targetErrorRate);

    double bloomSizeRatio = (bloomSize2 * 1.0 / bloomSize1);
    assertTrue(Math.abs(bloomSizeRatio - 0.9999) < 0.0001);
  }

  @Test
  public void testCreateKey() {
    CompoundBloomFilterBase cbfb = new CompoundBloomFilterBase();
    byte[] row = "myRow".getBytes();
    byte[] qualifier = "myQualifier".getBytes();
    byte[] rowKey = cbfb.createBloomKey(row, 0, row.length,
        row, 0, 0);
    byte[] rowColKey = cbfb.createBloomKey(row, 0, row.length,
        qualifier, 0, qualifier.length);
    KeyValue rowKV = KeyValue.createKeyValueFromKey(rowKey);
    KeyValue rowColKV = KeyValue.createKeyValueFromKey(rowColKey);
    assertEquals(rowKV.getTimestamp(), rowColKV.getTimestamp());
    assertEquals(Bytes.toStringBinary(rowKV.getRow()),
        Bytes.toStringBinary(rowColKV.getRow()));
    assertEquals(0, rowKV.getQualifier().length);
  }

}
