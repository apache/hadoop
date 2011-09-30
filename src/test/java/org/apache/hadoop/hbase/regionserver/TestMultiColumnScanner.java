/*
 * Copyright 2010 The Apache Software Foundation
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests optimized scanning of multiple columns.
 */
@RunWith(Parameterized.class)
public class TestMultiColumnScanner {

  private static final Log LOG = LogFactory.getLog(TestMultiColumnScanner.class);

  private static final String TABLE_NAME = "TestMultiColumnScanner";
  static final String FAMILY = "CF";
  static final byte[] FAMILY_BYTES = Bytes.toBytes(FAMILY);
  static final int MAX_VERSIONS = 50;

  /**
   * The size of the column qualifier set used. Increasing this parameter
   * exponentially increases test time.
   */
  private static final int NUM_COLUMNS = 8;

  private static final int MAX_COLUMN_BIT_MASK = 1 << NUM_COLUMNS - 1;
  private static final int NUM_FLUSHES = 10;
  private static final int NUM_ROWS = 20;

  /** A large value of type long for use as a timestamp */
  private static final long BIG_LONG = 9111222333444555666L;

  /**
   * Timestamps to test with. Cannot use {@link Long#MAX_VALUE} here, because
   * it will be replaced by an timestamp auto-generated based on the time.
   */
  private static final long[] TIMESTAMPS = new long[] { 1, 3, 5,
      Integer.MAX_VALUE, BIG_LONG, Long.MAX_VALUE - 1 };

  /** The probability that a column is skipped in a store file. */
  private static final double COLUMN_SKIP_IN_STORE_FILE_PROB = 0.7;

  /** The probability of skipping a column in a single row */
  private static final double COLUMN_SKIP_IN_ROW_PROB = 0.1;

  /** The probability of skipping a column everywhere */
  private static final double COLUMN_SKIP_EVERYWHERE_PROB = 0.1;

  /** The probability to delete a row/column pair */
  private static final double DELETE_PROBABILITY = 0.02;

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private Compression.Algorithm comprAlgo;
  private StoreFile.BloomType bloomType;

  // Some static sanity-checking.
  static {
    assertTrue(BIG_LONG > 0.9 * Long.MAX_VALUE); // Guard against typos.

    // Ensure TIMESTAMPS are sorted.
    for (int i = 0; i < TIMESTAMPS.length - 1; ++i)
      assertTrue(TIMESTAMPS[i] < TIMESTAMPS[i + 1]);
  }

  @Parameters
  public static final Collection<Object[]> parameters() {
    List<Object[]> configurations = new ArrayList<Object[]>();
    for (Compression.Algorithm comprAlgo : HBaseTestingUtility.COMPRESSION_ALGORITHMS) {
      for (StoreFile.BloomType bloomType : StoreFile.BloomType.values()) {
        configurations.add(new Object[] { comprAlgo, bloomType });
      }
    }
    return configurations;
  }

  public TestMultiColumnScanner(Compression.Algorithm comprAlgo,
      StoreFile.BloomType bloomType) {
    this.comprAlgo = comprAlgo;
    this.bloomType = bloomType;
  }

  @Test
  public void testMultiColumnScanner() throws IOException {
    HRegion region = createRegion(TABLE_NAME, comprAlgo, bloomType);
    List<String> rows = sequentialStrings("row", NUM_ROWS);
    List<String> qualifiers = sequentialStrings("qual", NUM_COLUMNS);
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    Set<String> keySet = new HashSet<String>();

    // A map from <row>_<qualifier> to the most recent delete timestamp for
    // that column.
    Map<String, Long> lastDelTimeMap = new HashMap<String, Long>();

    Random rand = new Random(29372937L);
    Set<String> rowQualSkip = new HashSet<String>();

    // Skip some columns in some rows. We need to test scanning over a set
    // of columns when some of the columns are not there.
    for (String row : rows)
      for (String qual : qualifiers)
        if (rand.nextDouble() < COLUMN_SKIP_IN_ROW_PROB) {
          LOG.info("Skipping " + qual + " in row " + row);
          rowQualSkip.add(rowQualKey(row, qual));
        }

    // Also skip some columns in all rows.
    for (String qual : qualifiers)
      if (rand.nextDouble() < COLUMN_SKIP_EVERYWHERE_PROB) {
        LOG.info("Skipping " + qual + " in all rows");
        for (String row : rows)
          rowQualSkip.add(rowQualKey(row, qual));
      }

    for (int iFlush = 0; iFlush < NUM_FLUSHES; ++iFlush) {
      for (String qual : qualifiers) {
        // This is where we decide to include or not include this column into
        // this store file, regardless of row and timestamp.
        if (rand.nextDouble() < COLUMN_SKIP_IN_STORE_FILE_PROB)
          continue;

        byte[] qualBytes = Bytes.toBytes(qual);
        for (String row : rows) {
          Put p = new Put(Bytes.toBytes(row));
          for (long ts : TIMESTAMPS) {
            String value = createValue(row, qual, ts);
            KeyValue kv = KeyValueTestUtil.create(row, FAMILY, qual, ts,
                value);
            assertEquals(kv.getTimestamp(), ts);
            p.add(kv);
            String keyAsString = kv.toString();
            if (!keySet.contains(keyAsString)) {
              keySet.add(keyAsString);
              kvs.add(kv);
            }
          }
          region.put(p);

          Delete d = new Delete(Bytes.toBytes(row));
          boolean deletedSomething = false;
          for (long ts : TIMESTAMPS)
            if (rand.nextDouble() < DELETE_PROBABILITY) {
              d.deleteColumns(FAMILY_BYTES, qualBytes, ts);
              String rowAndQual = row + "_" + qual;
              Long whenDeleted = lastDelTimeMap.get(rowAndQual);
              lastDelTimeMap.put(rowAndQual, whenDeleted == null ? ts
                  : Math.max(ts, whenDeleted));
              deletedSomething = true;
            }
          if (deletedSomething)
            region.delete(d, null, true);
        }
      }
      region.flushcache();
    }

    Collections.sort(kvs, KeyValue.COMPARATOR);
    for (int maxVersions = 1; maxVersions <= TIMESTAMPS.length; ++maxVersions) {
      for (int columnBitMask = 1; columnBitMask <= MAX_COLUMN_BIT_MASK; ++columnBitMask) {
        Scan scan = new Scan();
        scan.setMaxVersions(maxVersions);
        Set<String> qualSet = new TreeSet<String>();
        {
          int columnMaskTmp = columnBitMask;
          for (String qual : qualifiers) {
            if ((columnMaskTmp & 1) != 0) {
              scan.addColumn(FAMILY_BYTES, Bytes.toBytes(qual));
              qualSet.add(qual);
            }
            columnMaskTmp >>= 1;
          }
          assertEquals(0, columnMaskTmp);
        }

        InternalScanner scanner = region.getScanner(scan);
        List<KeyValue> results = new ArrayList<KeyValue>();

        int kvPos = 0;
        int numResults = 0;
        String queryInfo = "columns queried: " + qualSet + " (columnBitMask="
            + columnBitMask + "), maxVersions=" + maxVersions;

        while (scanner.next(results) || results.size() > 0) {
          for (KeyValue kv : results) {
            while (kvPos < kvs.size()
                && !matchesQuery(kvs.get(kvPos), qualSet, maxVersions,
                    lastDelTimeMap)) {
              ++kvPos;
            }
            String rowQual = getRowQualStr(kv);
            String deleteInfo = "";
            Long lastDelTS = lastDelTimeMap.get(rowQual);
            if (lastDelTS != null) {
              deleteInfo = "; last timestamp when row/column " + rowQual
                  + " was deleted: " + lastDelTS;
            }
            assertTrue("Scanner returned additional key/value: " + kv + ", "
                + queryInfo + deleteInfo + ";", kvPos < kvs.size());
            assertEquals("Scanner returned wrong key/value; " + queryInfo
                + deleteInfo + ";", kvs.get(kvPos), kv);
            ++kvPos;
            ++numResults;
          }
          results.clear();
        }
        for (; kvPos < kvs.size(); ++kvPos) {
          KeyValue remainingKV = kvs.get(kvPos);
          assertFalse("Matching column not returned by scanner: "
              + remainingKV + ", " + queryInfo + ", results returned: "
              + numResults, matchesQuery(remainingKV, qualSet, maxVersions,
              lastDelTimeMap));
        }
      }
    }
    assertTrue("This test is supposed to delete at least some row/column " +
        "pairs", lastDelTimeMap.size() > 0);
    LOG.info("Number of row/col pairs deleted at least once: " +
       lastDelTimeMap.size());
    region.close();
  }

  static HRegion createRegion(String tableName,
      Compression.Algorithm comprAlgo, BloomType bloomType)
      throws IOException {
    HColumnDescriptor hcd =
      new HColumnDescriptor(FAMILY_BYTES, MAX_VERSIONS,
          comprAlgo.getName(),
          HColumnDescriptor.DEFAULT_IN_MEMORY,
          HColumnDescriptor.DEFAULT_BLOCKCACHE,
          HColumnDescriptor.DEFAULT_TTL,
          bloomType.toString());
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(hcd);
    HRegionInfo info =
        new HRegionInfo(Bytes.toBytes(tableName), null, null, false);
    HRegion region = HRegion.createHRegion(
        info, HBaseTestingUtility.getTestDir(), TEST_UTIL.getConfiguration(),
        htd);
    return region;
  }

  private static String getRowQualStr(KeyValue kv) {
    String rowStr = Bytes.toString(kv.getBuffer(), kv.getRowOffset(),
        kv.getRowLength());
    String qualStr = Bytes.toString(kv.getBuffer(), kv.getQualifierOffset(),
        kv.getQualifierLength());
    return rowStr + "_" + qualStr;
  }

  private static boolean matchesQuery(KeyValue kv, Set<String> qualSet,
      int maxVersions, Map<String, Long> lastDelTimeMap) {
    Long lastDelTS = lastDelTimeMap.get(getRowQualStr(kv));
    long ts = kv.getTimestamp();
    return qualSet.contains(qualStr(kv))
        && ts >= TIMESTAMPS[TIMESTAMPS.length - maxVersions]
        && (lastDelTS == null || ts > lastDelTS);
  }

  private static String qualStr(KeyValue kv) {
    return Bytes.toString(kv.getBuffer(), kv.getQualifierOffset(),
        kv.getQualifierLength());
  }

  private static String rowQualKey(String row, String qual) {
    return row + "_" + qual;
  }

  static String createValue(String row, String qual, long ts) {
    return "value_for_" + row + "_" + qual + "_" + ts;
  }

  private static List<String> sequentialStrings(String prefix, int n) {
    List<String> lst = new ArrayList<String>();
    for (int i = 0; i < n; ++i) {
      StringBuilder sb = new StringBuilder();
      sb.append(prefix + i);

      // Make column length depend on i.
      int iBitShifted = i;
      while (iBitShifted != 0) {
        sb.append((iBitShifted & 1) == 0 ? 'a' : 'b');
        iBitShifted >>= 1;
      }

      lst.add(sb.toString());
    }

    return lst;
  }

}
