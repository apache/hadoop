/**
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestColumnSeeking {

  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  static final Log LOG = LogFactory.getLog(TestColumnSeeking.class);

  @SuppressWarnings("unchecked")
  @Test
  public void testDuplicateVersions() throws IOException {
    String family = "Family";
    byte[] familyBytes = Bytes.toBytes("Family");
    String table = "TestDuplicateVersions";

    HColumnDescriptor hcd =
        new HColumnDescriptor(familyBytes, 1000,
            HColumnDescriptor.DEFAULT_COMPRESSION,
            HColumnDescriptor.DEFAULT_IN_MEMORY,
            HColumnDescriptor.DEFAULT_BLOCKCACHE,
            HColumnDescriptor.DEFAULT_TTL,
            HColumnDescriptor.DEFAULT_BLOOMFILTER);
    HTableDescriptor htd = new HTableDescriptor(table);
    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(Bytes.toBytes(table), null, null, false);
    HRegion region =
        HRegion.createHRegion(info, TEST_UTIL.getDataTestDir(), TEST_UTIL
            .getConfiguration(), htd);

    List<String> rows = generateRandomWords(10, "row");
    List<String> allColumns = generateRandomWords(10, "column");
    List<String> values = generateRandomWords(100, "value");

    long maxTimestamp = 2;
    double selectPercent = 0.5;
    int numberOfTests = 5;
    double flushPercentage = 0.2;
    double minorPercentage = 0.2;
    double majorPercentage = 0.2;
    double putPercentage = 0.2;

    HashMap<String, KeyValue> allKVMap = new HashMap<String, KeyValue>();

    HashMap<String, KeyValue>[] kvMaps = new HashMap[numberOfTests];
    ArrayList<String>[] columnLists = new ArrayList[numberOfTests];

    for (int i = 0; i < numberOfTests; i++) {
      kvMaps[i] = new HashMap<String, KeyValue>();
      columnLists[i] = new ArrayList<String>();
      for (String column : allColumns) {
        if (Math.random() < selectPercent) {
          columnLists[i].add(column);
        }
      }
    }

    for (String value : values) {
      for (String row : rows) {
        Put p = new Put(Bytes.toBytes(row));
        for (String column : allColumns) {
          for (long timestamp = 1; timestamp <= maxTimestamp; timestamp++) {
            KeyValue kv =
                KeyValueTestUtil.create(row, family, column, timestamp, value);
            if (Math.random() < putPercentage) {
              p.add(kv);
              allKVMap.put(kv.getKeyString(), kv);
              for (int i = 0; i < numberOfTests; i++) {
                if (columnLists[i].contains(column)) {
                  kvMaps[i].put(kv.getKeyString(), kv);
                }
              }
            }
          }
        }
        region.put(p);
        if (Math.random() < flushPercentage) {
          LOG.info("Flushing... ");
          region.flushcache();
        }

        if (Math.random() < minorPercentage) {
          LOG.info("Minor compacting... ");
          region.compactStores(false);
        }

        if (Math.random() < majorPercentage) {
          LOG.info("Major compacting... ");
          region.compactStores(true);
        }
      }
    }

    for (int i = 0; i < numberOfTests + 1; i++) {
      Collection<KeyValue> kvSet;
      Scan scan = new Scan();
      scan.setMaxVersions();
      if (i < numberOfTests) {
        kvSet = kvMaps[i].values();
        for (String column : columnLists[i]) {
          scan.addColumn(familyBytes, Bytes.toBytes(column));
        }
        LOG.info("ExplicitColumns scanner");
        LOG.info("Columns: " + columnLists[i].size() + "  Keys: "
            + kvSet.size());
      } else {
        kvSet = allKVMap.values();
        LOG.info("Wildcard scanner");
        LOG.info("Columns: " + allColumns.size() + "  Keys: " + kvSet.size());

      }
      InternalScanner scanner = region.getScanner(scan);
      List<KeyValue> results = new ArrayList<KeyValue>();
      while (scanner.next(results))
        ;
      assertEquals(kvSet.size(), results.size());
      assertTrue(results.containsAll(kvSet));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReseeking() throws IOException {
    String family = "Family";
    byte[] familyBytes = Bytes.toBytes("Family");
    String table = "TestSingleVersions";

    HTableDescriptor htd = new HTableDescriptor(table);
    htd.addFamily(new HColumnDescriptor(family));

    HRegionInfo info = new HRegionInfo(Bytes.toBytes(table), null, null, false);
    HRegion region =
        HRegion.createHRegion(info, TEST_UTIL.getDataTestDir(), TEST_UTIL
            .getConfiguration(), htd);

    List<String> rows = generateRandomWords(10, "row");
    List<String> allColumns = generateRandomWords(100, "column");

    long maxTimestamp = 2;
    double selectPercent = 0.5;
    int numberOfTests = 5;
    double flushPercentage = 0.2;
    double minorPercentage = 0.2;
    double majorPercentage = 0.2;
    double putPercentage = 0.2;

    HashMap<String, KeyValue> allKVMap = new HashMap<String, KeyValue>();

    HashMap<String, KeyValue>[] kvMaps = new HashMap[numberOfTests];
    ArrayList<String>[] columnLists = new ArrayList[numberOfTests];
    String valueString = "Value";

    for (int i = 0; i < numberOfTests; i++) {
      kvMaps[i] = new HashMap<String, KeyValue>();
      columnLists[i] = new ArrayList<String>();
      for (String column : allColumns) {
        if (Math.random() < selectPercent) {
          columnLists[i].add(column);
        }
      }
    }

    for (String row : rows) {
      Put p = new Put(Bytes.toBytes(row));
      for (String column : allColumns) {
        for (long timestamp = 1; timestamp <= maxTimestamp; timestamp++) {
          KeyValue kv =
              KeyValueTestUtil.create(row, family, column, timestamp,
                  valueString);
          if (Math.random() < putPercentage) {
            p.add(kv);
            allKVMap.put(kv.getKeyString(), kv);
            for (int i = 0; i < numberOfTests; i++) {
              if (columnLists[i].contains(column)) {
                kvMaps[i].put(kv.getKeyString(), kv);
              }
            }
          }

        }
      }
      region.put(p);
      if (Math.random() < flushPercentage) {
        LOG.info("Flushing... ");
        region.flushcache();
      }

      if (Math.random() < minorPercentage) {
        LOG.info("Minor compacting... ");
        region.compactStores(false);
      }

      if (Math.random() < majorPercentage) {
        LOG.info("Major compacting... ");
        region.compactStores(true);
      }
    }

    for (int i = 0; i < numberOfTests + 1; i++) {
      Collection<KeyValue> kvSet;
      Scan scan = new Scan();
      scan.setMaxVersions();
      if (i < numberOfTests) {
        kvSet = kvMaps[i].values();
        for (String column : columnLists[i]) {
          scan.addColumn(familyBytes, Bytes.toBytes(column));
        }
        LOG.info("ExplicitColumns scanner");
        LOG.info("Columns: " + columnLists[i].size() + "  Keys: "
            + kvSet.size());
      } else {
        kvSet = allKVMap.values();
        LOG.info("Wildcard scanner");
        LOG.info("Columns: " + allColumns.size() + "  Keys: " + kvSet.size());

      }
      InternalScanner scanner = region.getScanner(scan);
      List<KeyValue> results = new ArrayList<KeyValue>();
      while (scanner.next(results))
        ;
      assertEquals(kvSet.size(), results.size());
      assertTrue(results.containsAll(kvSet));
    }
  }

  List<String> generateRandomWords(int numberOfWords, String suffix) {
    Set<String> wordSet = new HashSet<String>();
    for (int i = 0; i < numberOfWords; i++) {
      int lengthOfWords = (int) (Math.random() * 5) + 1;
      char[] wordChar = new char[lengthOfWords];
      for (int j = 0; j < wordChar.length; j++) {
        wordChar[j] = (char) (Math.random() * 26 + 97);
      }
      String word;
      if (suffix == null) {
        word = new String(wordChar);
      } else {
        word = new String(wordChar) + suffix;
      }
      wordSet.add(word);
    }
    List<String> wordList = new ArrayList<String>(wordSet);
    return wordList;
  }
}
