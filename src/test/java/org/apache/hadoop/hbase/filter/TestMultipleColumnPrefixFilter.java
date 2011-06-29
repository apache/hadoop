/*
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
package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestMultipleColumnPrefixFilter {

  private final static HBaseTestingUtility TEST_UTIL = new
      HBaseTestingUtility();

  @Test
  public void testMultipleColumnPrefixFilter() throws IOException {
    String family = "Family";
    HTableDescriptor htd = new HTableDescriptor("TestMultipleColumnPrefixFilter");
    htd.addFamily(new HColumnDescriptor(family));
    // HRegionInfo info = new HRegionInfo(htd, null, null, false);
    HRegionInfo info = new HRegionInfo(htd.getName(), null, null, false);
    HRegion region = HRegion.createHRegion(info, HBaseTestingUtility.
        getTestDir(), TEST_UTIL.getConfiguration(), htd);

    List<String> rows = generateRandomWords(100, "row");
    List<String> columns = generateRandomWords(10000, "column");
    long maxTimestamp = 2;

    List<KeyValue> kvList = new ArrayList<KeyValue>();

    Map<String, List<KeyValue>> prefixMap = new HashMap<String,
        List<KeyValue>>();

    prefixMap.put("p", new ArrayList<KeyValue>());
    prefixMap.put("q", new ArrayList<KeyValue>());
    prefixMap.put("s", new ArrayList<KeyValue>());

    String valueString = "ValueString";

    for (String row: rows) {
      Put p = new Put(Bytes.toBytes(row));
      for (String column: columns) {
        for (long timestamp = 1; timestamp <= maxTimestamp; timestamp++) {
          KeyValue kv = KeyValueTestUtil.create(row, family, column, timestamp,
              valueString);
          p.add(kv);
          kvList.add(kv);
          for (String s: prefixMap.keySet()) {
            if (column.startsWith(s)) {
              prefixMap.get(s).add(kv);
            }
          }
        }
      }
      region.put(p);
    }

    MultipleColumnPrefixFilter filter;
    Scan scan = new Scan();
    scan.setMaxVersions();
    byte [][] filter_prefix = new byte [2][];
    filter_prefix[0] = new byte [] {'p'};
    filter_prefix[1] = new byte [] {'q'};
    
    filter = new MultipleColumnPrefixFilter(filter_prefix);
    scan.setFilter(filter);
    List<KeyValue> results = new ArrayList<KeyValue>();  
    InternalScanner scanner = region.getScanner(scan);
    while(scanner.next(results));
    assertEquals(prefixMap.get("p").size() + prefixMap.get("q").size(), results.size());
  }

  @Test
  public void testMultipleColumnPrefixFilterWithManyFamilies() throws IOException {
    String family1 = "Family1";
    String family2 = "Family2";
    HTableDescriptor htd = new HTableDescriptor("TestMultipleColumnPrefixFilter");
    htd.addFamily(new HColumnDescriptor(family1));
    htd.addFamily(new HColumnDescriptor(family2));
    HRegionInfo info = new HRegionInfo(htd.getName(), null, null, false);
    HRegion region = HRegion.createHRegion(info, HBaseTestingUtility.
        getTestDir(), TEST_UTIL.getConfiguration(), htd);

    List<String> rows = generateRandomWords(100, "row");
    List<String> columns = generateRandomWords(10000, "column");
    long maxTimestamp = 3;

    List<KeyValue> kvList = new ArrayList<KeyValue>();

    Map<String, List<KeyValue>> prefixMap = new HashMap<String,
        List<KeyValue>>();

    prefixMap.put("p", new ArrayList<KeyValue>());
    prefixMap.put("q", new ArrayList<KeyValue>());
    prefixMap.put("s", new ArrayList<KeyValue>());

    String valueString = "ValueString";

    for (String row: rows) {
      Put p = new Put(Bytes.toBytes(row));
      for (String column: columns) {
        for (long timestamp = 1; timestamp <= maxTimestamp; timestamp++) {
          double rand = Math.random();
          KeyValue kv;
          if (rand < 0.5) 
            kv = KeyValueTestUtil.create(row, family1, column, timestamp,
                valueString);
          else 
            kv = KeyValueTestUtil.create(row, family2, column, timestamp,
                valueString);
          p.add(kv);
          kvList.add(kv);
          for (String s: prefixMap.keySet()) {
            if (column.startsWith(s)) {
              prefixMap.get(s).add(kv);
            }
          }
        }
      }
      region.put(p);
    }

    MultipleColumnPrefixFilter filter;
    Scan scan = new Scan();
    scan.setMaxVersions();
    byte [][] filter_prefix = new byte [2][];
    filter_prefix[0] = new byte [] {'p'};
    filter_prefix[1] = new byte [] {'q'};
    
    filter = new MultipleColumnPrefixFilter(filter_prefix);
    scan.setFilter(filter);
    List<KeyValue> results = new ArrayList<KeyValue>();  
    InternalScanner scanner = region.getScanner(scan);
    while(scanner.next(results));
    assertEquals(prefixMap.get("p").size() + prefixMap.get("q").size(), results.size());
  }
  
  @Test
  public void testMultipleColumnPrefixFilterWithColumnPrefixFilter() throws IOException {
    String family = "Family";
    HTableDescriptor htd = new HTableDescriptor("TestMultipleColumnPrefixFilter");
    htd.addFamily(new HColumnDescriptor(family));
    HRegionInfo info = new HRegionInfo(htd.getName(), null, null, false);
    HRegion region = HRegion.createHRegion(info, HBaseTestingUtility.
        getTestDir(), TEST_UTIL.getConfiguration(),htd);

    List<String> rows = generateRandomWords(100, "row");
    List<String> columns = generateRandomWords(10000, "column");
    long maxTimestamp = 2;

    String valueString = "ValueString";

    for (String row: rows) {
      Put p = new Put(Bytes.toBytes(row));
      for (String column: columns) {
        for (long timestamp = 1; timestamp <= maxTimestamp; timestamp++) {
          KeyValue kv = KeyValueTestUtil.create(row, family, column, timestamp,
              valueString);
          p.add(kv);
        }
      }
      region.put(p);
    }

    MultipleColumnPrefixFilter multiplePrefixFilter;
    Scan scan1 = new Scan();
    scan1.setMaxVersions();
    byte [][] filter_prefix = new byte [1][];
    filter_prefix[0] = new byte [] {'p'};
 
    multiplePrefixFilter = new MultipleColumnPrefixFilter(filter_prefix);
    scan1.setFilter(multiplePrefixFilter);
    List<KeyValue> results1 = new ArrayList<KeyValue>();  
    InternalScanner scanner1 = region.getScanner(scan1);
    while(scanner1.next(results1));
    
    ColumnPrefixFilter singlePrefixFilter;
    Scan scan2 = new Scan();
    scan2.setMaxVersions();
    singlePrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("p"));
 
    scan2.setFilter(singlePrefixFilter);
    List<KeyValue> results2 = new ArrayList<KeyValue>();  
    InternalScanner scanner2 = region.getScanner(scan1);
    while(scanner2.next(results2));
    
    assertEquals(results1.size(), results2.size());
  }
  
  List<String> generateRandomWords(int numberOfWords, String suffix) {
    Set<String> wordSet = new HashSet<String>();
    for (int i = 0; i < numberOfWords; i++) {
      int lengthOfWords = (int) (Math.random()*2) + 1;
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

