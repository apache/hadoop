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
package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

class StringRange {
  private String start = null;
  private String end = null;
  private boolean startInclusive = true;
  private boolean endInclusive = false;

  public StringRange(String start, boolean startInclusive, String end,
      boolean endInclusive) {
    this.start = start;
    this.startInclusive = startInclusive;
    this.end = end;
    this.endInclusive = endInclusive;
  }

  public String getStart() {
    return this.start;
  }

  public String getEnd() {
    return this.end;
  }

  public boolean isStartInclusive() {
    return this.startInclusive;
  }

  public boolean isEndInclusive() {
    return this.endInclusive;
  }

  @Override
  public int hashCode() {
    int hashCode = 0;
    if (this.start != null) {
      hashCode ^= this.start.hashCode();
    }

    if (this.end != null) {
      hashCode ^= this.end.hashCode();
    }
    return hashCode;
  }

  @Override
  public String toString() {
    String result = (this.startInclusive ? "[" : "(")
          + (this.start == null ? null : this.start) + ", "
          + (this.end == null ? null : this.end)
          + (this.endInclusive ? "]" : ")");
    return result;
  }

   public boolean inRange(String value) {
    boolean afterStart = true;
    if (this.start != null) {
      int startCmp = value.compareTo(this.start);
      afterStart = this.startInclusive ? startCmp >= 0 : startCmp > 0;
    }

    boolean beforeEnd = true;
    if (this.end != null) {
      int endCmp = value.compareTo(this.end);
      beforeEnd = this.endInclusive ? endCmp <= 0 : endCmp < 0;
    }

    return afterStart && beforeEnd;
  }
}

public class TestColumnRangeFilter {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final Log LOG = LogFactory.getLog(this.getClass());

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // Nothing to do.
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  @Test
  public void TestColumnRangeFilterClient() throws Exception {
    String family = "Family";
    String table = "TestColumnRangeFilterClient";
    HTable ht = TEST_UTIL.createTable(Bytes.toBytes(table),
        Bytes.toBytes(family), Integer.MAX_VALUE);

    List<String> rows = generateRandomWords(10, 8);
    long maxTimestamp = 2;
    List<String> columns = generateRandomWords(20000, 8);

    List<KeyValue> kvList = new ArrayList<KeyValue>();

    Map<StringRange, List<KeyValue>> rangeMap = new HashMap<StringRange, List<KeyValue>>();

    rangeMap.put(new StringRange(null, true, "b", false),
        new ArrayList<KeyValue>());
    rangeMap.put(new StringRange("p", true, "q", false),
        new ArrayList<KeyValue>());
    rangeMap.put(new StringRange("r", false, "s", true),
        new ArrayList<KeyValue>());
    rangeMap.put(new StringRange("z", false, null, false),
        new ArrayList<KeyValue>());
    String valueString = "ValueString";

    for (String row : rows) {
      Put p = new Put(Bytes.toBytes(row));
      p.setWriteToWAL(false);
      for (String column : columns) {
        for (long timestamp = 1; timestamp <= maxTimestamp; timestamp++) {
          KeyValue kv = KeyValueTestUtil.create(row, family, column, timestamp,
              valueString);
          p.add(kv);
          kvList.add(kv);
          for (StringRange s : rangeMap.keySet()) {
            if (s.inRange(column)) {
              rangeMap.get(s).add(kv);
            }
          }
        }
      }
      ht.put(p);
    }

    TEST_UTIL.flush();

    ColumnRangeFilter filter;
    Scan scan = new Scan();
    scan.setMaxVersions();
    for (StringRange s : rangeMap.keySet()) {
      filter = new ColumnRangeFilter(s.getStart() == null ? null
          : Bytes.toBytes(s.getStart()), s.isStartInclusive(),
          s.getEnd() == null ? null : Bytes.toBytes(s.getEnd()),
          s.isEndInclusive());
      scan.setFilter(filter);
      ResultScanner scanner = ht.getScanner(scan);
      List<KeyValue> results = new ArrayList<KeyValue>();
      LOG.info("scan column range: " + s.toString());
      long timeBeforeScan = System.currentTimeMillis();

      Result result;
      while ((result = scanner.next()) != null) {
        for (KeyValue kv : result.list()) {
          results.add(kv);
        }
      }
      long scanTime = System.currentTimeMillis() - timeBeforeScan;
      LOG.info("scan time = " + scanTime + "ms");
      LOG.info("found " + results.size() + " results");
      LOG.info("Expecting " + rangeMap.get(s).size() + " results");

      /*
      for (KeyValue kv : results) {
        LOG.info("found row " + Bytes.toString(kv.getRow()) + ", column "
            + Bytes.toString(kv.getQualifier()));
      }
      */

      assertEquals(rangeMap.get(s).size(), results.size());
    }
  }

  List<String> generateRandomWords(int numberOfWords, int maxLengthOfWords) {
    Set<String> wordSet = new HashSet<String>();
    for (int i = 0; i < numberOfWords; i++) {
      int lengthOfWords = (int) (Math.random() * maxLengthOfWords) + 1;
      char[] wordChar = new char[lengthOfWords];
      for (int j = 0; j < wordChar.length; j++) {
        wordChar[j] = (char) (Math.random() * 26 + 97);
      }
      String word = new String(wordChar);
      wordSet.add(word);
    }
    List<String> wordList = new ArrayList<String>(wordSet);
    return wordList;
  }
}
