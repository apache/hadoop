/*
 * Copyright 2011 The Apache Software Foundation
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFilePrettyPrinter;
import org.apache.hadoop.hbase.regionserver.HRegion.RegionScannerImpl;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.hadoop.hbase.regionserver.TestMultiColumnScanner.*;
import static org.junit.Assert.*;

/**
 * Test a multi-column scanner when there is a Bloom filter false-positive.
 * This is needed for the multi-column Bloom filter optimization.
 */
@RunWith(Parameterized.class)
public class TestScanWithBloomError {

  private static final Log LOG =
    LogFactory.getLog(TestScanWithBloomError.class);

  private static final String TABLE_NAME = "ScanWithBloomError";
  private static final String ROW = "theRow";
  private static final String QUALIFIER_PREFIX = "qual";
  private static final byte[] ROW_BYTES = Bytes.toBytes(ROW);
  private static NavigableSet<Integer> allColIds = new TreeSet<Integer>();
  private HRegion region;
  private StoreFile.BloomType bloomType;
  private FileSystem fs;
  private Configuration conf;

  private final static HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();

  @Parameters
  public static final Collection<Object[]> parameters() {
    List<Object[]> configurations = new ArrayList<Object[]>();
    for (StoreFile.BloomType bloomType : StoreFile.BloomType.values()) {
      configurations.add(new Object[] { bloomType });
    }
    return configurations;
  }

  public TestScanWithBloomError(StoreFile.BloomType bloomType) {
    this.bloomType = bloomType;
  }

  @Before
  public void setUp() throws IOException{
    conf = TEST_UTIL.getConfiguration();
    fs = FileSystem.get(conf);
  }

  @Test
  public void testThreeStoreFiles() throws IOException {
    region = createRegion(TABLE_NAME, Compression.Algorithm.GZ, bloomType);
    createStoreFile(new int[] {1, 2, 6});
    createStoreFile(new int[] {1, 2, 3, 7});
    createStoreFile(new int[] {1, 9});
    scanColSet(new int[]{1, 4, 6, 7}, new int[]{1, 6, 7});

    region.close();
  }

  private void scanColSet(int[] colSet, int[] expectedResultCols)
      throws IOException {
    LOG.info("Scanning column set: " + Arrays.toString(colSet));
    Scan scan = new Scan(ROW_BYTES, ROW_BYTES);
    addColumnSetToScan(scan, colSet);
    RegionScannerImpl scanner = (RegionScannerImpl) region.getScanner(scan);
    KeyValueHeap storeHeap = scanner.getStoreHeapForTesting();
    assertEquals(0, storeHeap.getHeap().size());
    StoreScanner storeScanner =
        (StoreScanner) storeHeap.getCurrentForTesting();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    List<StoreFileScanner> scanners = (List<StoreFileScanner>)
        (List) storeScanner.getAllScannersForTesting();

    // Sort scanners by their HFile's modification time.
    Collections.sort(scanners, new Comparator<StoreFileScanner>() {
      @Override
      public int compare(StoreFileScanner s1, StoreFileScanner s2) {
        Path p1 = s1.getReaderForTesting().getHFileReader().getPath();
        Path p2 = s2.getReaderForTesting().getHFileReader().getPath();
        long t1, t2;
        try {
          t1 = fs.getFileStatus(p1).getModificationTime();
          t2 = fs.getFileStatus(p2).getModificationTime();
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
        return t1 < t2 ? -1 : t1 == t2 ? 1 : 0;
      }
    });

    StoreFile.Reader lastStoreFileReader = null;
    for (StoreFileScanner sfScanner : scanners)
      lastStoreFileReader = sfScanner.getReaderForTesting();

    new HFilePrettyPrinter().run(new String[]{ "-m", "-p", "-f",
        lastStoreFileReader.getHFileReader().getPath().toString()});

    // Disable Bloom filter for the last store file. The disabled Bloom filter
    // will always return "true".
    LOG.info("Disabling Bloom filter for: "
        + lastStoreFileReader.getHFileReader().getName());
    lastStoreFileReader.disableBloomFilterForTesting();

    List<KeyValue> allResults = new ArrayList<KeyValue>();

    { // Limit the scope of results.
      List<KeyValue> results = new ArrayList<KeyValue>();
      while (scanner.next(results) || results.size() > 0) {
        allResults.addAll(results);
        results.clear();
      }
    }

    List<Integer> actualIds = new ArrayList<Integer>();
    for (KeyValue kv : allResults) {
      String qual = Bytes.toString(kv.getQualifier());
      assertTrue(qual.startsWith(QUALIFIER_PREFIX));
      actualIds.add(Integer.valueOf(qual.substring(
          QUALIFIER_PREFIX.length())));
    }
    List<Integer> expectedIds = new ArrayList<Integer>();
    for (int expectedId : expectedResultCols)
      expectedIds.add(expectedId);

    LOG.info("Column ids returned: " + actualIds + ", expected: "
        + expectedIds);
    assertEquals(expectedIds.toString(), actualIds.toString());
  }

  private void addColumnSetToScan(Scan scan, int[] colIds) {
    for (int colId : colIds)
      scan.addColumn(FAMILY_BYTES, Bytes.toBytes(qualFromId(colId)));
  }

  private String qualFromId(int colId) {
    return QUALIFIER_PREFIX + colId;
  }

  private void createStoreFile(int[] colIds)
      throws IOException {
    Put p = new Put(ROW_BYTES);
    for (int colId : colIds) {
      long ts = Long.MAX_VALUE;
      String qual = qualFromId(colId);
      allColIds.add(colId);
      KeyValue kv = KeyValueTestUtil.create(ROW, FAMILY,
          qual, ts, createValue(ROW, qual, ts));
      p.add(kv);
    }
    region.put(p);
    region.flushcache();
  }

}
