/**
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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Run tests that use the HBase clients; {@link HTable} and {@link HTablePool}.
 * Sets up the HBase mini cluster once at start and runs through all client tests.
 * Each creates a table named for the method and does its stuff against that.
 */
public class TestFromClientSide {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
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

  /**
   * HBASE-2468 use case 1 and 2: region info de/serialization
   */
   @Test
   public void testRegionCacheDeSerialization() throws Exception {
     // 1. test serialization.
     LOG.info("Starting testRegionCacheDeSerialization");
     final byte[] TABLENAME = Bytes.toBytes("testCachePrewarm2");
     final byte[] FAMILY = Bytes.toBytes("family");
     Configuration conf = TEST_UTIL.getConfiguration();
     TEST_UTIL.createTable(TABLENAME, FAMILY);

     // Set up test table:
     // Create table:
     HTable table = new HTable(conf, TABLENAME);

     // Create multiple regions for this table
     TEST_UTIL.createMultiRegions(table, FAMILY);
     Scan s = new Scan();
     ResultScanner scanner = table.getScanner(s);
     while (scanner.next() != null) continue;

     Path tempPath = new Path(HBaseTestingUtility.getTestDir(), "regions.dat");

     final String tempFileName = tempPath.toString();

     FileOutputStream fos = new FileOutputStream(tempFileName);
     DataOutputStream dos = new DataOutputStream(fos);

     // serialize the region info and output to a local file.
     table.serializeRegionInfo(dos);
     dos.flush();
     dos.close();

     // read a local file and deserialize the region info from it.
     FileInputStream fis = new FileInputStream(tempFileName);
     DataInputStream dis = new DataInputStream(fis);

     Map<HRegionInfo, HServerAddress> deserRegions =
       table.deserializeRegionInfo(dis);
     dis.close();

     // regions obtained from meta scanner.
     Map<HRegionInfo, HServerAddress> loadedRegions =
       table.getRegionsInfo();

     // set the deserialized regions to the global cache.
     table.getConnection().clearRegionCache();

     table.getConnection().prewarmRegionCache(table.getTableName(),
         deserRegions);

     // verify whether the 2 maps are identical or not.
     assertEquals("Number of cached region is incorrect",
         HConnectionManager.getCachedRegionCount(conf, TABLENAME),
         loadedRegions.size());

     // verify each region is prefetched or not.
     for (Map.Entry<HRegionInfo, HServerAddress> e: loadedRegions.entrySet()) {
       HRegionInfo hri = e.getKey();
       assertTrue(HConnectionManager.isRegionCached(conf,
           hri.getTableDesc().getName(), hri.getStartKey()));
     }

     // delete the temp file
     File f = new java.io.File(tempFileName);
     f.delete();
     LOG.info("Finishing testRegionCacheDeSerialization");
   }

  /**
   * HBASE-2468 use case 3:
   */
  @Test
  public void testRegionCachePreWarm() throws Exception {
    LOG.info("Starting testRegionCachePreWarm");
    final byte [] TABLENAME = Bytes.toBytes("testCachePrewarm");
    Configuration conf = TEST_UTIL.getConfiguration();

    // Set up test table:
    // Create table:
    TEST_UTIL.createTable(TABLENAME, FAMILY);

    // disable region cache for the table.
    HTable.setRegionCachePrefetch(conf, TABLENAME, false);
    assertFalse("The table is disabled for region cache prefetch",
        HTable.getRegionCachePrefetch(conf, TABLENAME));

    HTable table = new HTable(conf, TABLENAME);

    // create many regions for the table.
    TEST_UTIL.createMultiRegions(table, FAMILY);
    // This count effectively waits until the regions have been
    // fully assigned
    TEST_UTIL.countRows(table);
    table.getConnection().clearRegionCache();
    assertEquals("Clearing cache should have 0 cached ", 0,
        HConnectionManager.getCachedRegionCount(conf, TABLENAME));

    // A Get is suppose to do a region lookup request
    Get g = new Get(Bytes.toBytes("aaa"));
    table.get(g);

    // only one region should be cached if the cache prefetch is disabled.
    assertEquals("Number of cached region is incorrect ", 1,
        HConnectionManager.getCachedRegionCount(conf, TABLENAME));

    // now we enable cached prefetch.
    HTable.setRegionCachePrefetch(conf, TABLENAME, true);
    assertTrue("The table is enabled for region cache prefetch",
        HTable.getRegionCachePrefetch(conf, TABLENAME));

    HTable.setRegionCachePrefetch(conf, TABLENAME, false);
    assertFalse("The table is disabled for region cache prefetch",
        HTable.getRegionCachePrefetch(conf, TABLENAME));

    HTable.setRegionCachePrefetch(conf, TABLENAME, true);
    assertTrue("The table is enabled for region cache prefetch",
        HTable.getRegionCachePrefetch(conf, TABLENAME));

    table.getConnection().clearRegionCache();

    assertEquals("Number of cached region is incorrect ", 0,
        HConnectionManager.getCachedRegionCount(conf, TABLENAME));

    // if there is a cache miss, some additional regions should be prefetched.
    Get g2 = new Get(Bytes.toBytes("bbb"));
    table.get(g2);

    // Get the configured number of cache read-ahead regions.
    int prefetchRegionNumber = conf.getInt("hbase.client.prefetch.limit", 10);

    // the total number of cached regions == region('aaa") + prefeched regions.
    LOG.info("Testing how many regions cached");
    assertEquals("Number of cached region is incorrect ", prefetchRegionNumber,
        HConnectionManager.getCachedRegionCount(conf, TABLENAME));

    table.getConnection().clearRegionCache();

    Get g3 = new Get(Bytes.toBytes("abc"));
    table.get(g3);
    assertEquals("Number of cached region is incorrect ", prefetchRegionNumber,
        HConnectionManager.getCachedRegionCount(conf, TABLENAME));

    LOG.info("Finishing testRegionCachePreWarm");
  }


  /**
   * Verifies that getConfiguration returns the same Configuration object used
   * to create the HTable instance.
   */
  @Test
  public void testGetConfiguration() throws Exception {
    byte[] TABLE = Bytes.toBytes("testGetConfiguration");
    byte[][] FAMILIES = new byte[][] { Bytes.toBytes("foo") };
    Configuration conf = TEST_UTIL.getConfiguration();
    HTable table = TEST_UTIL.createTable(TABLE, FAMILIES, conf);
    assertSame(conf, table.getConfiguration());
  }

  /**
   * Test from client side of an involved filter against a multi family that
   * involves deletes.
   *
   * @throws Exception
   */
  @Test
  public void testWeirdCacheBehaviour() throws Exception {
    byte [] TABLE = Bytes.toBytes("testWeirdCacheBehaviour");
    byte [][] FAMILIES = new byte[][] { Bytes.toBytes("trans-blob"),
        Bytes.toBytes("trans-type"), Bytes.toBytes("trans-date"),
        Bytes.toBytes("trans-tags"), Bytes.toBytes("trans-group") };
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    String value = "this is the value";
    String value2 = "this is some other value";
    String keyPrefix1 = UUID.randomUUID().toString();
    String keyPrefix2 = UUID.randomUUID().toString();
    String keyPrefix3 = UUID.randomUUID().toString();
    putRows(ht, 3, value, keyPrefix1);
    putRows(ht, 3, value, keyPrefix2);
    putRows(ht, 3, value, keyPrefix3);
    ht.flushCommits();
    putRows(ht, 3, value2, keyPrefix1);
    putRows(ht, 3, value2, keyPrefix2);
    putRows(ht, 3, value2, keyPrefix3);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), TABLE);
    System.out.println("Checking values for key: " + keyPrefix1);
    assertEquals("Got back incorrect number of rows from scan", 3,
        getNumberOfRows(keyPrefix1, value2, table));
    System.out.println("Checking values for key: " + keyPrefix2);
    assertEquals("Got back incorrect number of rows from scan", 3,
        getNumberOfRows(keyPrefix2, value2, table));
    System.out.println("Checking values for key: " + keyPrefix3);
    assertEquals("Got back incorrect number of rows from scan", 3,
        getNumberOfRows(keyPrefix3, value2, table));
    deleteColumns(ht, value2, keyPrefix1);
    deleteColumns(ht, value2, keyPrefix2);
    deleteColumns(ht, value2, keyPrefix3);
    System.out.println("Starting important checks.....");
    assertEquals("Got back incorrect number of rows from scan: " + keyPrefix1,
      0, getNumberOfRows(keyPrefix1, value2, table));
    assertEquals("Got back incorrect number of rows from scan: " + keyPrefix2,
      0, getNumberOfRows(keyPrefix2, value2, table));
    assertEquals("Got back incorrect number of rows from scan: " + keyPrefix3,
      0, getNumberOfRows(keyPrefix3, value2, table));
    ht.setScannerCaching(0);
    assertEquals("Got back incorrect number of rows from scan", 0,
      getNumberOfRows(keyPrefix1, value2, table)); ht.setScannerCaching(100);
    assertEquals("Got back incorrect number of rows from scan", 0,
      getNumberOfRows(keyPrefix2, value2, table));
  }

  private void deleteColumns(HTable ht, String value, String keyPrefix)
  throws IOException {
    ResultScanner scanner = buildScanner(keyPrefix, value, ht);
    Iterator<Result> it = scanner.iterator();
    int count = 0;
    while (it.hasNext()) {
      Result result = it.next();
      Delete delete = new Delete(result.getRow());
      delete.deleteColumn(Bytes.toBytes("trans-tags"), Bytes.toBytes("qual2"));
      ht.delete(delete);
      count++;
    }
    assertEquals("Did not perform correct number of deletes", 3, count);
  }

  private int getNumberOfRows(String keyPrefix, String value, HTable ht)
      throws Exception {
    ResultScanner resultScanner = buildScanner(keyPrefix, value, ht);
    Iterator<Result> scanner = resultScanner.iterator();
    int numberOfResults = 0;
    while (scanner.hasNext()) {
      Result result = scanner.next();
      System.out.println("Got back key: " + Bytes.toString(result.getRow()));
      for (KeyValue kv : result.raw()) {
        System.out.println("kv=" + kv.toString() + ", "
            + Bytes.toString(kv.getValue()));
      }
      numberOfResults++;
    }
    return numberOfResults;
  }

  private ResultScanner buildScanner(String keyPrefix, String value, HTable ht)
      throws IOException {
    // OurFilterList allFilters = new OurFilterList();
    FilterList allFilters = new FilterList(/* FilterList.Operator.MUST_PASS_ALL */);
    allFilters.addFilter(new PrefixFilter(Bytes.toBytes(keyPrefix)));
    SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes
        .toBytes("trans-tags"), Bytes.toBytes("qual2"), CompareOp.EQUAL, Bytes
        .toBytes(value));
    filter.setFilterIfMissing(true);
    allFilters.addFilter(filter);

    // allFilters.addFilter(new
    // RowExcludingSingleColumnValueFilter(Bytes.toBytes("trans-tags"),
    // Bytes.toBytes("qual2"), CompareOp.EQUAL, Bytes.toBytes(value)));

    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes("trans-blob"));
    scan.addFamily(Bytes.toBytes("trans-type"));
    scan.addFamily(Bytes.toBytes("trans-date"));
    scan.addFamily(Bytes.toBytes("trans-tags"));
    scan.addFamily(Bytes.toBytes("trans-group"));
    scan.setFilter(allFilters);

    return ht.getScanner(scan);
  }

  private void putRows(HTable ht, int numRows, String value, String key)
      throws IOException {
    for (int i = 0; i < numRows; i++) {
      String row = key + "_" + UUID.randomUUID().toString();
      System.out.println(String.format("Saving row: %s, with value %s", row,
          value));
      Put put = new Put(Bytes.toBytes(row));
      put.add(Bytes.toBytes("trans-blob"), null, Bytes
          .toBytes("value for blob"));
      put.add(Bytes.toBytes("trans-type"), null, Bytes.toBytes("statement"));
      put.add(Bytes.toBytes("trans-date"), null, Bytes
          .toBytes("20090921010101999"));
      put.add(Bytes.toBytes("trans-tags"), Bytes.toBytes("qual2"), Bytes
          .toBytes(value));
      put.add(Bytes.toBytes("trans-group"), null, Bytes
          .toBytes("adhocTransactionGroupId"));
      ht.put(put);
    }
  }

  /**
   * Test filters when multiple regions.  It does counts.  Needs eye-balling of
   * logs to ensure that we're not scanning more regions that we're supposed to.
   * Related to the TestFilterAcrossRegions over in the o.a.h.h.filter package.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testFilterAcrossMultipleRegions()
  throws IOException, InterruptedException {
    byte [] name = Bytes.toBytes("testFilterAcrossMutlipleRegions");
    HTable t = TEST_UTIL.createTable(name, FAMILY);
    int rowCount = TEST_UTIL.loadTable(t, FAMILY);
    assertRowCount(t, rowCount);
    // Split the table.  Should split on a reasonable key; 'lqj'
    Map<HRegionInfo, HServerAddress> regions  = splitTable(t);
    assertRowCount(t, rowCount);
    // Get end key of first region.
    byte [] endKey = regions.keySet().iterator().next().getEndKey();
    // Count rows with a filter that stops us before passed 'endKey'.
    // Should be count of rows in first region.
    int endKeyCount = countRows(t, createScanWithRowFilter(endKey));
    assertTrue(endKeyCount < rowCount);

    // How do I know I did not got to second region?  Thats tough.  Can't really
    // do that in client-side region test.  I verified by tracing in debugger.
    // I changed the messages that come out when set to DEBUG so should see
    // when scanner is done. Says "Finished with scanning..." with region name.
    // Check that its finished in right region.

    // New test.  Make it so scan goes into next region by one and then two.
    // Make sure count comes out right.
    byte [] key = new byte [] {endKey[0], endKey[1], (byte)(endKey[2] + 1)};
    int plusOneCount = countRows(t, createScanWithRowFilter(key));
    assertEquals(endKeyCount + 1, plusOneCount);
    key = new byte [] {endKey[0], endKey[1], (byte)(endKey[2] + 2)};
    int plusTwoCount = countRows(t, createScanWithRowFilter(key));
    assertEquals(endKeyCount + 2, plusTwoCount);

    // New test.  Make it so I scan one less than endkey.
    key = new byte [] {endKey[0], endKey[1], (byte)(endKey[2] - 1)};
    int minusOneCount = countRows(t, createScanWithRowFilter(key));
    assertEquals(endKeyCount - 1, minusOneCount);
    // For above test... study logs.  Make sure we do "Finished with scanning.."
    // in first region and that we do not fall into the next region.

    key = new byte [] {'a', 'a', 'a'};
    int countBBB = countRows(t,
      createScanWithRowFilter(key, null, CompareFilter.CompareOp.EQUAL));
    assertEquals(1, countBBB);

    int countGreater = countRows(t, createScanWithRowFilter(endKey, null,
      CompareFilter.CompareOp.GREATER_OR_EQUAL));
    // Because started at start of table.
    assertEquals(0, countGreater);
    countGreater = countRows(t, createScanWithRowFilter(endKey, endKey,
      CompareFilter.CompareOp.GREATER_OR_EQUAL));
    assertEquals(rowCount - endKeyCount, countGreater);
  }

  /*
   * @param key
   * @return Scan with RowFilter that does LESS than passed key.
   */
  private Scan createScanWithRowFilter(final byte [] key) {
    return createScanWithRowFilter(key, null, CompareFilter.CompareOp.LESS);
  }

  /*
   * @param key
   * @param op
   * @param startRow
   * @return Scan with RowFilter that does CompareOp op on passed key.
   */
  private Scan createScanWithRowFilter(final byte [] key,
      final byte [] startRow, CompareFilter.CompareOp op) {
    // Make sure key is of some substance... non-null and > than first key.
    assertTrue(key != null && key.length > 0 &&
      Bytes.BYTES_COMPARATOR.compare(key, new byte [] {'a', 'a', 'a'}) >= 0);
    LOG.info("Key=" + Bytes.toString(key));
    Scan s = startRow == null? new Scan(): new Scan(startRow);
    Filter f = new RowFilter(op, new BinaryComparator(key));
    f = new WhileMatchFilter(f);
    s.setFilter(f);
    return s;
  }

  /*
   * @param t
   * @param s
   * @return Count of rows in table.
   * @throws IOException
   */
  private int countRows(final HTable t, final Scan s)
  throws IOException {
    // Assert all rows in table.
    ResultScanner scanner = t.getScanner(s);
    int count = 0;
    for (Result result: scanner) {
      count++;
      assertTrue(result.size() > 0);
      // LOG.info("Count=" + count + ", row=" + Bytes.toString(result.getRow()));
    }
    return count;
  }

  private void assertRowCount(final HTable t, final int expected)
  throws IOException {
    assertEquals(expected, countRows(t, new Scan()));
  }

  /*
   * Split table into multiple regions.
   * @param t Table to split.
   * @return Map of regions to servers.
   * @throws IOException
   */
  private Map<HRegionInfo, HServerAddress> splitTable(final HTable t)
  throws IOException, InterruptedException {
    // Split this table in two.
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    admin.split(t.getTableName());
    Map<HRegionInfo, HServerAddress> regions = waitOnSplit(t);
    assertTrue(regions.size() > 1);
    return regions;
  }

  /*
   * Wait on table split.  May return because we waited long enough on the split
   * and it didn't happen.  Caller should check.
   * @param t
   * @return Map of table regions; caller needs to check table actually split.
   */
  private Map<HRegionInfo, HServerAddress> waitOnSplit(final HTable t)
  throws IOException {
    Map<HRegionInfo, HServerAddress> regions = t.getRegionsInfo();
    int originalCount = regions.size();
    for (int i = 0; i < TEST_UTIL.getConfiguration().getInt("hbase.test.retries", 30); i++) {
      Thread.currentThread();
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      regions = t.getRegionsInfo();
      if (regions.size() > originalCount) break;
    }
    return regions;
  }

  @Test
  public void testSuperSimple() throws Exception {
    byte [] TABLE = Bytes.toBytes("testSuperSimple");
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
    Scan scan = new Scan();
    scan.addColumn(FAMILY, TABLE);
    ResultScanner scanner = ht.getScanner(scan);
    Result result = scanner.next();
    assertTrue("Expected null result", result == null);
    scanner.close();
    System.out.println("Done.");
  }

  @Test
  public void testMaxKeyValueSize() throws Exception {
    byte [] TABLE = Bytes.toBytes("testMaxKeyValueSize");
    Configuration conf = TEST_UTIL.getConfiguration();
    String oldMaxSize = conf.get("hbase.client.keyvalue.maxsize");
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY);
    byte[] value = new byte[4 * 1024 * 1024];
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, value);
    ht.put(put);
    try {
      conf.setInt("hbase.client.keyvalue.maxsize", 2 * 1024 * 1024);
      TABLE = Bytes.toBytes("testMaxKeyValueSize2");
      ht = TEST_UTIL.createTable(TABLE, FAMILY);
      put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, value);
      ht.put(put);
      fail("Inserting a too large KeyValue worked, should throw exception");
    } catch(Exception e) {}
    conf.set("hbase.client.keyvalue.maxsize", oldMaxSize);
  }

  @Test
  public void testFilters() throws Exception {
    byte [] TABLE = Bytes.toBytes("testFilters");
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY);
    byte [][] ROWS = makeN(ROW, 10);
    byte [][] QUALIFIERS = {
        Bytes.toBytes("col0-<d2v1>-<d3v2>"), Bytes.toBytes("col1-<d2v1>-<d3v2>"),
        Bytes.toBytes("col2-<d2v1>-<d3v2>"), Bytes.toBytes("col3-<d2v1>-<d3v2>"),
        Bytes.toBytes("col4-<d2v1>-<d3v2>"), Bytes.toBytes("col5-<d2v1>-<d3v2>"),
        Bytes.toBytes("col6-<d2v1>-<d3v2>"), Bytes.toBytes("col7-<d2v1>-<d3v2>"),
        Bytes.toBytes("col8-<d2v1>-<d3v2>"), Bytes.toBytes("col9-<d2v1>-<d3v2>")
    };
    for(int i=0;i<10;i++) {
      Put put = new Put(ROWS[i]);
      put.add(FAMILY, QUALIFIERS[i], VALUE);
      ht.put(put);
    }
    Scan scan = new Scan();
    scan.addFamily(FAMILY);
    Filter filter = new QualifierFilter(CompareOp.EQUAL,
      new RegexStringComparator("col[1-5]"));
    scan.setFilter(filter);
    ResultScanner scanner = ht.getScanner(scan);
    int expectedIndex = 1;
    for(Result result : ht.getScanner(scan)) {
      assertEquals(result.size(), 1);
      assertTrue(Bytes.equals(result.raw()[0].getRow(), ROWS[expectedIndex]));
      assertTrue(Bytes.equals(result.raw()[0].getQualifier(),
          QUALIFIERS[expectedIndex]));
      expectedIndex++;
    }
    assertEquals(expectedIndex, 6);
    scanner.close();
  }

  @Test
  public void testKeyOnlyFilter() throws Exception {
    byte [] TABLE = Bytes.toBytes("testKeyOnlyFilter");
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY);
    byte [][] ROWS = makeN(ROW, 10);
    byte [][] QUALIFIERS = {
        Bytes.toBytes("col0-<d2v1>-<d3v2>"), Bytes.toBytes("col1-<d2v1>-<d3v2>"),
        Bytes.toBytes("col2-<d2v1>-<d3v2>"), Bytes.toBytes("col3-<d2v1>-<d3v2>"),
        Bytes.toBytes("col4-<d2v1>-<d3v2>"), Bytes.toBytes("col5-<d2v1>-<d3v2>"),
        Bytes.toBytes("col6-<d2v1>-<d3v2>"), Bytes.toBytes("col7-<d2v1>-<d3v2>"),
        Bytes.toBytes("col8-<d2v1>-<d3v2>"), Bytes.toBytes("col9-<d2v1>-<d3v2>")
    };
    for(int i=0;i<10;i++) {
      Put put = new Put(ROWS[i]);
      put.add(FAMILY, QUALIFIERS[i], VALUE);
      ht.put(put);
    }
    Scan scan = new Scan();
    scan.addFamily(FAMILY);
    Filter filter = new KeyOnlyFilter(true);
    scan.setFilter(filter);
    ResultScanner scanner = ht.getScanner(scan);
    int count = 0;
    for(Result result : ht.getScanner(scan)) {
      assertEquals(result.size(), 1);
      assertEquals(result.raw()[0].getValueLength(), Bytes.SIZEOF_INT);
      assertEquals(Bytes.toInt(result.raw()[0].getValue()), VALUE.length);
      count++;
    }
    assertEquals(count, 10);
    scanner.close();
  }

  /**
   * Test simple table and non-existent row cases.
   */
  @Test
  public void testSimpleMissing() throws Exception {
    byte [] TABLE = Bytes.toBytes("testSimpleMissing");
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY);
    byte [][] ROWS = makeN(ROW, 4);

    // Try to get a row on an empty table
    Get get = new Get(ROWS[0]);
    Result result = ht.get(get);
    assertEmptyResult(result);

    get = new Get(ROWS[0]);
    get.addFamily(FAMILY);
    result = ht.get(get);
    assertEmptyResult(result);

    get = new Get(ROWS[0]);
    get.addColumn(FAMILY, QUALIFIER);
    result = ht.get(get);
    assertEmptyResult(result);

    Scan scan = new Scan();
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);


    scan = new Scan(ROWS[0]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    scan = new Scan(ROWS[0],ROWS[1]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    scan = new Scan();
    scan.addFamily(FAMILY);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    scan = new Scan();
    scan.addColumn(FAMILY, QUALIFIER);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    // Insert a row

    Put put = new Put(ROWS[2]);
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);

    // Try to get empty rows around it

    get = new Get(ROWS[1]);
    result = ht.get(get);
    assertEmptyResult(result);

    get = new Get(ROWS[0]);
    get.addFamily(FAMILY);
    result = ht.get(get);
    assertEmptyResult(result);

    get = new Get(ROWS[3]);
    get.addColumn(FAMILY, QUALIFIER);
    result = ht.get(get);
    assertEmptyResult(result);

    // Try to scan empty rows around it

    scan = new Scan(ROWS[3]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    scan = new Scan(ROWS[0],ROWS[2]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    // Make sure we can actually get the row

    get = new Get(ROWS[2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

    get = new Get(ROWS[2]);
    get.addFamily(FAMILY);
    result = ht.get(get);
    assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

    get = new Get(ROWS[2]);
    get.addColumn(FAMILY, QUALIFIER);
    result = ht.get(get);
    assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

    // Make sure we can scan the row

    scan = new Scan();
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

    scan = new Scan(ROWS[0],ROWS[3]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

    scan = new Scan(ROWS[2],ROWS[3]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);
  }

  /**
   * Test basic puts, gets, scans, and deletes for a single row
   * in a multiple family table.
   */
  @Test
  public void testSingleRowMultipleFamily() throws Exception {
    byte [] TABLE = Bytes.toBytes("testSingleRowMultipleFamily");
    byte [][] ROWS = makeN(ROW, 3);
    byte [][] FAMILIES = makeNAscii(FAMILY, 10);
    byte [][] QUALIFIERS = makeN(QUALIFIER, 10);
    byte [][] VALUES = makeN(VALUE, 10);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);

    Get get;
    Scan scan;
    Delete delete;
    Put put;
    Result result;

    ////////////////////////////////////////////////////////////////////////////
    // Insert one column to one family
    ////////////////////////////////////////////////////////////////////////////

    put = new Put(ROWS[0]);
    put.add(FAMILIES[4], QUALIFIERS[0], VALUES[0]);
    ht.put(put);

    // Get the single column
    getVerifySingleColumn(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0, VALUES, 0);

    // Scan the single column
    scanVerifySingleColumn(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0, VALUES, 0);

    // Get empty results around inserted column
    getVerifySingleEmpty(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0);

    // Scan empty results around inserted column
    scanVerifySingleEmpty(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0);

    ////////////////////////////////////////////////////////////////////////////
    // Flush memstore and run same tests from storefiles
    ////////////////////////////////////////////////////////////////////////////

    TEST_UTIL.flush();

    // Redo get and scan tests from storefile
    getVerifySingleColumn(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0, VALUES, 0);
    scanVerifySingleColumn(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0, VALUES, 0);
    getVerifySingleEmpty(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0);
    scanVerifySingleEmpty(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0);

    ////////////////////////////////////////////////////////////////////////////
    // Now, Test reading from memstore and storefiles at once
    ////////////////////////////////////////////////////////////////////////////

    // Insert multiple columns to two other families
    put = new Put(ROWS[0]);
    put.add(FAMILIES[2], QUALIFIERS[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIERS[4], VALUES[4]);
    put.add(FAMILIES[4], QUALIFIERS[4], VALUES[4]);
    put.add(FAMILIES[6], QUALIFIERS[6], VALUES[6]);
    put.add(FAMILIES[6], QUALIFIERS[7], VALUES[7]);
    put.add(FAMILIES[7], QUALIFIERS[7], VALUES[7]);
    put.add(FAMILIES[9], QUALIFIERS[0], VALUES[0]);
    ht.put(put);

    // Get multiple columns across multiple families and get empties around it
    singleRowGetTest(ht, ROWS, FAMILIES, QUALIFIERS, VALUES);

    // Scan multiple columns across multiple families and scan empties around it
    singleRowScanTest(ht, ROWS, FAMILIES, QUALIFIERS, VALUES);

    ////////////////////////////////////////////////////////////////////////////
    // Flush the table again
    ////////////////////////////////////////////////////////////////////////////

    TEST_UTIL.flush();

    // Redo tests again
    singleRowGetTest(ht, ROWS, FAMILIES, QUALIFIERS, VALUES);
    singleRowScanTest(ht, ROWS, FAMILIES, QUALIFIERS, VALUES);

    // Insert more data to memstore
    put = new Put(ROWS[0]);
    put.add(FAMILIES[6], QUALIFIERS[5], VALUES[5]);
    put.add(FAMILIES[6], QUALIFIERS[8], VALUES[8]);
    put.add(FAMILIES[6], QUALIFIERS[9], VALUES[9]);
    put.add(FAMILIES[4], QUALIFIERS[3], VALUES[3]);
    ht.put(put);

    ////////////////////////////////////////////////////////////////////////////
    // Delete a storefile column
    ////////////////////////////////////////////////////////////////////////////
    delete = new Delete(ROWS[0]);
    delete.deleteColumns(FAMILIES[6], QUALIFIERS[7]);
    ht.delete(delete);

    // Try to get deleted column
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[6], QUALIFIERS[7]);
    result = ht.get(get);
    assertEmptyResult(result);

    // Try to scan deleted column
    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[7]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    // Make sure we can still get a column before it and after it
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[6], QUALIFIERS[6]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);

    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[6], QUALIFIERS[8]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[8], VALUES[8]);

    // Make sure we can still scan a column before it and after it
    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[6]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);

    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[8]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[8], VALUES[8]);

    ////////////////////////////////////////////////////////////////////////////
    // Delete a memstore column
    ////////////////////////////////////////////////////////////////////////////
    delete = new Delete(ROWS[0]);
    delete.deleteColumns(FAMILIES[6], QUALIFIERS[8]);
    ht.delete(delete);

    // Try to get deleted column
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[6], QUALIFIERS[8]);
    result = ht.get(get);
    assertEmptyResult(result);

    // Try to scan deleted column
    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[8]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    // Make sure we can still get a column before it and after it
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[6], QUALIFIERS[6]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);

    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[6], QUALIFIERS[9]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[9], VALUES[9]);

    // Make sure we can still scan a column before it and after it
    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[6]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);

    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[9]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[9], VALUES[9]);

    ////////////////////////////////////////////////////////////////////////////
    // Delete joint storefile/memstore family
    ////////////////////////////////////////////////////////////////////////////

    delete = new Delete(ROWS[0]);
    delete.deleteFamily(FAMILIES[4]);
    ht.delete(delete);

    // Try to get storefile column in deleted family
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[4]);
    result = ht.get(get);
    assertEmptyResult(result);

    // Try to get memstore column in deleted family
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[3]);
    result = ht.get(get);
    assertEmptyResult(result);

    // Try to get deleted family
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[4]);
    result = ht.get(get);
    assertEmptyResult(result);

    // Try to scan storefile column in deleted family
    scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[4]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    // Try to scan memstore column in deleted family
    scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[3]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    // Try to scan deleted family
    scan = new Scan();
    scan.addFamily(FAMILIES[4]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    // Make sure we can still get another family
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[2], QUALIFIERS[2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[2], QUALIFIERS[2], VALUES[2]);

    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[6], QUALIFIERS[9]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[9], VALUES[9]);

    // Make sure we can still scan another family
    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[6]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);

    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[9]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[9], VALUES[9]);

    ////////////////////////////////////////////////////////////////////////////
    // Flush everything and rerun delete tests
    ////////////////////////////////////////////////////////////////////////////

    TEST_UTIL.flush();

    // Try to get storefile column in deleted family
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[4]);
    result = ht.get(get);
    assertEmptyResult(result);

    // Try to get memstore column in deleted family
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[3]);
    result = ht.get(get);
    assertEmptyResult(result);

    // Try to get deleted family
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[4]);
    result = ht.get(get);
    assertEmptyResult(result);

    // Try to scan storefile column in deleted family
    scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[4]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    // Try to scan memstore column in deleted family
    scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[3]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    // Try to scan deleted family
    scan = new Scan();
    scan.addFamily(FAMILIES[4]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    // Make sure we can still get another family
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[2], QUALIFIERS[2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[2], QUALIFIERS[2], VALUES[2]);

    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[6], QUALIFIERS[9]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[9], VALUES[9]);

    // Make sure we can still scan another family
    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[6]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);

    scan = new Scan();
    scan.addColumn(FAMILIES[6], QUALIFIERS[9]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[9], VALUES[9]);

  }

  @Test
  public void testNull() throws Exception {
    byte [] TABLE = Bytes.toBytes("testNull");

    // Null table name (should NOT work)
    try {
      TEST_UTIL.createTable(null, FAMILY);
      fail("Creating a table with null name passed, should have failed");
    } catch(Exception e) {}

    // Null family (should NOT work)
    try {
      TEST_UTIL.createTable(TABLE, (byte[])null);
      fail("Creating a table with a null family passed, should fail");
    } catch(Exception e) {}

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY);

    // Null row (should NOT work)
    try {
      Put put = new Put((byte[])null);
      put.add(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      fail("Inserting a null row worked, should throw exception");
    } catch(Exception e) {}

    // Null qualifier (should work)
    {
      Put put = new Put(ROW);
      put.add(FAMILY, null, VALUE);
      ht.put(put);

      getTestNull(ht, ROW, FAMILY, VALUE);

      scanTestNull(ht, ROW, FAMILY, VALUE);

      Delete delete = new Delete(ROW);
      delete.deleteColumns(FAMILY, null);
      ht.delete(delete);

      Get get = new Get(ROW);
      Result result = ht.get(get);
      assertEmptyResult(result);
    }

    // Use a new table
    byte [] TABLE2 = Bytes.toBytes("testNull2");
    ht = TEST_UTIL.createTable(TABLE2, FAMILY);

    // Empty qualifier, byte[0] instead of null (should work)
    try {
      Put put = new Put(ROW);
      put.add(FAMILY, HConstants.EMPTY_BYTE_ARRAY, VALUE);
      ht.put(put);

      getTestNull(ht, ROW, FAMILY, VALUE);

      scanTestNull(ht, ROW, FAMILY, VALUE);

      // Flush and try again

      TEST_UTIL.flush();

      getTestNull(ht, ROW, FAMILY, VALUE);

      scanTestNull(ht, ROW, FAMILY, VALUE);

      Delete delete = new Delete(ROW);
      delete.deleteColumns(FAMILY, HConstants.EMPTY_BYTE_ARRAY);
      ht.delete(delete);

      Get get = new Get(ROW);
      Result result = ht.get(get);
      assertEmptyResult(result);

    } catch(Exception e) {
      throw new IOException("Using a row with null qualifier threw exception, should ");
    }

    // Null value
    try {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, null);
      ht.put(put);

      Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      Result result = ht.get(get);
      assertSingleResult(result, ROW, FAMILY, QUALIFIER, null);

      Scan scan = new Scan();
      scan.addColumn(FAMILY, QUALIFIER);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROW, FAMILY, QUALIFIER, null);

      Delete delete = new Delete(ROW);
      delete.deleteColumns(FAMILY, QUALIFIER);
      ht.delete(delete);

      get = new Get(ROW);
      result = ht.get(get);
      assertEmptyResult(result);

    } catch(Exception e) {
      throw new IOException("Null values should be allowed, but threw exception");
    }
  }

  @Test
  public void testVersions() throws Exception {
    byte [] TABLE = Bytes.toBytes("testVersions");

    long [] STAMPS = makeStamps(20);
    byte [][] VALUES = makeNAscii(VALUE, 20);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);

    // Insert 4 versions of same column
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    // Flush and redo

    TEST_UTIL.flush();

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);


    // Add some memstore and retest

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    put.add(FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    put.add(FAMILY, QUALIFIER, STAMPS[8], VALUES[8]);
    ht.put(put);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    get = new Get(ROW);
    get.setMaxVersions();
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    scan = new Scan(ROW);
    scan.setMaxVersions();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);

    // Ensure maxVersions of table is respected

    TEST_UTIL.flush();

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);
    put.add(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);
    put.add(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);
    put.add(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);
    ht.put(put);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    // Delete a version in the memstore and a version in a storefile
    Delete delete = new Delete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[11]);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[7]);
    ht.delete(delete);

    // Test that it's gone
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

  }

  @Test
  public void testVersionLimits() throws Exception {
    byte [] TABLE = Bytes.toBytes("testVersionLimits");
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    int [] LIMITS = {1,3,5};
    long [] STAMPS = makeStamps(10);
    byte [][] VALUES = makeNAscii(VALUE, 10);
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES, LIMITS);

    // Insert limit + 1 on each family
    Put put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[0], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[5], VALUES[5]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[6], VALUES[6]);
    ht.put(put);

    // Verify we only get the right number out of each

    // Family0

    Get get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    // Family1

    get = new Get(ROW);
    get.addColumn(FAMILIES[1], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    get = new Get(ROW);
    get.addFamily(FAMILIES[1]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    scan = new Scan(ROW);
    scan.addColumn(FAMILIES[1], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[1]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Family2

    get = new Get(ROW);
    get.addColumn(FAMILIES[2], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    get = new Get(ROW);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    scan = new Scan(ROW);
    scan.addColumn(FAMILIES[2], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    // Try all families

    get = new Get(ROW);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.addColumn(FAMILIES[1], QUALIFIER);
    get.addColumn(FAMILIES[2], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.addFamily(FAMILIES[0]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    scan = new Scan(ROW);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.addColumn(FAMILIES[1], QUALIFIER);
    scan.addColumn(FAMILIES[2], QUALIFIER);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

  }

  @Test
  public void testDeletes() throws Exception {
    byte [] TABLE = Bytes.toBytes("testDeletes");

    byte [][] ROWS = makeNAscii(ROW, 6);
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);

    Put put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[0], QUALIFIER, ts[1], VALUES[1]);
    ht.put(put);

    Delete delete = new Delete(ROW);
    delete.deleteFamily(FAMILIES[0], ts[0]);
    ht.delete(delete);

    Get get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    Scan scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    // Test delete latest version
    put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]);
    put.add(FAMILIES[0], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[0], QUALIFIER, ts[3], VALUES[3]);
    put.add(FAMILIES[0], null, ts[4], VALUES[4]);
    put.add(FAMILIES[0], null, ts[2], VALUES[2]);
    put.add(FAMILIES[0], null, ts[3], VALUES[3]);
    ht.put(put);

    delete = new Delete(ROW);
    delete.deleteColumn(FAMILIES[0], QUALIFIER); // ts[4]
    ht.delete(delete);

    get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    scan = new Scan(ROW);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Test for HBASE-1847
    delete = new Delete(ROW);
    delete.deleteColumn(FAMILIES[0], null);
    ht.delete(delete);

    // Cleanup null qualifier
    delete = new Delete(ROW);
    delete.deleteColumns(FAMILIES[0], null);
    ht.delete(delete);

    // Expected client behavior might be that you can re-put deleted values
    // But alas, this is not to be.  We can't put them back in either case.

    put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]); // 1000
    put.add(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]); // 5000
    ht.put(put);


    // It used to be due to the internal implementation of Get, that
    // the Get() call would return ts[4] UNLIKE the Scan below. With
    // the switch to using Scan for Get this is no longer the case.
    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // The Scanner returns the previous values, the expected-naive-unexpected behavior

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Test deleting an entire family from one row but not the other various ways

    put = new Put(ROWS[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    put = new Put(ROWS[1]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    put = new Put(ROWS[2]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    // Assert that above went in.
    get = new Get(ROWS[2]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 4 key but received " + result.size() + ": " + result,
        result.size() == 4);

    delete = new Delete(ROWS[0]);
    delete.deleteFamily(FAMILIES[2]);
    ht.delete(delete);

    delete = new Delete(ROWS[1]);
    delete.deleteColumns(FAMILIES[1], QUALIFIER);
    ht.delete(delete);

    delete = new Delete(ROWS[2]);
    delete.deleteColumn(FAMILIES[1], QUALIFIER);
    delete.deleteColumn(FAMILIES[1], QUALIFIER);
    delete.deleteColumn(FAMILIES[2], QUALIFIER);
    ht.delete(delete);

    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER,
        new long [] {ts[0], ts[1]},
        new byte[][] {VALUES[0], VALUES[1]},
        0, 1);

    scan = new Scan(ROWS[0]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER,
        new long [] {ts[0], ts[1]},
        new byte[][] {VALUES[0], VALUES[1]},
        0, 1);

    get = new Get(ROWS[1]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);

    scan = new Scan(ROWS[1]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);

    get = new Get(ROWS[2]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertEquals(1, result.size());
    assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER,
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);

    scan = new Scan(ROWS[2]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertEquals(1, result.size());
    assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER,
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);

    // Test if we delete the family first in one row (HBASE-1541)

    delete = new Delete(ROWS[3]);
    delete.deleteFamily(FAMILIES[1]);
    ht.delete(delete);

    put = new Put(ROWS[3]);
    put.add(FAMILIES[2], QUALIFIER, VALUES[0]);
    ht.put(put);

    put = new Put(ROWS[4]);
    put.add(FAMILIES[1], QUALIFIER, VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, VALUES[2]);
    ht.put(put);

    get = new Get(ROWS[3]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 1 key but received " + result.size(),
        result.size() == 1);

    get = new Get(ROWS[4]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);

    scan = new Scan(ROWS[3]);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    ResultScanner scanner = ht.getScanner(scan);
    result = scanner.next();
    assertTrue("Expected 1 key but received " + result.size(),
        result.size() == 1);
    assertTrue(Bytes.equals(result.sorted()[0].getRow(), ROWS[3]));
    assertTrue(Bytes.equals(result.sorted()[0].getValue(), VALUES[0]));
    result = scanner.next();
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertTrue(Bytes.equals(result.sorted()[0].getRow(), ROWS[4]));
    assertTrue(Bytes.equals(result.sorted()[1].getRow(), ROWS[4]));
    assertTrue(Bytes.equals(result.sorted()[0].getValue(), VALUES[1]));
    assertTrue(Bytes.equals(result.sorted()[1].getValue(), VALUES[2]));
    scanner.close();

    // Add test of bulk deleting.
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      put = new Put(bytes);
      put.add(FAMILIES[0], QUALIFIER, bytes);
      ht.put(put);
    }
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      get = new Get(bytes);
      get.addFamily(FAMILIES[0]);
      result = ht.get(get);
      assertTrue(result.size() == 1);
    }
    ArrayList<Delete> deletes = new ArrayList<Delete>();
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      delete = new Delete(bytes);
      delete.deleteFamily(FAMILIES[0]);
      deletes.add(delete);
    }
    ht.delete(deletes);
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      get = new Get(bytes);
      get.addFamily(FAMILIES[0]);
      result = ht.get(get);
      assertTrue(result.size() == 0);
    }
  }

  /*
   * Baseline "scalability" test.
   *
   * Tests one hundred families, one million columns, one million versions
   */
  @Ignore @Test
  public void testMillions() throws Exception {

    // 100 families

    // millions of columns

    // millions of versions

  }

  @Ignore @Test
  public void testMultipleRegionsAndBatchPuts() throws Exception {
    // Two family table

    // Insert lots of rows

    // Insert to the same row with batched puts

    // Insert to multiple rows with batched puts

    // Split the table

    // Get row from first region

    // Get row from second region

    // Scan all rows

    // Insert to multiple regions with batched puts

    // Get row from first region

    // Get row from second region

    // Scan all rows


  }

  @Ignore @Test
  public void testMultipleRowMultipleFamily() throws Exception {

  }

  //
  // JIRA Testers
  //

  /**
   * HBASE-867
   *    If millions of columns in a column family, hbase scanner won't come up
   *
   *    Test will create numRows rows, each with numColsPerRow columns
   *    (1 version each), and attempt to scan them all.
   *
   *    To test at scale, up numColsPerRow to the millions
   *    (have not gotten that to work running as junit though)
   */
  @Test
  public void testJiraTest867() throws Exception {
    int numRows = 10;
    int numColsPerRow = 2000;

    byte [] TABLE = Bytes.toBytes("testJiraTest867");

    byte [][] ROWS = makeN(ROW, numRows);
    byte [][] QUALIFIERS = makeN(QUALIFIER, numColsPerRow);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY);

    // Insert rows

    for(int i=0;i<numRows;i++) {
      Put put = new Put(ROWS[i]);
      for(int j=0;j<numColsPerRow;j++) {
        put.add(FAMILY, QUALIFIERS[j], QUALIFIERS[j]);
      }
      assertTrue("Put expected to contain " + numColsPerRow + " columns but " +
          "only contains " + put.size(), put.size() == numColsPerRow);
      ht.put(put);
    }

    // Get a row
    Get get = new Get(ROWS[numRows-1]);
    Result result = ht.get(get);
    assertNumKeys(result, numColsPerRow);
    KeyValue [] keys = result.sorted();
    for(int i=0;i<result.size();i++) {
      assertKey(keys[i], ROWS[numRows-1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
    }

    // Scan the rows
    Scan scan = new Scan();
    ResultScanner scanner = ht.getScanner(scan);
    int rowCount = 0;
    while((result = scanner.next()) != null) {
      assertNumKeys(result, numColsPerRow);
      KeyValue [] kvs = result.sorted();
      for(int i=0;i<numColsPerRow;i++) {
        assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }
      rowCount++;
    }
    scanner.close();
    assertTrue("Expected to scan " + numRows + " rows but actually scanned "
        + rowCount + " rows", rowCount == numRows);

    // flush and try again

    TEST_UTIL.flush();

    // Get a row
    get = new Get(ROWS[numRows-1]);
    result = ht.get(get);
    assertNumKeys(result, numColsPerRow);
    keys = result.sorted();
    for(int i=0;i<result.size();i++) {
      assertKey(keys[i], ROWS[numRows-1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
    }

    // Scan the rows
    scan = new Scan();
    scanner = ht.getScanner(scan);
    rowCount = 0;
    while((result = scanner.next()) != null) {
      assertNumKeys(result, numColsPerRow);
      KeyValue [] kvs = result.sorted();
      for(int i=0;i<numColsPerRow;i++) {
        assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }
      rowCount++;
    }
    scanner.close();
    assertTrue("Expected to scan " + numRows + " rows but actually scanned "
        + rowCount + " rows", rowCount == numRows);

  }

  /**
   * HBASE-861
   *    get with timestamp will return a value if there is a version with an
   *    earlier timestamp
   */
  @Test
  public void testJiraTest861() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest861");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);

    // Insert three versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    ht.put(put);

    // Get the middle value
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);

    // Try to get one version before (expect fail)
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);

    // Try to get one version after (expect fail)
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);

    // Try same from storefile
    TEST_UTIL.flush();
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);

    // Insert two more versions surrounding others, into memstore
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    ht.put(put);

    // Check we can get everything we should and can't get what we shouldn't
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);

    // Try same from two storefiles
    TEST_UTIL.flush();
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);

  }

  /**
   * HBASE-33
   *    Add a HTable get/obtainScanner method that retrieves all versions of a
   *    particular column and row between two timestamps
   */
  @Test
  public void testJiraTest33() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest33");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

    // Try same from storefile
    TEST_UTIL.flush();

    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

  }

  /**
   * HBASE-1014
   *    commit(BatchUpdate) method should return timestamp
   */
  @Test
  public void testJiraTest1014() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest1014");

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);

    long manualStamp = 12345;

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, manualStamp, VALUE);
    ht.put(put);

    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, manualStamp, VALUE);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp-1);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp+1);

  }

  /**
   * HBASE-1182
   *    Scan for columns > some timestamp
   */
  @Test
  public void testJiraTest1182() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest1182");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

    // Try same from storefile
    TEST_UTIL.flush();

    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
    scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
  }

  /**
   * HBASE-52
   *    Add a means of scanning over all versions
   */
  @Test
  public void testJiraTest52() throws Exception {
    byte [] TABLE = Bytes.toBytes("testJiraTest52");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    // Try same from storefile
    TEST_UTIL.flush();

    getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
  }

  //
  // Bulk Testers
  //

  private void getVersionRangeAndVerifyGreaterThan(HTable ht, byte [] row,
      byte [] family, byte [] qualifier, long [] stamps, byte [][] values,
      int start, int end)
  throws IOException {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.setMaxVersions(Integer.MAX_VALUE);
    get.setTimeRange(stamps[start+1], Long.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, row, family, qualifier, stamps, values, start+1, end);
  }

  private void getVersionRangeAndVerify(HTable ht, byte [] row, byte [] family,
      byte [] qualifier, long [] stamps, byte [][] values, int start, int end)
  throws IOException {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.setMaxVersions(Integer.MAX_VALUE);
    get.setTimeRange(stamps[start], stamps[end]+1);
    Result result = ht.get(get);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }

  private void getAllVersionsAndVerify(HTable ht, byte [] row, byte [] family,
      byte [] qualifier, long [] stamps, byte [][] values, int start, int end)
  throws IOException {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }

  private void scanVersionRangeAndVerifyGreaterThan(HTable ht, byte [] row,
      byte [] family, byte [] qualifier, long [] stamps, byte [][] values,
      int start, int end)
  throws IOException {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.setTimeRange(stamps[start+1], Long.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertNResult(result, row, family, qualifier, stamps, values, start+1, end);
  }

  private void scanVersionRangeAndVerify(HTable ht, byte [] row, byte [] family,
      byte [] qualifier, long [] stamps, byte [][] values, int start, int end)
  throws IOException {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.setTimeRange(stamps[start], stamps[end]+1);
    Result result = getSingleScanResult(ht, scan);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }

  private void scanAllVersionsAndVerify(HTable ht, byte [] row, byte [] family,
      byte [] qualifier, long [] stamps, byte [][] values, int start, int end)
  throws IOException {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setMaxVersions(Integer.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }

  private void getVersionAndVerify(HTable ht, byte [] row, byte [] family,
      byte [] qualifier, long stamp, byte [] value)
  throws Exception {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.setTimeStamp(stamp);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertSingleResult(result, row, family, qualifier, stamp, value);
  }

  private void getVersionAndVerifyMissing(HTable ht, byte [] row, byte [] family,
      byte [] qualifier, long stamp)
  throws Exception {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.setTimeStamp(stamp);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertEmptyResult(result);
  }

  private void scanVersionAndVerify(HTable ht, byte [] row, byte [] family,
      byte [] qualifier, long stamp, byte [] value)
  throws Exception {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setTimeStamp(stamp);
    scan.setMaxVersions(Integer.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, qualifier, stamp, value);
  }

  private void scanVersionAndVerifyMissing(HTable ht, byte [] row,
      byte [] family, byte [] qualifier, long stamp)
  throws Exception {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setTimeStamp(stamp);
    scan.setMaxVersions(Integer.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertNullResult(result);
  }

  private void getTestNull(HTable ht, byte [] row, byte [] family,
      byte [] value)
  throws Exception {

    Get get = new Get(row);
    get.addColumn(family, null);
    Result result = ht.get(get);
    assertSingleResult(result, row, family, null, value);

    get = new Get(row);
    get.addColumn(family, HConstants.EMPTY_BYTE_ARRAY);
    result = ht.get(get);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    get = new Get(row);
    get.addFamily(family);
    result = ht.get(get);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    get = new Get(row);
    result = ht.get(get);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

  }

  private void scanTestNull(HTable ht, byte [] row, byte [] family,
      byte [] value)
  throws Exception {

    Scan scan = new Scan();
    scan.addColumn(family, null);
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    scan = new Scan();
    scan.addColumn(family, HConstants.EMPTY_BYTE_ARRAY);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    scan = new Scan();
    scan.addFamily(family);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    scan = new Scan();
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

  }

  private void singleRowGetTest(HTable ht, byte [][] ROWS, byte [][] FAMILIES,
      byte [][] QUALIFIERS, byte [][] VALUES)
  throws Exception {

    // Single column from memstore
    Get get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[0]);
    Result result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0]);

    // Single column from storefile
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[2], QUALIFIERS[2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[2], QUALIFIERS[2], VALUES[2]);

    // Single column from storefile, family match
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[7]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[7], QUALIFIERS[7], VALUES[7]);

    // Two columns, one from memstore one from storefile, same family,
    // wildcard match
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[4]);
    result = ht.get(get);
    assertDoubleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0],
        FAMILIES[4], QUALIFIERS[4], VALUES[4]);

    // Two columns, one from memstore one from storefile, same family,
    // explicit match
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[4]);
    result = ht.get(get);
    assertDoubleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0],
        FAMILIES[4], QUALIFIERS[4], VALUES[4]);

    // Three column, one from memstore two from storefile, different families,
    // wildcard match
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[4]);
    get.addFamily(FAMILIES[7]);
    result = ht.get(get);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] { {4, 0, 0}, {4, 4, 4}, {7, 7, 7} });

    // Multiple columns from everywhere storefile, many family, wildcard
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[2]);
    get.addFamily(FAMILIES[4]);
    get.addFamily(FAMILIES[6]);
    get.addFamily(FAMILIES[7]);
    result = ht.get(get);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] {
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}
    });

    // Multiple columns from everywhere storefile, many family, wildcard
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[2], QUALIFIERS[2]);
    get.addColumn(FAMILIES[2], QUALIFIERS[4]);
    get.addColumn(FAMILIES[4], QUALIFIERS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[4]);
    get.addColumn(FAMILIES[6], QUALIFIERS[6]);
    get.addColumn(FAMILIES[6], QUALIFIERS[7]);
    get.addColumn(FAMILIES[7], QUALIFIERS[7]);
    get.addColumn(FAMILIES[7], QUALIFIERS[8]);
    result = ht.get(get);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] {
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}
    });

    // Everything
    get = new Get(ROWS[0]);
    result = ht.get(get);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] {
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}, {9, 0, 0}
    });

    // Get around inserted columns

    get = new Get(ROWS[1]);
    result = ht.get(get);
    assertEmptyResult(result);

    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[3]);
    get.addColumn(FAMILIES[2], QUALIFIERS[3]);
    result = ht.get(get);
    assertEmptyResult(result);

  }

  private void singleRowScanTest(HTable ht, byte [][] ROWS, byte [][] FAMILIES,
      byte [][] QUALIFIERS, byte [][] VALUES)
  throws Exception {

    // Single column from memstore
    Scan scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[0]);
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0]);

    // Single column from storefile
    scan = new Scan();
    scan.addColumn(FAMILIES[2], QUALIFIERS[2]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[2], QUALIFIERS[2], VALUES[2]);

    // Single column from storefile, family match
    scan = new Scan();
    scan.addFamily(FAMILIES[7]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[7], QUALIFIERS[7], VALUES[7]);

    // Two columns, one from memstore one from storefile, same family,
    // wildcard match
    scan = new Scan();
    scan.addFamily(FAMILIES[4]);
    result = getSingleScanResult(ht, scan);
    assertDoubleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0],
        FAMILIES[4], QUALIFIERS[4], VALUES[4]);

    // Two columns, one from memstore one from storefile, same family,
    // explicit match
    scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[0]);
    scan.addColumn(FAMILIES[4], QUALIFIERS[4]);
    result = getSingleScanResult(ht, scan);
    assertDoubleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0],
        FAMILIES[4], QUALIFIERS[4], VALUES[4]);

    // Three column, one from memstore two from storefile, different families,
    // wildcard match
    scan = new Scan();
    scan.addFamily(FAMILIES[4]);
    scan.addFamily(FAMILIES[7]);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] { {4, 0, 0}, {4, 4, 4}, {7, 7, 7} });

    // Multiple columns from everywhere storefile, many family, wildcard
    scan = new Scan();
    scan.addFamily(FAMILIES[2]);
    scan.addFamily(FAMILIES[4]);
    scan.addFamily(FAMILIES[6]);
    scan.addFamily(FAMILIES[7]);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] {
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}
    });

    // Multiple columns from everywhere storefile, many family, wildcard
    scan = new Scan();
    scan.addColumn(FAMILIES[2], QUALIFIERS[2]);
    scan.addColumn(FAMILIES[2], QUALIFIERS[4]);
    scan.addColumn(FAMILIES[4], QUALIFIERS[0]);
    scan.addColumn(FAMILIES[4], QUALIFIERS[4]);
    scan.addColumn(FAMILIES[6], QUALIFIERS[6]);
    scan.addColumn(FAMILIES[6], QUALIFIERS[7]);
    scan.addColumn(FAMILIES[7], QUALIFIERS[7]);
    scan.addColumn(FAMILIES[7], QUALIFIERS[8]);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] {
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}
    });

    // Everything
    scan = new Scan();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] {
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}, {9, 0, 0}
    });

    // Scan around inserted columns

    scan = new Scan(ROWS[1]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[3]);
    scan.addColumn(FAMILIES[2], QUALIFIERS[3]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
  }

  /**
   * Verify a single column using gets.
   * Expects family and qualifier arrays to be valid for at least
   * the range:  idx-2 < idx < idx+2
   */
  private void getVerifySingleColumn(HTable ht,
      byte [][] ROWS, int ROWIDX,
      byte [][] FAMILIES, int FAMILYIDX,
      byte [][] QUALIFIERS, int QUALIFIERIDX,
      byte [][] VALUES, int VALUEIDX)
  throws Exception {

    Get get = new Get(ROWS[ROWIDX]);
    Result result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[FAMILYIDX]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[FAMILYIDX-2]);
    get.addFamily(FAMILIES[FAMILYIDX]);
    get.addFamily(FAMILIES[FAMILYIDX+2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    get = new Get(ROWS[ROWIDX]);
    get.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[0]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    get = new Get(ROWS[ROWIDX]);
    get.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[1]);
    get.addFamily(FAMILIES[FAMILYIDX]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[FAMILYIDX]);
    get.addColumn(FAMILIES[FAMILYIDX+1], QUALIFIERS[1]);
    get.addColumn(FAMILIES[FAMILYIDX-2], QUALIFIERS[1]);
    get.addFamily(FAMILIES[FAMILYIDX-1]);
    get.addFamily(FAMILIES[FAMILYIDX+2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

  }


  /**
   * Verify a single column using scanners.
   * Expects family and qualifier arrays to be valid for at least
   * the range:  idx-2 to idx+2
   * Expects row array to be valid for at least idx to idx+2
   */
  private void scanVerifySingleColumn(HTable ht,
      byte [][] ROWS, int ROWIDX,
      byte [][] FAMILIES, int FAMILYIDX,
      byte [][] QUALIFIERS, int QUALIFIERIDX,
      byte [][] VALUES, int VALUEIDX)
  throws Exception {

    Scan scan = new Scan();
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan(ROWS[ROWIDX]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan(ROWS[ROWIDX], ROWS[ROWIDX+1]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan(HConstants.EMPTY_START_ROW, ROWS[ROWIDX+1]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan();
    scan.addFamily(FAMILIES[FAMILYIDX]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan();
    scan.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[QUALIFIERIDX]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan();
    scan.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[QUALIFIERIDX+1]);
    scan.addFamily(FAMILIES[FAMILYIDX]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan();
    scan.addColumn(FAMILIES[FAMILYIDX-1], QUALIFIERS[QUALIFIERIDX+1]);
    scan.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[QUALIFIERIDX]);
    scan.addFamily(FAMILIES[FAMILYIDX+1]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

  }

  /**
   * Verify we do not read any values by accident around a single column
   * Same requirements as getVerifySingleColumn
   */
  private void getVerifySingleEmpty(HTable ht,
      byte [][] ROWS, int ROWIDX,
      byte [][] FAMILIES, int FAMILYIDX,
      byte [][] QUALIFIERS, int QUALIFIERIDX)
  throws Exception {

    Get get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[4]);
    get.addColumn(FAMILIES[4], QUALIFIERS[1]);
    Result result = ht.get(get);
    assertEmptyResult(result);

    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[4]);
    get.addColumn(FAMILIES[4], QUALIFIERS[2]);
    result = ht.get(get);
    assertEmptyResult(result);

    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[3]);
    get.addColumn(FAMILIES[4], QUALIFIERS[2]);
    get.addFamily(FAMILIES[5]);
    result = ht.get(get);
    assertEmptyResult(result);

    get = new Get(ROWS[ROWIDX+1]);
    result = ht.get(get);
    assertEmptyResult(result);

  }

  private void scanVerifySingleEmpty(HTable ht,
      byte [][] ROWS, int ROWIDX,
      byte [][] FAMILIES, int FAMILYIDX,
      byte [][] QUALIFIERS, int QUALIFIERIDX)
  throws Exception {

    Scan scan = new Scan(ROWS[ROWIDX+1]);
    Result result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    scan = new Scan(ROWS[ROWIDX+1],ROWS[ROWIDX+2]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    scan = new Scan(HConstants.EMPTY_START_ROW, ROWS[ROWIDX]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    scan = new Scan();
    scan.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[QUALIFIERIDX+1]);
    scan.addFamily(FAMILIES[FAMILYIDX-1]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

  }

  //
  // Verifiers
  //

  private void assertKey(KeyValue key, byte [] row, byte [] family,
      byte [] qualifier, byte [] value)
  throws Exception {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(key.getRow()) +"]",
        equals(row, key.getRow()));
    assertTrue("Expected family [" + Bytes.toString(family) + "] " +
        "Got family [" + Bytes.toString(key.getFamily()) + "]",
        equals(family, key.getFamily()));
    assertTrue("Expected qualifier [" + Bytes.toString(qualifier) + "] " +
        "Got qualifier [" + Bytes.toString(key.getQualifier()) + "]",
        equals(qualifier, key.getQualifier()));
    assertTrue("Expected value [" + Bytes.toString(value) + "] " +
        "Got value [" + Bytes.toString(key.getValue()) + "]",
        equals(value, key.getValue()));
  }

  private void assertIncrementKey(KeyValue key, byte [] row, byte [] family,
      byte [] qualifier, long value)
  throws Exception {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(key.getRow()) +"]",
        equals(row, key.getRow()));
    assertTrue("Expected family [" + Bytes.toString(family) + "] " +
        "Got family [" + Bytes.toString(key.getFamily()) + "]",
        equals(family, key.getFamily()));
    assertTrue("Expected qualifier [" + Bytes.toString(qualifier) + "] " +
        "Got qualifier [" + Bytes.toString(key.getQualifier()) + "]",
        equals(qualifier, key.getQualifier()));
    assertTrue("Expected value [" + value + "] " +
        "Got value [" + Bytes.toLong(key.getValue()) + "]",
        Bytes.toLong(key.getValue()) == value);
  }

  private void assertNumKeys(Result result, int n) throws Exception {
    assertTrue("Expected " + n + " keys but got " + result.size(),
        result.size() == n);
  }

  private void assertNResult(Result result, byte [] row,
      byte [][] families, byte [][] qualifiers, byte [][] values,
      int [][] idxs)
  throws Exception {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
        equals(row, result.getRow()));
    assertTrue("Expected " + idxs.length + " keys but result contains "
        + result.size(), result.size() == idxs.length);

    KeyValue [] keys = result.sorted();

    for(int i=0;i<keys.length;i++) {
      byte [] family = families[idxs[i][0]];
      byte [] qualifier = qualifiers[idxs[i][1]];
      byte [] value = values[idxs[i][2]];
      KeyValue key = keys[i];

      assertTrue("(" + i + ") Expected family [" + Bytes.toString(family)
          + "] " + "Got family [" + Bytes.toString(key.getFamily()) + "]",
          equals(family, key.getFamily()));
      assertTrue("(" + i + ") Expected qualifier [" + Bytes.toString(qualifier)
          + "] " + "Got qualifier [" + Bytes.toString(key.getQualifier()) + "]",
          equals(qualifier, key.getQualifier()));
      assertTrue("(" + i + ") Expected value [" + Bytes.toString(value) + "] "
          + "Got value [" + Bytes.toString(key.getValue()) + "]",
          equals(value, key.getValue()));
    }
  }

  private void assertNResult(Result result, byte [] row,
      byte [] family, byte [] qualifier, long [] stamps, byte [][] values,
      int start, int end)
  throws IOException {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
        equals(row, result.getRow()));
    int expectedResults = end - start + 1;
    assertEquals(expectedResults, result.size());

    KeyValue [] keys = result.sorted();

    for (int i=0; i<keys.length; i++) {
      byte [] value = values[end-i];
      long ts = stamps[end-i];
      KeyValue key = keys[i];

      assertTrue("(" + i + ") Expected family [" + Bytes.toString(family)
          + "] " + "Got family [" + Bytes.toString(key.getFamily()) + "]",
          equals(family, key.getFamily()));
      assertTrue("(" + i + ") Expected qualifier [" + Bytes.toString(qualifier)
          + "] " + "Got qualifier [" + Bytes.toString(key.getQualifier()) + "]",
          equals(qualifier, key.getQualifier()));
      assertTrue("Expected ts [" + ts + "] " +
          "Got ts [" + key.getTimestamp() + "]", ts == key.getTimestamp());
      assertTrue("(" + i + ") Expected value [" + Bytes.toString(value) + "] "
          + "Got value [" + Bytes.toString(key.getValue()) + "]",
          equals(value, key.getValue()));
    }
  }

  /**
   * Validate that result contains two specified keys, exactly.
   * It is assumed key A sorts before key B.
   */
  private void assertDoubleResult(Result result, byte [] row,
      byte [] familyA, byte [] qualifierA, byte [] valueA,
      byte [] familyB, byte [] qualifierB, byte [] valueB)
  throws Exception {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
        equals(row, result.getRow()));
    assertTrue("Expected two keys but result contains " + result.size(),
        result.size() == 2);
    KeyValue [] kv = result.sorted();
    KeyValue kvA = kv[0];
    assertTrue("(A) Expected family [" + Bytes.toString(familyA) + "] " +
        "Got family [" + Bytes.toString(kvA.getFamily()) + "]",
        equals(familyA, kvA.getFamily()));
    assertTrue("(A) Expected qualifier [" + Bytes.toString(qualifierA) + "] " +
        "Got qualifier [" + Bytes.toString(kvA.getQualifier()) + "]",
        equals(qualifierA, kvA.getQualifier()));
    assertTrue("(A) Expected value [" + Bytes.toString(valueA) + "] " +
        "Got value [" + Bytes.toString(kvA.getValue()) + "]",
        equals(valueA, kvA.getValue()));
    KeyValue kvB = kv[1];
    assertTrue("(B) Expected family [" + Bytes.toString(familyB) + "] " +
        "Got family [" + Bytes.toString(kvB.getFamily()) + "]",
        equals(familyB, kvB.getFamily()));
    assertTrue("(B) Expected qualifier [" + Bytes.toString(qualifierB) + "] " +
        "Got qualifier [" + Bytes.toString(kvB.getQualifier()) + "]",
        equals(qualifierB, kvB.getQualifier()));
    assertTrue("(B) Expected value [" + Bytes.toString(valueB) + "] " +
        "Got value [" + Bytes.toString(kvB.getValue()) + "]",
        equals(valueB, kvB.getValue()));
  }

  private void assertSingleResult(Result result, byte [] row, byte [] family,
      byte [] qualifier, byte [] value)
  throws Exception {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
        equals(row, result.getRow()));
    assertTrue("Expected a single key but result contains " + result.size(),
        result.size() == 1);
    KeyValue kv = result.sorted()[0];
    assertTrue("Expected family [" + Bytes.toString(family) + "] " +
        "Got family [" + Bytes.toString(kv.getFamily()) + "]",
        equals(family, kv.getFamily()));
    assertTrue("Expected qualifier [" + Bytes.toString(qualifier) + "] " +
        "Got qualifier [" + Bytes.toString(kv.getQualifier()) + "]",
        equals(qualifier, kv.getQualifier()));
    assertTrue("Expected value [" + Bytes.toString(value) + "] " +
        "Got value [" + Bytes.toString(kv.getValue()) + "]",
        equals(value, kv.getValue()));
  }

  private void assertSingleResult(Result result, byte [] row, byte [] family,
      byte [] qualifier, long ts, byte [] value)
  throws Exception {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
        equals(row, result.getRow()));
    assertTrue("Expected a single key but result contains " + result.size(),
        result.size() == 1);
    KeyValue kv = result.sorted()[0];
    assertTrue("Expected family [" + Bytes.toString(family) + "] " +
        "Got family [" + Bytes.toString(kv.getFamily()) + "]",
        equals(family, kv.getFamily()));
    assertTrue("Expected qualifier [" + Bytes.toString(qualifier) + "] " +
        "Got qualifier [" + Bytes.toString(kv.getQualifier()) + "]",
        equals(qualifier, kv.getQualifier()));
    assertTrue("Expected ts [" + ts + "] " +
        "Got ts [" + kv.getTimestamp() + "]", ts == kv.getTimestamp());
    assertTrue("Expected value [" + Bytes.toString(value) + "] " +
        "Got value [" + Bytes.toString(kv.getValue()) + "]",
        equals(value, kv.getValue()));
  }

  private void assertEmptyResult(Result result) throws Exception {
    assertTrue("expected an empty result but result contains " +
        result.size() + " keys", result.isEmpty());
  }

  private void assertNullResult(Result result) throws Exception {
    assertTrue("expected null result but received a non-null result",
        result == null);
  }

  //
  // Helpers
  //

  private Result getSingleScanResult(HTable ht, Scan scan) throws IOException {
    ResultScanner scanner = ht.getScanner(scan);
    Result result = scanner.next();
    scanner.close();
    return result;
  }

  private byte [][] makeNAscii(byte [] base, int n) {
    if(n > 256) {
      return makeNBig(base, n);
    }
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      byte [] tail = Bytes.toBytes(Integer.toString(i));
      ret[i] = Bytes.add(base, tail);
    }
    return ret;
  }

  private byte [][] makeN(byte [] base, int n) {
    if (n > 256) {
      return makeNBig(base, n);
    }
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      ret[i] = Bytes.add(base, new byte[]{(byte)i});
    }
    return ret;
  }

  private byte [][] makeNBig(byte [] base, int n) {
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      int byteA = (i % 256);
      int byteB = (i >> 8);
      ret[i] = Bytes.add(base, new byte[]{(byte)byteB,(byte)byteA});
    }
    return ret;
  }

  private long [] makeStamps(int n) {
    long [] stamps = new long[n];
    for(int i=0;i<n;i++) stamps[i] = i+1;
    return stamps;
  }

  private boolean equals(byte [] left, byte [] right) {
    if (left == null && right == null) return true;
    if (left == null && right.length == 0) return true;
    if (right == null && left.length == 0) return true;
    return Bytes.equals(left, right);
  }

  @Test
  public void testDuplicateVersions() throws Exception {
    byte [] TABLE = Bytes.toBytes("testDuplicateVersions");

    long [] STAMPS = makeStamps(20);
    byte [][] VALUES = makeNAscii(VALUE, 20);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);

    // Insert 4 versions of same column
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    // Flush and redo

    TEST_UTIL.flush();

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(2);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);


    // Add some memstore and retest

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    put.add(FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    put.add(FAMILY, QUALIFIER, STAMPS[8], VALUES[8]);
    ht.put(put);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(7);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(7);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    get = new Get(ROW);
    get.setMaxVersions(7);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    scan = new Scan(ROW);
    scan.setMaxVersions(7);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    // Verify we can get each one properly
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);

    // Verify we don't accidentally get others
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
    scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);

    // Ensure maxVersions of table is respected

    TEST_UTIL.flush();

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);
    put.add(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);
    put.add(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);
    put.add(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);
    ht.put(put);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    // Delete a version in the memstore and a version in a storefile
    Delete delete = new Delete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[11]);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[7]);
    ht.delete(delete);

    // Test that it's gone
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);
  }

  @Test
  public void testUpdates() throws Exception {

    byte [] TABLE = Bytes.toBytes("testUpdates");
    HTable hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row1");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

    // Check that the column indeed has the right values at timestamps 1 and
    // 2
    Result result = hTable.get(get);
    NavigableMap<Long, byte[]> navigableMap =
        result.getMap().get(FAMILY).get(qualifier);
    assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
    assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

    // Update the value at timestamp 1
    put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
    hTable.put(put);

    // Check that the values at timestamp 2 and 1 got updated
    result = hTable.get(get);
    navigableMap = result.getMap().get(FAMILY).get(qualifier);
    assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
    assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
  }

  @Test
  public void testUpdatesWithMajorCompaction() throws Exception {

    String tableName = "testUpdatesWithMajorCompaction";
    byte [] TABLE = Bytes.toBytes(tableName);
    HTable hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row2");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

    // Check that the column indeed has the right values at timestamps 1 and
    // 2
    Result result = hTable.get(get);
    NavigableMap<Long, byte[]> navigableMap =
        result.getMap().get(FAMILY).get(qualifier);
    assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
    assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Update the value at timestamp 1
    put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
    hTable.put(put);

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Check that the values at timestamp 2 and 1 got updated
    result = hTable.get(get);
    navigableMap = result.getMap().get(FAMILY).get(qualifier);
    assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
    assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
  }

  @Test
  public void testMajorCompactionBetweenTwoUpdates() throws Exception {

    String tableName = "testMajorCompactionBetweenTwoUpdates";
    byte [] TABLE = Bytes.toBytes(tableName);
    HTable hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row3");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

    // Check that the column indeed has the right values at timestamps 1 and
    // 2
    Result result = hTable.get(get);
    NavigableMap<Long, byte[]> navigableMap =
        result.getMap().get(FAMILY).get(qualifier);
    assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
    assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Update the value at timestamp 1
    put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
    hTable.put(put);

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Check that the values at timestamp 2 and 1 got updated
    result = hTable.get(get);
    navigableMap = result.getMap().get(FAMILY).get(qualifier);

    assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
    assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
  }

  @Test
  public void testGet_EmptyTable() throws IOException {
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testGet_EmptyTable"), FAMILY);
    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    Result r = table.get(get);
    assertTrue(r.isEmpty());
  }

  @Test
  public void testGet_NonExistentRow() throws IOException {
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testGet_NonExistentRow"), FAMILY);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);
    LOG.info("Row put");

    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    Result r = table.get(get);
    assertFalse(r.isEmpty());
    System.out.println("Row retrieved successfully");

    byte [] missingrow = Bytes.toBytes("missingrow");
    get = new Get(missingrow);
    get.addFamily(FAMILY);
    r = table.get(get);
    assertTrue(r.isEmpty());
    LOG.info("Row missing as it should be");
  }

  @Test
  public void testPut() throws IOException {
    final byte [] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte [] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte [] row1 = Bytes.toBytes("row1");
    final byte [] row2 = Bytes.toBytes("row2");
    final byte [] value = Bytes.toBytes("abcd");
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testPut"),
      new byte [][] {CONTENTS_FAMILY, SMALL_FAMILY});
    Put put = new Put(row1);
    put.add(CONTENTS_FAMILY, null, value);
    table.put(put);

    put = new Put(row2);
    put.add(CONTENTS_FAMILY, null, value);

    assertEquals(put.size(), 1);
    assertEquals(put.getFamilyMap().get(CONTENTS_FAMILY).size(), 1);

    KeyValue kv = put.getFamilyMap().get(CONTENTS_FAMILY).get(0);

    assertTrue(Bytes.equals(kv.getFamily(), CONTENTS_FAMILY));
    // will it return null or an empty byte array?
    assertTrue(Bytes.equals(kv.getQualifier(), new byte[0]));

    assertTrue(Bytes.equals(kv.getValue(), value));

    table.put(put);

    Scan scan = new Scan();
    scan.addColumn(CONTENTS_FAMILY, null);
    ResultScanner scanner = table.getScanner(scan);
    for (Result r : scanner) {
      for(KeyValue key : r.sorted()) {
        System.out.println(Bytes.toString(r.getRow()) + ": " + key.toString());
      }
    }
  }

  @Test
  public void testRowsPut() throws IOException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final int NB_BATCH_ROWS = 10;
    final byte[] value = Bytes.toBytes("abcd");
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPut"),
      new byte[][] {CONTENTS_FAMILY, SMALL_FAMILY });
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);
    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS, nbRows);
  }

  @Test
  public void testRowsPutBufferedOneFlush() throws IOException {
    final byte [] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte [] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte [] value = Bytes.toBytes("abcd");
    final int NB_BATCH_ROWS = 10;
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPutBufferedOneFlush"),
      new byte [][] {CONTENTS_FAMILY, SMALL_FAMILY});
    table.setAutoFlush(false);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS * 10; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);

    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(0, nbRows);
    scanner.close();

    table.flushCommits();

    scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    scanner = table.getScanner(scan);
    nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS * 10, nbRows);
  }

  @Test
  public void testRowsPutBufferedManyManyFlushes() throws IOException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte[] value = Bytes.toBytes("abcd");
    final int NB_BATCH_ROWS = 10;
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPutBufferedManyManyFlushes"),
      new byte[][] {CONTENTS_FAMILY, SMALL_FAMILY });
    table.setAutoFlush(false);
    table.setWriteBufferSize(10);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS * 10; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);

    table.flushCommits();

    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS * 10, nbRows);
  }

  @Test
  public void testAddKeyValue() throws IOException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] value = Bytes.toBytes("abcd");
    final byte[] row1 = Bytes.toBytes("row1");
    final byte[] row2 = Bytes.toBytes("row2");
    byte[] qualifier = Bytes.toBytes("qf1");
    Put put = new Put(row1);

    // Adding KeyValue with the same row
    KeyValue kv = new KeyValue(row1, CONTENTS_FAMILY, qualifier, value);
    boolean ok = true;
    try {
      put.add(kv);
    } catch (IOException e) {
      ok = false;
    }
    assertEquals(true, ok);

    // Adding KeyValue with the different row
    kv = new KeyValue(row2, CONTENTS_FAMILY, qualifier, value);
    ok = false;
    try {
      put.add(kv);
    } catch (IOException e) {
      ok = true;
    }
    assertEquals(true, ok);
  }

  /**
   * test for HBASE-737
   * @throws IOException
   */
  @Test
  public void testHBase737 () throws IOException {
    final byte [] FAM1 = Bytes.toBytes("fam1");
    final byte [] FAM2 = Bytes.toBytes("fam2");
    // Open table
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testHBase737"),
      new byte [][] {FAM1, FAM2});
    // Insert some values
    Put put = new Put(ROW);
    put.add(FAM1, Bytes.toBytes("letters"), Bytes.toBytes("abcdefg"));
    table.put(put);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }

    put = new Put(ROW);
    put.add(FAM1, Bytes.toBytes("numbers"), Bytes.toBytes("123456"));
    table.put(put);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }

    put = new Put(ROW);
    put.add(FAM2, Bytes.toBytes("letters"), Bytes.toBytes("hijklmnop"));
    table.put(put);

    long times[] = new long[3];

    // First scan the memstore

    Scan scan = new Scan();
    scan.addFamily(FAM1);
    scan.addFamily(FAM2);
    ResultScanner s = table.getScanner(scan);
    try {
      int index = 0;
      Result r = null;
      while ((r = s.next()) != null) {
        for(KeyValue key : r.sorted()) {
          times[index++] = key.getTimestamp();
        }
      }
    } finally {
      s.close();
    }
    for (int i = 0; i < times.length - 1; i++) {
      for (int j = i + 1; j < times.length; j++) {
        assertTrue(times[j] > times[i]);
      }
    }

    // Flush data to disk and try again
    TEST_UTIL.flush();

    // Reset times
    for(int i=0;i<times.length;i++) {
      times[i] = 0;
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }
    scan = new Scan();
    scan.addFamily(FAM1);
    scan.addFamily(FAM2);
    s = table.getScanner(scan);
    try {
      int index = 0;
      Result r = null;
      while ((r = s.next()) != null) {
        for(KeyValue key : r.sorted()) {
          times[index++] = key.getTimestamp();
        }
      }
    } finally {
      s.close();
    }
    for (int i = 0; i < times.length - 1; i++) {
      for (int j = i + 1; j < times.length; j++) {
        assertTrue(times[j] > times[i]);
      }
    }
  }

  @Test
  public void testListTables() throws IOException, InterruptedException {
    byte [] t1 = Bytes.toBytes("testListTables1");
    byte [] t2 = Bytes.toBytes("testListTables2");
    byte [] t3 = Bytes.toBytes("testListTables3");
    byte [][] tables = new byte[][] { t1, t2, t3 };
    for (int i = 0; i < tables.length; i++) {
      TEST_UTIL.createTable(tables[i], FAMILY);
    }
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    HTableDescriptor[] ts = admin.listTables();
    HashSet<HTableDescriptor> result = new HashSet<HTableDescriptor>(ts.length);
    for (int i = 0; i < ts.length; i++) {
      result.add(ts[i]);
    }
    int size = result.size();
    assertTrue(size >= tables.length);
    for (int i = 0; i < tables.length && i < size; i++) {
      boolean found = false;
      for (int j = 0; j < ts.length; j++) {
        if (Bytes.equals(ts[j].getName(), tables[i])) {
          found = true;
          break;
        }
      }
      assertTrue("Not found: " + Bytes.toString(tables[i]), found);
    }
  }

  @Test
  public void testMiscHTableStuff() throws IOException {
    final byte[] tableAname = Bytes.toBytes("testMiscHTableStuffA");
    final byte[] tableBname = Bytes.toBytes("testMiscHTableStuffB");
    final byte[] attrName = Bytes.toBytes("TESTATTR");
    final byte[] attrValue = Bytes.toBytes("somevalue");
    byte[] value = Bytes.toBytes("value");

    HTable a = TEST_UTIL.createTable(tableAname, HConstants.CATALOG_FAMILY);
    HTable b = TEST_UTIL.createTable(tableBname, HConstants.CATALOG_FAMILY);
    Put put = new Put(ROW);
    put.add(HConstants.CATALOG_FAMILY, null, value);
    a.put(put);

    // open a new connection to A and a connection to b
    HTable newA = new HTable(TEST_UTIL.getConfiguration(), tableAname);

    // copy data from A to B
    Scan scan = new Scan();
    scan.addFamily(HConstants.CATALOG_FAMILY);
    ResultScanner s = newA.getScanner(scan);
    try {
      for (Result r : s) {
        put = new Put(r.getRow());
        for (KeyValue kv : r.sorted()) {
          put.add(kv);
        }
        b.put(put);
      }
    } finally {
      s.close();
    }

    // Opening a new connection to A will cause the tables to be reloaded
    HTable anotherA = new HTable(TEST_UTIL.getConfiguration(), tableAname);
    Get get = new Get(ROW);
    get.addFamily(HConstants.CATALOG_FAMILY);
    anotherA.get(get);

    // We can still access A through newA because it has the table information
    // cached. And if it needs to recalibrate, that will cause the information
    // to be reloaded.

    // Test user metadata
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    // make a modifiable descriptor
    HTableDescriptor desc = new HTableDescriptor(a.getTableDescriptor());
    // offline the table
    admin.disableTable(tableAname);
    // add a user attribute to HTD
    desc.setValue(attrName, attrValue);
    // add a user attribute to HCD
    for (HColumnDescriptor c : desc.getFamilies())
      c.setValue(attrName, attrValue);
    // update metadata for all regions of this table
    admin.modifyTable(tableAname, desc);
    // enable the table
    admin.enableTable(tableAname);

    // Test that attribute changes were applied
    desc = a.getTableDescriptor();
    assertTrue("wrong table descriptor returned",
      Bytes.compareTo(desc.getName(), tableAname) == 0);
    // check HTD attribute
    value = desc.getValue(attrName);
    assertFalse("missing HTD attribute value", value == null);
    assertFalse("HTD attribute value is incorrect",
      Bytes.compareTo(value, attrValue) != 0);
    // check HCD attribute
    for (HColumnDescriptor c : desc.getFamilies()) {
      value = c.getValue(attrName);
      assertFalse("missing HCD attribute value", value == null);
      assertFalse("HCD attribute value is incorrect",
        Bytes.compareTo(value, attrValue) != 0);
    }
  }

  @Test
  public void testGetClosestRowBefore() throws IOException {
    final byte [] tableAname = Bytes.toBytes("testGetClosestRowBefore");
    final byte [] row = Bytes.toBytes("row");


    byte[] firstRow = Bytes.toBytes("ro");
    byte[] beforeFirstRow = Bytes.toBytes("rn");
    byte[] beforeSecondRow = Bytes.toBytes("rov");

    HTable table = TEST_UTIL.createTable(tableAname,
      new byte [][] {HConstants.CATALOG_FAMILY, Bytes.toBytes("info2")});
    Put put = new Put(firstRow);
    Put put2 = new Put(row);
    byte[] zero = new byte[]{0};
    byte[] one = new byte[]{1};

    put.add(HConstants.CATALOG_FAMILY, null, zero);
    put2.add(HConstants.CATALOG_FAMILY, null, one);

    table.put(put);
    table.put(put2);

    Result result = null;

    // Test before first that null is returned
    result = table.getRowOrBefore(beforeFirstRow, HConstants.CATALOG_FAMILY);
    assertTrue(result == null);

    // Test at first that first is returned
    result = table.getRowOrBefore(firstRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), zero));

    // Test in between first and second that first is returned
    result = table.getRowOrBefore(beforeSecondRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), zero));

    // Test at second make sure second is returned
    result = table.getRowOrBefore(row, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));

    // Test after second, make sure second is returned
    result = table.getRowOrBefore(Bytes.add(row,one), HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));
  }

  /**
   * For HBASE-2156
   * @throws Exception
   */
  @Test
  public void testScanVariableReuse() throws Exception {
    Scan scan = new Scan();
    scan.addFamily(FAMILY);
    scan.addColumn(FAMILY, ROW);

    assertTrue(scan.getFamilyMap().get(FAMILY).size() == 1);

    scan = new Scan();
    scan.addFamily(FAMILY);

    assertTrue(scan.getFamilyMap().get(FAMILY) == null);
    assertTrue(scan.getFamilyMap().containsKey(FAMILY));
  }

 
  @Test
  public void testIncrement() throws Exception {
    LOG.info("Starting testIncrement");
    final byte [] TABLENAME = Bytes.toBytes("testIncrement");
    HTable ht = TEST_UTIL.createTable(TABLENAME, FAMILY);

    byte [][] ROWS = new byte [][] {
        Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c"),
        Bytes.toBytes("d"), Bytes.toBytes("e"), Bytes.toBytes("f"),
        Bytes.toBytes("g"), Bytes.toBytes("h"), Bytes.toBytes("i")
    };
    byte [][] QUALIFIERS = new byte [][] {
        Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c"),
        Bytes.toBytes("d"), Bytes.toBytes("e"), Bytes.toBytes("f"),
        Bytes.toBytes("g"), Bytes.toBytes("h"), Bytes.toBytes("i")
    };

    // Do some simple single-column increments

    // First with old API
    ht.incrementColumnValue(ROW, FAMILY, QUALIFIERS[0], 1);
    ht.incrementColumnValue(ROW, FAMILY, QUALIFIERS[1], 2);
    ht.incrementColumnValue(ROW, FAMILY, QUALIFIERS[2], 3);
    ht.incrementColumnValue(ROW, FAMILY, QUALIFIERS[3], 4);

    // Now increment things incremented with old and do some new
    Increment inc = new Increment(ROW);
    inc.addColumn(FAMILY, QUALIFIERS[1], 1);
    inc.addColumn(FAMILY, QUALIFIERS[3], 1);
    inc.addColumn(FAMILY, QUALIFIERS[4], 1);
    ht.increment(inc);

    // Verify expected results
    Result r = ht.get(new Get(ROW));
    KeyValue [] kvs = r.raw();
    assertEquals(5, kvs.length);
    assertIncrementKey(kvs[0], ROW, FAMILY, QUALIFIERS[0], 1);
    assertIncrementKey(kvs[1], ROW, FAMILY, QUALIFIERS[1], 3);
    assertIncrementKey(kvs[2], ROW, FAMILY, QUALIFIERS[2], 3);
    assertIncrementKey(kvs[3], ROW, FAMILY, QUALIFIERS[3], 5);
    assertIncrementKey(kvs[4], ROW, FAMILY, QUALIFIERS[4], 1);

    // Now try multiple columns by different amounts
    inc = new Increment(ROWS[0]);
    for (int i=0;i<QUALIFIERS.length;i++) {
      inc.addColumn(FAMILY, QUALIFIERS[i], i+1);
    }
    ht.increment(inc);
    // Verify
    r = ht.get(new Get(ROWS[0]));
    kvs = r.raw();
    assertEquals(QUALIFIERS.length, kvs.length);
    for (int i=0;i<QUALIFIERS.length;i++) {
      assertIncrementKey(kvs[i], ROWS[0], FAMILY, QUALIFIERS[i], i+1);
    }

    // Re-increment them
    inc = new Increment(ROWS[0]);
    for (int i=0;i<QUALIFIERS.length;i++) {
      inc.addColumn(FAMILY, QUALIFIERS[i], i+1);
    }
    ht.increment(inc);
    // Verify
    r = ht.get(new Get(ROWS[0]));
    kvs = r.raw();
    assertEquals(QUALIFIERS.length, kvs.length);
    for (int i=0;i<QUALIFIERS.length;i++) {
      assertIncrementKey(kvs[i], ROWS[0], FAMILY, QUALIFIERS[i], 2*(i+1));
    }
  }

  /**
   * This test demonstrates how we use ThreadPoolExecutor.
   * It needs to show that we only use as many threads in the pool as we have
   * region servers. To do this, instead of doing real requests, we use a
   * SynchronousQueue where each put must wait for a take (and vice versa)
   * so that way we have full control of the number of active threads.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testPoolBehavior() throws IOException, InterruptedException {
    byte[] someBytes = Bytes.toBytes("pool");
    HTable table = TEST_UTIL.createTable(someBytes, someBytes);
    ThreadPoolExecutor pool = (ThreadPoolExecutor)table.getPool();

    // Make sure that the TPE stars with a core pool size of one and 0
    // initialized worker threads
    assertEquals(1, pool.getCorePoolSize());
    assertEquals(0, pool.getPoolSize());

    // Build a SynchronousQueue that we use for thread coordination
    final SynchronousQueue<Object> queue = new SynchronousQueue<Object>();
    List<Thread> threads = new ArrayList<Thread>(5);
    for (int i = 0; i < 5; i++) {
      threads.add(new Thread() {
        public void run() {
          try {
            // The thread blocks here until we decide to let it go
            queue.take();
          } catch (InterruptedException ie) { }
        }
      });
    }
    // First, add two threads and make sure the pool size follows
    pool.submit(threads.get(0));
    assertEquals(1, pool.getPoolSize());
    pool.submit(threads.get(1));
    assertEquals(2, pool.getPoolSize());

    // Next, terminate those threads and then make sure the pool is still the
    // same size
    queue.put(new Object());
    threads.get(0).join();
    queue.put(new Object());
    threads.get(1).join();
    assertEquals(2, pool.getPoolSize());

    // Now let's simulate adding a RS meaning that we'll go up to three
    // concurrent threads. The pool should not grow larger than three.
    pool.submit(threads.get(2));
    pool.submit(threads.get(3));
    pool.submit(threads.get(4));
    assertEquals(3, pool.getPoolSize());
    queue.put(new Object());
    queue.put(new Object());
    queue.put(new Object());
  }

  @Test
  public void testClientPoolRoundRobin() throws IOException {
    final byte[] tableName = Bytes.toBytes("testClientPoolRoundRobin");

    int poolSize = 3;
    int numVersions = poolSize * 2;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "round-robin");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);

    HTable table = TEST_UTIL.createTable(tableName, new byte[][] { FAMILY },
        conf, Integer.MAX_VALUE);
    table.setAutoFlush(true);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);

    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();

    for (int versions = 1; versions <= numVersions; versions++) {
      table.put(put);

      Result result = table.get(get);
      NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY)
          .get(QUALIFIER);

      assertEquals("The number of versions of '" + FAMILY + ":" + QUALIFIER
          + " did not match " + versions, versions, navigableMap.size());
      for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
        assertTrue("The value at time " + entry.getKey()
            + " did not match what was put",
            Bytes.equals(VALUE, entry.getValue()));
      }
    }
  }

  @Test
  public void testClientPoolThreadLocal() throws IOException {
    final byte[] tableName = Bytes.toBytes("testClientPoolThreadLocal");

    int poolSize = Integer.MAX_VALUE;
    int numVersions = 3;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "thread-local");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);

    final HTable table = TEST_UTIL.createTable(tableName,
        new byte[][] { FAMILY }, conf);
    table.setAutoFlush(true);
    final Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);

    final Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();

    for (int versions = 1; versions <= numVersions; versions++) {
      table.put(put);

      Result result = table.get(get);
      NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY)
          .get(QUALIFIER);

      assertEquals("The number of versions of '" + FAMILY + ":" + QUALIFIER
          + " did not match " + versions, versions, navigableMap.size());
      for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
        assertTrue("The value at time " + entry.getKey()
            + " did not match what was put",
            Bytes.equals(VALUE, entry.getValue()));
      }
    }

    final Object waitLock = new Object();

    ExecutorService executorService = Executors.newFixedThreadPool(numVersions);
    for (int versions = numVersions; versions < numVersions * 2; versions++) {
      final int versionsCopy = versions;
      executorService.submit(new Callable<Void>() {
        @Override
        public Void call() {
          try {
            table.put(put);

            Result result = table.get(get);
            NavigableMap<Long, byte[]> navigableMap = result.getMap()
                .get(FAMILY).get(QUALIFIER);

            assertEquals("The number of versions of '" + FAMILY + ":"
                + QUALIFIER + " did not match " + versionsCopy, versionsCopy,
                navigableMap.size());
            for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
              assertTrue("The value at time " + entry.getKey()
                  + " did not match what was put",
                  Bytes.equals(VALUE, entry.getValue()));
            }
            synchronized (waitLock) {
              waitLock.wait();
            }
          } catch (Exception e) {
          }

          return null;
        }
      });
    }
    synchronized (waitLock) {
      waitLock.notifyAll();
    }
    executorService.shutdownNow();
  }
}

