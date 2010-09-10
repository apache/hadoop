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
package org.apache.hadoop.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class TestMultiParallel extends MultiRegionTable {
  // This test needs to be rewritten to use HBaseTestingUtility -- St.Ack 20100910
  private static final Log LOG = LogFactory.getLog(TestMultiParallel.class);
  private static final byte[] VALUE = Bytes.toBytes("value");
  private static final byte[] QUALIFIER = Bytes.toBytes("qual");
  private static final String FAMILY = "family";
  private static final String TEST_TABLE = "multi_test_table";
  private static final byte[] BYTES_FAMILY = Bytes.toBytes(FAMILY);
  private static final byte[] ONE_ROW = Bytes.toBytes("xxx");

  List<byte[]> keys = new ArrayList<byte[]>();

  public TestMultiParallel() {
    super(2, FAMILY);
    desc = new HTableDescriptor(TEST_TABLE);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    makeKeys();
  }

  private void makeKeys() {
    // Create a "non-uniform" test set with the following characteristics:
    // a) Unequal number of keys per region

    // Don't use integer as a multiple, so that we have a number of keys that is
    // not a multiple of the number of regions
    int numKeys = (int) ((float) KEYS.length * 10.33F);

    for (int i = 0; i < numKeys; i++) {
      int kIdx = i % KEYS.length;
      byte[] k = KEYS[kIdx];
      byte[] cp = new byte[k.length + 1];
      System.arraycopy(k, 0, cp, 0, k.length);
      cp[k.length] = new Integer(i % 256).byteValue();
      keys.add(cp);
    }

    // b) Same duplicate keys (showing multiple Gets/Puts to the same row, which
    // should work)
    // c) keys are not in sorted order (within a region), to ensure that the
    // sorting code and index mapping doesn't break the functionality
    for (int i = 0; i < 100; i++) {
      int kIdx = i % KEYS.length;
      byte[] k = KEYS[kIdx];
      byte[] cp = new byte[k.length + 1];
      System.arraycopy(k, 0, cp, 0, k.length);
      cp[k.length] = new Integer(i % 256).byteValue();
      keys.add(cp);
    }
  }

  public void testBatchWithGet() throws Exception {
    LOG.info("test=testBatchWithGet");
    HTable table = new HTable(conf, TEST_TABLE);

    // load test data
    List<Row> puts = constructPutRequests();
    table.batch(puts);

    // create a list of gets and run it
    List<Row> gets = new ArrayList<Row>();
    for (byte[] k : keys) {
      Get get = new Get(k);
      get.addColumn(BYTES_FAMILY, QUALIFIER);
      gets.add(get);
    }
    Result[] multiRes = new Result[gets.size()];
    table.batch(gets, multiRes);

    // Same gets using individual call API
    List<Result> singleRes = new ArrayList<Result>();
    for (Row get : gets) {
      singleRes.add(table.get((Get) get));
    }

    // Compare results
    assertEquals(singleRes.size(), multiRes.length);
    for (int i = 0; i < singleRes.size(); i++) {
      assertTrue(singleRes.get(i).containsColumn(BYTES_FAMILY, QUALIFIER));
      KeyValue[] singleKvs = singleRes.get(i).raw();
      KeyValue[] multiKvs = multiRes[i].raw();
      for (int j = 0; j < singleKvs.length; j++) {
        assertEquals(singleKvs[j], multiKvs[j]);
        assertEquals(0, Bytes.compareTo(singleKvs[j].getValue(), multiKvs[j]
            .getValue()));
      }
    }
  }

  /**
   * Only run one Multi test with a forced RegionServer abort. Otherwise, the
   * unit tests will take an unnecessarily long time to run.
   * 
   * @throws Exception
   */
  public void testFlushCommitsWithAbort() throws Exception {
    LOG.info("test=testFlushCommitsWithAbort");
    doTestFlushCommits(true);
  }

  public void testFlushCommitsNoAbort() throws Exception {
    doTestFlushCommits(false);
  }

  public void doTestFlushCommits(boolean doAbort) throws Exception {
    LOG.info("test=doTestFlushCommits");
    // Load the data
    Configuration newconf = new Configuration(conf);
    newconf.setInt("hbase.client.retries.number", 10);
    HTable table = new HTable(newconf, TEST_TABLE);
    table.setAutoFlush(false);
    table.setWriteBufferSize(10 * 1024 * 1024);

    List<Row> puts = constructPutRequests();
    for (Row put : puts) {
      table.put((Put) put);
    }
    table.flushCommits();

    if (doAbort) {
      cluster.abortRegionServer(0);

      // try putting more keys after the abort. same key/qual... just validating
      // no exceptions thrown
      puts = constructPutRequests();
      for (Row put : puts) {
        table.put((Put) put);
      }

      table.flushCommits();
    }

    validateLoadedData(table);

    // Validate server and region count
    HBaseAdmin admin = new HBaseAdmin(conf);
    ClusterStatus cs = admin.getClusterStatus();
    assertEquals((doAbort ? 1 : 2), cs.getServers());
    for (HServerInfo info : cs.getServerInfo()) {
      System.out.println(info);
      assertTrue(info.getLoad().getNumberOfRegions() > 10);
    }
  }

  public void testBatchWithPut() throws Exception {
    LOG.info("test=testBatchWithPut");
    Configuration newconf = new Configuration(conf);
    newconf.setInt("hbase.client.retries.number", 10);
    HTable table = new HTable(newconf, TEST_TABLE);

    // put multiple rows using a batch
    List<Row> puts = constructPutRequests();

    Result[] results = table.batch(puts);
    validateSizeAndEmpty(results, keys.size());

    if (true) {
      cluster.abortRegionServer(0);

      puts = constructPutRequests();
      results = table.batch(puts);
      validateSizeAndEmpty(results, keys.size());
    }

    validateLoadedData(table);
  }

  public void testBatchWithDelete() throws Exception {
    LOG.info("test=testBatchWithDelete");
    HTable table = new HTable(conf, TEST_TABLE);

    // Load some data
    List<Row> puts = constructPutRequests();
    Result[] results = table.batch(puts);
    validateSizeAndEmpty(results, keys.size());

    // Deletes
    List<Row> deletes = new ArrayList<Row>();
    for (int i = 0; i < keys.size(); i++) {
      Delete delete = new Delete(keys.get(i));
      delete.deleteFamily(BYTES_FAMILY);
      deletes.add(delete);
    }
    results = table.batch(deletes);
    validateSizeAndEmpty(results, keys.size());

    // Get to make sure ...
    for (byte[] k : keys) {
      Get get = new Get(k);
      get.addColumn(BYTES_FAMILY, QUALIFIER);
      assertFalse(table.exists(get));
    }

  }

  public void testHTableDeleteWithList() throws Exception {
    LOG.info("test=testHTableDeleteWithList");
    HTable table = new HTable(conf, TEST_TABLE);

    // Load some data
    List<Row> puts = constructPutRequests();
    Result[] results = table.batch(puts);
    validateSizeAndEmpty(results, keys.size());

    // Deletes
    ArrayList<Delete> deletes = new ArrayList<Delete>();
    for (int i = 0; i < keys.size(); i++) {
      Delete delete = new Delete(keys.get(i));
      delete.deleteFamily(BYTES_FAMILY);
      deletes.add(delete);
    }
    table.delete(deletes);
    assertTrue(deletes.isEmpty());

    // Get to make sure ...
    for (byte[] k : keys) {
      Get get = new Get(k);
      get.addColumn(BYTES_FAMILY, QUALIFIER);
      assertFalse(table.exists(get));
    }

  }

  public void testBatchWithManyColsInOneRowGetAndPut() throws Exception {
    LOG.info("test=testBatchWithManyColsInOneRowGetAndPut");
    HTable table = new HTable(conf, TEST_TABLE);

    List<Row> puts = new ArrayList<Row>();
    for (int i = 0; i < 100; i++) {
      Put put = new Put(ONE_ROW);
      byte[] qual = Bytes.toBytes("column" + i);
      put.add(BYTES_FAMILY, qual, VALUE);
      puts.add(put);
    }
    Result[] results = table.batch(puts);

    // validate
    validateSizeAndEmpty(results, 100);

    // get the data back and validate that it is correct
    List<Row> gets = new ArrayList<Row>();
    for (int i = 0; i < 100; i++) {
      Get get = new Get(ONE_ROW);
      byte[] qual = Bytes.toBytes("column" + i);
      get.addColumn(BYTES_FAMILY, qual);
      gets.add(get);
    }

    Result[] multiRes = table.batch(gets);

    int idx = 0;
    for (Result r : multiRes) {
      byte[] qual = Bytes.toBytes("column" + idx);
      validateResult(r, qual, VALUE);
      idx++;
    }

  }

  public void testBatchWithMixedActions() throws Exception {
    LOG.info("test=testBatchWithMixedActions");
    HTable table = new HTable(conf, TEST_TABLE);

    // Load some data to start
    Result[] results = table.batch(constructPutRequests());
    validateSizeAndEmpty(results, keys.size());

    // Batch: get, get, put(new col), delete, get, get of put, get of deleted,
    // put
    List<Row> actions = new ArrayList<Row>();

    byte[] qual2 = Bytes.toBytes("qual2");
    byte[] val2 = Bytes.toBytes("putvalue2");

    // 0 get
    Get get = new Get(keys.get(10));
    get.addColumn(BYTES_FAMILY, QUALIFIER);
    actions.add(get);

    // 1 get
    get = new Get(keys.get(11));
    get.addColumn(BYTES_FAMILY, QUALIFIER);
    actions.add(get);

    // 2 put of new column
    Put put = new Put(keys.get(10));
    put.add(BYTES_FAMILY, qual2, val2);
    actions.add(put);

    // 3 delete
    Delete delete = new Delete(keys.get(20));
    delete.deleteFamily(BYTES_FAMILY);
    actions.add(delete);

    // 4 get
    get = new Get(keys.get(30));
    get.addColumn(BYTES_FAMILY, QUALIFIER);
    actions.add(get);

    // 5 get of the put in #2 (entire family)
    get = new Get(keys.get(10));
    get.addFamily(BYTES_FAMILY);
    actions.add(get);

    // 6 get of the delete from #3
    get = new Get(keys.get(20));
    get.addColumn(BYTES_FAMILY, QUALIFIER);
    actions.add(get);

    // 7 put of new column
    put = new Put(keys.get(40));
    put.add(BYTES_FAMILY, qual2, val2);
    actions.add(put);

    results = table.batch(actions);

    // Validation

    validateResult(results[0]);
    validateResult(results[1]);
    validateEmpty(results[2]);
    validateEmpty(results[3]);
    validateResult(results[4]);
    validateResult(results[5]);
    validateResult(results[5], qual2, val2); // testing second column in #5
    validateEmpty(results[6]); // deleted
    validateEmpty(results[7]);

    // validate last put, externally from the batch
    get = new Get(keys.get(40));
    get.addColumn(BYTES_FAMILY, qual2);
    Result r = table.get(get);
    validateResult(r, qual2, val2);
  }

  // // Helper methods ////

  private void validateResult(Result r) {
    validateResult(r, QUALIFIER, VALUE);
  }

  private void validateResult(Result r, byte[] qual, byte[] val) {
    assertTrue(r.containsColumn(BYTES_FAMILY, qual));
    assertEquals(0, Bytes.compareTo(val, r.getValue(BYTES_FAMILY, qual)));
  }

  private List<Row> constructPutRequests() {
    List<Row> puts = new ArrayList<Row>();
    for (byte[] k : keys) {
      Put put = new Put(k);
      put.add(BYTES_FAMILY, QUALIFIER, VALUE);
      puts.add(put);
    }
    return puts;
  }

  private void validateLoadedData(HTable table) throws IOException {
    // get the data back and validate that it is correct
    for (byte[] k : keys) {
      Get get = new Get(k);
      get.addColumn(BYTES_FAMILY, QUALIFIER);
      Result r = table.get(get);
      assertTrue(r.containsColumn(BYTES_FAMILY, QUALIFIER));
      assertEquals(0, Bytes.compareTo(VALUE, r
          .getValue(BYTES_FAMILY, QUALIFIER)));
    }
  }

  private void validateEmpty(Result result) {
    assertTrue(result != null);
    assertTrue(result.getRow() == null);
    assertEquals(0, result.raw().length);
  }

  private void validateSizeAndEmpty(Result[] results, int expectedSize) {
    // Validate got back the same number of Result objects, all empty
    assertEquals(expectedSize, results.length);
    for (Result result : results) {
      validateEmpty(result);
    }
  }

}
