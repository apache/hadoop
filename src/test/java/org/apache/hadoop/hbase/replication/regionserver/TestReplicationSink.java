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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestReplicationSink {
  private static final Log LOG = LogFactory.getLog(TestReplicationSink.class);
  private static final int BATCH_SIZE = 10;

  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private static ReplicationSink SINK;

  private static final byte[] TABLE_NAME1 =
      Bytes.toBytes("table1");
  private static final byte[] TABLE_NAME2 =
      Bytes.toBytes("table2");

  private static final byte[] FAM_NAME1 = Bytes.toBytes("info1");
  private static final byte[] FAM_NAME2 = Bytes.toBytes("info2");

  private static HTable table1;
  private static Stoppable STOPPABLE = new Stoppable() {
    final AtomicBoolean stop = new AtomicBoolean(false);

    @Override
    public boolean isStopped() {
      return this.stop.get();
    }

    @Override
    public void stop(String why) {
      LOG.info("STOPPING BECAUSE: " + why);
      this.stop.set(true);
    }
    
  };

  private static HTable table2;

   /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    TEST_UTIL.startMiniCluster(3);
    SINK =
      new ReplicationSink(new Configuration(TEST_UTIL.getConfiguration()), STOPPABLE);
    table1 = TEST_UTIL.createTable(TABLE_NAME1, FAM_NAME1);
    table2 = TEST_UTIL.createTable(TABLE_NAME2, FAM_NAME2);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    STOPPABLE.stop("Shutting down");
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    table1 = TEST_UTIL.truncateTable(TABLE_NAME1);
    table2 = TEST_UTIL.truncateTable(TABLE_NAME2);
  }

  /**
   * Insert a whole batch of entries
   * @throws Exception
   */
  @Test
  public void testBatchSink() throws Exception {
    HLog.Entry[] entries = new HLog.Entry[BATCH_SIZE];
    for(int i = 0; i < BATCH_SIZE; i++) {
      entries[i] = createEntry(TABLE_NAME1, i, KeyValue.Type.Put);
    }
    SINK.replicateEntries(entries);
    Scan scan = new Scan();
    ResultScanner scanRes = table1.getScanner(scan);
    assertEquals(BATCH_SIZE, scanRes.next(BATCH_SIZE).length);
  }

  /**
   * Insert a mix of puts and deletes
   * @throws Exception
   */
  @Test
  public void testMixedPutDelete() throws Exception {
    HLog.Entry[] entries = new HLog.Entry[BATCH_SIZE/2];
    for(int i = 0; i < BATCH_SIZE/2; i++) {
      entries[i] = createEntry(TABLE_NAME1, i, KeyValue.Type.Put);
    }
    SINK.replicateEntries(entries);

    entries = new HLog.Entry[BATCH_SIZE];
    for(int i = 0; i < BATCH_SIZE; i++) {
      entries[i] = createEntry(TABLE_NAME1, i,
          i % 2 != 0 ? KeyValue.Type.Put: KeyValue.Type.DeleteColumn);
    }

    SINK.replicateEntries(entries);
    Scan scan = new Scan();
    ResultScanner scanRes = table1.getScanner(scan);
    assertEquals(BATCH_SIZE/2, scanRes.next(BATCH_SIZE).length);
  }

  /**
   * Insert to 2 different tables
   * @throws Exception
   */
  @Test
  public void testMixedPutTables() throws Exception {
    HLog.Entry[] entries = new HLog.Entry[BATCH_SIZE];
    for(int i = 0; i < BATCH_SIZE; i++) {
      entries[i] =
          createEntry( i % 2 == 0 ? TABLE_NAME2 : TABLE_NAME1,
              i, KeyValue.Type.Put);
    }

    SINK.replicateEntries(entries);
    Scan scan = new Scan();
    ResultScanner scanRes = table2.getScanner(scan);
    for(Result res : scanRes) {
      assertTrue(Bytes.toInt(res.getRow()) % 2 == 0);
    }
  }

  /**
   * Insert then do different types of deletes
   * @throws Exception
   */
  @Test
  public void testMixedDeletes() throws Exception {
    HLog.Entry[] entries = new HLog.Entry[3];
    for(int i = 0; i < 3; i++) {
      entries[i] = createEntry(TABLE_NAME1, i, KeyValue.Type.Put);
    }
    SINK.replicateEntries(entries);
    entries = new HLog.Entry[3];

    entries[0] = createEntry(TABLE_NAME1, 0, KeyValue.Type.DeleteColumn);
    entries[1] = createEntry(TABLE_NAME1, 1, KeyValue.Type.DeleteFamily);
    entries[2] = createEntry(TABLE_NAME1, 2, KeyValue.Type.DeleteColumn);

    SINK.replicateEntries(entries);

    Scan scan = new Scan();
    ResultScanner scanRes = table1.getScanner(scan);
    assertEquals(0, scanRes.next(3).length);
  }

  /**
   * Puts are buffered, but this tests when a delete (not-buffered) is applied
   * before the actual Put that creates it.
   * @throws Exception
   */
  @Test
  public void testApplyDeleteBeforePut() throws Exception {
    HLog.Entry[] entries = new HLog.Entry[5];
    for(int i = 0; i < 2; i++) {
      entries[i] = createEntry(TABLE_NAME1, i, KeyValue.Type.Put);
    }
    entries[2] = createEntry(TABLE_NAME1, 1, KeyValue.Type.DeleteFamily);
    for(int i = 3; i < 5; i++) {
      entries[i] = createEntry(TABLE_NAME1, i, KeyValue.Type.Put);
    }
    SINK.replicateEntries(entries);
    Get get = new Get(Bytes.toBytes(1));
    Result res = table1.get(get);
    assertEquals(0, res.size());
  }

  private HLog.Entry createEntry(byte [] table, int row,  KeyValue.Type type) {
    byte[] fam = Bytes.equals(table, TABLE_NAME1) ? FAM_NAME1 : FAM_NAME2;
    byte[] rowBytes = Bytes.toBytes(row);
    // Just make sure we don't get the same ts for two consecutive rows with
    // same key
    try {
      Thread.sleep(1);
    } catch (InterruptedException e) {
      LOG.info("Was interrupted while sleep, meh", e);
    }
    final long now = System.currentTimeMillis();
    KeyValue kv = null;
    if(type.getCode() == KeyValue.Type.Put.getCode()) {
      kv = new KeyValue(rowBytes, fam, fam, now,
          KeyValue.Type.Put, Bytes.toBytes(row));
    } else if (type.getCode() == KeyValue.Type.DeleteColumn.getCode()) {
        kv = new KeyValue(rowBytes, fam, fam,
            now, KeyValue.Type.DeleteColumn);
    } else if (type.getCode() == KeyValue.Type.DeleteFamily.getCode()) {
        kv = new KeyValue(rowBytes, fam, null,
            now, KeyValue.Type.DeleteFamily);
    }

    HLogKey key = new HLogKey(table, table, now, now,
        HConstants.DEFAULT_CLUSTER_ID);

    WALEdit edit = new WALEdit();
    edit.add(kv);

    return new HLog.Entry(key, edit);
  }
}
