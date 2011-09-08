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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMasterReplication {

  private static final Log LOG = LogFactory.getLog(TestReplication.class);

  private static Configuration conf1;
  private static Configuration conf2;
  private static Configuration conf3;

  private static String clusterKey1;
  private static String clusterKey2;
  private static String clusterKey3;

  private static HBaseTestingUtility utility1;
  private static HBaseTestingUtility utility2;
  private static HBaseTestingUtility utility3;
  private static final long SLEEP_TIME = 500;
  private static final int NB_RETRIES = 10;

  private static final byte[] tableName = Bytes.toBytes("test");
  private static final byte[] famName = Bytes.toBytes("f");
  private static final byte[] row = Bytes.toBytes("row");
  private static final byte[] row1 = Bytes.toBytes("row1");
  private static final byte[] row2 = Bytes.toBytes("row2");
  private static final byte[] noRepfamName = Bytes.toBytes("norep");

  private static final byte[] count = Bytes.toBytes("count");
  private static final byte[] put = Bytes.toBytes("put");
  private static final byte[] delete = Bytes.toBytes("delete");

  private static HTableDescriptor table;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1 = HBaseConfiguration.create();
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    // smaller block size and capacity to trigger more operations
    // and test them
    conf1.setInt("hbase.regionserver.hlog.blocksize", 1024*20);
    conf1.setInt("replication.source.size.capacity", 1024);
    conf1.setLong("replication.source.sleepforretries", 100);
    conf1.setInt("hbase.regionserver.maxlogs", 10);
    conf1.setLong("hbase.master.logcleaner.ttl", 10);
    conf1.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    conf1.setBoolean("dfs.support.append", true);
    conf1.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf1.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        "org.apache.hadoop.hbase.replication.TestMasterReplication$CoprocessorCounter");

    utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    new ZooKeeperWatcher(conf1, "cluster1", null, true);

    conf2 = new Configuration(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");

    conf3 = new Configuration(conf1);
    conf3.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/3");

    utility2 = new HBaseTestingUtility(conf2);
    utility2.setZkCluster(miniZK);
    new ZooKeeperWatcher(conf2, "cluster3", null, true);

    utility3 = new HBaseTestingUtility(conf3);
    utility3.setZkCluster(miniZK);
    new ZooKeeperWatcher(conf3, "cluster3", null, true);

    clusterKey1 = conf1.get(HConstants.ZOOKEEPER_QUORUM)+":" +
    conf1.get("hbase.zookeeper.property.clientPort")+":/1";

    clusterKey2 = conf2.get(HConstants.ZOOKEEPER_QUORUM)+":" +
    conf2.get("hbase.zookeeper.property.clientPort")+":/2";

    clusterKey3 = conf3.get(HConstants.ZOOKEEPER_QUORUM)+":" +
    conf3.get("hbase.zookeeper.property.clientPort")+":/3";
    
    table = new HTableDescriptor(tableName);
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);
    fam = new HColumnDescriptor(noRepfamName);
    table.addFamily(fam);
  }

  @Test(timeout=300000)
  public void testCyclicReplication() throws Exception {
    LOG.info("testCyclicReplication");
    utility1.startMiniCluster();
    utility2.startMiniCluster();
    utility3.startMiniCluster();
    ReplicationAdmin admin1 = new ReplicationAdmin(conf1);
    ReplicationAdmin admin2 = new ReplicationAdmin(conf2);
    ReplicationAdmin admin3 = new ReplicationAdmin(conf3);

    new HBaseAdmin(conf1).createTable(table);
    new HBaseAdmin(conf2).createTable(table);
    new HBaseAdmin(conf3).createTable(table);
    HTable htable1 = new HTable(conf1, tableName);
    htable1.setWriteBufferSize(1024);
    HTable htable2 = new HTable(conf2, tableName);
    htable2.setWriteBufferSize(1024);
    HTable htable3 = new HTable(conf3, tableName);
    htable3.setWriteBufferSize(1024);
    
    admin1.addPeer("1", clusterKey2);
    admin2.addPeer("1", clusterKey3);
    admin3.addPeer("1", clusterKey1);

    // put "row" and wait 'til it got around
    putAndWait(row, famName, htable1, htable3);
    // it should have passed through table2
    check(row,famName,htable2);

    putAndWait(row1, famName, htable2, htable1);
    check(row,famName,htable3);
    putAndWait(row2, famName, htable3, htable2);
    check(row,famName,htable1);
    
    deleteAndWait(row,htable1,htable3);
    deleteAndWait(row1,htable2,htable1);
    deleteAndWait(row2,htable3,htable2);

    assertEquals("Puts were replicated back ", 3, getCount(htable1, put));
    assertEquals("Puts were replicated back ", 3, getCount(htable2, put));
    assertEquals("Puts were replicated back ", 3, getCount(htable3, put));
    assertEquals("Deletes were replicated back ", 3, getCount(htable1, delete));
    assertEquals("Deletes were replicated back ", 3, getCount(htable2, delete));
    assertEquals("Deletes were replicated back ", 3, getCount(htable3, delete));
    utility3.shutdownMiniCluster();
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  /**
   * Add a row to a table in each cluster, check it's replicated,
   * delete it, check's gone
   * Also check the puts and deletes are not replicated back to
   * the originating cluster.
   */
  @Test(timeout=300000)
  public void testSimplePutDelete() throws Exception {
    LOG.info("testSimplePutDelete");
    utility1.startMiniCluster();
    utility2.startMiniCluster();

    ReplicationAdmin admin1 = new ReplicationAdmin(conf1);
    ReplicationAdmin admin2 = new ReplicationAdmin(conf2);

    new HBaseAdmin(conf1).createTable(table);
    new HBaseAdmin(conf2).createTable(table);
    HTable htable1 = new HTable(conf1, tableName);
    htable1.setWriteBufferSize(1024);
    HTable htable2 = new HTable(conf2, tableName);
    htable2.setWriteBufferSize(1024);

    // set M-M
    admin1.addPeer("1", clusterKey2);
    admin2.addPeer("1", clusterKey1);

    // add rows to both clusters,
    // make sure they are both replication
    putAndWait(row, famName, htable1, htable2);
    putAndWait(row1, famName, htable2, htable1);

    // make sure "row" did not get replicated back.
    assertEquals("Puts were replicated back ", 2, getCount(htable1, put));

    // delete "row" and wait
    deleteAndWait(row, htable1, htable2);

    // make the 2nd cluster replicated back
    assertEquals("Puts were replicated back ", 2, getCount(htable2, put));

    deleteAndWait(row1, htable2, htable1);

    assertEquals("Deletes were replicated back ", 2, getCount(htable1, delete));
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  private int getCount(HTable t, byte[] type)  throws IOException {
    Get test = new Get(row);
    test.setAttribute("count", new byte[]{});
    Result res = t.get(test);
    return Bytes.toInt(res.getValue(count, type));
  }

  private void deleteAndWait(byte[] row, HTable source, HTable target)
  throws Exception {
    Delete del = new Delete(row);
    source.delete(del);

    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for del replication");
      }
      Result res = target.get(get);
      if (res.size() >= 1) {
        LOG.info("Row not deleted");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
  }

  private void check(byte[] row, byte[] fam, HTable t) throws IOException {
    Get get = new Get(row);
    Result res = t.get(get);
    if (res.size() == 0) {
      fail("Row is missing");
    }
  }

  private void putAndWait(byte[] row, byte[] fam, HTable source, HTable target)
  throws Exception {
    Put put = new Put(row);
    put.add(fam, row, row);
    source.put(put);

    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      Result res = target.get(get);
      if (res.size() == 0) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(res.value(), row);
        break;
      }
    }
  }

  /**
   * Use a coprocessor to count puts and deletes.
   * as KVs would be replicated back with the same timestamp
   * there is otherwise no way to count them.
   */
  public static class CoprocessorCounter extends BaseRegionObserver {
    private int nCount = 0;
    private int nDelete = 0;

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Map<byte[], List<KeyValue>> familyMap, final boolean writeToWAL)
        throws IOException {
      nCount++;
    }
    @Override
    public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
        final Map<byte[], List<KeyValue>> familyMap, final boolean writeToWAL)
        throws IOException {
      nDelete++;
    }
    @Override
    public void preGet(final ObserverContext<RegionCoprocessorEnvironment> c,
        final Get get, final List<KeyValue> result) throws IOException {
      if (get.getAttribute("count") != null) {
        result.clear();
        // order is important!
        result.add(new KeyValue(count, count, delete, Bytes.toBytes(nDelete)));
        result.add(new KeyValue(count, count, put, Bytes.toBytes(nCount)));
        c.bypass();
      }
    }
  }
}
