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
package org.apache.hadoop.hbase.replication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.EmptyWatcher;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.ReplicationRegionInterface;
import org.apache.hadoop.hbase.mapreduce.CopyTable;
import org.apache.hadoop.hbase.regionserver.replication.ReplicationRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestReplication implements HConstants{

  protected static final Log LOG = LogFactory.getLog(TestReplication.class);

  private static Configuration conf1;
  private static Configuration conf2;

  private static ZooKeeperWrapper zkw1;
  private static ZooKeeperWrapper zkw2;

  private static HBaseTestingUtility utility1;
  private static HBaseTestingUtility utility2;
  private static final int NB_ROWS_IN_BATCH = 100;
  private static final long SLEEP_TIME = 500; //ms
  private static final int NB_RETRIES = 10;

  private static final byte[] tableName = Bytes.toBytes("test");
  private static final byte[] famName = Bytes.toBytes("f");
  private static final byte[] row = Bytes.toBytes("row");
  private static final byte[] noRepfamName = Bytes.toBytes("norep");

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    try {
      conf1 = HBaseConfiguration.create();
      conf1.set(REGION_SERVER_CLASS, ReplicationRegionInterface.class
          .getName());
      conf1.set(REGION_SERVER_IMPL, ReplicationRegionServer.class
          .getName());
      conf1.set(ZOOKEEPER_ZNODE_PARENT, "/1");

      utility1 = new HBaseTestingUtility(conf1);
      utility1.startMiniZKCluster();
      MiniZooKeeperCluster miniZK = utility1.getZkCluster();
      zkw1 = new ZooKeeperWrapper(conf1, EmptyWatcher.instance);
      zkw1.writeZNode("/1", "replication", "");
      zkw1.writeZNode("/1/replication", "master",
          conf1.get(ZOOKEEPER_QUORUM)+":" +
          conf1.get("hbase.zookeeper.property.clientPort")+":/1");
      setIsReplication("true");

      LOG.info("Setup first Zk");

      conf2 = HBaseConfiguration.create();
      conf2.set(REGION_SERVER_CLASS, ReplicationRegionInterface.class
          .getName());
      conf2.set(REGION_SERVER_IMPL, ReplicationRegionServer.class
          .getName());
      conf2.set(ZOOKEEPER_ZNODE_PARENT, "/2");

      utility2 = new HBaseTestingUtility(conf2);
      utility2.setZkCluster(miniZK);
      zkw2 = new ZooKeeperWrapper(conf2, EmptyWatcher.instance);
      zkw2.writeZNode("/2", "replication", "");
      zkw2.writeZNode("/2/replication", "master",
          conf1.get(ZOOKEEPER_QUORUM)+":" +
          conf1.get("hbase.zookeeper.property.clientPort")+":/1");

      zkw1.writeZNode("/1/replication/peers", "test",
          conf2.get(ZOOKEEPER_QUORUM)+":" +
          conf2.get("hbase.zookeeper.property.clientPort")+":/2");

      LOG.info("Setup second Zk");

      utility1.startMiniCluster();
      utility2.startMiniCluster();

      utility1.startMiniMapReduceCluster();

      HTableDescriptor table = new HTableDescriptor(tableName);
      HColumnDescriptor fam = new HColumnDescriptor(famName);
      fam.setScope(REPLICATION_SCOPE_GLOBAL);
      table.addFamily(fam);
      fam = new HColumnDescriptor(noRepfamName);
      table.addFamily(fam);

      HBaseAdmin admin1 = new HBaseAdmin(conf1);
      HBaseAdmin admin2 = new HBaseAdmin(conf2);
      admin1.createTable(table);
      admin2.createTable(table);

    } catch (Exception ex) { ex.printStackTrace(); throw ex; }
  }

  private static void setIsReplication(String bool) throws Exception {
    zkw1.writeZNode("/1/replication", "state", bool);
    // Takes some ms for ZK to fire the watcher
    Thread.sleep(100);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    utility1.shutdownMiniCluster();
    utility2.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {}

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    setIsReplication("false");
    utility1.truncateTable(tableName);
    utility2.truncateTable(tableName);
    setIsReplication("true");
  }

  @Test
  public void testReplication() throws Exception {
    Put put = new Put(row);
    put.add(famName, row, row);

    HTable table1 = new HTable(conf1, tableName);
    table1.put(put);

    HTable table2 = new HTable(conf2, tableName);
    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      Result res = table2.get(get);
      if (res.size() == 0) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(res.value(), row);
        break;
      }
    }

    Delete del = new Delete(row);
    table1.delete(del);

    table2 = new HTable(conf2, tableName);  
    get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for del replication");
      }
      Result res = table2.get(get);
      if (res.size() >= 1) {
        LOG.info("Row not deleted");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }

    // normal Batch tests
    table1.setAutoFlush(false);
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      put = new Put(Bytes.toBytes(i));
      put.add(famName, row, row);
      table1.put(put);
    }
    table1.flushCommits();

    Scan scan = new Scan();

    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for normal batch replication");
      }
      ResultScanner scanner = table2.getScanner(scan);
      Result[] res = scanner.next(NB_ROWS_IN_BATCH);
      scanner.close();
      if (res.length != NB_ROWS_IN_BATCH) {
        LOG.info("Only got " + res.length + " rows");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }

    table1.setAutoFlush(true);

    // Test stopping replication
    setIsReplication("false");

    put = new Put(Bytes.toBytes("stop start"));
    put.add(famName, row, row);
    table1.put(put);

    get = new Get(Bytes.toBytes("stop start"));
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        break;
      }
      Result res = table2.get(get);
      if(res.size() >= 1) {
        fail("Replication wasn't stopped");

      } else {
        LOG.info("Row not replicated, let's wait a bit more...");
        Thread.sleep(SLEEP_TIME);
      }
    }

    // Test restart replication
    setIsReplication("true");

    table1.put(put);

    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      Result res = table2.get(get);
      if(res.size() == 0) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(res.value(), row);
        break;
      }
    }

    put = new Put(Bytes.toBytes("do not rep"));
    put.add(noRepfamName, row, row);
    table1.put(put);

    get = new Get(Bytes.toBytes("do not rep"));
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES-1) {
        break;
      }
      Result res = table2.get(get);
      if (res.size() >= 1) {
        fail("Not supposed to be replicated");
      } else {
        LOG.info("Row not replicated, let's wait a bit more...");
        Thread.sleep(SLEEP_TIME);
      }
    }

  }

  @Test
  public void testMRCopy() throws Exception {
    setIsReplication("false");
    HTable table1 = new HTable(conf1, tableName);
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.add(famName, row, row);
      table1.put(put);
    }
    String[] args = new String[] {
        "--rs.class="+ReplicationRegionInterface.class.getName(),
        "--rs.impl="+ReplicationRegionServer.class.getName(),
        "--peer.adr="+conf2.get(ZOOKEEPER_QUORUM)+":/2",
        "--families=f", "test"};
    Job job = CopyTable.createSubmittableJob(conf1, args);
    assertTrue(job.waitForCompletion(true));

    HTable table2 = new HTable(conf2, tableName);
    Scan scan = new Scan();
    ResultScanner scanner = table2.getScanner(scan);
    Result[] res = scanner.next(NB_ROWS_IN_BATCH);
    scanner.close();
    assertEquals(NB_ROWS_IN_BATCH, res.length);

  }
}
