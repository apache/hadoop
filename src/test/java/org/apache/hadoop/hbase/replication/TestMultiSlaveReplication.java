/*
 * Copyright The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMultiSlaveReplication {

  private static final Log LOG = LogFactory.getLog(TestReplication.class);

  private static Configuration conf1;
  private static Configuration conf2;
  private static Configuration conf3;

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
  private static final byte[] row3 = Bytes.toBytes("row3");
  private static final byte[] noRepfamName = Bytes.toBytes("norep");

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
  public void testMultiSlaveReplication() throws Exception {
    LOG.info("testCyclicReplication");
    MiniHBaseCluster master = utility1.startMiniCluster();
    utility2.startMiniCluster();
    utility3.startMiniCluster();
    ReplicationAdmin admin1 = new ReplicationAdmin(conf1);

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

    // put "row" and wait 'til it got around, then delete
    putAndWait(row, famName, htable1, htable2);
    deleteAndWait(row, htable1, htable2);
    // check it wasn't replication to cluster 3
    checkRow(row,0,htable3);

    putAndWait(row2, famName, htable1, htable2);

    // now roll the region server's logs
    new HBaseAdmin(conf1).rollHLogWriter(master.getRegionServer(0).getServerName().toString());
    // after the log was rolled put a new row
    putAndWait(row3, famName, htable1, htable2);

    admin1.addPeer("2", clusterKey3);

    // put a row, check it was replicated to all clusters
    putAndWait(row1, famName, htable1, htable2, htable3);
    // delete and verify
    deleteAndWait(row1, htable1, htable2, htable3);

    // make sure row2 did not get replicated after
    // cluster 3 was added
    checkRow(row2,0,htable3);

    // row3 will get replicated, because it was in the
    // latest log
    checkRow(row3,1,htable3);

    Put p = new Put(row);
    p.add(famName, row, row);
    htable1.put(p);
    // now roll the logs again
    new HBaseAdmin(conf1).rollHLogWriter(master.getRegionServer(0)
        .getServerName().toString());

    // cleanup "row2", also conveniently use this to wait replication
    // to finish
    deleteAndWait(row2, htable1, htable2, htable3);
    // Even if the log was rolled in the middle of the replication
    // "row" is still replication.
    checkRow(row, 1, htable2, htable3);

    // cleanup the rest
    deleteAndWait(row, htable1, htable2, htable3);
    deleteAndWait(row3, htable1, htable2, htable3);

    utility3.shutdownMiniCluster();
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  private void checkRow(byte[] row, int count, HTable... tables) throws IOException {
    Get get = new Get(row);
    for (HTable table : tables) {
      Result res = table.get(get);
      assertEquals(count, res.size());
    }
  }

  private void deleteAndWait(byte[] row, HTable source, HTable... targets)
  throws Exception {
    Delete del = new Delete(row);
    source.delete(del);

    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for del replication");
      }
      boolean removedFromAll = true;
      for (HTable target : targets) {
        Result res = target.get(get);
        if (res.size() >= 1) {
          LOG.info("Row not deleted");
          removedFromAll = false;
          break;
        }
      }
      if (removedFromAll) {
        break;
      } else {
        Thread.sleep(SLEEP_TIME);        
      }
    }
  }

  private void putAndWait(byte[] row, byte[] fam, HTable source, HTable... targets)
  throws Exception {
    Put put = new Put(row);
    put.add(fam, row, row);
    source.put(put);

    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      boolean replicatedToAll = true;
      for (HTable target : targets) {
        Result res = target.get(get);
        if (res.size() == 0) {
          LOG.info("Row not available");
          replicatedToAll = false;
          break;
        } else {
          assertArrayEquals(res.value(), row);
        }
      }
      if (replicatedToAll) {
        break;
      } else {
        Thread.sleep(SLEEP_TIME);
      }
    }
  }
}
