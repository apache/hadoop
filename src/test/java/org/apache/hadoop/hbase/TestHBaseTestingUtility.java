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
package org.apache.hadoop.hbase;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test our testing utility class
 */
public class TestHBaseTestingUtility {
  private final Log LOG = LogFactory.getLog(this.getClass());

  /**
   * Basic sanity test that spins up multiple HDFS and HBase clusters that share
   * the same ZK ensemble. We then create the same table in both and make sure
   * that what we insert in one place doesn't end up in the other.
   * @throws Exception
   */
  @Test (timeout=180000)
  public void testMultiClusters() throws Exception {
    // Create three clusters

    // Cluster 1.
    HBaseTestingUtility htu1 = new HBaseTestingUtility();
    // Set a different zk path for each cluster
    htu1.getConfiguration().set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    htu1.startMiniZKCluster();

    // Cluster 2
    HBaseTestingUtility htu2 = new HBaseTestingUtility();
    htu2.getConfiguration().set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    htu2.getConfiguration().set("hbase.zookeeper.property.clientPort",
      htu1.getConfiguration().get("hbase.zookeeper.property.clientPort", "-1"));
    htu2.setZkCluster(htu1.getZkCluster());

    // Cluster 3; seed it with the conf from htu1 so we pickup the 'right'
    // zk cluster config; it is set back into the config. as part of the
    // start of minizkcluster.
    HBaseTestingUtility htu3 = new HBaseTestingUtility();
    htu3.getConfiguration().set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/3");
    htu3.getConfiguration().set("hbase.zookeeper.property.clientPort",
      htu1.getConfiguration().get("hbase.zookeeper.property.clientPort", "-1"));
    htu3.setZkCluster(htu1.getZkCluster());

    try {
      htu1.startMiniCluster();
      htu2.startMiniCluster();
      htu3.startMiniCluster();

      final byte[] TABLE_NAME = Bytes.toBytes("test");
      final byte[] FAM_NAME = Bytes.toBytes("fam");
      final byte[] ROW = Bytes.toBytes("row");
      final byte[] QUAL_NAME = Bytes.toBytes("qual");
      final byte[] VALUE = Bytes.toBytes("value");

      HTable table1 = htu1.createTable(TABLE_NAME, FAM_NAME);
      HTable table2 = htu2.createTable(TABLE_NAME, FAM_NAME);

      Put put = new Put(ROW);
      put.add(FAM_NAME, QUAL_NAME, VALUE);
      table1.put(put);

      Get get = new Get(ROW);
      get.addColumn(FAM_NAME, QUAL_NAME);
      Result res = table1.get(get);
      assertEquals(1, res.size());

      res = table2.get(get);
      assertEquals(0, res.size());

    } finally {
      htu3.shutdownMiniCluster();
      htu2.shutdownMiniCluster();
      htu1.shutdownMiniCluster();
    }
  }

  @Test public void testMiniCluster() throws Exception {
    HBaseTestingUtility hbt = new HBaseTestingUtility();

    MiniHBaseCluster cluster = hbt.startMiniCluster();
    try {
      assertEquals(1, cluster.getLiveRegionServerThreads().size());
    } finally {
      hbt.shutdownMiniCluster();
    }
  }

  /**
   *  Test that we can start and stop multiple time a cluster
   *   with the same HBaseTestingUtility.
   */
  @Test public void testMultipleStartStop() throws Exception{
    HBaseTestingUtility htu1 = new HBaseTestingUtility();
    Path foo = new Path("foo");

    htu1.startMiniCluster();
    htu1.getDFSCluster().getFileSystem().create(foo);
    assertTrue( htu1.getDFSCluster().getFileSystem().exists(foo));
    htu1.shutdownMiniCluster();

    htu1.startMiniCluster();
    assertFalse( htu1.getDFSCluster().getFileSystem().exists(foo));
    htu1.getDFSCluster().getFileSystem().create(foo);
    assertTrue( htu1.getDFSCluster().getFileSystem().exists(foo));
    htu1.shutdownMiniCluster();
  }


  @Test public void testMiniZooKeeper() throws Exception {
    HBaseTestingUtility hbt = new HBaseTestingUtility();
    MiniZooKeeperCluster cluster1 = hbt.startMiniZKCluster();
    try {
      assertEquals(0, cluster1.getBackupZooKeeperServerNum());
      assertTrue((cluster1.killCurrentActiveZooKeeperServer() == -1));
    } finally {
      hbt.shutdownMiniZKCluster();
    }

    // set up zookeeper cluster with 5 zk servers
    MiniZooKeeperCluster cluster2 = hbt.startMiniZKCluster(5);
    int defaultClientPort = 21818;
    cluster2.setDefaultClientPort(defaultClientPort);
    try {
      assertEquals(4, cluster2.getBackupZooKeeperServerNum());

      // killing the current active zk server
      assertTrue((cluster2.killCurrentActiveZooKeeperServer() >= defaultClientPort));
      assertTrue((cluster2.killCurrentActiveZooKeeperServer() >= defaultClientPort));
      assertEquals(2, cluster2.getBackupZooKeeperServerNum());
      assertEquals(3, cluster2.getZooKeeperServerNum());

      // killing the backup zk servers
      cluster2.killOneBackupZooKeeperServer();
      cluster2.killOneBackupZooKeeperServer();
      assertEquals(0, cluster2.getBackupZooKeeperServerNum());
      assertEquals(1, cluster2.getZooKeeperServerNum());

      // killing the last zk server
      assertTrue((cluster2.killCurrentActiveZooKeeperServer() == -1));
      // this should do nothing.
      cluster2.killOneBackupZooKeeperServer();
      assertEquals(-1, cluster2.getBackupZooKeeperServerNum());
      assertEquals(0, cluster2.getZooKeeperServerNum());
    } finally {
      hbt.shutdownMiniZKCluster();
    }
  }

  @Test public void testMiniDFSCluster() throws Exception {
    HBaseTestingUtility hbt = new HBaseTestingUtility();
    MiniDFSCluster cluster = hbt.startMiniDFSCluster(1);
    FileSystem dfs = cluster.getFileSystem();
    Path dir = new Path("dir");
    Path qualifiedDir = dfs.makeQualified(dir);
    LOG.info("dir=" + dir + ", qualifiedDir=" + qualifiedDir);
    assertFalse(dfs.exists(qualifiedDir));
    assertTrue(dfs.mkdirs(qualifiedDir));
    assertTrue(dfs.delete(qualifiedDir, true));
    hbt.shutdownMiniCluster();
  }

  @Test public void testSetupClusterTestBuildDir() throws Exception {
    HBaseTestingUtility hbt = new HBaseTestingUtility();
    Path testdir = hbt.getClusterTestDir();
    LOG.info("uuid-subdir=" + testdir);
    FileSystem fs = hbt.getTestFileSystem();

    assertFalse(fs.exists(testdir));

    hbt.startMiniDFSCluster(1);
    assertTrue(fs.exists(testdir));

    hbt.shutdownMiniCluster();
    assertFalse(fs.exists(testdir));

  }

  @Test public void testTestDir() throws Exception {
    HBaseTestingUtility hbt = new HBaseTestingUtility();
    Path testdir = hbt.getDataTestDir();
    LOG.info("testdir=" + testdir);
    FileSystem fs = hbt.getTestFileSystem();
    assertTrue(!fs.exists(testdir));
    assertTrue(fs.mkdirs(testdir));
    assertTrue(hbt.cleanupTestDir());
  }
}
