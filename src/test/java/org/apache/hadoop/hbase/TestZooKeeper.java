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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestZooKeeper {
  private final Log LOG = LogFactory.getLog(this.getClass());

  private final static HBaseTestingUtility
      TEST_UTIL = new HBaseTestingUtility();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Test we can first start the ZK cluster by itself
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    TEST_UTIL.startMiniCluster(2);
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
    TEST_UTIL.ensureSomeRegionServersAvailable(2);
  }

  /**
   * See HBASE-1232 and http://wiki.apache.org/hadoop/ZooKeeper/FAQ#4.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testClientSessionExpired()
  throws IOException, InterruptedException {
    LOG.info("testClientSessionExpired");
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    new HTable(c, HConstants.META_TABLE_NAME);
    String quorumServers = ZKConfig.getZKQuorumServersString(c);
    int sessionTimeout = 5 * 1000; // 5 seconds
    HConnection connection = HConnectionManager.getConnection(c);
    ZooKeeperWatcher connectionZK = connection.getZooKeeperWatcher();
    long sessionID = connectionZK.getZooKeeper().getSessionId();
    byte[] password = connectionZK.getZooKeeper().getSessionPasswd();
    ZooKeeper zk = new ZooKeeper(quorumServers, sessionTimeout,
        EmptyWatcher.instance, sessionID, password);
    zk.close();

    Thread.sleep(sessionTimeout * 3L);

    // provoke session expiration by doing something with ZK
    ZKUtil.dump(connectionZK);

    // Check that the old ZK conenction is closed, means we did expire
    System.err.println("ZooKeeper should have timed out");
    LOG.info("state=" + connectionZK.getZooKeeper().getState());
    Assert.assertTrue(connectionZK.getZooKeeper().getState().equals(
        States.CLOSED));

    // Check that the client recovered
    ZooKeeperWatcher newConnectionZK = connection.getZooKeeperWatcher();
    LOG.info("state=" + newConnectionZK.getZooKeeper().getState());
    Assert.assertTrue(newConnectionZK.getZooKeeper().getState().equals(
        States.CONNECTED));
  }
  
  @Test
  public void testRegionServerSessionExpired() throws Exception {
    LOG.info("Starting testRegionServerSessionExpired");
    int metaIndex = TEST_UTIL.getMiniHBaseCluster().getServerWithMeta();
    TEST_UTIL.expireRegionServerSession(metaIndex);
    testSanity();
  }

  //@Test
  public void disabledTestMasterSessionExpired() throws Exception {
    LOG.info("Starting testMasterSessionExpired");
    TEST_UTIL.expireMasterSession();
    testSanity();
  }

  /**
   * Make sure we can use the cluster
   * @throws Exception
   */
  public void testSanity() throws Exception{
    HBaseAdmin admin =
      new HBaseAdmin(new Configuration(TEST_UTIL.getConfiguration()));
    String tableName = "test"+System.currentTimeMillis();
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor family = new HColumnDescriptor("fam");
    desc.addFamily(family);
    LOG.info("Creating table " + tableName);
    admin.createTable(desc);

    HTable table =
      new HTable(new Configuration(TEST_UTIL.getConfiguration()), tableName);
    Put put = new Put(Bytes.toBytes("testrow"));
    put.add(Bytes.toBytes("fam"),
        Bytes.toBytes("col"), Bytes.toBytes("testdata"));
    LOG.info("Putting table " + tableName);
    table.put(put);

  }

  @Test
  public void testMultipleZK() {
    try {
      HTable localMeta =
        new HTable(new Configuration(TEST_UTIL.getConfiguration()), HConstants.META_TABLE_NAME);
      Configuration otherConf = new Configuration(TEST_UTIL.getConfiguration());
      otherConf.set(HConstants.ZOOKEEPER_QUORUM, "127.0.0.1");
      HTable ipMeta = new HTable(otherConf, HConstants.META_TABLE_NAME);

      // dummy, just to open the connection
      localMeta.exists(new Get(HConstants.LAST_ROW));
      ipMeta.exists(new Get(HConstants.LAST_ROW));

      // make sure they aren't the same
      assertFalse(HConnectionManager.getConnection(localMeta.getConfiguration()).getZooKeeperWatcher()
          == HConnectionManager.getConnection(otherConf).getZooKeeperWatcher());
      assertFalse(HConnectionManager.getConnection(localMeta.getConfiguration())
          .getZooKeeperWatcher().getQuorum().equals(HConnectionManager
              .getConnection(otherConf).getZooKeeperWatcher().getQuorum()));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * Create a bunch of znodes in a hierarchy, try deleting one that has childs
   * (it will fail), then delete it recursively, then delete the last znode
   * @throws Exception
   */
  @Test
  public void testZNodeDeletes() throws Exception {
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(
      new Configuration(TEST_UTIL.getConfiguration()), 
      TestZooKeeper.class.getName(), null);
    ZKUtil.createWithParents(zkw, "/l1/l2/l3/l4");
    try {
      ZKUtil.deleteNode(zkw, "/l1/l2");
      fail("We should not be able to delete if znode has childs");
    } catch (KeeperException ex) {
      assertNotNull(ZKUtil.getDataNoWatch(zkw, "/l1/l2/l3/l4", null));
    }
    ZKUtil.deleteNodeRecursively(zkw, "/l1/l2");
    assertNull(ZKUtil.getDataNoWatch(zkw, "/l1/l2/l3/l4", null));
    ZKUtil.deleteNode(zkw, "/l1");
    assertNull(ZKUtil.getDataNoWatch(zkw, "/l1/l2", null));
  }

  @Test
  public void testClusterKey() throws Exception {
    testKey("server", "2181", "hbase");
    testKey("server1,server2,server3", "2181", "hbase");
    try {
      ZKUtil.transformClusterKey("2181:hbase");
    } catch (IOException ex) {
      // OK
    }
  }

  private void testKey(String ensemble, String port, String znode)
      throws IOException {
    Configuration conf = new Configuration();
    String key = ensemble+":"+port+":"+znode;
    String[] parts = ZKUtil.transformClusterKey(key);
    assertEquals(ensemble, parts[0]);
    assertEquals(port, parts[1]);
    assertEquals(znode, parts[2]);
    ZKUtil.applyClusterKeyToConf(conf, key);
    assertEquals(parts[0], conf.get(HConstants.ZOOKEEPER_QUORUM));
    assertEquals(parts[1], conf.get("hbase.zookeeper.property.clientPort"));
    assertEquals(parts[2], conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
    String reconstructedKey = ZKUtil.getZooKeeperClusterKey(conf);
    assertEquals(key, reconstructedKey);
  }

  /**
   * A test for HBASE-3238
   * @throws IOException A connection attempt to zk failed
   * @throws InterruptedException One of the non ZKUtil actions was interrupted
   * @throws KeeperException Any of the zookeeper connections had a
   * KeeperException
   */
  @Test
  public void testCreateSilentIsReallySilent() throws InterruptedException,
      KeeperException, IOException {
    Configuration c = TEST_UTIL.getConfiguration();

    String aclZnode = "/aclRoot";
    String quorumServers = ZKConfig.getZKQuorumServersString(c);
    int sessionTimeout = 5 * 1000; // 5 seconds
    ZooKeeper zk = new ZooKeeper(quorumServers, sessionTimeout, EmptyWatcher.instance);
    zk.addAuthInfo("digest", "hbase:rox".getBytes());

    // Assumes the  root of the ZooKeeper space is writable as it creates a node
    // wherever the cluster home is defined.
    ZooKeeperWatcher zk2 = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "testMasterAddressManagerFromZK",
        null);

    // I set this acl after the attempted creation of the cluster home node.
    zk.setACL("/", ZooDefs.Ids.CREATOR_ALL_ACL, -1);
    zk.create(aclZnode, null, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
    zk.close();

    ZKUtil.createAndFailSilent(zk2, aclZnode);
 }
}