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
package org.apache.hadoop.util.curator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.util.JaasConfiguration;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the manager for ZooKeeper Curator.
 */
public class TestZKCuratorManager {

  private TestingServer server;
  private ZKCuratorManager curator;

  @Before
  public void setup() throws Exception {
    this.server = new TestingServer();

    Configuration conf = new Configuration();
    conf.set(
        CommonConfigurationKeys.ZK_ADDRESS, this.server.getConnectString());

    this.curator = new ZKCuratorManager(conf);
    this.curator.start();
  }

  @After
  public void teardown() throws Exception {
    this.curator.close();
    if (this.server != null) {
      this.server.close();
      this.server = null;
    }
  }

  @Test
  public void testReadWriteData() throws Exception {
    String testZNode = "/test";
    String expectedString = "testString";
    assertFalse(curator.exists(testZNode));
    curator.create(testZNode);
    assertTrue(curator.exists(testZNode));
    curator.setData(testZNode, expectedString, -1);
    String testString = curator.getStringData("/test");
    assertEquals(expectedString, testString);
  }

  @Test
  public void testChildren() throws Exception {
    List<String> children = curator.getChildren("/");
    assertEquals(1, children.size());

    assertFalse(curator.exists("/node1"));
    curator.create("/node1");
    assertTrue(curator.exists("/node1"));

    assertFalse(curator.exists("/node2"));
    curator.create("/node2");
    assertTrue(curator.exists("/node2"));

    children = curator.getChildren("/");
    assertEquals(3, children.size());

    curator.delete("/node2");
    assertFalse(curator.exists("/node2"));
    children = curator.getChildren("/");
    assertEquals(2, children.size());
  }

  @Test
  public void testGetStringData() throws Exception {
    String node1 = "/node1";
    String node2 = "/node2";
    assertFalse(curator.exists(node1));
    curator.create(node1);
    assertNull(curator.getStringData(node1));

    byte[] setData = "setData".getBytes(StandardCharsets.UTF_8);
    curator.setData(node1, setData, -1);
    assertEquals("setData", curator.getStringData(node1));

    Stat stat = new Stat();
    assertFalse(curator.exists(node2));
    curator.create(node2);
    assertNull(curator.getStringData(node2, stat));

    curator.setData(node2, setData, -1);
    assertEquals("setData", curator.getStringData(node2, stat));

  }
  @Test
  public void testTransaction() throws Exception {
    List<ACL> zkAcl = ZKUtil.parseACLs(CommonConfigurationKeys.ZK_ACL_DEFAULT);
    String fencingNodePath = "/fencing";
    String node1 = "/node1";
    String node2 = "/node2";
    byte[] testData = "testData".getBytes(StandardCharsets.UTF_8);
    assertFalse(curator.exists(fencingNodePath));
    assertFalse(curator.exists(node1));
    assertFalse(curator.exists(node2));
    ZKCuratorManager.SafeTransaction txn = curator.createTransaction(
        zkAcl, fencingNodePath);
    txn.create(node1, testData, zkAcl, CreateMode.PERSISTENT);
    txn.create(node2, testData, zkAcl, CreateMode.PERSISTENT);
    assertFalse(curator.exists(fencingNodePath));
    assertFalse(curator.exists(node1));
    assertFalse(curator.exists(node2));
    txn.commit();
    assertFalse(curator.exists(fencingNodePath));
    assertTrue(curator.exists(node1));
    assertTrue(curator.exists(node2));
    assertTrue(Arrays.equals(testData, curator.getData(node1)));
    assertTrue(Arrays.equals(testData, curator.getData(node2)));

    byte[] setData = "setData".getBytes(StandardCharsets.UTF_8);
    txn = curator.createTransaction(zkAcl, fencingNodePath);
    txn.setData(node1, setData, -1);
    txn.delete(node2);
    assertTrue(curator.exists(node2));
    assertTrue(Arrays.equals(testData, curator.getData(node1)));
    txn.commit();
    assertFalse(curator.exists(node2));
    assertTrue(Arrays.equals(setData, curator.getData(node1)));
  }

  @Test
  public void testJaasConfiguration() throws Exception {
    // Validate that HadoopZooKeeperFactory will set ZKConfig with given principals
    ZKCuratorManager.HadoopZookeeperFactory factory1 =
        new ZKCuratorManager.HadoopZookeeperFactory("foo1", "bar1", "bar1.keytab");
    ZooKeeper zk1 = factory1.newZooKeeper("connString", 1000, null, false);
    validateJaasConfiguration(ZKCuratorManager.HadoopZookeeperFactory.JAAS_CLIENT_ENTRY,
        "bar1", "bar1.keytab", zk1);

    // Validate that a new HadoopZooKeeperFactory will use the new principals
    ZKCuratorManager.HadoopZookeeperFactory factory2 =
        new ZKCuratorManager.HadoopZookeeperFactory("foo2", "bar2", "bar2.keytab");
    ZooKeeper zk2 = factory2.newZooKeeper("connString", 1000, null, false);
    validateJaasConfiguration(ZKCuratorManager.HadoopZookeeperFactory.JAAS_CLIENT_ENTRY,
        "bar2", "bar2.keytab", zk2);

    try {
      // Setting global configuration
      String testClientConfig = "TestClientConfig";
      JaasConfiguration jconf = new JaasConfiguration(testClientConfig, "test", "test.keytab");
      javax.security.auth.login.Configuration.setConfiguration(jconf);
      System.setProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY, testClientConfig);

      // Validate that a new HadoopZooKeeperFactory will use the global principals
      ZKCuratorManager.HadoopZookeeperFactory factory3 =
          new ZKCuratorManager.HadoopZookeeperFactory("foo3", "bar3", "bar3.keytab");
      ZooKeeper zk3 = factory3.newZooKeeper("connString", 1000, null, false);
      validateJaasConfiguration(testClientConfig, "test", "test.keytab", zk3);
    } finally {
      // Remove global configuration
      System.clearProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY);
    }
  }

  @Test
  public void testCuratorFrameworkFactory() throws Exception{
    // By not explicitly calling the NewZooKeeper method validate that the Curator override works.
    ZKClientConfig zkClientConfig = new ZKClientConfig();
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.ZK_ADDRESS, this.server.getConnectString());
    int numRetries = conf.getInt(CommonConfigurationKeys.ZK_NUM_RETRIES,
        CommonConfigurationKeys.ZK_NUM_RETRIES_DEFAULT);
    int zkSessionTimeout = conf.getInt(CommonConfigurationKeys.ZK_TIMEOUT_MS,
        CommonConfigurationKeys.ZK_TIMEOUT_MS_DEFAULT);
    int zkRetryInterval = conf.getInt(
        CommonConfigurationKeys.ZK_RETRY_INTERVAL_MS,
        CommonConfigurationKeys.ZK_RETRY_INTERVAL_MS_DEFAULT);
    RetryNTimes retryPolicy = new RetryNTimes(numRetries, zkRetryInterval);

    CuratorFramework client = CuratorFrameworkFactory.builder()
        .connectString(conf.get(CommonConfigurationKeys.ZK_ADDRESS))
        .zkClientConfig(zkClientConfig)
        .sessionTimeoutMs(zkSessionTimeout).retryPolicy(retryPolicy)
        .authorization(new ArrayList<>())
        .zookeeperFactory(new ZKCuratorManager.HadoopZookeeperFactory(
            "foo1", "bar1", "bar1.keytab", false,
            new SecurityUtil.TruststoreKeystore(conf))

        ).build();
    client.start();
    validateJaasConfiguration(ZKCuratorManager.HadoopZookeeperFactory.JAAS_CLIENT_ENTRY,
        "bar1", "bar1.keytab", client.getZookeeperClient().getZooKeeper());
  }

  private void validateJaasConfiguration(String clientConfig, String principal, String keytab,
      ZooKeeper zk) {
    assertEquals("Validate that expected clientConfig is set in ZK config", clientConfig,
        zk.getClientConfig().getProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY));

    AppConfigurationEntry[] entries = javax.security.auth.login.Configuration.getConfiguration()
        .getAppConfigurationEntry(clientConfig);
    assertEquals("Validate that expected principal is set in Jaas config", principal,
        entries[0].getOptions().get("principal"));
    assertEquals("Validate that expected keytab is set in Jaas config", keytab,
        entries[0].getOptions().get("keyTab"));
  }
}