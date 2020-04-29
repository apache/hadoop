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
package org.apache.hadoop.hdfs.server.federation.store.driver;

import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.getStateStoreConfiguration;
import static org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreZooKeeperImpl.FEDERATION_STORE_ZK_PARENT_PATH;
import static org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreZooKeeperImpl.FEDERATION_STORE_ZK_PARENT_PATH_DEFAULT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreZooKeeperImpl;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.DisabledNameservice;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the ZooKeeper implementation of the State Store driver.
 */
public class TestStateStoreZK extends TestStateStoreDriverBase {

  private static TestingServer curatorTestingServer;
  private static CuratorFramework curatorFramework;
  private static String baseZNode;

  @BeforeClass
  public static void setupCluster() throws Exception {
    curatorTestingServer = new TestingServer();
    curatorTestingServer.start();
    String connectString = curatorTestingServer.getConnectString();
    curatorFramework = CuratorFrameworkFactory.builder()
        .connectString(connectString)
        .retryPolicy(new RetryNTimes(100, 100))
        .build();
    curatorFramework.start();

    // Create the ZK State Store
    Configuration conf =
        getStateStoreConfiguration(StateStoreZooKeeperImpl.class);
    conf.set(CommonConfigurationKeys.ZK_ADDRESS, connectString);
    // Disable auto-repair of connection
    conf.setLong(RBFConfigKeys.FEDERATION_STORE_CONNECTION_TEST_MS,
        TimeUnit.HOURS.toMillis(1));

    baseZNode = conf.get(FEDERATION_STORE_ZK_PARENT_PATH,
        FEDERATION_STORE_ZK_PARENT_PATH_DEFAULT);
    getStateStore(conf);
  }

  @AfterClass
  public static void tearDownCluster() {
    curatorFramework.close();
    try {
      curatorTestingServer.stop();
    } catch (IOException e) {
    }
  }

  @Before
  public void startup() throws IOException {
    removeAll(getStateStoreDriver());
  }

  private <T extends BaseRecord> String generateFakeZNode(
      Class<T> recordClass) throws IOException {
    String nodeName = StateStoreUtils.getRecordName(recordClass);
    String primaryKey = "test";

    if (nodeName != null) {
      return baseZNode + "/" + nodeName + "/" + primaryKey;
    }
    return null;
  }

  private void testGetNullRecord(StateStoreDriver driver) throws Exception {
    testGetNullRecord(driver, MembershipState.class);
    testGetNullRecord(driver, MountTable.class);
    testGetNullRecord(driver, RouterState.class);
    testGetNullRecord(driver, DisabledNameservice.class);
  }

  private <T extends BaseRecord> void testGetNullRecord(
      StateStoreDriver driver, Class<T> recordClass) throws Exception {
    driver.removeAll(recordClass);

    String znode = generateFakeZNode(recordClass);
    assertNull(curatorFramework.checkExists().forPath(znode));

    curatorFramework.create().withMode(CreateMode.PERSISTENT)
        .withACL(null).forPath(znode, null);
    assertNotNull(curatorFramework.checkExists().forPath(znode));

    driver.get(recordClass);
    assertNull(curatorFramework.checkExists().forPath(znode));
  }

  @Test
  public void testGetNullRecord() throws Exception {
    testGetNullRecord(getStateStoreDriver());
  }

  @Test
  public void testInsert()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testInsert(getStateStoreDriver());
  }

  @Test
  public void testUpdate()
      throws IllegalArgumentException, ReflectiveOperationException,
      IOException, SecurityException {
    testPut(getStateStoreDriver());
  }

  @Test
  public void testDelete()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testRemove(getStateStoreDriver());
  }

  @Test
  public void testFetchErrors()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testFetchErrors(getStateStoreDriver());
  }
}