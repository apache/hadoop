/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.impl;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecords;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.util.curator.ZKCuratorManager.getNodePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for ZookeeperFederationStateStore.
 */
public class TestZookeeperFederationStateStore extends FederationStateStoreBaseTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestZookeeperFederationStateStore.class);

  private static final String ZNODE_FEDERATIONSTORE =
      "/federationstore";
  private static final String ZNODE_ROUTER_RM_DT_SECRET_MANAGER_ROOT =
      "/router_rm_dt_secret_manager_root";
  private static final String ZNODE_ROUTER_RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME =
      "/router_rm_delegation_tokens_root";
  private static final String ZNODE_ROUTER_RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME =
      "/router_rm_dt_master_keys_root/";
  private static final String ROUTER_RM_DELEGATION_TOKEN_PREFIX = "rm_delegation_token_";
  private static final String ROUTER_RM_DELEGATION_KEY_PREFIX = "delegation_key_";

  private static final String ZNODE_DT_PREFIX = ZNODE_FEDERATIONSTORE +
      ZNODE_ROUTER_RM_DT_SECRET_MANAGER_ROOT + ZNODE_ROUTER_RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME;
  private static final String ZNODE_MASTER_KEY_PREFIX = ZNODE_FEDERATIONSTORE +
      ZNODE_ROUTER_RM_DT_SECRET_MANAGER_ROOT + ZNODE_ROUTER_RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME;

  /** Zookeeper test server. */
  private static TestingServer curatorTestingServer;
  private static CuratorFramework curatorFramework;

  @Before
  public void before() throws IOException, YarnException {
    try {
      curatorTestingServer = new TestingServer();
      curatorTestingServer.start();
      String connectString = curatorTestingServer.getConnectString();
      curatorFramework = CuratorFrameworkFactory.builder()
          .connectString(connectString)
          .retryPolicy(new RetryNTimes(100, 100))
          .build();
      curatorFramework.start();

      Configuration conf = new YarnConfiguration();
      conf.set(CommonConfigurationKeys.ZK_ADDRESS, connectString);
      conf.setInt(YarnConfiguration.FEDERATION_STATESTORE_MAX_APPLICATIONS, 10);
      setConf(conf);
    } catch (Exception e) {
      LOG.error("Cannot initialize ZooKeeper store", e);
      throw new IOException(e);
    }

    super.before();
  }

  @After
  public void after() throws Exception {
    super.after();
    curatorFramework.close();
    try {
      curatorTestingServer.stop();
    } catch (IOException e) {
    }
  }

  @Override
  protected FederationStateStore createStateStore() {
    super.setConf(getConf());
    return new ZookeeperFederationStateStore();
  }

  @Test
  public void testMetricsInited() throws Exception {
    ZookeeperFederationStateStore zkStateStore = (ZookeeperFederationStateStore) createStateStore();
    ZKFederationStateStoreOpDurations zkStateStoreOpDurations = zkStateStore.getOpDurations();
    MetricsCollectorImpl collector = new MetricsCollectorImpl();

    long anyDuration = 10;
    long start = Time.now();
    long end = start + anyDuration;

    zkStateStoreOpDurations.addAppHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addUpdateAppHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addGetAppHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addGetAppsHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addDeleteAppHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addRegisterSubClusterDuration(start, end);
    zkStateStoreOpDurations.addDeregisterSubClusterDuration(start, end);
    zkStateStoreOpDurations.addSubClusterHeartbeatDuration(start, end);
    zkStateStoreOpDurations.addGetSubClusterDuration(start, end);
    zkStateStoreOpDurations.addGetSubClustersDuration(start, end);
    zkStateStoreOpDurations.addGetPolicyConfigurationDuration(start, end);
    zkStateStoreOpDurations.addSetPolicyConfigurationDuration(start, end);
    zkStateStoreOpDurations.addGetPoliciesConfigurationsDuration(start, end);
    zkStateStoreOpDurations.addReservationHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addGetReservationHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addGetReservationsHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addDeleteReservationHomeSubClusterDuration(start, end);
    zkStateStoreOpDurations.addUpdateReservationHomeSubClusterDuration(start, end);

    zkStateStoreOpDurations.getMetrics(collector, true);
    assertEquals("Incorrect number of perf metrics", 1, collector.getRecords().size());

    MetricsRecord record = collector.getRecords().get(0);
    MetricsRecords.assertTag(record, ZKFederationStateStoreOpDurations.RECORD_INFO.name(),
        "ZKFederationStateStoreOpDurations");

    double expectAvgTime = anyDuration;
    MetricsRecords.assertMetric(record, "AddAppHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "UpdateAppHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetAppHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetAppsHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "DeleteAppHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "RegisterSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "DeregisterSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "SubClusterHeartbeatAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetSubClustersAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetPolicyConfigurationAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "SetPolicyConfigurationAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetPoliciesConfigurationsAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "AddReservationHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetReservationHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetReservationsHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "DeleteReservationHomeSubClusterAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "UpdateReservationHomeSubClusterAvgTime",  expectAvgTime);

    long expectOps = 1;
    MetricsRecords.assertMetric(record, "AddAppHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "UpdateAppHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetAppHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetAppsHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "DeleteAppHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "RegisterSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "DeregisterSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "SubClusterHeartbeatNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetSubClustersNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetPolicyConfigurationNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "SetPolicyConfigurationNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetPoliciesConfigurationsNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "AddReservationHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetReservationHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetReservationsHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "DeleteReservationHomeSubClusterNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "UpdateReservationHomeSubClusterNumOps",  expectOps);
  }

  private RouterStoreToken getStoreTokenFromZK(String nodePath)
      throws YarnException {
    try {
      byte[] data = curatorFramework.getData().forPath(nodePath);
      if ((data == null) || (data.length == 0)) {
        return null;
      }
      ByteArrayInputStream bin = new ByteArrayInputStream(data);
      DataInputStream din = new DataInputStream(bin);
      RouterStoreToken storeToken = Records.newRecord(RouterStoreToken.class);
      storeToken.readFields(din);
      return storeToken;
    } catch (Exception e) {
      throw new YarnException(e);
    }
  }

  private RouterMasterKey getRouterMasterKeyFromZK(String nodePath) throws YarnException {
    try {
      byte[] data = curatorFramework.getData().forPath(nodePath);
      ByteArrayInputStream bin = new ByteArrayInputStream(data);
      DataInputStream din = new DataInputStream(bin);
      DelegationKey zkDT = new DelegationKey();
      zkDT.readFields(din);
      RouterMasterKey zkRouterMasterKey = RouterMasterKey.newInstance(
          zkDT.getKeyId(), ByteBuffer.wrap(zkDT.getEncodedKey()), zkDT.getExpiryDate());
      return zkRouterMasterKey;
    } catch (Exception e) {
      throw new YarnException(e);
    }
  }

  private boolean isExists(String path) throws YarnException {
    try {
      return (curatorFramework.checkExists().forPath(path) != null);
    } catch (Exception e) {
      throw new YarnException(e);
    }
  }

  protected void checkRouterMasterKey(DelegationKey delegationKey,
      RouterMasterKey routerMasterKey) throws YarnException, IOException {
    // Check for MasterKey stored in ZK
    RouterMasterKeyRequest routerMasterKeyRequest =
        RouterMasterKeyRequest.newInstance(routerMasterKey);

    // Get Data From zk.
    String nodeName = ROUTER_RM_DELEGATION_KEY_PREFIX + delegationKey.getKeyId();
    String nodePath = ZNODE_MASTER_KEY_PREFIX + nodeName;
    RouterMasterKey zkRouterMasterKey = getRouterMasterKeyFromZK(nodePath);

    // Call the getMasterKeyByDelegationKey interface to get the returned result.
    // The zk data should be consistent with the returned data.
    RouterMasterKeyResponse response = getStateStore().
        getMasterKeyByDelegationKey(routerMasterKeyRequest);
    assertNotNull(response);
    RouterMasterKey respRouterMasterKey = response.getRouterMasterKey();
    assertEquals(routerMasterKey, respRouterMasterKey);
    assertEquals(routerMasterKey, zkRouterMasterKey);
    assertEquals(zkRouterMasterKey, respRouterMasterKey);
  }

  protected void checkRouterStoreToken(RMDelegationTokenIdentifier identifier,
      RouterStoreToken token) throws YarnException, IOException {
    // Get delegationToken Path
    String nodeName = ROUTER_RM_DELEGATION_TOKEN_PREFIX + identifier.getSequenceNumber();
    String nodePath = getNodePath(ZNODE_DT_PREFIX, nodeName);

    // Check if the path exists, we expect the result to exist.
    assertTrue(isExists(nodePath));

    // Check whether the token (paramStoreToken)
    // We generated is consistent with the data stored in zk.
    // We expect data to be consistent.
    RouterStoreToken zkRouterStoreToken = getStoreTokenFromZK(nodePath);
    assertNotNull(zkRouterStoreToken);
    assertEquals(token, zkRouterStoreToken);
  }
}