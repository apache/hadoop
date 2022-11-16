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
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.util.curator.ZKCuratorManager.getNodePath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
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

  @Test
  public void testStoreNewMasterKey() throws Exception {

    // Manually create a DelegationKey,
    // and call the interface storeNewMasterKey to write the data to zk.
    DelegationKey key = new DelegationKey(1234, Time.now() + 60 * 60, "keyBytes".getBytes());
    RouterMasterKey paramRouterMasterKey = RouterMasterKey.newInstance(key.getKeyId(),
        ByteBuffer.wrap(key.getEncodedKey()), key.getExpiryDate());
    FederationStateStore stateStore = this.getStateStore();

    assertTrue(stateStore instanceof ZookeeperFederationStateStore);

    // Compare the data returned by the storeNewMasterKey
    // interface with the data queried by zk, and ensure that the data is consistent.
    RouterMasterKeyRequest routerMasterKeyRequest =
        RouterMasterKeyRequest.newInstance(paramRouterMasterKey);
    RouterMasterKeyResponse response = stateStore.storeNewMasterKey(routerMasterKeyRequest);
    assertNotNull(response);
    RouterMasterKey respRouterMasterKey = response.getRouterMasterKey();
    assertNotNull(respRouterMasterKey);

    // Get Data From zk.
    String nodeName = ROUTER_RM_DELEGATION_KEY_PREFIX + key.getKeyId();
    String nodePath = ZNODE_MASTER_KEY_PREFIX + nodeName;
    RouterMasterKey zkRouterMasterKey = getRouterMasterKeyFromZK(nodePath);

    assertNotNull(zkRouterMasterKey);
    assertEquals(paramRouterMasterKey, respRouterMasterKey);
    assertEquals(paramRouterMasterKey, zkRouterMasterKey);
    assertEquals(zkRouterMasterKey, respRouterMasterKey);
  }

  @Test
  public void testGetMasterKeyByDelegationKey() throws YarnException, IOException {

    // Manually create a DelegationKey,
    // and call the interface storeNewMasterKey to write the data to zk.
    DelegationKey key = new DelegationKey(5678, Time.now() + 60 * 60, "keyBytes".getBytes());
    RouterMasterKey paramRouterMasterKey = RouterMasterKey.newInstance(key.getKeyId(),
        ByteBuffer.wrap(key.getEncodedKey()), key.getExpiryDate());
    FederationStateStore stateStore = this.getStateStore();

    assertTrue(stateStore instanceof ZookeeperFederationStateStore);

    // Call the getMasterKeyByDelegationKey interface of stateStore to get the MasterKey data.
    RouterMasterKeyRequest routerMasterKeyRequest =
        RouterMasterKeyRequest.newInstance(paramRouterMasterKey);
    RouterMasterKeyResponse response = stateStore.storeNewMasterKey(routerMasterKeyRequest);
    assertNotNull(response);

    // Get Data From zk.
    String nodeName = ROUTER_RM_DELEGATION_KEY_PREFIX + key.getKeyId();
    String nodePath = ZNODE_MASTER_KEY_PREFIX + nodeName;
    RouterMasterKey zkRouterMasterKey = getRouterMasterKeyFromZK(nodePath);

    // Call the getMasterKeyByDelegationKey interface to get the returned result.
    // The zk data should be consistent with the returned data.
    RouterMasterKeyResponse response1 =
        stateStore.getMasterKeyByDelegationKey(routerMasterKeyRequest);
    assertNotNull(response1);
    RouterMasterKey respRouterMasterKey = response1.getRouterMasterKey();
    assertEquals(paramRouterMasterKey, respRouterMasterKey);
    assertEquals(paramRouterMasterKey, zkRouterMasterKey);
    assertEquals(zkRouterMasterKey, respRouterMasterKey);
  }

  @Test
  public void testRemoveStoredMasterKey() throws YarnException, IOException {

    // Manually create a DelegationKey,
    // and call the interface storeNewMasterKey to write the data to zk.
    DelegationKey key = new DelegationKey(2345, Time.now() + 60 * 60, "keyBytes".getBytes());
    RouterMasterKey paramRouterMasterKey = RouterMasterKey.newInstance(key.getKeyId(),
        ByteBuffer.wrap(key.getEncodedKey()), key.getExpiryDate());
    FederationStateStore stateStore = this.getStateStore();

    assertTrue(stateStore instanceof ZookeeperFederationStateStore);

    // We need to ensure that the returned result is not empty.
    RouterMasterKeyRequest routerMasterKeyRequest =
        RouterMasterKeyRequest.newInstance(paramRouterMasterKey);
    RouterMasterKeyResponse response = stateStore.storeNewMasterKey(routerMasterKeyRequest);
    assertNotNull(response);

    // We will check if delegationToken exists in zk.
    String nodeName = ROUTER_RM_DELEGATION_KEY_PREFIX + key.getKeyId();
    String nodePath = ZNODE_MASTER_KEY_PREFIX + nodeName;
    assertTrue(isExists(nodePath));

    // Call removeStoredMasterKey to remove the MasterKey data in zk.
    RouterMasterKeyResponse response1 = stateStore.removeStoredMasterKey(routerMasterKeyRequest);
    assertNotNull(response1);
    RouterMasterKey respRouterMasterKey = response1.getRouterMasterKey();
    assertNotNull(respRouterMasterKey);
    assertEquals(paramRouterMasterKey, respRouterMasterKey);

    // We have removed the RouterMasterKey data from zk,
    // the path should be empty at this point.
    assertFalse(isExists(nodePath));
  }

  @Test
  public void testStoreNewToken() throws YarnException, IOException {

    // We manually generate the DelegationToken,
    // and then call the StoreNewToken method to store the Token in zk.
    RMDelegationTokenIdentifier identifier = new RMDelegationTokenIdentifier(
        new Text("owner2"), new Text("renewer2"), new Text("realuser2"));
    FederationStateStore stateStore = this.getStateStore();
    int seqNum = stateStore.incrementDelegationTokenSeqNum();
    identifier.setSequenceNumber(seqNum);
    Long renewDate = Time.now();

    // Store new rm-token
    RouterStoreToken paramStoreToken = RouterStoreToken.newInstance(identifier, renewDate);
    RouterRMTokenRequest request = RouterRMTokenRequest.newInstance(paramStoreToken);
    RouterRMTokenResponse routerRMTokenResponse = stateStore.storeNewToken(request);
    assertNotNull(routerRMTokenResponse);
    RouterStoreToken respStoreToken = routerRMTokenResponse.getRouterStoreToken();
    assertNotNull(respStoreToken);

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
    assertEquals(paramStoreToken, zkRouterStoreToken);
    assertEquals(respStoreToken, zkRouterStoreToken);
  }

  @Test
  public void testUpdateStoredToken() throws YarnException, IOException {

    // We manually generate the DelegationToken,
    // and then call the StoreNewToken method to store the Token in zk.
    RMDelegationTokenIdentifier identifier = new RMDelegationTokenIdentifier(
        new Text("owner2"), new Text("renewer2"), new Text("realuser2"));
    FederationStateStore stateStore = this.getStateStore();
    int seqNum = stateStore.incrementDelegationTokenSeqNum();
    identifier.setSequenceNumber(seqNum);
    Long renewDate = Time.now();

    // Store new rm-token()
    RouterStoreToken storeToken = RouterStoreToken.newInstance(identifier, renewDate);
    RouterRMTokenRequest request = RouterRMTokenRequest.newInstance(storeToken);
    RouterRMTokenResponse routerRMTokenResponse = stateStore.storeNewToken(request);
    Assert.assertNotNull(routerRMTokenResponse);

    // We are ready to update some data renewDate2 & sequenceNumber2
    Long renewDate2 = Time.now();
    int sequenceNumber2 = stateStore.incrementDelegationTokenSeqNum();
    identifier.setSequenceNumber(sequenceNumber2);

    // Update rm-token
    RouterStoreToken paramStoreToken = RouterStoreToken.newInstance(identifier, renewDate2);
    RouterRMTokenRequest updateTokenRequest = RouterRMTokenRequest.newInstance(paramStoreToken);
    RouterRMTokenResponse updateTokenResponse = stateStore.updateStoredToken(updateTokenRequest);
    Assert.assertNotNull(updateTokenResponse);
    RouterStoreToken updateTokenResp = updateTokenResponse.getRouterStoreToken();

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
    assertEquals(paramStoreToken, zkRouterStoreToken);
    assertEquals(updateTokenResp, zkRouterStoreToken);
  }

  @Test
  public void testRemoveStoredToken() throws YarnException, IOException {

    // We manually generate the DelegationToken,
    // and then call the StoreNewToken method to store the Token in zk.
    RMDelegationTokenIdentifier identifier = new RMDelegationTokenIdentifier(
        new Text("owner2"), new Text("renewer2"), new Text("realuser2"));
    FederationStateStore stateStore = this.getStateStore();
    int seqNum = stateStore.incrementDelegationTokenSeqNum();
    identifier.setSequenceNumber(seqNum);
    Long renewDate = Time.now();

    // Store new rm-token
    RouterStoreToken storeToken = RouterStoreToken.newInstance(identifier, renewDate);
    RouterRMTokenRequest request = RouterRMTokenRequest.newInstance(storeToken);
    RouterRMTokenResponse routerRMTokenResponse = stateStore.storeNewToken(request);
    Assert.assertNotNull(routerRMTokenResponse);

    // Get delegationToken Path
    String nodeName = ROUTER_RM_DELEGATION_TOKEN_PREFIX + identifier.getSequenceNumber();
    String nodePath = getNodePath(ZNODE_DT_PREFIX, nodeName);

    // Check if the path exists, we expect the result to exist.
    assertTrue(isExists(nodePath));

    // Remove stored-token
    stateStore.removeStoredToken(request);

    // After the data is deleted, the path should not exist in zk
    assertFalse(isExists(nodePath));
  }

  @Test
  public void testGetTokenByRouterStoreToken() throws YarnException, IOException {

    // We manually generate the DelegationToken,
    // and then call the StoreNewToken method to store the Token in zk.
    RMDelegationTokenIdentifier identifier = new RMDelegationTokenIdentifier(
        new Text("owner2"), new Text("renewer2"), new Text("realuser2"));
    FederationStateStore stateStore = this.getStateStore();
    int seqNum = stateStore.incrementDelegationTokenSeqNum();
    identifier.setSequenceNumber(seqNum);
    Long renewDate = Time.now();

    // Store new rm-token
    RouterStoreToken storeToken = RouterStoreToken.newInstance(identifier, renewDate);
    RouterRMTokenRequest request = RouterRMTokenRequest.newInstance(storeToken);
    RouterRMTokenResponse routerRMTokenResponse = stateStore.storeNewToken(request);
    Assert.assertNotNull(routerRMTokenResponse);
    RouterStoreToken getTokenResp = routerRMTokenResponse.getRouterStoreToken();

    // Call getTokenByRouterStoreToken And Get Result
    RouterRMTokenResponse routerGetRMTokenResponse = stateStore.getTokenByRouterStoreToken(request);
    Assert.assertNotNull(routerGetRMTokenResponse);

    // Get delegationToken Path
    String nodeName = ROUTER_RM_DELEGATION_TOKEN_PREFIX + identifier.getSequenceNumber();
    String nodePath = getNodePath(ZNODE_DT_PREFIX, nodeName);

    // Check whether the token (paramStoreToken)
    // We generated is consistent with the data stored in zk.
    // We expect data to be consistent.
    RouterStoreToken zkRouterStoreToken = getStoreTokenFromZK(nodePath);
    assertNotNull(zkRouterStoreToken);
    assertEquals(getTokenResp, zkRouterStoreToken);
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
}