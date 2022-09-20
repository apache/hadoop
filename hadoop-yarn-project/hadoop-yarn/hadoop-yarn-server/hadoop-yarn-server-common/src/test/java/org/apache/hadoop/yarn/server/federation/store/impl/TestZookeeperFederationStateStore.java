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

import java.io.IOException;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecords;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for ZookeeperFederationStateStore.
 */
public class TestZookeeperFederationStateStore
    extends FederationStateStoreBaseTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestZookeeperFederationStateStore.class);

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

  @Test(expected = NotImplementedException.class)
  public void testStoreNewMasterKey() throws Exception {
    super.testStoreNewMasterKey();
  }

  @Test(expected = NotImplementedException.class)
  public void testGetMasterKeyByDelegationKey() throws YarnException, IOException {
    super.testGetMasterKeyByDelegationKey();
  }

  @Test(expected = NotImplementedException.class)
  public void testRemoveStoredMasterKey() throws YarnException, IOException {
    super.testRemoveStoredMasterKey();
  }
}