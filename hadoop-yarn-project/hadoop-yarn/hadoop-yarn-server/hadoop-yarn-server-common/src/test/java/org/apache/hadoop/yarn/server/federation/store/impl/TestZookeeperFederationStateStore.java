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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecords;
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
    zkStateStoreOpDurations.addAppHomeSubClusterCallDuration(anyDuration);
    zkStateStoreOpDurations.addUpdateAppHomeSubClusterCallDuration(anyDuration);
    zkStateStoreOpDurations.addGetAppHomeSubClusterCallDuration(anyDuration);
    zkStateStoreOpDurations.addGetAppsHomeSubClusterCallDuration(anyDuration);
    zkStateStoreOpDurations.addDeleteAppHomeSubClusterCallDuration(anyDuration);
    zkStateStoreOpDurations.addRegisterSubClusterCallDuration(anyDuration);
    zkStateStoreOpDurations.addDeregisterSubClusterCallDuration(anyDuration);
    zkStateStoreOpDurations.addSubClusterHeartbeatCallDuration(anyDuration);
    zkStateStoreOpDurations.addGetSubClusterCallDuration(anyDuration);
    zkStateStoreOpDurations.addGetSubClustersCallDuration(anyDuration);
    zkStateStoreOpDurations.addGetPolicyConfigurationDuration(anyDuration);
    zkStateStoreOpDurations.addSetPolicyConfigurationDuration(anyDuration);
    zkStateStoreOpDurations.addGetPoliciesConfigurationsDuration(anyDuration);

    zkStateStoreOpDurations.getMetrics(collector, true);
    assertEquals("Incorrect number of perf metrics", 1, collector.getRecords().size());

    MetricsRecord record = collector.getRecords().get(0);
    MetricsRecords.assertTag(record, ZKFederationStateStoreOpDurations.RECORD_INFO.name(),
        "ZKFederationStateStoreOpDurations");

    double expectAvgTime = anyDuration;
    MetricsRecords.assertMetric(record, "AddAppHomeSubClusterCallAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "UpdateAppHomeSubClusterCallAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetAppHomeSubClusterCallAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetAppsHomeSubClusterCallAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "DeleteAppHomeSubClusterCallAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "RegisterSubClusterCallAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "DeregisterSubClusterCallAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "SubClusterHeartbeatCallAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetSubClusterCallAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetSubClustersCallAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetPolicyConfigurationCallAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "SetPolicyConfigurationCallAvgTime",  expectAvgTime);
    MetricsRecords.assertMetric(record, "GetPoliciesConfigurationsCallAvgTime",  expectAvgTime);

    long expectOps = 1;
    MetricsRecords.assertMetric(record, "AddAppHomeSubClusterCallNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "UpdateAppHomeSubClusterCallNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetAppHomeSubClusterCallNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetAppsHomeSubClusterCallNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "DeleteAppHomeSubClusterCallNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "RegisterSubClusterCallNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "DeregisterSubClusterCallNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "SubClusterHeartbeatCallNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetSubClusterCallNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetSubClustersCallNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetPolicyConfigurationCallNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "SetPolicyConfigurationCallNumOps",  expectOps);
    MetricsRecords.assertMetric(record, "GetPoliciesConfigurationsCallNumOps",  expectOps);
  }
}