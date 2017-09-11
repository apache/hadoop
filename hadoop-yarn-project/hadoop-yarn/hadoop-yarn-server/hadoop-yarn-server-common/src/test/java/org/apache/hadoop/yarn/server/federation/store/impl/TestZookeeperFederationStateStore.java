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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    Configuration conf = new Configuration();
    super.setConf(conf);
    return new ZookeeperFederationStateStore();
  }
}