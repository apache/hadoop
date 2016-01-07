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

package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;


public class LeaderElectorService extends AbstractService implements
    LeaderLatchListener {
  public static final Log LOG = LogFactory.getLog(LeaderElectorService.class);
  private LeaderLatch leaderLatch;
  private CuratorFramework curator;
  private RMContext rmContext;
  private String latchPath;
  private String rmId;

  public LeaderElectorService(RMContext rmContext) {
    super(LeaderElectorService.class.getName());
    this.rmContext = rmContext;

  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    String zkHostPort = conf.get(YarnConfiguration.RM_ZK_ADDRESS);
    Preconditions.checkNotNull(zkHostPort,
        YarnConfiguration.RM_ZK_ADDRESS + " is not set");

    rmId = HAUtil.getRMHAId(conf);
    String clusterId = YarnConfiguration.getClusterId(conf);

    int zkSessionTimeout = conf.getInt(YarnConfiguration.RM_ZK_TIMEOUT_MS,
        YarnConfiguration.DEFAULT_RM_ZK_TIMEOUT_MS);
    int maxRetryNum = conf.getInt(YarnConfiguration.RM_ZK_NUM_RETRIES,
        YarnConfiguration.DEFAULT_ZK_RM_NUM_RETRIES);

    String zkBasePath = conf.get(
        YarnConfiguration.AUTO_FAILOVER_ZK_BASE_PATH,
        YarnConfiguration.DEFAULT_AUTO_FAILOVER_ZK_BASE_PATH);
    latchPath = zkBasePath + "/" + clusterId;

    curator = CuratorFrameworkFactory.builder().connectString(zkHostPort)
        .retryPolicy(new RetryNTimes(maxRetryNum, zkSessionTimeout)).build();
    curator.start();
    initAndStartLeaderLatch();
    super.serviceInit(conf);
  }

  private void initAndStartLeaderLatch() throws Exception {
    leaderLatch = new LeaderLatch(curator, latchPath, rmId);
    leaderLatch.addListener(this);
    leaderLatch.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    closeLeaderLatch();
    super.serviceStop();
  }

  public boolean hasLeaderShip() {
    return leaderLatch.hasLeadership();
  }


  @Override
  public void isLeader() {
    LOG.info(rmId + "is elected leader, transitioning to active");
    try {
      rmContext.getRMAdminService().transitionToActive(
          new HAServiceProtocol.StateChangeRequestInfo(
              HAServiceProtocol.RequestSource.REQUEST_BY_ZKFC));
    } catch (Exception e) {
      LOG.info(rmId + " failed to transition to active, giving up leadership",
          e);
      notLeader();
      reJoinElection();
    }
  }

  public void reJoinElection() {
    try {
      closeLeaderLatch();
      Thread.sleep(1000);
      initAndStartLeaderLatch();
    } catch (Exception e) {
      LOG.info("Fail to re-join election.", e);
    }
  }

  private void closeLeaderLatch() throws IOException {
    if (leaderLatch != null) {
      leaderLatch.close();
    }
  }
  @Override
  public void notLeader() {
    LOG.info(rmId + " relinquish leadership");
    try {
      rmContext.getRMAdminService().transitionToStandby(
          new HAServiceProtocol.StateChangeRequestInfo(
              HAServiceProtocol.RequestSource.REQUEST_BY_ZKFC));
    } catch (Exception e) {
      LOG.info(rmId + " did not transition to standby successfully.");
    }
  }

  // only for testing
  @VisibleForTesting
  public CuratorFramework getCuratorClient() {
    return this.curator;
  }
}
